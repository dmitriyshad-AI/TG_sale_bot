from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Optional
from uuid import uuid4

from fastapi import HTTPException, UploadFile, status

from sales_agent.sales_core.call_copilot import (
    build_call_insights,
    build_transcript_fallback,
    extract_transcript_from_file,
)
from sales_agent.sales_core.config import Settings, project_root
from sales_agent.sales_core.db import (
    claim_failed_call_record_for_retry,
    claim_failed_mango_event_for_retry,
    clear_call_record_file_path,
    create_approval_action,
    create_call_record,
    create_or_get_mango_event,
    create_followup_task,
    create_lead_score,
    create_reply_draft,
    get_connection,
    get_conversation_outcome,
    get_or_create_user,
    get_call_record,
    get_latest_mango_event_created_at,
    list_call_records_with_files_for_cleanup,
    list_call_records_for_retry,
    list_mango_events,
    update_call_record_status,
    update_mango_event_status,
    upsert_call_summary,
    upsert_call_transcript,
)
from sales_agent.sales_core.mango_auto_ingest import (
    event_from_mango_record,
    extract_mango_user_and_thread,
    fetch_mango_poll_events_with_retries,
)
from sales_agent.sales_core.mango_client import MangoCallEvent, MangoClient, MangoClientError


class RevenueOpsService:
    def __init__(
        self,
        *,
        settings: Settings,
        db_path: Path,
        require_user_exists: Callable[[Any, int], None],
        thread_id_from_user_id: Callable[[int], str],
        lead_radar_rule_no_reply: str,
        lead_radar_rule_call_no_next_step: str,
        lead_radar_rule_stale_warm: str,
        lead_radar_model_name: str,
        call_copilot_model_name: str,
        mango_cleanup_batch_size: int,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.settings = settings
        self.db_path = db_path
        self.require_user_exists = require_user_exists
        self.thread_id_from_user_id = thread_id_from_user_id
        self.lead_radar_rule_no_reply = lead_radar_rule_no_reply
        self.lead_radar_rule_call_no_next_step = lead_radar_rule_call_no_next_step
        self.lead_radar_rule_stale_warm = lead_radar_rule_stale_warm
        self.lead_radar_model_name = lead_radar_model_name
        self.call_copilot_model_name = call_copilot_model_name
        self.mango_cleanup_batch_size = mango_cleanup_batch_size
        self.logger = logger or logging.getLogger(__name__)

        self._lead_radar_lock: Optional[asyncio.Lock] = None
        self._mango_ingest_lock: Optional[asyncio.Lock] = None
        self._mango_batch_lock: Optional[asyncio.Lock] = None
        self._call_retry_lock: Optional[asyncio.Lock] = None

    def mango_ingest_enabled(self) -> bool:
        return bool(self.settings.enable_mango_auto_ingest and self.settings.enable_call_copilot)

    def build_mango_client(self) -> MangoClient:
        return self._build_mango_client()

    def _calls_storage_root(self) -> Path:
        if self.settings.persistent_data_root != Path():
            return self.settings.persistent_data_root / "calls_uploads"
        return project_root() / "data" / "calls_uploads"

    def _resolve_call_thread_and_user(
        self,
        conn: Any,
        *,
        user_id_input: Optional[int],
        thread_id_input: Optional[str],
    ) -> tuple[int, str]:
        normalized_thread = (thread_id_input or "").strip()
        resolved_user_id: Optional[int] = None

        if isinstance(user_id_input, int):
            self.require_user_exists(conn, user_id_input)
            resolved_user_id = int(user_id_input)
            if not normalized_thread:
                normalized_thread = self.thread_id_from_user_id(resolved_user_id)

        if resolved_user_id is None and normalized_thread.startswith("tg:"):
            user_token = normalized_thread[3:]
            if user_token.isdigit():
                candidate_user_id = int(user_token)
                self.require_user_exists(conn, candidate_user_id)
                resolved_user_id = candidate_user_id

        if resolved_user_id is None and normalized_thread.startswith("biz:"):
            row = conn.execute(
                """
                SELECT user_id
                FROM business_threads
                WHERE thread_key = ?
                LIMIT 1
                """,
                (normalized_thread,),
            ).fetchone()
            if row and row["user_id"] is not None:
                resolved_user_id = int(row["user_id"])

        if resolved_user_id is None:
            external_id = f"call-import-{uuid4().hex[:12]}"
            resolved_user_id = get_or_create_user(
                conn,
                channel="call_import",
                external_id=external_id,
            )
            if not normalized_thread:
                normalized_thread = self.thread_id_from_user_id(resolved_user_id)

        if not normalized_thread:
            normalized_thread = self.thread_id_from_user_id(resolved_user_id)
        return resolved_user_id, normalized_thread

    async def _store_call_upload_file(self, upload: UploadFile) -> Optional[Path]:
        filename = (upload.filename or "").strip()
        if not filename:
            return None
        suffix = Path(filename).suffix.lower()
        if not suffix or len(suffix) > 12:
            suffix = ".bin"
        target_dir = self._calls_storage_root()
        target_dir.mkdir(parents=True, exist_ok=True)
        file_path = target_dir / f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid4().hex}{suffix}"
        content = await upload.read()
        if not content:
            return None
        file_path.write_bytes(content)
        return file_path

    def _priority_from_warmth(self, value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized in {"hot", "warm", "cold"}:
            return normalized
        return "warm"

    def _call_side_effects_exist(self, conn: Any, *, call_id: int) -> bool:
        call_token = str(int(call_id))
        with_space = f'%\"call_id\": {call_token}%'
        no_space = f'%\"call_id\":{call_token}%'
        as_string = f'%\"call_id\":\"{call_token}\"%'
        row = conn.execute(
            """
            SELECT id
            FROM approval_actions
            WHERE action IN ('manual_action', 'followup_created', 'lead_scored')
              AND (payload_json LIKE ? OR payload_json LIKE ? OR payload_json LIKE ?)
            LIMIT 1
            """,
            (with_space, no_space, as_string),
        ).fetchone()
        return row is not None

    def _build_mango_client(self) -> MangoClient:
        if not self.settings.mango_api_base_url or not self.settings.mango_api_token:
            raise MangoClientError(
                "Mango API is not configured. Fill MANGO_API_BASE_URL and MANGO_API_TOKEN."
            )
        return MangoClient(
            base_url=self.settings.mango_api_base_url,
            token=self.settings.mango_api_token,
            calls_path=self.settings.mango_calls_path,
            timeout_seconds=12.0,
            webhook_secret=self.settings.mango_webhook_secret,
        )

    def cleanup_old_call_files(self) -> Dict[str, Any]:
        conn = get_connection(self.db_path)
        cleaned = 0
        missing = 0
        errors = 0
        try:
            rows = list_call_records_with_files_for_cleanup(
                conn,
                older_than_hours=max(1, int(self.settings.mango_call_recording_ttl_hours)),
                limit=self.mango_cleanup_batch_size,
            )
            for row in rows:
                call_id = int(row.get("id") or 0)
                raw_path = str(row.get("file_path") or "").strip()
                if not call_id or not raw_path:
                    continue
                file_path = Path(raw_path)
                try:
                    if file_path.exists():
                        file_path.unlink()
                        cleaned += 1
                    else:
                        missing += 1
                    clear_call_record_file_path(conn, call_id=call_id)
                except Exception:
                    errors += 1
            return {"ok": True, "checked": len(rows), "cleaned": cleaned, "missing": missing, "errors": errors}
        finally:
            conn.close()

    def _pending_radar_followup_exists(self, conn: Any, *, thread_id: str, rule_key: str) -> bool:
        row = conn.execute(
            """
            SELECT id
            FROM followup_tasks
            WHERE thread_id = ?
              AND status = 'pending'
              AND reason LIKE ?
            LIMIT 1
            """,
            (thread_id, f"{rule_key}%"),
        ).fetchone()
        return row is not None

    def _radar_followup_exists_for_source_token(
        self,
        conn: Any,
        *,
        thread_id: str,
        rule_key: str,
        source_token: str,
    ) -> bool:
        normalized_source = (source_token or "").strip()
        if not normalized_source:
            return False
        row = conn.execute(
            """
            SELECT id
            FROM followup_tasks
            WHERE thread_id = ?
              AND reason LIKE ?
            LIMIT 1
            """,
            (thread_id, f"{rule_key}%[source={normalized_source}]%"),
        ).fetchone()
        return row is not None

    def _count_recent_radar_followups(self, conn: Any, *, thread_id: str, hours: int) -> int:
        normalized_hours = max(0, int(hours))
        if normalized_hours <= 0:
            return 0
        row = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM followup_tasks
            WHERE thread_id = ?
              AND reason LIKE 'radar:%'
              AND created_at >= datetime('now', ?)
            """,
            (thread_id, f"-{normalized_hours} hours"),
        ).fetchone()
        return int(row["cnt"]) if row else 0

    def _has_recent_draft_sent(self, conn: Any, *, thread_id: str, hours: int) -> bool:
        normalized_hours = max(0, int(hours))
        if normalized_hours <= 0:
            return False
        row = conn.execute(
            """
            SELECT id
            FROM approval_actions
            WHERE thread_id = ?
              AND action = 'draft_sent'
              AND created_at >= datetime('now', ?)
            LIMIT 1
            """,
            (thread_id, f"-{normalized_hours} hours"),
        ).fetchone()
        return row is not None

    def _count_today_radar_followups(self, conn: Any, *, thread_id: str) -> int:
        row = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM followup_tasks
            WHERE thread_id = ?
              AND reason LIKE 'radar:%'
              AND created_at >= date('now')
            """,
            (thread_id,),
        ).fetchone()
        return int(row["cnt"]) if row else 0

    def _lead_radar_guard(
        self,
        conn: Any,
        *,
        thread_id: str,
        rule_key: str,
        source_token: str,
    ) -> tuple[bool, str]:
        if self._pending_radar_followup_exists(conn, thread_id=thread_id, rule_key=rule_key):
            return (False, "pending_rule")
        if self._radar_followup_exists_for_source_token(
            conn,
            thread_id=thread_id,
            rule_key=rule_key,
            source_token=source_token,
        ):
            return (False, "duplicate_trigger")
        if self.settings.lead_radar_thread_cooldown_hours > 0:
            if self._has_recent_draft_sent(
                conn,
                thread_id=thread_id,
                hours=self.settings.lead_radar_thread_cooldown_hours,
            ):
                return (False, "recent_sent")
            recent_count = self._count_recent_radar_followups(
                conn,
                thread_id=thread_id,
                hours=self.settings.lead_radar_thread_cooldown_hours,
            )
            if recent_count > 0:
                return (False, "cooldown")
        today_count = self._count_today_radar_followups(conn, thread_id=thread_id)
        if today_count >= int(self.settings.lead_radar_daily_cap_per_thread):
            return (False, "daily_cap")
        return (True, "ok")

    def _collect_no_reply_candidates(self, conn: Any, *, no_reply_hours: int, limit: int) -> list[Dict[str, Any]]:
        cursor = conn.execute(
            """
            WITH last_inbound AS (
                SELECT m.user_id, m.id AS inbound_message_id, m.created_at AS inbound_at, m.text AS inbound_text
                FROM messages m
                JOIN (
                    SELECT user_id, MAX(id) AS max_id
                    FROM messages
                    WHERE direction = 'inbound'
                    GROUP BY user_id
                ) latest ON latest.max_id = m.id
            ),
            last_outbound AS (
                SELECT user_id, MAX(id) AS outbound_message_id
                FROM messages
                WHERE direction = 'outbound'
                GROUP BY user_id
            )
            SELECT li.user_id, ('tg:' || li.user_id) AS thread_id, li.inbound_message_id, li.inbound_at, li.inbound_text
            FROM last_inbound li
            LEFT JOIN last_outbound lo ON lo.user_id = li.user_id
            WHERE (lo.outbound_message_id IS NULL OR lo.outbound_message_id < li.inbound_message_id)
              AND julianday(li.inbound_at) <= julianday('now') - (? / 24.0)
            ORDER BY li.inbound_message_id DESC
            LIMIT ?
            """,
            (max(1, int(no_reply_hours)), max(1, int(limit))),
        )
        return [dict(row) for row in cursor.fetchall()]

    def _collect_business_no_reply_candidates(self, conn: Any, *, no_reply_hours: int, limit: int) -> list[Dict[str, Any]]:
        cursor = conn.execute(
            """
            WITH last_inbound AS (
                SELECT
                    bm.thread_key,
                    bm.user_id,
                    bm.id AS inbound_message_id,
                    bm.created_at AS inbound_at,
                    bm.text AS inbound_text
                FROM business_messages bm
                JOIN (
                    SELECT thread_key, MAX(id) AS max_id
                    FROM business_messages
                    WHERE direction = 'inbound' AND is_deleted = 0
                    GROUP BY thread_key
                ) latest ON latest.max_id = bm.id
            ),
            last_outbound AS (
                SELECT thread_key, MAX(id) AS outbound_message_id
                FROM business_messages
                WHERE direction = 'outbound' AND is_deleted = 0
                GROUP BY thread_key
            )
            SELECT
                li.user_id,
                li.thread_key AS thread_id,
                li.inbound_message_id,
                li.inbound_at,
                li.inbound_text
            FROM last_inbound li
            LEFT JOIN last_outbound lo ON lo.thread_key = li.thread_key
            WHERE li.user_id IS NOT NULL
              AND (lo.outbound_message_id IS NULL OR lo.outbound_message_id < li.inbound_message_id)
              AND julianday(li.inbound_at) <= julianday('now') - (? / 24.0)
            ORDER BY li.inbound_message_id DESC
            LIMIT ?
            """,
            (max(1, int(no_reply_hours)), max(1, int(limit))),
        )
        return [dict(row) for row in cursor.fetchall()]

    def _collect_call_no_next_step_candidates(
        self,
        conn: Any,
        *,
        call_no_next_step_hours: int,
        limit: int,
    ) -> list[Dict[str, Any]]:
        cursor = conn.execute(
            """
            WITH call_actions AS (
                SELECT thread_id, MAX(id) AS max_id
                FROM approval_actions
                WHERE action = 'manual_action'
                  AND (
                    LOWER(COALESCE(payload_json, '')) LIKE '%call%'
                    OR LOWER(COALESCE(payload_json, '')) LIKE '%звон%'
                  )
                GROUP BY thread_id
            )
            SELECT aa.id AS action_id, aa.user_id, aa.thread_id, aa.created_at, aa.payload_json
            FROM approval_actions aa
            JOIN call_actions ca ON ca.max_id = aa.id
            WHERE julianday(aa.created_at) <= julianday('now') - (? / 24.0)
            ORDER BY aa.id DESC
            LIMIT ?
            """,
            (max(1, int(call_no_next_step_hours)), max(1, int(limit))),
        )
        rows: list[Dict[str, Any]] = []
        for row in cursor.fetchall():
            item = dict(row)
            raw_payload = item.pop("payload_json", None)
            try:
                payload = json.loads(raw_payload) if isinstance(raw_payload, str) else {}
            except json.JSONDecodeError:
                payload = {}
            item["payload"] = payload if isinstance(payload, dict) else {}
            rows.append(item)
        return rows

    def _collect_stale_warm_candidates(self, conn: Any, *, stale_warm_days: int, limit: int) -> list[Dict[str, Any]]:
        cursor = conn.execute(
            """
            WITH latest_scores AS (
                SELECT thread_id, MAX(id) AS max_id
                FROM lead_scores
                GROUP BY thread_id
            )
            SELECT ls.id AS score_id, ls.user_id, ls.thread_id, ls.score, ls.temperature, ls.created_at
            FROM lead_scores ls
            JOIN latest_scores latest ON latest.max_id = ls.id
            WHERE LOWER(ls.temperature) = 'warm'
              AND julianday(ls.created_at) <= julianday('now') - ?
            ORDER BY ls.id DESC
            LIMIT ?
            """,
            (max(1, int(stale_warm_days)), max(1, int(limit))),
        )
        return [dict(row) for row in cursor.fetchall()]

    def _safe_thread_id(self, value: object) -> str:
        if not isinstance(value, str):
            return ""
        return value.strip()

    def _build_radar_idempotency_key(self, *, rule_key: str, thread_id: str, source_token: str) -> str:
        cleaned_rule = rule_key.replace(":", "_").strip("_")
        cleaned_thread = self._safe_thread_id(thread_id).replace(":", "_").replace("/", "_")
        cleaned_token = (source_token or "").strip().replace(":", "_").replace("/", "_")
        key = f"lr:{cleaned_rule}:{cleaned_thread}:{cleaned_token}"
        return key[:120]

    def _create_radar_artifacts(
        self,
        conn: Any,
        *,
        user_id: int,
        thread_id: str,
        rule_key: str,
        priority: str,
        reason_human: str,
        draft_text: str,
        source_token: str,
        trigger: str,
    ) -> Dict[str, Any]:
        task_id = create_followup_task(
            conn,
            user_id=user_id,
            thread_id=thread_id,
            priority=priority,
            reason=f"{rule_key} [source={source_token}] :: {reason_human}",
            status="pending",
            due_at=None,
            assigned_to="lead_radar:auto",
            related_draft_id=None,
        )
        create_approval_action(
            conn,
            draft_id=None,
            user_id=user_id,
            thread_id=thread_id,
            action="followup_created",
            actor="lead_radar:auto",
            payload={
                "source": "lead_radar",
                "rule": rule_key,
                "trigger": trigger,
                "followup_task_id": task_id,
            },
        )

        draft_id = create_reply_draft(
            conn,
            user_id=user_id,
            thread_id=thread_id,
            source_message_id=None,
            draft_text=draft_text.strip(),
            model_name=self.lead_radar_model_name,
            quality={"source": "lead_radar", "rule": rule_key, "trigger": trigger},
            created_by="lead_radar:auto",
            status="created",
            idempotency_key=self._build_radar_idempotency_key(
                rule_key=rule_key,
                thread_id=thread_id,
                source_token=source_token,
            ),
        )
        create_approval_action(
            conn,
            draft_id=draft_id,
            user_id=user_id,
            thread_id=thread_id,
            action="draft_created",
            actor="lead_radar:auto",
            payload={
                "source": "lead_radar",
                "rule": rule_key,
                "trigger": trigger,
                "followup_task_id": task_id,
            },
        )
        return {"task_id": int(task_id), "draft_id": int(draft_id)}

    def _resolve_radar_limit(self, limit_override: Optional[int]) -> int:
        max_cfg = max(1, int(self.settings.lead_radar_max_items_per_run))
        if limit_override is None:
            return max_cfg
        return max(1, min(int(limit_override), max_cfg))

    async def run_lead_radar_once(
        self,
        *,
        trigger: str,
        dry_run: bool = False,
        limit_override: Optional[int] = None,
    ) -> Dict[str, Any]:
        effective_limit = self._resolve_radar_limit(limit_override)
        if not self.settings.enable_lead_radar:
            return {
                "ok": False,
                "enabled": False,
                "trigger": trigger,
                "dry_run": bool(dry_run),
                "limit": effective_limit,
                "created_followups": 0,
                "created_drafts": 0,
                "scanned": 0,
                "rules": {},
            }

        if self._lead_radar_lock is None:
            self._lead_radar_lock = asyncio.Lock()

        async with self._lead_radar_lock:
            conn = get_connection(self.db_path)
            try:
                no_reply_candidates = self._collect_no_reply_candidates(
                    conn,
                    no_reply_hours=self.settings.lead_radar_no_reply_hours,
                    limit=effective_limit,
                )
                business_no_reply_candidates = self._collect_business_no_reply_candidates(
                    conn,
                    no_reply_hours=self.settings.lead_radar_no_reply_hours,
                    limit=effective_limit,
                )
                call_candidates = self._collect_call_no_next_step_candidates(
                    conn,
                    call_no_next_step_hours=self.settings.lead_radar_call_no_next_step_hours,
                    limit=effective_limit,
                )
                stale_warm_candidates = self._collect_stale_warm_candidates(
                    conn,
                    stale_warm_days=self.settings.lead_radar_stale_warm_days,
                    limit=effective_limit,
                )

                result: Dict[str, Any] = {
                    "ok": True,
                    "enabled": True,
                    "trigger": trigger,
                    "dry_run": bool(dry_run),
                    "limit": effective_limit,
                    "created_followups": 0,
                    "created_drafts": 0,
                    "scanned": len(no_reply_candidates)
                    + len(business_no_reply_candidates)
                    + len(call_candidates)
                    + len(stale_warm_candidates),
                    "rules": {
                        self.lead_radar_rule_no_reply: {
                            "candidates": len(no_reply_candidates) + len(business_no_reply_candidates),
                            "created": 0,
                            "skipped_pending_rule": 0,
                            "skipped_duplicate_trigger": 0,
                            "skipped_outcome": 0,
                            "skipped_cooldown": 0,
                            "skipped_recent_sent": 0,
                            "skipped_daily_cap": 0,
                        },
                        self.lead_radar_rule_call_no_next_step: {
                            "candidates": len(call_candidates),
                            "created": 0,
                            "skipped_pending_rule": 0,
                            "skipped_duplicate_trigger": 0,
                            "skipped_outcome": 0,
                            "skipped_cooldown": 0,
                            "skipped_recent_sent": 0,
                            "skipped_daily_cap": 0,
                        },
                        self.lead_radar_rule_stale_warm: {
                            "candidates": len(stale_warm_candidates),
                            "created": 0,
                            "skipped_pending_rule": 0,
                            "skipped_duplicate_trigger": 0,
                            "skipped_outcome": 0,
                            "skipped_cooldown": 0,
                            "skipped_recent_sent": 0,
                            "skipped_daily_cap": 0,
                        },
                    },
                }

                remaining = effective_limit
                no_reply_all_candidates = [*no_reply_candidates, *business_no_reply_candidates]
                for candidate in no_reply_all_candidates:
                    if remaining <= 0:
                        break
                    user_id = int(candidate.get("user_id") or 0)
                    if user_id <= 0:
                        continue
                    thread_id = self._safe_thread_id(candidate.get("thread_id")) or self.thread_id_from_user_id(user_id)
                    if get_conversation_outcome(conn, thread_id=thread_id) is not None:
                        result["rules"][self.lead_radar_rule_no_reply]["skipped_outcome"] += 1
                        continue
                    allowed, skip_reason = self._lead_radar_guard(
                        conn,
                        thread_id=thread_id,
                        rule_key=self.lead_radar_rule_no_reply,
                        source_token=f"msg:{str(candidate.get('inbound_message_id') or 'na')}",
                    )
                    if not allowed:
                        if skip_reason == "pending_rule":
                            result["rules"][self.lead_radar_rule_no_reply]["skipped_pending_rule"] += 1
                        elif skip_reason == "duplicate_trigger":
                            result["rules"][self.lead_radar_rule_no_reply]["skipped_duplicate_trigger"] += 1
                        elif skip_reason == "cooldown":
                            result["rules"][self.lead_radar_rule_no_reply]["skipped_cooldown"] += 1
                        elif skip_reason == "recent_sent":
                            result["rules"][self.lead_radar_rule_no_reply]["skipped_recent_sent"] += 1
                        elif skip_reason == "daily_cap":
                            result["rules"][self.lead_radar_rule_no_reply]["skipped_daily_cap"] += 1
                        continue
                    inbound_text = str(candidate.get("inbound_text") or "").strip()
                    reason_human = (
                        f"Нет ответа менеджера после входящего сообщения более {self.settings.lead_radar_no_reply_hours} ч."
                    )
                    draft_text = (
                        "Здравствуйте! Спасибо за ожидание. Возвращаюсь к вашему запросу.\n\n"
                        "Хочу уточнить 1-2 детали и сразу предложить конкретный следующий шаг под вашу цель.\n\n"
                        f"Последний запрос клиента: {inbound_text or 'без текста'}"
                    )
                    if dry_run:
                        result["rules"][self.lead_radar_rule_no_reply]["created"] += 1
                        remaining -= 1
                        continue
                    created = self._create_radar_artifacts(
                        conn,
                        user_id=user_id,
                        thread_id=thread_id,
                        rule_key=self.lead_radar_rule_no_reply,
                        priority="hot",
                        reason_human=reason_human,
                        draft_text=draft_text,
                        source_token=f"msg:{str(candidate.get('inbound_message_id') or 'na')}",
                        trigger=trigger,
                    )
                    result["rules"][self.lead_radar_rule_no_reply]["created"] += 1
                    result["created_followups"] += 1
                    result["created_drafts"] += 1 if created.get("draft_id") else 0
                    remaining -= 1

                for candidate in call_candidates:
                    if remaining <= 0:
                        break
                    user_id = int(candidate.get("user_id") or 0)
                    thread_id = self._safe_thread_id(candidate.get("thread_id"))
                    if user_id <= 0 or not thread_id:
                        continue
                    if get_conversation_outcome(conn, thread_id=thread_id) is not None:
                        result["rules"][self.lead_radar_rule_call_no_next_step]["skipped_outcome"] += 1
                        continue
                    allowed, skip_reason = self._lead_radar_guard(
                        conn,
                        thread_id=thread_id,
                        rule_key=self.lead_radar_rule_call_no_next_step,
                        source_token=f"action:{str(candidate.get('action_id') or 'na')}",
                    )
                    if not allowed:
                        if skip_reason == "pending_rule":
                            result["rules"][self.lead_radar_rule_call_no_next_step]["skipped_pending_rule"] += 1
                        elif skip_reason == "duplicate_trigger":
                            result["rules"][self.lead_radar_rule_call_no_next_step]["skipped_duplicate_trigger"] += 1
                        elif skip_reason == "cooldown":
                            result["rules"][self.lead_radar_rule_call_no_next_step]["skipped_cooldown"] += 1
                        elif skip_reason == "recent_sent":
                            result["rules"][self.lead_radar_rule_call_no_next_step]["skipped_recent_sent"] += 1
                        elif skip_reason == "daily_cap":
                            result["rules"][self.lead_radar_rule_call_no_next_step]["skipped_daily_cap"] += 1
                        continue
                    action_time = str(candidate.get("created_at") or "").strip()
                    reason_human = (
                        "После звонка не зафиксирован следующий шаг "
                        f"более {self.settings.lead_radar_call_no_next_step_hours} ч."
                    )
                    draft_text = (
                        "Спасибо за разговор. Подтверждаю, что зафиксировал ваш запрос.\n\n"
                        "Предлагаю согласовать удобное время короткого follow-up, "
                        "чтобы закрыть оставшиеся вопросы и выбрать следующий шаг.\n\n"
                        f"Контекст звонка (время события): {action_time or 'не указано'}"
                    )
                    if dry_run:
                        result["rules"][self.lead_radar_rule_call_no_next_step]["created"] += 1
                        remaining -= 1
                        continue
                    created = self._create_radar_artifacts(
                        conn,
                        user_id=user_id,
                        thread_id=thread_id,
                        rule_key=self.lead_radar_rule_call_no_next_step,
                        priority="hot",
                        reason_human=reason_human,
                        draft_text=draft_text,
                        source_token=f"action:{str(candidate.get('action_id') or 'na')}",
                        trigger=trigger,
                    )
                    result["rules"][self.lead_radar_rule_call_no_next_step]["created"] += 1
                    result["created_followups"] += 1
                    result["created_drafts"] += 1 if created.get("draft_id") else 0
                    remaining -= 1

                for candidate in stale_warm_candidates:
                    if remaining <= 0:
                        break
                    user_id = int(candidate.get("user_id") or 0)
                    thread_id = self._safe_thread_id(candidate.get("thread_id"))
                    if user_id <= 0 or not thread_id:
                        continue
                    if get_conversation_outcome(conn, thread_id=thread_id) is not None:
                        result["rules"][self.lead_radar_rule_stale_warm]["skipped_outcome"] += 1
                        continue
                    allowed, skip_reason = self._lead_radar_guard(
                        conn,
                        thread_id=thread_id,
                        rule_key=self.lead_radar_rule_stale_warm,
                        source_token=f"score:{str(candidate.get('score_id') or 'na')}",
                    )
                    if not allowed:
                        if skip_reason == "pending_rule":
                            result["rules"][self.lead_radar_rule_stale_warm]["skipped_pending_rule"] += 1
                        elif skip_reason == "duplicate_trigger":
                            result["rules"][self.lead_radar_rule_stale_warm]["skipped_duplicate_trigger"] += 1
                        elif skip_reason == "cooldown":
                            result["rules"][self.lead_radar_rule_stale_warm]["skipped_cooldown"] += 1
                        elif skip_reason == "recent_sent":
                            result["rules"][self.lead_radar_rule_stale_warm]["skipped_recent_sent"] += 1
                        elif skip_reason == "daily_cap":
                            result["rules"][self.lead_radar_rule_stale_warm]["skipped_daily_cap"] += 1
                        continue
                    score = float(candidate.get("score") or 0.0)
                    score_created_at = str(candidate.get("created_at") or "").strip()
                    reason_human = (
                        f"Теплый лид без активности более {self.settings.lead_radar_stale_warm_days} дней "
                        "(нужна реактивация)."
                    )
                    draft_text = (
                        "Возвращаюсь к вашему запросу и подготовил обновленный следующий шаг.\n\n"
                        "Если вам удобно, можем быстро сверить текущую цель и выбрать оптимальную программу.\n\n"
                        f"Текущий warm-score: {score:.1f}; дата оценки: {score_created_at or 'не указана'}."
                    )
                    if dry_run:
                        result["rules"][self.lead_radar_rule_stale_warm]["created"] += 1
                        remaining -= 1
                        continue
                    created = self._create_radar_artifacts(
                        conn,
                        user_id=user_id,
                        thread_id=thread_id,
                        rule_key=self.lead_radar_rule_stale_warm,
                        priority="warm",
                        reason_human=reason_human,
                        draft_text=draft_text,
                        source_token=f"score:{str(candidate.get('score_id') or 'na')}",
                        trigger=trigger,
                    )
                    result["rules"][self.lead_radar_rule_stale_warm]["created"] += 1
                    result["created_followups"] += 1
                    result["created_drafts"] += 1 if created.get("draft_id") else 0
                    remaining -= 1
            finally:
                conn.close()
        return result

    async def process_manual_call_upload(
        self,
        *,
        user_id: Optional[int],
        thread_id: Optional[str],
        recording_url: Optional[str],
        transcript_hint: Optional[str],
        audio_file: Optional[UploadFile],
        source_type_override: Optional[str] = None,
        source_ref_override: Optional[str] = None,
        created_by: str = "admin:manual",
        action_source: str = "call_copilot",
        assigned_to: str = "sales:manual",
    ) -> Dict[str, Any]:
        has_file = audio_file is not None and bool((audio_file.filename or "").strip())
        has_url = bool((recording_url or "").strip())
        has_hint = bool((transcript_hint or "").strip())
        if not has_file and not has_url and not has_hint:
            if audio_file is not None:
                await audio_file.close()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Provide audio_file, recording_url or transcript_hint.",
            )

        stored_file_path: Optional[Path] = None
        transcript_text = ""
        source_type = (source_type_override or "").strip() or ("url" if has_url and not has_file else "upload")
        source_ref = (source_ref_override or "").strip() or (recording_url or "").strip() or None
        if has_file and audio_file is not None:
            stored_file_path = await self._store_call_upload_file(audio_file)
            if stored_file_path is None:
                await audio_file.close()
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Uploaded file is empty.",
                )
            transcript_text = extract_transcript_from_file(stored_file_path)
            if not source_ref:
                source_ref = (audio_file.filename or "").strip() or None

        conn = get_connection(self.db_path)
        try:
            actor_name = f"{(action_source or 'call_copilot').strip()}:auto"
            resolved_user_id, resolved_thread_id = self._resolve_call_thread_and_user(
                conn,
                user_id_input=user_id,
                thread_id_input=thread_id,
            )
            call_id = create_call_record(
                conn,
                user_id=resolved_user_id,
                thread_id=resolved_thread_id,
                source_type=source_type,
                source_ref=source_ref,
                file_path=str(stored_file_path) if stored_file_path else None,
                status="queued",
                created_by=created_by,
            )
            update_call_record_status(conn, call_id=call_id, status="processing")

            effective_transcript = transcript_text or build_transcript_fallback(
                source_type=source_type,
                source_ref=source_ref,
                transcript_hint=transcript_hint,
            )
            upsert_call_transcript(
                conn,
                call_id=call_id,
                provider="heuristic",
                transcript_text=effective_transcript,
                language="ru",
                confidence=0.55 if transcript_text else 0.4,
            )

            insights = build_call_insights(effective_transcript)
            upsert_call_summary(
                conn,
                call_id=call_id,
                summary_text=insights.summary_text,
                interests=insights.interests,
                objections=insights.objections,
                next_best_action=insights.next_best_action,
                warmth=insights.warmth,
                confidence=insights.confidence,
                model_name=self.call_copilot_model_name,
            )

            lead_score_id = create_lead_score(
                conn,
                user_id=resolved_user_id,
                thread_id=resolved_thread_id,
                score=insights.score,
                temperature=insights.warmth,
                confidence=insights.confidence,
                factors={
                    "source": action_source,
                    "interests": insights.interests,
                    "objections": insights.objections,
                },
            )
            followup_id = create_followup_task(
                conn,
                user_id=resolved_user_id,
                thread_id=resolved_thread_id,
                priority=self._priority_from_warmth(insights.warmth),
                reason=f"{action_source}: {insights.next_best_action}",
                status="pending",
                due_at=None,
                assigned_to=assigned_to,
                related_draft_id=None,
            )
            create_approval_action(
                conn,
                draft_id=None,
                user_id=resolved_user_id,
                thread_id=resolved_thread_id,
                action="manual_action",
                actor=actor_name,
                payload={
                    "source": action_source,
                    "call_id": call_id,
                    "followup_task_id": followup_id,
                    "lead_score_id": lead_score_id,
                    "next_best_action": insights.next_best_action,
                },
            )
            create_approval_action(
                conn,
                draft_id=None,
                user_id=resolved_user_id,
                thread_id=resolved_thread_id,
                action="followup_created",
                actor=actor_name,
                payload={
                    "source": action_source,
                    "call_id": call_id,
                    "followup_task_id": followup_id,
                },
            )
            create_approval_action(
                conn,
                draft_id=None,
                user_id=resolved_user_id,
                thread_id=resolved_thread_id,
                action="lead_scored",
                actor=actor_name,
                payload={
                    "source": action_source,
                    "call_id": call_id,
                    "lead_score_id": lead_score_id,
                    "temperature": insights.warmth,
                    "score": insights.score,
                },
            )
            update_call_record_status(conn, call_id=call_id, status="done")
            item = get_call_record(conn, call_id=call_id)
        except HTTPException:
            raise
        except Exception as exc:
            if "call_id" in locals():
                update_call_record_status(
                    conn,
                    call_id=int(call_id),
                    status="failed",
                    error_text=str(exc),
                )
            self.logger.exception("Call copilot processing failed")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Call processing failed: {exc}",
            ) from exc
        finally:
            conn.close()
            if audio_file is not None:
                await audio_file.close()

        return {"ok": True, "item": item}

    async def run_call_retry_failed_once(
        self,
        *,
        trigger: str,
        limit_override: Optional[int] = None,
    ) -> Dict[str, Any]:
        if not self.settings.enable_call_copilot:
            return {
                "ok": False,
                "enabled": False,
                "trigger": trigger,
                "processed": 0,
                "retried": 0,
                "failed": 0,
                "skipped_not_failed": 0,
                "skipped_already_materialized": 0,
                "cleanup": self.cleanup_old_call_files(),
            }

        if self._call_retry_lock is None:
            self._call_retry_lock = asyncio.Lock()

        async with self._call_retry_lock:
            effective_limit = max(1, min(int(limit_override or 50), 500))
            conn = get_connection(self.db_path)
            try:
                items = list_call_records_for_retry(conn, limit=effective_limit)
            finally:
                conn.close()

            processed = 0
            retried = 0
            failed = 0
            skipped_not_failed = 0
            skipped_already_materialized = 0

            for item in items:
                call_id = int(item.get("id") or 0)
                if call_id <= 0:
                    continue
                conn_claim = get_connection(self.db_path)
                try:
                    claim_state = claim_failed_call_record_for_retry(conn_claim, call_id=call_id)
                finally:
                    conn_claim.close()
                if claim_state != "claimed":
                    skipped_not_failed += 1
                    continue

                processed += 1
                conn_call = get_connection(self.db_path)
                try:
                    call_item = get_call_record(conn_call, call_id=call_id)
                    if not isinstance(call_item, dict):
                        raise RuntimeError(f"Call {call_id} not found during retry.")
                    if self._call_side_effects_exist(conn_call, call_id=call_id):
                        update_call_record_status(
                            conn_call,
                            call_id=call_id,
                            status="done",
                            error_text=None,
                        )
                        skipped_already_materialized += 1
                        continue

                    user_id = int(call_item.get("user_id") or 0)
                    thread_id = self._safe_thread_id(call_item.get("thread_id"))
                    if user_id <= 0 or not thread_id:
                        raise RuntimeError("call record has invalid user/thread for retry")

                    source_type = str(call_item.get("source_type") or "upload").strip() or "upload"
                    source_ref = str(call_item.get("source_ref") or "").strip() or None
                    transcript_text = str(call_item.get("transcript_text") or "").strip()
                    if not transcript_text:
                        raw_path = str(call_item.get("file_path") or "").strip()
                        if raw_path:
                            file_path = Path(raw_path)
                            if file_path.exists():
                                transcript_text = extract_transcript_from_file(file_path)
                    effective_transcript = transcript_text or build_transcript_fallback(
                        source_type=source_type,
                        source_ref=source_ref,
                        transcript_hint=None,
                    )
                    upsert_call_transcript(
                        conn_call,
                        call_id=call_id,
                        provider="heuristic_retry",
                        transcript_text=effective_transcript,
                        language="ru",
                        confidence=0.55 if transcript_text else 0.4,
                    )
                    insights = build_call_insights(effective_transcript)
                    upsert_call_summary(
                        conn_call,
                        call_id=call_id,
                        summary_text=insights.summary_text,
                        interests=insights.interests,
                        objections=insights.objections,
                        next_best_action=insights.next_best_action,
                        warmth=insights.warmth,
                        confidence=insights.confidence,
                        model_name=self.call_copilot_model_name,
                    )

                    lead_score_id = create_lead_score(
                        conn_call,
                        user_id=user_id,
                        thread_id=thread_id,
                        score=insights.score,
                        temperature=insights.warmth,
                        confidence=insights.confidence,
                        factors={
                            "source": "call_retry",
                            "trigger": trigger,
                            "interests": insights.interests,
                            "objections": insights.objections,
                        },
                    )
                    followup_id = create_followup_task(
                        conn_call,
                        user_id=user_id,
                        thread_id=thread_id,
                        priority=self._priority_from_warmth(insights.warmth),
                        reason=f"call_retry: {insights.next_best_action}",
                        status="pending",
                        due_at=None,
                        assigned_to="call_retry:auto",
                        related_draft_id=None,
                    )
                    create_approval_action(
                        conn_call,
                        draft_id=None,
                        user_id=user_id,
                        thread_id=thread_id,
                        action="manual_action",
                        actor="call_retry:auto",
                        payload={
                            "source": "call_retry",
                            "trigger": trigger,
                            "call_id": call_id,
                            "followup_task_id": followup_id,
                            "lead_score_id": lead_score_id,
                            "next_best_action": insights.next_best_action,
                        },
                    )
                    create_approval_action(
                        conn_call,
                        draft_id=None,
                        user_id=user_id,
                        thread_id=thread_id,
                        action="followup_created",
                        actor="call_retry:auto",
                        payload={
                            "source": "call_retry",
                            "trigger": trigger,
                            "call_id": call_id,
                            "followup_task_id": followup_id,
                        },
                    )
                    create_approval_action(
                        conn_call,
                        draft_id=None,
                        user_id=user_id,
                        thread_id=thread_id,
                        action="lead_scored",
                        actor="call_retry:auto",
                        payload={
                            "source": "call_retry",
                            "trigger": trigger,
                            "call_id": call_id,
                            "lead_score_id": lead_score_id,
                            "temperature": insights.warmth,
                            "score": insights.score,
                        },
                    )
                    update_call_record_status(
                        conn_call,
                        call_id=call_id,
                        status="done",
                        error_text=None,
                    )
                    retried += 1
                except Exception as exc:
                    failed += 1
                    update_call_record_status(
                        conn_call,
                        call_id=call_id,
                        status="failed",
                        error_text=str(exc)[:700],
                    )
                    self.logger.exception("Call retry-failed processing error (call_id=%s)", call_id)
                finally:
                    conn_call.close()

            cleanup_result = self.cleanup_old_call_files()
            return {
                "ok": failed == 0,
                "enabled": True,
                "trigger": trigger,
                "limit": effective_limit,
                "fetched": len(items),
                "processed": processed,
                "retried": retried,
                "failed": failed,
                "skipped_not_failed": skipped_not_failed,
                "skipped_already_materialized": skipped_already_materialized,
                "cleanup": cleanup_result,
            }

    async def ingest_mango_event(
        self,
        *,
        event: MangoCallEvent,
        source: str,
        existing_event_row_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        if self._mango_ingest_lock is None:
            self._mango_ingest_lock = asyncio.Lock()

        async with self._mango_ingest_lock:
            conn = get_connection(self.db_path)
            try:
                if isinstance(existing_event_row_id, int) and existing_event_row_id > 0:
                    event_row_id = int(existing_event_row_id)
                    updated = update_mango_event_status(
                        conn,
                        event_row_id=event_row_id,
                        status="processing",
                        error_text=None,
                    )
                    if not updated:
                        raise ValueError(f"Mango event row {event_row_id} not found.")
                else:
                    state = create_or_get_mango_event(
                        conn,
                        event_id=event.event_id,
                        call_external_id=event.call_id,
                        source=source,
                        payload=event.payload,
                    )
                    event_row_id = int(state.get("id") or 0)
                    if not bool(state.get("is_new")):
                        return {
                            "ok": True,
                            "duplicate": True,
                            "event_row_id": event_row_id,
                            "event_id": event.event_id,
                            "call_external_id": event.call_id,
                        }
                    update_mango_event_status(conn, event_row_id=event_row_id, status="processing")
                user_id, thread_id = extract_mango_user_and_thread(conn, event=event)
            finally:
                conn.close()

            try:
                recording_url = event.recording_url or ""
                transcript_hint = event.transcript_hint or ""
                if not recording_url and not transcript_hint:
                    transcript_hint = (
                        "Звонок импортирован из Mango. Нет ссылки на запись, нужен ручной конспект менеджера."
                    )
                process_result = await self.process_manual_call_upload(
                    user_id=user_id,
                    thread_id=thread_id,
                    recording_url=recording_url,
                    transcript_hint=transcript_hint,
                    audio_file=None,
                    source_type_override="mango",
                    source_ref_override=event.call_id or event.event_id,
                    created_by="mango:auto",
                    action_source="mango_auto_ingest",
                    assigned_to="mango:auto",
                )
                conn_done = get_connection(self.db_path)
                try:
                    update_mango_event_status(conn_done, event_row_id=event_row_id, status="done")
                finally:
                    conn_done.close()
                return {
                    "ok": True,
                    "duplicate": False,
                    "retried": isinstance(existing_event_row_id, int) and existing_event_row_id > 0,
                    "event_row_id": event_row_id,
                    "event_id": event.event_id,
                    "call_external_id": event.call_id,
                    "call": process_result.get("item"),
                }
            except Exception as exc:
                conn_failed = get_connection(self.db_path)
                try:
                    update_mango_event_status(
                        conn_failed,
                        event_row_id=event_row_id,
                        status="failed",
                        error_text=str(exc)[:700],
                    )
                finally:
                    conn_failed.close()
                raise

    async def run_mango_poll_once(self, *, trigger: str, limit_override: Optional[int] = None) -> Dict[str, Any]:
        if self._mango_batch_lock is None:
            self._mango_batch_lock = asyncio.Lock()
        if not self.mango_ingest_enabled():
            return {
                "ok": False,
                "enabled": False,
                "trigger": trigger,
                "processed": 0,
                "created": 0,
                "duplicates": 0,
                "failed": 0,
                "attempts": 0,
                "cleanup": self.cleanup_old_call_files(),
            }

        async with self._mango_batch_lock:
            try:
                client = self._build_mango_client()
            except MangoClientError as exc:
                return {
                    "ok": False,
                    "enabled": True,
                    "trigger": trigger,
                    "error": str(exc),
                    "processed": 0,
                    "created": 0,
                    "duplicates": 0,
                    "failed": 0,
                    "attempts": 0,
                    "cleanup": self.cleanup_old_call_files(),
                }

            conn = get_connection(self.db_path)
            try:
                since = get_latest_mango_event_created_at(conn)
            finally:
                conn.close()

            effective_limit = max(1, min(int(limit_override or self.settings.mango_poll_limit_per_run), 500))
            try:
                events, attempts_used = await fetch_mango_poll_events_with_retries(
                    client=client,
                    since=since,
                    limit=effective_limit,
                    attempts=self.settings.mango_poll_retry_attempts,
                    base_backoff_seconds=self.settings.mango_poll_retry_backoff_seconds,
                )
            except MangoClientError as exc:
                return {
                    "ok": False,
                    "enabled": True,
                    "trigger": trigger,
                    "error": str(exc),
                    "processed": 0,
                    "created": 0,
                    "duplicates": 0,
                    "failed": 0,
                    "attempts": max(1, int(self.settings.mango_poll_retry_attempts)),
                    "cleanup": self.cleanup_old_call_files(),
                }

            processed = 0
            created = 0
            duplicates = 0
            failed = 0
            for event in events:
                processed += 1
                try:
                    result = await self.ingest_mango_event(event=event, source=f"poll:{trigger}")
                    if result.get("duplicate"):
                        duplicates += 1
                    else:
                        created += 1
                except Exception:
                    failed += 1
                    self.logger.exception("Mango poll event processing failed (event_id=%s)", event.event_id)

            cleanup_result = self.cleanup_old_call_files()
            return {
                "ok": failed == 0,
                "enabled": True,
                "trigger": trigger,
                "since": since,
                "limit": effective_limit,
                "attempts": attempts_used,
                "fetched": len(events),
                "processed": processed,
                "created": created,
                "duplicates": duplicates,
                "failed": failed,
                "cleanup": cleanup_result,
            }

    async def run_mango_retry_failed_once(
        self,
        *,
        trigger: str,
        limit_override: Optional[int] = None,
    ) -> Dict[str, Any]:
        if self._mango_batch_lock is None:
            self._mango_batch_lock = asyncio.Lock()
        if not self.mango_ingest_enabled():
            return {
                "ok": False,
                "enabled": False,
                "trigger": trigger,
                "processed": 0,
                "retried": 0,
                "failed": 0,
                "duplicates": 0,
                "cleanup": self.cleanup_old_call_files(),
            }

        async with self._mango_batch_lock:
            effective_limit = max(1, min(int(limit_override or self.settings.mango_retry_failed_limit_per_run), 500))
            conn = get_connection(self.db_path)
            try:
                items = list_mango_events(conn, status="failed", limit=effective_limit, newest_first=False)
            finally:
                conn.close()

            processed = 0
            retried = 0
            failed = 0
            duplicates = 0
            skipped_not_failed = 0
            for item in items:
                event_row_id = int(item.get("id") or 0)
                conn_claim = get_connection(self.db_path)
                try:
                    claim_state = claim_failed_mango_event_for_retry(conn_claim, event_row_id=event_row_id)
                finally:
                    conn_claim.close()
                if claim_state != "claimed":
                    skipped_not_failed += 1
                    continue

                processed += 1
                event = event_from_mango_record(item)
                if event is None:
                    failed += 1
                    conn_failed = get_connection(self.db_path)
                    try:
                        update_mango_event_status(
                            conn_failed,
                            event_row_id=event_row_id,
                            status="failed",
                            error_text="retry_parse_failed",
                        )
                    finally:
                        conn_failed.close()
                    continue
                try:
                    result = await self.ingest_mango_event(
                        event=event,
                        source=f"retry:{trigger}",
                        existing_event_row_id=event_row_id,
                    )
                    if result.get("duplicate"):
                        duplicates += 1
                    else:
                        retried += 1
                except Exception:
                    failed += 1
                    self.logger.exception("Mango retry-failed event processing failed (event_id=%s)", event.event_id)

            cleanup_result = self.cleanup_old_call_files()
            return {
                "ok": failed == 0,
                "enabled": True,
                "trigger": trigger,
                "limit": effective_limit,
                "fetched": len(items),
                "processed": processed,
                "retried": retried,
                "duplicates": duplicates,
                "failed": failed,
                "skipped_not_failed": skipped_not_failed,
                "cleanup": cleanup_result,
            }
