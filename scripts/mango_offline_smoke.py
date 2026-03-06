#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import sys
import tempfile
import asyncio
from pathlib import Path
from typing import Any
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import httpx

from sales_agent.sales_api.main import create_app
from sales_agent.sales_core import db
from sales_agent.sales_core.config import Settings
from sales_agent.sales_core.mango_client import MangoCallEvent


class _CompatAsyncAsgiClient:
    def __init__(self, app: Any, *, base_url: str = "http://testserver", follow_redirects: bool = True) -> None:
        self._app = app
        self._base_url = base_url
        self._follow_redirects = follow_redirects

    async def _request_async(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        async def _call_once() -> httpx.Response:
            transport = httpx.ASGITransport(app=self._app)
            async with httpx.AsyncClient(
                transport=transport,
                base_url=self._base_url,
                follow_redirects=self._follow_redirects,
            ) as client:
                response = await client.request(method, url, **kwargs)
                await asyncio.sleep(0)
                return response

        router = getattr(self._app, "router", None)
        lifespan_context = getattr(router, "lifespan_context", None)
        if callable(lifespan_context):
            async with lifespan_context(self._app):
                return await _call_once()
        return await _call_once()

    def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        return asyncio.run(self._request_async(method, url, **kwargs))

    def get(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("POST", url, **kwargs)


def _build_client(app: Any) -> Any:
    from fastapi.testclient import TestClient

    try:
        return TestClient(app)
    except TypeError:
        return _CompatAsyncAsgiClient(app)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline smoke for Mango auto-ingest (no external network).")
    parser.add_argument(
        "--fixture",
        default=str(PROJECT_ROOT / "scripts" / "fixtures" / "mango_call_recording_ready.json"),
        help="Path to webhook fixture JSON.",
    )
    parser.add_argument(
        "--webhook-secret",
        default="mango-secret",
        help="Webhook secret for signature validation.",
    )
    parser.add_argument(
        "--db-path",
        default="",
        help="Optional sqlite DB path. If empty, temporary DB is used.",
    )
    return parser


def _sign(secret: str, body: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


def _load_payload(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("Fixture payload must be a JSON object.")
    return payload


def _settings(db_path: Path, *, webhook_secret: str) -> Settings:
    return Settings(
        telegram_bot_token="",
        openai_api_key="sk-test",
        openai_model="gpt-4.1",
        tallanto_api_url="",
        tallanto_api_key="",
        brand_default="kmipt",
        database_path=db_path,
        catalog_path=PROJECT_ROOT / "catalog" / "products.yaml",
        knowledge_path=PROJECT_ROOT / "knowledge",
        vector_store_meta_path=db_path.parent / "vector_store.json",
        openai_vector_store_id="",
        admin_user="admin",
        admin_pass="secret",
        enable_call_copilot=True,
        enable_mango_auto_ingest=True,
        mango_api_base_url="https://mango.example/api",
        mango_api_token="mango-token",
        mango_webhook_secret=webhook_secret,
        mango_polling_enabled=False,
        mango_poll_limit_per_run=10,
        mango_poll_retry_attempts=2,
        mango_poll_retry_backoff_seconds=0,
        mango_retry_failed_limit_per_run=5,
    )


def _ensure_fixture_event(payload: dict[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    data = result.get("data")
    if not isinstance(data, dict):
        data = {}
        result["data"] = data

    if not str(result.get("event") or "").strip():
        result["event"] = "call_recording_ready"
    if not str(result.get("event_id") or "").strip():
        result["event_id"] = "fixture-event-1"
    if not str(data.get("call_id") or "").strip():
        data["call_id"] = "fixture-call-1"
    if not str(data.get("phone") or "").strip():
        data["phone"] = "+79990000011"
    if not str(data.get("recording_url") or "").strip():
        data["recording_url"] = "https://cdn.example/fixture-recording.mp3"
    return result


def run_offline_smoke(*, fixture_path: Path, webhook_secret: str, db_path: Path) -> tuple[bool, list[str]]:
    checks: list[tuple[str, bool, str]] = []
    payload = _ensure_fixture_event(_load_payload(fixture_path))

    settings = _settings(db_path, webhook_secret=webhook_secret)
    app = create_app(settings)
    client = _build_client(app)

    conn = db.get_connection(db_path)
    try:
        user_id = db.get_or_create_user(conn, channel="telegram", external_id="offline-smoke-user")
        db.create_lead_record(
            conn=conn,
            user_id=user_id,
            status="created",
            contact={"phone": "+79990000011"},
            tallanto_entry_id=None,
        )
    finally:
        conn.close()

    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "X-Mango-Signature": _sign(webhook_secret, raw),
    }
    webhook = client.post(settings.mango_webhook_path, content=raw, headers=headers)
    webhook_body = webhook.json() if webhook.status_code == 200 else {}
    checks.append(
        (
            "webhook_ingest",
            webhook.status_code == 200 and bool(webhook_body.get("ok")),
            f"http={webhook.status_code}",
        )
    )

    duplicate = client.post(settings.mango_webhook_path, content=raw, headers=headers)
    duplicate_body = duplicate.json() if duplicate.status_code == 200 else {}
    checks.append(
        (
            "webhook_deduplicate",
            duplicate.status_code == 200 and bool(duplicate_body.get("result", {}).get("duplicate")),
            f"http={duplicate.status_code}",
        )
    )

    poll_events = [
        MangoCallEvent(
            event_id="offline-poll-1",
            call_id="offline-call-1",
            phone="+79990000011",
            recording_url="https://cdn.example/offline-poll-1.mp3",
            transcript_hint="",
            occurred_at="",
            payload={"event": "call_recording_ready"},
        )
    ]
    with patch("sales_agent.sales_api.main.MangoClient.list_recent_calls", return_value=poll_events):
        poll = client.post("/admin/calls/mango/poll?limit=1", auth=(settings.admin_user, settings.admin_pass))
    poll_body = poll.json() if poll.status_code == 200 else {}
    checks.append(
        (
            "manual_poll",
            poll.status_code == 200 and int(poll_body.get("created") or 0) == 1,
            f"http={poll.status_code}",
        )
    )

    conn_retry = db.get_connection(db_path)
    try:
        state = db.create_or_get_mango_event(
            conn_retry,
            event_id="offline-failed-1",
            call_external_id="offline-failed-call-1",
            source="webhook",
            payload={
                "event": "call_recording_ready",
                "event_id": "offline-failed-1",
                "data": {
                    "call_id": "offline-failed-call-1",
                    "phone": "+79990000011",
                    "recording_url": "https://cdn.example/offline-failed.mp3",
                },
            },
        )
        db.update_mango_event_status(
            conn_retry,
            event_row_id=int(state["id"]),
            status="failed",
            error_text="simulated failure",
        )
    finally:
        conn_retry.close()

    retry_failed = client.post("/admin/calls/mango/retry-failed?limit=1", auth=(settings.admin_user, settings.admin_pass))
    retry_body = retry_failed.json() if retry_failed.status_code == 200 else {}
    checks.append(
        (
            "retry_failed_events",
            retry_failed.status_code == 200 and int(retry_body.get("retried") or 0) >= 1,
            f"http={retry_failed.status_code}",
        )
    )

    diagnostics = client.get("/api/runtime/diagnostics")
    diagnostics_body = diagnostics.json() if diagnostics.status_code == 200 else {}
    runtime = diagnostics_body.get("runtime", {}) if isinstance(diagnostics_body, dict) else {}
    mango_runtime = runtime.get("mango", {}) if isinstance(runtime, dict) else {}
    checks.append(
        (
            "runtime_mango_metrics",
            diagnostics.status_code == 200
            and isinstance(mango_runtime, dict)
            and "events_failed" in mango_runtime
            and "events_total" in mango_runtime,
            f"http={diagnostics.status_code}",
        )
    )

    lines: list[str] = []
    failed = 0
    for name, ok, details in checks:
        lines.append(f"[{'OK' if ok else 'FAIL'}] {name}: {details}")
        if not ok:
            failed += 1
    lines.append("Offline Mango smoke: " + ("OK" if failed == 0 else f"FAIL ({failed} checks failed)"))
    return failed == 0, lines


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    fixture_path = Path(args.fixture)
    if not fixture_path.exists():
        print(f"[FAIL] fixture_missing: {fixture_path}")
        print("Offline Mango smoke: FAIL (1 checks failed)")
        return 1

    if args.db_path.strip():
        db_path = Path(args.db_path).resolve()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        ok, lines = run_offline_smoke(
            fixture_path=fixture_path,
            webhook_secret=args.webhook_secret,
            db_path=db_path,
        )
        for line in lines:
            print(line)
        return 0 if ok else 1

    with tempfile.TemporaryDirectory(prefix="mango_offline_smoke_") as tmpdir:
        db_path = Path(tmpdir) / "mango_offline_smoke.db"
        ok, lines = run_offline_smoke(
            fixture_path=fixture_path,
            webhook_secret=args.webhook_secret,
            db_path=db_path,
        )
    for line in lines:
        print(line)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
