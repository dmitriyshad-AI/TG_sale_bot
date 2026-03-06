import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

SQLITE_BUSY_TIMEOUT_MS = 5000
SQLITE_CONNECT_TIMEOUT_SECONDS = SQLITE_BUSY_TIMEOUT_MS / 1000


CREATE_TABLE_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel TEXT NOT NULL,
        external_id TEXT NOT NULL,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(channel, external_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        state_json TEXT DEFAULT '{}',
        meta_json TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id),
        UNIQUE(user_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        direction TEXT NOT NULL,
        text TEXT,
        meta_json TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS leads (
        lead_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        status TEXT NOT NULL,
        tallanto_entry_id TEXT,
        contact_json TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS conversation_contexts (
        user_id INTEGER PRIMARY KEY,
        summary_json TEXT NOT NULL DEFAULT '{}',
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS crm_cache (
        key TEXT PRIMARY KEY,
        value_json TEXT NOT NULL,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS webhook_updates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        update_id INTEGER,
        payload_json TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        attempts INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        next_attempt_at TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(update_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS conversation_outcomes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        thread_id TEXT NOT NULL,
        outcome TEXT NOT NULL,
        note TEXT,
        created_by TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id),
        UNIQUE(thread_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS reply_drafts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        thread_id TEXT NOT NULL,
        source_message_id INTEGER,
        draft_text TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'created',
        model_name TEXT,
        quality_json TEXT,
        created_by TEXT,
        approved_by TEXT,
        sent_by TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        approved_at TEXT,
        sent_at TEXT,
        sent_message_id TEXT,
        last_error TEXT,
        idempotency_key TEXT,
        FOREIGN KEY(user_id) REFERENCES users(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS approval_actions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        draft_id INTEGER,
        user_id INTEGER NOT NULL,
        thread_id TEXT NOT NULL,
        action TEXT NOT NULL,
        actor TEXT,
        payload_json TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(draft_id) REFERENCES reply_drafts(id),
        FOREIGN KEY(user_id) REFERENCES users(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS followup_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        thread_id TEXT NOT NULL,
        priority TEXT NOT NULL DEFAULT 'warm',
        reason TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        due_at TEXT,
        assigned_to TEXT,
        related_draft_id INTEGER,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id),
        FOREIGN KEY(related_draft_id) REFERENCES reply_drafts(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS lead_scores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        thread_id TEXT NOT NULL,
        score REAL NOT NULL,
        temperature TEXT NOT NULL,
        confidence REAL,
        factors_json TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id)
    );
    """,
]

CREATE_INDEX_STATEMENTS = [
    "CREATE INDEX IF NOT EXISTS idx_messages_user_created ON messages(user_id, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_leads_user_created ON leads(user_id, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_users_channel_external ON users(channel, external_id);",
    "CREATE INDEX IF NOT EXISTS idx_sessions_updated_at ON sessions(updated_at);",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_user_unique ON sessions(user_id);",
    "CREATE INDEX IF NOT EXISTS idx_conversation_contexts_updated_at ON conversation_contexts(updated_at);",
    "CREATE INDEX IF NOT EXISTS idx_crm_cache_updated_at ON crm_cache(updated_at);",
    "CREATE INDEX IF NOT EXISTS idx_webhook_updates_status_next_attempt ON webhook_updates(status, next_attempt_at, id);",
    "CREATE INDEX IF NOT EXISTS idx_webhook_updates_update_id ON webhook_updates(update_id);",
    "CREATE INDEX IF NOT EXISTS idx_conversation_outcomes_user_created ON conversation_outcomes(user_id, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_conversation_outcomes_thread ON conversation_outcomes(thread_id);",
    "CREATE INDEX IF NOT EXISTS idx_reply_drafts_user_created ON reply_drafts(user_id, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_reply_drafts_thread_created ON reply_drafts(thread_id, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_reply_drafts_status_created ON reply_drafts(status, created_at);",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_reply_drafts_idempotency ON reply_drafts(idempotency_key) WHERE idempotency_key IS NOT NULL AND idempotency_key <> '';",
    "CREATE INDEX IF NOT EXISTS idx_approval_actions_draft_created ON approval_actions(draft_id, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_approval_actions_thread_created ON approval_actions(thread_id, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_approval_actions_action_created ON approval_actions(action, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_followup_tasks_user_created ON followup_tasks(user_id, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_followup_tasks_thread_created ON followup_tasks(thread_id, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_followup_tasks_status_due ON followup_tasks(status, due_at, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_followup_tasks_priority_status ON followup_tasks(priority, status, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_lead_scores_thread_created ON lead_scores(thread_id, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_lead_scores_user_created ON lead_scores(user_id, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_lead_scores_temperature_created ON lead_scores(temperature, created_at);",
]


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS};")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")


def _migrate_sessions_uniqueness(conn: sqlite3.Connection) -> None:
    # Keep the latest row per user if legacy DB contains duplicates.
    conn.execute(
        """
        DELETE FROM sessions
        WHERE id NOT IN (
            SELECT MAX(id) FROM sessions GROUP BY user_id
        )
        """
    )


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        _apply_pragmas(conn)
        for stmt in CREATE_TABLE_STATEMENTS:
            conn.execute(stmt)
        _migrate_sessions_uniqueness(conn)
        for stmt in CREATE_INDEX_STATEMENTS:
            conn.execute(stmt)
        conn.commit()


def get_connection(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(
        db_path,
        check_same_thread=False,
        timeout=SQLITE_CONNECT_TIMEOUT_SECONDS,
    )
    _apply_pragmas(conn)
    conn.row_factory = sqlite3.Row
    return conn


def get_or_create_user(
    conn: sqlite3.Connection,
    channel: str,
    external_id: str,
    username: Optional[str] = None,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
) -> int:
    cursor = conn.execute(
        "SELECT id FROM users WHERE channel = ? AND external_id = ?",
        (channel, external_id),
    )
    row = cursor.fetchone()
    if row:
        return int(row["id"])
    cursor = conn.execute(
        """
        INSERT INTO users (channel, external_id, username, first_name, last_name)
        VALUES (?, ?, ?, ?, ?)
        """,
        (channel, external_id, username, first_name, last_name),
    )
    conn.commit()
    return int(cursor.lastrowid)


def log_message(
    conn: sqlite3.Connection,
    user_id: int,
    direction: str,
    text: str,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    meta_json = json.dumps(meta or {}, ensure_ascii=False)
    conn.execute(
        """
        INSERT INTO messages (user_id, direction, text, meta_json)
        VALUES (?, ?, ?, ?)
        """,
        (user_id, direction, text, meta_json),
    )
    conn.commit()


def upsert_session_state(
    conn: sqlite3.Connection,
    user_id: int,
    state: Dict[str, Any],
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    state_json = json.dumps(state or {}, ensure_ascii=False)
    meta_json = json.dumps(meta or {}, ensure_ascii=False) if meta else None
    if meta_json is None:
        conn.execute(
            """
            INSERT INTO sessions (user_id, state_json, meta_json)
            VALUES (?, ?, NULL)
            ON CONFLICT(user_id) DO UPDATE SET
                state_json = excluded.state_json,
                updated_at = CURRENT_TIMESTAMP
            """,
            (user_id, state_json),
        )
    else:
        conn.execute(
            """
            INSERT INTO sessions (user_id, state_json, meta_json)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                state_json = excluded.state_json,
                meta_json = excluded.meta_json,
                updated_at = CURRENT_TIMESTAMP
            """,
            (user_id, state_json, meta_json),
        )
    conn.commit()


def get_session(conn: sqlite3.Connection, user_id: int) -> Dict[str, Any]:
    cursor = conn.execute(
        "SELECT state_json, meta_json FROM sessions WHERE user_id = ?",
        (user_id,),
    )
    row = cursor.fetchone()
    if not row:
        return {"state": {}, "meta": {}}

    state = json.loads(row["state_json"] or "{}")
    meta = json.loads(row["meta_json"]) if row["meta_json"] else {}
    return {"state": state, "meta": meta}


def create_lead_record(
    conn: sqlite3.Connection,
    user_id: int,
    status: str,
    contact: Dict[str, Any],
    tallanto_entry_id: Optional[str] = None,
) -> int:
    contact_json = json.dumps(contact or {}, ensure_ascii=False)
    cursor = conn.execute(
        """
        INSERT INTO leads (user_id, status, tallanto_entry_id, contact_json)
        VALUES (?, ?, ?, ?)
        """,
        (user_id, status, tallanto_entry_id, contact_json),
    )
    conn.commit()
    return int(cursor.lastrowid)


def list_recent_leads(conn: sqlite3.Connection, limit: int = 100) -> list[Dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT
            l.lead_id,
            l.user_id,
            l.status,
            l.tallanto_entry_id,
            l.contact_json,
            l.created_at,
            u.channel,
            u.external_id,
            u.username,
            u.first_name,
            u.last_name
        FROM leads l
        JOIN users u ON u.id = l.user_id
        ORDER BY l.created_at DESC, l.lead_id DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = []
    for row in cursor.fetchall():
        item = dict(row)
        item["contact"] = json.loads(item.pop("contact_json") or "{}")
        rows.append(item)
    return rows


def list_recent_conversations(conn: sqlite3.Connection, limit: int = 100) -> list[Dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT
            u.id AS user_id,
            u.channel,
            u.external_id,
            u.username,
            u.first_name,
            u.last_name,
            COUNT(m.id) AS messages_count,
            MAX(m.created_at) AS last_message_at
        FROM users u
        LEFT JOIN messages m ON m.user_id = u.id
        GROUP BY u.id, u.channel, u.external_id, u.username, u.first_name, u.last_name
        ORDER BY (MAX(m.created_at) IS NULL), MAX(m.created_at) DESC, u.id DESC
        LIMIT ?
        """,
        (limit,),
    )
    return [dict(row) for row in cursor.fetchall()]


def list_conversation_messages(
    conn: sqlite3.Connection,
    user_id: int,
    limit: int = 500,
) -> list[Dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT id, direction, text, meta_json, created_at
        FROM messages
        WHERE user_id = ?
        ORDER BY id ASC
        LIMIT ?
        """,
        (user_id, limit),
    )
    messages = []
    for row in cursor.fetchall():
        item = dict(row)
        item["meta"] = json.loads(item.pop("meta_json") or "{}")
        messages.append(item)
    return messages


def list_recent_messages(
    conn: sqlite3.Connection,
    user_id: int,
    limit: int = 8,
) -> list[Dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT id, direction, text, meta_json, created_at
        FROM messages
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (user_id, limit),
    )
    rows: list[Dict[str, Any]] = []
    for row in cursor.fetchall():
        item = dict(row)
        item["meta"] = json.loads(item.pop("meta_json") or "{}")
        rows.append(item)
    rows.reverse()
    return rows


def get_conversation_context(conn: sqlite3.Connection, user_id: int) -> Dict[str, Any]:
    cursor = conn.execute(
        "SELECT summary_json FROM conversation_contexts WHERE user_id = ?",
        (user_id,),
    )
    row = cursor.fetchone()
    if not row:
        return {}

    try:
        payload = json.loads(row["summary_json"] or "{}")
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def upsert_conversation_context(
    conn: sqlite3.Connection,
    user_id: int,
    summary: Dict[str, Any],
) -> None:
    summary_json = json.dumps(summary or {}, ensure_ascii=False)
    conn.execute(
        """
        INSERT INTO conversation_contexts (user_id, summary_json)
        VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            summary_json = excluded.summary_json,
            updated_at = CURRENT_TIMESTAMP
        """,
        (user_id, summary_json),
    )
    conn.commit()


def get_crm_cache(
    conn: sqlite3.Connection,
    key: str,
    max_age_seconds: int,
) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT value_json
        FROM crm_cache
        WHERE key = ?
          AND updated_at >= datetime('now', ?)
        LIMIT 1
        """,
        (key, f"-{max(1, int(max_age_seconds))} seconds"),
    ).fetchone()
    if not row:
        return None
    try:
        payload = json.loads(row["value_json"] or "{}")
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def upsert_crm_cache(
    conn: sqlite3.Connection,
    key: str,
    value: Dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO crm_cache (key, value_json, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(key) DO UPDATE SET
            value_json = excluded.value_json,
            updated_at = CURRENT_TIMESTAMP
        """,
        (key, json.dumps(value or {}, ensure_ascii=False)),
    )
    conn.commit()


def enqueue_webhook_update(
    conn: sqlite3.Connection,
    payload: Dict[str, Any],
    update_id: Optional[int] = None,
) -> Dict[str, Any]:
    payload_json = json.dumps(payload or {}, ensure_ascii=False)
    try:
        cursor = conn.execute(
            """
            INSERT INTO webhook_updates (update_id, payload_json, status, next_attempt_at)
            VALUES (?, ?, 'pending', CURRENT_TIMESTAMP)
            """,
            (update_id, payload_json),
        )
        conn.commit()
        return {"id": int(cursor.lastrowid), "is_new": True}
    except sqlite3.IntegrityError:
        if update_id is None:
            raise
        row = conn.execute(
            "SELECT id FROM webhook_updates WHERE update_id = ?",
            (update_id,),
        ).fetchone()
        return {"id": int(row["id"]) if row else 0, "is_new": False}


def claim_webhook_update(conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT id, update_id, payload_json, attempts
            FROM webhook_updates
            WHERE status IN ('pending', 'retry')
              AND COALESCE(next_attempt_at, CURRENT_TIMESTAMP) <= CURRENT_TIMESTAMP
            ORDER BY id ASC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            conn.execute("COMMIT")
            return None

        conn.execute(
            """
            UPDATE webhook_updates
            SET status = 'processing',
                attempts = attempts + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (int(row["id"]),),
        )
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass
        raise

    payload: Dict[str, Any]
    try:
        payload = json.loads(row["payload_json"] or "{}")
    except json.JSONDecodeError:
        payload = {}

    return {
        "id": int(row["id"]),
        "update_id": int(row["update_id"]) if row["update_id"] is not None else None,
        "payload": payload,
        "attempts": int(row["attempts"]) + 1,
    }


def mark_webhook_update_done(conn: sqlite3.Connection, queue_id: int) -> None:
    conn.execute(
        """
        UPDATE webhook_updates
        SET status = 'done',
            last_error = NULL,
            next_attempt_at = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (queue_id,),
    )
    conn.commit()


def mark_webhook_update_retry(
    conn: sqlite3.Connection,
    queue_id: int,
    error: str,
    *,
    retry_delay_seconds: int,
    max_attempts: int,
) -> str:
    row = conn.execute(
        "SELECT attempts FROM webhook_updates WHERE id = ?",
        (queue_id,),
    ).fetchone()
    attempts = int(row["attempts"]) if row else 0
    normalized_error = (error or "unknown_error").strip()[:500]

    if attempts >= max_attempts:
        conn.execute(
            """
            UPDATE webhook_updates
            SET status = 'failed',
                last_error = ?,
                next_attempt_at = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (normalized_error, queue_id),
        )
        conn.commit()
        return "failed"

    delay = max(1, int(retry_delay_seconds))
    conn.execute(
        """
        UPDATE webhook_updates
        SET status = 'retry',
            last_error = ?,
            next_attempt_at = datetime('now', ?),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (normalized_error, f"+{delay} seconds", queue_id),
    )
    conn.commit()
    return "retry"


def requeue_stuck_webhook_updates(conn: sqlite3.Connection, stale_after_seconds: int = 120) -> int:
    stale_seconds = max(1, int(stale_after_seconds))
    cursor = conn.execute(
        """
        UPDATE webhook_updates
        SET status = 'retry',
            next_attempt_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE status = 'processing'
          AND updated_at < datetime('now', ?)
        """,
        (f"-{stale_seconds} seconds",),
    )
    conn.commit()
    return int(cursor.rowcount)


def count_webhook_updates_by_status(conn: sqlite3.Connection, status: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM webhook_updates WHERE status = ?",
        (status,),
    ).fetchone()
    return int(row["cnt"]) if row else 0


def _safe_json_loads(raw_value: Optional[str]) -> Dict[str, Any]:
    if not raw_value:
        return {}
    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def create_reply_draft(
    conn: sqlite3.Connection,
    *,
    user_id: int,
    thread_id: str,
    draft_text: str,
    source_message_id: Optional[int] = None,
    model_name: Optional[str] = None,
    quality: Optional[Dict[str, Any]] = None,
    created_by: Optional[str] = None,
    status: str = "created",
    idempotency_key: Optional[str] = None,
) -> int:
    quality_json = json.dumps(quality or {}, ensure_ascii=False)
    normalized_key = (idempotency_key or "").strip() or None
    try:
        cursor = conn.execute(
            """
            INSERT INTO reply_drafts (
                user_id, thread_id, source_message_id, draft_text, status,
                model_name, quality_json, created_by, idempotency_key
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                thread_id,
                source_message_id,
                draft_text,
                status,
                model_name,
                quality_json,
                created_by,
                normalized_key,
            ),
        )
    except sqlite3.IntegrityError:
        if not normalized_key:
            raise
        row = conn.execute(
            "SELECT id FROM reply_drafts WHERE idempotency_key = ? LIMIT 1",
            (normalized_key,),
        ).fetchone()
        if not row:
            raise
        return int(row["id"])
    conn.commit()
    return int(cursor.lastrowid)


def get_reply_draft(conn: sqlite3.Connection, draft_id: int) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT
            id,
            user_id,
            thread_id,
            source_message_id,
            draft_text,
            status,
            model_name,
            quality_json,
            created_by,
            approved_by,
            sent_by,
            created_at,
            updated_at,
            approved_at,
            sent_at,
            sent_message_id,
            last_error,
            idempotency_key
        FROM reply_drafts
        WHERE id = ?
        LIMIT 1
        """,
        (draft_id,),
    ).fetchone()
    if not row:
        return None
    item = dict(row)
    item["quality"] = _safe_json_loads(item.pop("quality_json"))
    return item


def list_reply_drafts_for_thread(
    conn: sqlite3.Connection,
    *,
    thread_id: str,
    limit: int = 20,
) -> list[Dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT
            id,
            user_id,
            thread_id,
            source_message_id,
            draft_text,
            status,
            model_name,
            quality_json,
            created_by,
            approved_by,
            sent_by,
            created_at,
            updated_at,
            approved_at,
            sent_at,
            sent_message_id,
            last_error,
            idempotency_key
        FROM reply_drafts
        WHERE thread_id = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (thread_id, max(1, int(limit))),
    )
    rows: list[Dict[str, Any]] = []
    for row in cursor.fetchall():
        item = dict(row)
        item["quality"] = _safe_json_loads(item.pop("quality_json"))
        rows.append(item)
    return rows


def update_reply_draft_status(
    conn: sqlite3.Connection,
    *,
    draft_id: int,
    status: str,
    actor: Optional[str] = None,
    sent_message_id: Optional[str] = None,
    last_error: Optional[str] = None,
) -> bool:
    normalized_status = (status or "").strip().lower()
    if normalized_status not in {"created", "approved", "sent", "rejected"}:
        raise ValueError(f"Unsupported draft status: {status}")

    row = conn.execute(
        "SELECT id FROM reply_drafts WHERE id = ? LIMIT 1",
        (draft_id,),
    ).fetchone()
    if not row:
        return False

    approved_by: Optional[str] = None
    sent_by: Optional[str] = None
    approved_at_sql = "NULL"
    sent_at_sql = "NULL"
    if normalized_status == "approved":
        approved_by = actor
        approved_at_sql = "CURRENT_TIMESTAMP"
        sent_at_sql = "sent_at"
    elif normalized_status == "sent":
        sent_by = actor
        sent_at_sql = "CURRENT_TIMESTAMP"
        approved_at_sql = "approved_at"
    else:
        approved_at_sql = "approved_at"
        sent_at_sql = "sent_at"

    conn.execute(
        f"""
        UPDATE reply_drafts
        SET
            status = ?,
            approved_by = COALESCE(?, approved_by),
            sent_by = COALESCE(?, sent_by),
            approved_at = {approved_at_sql},
            sent_at = {sent_at_sql},
            sent_message_id = COALESCE(?, sent_message_id),
            last_error = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (
            normalized_status,
            approved_by,
            sent_by,
            sent_message_id,
            (last_error or "").strip() or None,
            draft_id,
        ),
    )
    conn.commit()
    return True


def update_reply_draft_text(
    conn: sqlite3.Connection,
    *,
    draft_id: int,
    draft_text: str,
    model_name: Optional[str] = None,
    quality: Optional[Dict[str, Any]] = None,
    actor: Optional[str] = None,
) -> bool:
    row = conn.execute(
        "SELECT id FROM reply_drafts WHERE id = ? LIMIT 1",
        (draft_id,),
    ).fetchone()
    if not row:
        return False
    quality_json = json.dumps(quality or {}, ensure_ascii=False)
    conn.execute(
        """
        UPDATE reply_drafts
        SET
            draft_text = ?,
            model_name = COALESCE(?, model_name),
            quality_json = ?,
            created_by = COALESCE(?, created_by),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (draft_text, model_name, quality_json, actor, draft_id),
    )
    conn.commit()
    return True


def create_approval_action(
    conn: sqlite3.Connection,
    *,
    draft_id: Optional[int],
    user_id: int,
    thread_id: str,
    action: str,
    actor: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> int:
    payload_json = json.dumps(payload or {}, ensure_ascii=False)
    cursor = conn.execute(
        """
        INSERT INTO approval_actions (draft_id, user_id, thread_id, action, actor, payload_json)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (draft_id, user_id, thread_id, action, actor, payload_json),
    )
    conn.commit()
    return int(cursor.lastrowid)


def list_approval_actions_for_thread(
    conn: sqlite3.Connection,
    *,
    thread_id: str,
    limit: int = 50,
) -> list[Dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT id, draft_id, user_id, thread_id, action, actor, payload_json, created_at
        FROM approval_actions
        WHERE thread_id = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (thread_id, max(1, int(limit))),
    )
    rows: list[Dict[str, Any]] = []
    for row in cursor.fetchall():
        item = dict(row)
        item["payload"] = _safe_json_loads(item.pop("payload_json"))
        rows.append(item)
    return rows


def upsert_conversation_outcome(
    conn: sqlite3.Connection,
    *,
    user_id: int,
    thread_id: str,
    outcome: str,
    note: Optional[str] = None,
    created_by: Optional[str] = None,
) -> int:
    conn.execute(
        """
        INSERT INTO conversation_outcomes (user_id, thread_id, outcome, note, created_by)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(thread_id) DO UPDATE SET
            user_id = excluded.user_id,
            outcome = excluded.outcome,
            note = excluded.note,
            created_by = excluded.created_by,
            updated_at = CURRENT_TIMESTAMP
        """,
        (user_id, thread_id, outcome, note, created_by),
    )
    conn.commit()
    row = conn.execute(
        "SELECT id FROM conversation_outcomes WHERE thread_id = ? LIMIT 1",
        (thread_id,),
    ).fetchone()
    return int(row["id"]) if row else 0


def get_conversation_outcome(
    conn: sqlite3.Connection,
    *,
    thread_id: str,
) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT id, user_id, thread_id, outcome, note, created_by, created_at, updated_at
        FROM conversation_outcomes
        WHERE thread_id = ?
        LIMIT 1
        """,
        (thread_id,),
    ).fetchone()
    return dict(row) if row else None


def create_followup_task(
    conn: sqlite3.Connection,
    *,
    user_id: int,
    thread_id: str,
    priority: str,
    reason: str,
    status: str = "pending",
    due_at: Optional[str] = None,
    assigned_to: Optional[str] = None,
    related_draft_id: Optional[int] = None,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO followup_tasks (
            user_id, thread_id, priority, reason, status, due_at, assigned_to, related_draft_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id,
            thread_id,
            priority,
            reason,
            status,
            due_at,
            assigned_to,
            related_draft_id,
        ),
    )
    conn.commit()
    return int(cursor.lastrowid)


def list_followup_tasks(
    conn: sqlite3.Connection,
    *,
    status: Optional[str] = None,
    limit: int = 200,
) -> list[Dict[str, Any]]:
    params: list[Any] = []
    where_sql = ""
    if isinstance(status, str) and status.strip():
        where_sql = "WHERE t.status = ?"
        params.append(status.strip())
    params.append(max(1, int(limit)))

    cursor = conn.execute(
        f"""
        SELECT
            t.id,
            t.user_id,
            t.thread_id,
            t.priority,
            t.reason,
            t.status,
            t.due_at,
            t.assigned_to,
            t.related_draft_id,
            t.created_at,
            t.updated_at,
            u.channel,
            u.external_id,
            u.username,
            u.first_name,
            u.last_name
        FROM followup_tasks t
        JOIN users u ON u.id = t.user_id
        {where_sql}
        ORDER BY
            CASE t.priority WHEN 'hot' THEN 0 WHEN 'warm' THEN 1 ELSE 2 END,
            COALESCE(t.due_at, t.created_at) ASC,
            t.id DESC
        LIMIT ?
        """,
        tuple(params),
    )
    return [dict(row) for row in cursor.fetchall()]


def create_lead_score(
    conn: sqlite3.Connection,
    *,
    user_id: int,
    thread_id: str,
    score: float,
    temperature: str,
    confidence: Optional[float] = None,
    factors: Optional[Dict[str, Any]] = None,
) -> int:
    factors_json = json.dumps(factors or {}, ensure_ascii=False)
    cursor = conn.execute(
        """
        INSERT INTO lead_scores (user_id, thread_id, score, temperature, confidence, factors_json)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (user_id, thread_id, float(score), temperature, confidence, factors_json),
    )
    conn.commit()
    return int(cursor.lastrowid)


def get_latest_lead_score(
    conn: sqlite3.Connection,
    *,
    thread_id: str,
) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT id, user_id, thread_id, score, temperature, confidence, factors_json, created_at
        FROM lead_scores
        WHERE thread_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (thread_id,),
    ).fetchone()
    if not row:
        return None
    item = dict(row)
    item["factors"] = _safe_json_loads(item.pop("factors_json"))
    return item


def list_inbox_threads(conn: sqlite3.Connection, *, limit: int = 100) -> list[Dict[str, Any]]:
    conversations = list_recent_conversations(conn, limit=max(1, int(limit)))
    items: list[Dict[str, Any]] = []
    for row in conversations:
        user_id = int(row["user_id"])
        thread_id = f"tg:{user_id}"
        latest_draft_row = conn.execute(
            """
            SELECT id, status, draft_text, created_at, updated_at, sent_at, approved_at
            FROM reply_drafts
            WHERE thread_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (thread_id,),
        ).fetchone()
        outcome = get_conversation_outcome(conn, thread_id=thread_id)
        pending_followups = int(
            conn.execute(
                "SELECT COUNT(*) AS cnt FROM followup_tasks WHERE thread_id = ? AND status = 'pending'",
                (thread_id,),
            ).fetchone()["cnt"]
        )
        lead_score = get_latest_lead_score(conn, thread_id=thread_id)
        status_value = "new"
        latest_draft: Optional[Dict[str, Any]] = None
        if latest_draft_row:
            latest_draft = dict(latest_draft_row)
            status_value = str(latest_draft.get("status") or "new")
        elif pending_followups > 0:
            status_value = "manual_required"

        items.append(
            {
                "thread_id": thread_id,
                "status": status_value,
                "user_id": user_id,
                "channel": row.get("channel"),
                "external_id": row.get("external_id"),
                "username": row.get("username"),
                "first_name": row.get("first_name"),
                "last_name": row.get("last_name"),
                "messages_count": int(row.get("messages_count") or 0),
                "last_message_at": row.get("last_message_at"),
                "latest_draft": latest_draft,
                "outcome": outcome,
                "pending_followups": pending_followups,
                "lead_score": lead_score,
            }
        )
    return items


def get_inbox_thread_detail(
    conn: sqlite3.Connection,
    *,
    user_id: int,
    limit_messages: int = 300,
) -> Dict[str, Any]:
    thread_id = f"tg:{user_id}"
    user_row = conn.execute(
        """
        SELECT id, channel, external_id, username, first_name, last_name, created_at
        FROM users
        WHERE id = ?
        LIMIT 1
        """,
        (user_id,),
    ).fetchone()
    messages = list_conversation_messages(conn, user_id=user_id, limit=max(1, int(limit_messages)))
    drafts = list_reply_drafts_for_thread(conn, thread_id=thread_id, limit=50)
    outcome = get_conversation_outcome(conn, thread_id=thread_id)
    followups = list_followup_tasks(conn, status=None, limit=500)
    followups = [item for item in followups if item.get("thread_id") == thread_id]
    lead_score = get_latest_lead_score(conn, thread_id=thread_id)
    actions = list_approval_actions_for_thread(conn, thread_id=thread_id, limit=100)
    return {
        "thread_id": thread_id,
        "user": dict(user_row) if user_row else None,
        "messages": messages,
        "drafts": drafts,
        "outcome": outcome,
        "followups": followups,
        "lead_score": lead_score,
        "approval_actions": actions,
    }


def get_revenue_metrics_snapshot(conn: sqlite3.Connection) -> Dict[str, Any]:
    drafts_created_today = int(
        conn.execute(
            "SELECT COUNT(*) AS cnt FROM reply_drafts WHERE created_at >= date('now')",
        ).fetchone()["cnt"]
    )
    drafts_approved_today = int(
        conn.execute(
            "SELECT COUNT(*) AS cnt FROM approval_actions WHERE action = 'draft_approved' AND created_at >= date('now')",
        ).fetchone()["cnt"]
    )
    drafts_sent_today = int(
        conn.execute(
            "SELECT COUNT(*) AS cnt FROM approval_actions WHERE action = 'draft_sent' AND created_at >= date('now')",
        ).fetchone()["cnt"]
    )
    followups_pending = int(
        conn.execute(
            "SELECT COUNT(*) AS cnt FROM followup_tasks WHERE status = 'pending'",
        ).fetchone()["cnt"]
    )
    lead_counts = {"hot": 0, "warm": 0, "cold": 0}
    cursor = conn.execute(
        """
        WITH latest AS (
            SELECT thread_id, MAX(id) AS max_id
            FROM lead_scores
            GROUP BY thread_id
        )
        SELECT ls.temperature, COUNT(*) AS cnt
        FROM lead_scores ls
        JOIN latest l ON l.max_id = ls.id
        GROUP BY ls.temperature
        """
    )
    for row in cursor.fetchall():
        temperature = str(row["temperature"] or "").strip().lower()
        if temperature in lead_counts:
            lead_counts[temperature] = int(row["cnt"] or 0)

    return {
        "drafts_created_today": drafts_created_today,
        "drafts_approved_today": drafts_approved_today,
        "drafts_sent_today": drafts_sent_today,
        "followups_pending": followups_pending,
        "lead_temperature": lead_counts,
    }
