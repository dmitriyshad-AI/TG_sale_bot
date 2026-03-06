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
    """
    CREATE TABLE IF NOT EXISTS business_connections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        business_connection_id TEXT NOT NULL UNIQUE,
        telegram_user_id INTEGER,
        user_chat_id INTEGER,
        can_reply INTEGER NOT NULL DEFAULT 0,
        is_enabled INTEGER NOT NULL DEFAULT 1,
        connected_at TEXT,
        meta_json TEXT,
        last_seen_at TEXT DEFAULT CURRENT_TIMESTAMP,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS business_threads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        thread_key TEXT NOT NULL UNIQUE,
        business_connection_id TEXT NOT NULL,
        chat_id INTEGER NOT NULL,
        user_id INTEGER,
        last_message_at TEXT,
        last_inbound_at TEXT,
        last_outbound_at TEXT,
        meta_json TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS business_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        business_connection_id TEXT NOT NULL,
        thread_key TEXT NOT NULL,
        telegram_message_id INTEGER,
        user_id INTEGER,
        direction TEXT NOT NULL,
        text TEXT,
        payload_json TEXT,
        is_deleted INTEGER NOT NULL DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id),
        UNIQUE(business_connection_id, telegram_message_id, direction)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS call_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        thread_id TEXT NOT NULL,
        source_type TEXT NOT NULL,
        source_ref TEXT,
        file_path TEXT,
        status TEXT NOT NULL DEFAULT 'queued',
        error_text TEXT,
        duration_seconds REAL,
        created_by TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS call_transcripts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        call_id INTEGER NOT NULL UNIQUE,
        provider TEXT NOT NULL,
        transcript_text TEXT NOT NULL,
        language TEXT,
        confidence REAL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(call_id) REFERENCES call_records(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS call_summaries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        call_id INTEGER NOT NULL UNIQUE,
        summary_text TEXT NOT NULL,
        interests_json TEXT NOT NULL DEFAULT '[]',
        objections_json TEXT NOT NULL DEFAULT '[]',
        next_best_action TEXT,
        warmth TEXT NOT NULL DEFAULT 'warm',
        confidence REAL,
        model_name TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(call_id) REFERENCES call_records(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS mango_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT NOT NULL UNIQUE,
        call_external_id TEXT,
        source TEXT NOT NULL DEFAULT 'webhook',
        status TEXT NOT NULL DEFAULT 'queued',
        payload_json TEXT NOT NULL,
        error_text TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS faq_candidates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        question_key TEXT NOT NULL UNIQUE,
        question_text TEXT NOT NULL,
        question_count INTEGER NOT NULL DEFAULT 0,
        thread_count INTEGER NOT NULL DEFAULT 0,
        approvals_count INTEGER NOT NULL DEFAULT 0,
        sends_count INTEGER NOT NULL DEFAULT 0,
        next_step_count INTEGER NOT NULL DEFAULT 0,
        reply_approved_rate REAL NOT NULL DEFAULT 0,
        next_step_rate REAL NOT NULL DEFAULT 0,
        first_seen_at TEXT,
        last_seen_at TEXT,
        sample_thread_id TEXT,
        status TEXT NOT NULL DEFAULT 'new',
        source_json TEXT NOT NULL DEFAULT '{}',
        suggested_answer TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS canonical_answers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        candidate_id INTEGER NOT NULL UNIQUE,
        question_key TEXT NOT NULL,
        question_text TEXT NOT NULL,
        answer_text TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        created_by TEXT,
        promoted_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(candidate_id) REFERENCES faq_candidates(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS answer_performance (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        answer_kind TEXT NOT NULL,
        answer_ref TEXT NOT NULL,
        question_key TEXT NOT NULL,
        question_text TEXT NOT NULL,
        question_count INTEGER NOT NULL DEFAULT 0,
        approvals_count INTEGER NOT NULL DEFAULT 0,
        sends_count INTEGER NOT NULL DEFAULT 0,
        next_step_count INTEGER NOT NULL DEFAULT 0,
        reply_approved_rate REAL NOT NULL DEFAULT 0,
        next_step_rate REAL NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'active',
        source_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(answer_kind, answer_ref, question_key)
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
    "CREATE INDEX IF NOT EXISTS idx_business_connections_last_seen ON business_connections(last_seen_at);",
    "CREATE INDEX IF NOT EXISTS idx_business_connections_enabled ON business_connections(is_enabled, updated_at);",
    "CREATE INDEX IF NOT EXISTS idx_business_threads_connection_chat ON business_threads(business_connection_id, chat_id);",
    "CREATE INDEX IF NOT EXISTS idx_business_threads_last_message ON business_threads(last_message_at);",
    "CREATE INDEX IF NOT EXISTS idx_business_messages_thread_created ON business_messages(thread_key, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_business_messages_connection_created ON business_messages(business_connection_id, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_business_messages_user_created ON business_messages(user_id, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_business_messages_deleted_created ON business_messages(is_deleted, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_call_records_user_created ON call_records(user_id, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_call_records_thread_created ON call_records(thread_id, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_call_records_status_created ON call_records(status, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_call_transcripts_call ON call_transcripts(call_id);",
    "CREATE INDEX IF NOT EXISTS idx_call_summaries_call ON call_summaries(call_id);",
    "CREATE INDEX IF NOT EXISTS idx_call_summaries_warmth_created ON call_summaries(warmth, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_mango_events_status_created ON mango_events(status, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_mango_events_call_external ON mango_events(call_external_id, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_faq_candidates_question_count ON faq_candidates(question_count DESC, updated_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_faq_candidates_status ON faq_candidates(status, updated_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_faq_candidates_last_seen ON faq_candidates(last_seen_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_canonical_answers_status ON canonical_answers(status, updated_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_canonical_answers_question_key ON canonical_answers(question_key);",
    "CREATE INDEX IF NOT EXISTS idx_answer_performance_kind_status ON answer_performance(answer_kind, status, next_step_rate DESC, question_count DESC);",
    "CREATE INDEX IF NOT EXISTS idx_answer_performance_question_key ON answer_performance(question_key);",
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


def _safe_json_list(raw_value: Optional[str]) -> list[Any]:
    if not raw_value:
        return []
    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError:
        return []
    return payload if isinstance(payload, list) else []


def normalize_phone(value: object) -> str:
    if not isinstance(value, str):
        return ""
    digits = "".join(ch for ch in value if ch.isdigit())
    if not digits:
        return ""
    if len(digits) == 11 and digits.startswith("8"):
        digits = f"7{digits[1:]}"
    return digits


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
    latest_call_insights = get_latest_call_summary_for_thread(conn, thread_id=thread_id)
    actions = list_approval_actions_for_thread(conn, thread_id=thread_id, limit=100)
    return {
        "thread_id": thread_id,
        "user": dict(user_row) if user_row else None,
        "messages": messages,
        "drafts": drafts,
        "outcome": outcome,
        "followups": followups,
        "lead_score": lead_score,
        "latest_call_insights": latest_call_insights,
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


def upsert_faq_candidate(
    conn: sqlite3.Connection,
    *,
    question_key: str,
    question_text: str,
    question_count: int,
    thread_count: int,
    approvals_count: int,
    sends_count: int,
    next_step_count: int,
    reply_approved_rate: float,
    next_step_rate: float,
    first_seen_at: Optional[str] = None,
    last_seen_at: Optional[str] = None,
    sample_thread_id: Optional[str] = None,
    status: str = "new",
    source: Optional[Dict[str, Any]] = None,
    suggested_answer: Optional[str] = None,
) -> int:
    normalized_key = (question_key or "").strip()
    if not normalized_key:
        raise ValueError("question_key is required")

    normalized_text = (question_text or "").strip()
    if not normalized_text:
        raise ValueError("question_text is required")

    source_json = json.dumps(source or {}, ensure_ascii=False)
    conn.execute(
        """
        INSERT INTO faq_candidates (
            question_key,
            question_text,
            question_count,
            thread_count,
            approvals_count,
            sends_count,
            next_step_count,
            reply_approved_rate,
            next_step_rate,
            first_seen_at,
            last_seen_at,
            sample_thread_id,
            status,
            source_json,
            suggested_answer
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(question_key) DO UPDATE SET
            question_text = excluded.question_text,
            question_count = excluded.question_count,
            thread_count = excluded.thread_count,
            approvals_count = excluded.approvals_count,
            sends_count = excluded.sends_count,
            next_step_count = excluded.next_step_count,
            reply_approved_rate = excluded.reply_approved_rate,
            next_step_rate = excluded.next_step_rate,
            first_seen_at = COALESCE(excluded.first_seen_at, faq_candidates.first_seen_at),
            last_seen_at = COALESCE(excluded.last_seen_at, faq_candidates.last_seen_at),
            sample_thread_id = COALESCE(excluded.sample_thread_id, faq_candidates.sample_thread_id),
            source_json = excluded.source_json,
            suggested_answer = COALESCE(excluded.suggested_answer, faq_candidates.suggested_answer),
            status = CASE
                WHEN faq_candidates.status IN ('promoted', 'archived') THEN faq_candidates.status
                ELSE excluded.status
            END,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            normalized_key,
            normalized_text,
            max(0, int(question_count)),
            max(0, int(thread_count)),
            max(0, int(approvals_count)),
            max(0, int(sends_count)),
            max(0, int(next_step_count)),
            float(reply_approved_rate),
            float(next_step_rate),
            (first_seen_at or "").strip() or None,
            (last_seen_at or "").strip() or None,
            (sample_thread_id or "").strip() or None,
            (status or "new").strip().lower() or "new",
            source_json,
            (suggested_answer or "").strip() or None,
        ),
    )
    conn.commit()
    row = conn.execute(
        "SELECT id FROM faq_candidates WHERE question_key = ? LIMIT 1",
        (normalized_key,),
    ).fetchone()
    return int(row["id"]) if row else 0


def get_faq_candidate(conn: sqlite3.Connection, *, candidate_id: int) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT
            id,
            question_key,
            question_text,
            question_count,
            thread_count,
            approvals_count,
            sends_count,
            next_step_count,
            reply_approved_rate,
            next_step_rate,
            first_seen_at,
            last_seen_at,
            sample_thread_id,
            status,
            source_json,
            suggested_answer,
            created_at,
            updated_at
        FROM faq_candidates
        WHERE id = ?
        LIMIT 1
        """,
        (candidate_id,),
    ).fetchone()
    if not row:
        return None
    item = dict(row)
    item["source"] = _safe_json_loads(item.pop("source_json"))
    return item


def list_faq_candidates(
    conn: sqlite3.Connection,
    *,
    status: Optional[str] = None,
    limit: int = 100,
) -> list[Dict[str, Any]]:
    params: list[Any] = []
    where_sql = ""
    if isinstance(status, str) and status.strip():
        where_sql = "WHERE status = ?"
        params.append(status.strip().lower())
    params.append(max(1, int(limit)))
    cursor = conn.execute(
        f"""
        SELECT
            id,
            question_key,
            question_text,
            question_count,
            thread_count,
            approvals_count,
            sends_count,
            next_step_count,
            reply_approved_rate,
            next_step_rate,
            first_seen_at,
            last_seen_at,
            sample_thread_id,
            status,
            source_json,
            suggested_answer,
            created_at,
            updated_at
        FROM faq_candidates
        {where_sql}
        ORDER BY question_count DESC, last_seen_at DESC, id DESC
        LIMIT ?
        """,
        tuple(params),
    )
    rows: list[Dict[str, Any]] = []
    for row in cursor.fetchall():
        item = dict(row)
        item["source"] = _safe_json_loads(item.pop("source_json"))
        rows.append(item)
    return rows


def promote_faq_candidate_to_canonical(
    conn: sqlite3.Connection,
    *,
    candidate_id: int,
    answer_text: Optional[str] = None,
    created_by: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    candidate = get_faq_candidate(conn, candidate_id=candidate_id)
    if not candidate:
        return None

    resolved_answer = (answer_text or "").strip() or (candidate.get("suggested_answer") or "").strip()
    if not resolved_answer:
        resolved_answer = (
            "Уточняем цель, класс и формат, затем даём 2-3 релевантных направления "
            "и предлагаем следующий шаг с менеджером."
        )

    conn.execute(
        """
        INSERT INTO canonical_answers (
            candidate_id,
            question_key,
            question_text,
            answer_text,
            status,
            created_by,
            promoted_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, 'active', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(candidate_id) DO UPDATE SET
            question_key = excluded.question_key,
            question_text = excluded.question_text,
            answer_text = excluded.answer_text,
            status = 'active',
            created_by = COALESCE(excluded.created_by, canonical_answers.created_by),
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            int(candidate_id),
            str(candidate.get("question_key") or ""),
            str(candidate.get("question_text") or ""),
            resolved_answer,
            (created_by or "").strip() or None,
        ),
    )
    conn.execute(
        """
        UPDATE faq_candidates
        SET status = 'promoted',
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (candidate_id,),
    )
    conn.commit()

    row = conn.execute(
        """
        SELECT
            id,
            candidate_id,
            question_key,
            question_text,
            answer_text,
            status,
            created_by,
            promoted_at,
            updated_at
        FROM canonical_answers
        WHERE candidate_id = ?
        LIMIT 1
        """,
        (candidate_id,),
    ).fetchone()
    return dict(row) if row else None


def list_canonical_answers(
    conn: sqlite3.Connection,
    *,
    status: Optional[str] = "active",
    limit: int = 100,
) -> list[Dict[str, Any]]:
    params: list[Any] = []
    where_sql = ""
    if isinstance(status, str) and status.strip():
        where_sql = "WHERE status = ?"
        params.append(status.strip().lower())
    params.append(max(1, int(limit)))
    cursor = conn.execute(
        f"""
        SELECT
            id,
            candidate_id,
            question_key,
            question_text,
            answer_text,
            status,
            created_by,
            promoted_at,
            updated_at
        FROM canonical_answers
        {where_sql}
        ORDER BY updated_at DESC, id DESC
        LIMIT ?
        """,
        tuple(params),
    )
    return [dict(row) for row in cursor.fetchall()]


def upsert_answer_performance(
    conn: sqlite3.Connection,
    *,
    answer_kind: str,
    answer_ref: str,
    question_key: str,
    question_text: str,
    question_count: int,
    approvals_count: int,
    sends_count: int,
    next_step_count: int,
    reply_approved_rate: float,
    next_step_rate: float,
    status: str = "active",
    source: Optional[Dict[str, Any]] = None,
) -> int:
    normalized_kind = (answer_kind or "").strip().lower()
    normalized_ref = (answer_ref or "").strip()
    normalized_key = (question_key or "").strip()
    normalized_text = (question_text or "").strip()
    if not normalized_kind or not normalized_ref or not normalized_key or not normalized_text:
        raise ValueError("answer_kind, answer_ref, question_key and question_text are required")

    source_json = json.dumps(source or {}, ensure_ascii=False)
    conn.execute(
        """
        INSERT INTO answer_performance (
            answer_kind,
            answer_ref,
            question_key,
            question_text,
            question_count,
            approvals_count,
            sends_count,
            next_step_count,
            reply_approved_rate,
            next_step_rate,
            status,
            source_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(answer_kind, answer_ref, question_key) DO UPDATE SET
            question_text = excluded.question_text,
            question_count = excluded.question_count,
            approvals_count = excluded.approvals_count,
            sends_count = excluded.sends_count,
            next_step_count = excluded.next_step_count,
            reply_approved_rate = excluded.reply_approved_rate,
            next_step_rate = excluded.next_step_rate,
            status = excluded.status,
            source_json = excluded.source_json,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            normalized_kind,
            normalized_ref,
            normalized_key,
            normalized_text,
            max(0, int(question_count)),
            max(0, int(approvals_count)),
            max(0, int(sends_count)),
            max(0, int(next_step_count)),
            float(reply_approved_rate),
            float(next_step_rate),
            (status or "active").strip().lower() or "active",
            source_json,
        ),
    )
    conn.commit()
    row = conn.execute(
        """
        SELECT id
        FROM answer_performance
        WHERE answer_kind = ? AND answer_ref = ? AND question_key = ?
        LIMIT 1
        """,
        (normalized_kind, normalized_ref, normalized_key),
    ).fetchone()
    return int(row["id"]) if row else 0


def list_answer_performance(
    conn: sqlite3.Connection,
    *,
    answer_kind: Optional[str] = None,
    status: Optional[str] = "active",
    limit: int = 100,
) -> list[Dict[str, Any]]:
    where_parts: list[str] = []
    params: list[Any] = []
    if isinstance(answer_kind, str) and answer_kind.strip():
        where_parts.append("answer_kind = ?")
        params.append(answer_kind.strip().lower())
    if isinstance(status, str) and status.strip():
        where_parts.append("status = ?")
        params.append(status.strip().lower())
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    params.append(max(1, int(limit)))
    cursor = conn.execute(
        f"""
        SELECT
            id,
            answer_kind,
            answer_ref,
            question_key,
            question_text,
            question_count,
            approvals_count,
            sends_count,
            next_step_count,
            reply_approved_rate,
            next_step_rate,
            status,
            source_json,
            created_at,
            updated_at
        FROM answer_performance
        {where_sql}
        ORDER BY next_step_rate DESC, reply_approved_rate DESC, question_count DESC, updated_at DESC
        LIMIT ?
        """,
        tuple(params),
    )
    rows: list[Dict[str, Any]] = []
    for row in cursor.fetchall():
        item = dict(row)
        item["source"] = _safe_json_loads(item.pop("source_json"))
        rows.append(item)
    return rows


def list_rejected_reply_drafts(conn: sqlite3.Connection, *, limit: int = 50) -> list[Dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT
            id,
            user_id,
            thread_id,
            source_message_id,
            draft_text,
            model_name,
            created_at,
            updated_at
        FROM reply_drafts
        WHERE status = 'rejected'
        ORDER BY updated_at DESC, id DESC
        LIMIT ?
        """,
        (max(1, int(limit)),),
    )
    return [dict(row) for row in cursor.fetchall()]


def build_business_thread_key(*, business_connection_id: str, chat_id: int) -> str:
    return f"biz:{business_connection_id}:{int(chat_id)}"


def upsert_business_connection(
    conn: sqlite3.Connection,
    *,
    business_connection_id: str,
    telegram_user_id: Optional[int] = None,
    user_chat_id: Optional[int] = None,
    can_reply: Optional[bool] = None,
    is_enabled: Optional[bool] = None,
    connected_at: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> int:
    connection_id = (business_connection_id or "").strip()
    if not connection_id:
        raise ValueError("business_connection_id is required")

    meta_json = json.dumps(meta or {}, ensure_ascii=False)
    can_reply_value = None if can_reply is None else (1 if bool(can_reply) else 0)
    is_enabled_value = None if is_enabled is None else (1 if bool(is_enabled) else 0)
    conn.execute(
        """
        INSERT INTO business_connections (
            business_connection_id,
            telegram_user_id,
            user_chat_id,
            can_reply,
            is_enabled,
            connected_at,
            meta_json,
            last_seen_at
        )
        VALUES (?, ?, ?, COALESCE(?, 0), COALESCE(?, 1), ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(business_connection_id) DO UPDATE SET
            telegram_user_id = COALESCE(excluded.telegram_user_id, business_connections.telegram_user_id),
            user_chat_id = COALESCE(excluded.user_chat_id, business_connections.user_chat_id),
            can_reply = COALESCE(excluded.can_reply, business_connections.can_reply),
            is_enabled = COALESCE(excluded.is_enabled, business_connections.is_enabled),
            connected_at = COALESCE(excluded.connected_at, business_connections.connected_at),
            meta_json = excluded.meta_json,
            last_seen_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            connection_id,
            telegram_user_id,
            user_chat_id,
            can_reply_value,
            is_enabled_value,
            connected_at,
            meta_json,
        ),
    )
    conn.commit()
    row = conn.execute(
        "SELECT id FROM business_connections WHERE business_connection_id = ? LIMIT 1",
        (connection_id,),
    ).fetchone()
    return int(row["id"]) if row else 0


def get_business_connection(
    conn: sqlite3.Connection,
    *,
    business_connection_id: str,
) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT
            id,
            business_connection_id,
            telegram_user_id,
            user_chat_id,
            can_reply,
            is_enabled,
            connected_at,
            meta_json,
            last_seen_at,
            created_at,
            updated_at
        FROM business_connections
        WHERE business_connection_id = ?
        LIMIT 1
        """,
        (business_connection_id.strip(),),
    ).fetchone()
    if not row:
        return None
    item = dict(row)
    item["meta"] = _safe_json_loads(item.pop("meta_json"))
    item["can_reply"] = bool(item.get("can_reply"))
    item["is_enabled"] = bool(item.get("is_enabled"))
    return item


def upsert_business_thread(
    conn: sqlite3.Connection,
    *,
    business_connection_id: str,
    chat_id: int,
    user_id: Optional[int] = None,
    direction: Optional[str] = None,
    occurred_at: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> str:
    connection_id = (business_connection_id or "").strip()
    if not connection_id:
        raise ValueError("business_connection_id is required")
    thread_key = build_business_thread_key(
        business_connection_id=connection_id,
        chat_id=int(chat_id),
    )
    direction_normalized = (direction or "").strip().lower()
    if direction_normalized not in {"inbound", "outbound"}:
        direction_normalized = ""

    message_at = (occurred_at or "").strip() or None
    last_inbound_at = message_at if direction_normalized == "inbound" else None
    last_outbound_at = message_at if direction_normalized == "outbound" else None
    meta_json = json.dumps(meta or {}, ensure_ascii=False)

    conn.execute(
        """
        INSERT INTO business_threads (
            thread_key,
            business_connection_id,
            chat_id,
            user_id,
            last_message_at,
            last_inbound_at,
            last_outbound_at,
            meta_json
        )
        VALUES (?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), ?, ?, ?)
        ON CONFLICT(thread_key) DO UPDATE SET
            user_id = COALESCE(excluded.user_id, business_threads.user_id),
            last_message_at = COALESCE(excluded.last_message_at, CURRENT_TIMESTAMP),
            last_inbound_at = COALESCE(excluded.last_inbound_at, business_threads.last_inbound_at),
            last_outbound_at = COALESCE(excluded.last_outbound_at, business_threads.last_outbound_at),
            meta_json = excluded.meta_json,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            thread_key,
            connection_id,
            int(chat_id),
            user_id,
            message_at,
            last_inbound_at,
            last_outbound_at,
            meta_json,
        ),
    )
    conn.commit()
    return thread_key


def log_business_message(
    conn: sqlite3.Connection,
    *,
    business_connection_id: str,
    chat_id: int,
    telegram_message_id: Optional[int],
    direction: str,
    text: Optional[str],
    user_id: Optional[int] = None,
    payload: Optional[Dict[str, Any]] = None,
    created_at: Optional[str] = None,
) -> int:
    thread_key = upsert_business_thread(
        conn,
        business_connection_id=business_connection_id,
        chat_id=chat_id,
        user_id=user_id,
        direction=direction,
        occurred_at=created_at,
    )
    payload_json = json.dumps(payload or {}, ensure_ascii=False)
    message_direction = (direction or "").strip().lower() or "inbound"
    message_id = telegram_message_id if isinstance(telegram_message_id, int) else None
    try:
        cursor = conn.execute(
            """
            INSERT INTO business_messages (
                business_connection_id,
                thread_key,
                telegram_message_id,
                user_id,
                direction,
                text,
                payload_json,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), CURRENT_TIMESTAMP)
            """,
            (
                business_connection_id.strip(),
                thread_key,
                message_id,
                user_id,
                message_direction,
                text,
                payload_json,
                (created_at or "").strip() or None,
            ),
        )
    except sqlite3.IntegrityError:
        if message_id is None:
            raise
        row = conn.execute(
            """
            SELECT id
            FROM business_messages
            WHERE business_connection_id = ?
              AND telegram_message_id = ?
              AND direction = ?
            LIMIT 1
            """,
            (business_connection_id.strip(), message_id, message_direction),
        ).fetchone()
        if not row:
            raise
        return int(row["id"])

    conn.commit()
    return int(cursor.lastrowid)


def mark_business_messages_deleted(
    conn: sqlite3.Connection,
    *,
    business_connection_id: str,
    chat_id: int,
    message_ids: list[int],
) -> int:
    if not message_ids:
        return 0
    thread_key = build_business_thread_key(
        business_connection_id=business_connection_id.strip(),
        chat_id=int(chat_id),
    )
    placeholders = ",".join("?" for _ in message_ids)
    params: list[Any] = [business_connection_id.strip(), thread_key, *[int(mid) for mid in message_ids]]
    cursor = conn.execute(
        f"""
        UPDATE business_messages
        SET
            is_deleted = 1,
            updated_at = CURRENT_TIMESTAMP
        WHERE business_connection_id = ?
          AND thread_key = ?
          AND telegram_message_id IN ({placeholders})
        """,
        tuple(params),
    )
    conn.commit()
    return int(cursor.rowcount)


def list_recent_business_threads(conn: sqlite3.Connection, *, limit: int = 100) -> list[Dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT
            t.thread_key,
            t.business_connection_id,
            t.chat_id,
            t.user_id,
            t.last_message_at,
            t.last_inbound_at,
            t.last_outbound_at,
            t.updated_at,
            u.external_id AS user_external_id,
            u.username,
            u.first_name,
            u.last_name,
            (
                SELECT COUNT(*)
                FROM business_messages bm
                WHERE bm.thread_key = t.thread_key
            ) AS messages_count
        FROM business_threads t
        LEFT JOIN users u ON u.id = t.user_id
        ORDER BY COALESCE(t.last_message_at, t.updated_at) DESC, t.id DESC
        LIMIT ?
        """,
        (max(1, int(limit)),),
    )
    return [dict(row) for row in cursor.fetchall()]


def list_business_messages(
    conn: sqlite3.Connection,
    *,
    thread_key: str,
    limit: int = 200,
) -> list[Dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT
            id,
            business_connection_id,
            thread_key,
            telegram_message_id,
            user_id,
            direction,
            text,
            payload_json,
            is_deleted,
            created_at,
            updated_at
        FROM business_messages
        WHERE thread_key = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (thread_key, max(1, int(limit))),
    )
    rows: list[Dict[str, Any]] = []
    for row in cursor.fetchall():
        item = dict(row)
        item["payload"] = _safe_json_loads(item.pop("payload_json"))
        item["is_deleted"] = bool(item.get("is_deleted"))
        rows.append(item)
    rows.reverse()
    return rows


def create_call_record(
    conn: sqlite3.Connection,
    *,
    user_id: int,
    thread_id: str,
    source_type: str,
    source_ref: Optional[str] = None,
    file_path: Optional[str] = None,
    status: str = "queued",
    error_text: Optional[str] = None,
    duration_seconds: Optional[float] = None,
    created_by: Optional[str] = None,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO call_records (
            user_id,
            thread_id,
            source_type,
            source_ref,
            file_path,
            status,
            error_text,
            duration_seconds,
            created_by
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(user_id),
            thread_id.strip(),
            source_type.strip(),
            (source_ref or "").strip() or None,
            (file_path or "").strip() or None,
            (status or "").strip() or "queued",
            (error_text or "").strip() or None,
            duration_seconds,
            (created_by or "").strip() or None,
        ),
    )
    conn.commit()
    return int(cursor.lastrowid)


def update_call_record_status(
    conn: sqlite3.Connection,
    *,
    call_id: int,
    status: str,
    error_text: Optional[str] = None,
    duration_seconds: Optional[float] = None,
) -> bool:
    row = conn.execute("SELECT id FROM call_records WHERE id = ? LIMIT 1", (int(call_id),)).fetchone()
    if not row:
        return False
    conn.execute(
        """
        UPDATE call_records
        SET
            status = ?,
            error_text = ?,
            duration_seconds = COALESCE(?, duration_seconds),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (
            (status or "").strip() or "queued",
            (error_text or "").strip() or None,
            duration_seconds,
            int(call_id),
        ),
    )
    conn.commit()
    return True


def upsert_call_transcript(
    conn: sqlite3.Connection,
    *,
    call_id: int,
    provider: str,
    transcript_text: str,
    language: Optional[str] = None,
    confidence: Optional[float] = None,
) -> int:
    conn.execute(
        """
        INSERT INTO call_transcripts (call_id, provider, transcript_text, language, confidence)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(call_id) DO UPDATE SET
            provider = excluded.provider,
            transcript_text = excluded.transcript_text,
            language = excluded.language,
            confidence = excluded.confidence,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            int(call_id),
            provider.strip(),
            transcript_text.strip(),
            (language or "").strip() or None,
            confidence,
        ),
    )
    conn.commit()
    row = conn.execute("SELECT id FROM call_transcripts WHERE call_id = ? LIMIT 1", (int(call_id),)).fetchone()
    return int(row["id"]) if row else 0


def upsert_call_summary(
    conn: sqlite3.Connection,
    *,
    call_id: int,
    summary_text: str,
    interests: Optional[list[str]] = None,
    objections: Optional[list[str]] = None,
    next_best_action: Optional[str] = None,
    warmth: str = "warm",
    confidence: Optional[float] = None,
    model_name: Optional[str] = None,
) -> int:
    conn.execute(
        """
        INSERT INTO call_summaries (
            call_id,
            summary_text,
            interests_json,
            objections_json,
            next_best_action,
            warmth,
            confidence,
            model_name
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(call_id) DO UPDATE SET
            summary_text = excluded.summary_text,
            interests_json = excluded.interests_json,
            objections_json = excluded.objections_json,
            next_best_action = excluded.next_best_action,
            warmth = excluded.warmth,
            confidence = excluded.confidence,
            model_name = excluded.model_name,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            int(call_id),
            summary_text.strip(),
            json.dumps(interests or [], ensure_ascii=False),
            json.dumps(objections or [], ensure_ascii=False),
            (next_best_action or "").strip() or None,
            (warmth or "").strip().lower() or "warm",
            confidence,
            (model_name or "").strip() or None,
        ),
    )
    conn.commit()
    row = conn.execute("SELECT id FROM call_summaries WHERE call_id = ? LIMIT 1", (int(call_id),)).fetchone()
    return int(row["id"]) if row else 0


def get_call_record(conn: sqlite3.Connection, *, call_id: int) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT
            c.id,
            c.user_id,
            c.thread_id,
            c.source_type,
            c.source_ref,
            c.file_path,
            c.status,
            c.error_text,
            c.duration_seconds,
            c.created_by,
            c.created_at,
            c.updated_at,
            u.channel,
            u.external_id,
            u.username,
            u.first_name,
            u.last_name,
            t.provider AS transcript_provider,
            t.transcript_text,
            t.language AS transcript_language,
            t.confidence AS transcript_confidence,
            t.created_at AS transcript_created_at,
            t.updated_at AS transcript_updated_at,
            s.summary_text,
            s.interests_json,
            s.objections_json,
            s.next_best_action,
            s.warmth,
            s.confidence AS summary_confidence,
            s.model_name AS summary_model_name,
            s.created_at AS summary_created_at,
            s.updated_at AS summary_updated_at
        FROM call_records c
        JOIN users u ON u.id = c.user_id
        LEFT JOIN call_transcripts t ON t.call_id = c.id
        LEFT JOIN call_summaries s ON s.call_id = c.id
        WHERE c.id = ?
        LIMIT 1
        """,
        (int(call_id),),
    ).fetchone()
    if not row:
        return None
    item = dict(row)
    item["interests"] = _safe_json_list(item.pop("interests_json", None))
    item["objections"] = _safe_json_list(item.pop("objections_json", None))
    return item


def list_call_records(
    conn: sqlite3.Connection,
    *,
    status: Optional[str] = None,
    limit: int = 100,
) -> list[Dict[str, Any]]:
    params: list[Any] = []
    where_sql = ""
    if isinstance(status, str) and status.strip():
        where_sql = "WHERE c.status = ?"
        params.append(status.strip())
    params.append(max(1, int(limit)))
    cursor = conn.execute(
        f"""
        SELECT
            c.id,
            c.user_id,
            c.thread_id,
            c.source_type,
            c.source_ref,
            c.status,
            c.error_text,
            c.duration_seconds,
            c.created_by,
            c.created_at,
            c.updated_at,
            u.channel,
            u.external_id,
            u.username,
            u.first_name,
            u.last_name,
            s.summary_text,
            s.warmth,
            s.next_best_action,
            s.confidence AS summary_confidence
        FROM call_records c
        JOIN users u ON u.id = c.user_id
        LEFT JOIN call_summaries s ON s.call_id = c.id
        {where_sql}
        ORDER BY c.id DESC
        LIMIT ?
        """,
        tuple(params),
    )
    return [dict(row) for row in cursor.fetchall()]


def get_latest_call_summary_for_thread(
    conn: sqlite3.Connection,
    *,
    thread_id: str,
) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT
            c.id AS call_id,
            c.thread_id,
            c.created_at AS call_created_at,
            s.summary_text,
            s.interests_json,
            s.objections_json,
            s.next_best_action,
            s.warmth,
            s.confidence AS summary_confidence,
            s.model_name AS summary_model_name,
            s.created_at AS summary_created_at
        FROM call_records c
        JOIN call_summaries s ON s.call_id = c.id
        WHERE c.thread_id = ?
        ORDER BY c.id DESC
        LIMIT 1
        """,
        (thread_id.strip(),),
    ).fetchone()
    if not row:
        return None
    item = dict(row)
    item["interests"] = _safe_json_list(item.pop("interests_json", None))
    item["objections"] = _safe_json_list(item.pop("objections_json", None))
    return item


def create_or_get_mango_event(
    conn: sqlite3.Connection,
    *,
    event_id: str,
    call_external_id: Optional[str],
    source: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    normalized_event_id = (event_id or "").strip()
    if not normalized_event_id:
        raise ValueError("event_id is required")
    try:
        cursor = conn.execute(
            """
            INSERT INTO mango_events (event_id, call_external_id, source, status, payload_json)
            VALUES (?, ?, ?, 'queued', ?)
            """,
            (
                normalized_event_id,
                (call_external_id or "").strip() or None,
                (source or "").strip() or "webhook",
                json.dumps(payload or {}, ensure_ascii=False),
            ),
        )
        conn.commit()
        return {"id": int(cursor.lastrowid), "is_new": True}
    except sqlite3.IntegrityError:
        row = conn.execute(
            "SELECT id FROM mango_events WHERE event_id = ? LIMIT 1",
            (normalized_event_id,),
        ).fetchone()
        return {"id": int(row["id"]) if row else 0, "is_new": False}


def update_mango_event_status(
    conn: sqlite3.Connection,
    *,
    event_row_id: int,
    status: str,
    error_text: Optional[str] = None,
) -> bool:
    row = conn.execute("SELECT id FROM mango_events WHERE id = ? LIMIT 1", (int(event_row_id),)).fetchone()
    if not row:
        return False
    conn.execute(
        """
        UPDATE mango_events
        SET
            status = ?,
            error_text = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (
            (status or "").strip() or "queued",
            (error_text or "").strip() or None,
            int(event_row_id),
        ),
    )
    conn.commit()
    return True


def list_mango_events(
    conn: sqlite3.Connection,
    *,
    status: Optional[str] = None,
    limit: int = 100,
    newest_first: bool = True,
) -> list[Dict[str, Any]]:
    params: list[Any] = []
    where_sql = ""
    if isinstance(status, str) and status.strip():
        where_sql = "WHERE status = ?"
        params.append(status.strip())
    params.append(max(1, int(limit)))
    order_sql = "DESC" if newest_first else "ASC"
    cursor = conn.execute(
        f"""
        SELECT id, event_id, call_external_id, source, status, payload_json, error_text, created_at, updated_at
        FROM mango_events
        {where_sql}
        ORDER BY id {order_sql}
        LIMIT ?
        """,
        tuple(params),
    )
    rows: list[Dict[str, Any]] = []
    for row in cursor.fetchall():
        item = dict(row)
        item["payload"] = _safe_json_loads(item.pop("payload_json", None))
        rows.append(item)
    return rows


def get_latest_mango_event_created_at(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        """
        SELECT created_at
        FROM mango_events
        WHERE status = 'done'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return ""
    return str(row["created_at"] or "").strip()


def get_oldest_mango_event_created_at(conn: sqlite3.Connection, *, status: Optional[str] = None) -> str:
    params: list[Any] = []
    where_sql = ""
    if isinstance(status, str) and status.strip():
        where_sql = "WHERE status = ?"
        params.append(status.strip())
    row = conn.execute(
        f"""
        SELECT created_at
        FROM mango_events
        {where_sql}
        ORDER BY id ASC
        LIMIT 1
        """,
        tuple(params),
    ).fetchone()
    if not row:
        return ""
    return str(row["created_at"] or "").strip()


def count_mango_events(conn: sqlite3.Connection, *, status: Optional[str] = None) -> int:
    params: list[Any] = []
    where_sql = ""
    if isinstance(status, str) and status.strip():
        where_sql = "WHERE status = ?"
        params.append(status.strip())
    row = conn.execute(
        f"""
        SELECT COUNT(1) AS cnt
        FROM mango_events
        {where_sql}
        """,
        tuple(params),
    ).fetchone()
    if not row:
        return 0
    return int(row["cnt"] or 0)


def find_user_by_phone(conn: sqlite3.Connection, *, phone: str) -> Optional[int]:
    normalized = normalize_phone(phone)
    if not normalized:
        return None
    tails = {normalized}
    if len(normalized) >= 10:
        tails.add(normalized[-10:])

    rows = conn.execute(
        """
        SELECT user_id, contact_json
        FROM leads
        ORDER BY lead_id DESC
        LIMIT 1000
        """
    ).fetchall()
    for row in rows:
        contact = _safe_json_loads(row["contact_json"])
        for value in contact.values():
            candidate = normalize_phone(str(value))
            if not candidate:
                continue
            candidate_tails = {candidate}
            if len(candidate) >= 10:
                candidate_tails.add(candidate[-10:])
            if tails.intersection(candidate_tails):
                return int(row["user_id"])
    return None


def resolve_preferred_thread_for_user(conn: sqlite3.Connection, *, user_id: int) -> str:
    row = conn.execute(
        """
        SELECT thread_key
        FROM business_threads
        WHERE user_id = ?
        ORDER BY COALESCE(last_message_at, updated_at) DESC, id DESC
        LIMIT 1
        """,
        (int(user_id),),
    ).fetchone()
    if row and row["thread_key"]:
        return str(row["thread_key"])
    return f"tg:{int(user_id)}"


def list_call_records_with_files_for_cleanup(
    conn: sqlite3.Connection,
    *,
    older_than_hours: int,
    limit: int = 200,
) -> list[Dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT id, file_path, created_at
        FROM call_records
        WHERE file_path IS NOT NULL
          AND file_path <> ''
          AND julianday(created_at) <= julianday('now') - (? / 24.0)
        ORDER BY id ASC
        LIMIT ?
        """,
        (max(1, int(older_than_hours)), max(1, int(limit))),
    )
    return [dict(row) for row in cursor.fetchall()]


def clear_call_record_file_path(conn: sqlite3.Connection, *, call_id: int) -> bool:
    row = conn.execute("SELECT id FROM call_records WHERE id = ? LIMIT 1", (int(call_id),)).fetchone()
    if not row:
        return False
    conn.execute(
        """
        UPDATE call_records
        SET
            file_path = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (int(call_id),),
    )
    conn.commit()
    return True
