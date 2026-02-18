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
