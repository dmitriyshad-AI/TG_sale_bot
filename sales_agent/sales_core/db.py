import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional


CREATE_STATEMENTS = [
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
        FOREIGN KEY(user_id) REFERENCES users(id)
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
]


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        for stmt in CREATE_STATEMENTS:
            conn.execute(stmt)
        conn.commit()


def get_connection(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
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
    cursor = conn.execute(
        "SELECT id FROM sessions WHERE user_id = ?",
        (user_id,),
    )
    row = cursor.fetchone()
    if row:
        conn.execute(
            """
            UPDATE sessions
            SET state_json = ?, meta_json = ?, updated_at = CURRENT_TIMESTAMP
            WHERE user_id = ?
            """,
            (state_json, meta_json, user_id),
        )
    else:
        conn.execute(
            """
            INSERT INTO sessions (user_id, state_json, meta_json)
            VALUES (?, ?, ?)
            """,
            (user_id, state_json, meta_json),
        )
    conn.commit()

