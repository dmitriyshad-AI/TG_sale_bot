import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from sales_agent.sales_core import db


class DatabaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "test_sales_agent.db"
        db.init_db(self.db_path)
        self.conn = db.get_connection(self.db_path)

    def tearDown(self) -> None:
        self.conn.close()
        self.tempdir.cleanup()

    def test_init_db_creates_required_tables(self) -> None:
        cursor = self.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        table_names = {row["name"] for row in cursor.fetchall()}
        self.assertTrue({"users", "sessions", "messages", "leads"}.issubset(table_names))

    def test_get_or_create_user_is_idempotent(self) -> None:
        first = db.get_or_create_user(
            conn=self.conn,
            channel="telegram",
            external_id="42",
            username="alice",
            first_name="Alice",
            last_name="Doe",
        )
        second = db.get_or_create_user(
            conn=self.conn,
            channel="telegram",
            external_id="42",
            username="alice",
            first_name="Alice",
            last_name="Doe",
        )
        self.assertEqual(first, second)

        cursor = self.conn.execute("SELECT COUNT(*) AS cnt FROM users")
        self.assertEqual(cursor.fetchone()["cnt"], 1)

    def test_log_message_persists_meta_json(self) -> None:
        user_id = db.get_or_create_user(self.conn, "telegram", "99")
        db.log_message(
            conn=self.conn,
            user_id=user_id,
            direction="inbound",
            text="Привет",
            meta={"source": "test", "message_id": 101},
        )

        cursor = self.conn.execute(
            "SELECT direction, text, meta_json FROM messages WHERE user_id = ?",
            (user_id,),
        )
        row = cursor.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["direction"], "inbound")
        self.assertEqual(row["text"], "Привет")
        self.assertEqual(json.loads(row["meta_json"]), {"source": "test", "message_id": 101})

    def test_upsert_session_state_insert_and_update(self) -> None:
        user_id = db.get_or_create_user(self.conn, "telegram", "77")

        db.upsert_session_state(
            conn=self.conn,
            user_id=user_id,
            state={"step": "ask_grade"},
            meta={"source": "site"},
        )
        db.upsert_session_state(
            conn=self.conn,
            user_id=user_id,
            state={"step": "ask_goal", "grade": 8},
            meta={"source": "site", "utm": "cpc"},
        )

        cursor = self.conn.execute(
            "SELECT state_json, meta_json FROM sessions WHERE user_id = ?",
            (user_id,),
        )
        rows = cursor.fetchall()
        self.assertEqual(len(rows), 1)

        row = rows[0]
        self.assertEqual(json.loads(row["state_json"]), {"step": "ask_goal", "grade": 8})
        self.assertEqual(json.loads(row["meta_json"]), {"source": "site", "utm": "cpc"})

    def test_upsert_session_state_keeps_meta_when_not_passed(self) -> None:
        user_id = db.get_or_create_user(self.conn, "telegram", "78")
        db.upsert_session_state(
            conn=self.conn,
            user_id=user_id,
            state={"step": "ask_grade"},
            meta={"source": "site"},
        )
        db.upsert_session_state(
            conn=self.conn,
            user_id=user_id,
            state={"step": "ask_goal"},
            meta=None,
        )

        session = db.get_session(self.conn, user_id)
        self.assertEqual(session["state"], {"step": "ask_goal"})
        self.assertEqual(session["meta"], {"source": "site"})

    def test_db_connection_uses_row_factory(self) -> None:
        cursor = self.conn.execute("SELECT 1 AS one")
        row = cursor.fetchone()
        self.assertIsInstance(row, sqlite3.Row)
        self.assertEqual(row["one"], 1)

    def test_db_connection_enables_foreign_keys(self) -> None:
        row = self.conn.execute("PRAGMA foreign_keys").fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], 1)

    def test_db_connection_sets_busy_timeout(self) -> None:
        row = self.conn.execute("PRAGMA busy_timeout").fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], 5000)

    def test_db_connection_uses_wal_journal_mode(self) -> None:
        row = self.conn.execute("PRAGMA journal_mode").fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(str(row[0]).lower(), "wal")

    def test_create_lead_record_persists_contact_json(self) -> None:
        user_id = db.get_or_create_user(self.conn, "telegram", "555")
        lead_row_id = db.create_lead_record(
            conn=self.conn,
            user_id=user_id,
            status="created",
            tallanto_entry_id="lead-1001",
            contact={"phone": "+79991234567", "brand": "kmipt"},
        )

        self.assertGreater(lead_row_id, 0)
        cursor = self.conn.execute(
            "SELECT status, tallanto_entry_id, contact_json FROM leads WHERE lead_id = ?",
            (lead_row_id,),
        )
        row = cursor.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["status"], "created")
        self.assertEqual(row["tallanto_entry_id"], "lead-1001")
        self.assertEqual(
            json.loads(row["contact_json"]),
            {"phone": "+79991234567", "brand": "kmipt"},
        )

    def test_get_session_returns_empty_when_absent(self) -> None:
        session = db.get_session(self.conn, user_id=999999)
        self.assertEqual(session, {"state": {}, "meta": {}})

    def test_list_recent_leads_returns_joined_user_and_contact(self) -> None:
        user_id = db.get_or_create_user(
            self.conn,
            channel="telegram",
            external_id="701",
            username="lead_user",
            first_name="Lead",
            last_name="User",
        )
        db.create_lead_record(
            conn=self.conn,
            user_id=user_id,
            status="created",
            tallanto_entry_id="tl-1",
            contact={"phone": "+79990000001", "source": "test"},
        )

        leads = db.list_recent_leads(self.conn, limit=10)
        self.assertGreaterEqual(len(leads), 1)
        first = leads[0]
        self.assertEqual(first["user_id"], user_id)
        self.assertEqual(first["contact"]["phone"], "+79990000001")
        self.assertEqual(first["username"], "lead_user")

    def test_list_recent_conversations_and_messages(self) -> None:
        user_id = db.get_or_create_user(self.conn, channel="telegram", external_id="702")
        db.log_message(
            conn=self.conn,
            user_id=user_id,
            direction="inbound",
            text="Привет",
            meta={"m": 1},
        )
        db.log_message(
            conn=self.conn,
            user_id=user_id,
            direction="outbound",
            text="Здравствуйте",
            meta={"m": 2},
        )

        conversations = db.list_recent_conversations(self.conn, limit=10)
        self.assertGreaterEqual(len(conversations), 1)
        self.assertEqual(conversations[0]["user_id"], user_id)
        self.assertEqual(conversations[0]["messages_count"], 2)

        messages = db.list_conversation_messages(self.conn, user_id=user_id, limit=10)
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0]["direction"], "inbound")
        self.assertEqual(messages[1]["direction"], "outbound")
        self.assertEqual(messages[0]["meta"]["m"], 1)

    def test_init_db_migrates_duplicate_sessions_and_enforces_unique_index(self) -> None:
        legacy_path = Path(self.tempdir.name) / "legacy_sessions.db"
        with sqlite3.connect(legacy_path) as conn:
            conn.execute(
                """
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel TEXT NOT NULL,
                    external_id TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    state_json TEXT DEFAULT '{}',
                    meta_json TEXT,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                "INSERT INTO users (channel, external_id) VALUES ('telegram', 'legacy-user')"
            )
            conn.execute(
                "INSERT INTO sessions (user_id, state_json) VALUES (1, '{\"step\":\"ask_grade\"}')"
            )
            conn.execute(
                "INSERT INTO sessions (user_id, state_json) VALUES (1, '{\"step\":\"ask_goal\"}')"
            )
            conn.commit()

        db.init_db(legacy_path)
        conn = db.get_connection(legacy_path)
        try:
            count = conn.execute("SELECT COUNT(*) AS cnt FROM sessions WHERE user_id = 1").fetchone()["cnt"]
            self.assertEqual(count, 1)

            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute(
                    "INSERT INTO sessions (user_id, state_json) VALUES (?, ?)",
                    (1, '{"step":"duplicate"}'),
                )
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
