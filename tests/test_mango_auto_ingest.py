import tempfile
import unittest
from pathlib import Path

from sales_agent.sales_core import db
from sales_agent.sales_core.mango_auto_ingest import (
    event_from_mango_record,
    extract_mango_user_and_thread,
    fetch_mango_poll_events_with_retries,
)
from sales_agent.sales_core.mango_client import MangoCallEvent, MangoClientError


class MangoAutoIngestTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "mango_auto_ingest.db"
        db.init_db(self.db_path)
        self.conn = db.get_connection(self.db_path)

    def tearDown(self) -> None:
        self.conn.close()
        self.tempdir.cleanup()

    def test_extract_mango_user_and_thread_from_payload_user(self) -> None:
        user_id = db.get_or_create_user(self.conn, channel="telegram", external_id="mango-u1")
        event = MangoCallEvent(
            event_id="evt-1",
            call_id="call-1",
            phone="",
            recording_url="https://example.com/rec.mp3",
            transcript_hint="",
            occurred_at="",
            payload={"user_id": user_id, "thread_id": f"tg:{user_id}"},
        )
        resolved_user_id, resolved_thread_id = extract_mango_user_and_thread(self.conn, event=event)
        self.assertEqual(resolved_user_id, user_id)
        self.assertEqual(resolved_thread_id, f"tg:{user_id}")

    def test_extract_mango_user_and_thread_from_biz_thread(self) -> None:
        user_id = db.get_or_create_user(self.conn, channel="telegram_business", external_id="mango-u2")
        db.upsert_business_connection(
            self.conn,
            business_connection_id="bc-mango",
            can_reply=True,
            is_enabled=True,
        )
        thread_key = db.upsert_business_thread(
            self.conn,
            business_connection_id="bc-mango",
            chat_id=99123,
            user_id=user_id,
            direction="inbound",
        )
        event = MangoCallEvent(
            event_id="evt-2",
            call_id="call-2",
            phone="",
            recording_url="https://example.com/rec2.mp3",
            transcript_hint="",
            occurred_at="",
            payload={"thread_id": thread_key},
        )
        resolved_user_id, resolved_thread_id = extract_mango_user_and_thread(self.conn, event=event)
        self.assertEqual(resolved_user_id, user_id)
        self.assertEqual(resolved_thread_id, thread_key)

    def test_extract_mango_user_and_thread_from_phone_lookup(self) -> None:
        user_id = db.get_or_create_user(self.conn, channel="telegram", external_id="mango-u3")
        db.create_lead_record(
            conn=self.conn,
            user_id=user_id,
            status="created",
            contact={"phone": "+7 (999) 123-45-67"},
        )
        event = MangoCallEvent(
            event_id="evt-3",
            call_id="call-3",
            phone="89991234567",
            recording_url="https://example.com/rec3.mp3",
            transcript_hint="",
            occurred_at="",
            payload={},
        )
        resolved_user_id, resolved_thread_id = extract_mango_user_and_thread(self.conn, event=event)
        self.assertEqual(resolved_user_id, user_id)
        self.assertEqual(resolved_thread_id, f"tg:{user_id}")

    def test_extract_mango_user_and_thread_from_payload_phone_and_chat_id(self) -> None:
        user_id = db.get_or_create_user(self.conn, channel="telegram_business", external_id="mango-u4")
        db.create_lead_record(
            conn=self.conn,
            user_id=user_id,
            status="created",
            contact={"phone": "+7 999 777 66 55"},
        )
        db.upsert_business_connection(
            self.conn,
            business_connection_id="bc-mango-chat",
            can_reply=True,
            is_enabled=True,
        )
        thread_key = db.upsert_business_thread(
            self.conn,
            business_connection_id="bc-mango-chat",
            chat_id=123456,
            user_id=user_id,
            direction="inbound",
        )
        event = MangoCallEvent(
            event_id="evt-4",
            call_id="call-4",
            phone="",
            recording_url="https://example.com/rec4.mp3",
            transcript_hint="",
            occurred_at="",
            payload={
                "chat_id": "123456",
                "contact": {"phone": "8 (999) 777-66-55"},
            },
        )
        resolved_user_id, resolved_thread_id = extract_mango_user_and_thread(self.conn, event=event)
        self.assertEqual(resolved_user_id, user_id)
        self.assertEqual(resolved_thread_id, thread_key)

    async def test_fetch_mango_poll_events_with_retries(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.calls = 0

            def list_recent_calls(self, *, since_iso: str, limit: int):
                self.calls += 1
                if self.calls < 3:
                    raise MangoClientError("temporary")
                return [
                    MangoCallEvent(
                        event_id="evt-ok",
                        call_id="call-ok",
                        phone="",
                        recording_url="https://example.com/ok.mp3",
                        transcript_hint="",
                        occurred_at="",
                        payload={},
                    )
                ]

        client = FakeClient()
        events, attempts = await fetch_mango_poll_events_with_retries(
            client=client,  # type: ignore[arg-type]
            since="",
            limit=10,
            attempts=4,
            base_backoff_seconds=0,
        )
        self.assertEqual(attempts, 3)
        self.assertEqual(len(events), 1)

    async def test_fetch_mango_poll_events_with_retries_raises_after_limit(self) -> None:
        class FailingClient:
            def list_recent_calls(self, *, since_iso: str, limit: int):
                _ = since_iso, limit
                raise MangoClientError("permanent")

        with self.assertRaises(MangoClientError):
            await fetch_mango_poll_events_with_retries(
                client=FailingClient(),  # type: ignore[arg-type]
                since="",
                limit=10,
                attempts=2,
                base_backoff_seconds=0,
            )

    def test_event_from_mango_record_parses_or_falls_back(self) -> None:
        parsed = event_from_mango_record(
            {
                "event_id": "evt-10",
                "payload": {
                    "event": "call_record",
                    "call": {"id": "call-10", "recording_url": "https://example.com/r10.mp3"},
                },
            }
        )
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.call_id, "call-10")

        fallback = event_from_mango_record(
            {
                "event_id": "evt-11",
                "call_external_id": "ext-11",
                "payload": {
                    "note": "no parsable structure",
                    "recording_url": "https://cdn.example/fallback.mp3",
                    "contact": {"phone": "+7 (999) 555-44-33"},
                },
            }
        )
        self.assertIsNotNone(fallback)
        assert fallback is not None
        self.assertEqual(fallback.call_id, "ext-11")
        self.assertEqual(fallback.recording_url, "https://cdn.example/fallback.mp3")
        self.assertEqual(fallback.phone, "+7 (999) 555-44-33")

        self.assertIsNone(event_from_mango_record({"payload": {}}))


if __name__ == "__main__":
    unittest.main()
