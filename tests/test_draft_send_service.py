import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict

from fastapi import HTTPException

from sales_agent.sales_api.services.draft_send import send_approved_draft
from sales_agent.sales_core import db


class DraftSendServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "draft_send_service.db"
        db.init_db(self.db_path)
        self.conn = db.get_connection(self.db_path)

    def tearDown(self) -> None:
        self.conn.close()
        self.tempdir.cleanup()

    def _create_approved_draft(self, *, thread_id: str = "tg:1") -> int:
        user_id = db.get_or_create_user(self.conn, channel="telegram", external_id="svc-user")
        draft_id = db.create_reply_draft(
            self.conn,
            user_id=user_id,
            thread_id=thread_id,
            draft_text="Тестовый черновик",
            model_name="gpt-test",
            created_by="test",
        )
        db.update_reply_draft_status(self.conn, draft_id=draft_id, status="approved", actor="test")
        return draft_id

    async def test_raises_404_for_missing_draft(self) -> None:
        with self.assertRaises(HTTPException) as context:
            await send_approved_draft(
                self.conn,
                draft_id=9999,
                actor="admin",
                manual_sent_message_id="",
                is_business_thread=lambda _thread_id: False,
                business_sender=lambda _draft, _actor: self._unreachable_sender(),
            )
        self.assertEqual(context.exception.status_code, 404)

    async def test_returns_already_sent_without_changes(self) -> None:
        draft_id = self._create_approved_draft(thread_id="tg:2")
        db.update_reply_draft_status(self.conn, draft_id=draft_id, status="sent", actor="admin", sent_message_id="100")

        result = await send_approved_draft(
            self.conn,
            draft_id=draft_id,
            actor="admin",
            manual_sent_message_id="",
            is_business_thread=lambda _thread_id: False,
            business_sender=lambda _draft, _actor: self._unreachable_sender(),
        )
        self.assertTrue(result.already_sent)
        self.assertEqual(result.draft["status"], "sent")

    async def test_non_business_requires_manual_message_id(self) -> None:
        draft_id = self._create_approved_draft(thread_id="tg:3")

        with self.assertRaises(HTTPException) as context:
            await send_approved_draft(
                self.conn,
                draft_id=draft_id,
                actor="admin",
                manual_sent_message_id="",
                is_business_thread=lambda _thread_id: False,
                business_sender=lambda _draft, _actor: self._unreachable_sender(),
            )
        self.assertEqual(context.exception.status_code, 409)

        draft = db.get_reply_draft(self.conn, draft_id)
        self.assertIsNotNone(draft)
        assert draft is not None
        self.assertEqual(draft["status"], "approved")
        self.assertEqual(draft["last_error"], "manual_confirmation_required")

    async def test_non_business_manual_confirmation_marks_sent(self) -> None:
        draft_id = self._create_approved_draft(thread_id="tg:4")
        result = await send_approved_draft(
            self.conn,
            draft_id=draft_id,
            actor="admin",
            manual_sent_message_id="manual-777",
            is_business_thread=lambda _thread_id: False,
            business_sender=lambda _draft, _actor: self._unreachable_sender(),
        )
        self.assertFalse(result.already_sent)
        self.assertEqual(result.draft["status"], "sent")
        self.assertEqual(result.draft["sent_message_id"], "manual-777")
        self.assertEqual(result.delivery["transport"], "manual_confirmed")

    async def test_business_sender_success_marks_sent(self) -> None:
        draft_id = self._create_approved_draft(thread_id="biz:bc-test:123")

        async def sender(_draft: Dict[str, Any], _actor: str) -> Dict[str, Any]:
            return {"transport": "telegram_business", "sent_message_id": "500", "message_ids": [500]}

        result = await send_approved_draft(
            self.conn,
            draft_id=draft_id,
            actor="admin",
            manual_sent_message_id="",
            is_business_thread=lambda _thread_id: True,
            business_sender=sender,
        )
        self.assertEqual(result.draft["status"], "sent")
        self.assertEqual(result.draft["sent_message_id"], "500")
        self.assertEqual(result.delivery["transport"], "telegram_business")

    async def test_business_sender_failure_restores_approved_status(self) -> None:
        draft_id = self._create_approved_draft(thread_id="biz:bc-test:124")

        async def sender(_draft: Dict[str, Any], _actor: str) -> Dict[str, Any]:
            raise HTTPException(status_code=502, detail="mock send failed")

        with self.assertRaises(HTTPException) as context:
            await send_approved_draft(
                self.conn,
                draft_id=draft_id,
                actor="admin",
                manual_sent_message_id="",
                is_business_thread=lambda _thread_id: True,
                business_sender=sender,
            )
        self.assertEqual(context.exception.status_code, 502)
        draft = db.get_reply_draft(self.conn, draft_id)
        self.assertIsNotNone(draft)
        assert draft is not None
        self.assertEqual(draft["status"], "approved")
        self.assertIn("mock send failed", str(draft["last_error"]))

    async def _unreachable_sender(self) -> Dict[str, Any]:
        raise AssertionError("business sender should not be called in this scenario")


if __name__ == "__main__":
    unittest.main()

