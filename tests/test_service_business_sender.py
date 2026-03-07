import unittest

from fastapi import HTTPException

from sales_agent.sales_api.services.business_sender import (
    send_business_draft_and_log,
    split_telegram_text_chunks,
)


class _SendError(Exception):
    pass


class BusinessSenderServiceTests(unittest.IsolatedAsyncioTestCase):
    def test_split_telegram_text_chunks_basic(self) -> None:
        self.assertEqual(split_telegram_text_chunks("", max_len=50), [])
        self.assertEqual(split_telegram_text_chunks("  a   b ", max_len=50), ["a b"])

    def test_split_telegram_text_chunks_long_text(self) -> None:
        chunks = split_telegram_text_chunks("one two three four five", max_len=7)
        self.assertGreaterEqual(len(chunks), 3)
        self.assertTrue(all(len(chunk) <= 7 for chunk in chunks))
        self.assertEqual(" ".join(chunks), "one two three four five")

    async def test_send_business_draft_and_log_success(self) -> None:
        log_calls = []
        send_calls = []

        def parse_thread(_: str):
            return ("bc-1", 12345)

        def get_connection(*, conn, business_connection_id):
            _ = conn
            if business_connection_id == "bc-1":
                return {"is_enabled": 1, "can_reply": 1}
            return None

        def send_message(**kwargs):
            send_calls.append(kwargs)
            return {"message_id": 100 + len(send_calls)}

        def log_message(conn, **kwargs):
            _ = conn
            log_calls.append(kwargs)

        result = await send_business_draft_and_log(
            conn=object(),
            draft={"id": 10, "user_id": 7, "thread_id": "biz:bc-1:12345", "draft_text": "hello world"},
            actor="admin:test",
            telegram_bot_token="token",
            parse_business_thread_key=parse_thread,
            get_business_connection=lambda conn, business_connection_id: get_connection(
                conn=conn, business_connection_id=business_connection_id
            ),
            send_business_message=send_message,
            send_error_type=_SendError,
            set_reply_draft_last_error=lambda *args, **kwargs: None,
            create_approval_action=lambda *args, **kwargs: None,
            log_business_message=log_message,
            max_text_chars=4000,
        )

        self.assertEqual(result["transport"], "telegram_business")
        self.assertEqual(result["business_connection_id"], "bc-1")
        self.assertEqual(result["chat_id"], 12345)
        self.assertEqual(result["message_ids"], [101])
        self.assertEqual(result["sent_message_id"], "101")
        self.assertEqual(len(send_calls), 1)
        self.assertEqual(len(log_calls), 1)

    async def test_send_business_draft_and_log_success_multi_chunk(self) -> None:
        sent_chunks = []

        def send_message(**kwargs):
            sent_chunks.append(kwargs["text"])
            return {"message_id": len(sent_chunks)}

        result = await send_business_draft_and_log(
            conn=object(),
            draft={"id": 11, "user_id": 9, "thread_id": "biz:bc-1:1", "draft_text": "a b c d e"},
            actor="admin:test",
            telegram_bot_token="token",
            parse_business_thread_key=lambda _: ("bc-1", 1),
            get_business_connection=lambda conn, business_connection_id: {"is_enabled": True, "can_reply": True},
            send_business_message=send_message,
            send_error_type=_SendError,
            set_reply_draft_last_error=lambda *args, **kwargs: None,
            create_approval_action=lambda *args, **kwargs: None,
            log_business_message=lambda *args, **kwargs: None,
            max_text_chars=3,
        )

        self.assertGreater(len(sent_chunks), 1)
        self.assertEqual(result["sent_message_id"], ",".join(str(i + 1) for i in range(len(sent_chunks))))

    async def test_send_business_draft_and_log_invalid_thread(self) -> None:
        with self.assertRaises(HTTPException) as exc:
            await send_business_draft_and_log(
                conn=object(),
                draft={"id": 1, "user_id": 2, "thread_id": "tg:2", "draft_text": "x"},
                actor="admin",
                telegram_bot_token="token",
                parse_business_thread_key=lambda _: None,
                get_business_connection=lambda *args, **kwargs: None,
                send_business_message=lambda **kwargs: {},
                send_error_type=_SendError,
                set_reply_draft_last_error=lambda *args, **kwargs: None,
                create_approval_action=lambda *args, **kwargs: None,
                log_business_message=lambda *args, **kwargs: None,
                max_text_chars=10,
            )
        self.assertEqual(exc.exception.status_code, 409)

    async def test_send_business_draft_and_log_requires_token(self) -> None:
        with self.assertRaises(HTTPException) as exc:
            await send_business_draft_and_log(
                conn=object(),
                draft={"id": 1, "user_id": 2, "thread_id": "biz:bc:1", "draft_text": "x"},
                actor="admin",
                telegram_bot_token="",
                parse_business_thread_key=lambda _: ("bc", 1),
                get_business_connection=lambda *args, **kwargs: {"is_enabled": True, "can_reply": True},
                send_business_message=lambda **kwargs: {},
                send_error_type=_SendError,
                set_reply_draft_last_error=lambda *args, **kwargs: None,
                create_approval_action=lambda *args, **kwargs: None,
                log_business_message=lambda *args, **kwargs: None,
                max_text_chars=10,
            )
        self.assertEqual(exc.exception.status_code, 503)

    async def test_send_business_draft_and_log_connection_checks(self) -> None:
        with self.assertRaises(HTTPException) as exc_missing:
            await send_business_draft_and_log(
                conn=object(),
                draft={"id": 1, "user_id": 2, "thread_id": "biz:bc:1", "draft_text": "x"},
                actor="admin",
                telegram_bot_token="token",
                parse_business_thread_key=lambda _: ("bc", 1),
                get_business_connection=lambda *args, **kwargs: None,
                send_business_message=lambda **kwargs: {},
                send_error_type=_SendError,
                set_reply_draft_last_error=lambda *args, **kwargs: None,
                create_approval_action=lambda *args, **kwargs: None,
                log_business_message=lambda *args, **kwargs: None,
                max_text_chars=10,
            )
        self.assertEqual(exc_missing.exception.status_code, 404)

        with self.assertRaises(HTTPException) as exc_disabled:
            await send_business_draft_and_log(
                conn=object(),
                draft={"id": 1, "user_id": 2, "thread_id": "biz:bc:1", "draft_text": "x"},
                actor="admin",
                telegram_bot_token="token",
                parse_business_thread_key=lambda _: ("bc", 1),
                get_business_connection=lambda *args, **kwargs: {"is_enabled": False, "can_reply": True},
                send_business_message=lambda **kwargs: {},
                send_error_type=_SendError,
                set_reply_draft_last_error=lambda *args, **kwargs: None,
                create_approval_action=lambda *args, **kwargs: None,
                log_business_message=lambda *args, **kwargs: None,
                max_text_chars=10,
            )
        self.assertEqual(exc_disabled.exception.status_code, 409)

        with self.assertRaises(HTTPException) as exc_no_reply:
            await send_business_draft_and_log(
                conn=object(),
                draft={"id": 1, "user_id": 2, "thread_id": "biz:bc:1", "draft_text": "x"},
                actor="admin",
                telegram_bot_token="token",
                parse_business_thread_key=lambda _: ("bc", 1),
                get_business_connection=lambda *args, **kwargs: {"is_enabled": True, "can_reply": False},
                send_business_message=lambda **kwargs: {},
                send_error_type=_SendError,
                set_reply_draft_last_error=lambda *args, **kwargs: None,
                create_approval_action=lambda *args, **kwargs: None,
                log_business_message=lambda *args, **kwargs: None,
                max_text_chars=10,
            )
        self.assertEqual(exc_no_reply.exception.status_code, 409)

    async def test_send_business_draft_and_log_empty_text_after_normalization(self) -> None:
        with self.assertRaises(HTTPException) as exc:
            await send_business_draft_and_log(
                conn=object(),
                draft={"id": 1, "user_id": 2, "thread_id": "biz:bc:1", "draft_text": "   "},
                actor="admin",
                telegram_bot_token="token",
                parse_business_thread_key=lambda _: ("bc", 1),
                get_business_connection=lambda *args, **kwargs: {"is_enabled": True, "can_reply": True},
                send_business_message=lambda **kwargs: {},
                send_error_type=_SendError,
                set_reply_draft_last_error=lambda *args, **kwargs: None,
                create_approval_action=lambda *args, **kwargs: None,
                log_business_message=lambda *args, **kwargs: None,
                max_text_chars=10,
            )
        self.assertEqual(exc.exception.status_code, 400)

    async def test_send_business_draft_and_log_send_error(self) -> None:
        errors = []
        approval_events = []

        def send_message(**kwargs):
            _ = kwargs
            raise _SendError("boom")

        with self.assertRaises(HTTPException) as exc:
            await send_business_draft_and_log(
                conn=object(),
                draft={"id": 99, "user_id": 77, "thread_id": "biz:bc:1", "draft_text": "hello"},
                actor="admin",
                telegram_bot_token="token",
                parse_business_thread_key=lambda _: ("bc", 1),
                get_business_connection=lambda *args, **kwargs: {"is_enabled": True, "can_reply": True},
                send_business_message=send_message,
                send_error_type=_SendError,
                set_reply_draft_last_error=lambda conn, draft_id, last_error: errors.append((draft_id, last_error)),
                create_approval_action=lambda conn, **kwargs: approval_events.append(kwargs),
                log_business_message=lambda *args, **kwargs: None,
                max_text_chars=10,
            )
        self.assertEqual(exc.exception.status_code, 502)
        self.assertEqual(errors[0][0], 99)
        self.assertIn("boom", errors[0][1])
        self.assertEqual(approval_events[0]["action"], "draft_send_failed")

    async def test_send_business_draft_and_log_partial_delivery_sets_conflict(self) -> None:
        errors = []
        approval_events = []
        calls = {"count": 0}

        def send_message(**kwargs):
            _ = kwargs
            calls["count"] += 1
            if calls["count"] == 1:
                return {"message_id": 501}
            raise _SendError("chunk failed")

        with self.assertRaises(HTTPException) as exc:
            await send_business_draft_and_log(
                conn=object(),
                draft={"id": 100, "user_id": 77, "thread_id": "biz:bc:1", "draft_text": "a b c d e f"},
                actor="admin",
                telegram_bot_token="token",
                parse_business_thread_key=lambda _: ("bc", 1),
                get_business_connection=lambda *args, **kwargs: {"is_enabled": True, "can_reply": True},
                send_business_message=send_message,
                send_error_type=_SendError,
                set_reply_draft_last_error=lambda conn, draft_id, last_error: errors.append((draft_id, last_error)),
                create_approval_action=lambda conn, **kwargs: approval_events.append(kwargs),
                log_business_message=lambda *args, **kwargs: None,
                max_text_chars=3,
            )
        self.assertEqual(exc.exception.status_code, 409)
        self.assertIn("Partial Telegram Business delivery detected", str(exc.exception.detail))
        self.assertEqual(errors[0][0], 100)
        self.assertIn("partial_delivery|sent_message_ids=501|error=chunk failed", errors[0][1])
        self.assertEqual(approval_events[0]["action"], "draft_send_partial")
        self.assertEqual(approval_events[0]["payload"]["sent_message_ids"], [501])


if __name__ == "__main__":
    unittest.main()
