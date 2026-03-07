import io
import json
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from sales_agent.sales_core.telegram_business_sender import (
    TelegramBusinessSendError,
    send_business_message,
)


class _FakeResponse:
    def __init__(self, payload: dict, status: int = 200) -> None:
        self._payload = json.dumps(payload).encode("utf-8")
        self._status = status

    def getcode(self) -> int:
        return self._status

    def read(self) -> bytes:
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class TelegramBusinessSenderTests(unittest.TestCase):
    def test_send_business_message_success(self) -> None:
        payload = {"ok": True, "result": {"message_id": 321, "date": 1700000000}}
        with patch("sales_agent.sales_core.telegram_business_sender.request.urlopen", return_value=_FakeResponse(payload)):
            result = send_business_message(
                bot_token="token",
                business_connection_id="bc-1",
                chat_id=123,
                text="hello",
            )
        self.assertEqual(result["message_id"], 321)

    def test_send_business_message_rejects_empty_input(self) -> None:
        with self.assertRaises(ValueError):
            send_business_message(bot_token="", business_connection_id="bc-1", chat_id=1, text="x")
        with self.assertRaises(ValueError):
            send_business_message(bot_token="t", business_connection_id="", chat_id=1, text="x")
        with self.assertRaises(ValueError):
            send_business_message(bot_token="t", business_connection_id="bc-1", chat_id=1, text="  ")

    def test_send_business_message_handles_api_error(self) -> None:
        payload = {"ok": False, "description": "Bad Request: test"}
        with patch("sales_agent.sales_core.telegram_business_sender.request.urlopen", return_value=_FakeResponse(payload)):
            with self.assertRaises(TelegramBusinessSendError) as ctx:
                send_business_message(
                    bot_token="token",
                    business_connection_id="bc-1",
                    chat_id=123,
                    text="hello",
                )
        self.assertIn("Bad Request", str(ctx.exception))

    def test_send_business_message_handles_http_error(self) -> None:
        err = HTTPError(
            url="https://api.telegram.org/botTOKEN/sendMessage",
            code=403,
            msg="Forbidden",
            hdrs=None,
            fp=io.BytesIO(b'{"ok":false,"description":"Forbidden"}'),
        )
        with patch("sales_agent.sales_core.telegram_business_sender.request.urlopen", side_effect=err):
            with self.assertRaises(TelegramBusinessSendError) as ctx:
                send_business_message(
                    bot_token="token",
                    business_connection_id="bc-1",
                    chat_id=123,
                    text="hello",
                )
        self.assertIn("HTTP error: 403", str(ctx.exception))

    def test_send_business_message_handles_invalid_json(self) -> None:
        class _BadJsonResponse(_FakeResponse):
            def read(self) -> bytes:
                return b"{bad-json"

        with patch(
            "sales_agent.sales_core.telegram_business_sender.request.urlopen",
            return_value=_BadJsonResponse({"ok": True, "result": {"message_id": 1}}),
        ):
            with self.assertRaises(TelegramBusinessSendError) as ctx:
                send_business_message(
                    bot_token="token",
                    business_connection_id="bc-1",
                    chat_id=123,
                    text="hello",
                )
        self.assertIn("invalid JSON", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
