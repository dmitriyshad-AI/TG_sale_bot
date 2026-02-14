import hashlib
import hmac
import json
import time
import unittest
from urllib.parse import urlencode

from sales_agent.sales_core.telegram_webapp import parse_init_data, verify_telegram_webapp_init_data


def _build_init_data(payload: dict, bot_token: str) -> str:
    data = {key: value for key, value in payload.items() if key != "hash"}
    check_lines = [f"{key}={value}" for key, value in sorted(data.items())]
    data_check_string = "\n".join(check_lines)
    secret_key = hmac.new(b"WebAppData", bot_token.encode("utf-8"), hashlib.sha256).digest()
    digest = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    data["hash"] = digest
    return urlencode(data)


class TelegramWebAppTests(unittest.TestCase):
    def test_parse_init_data_extracts_pairs(self) -> None:
        parsed = parse_init_data("auth_date=1700000000&query_id=q1")
        self.assertEqual(parsed["auth_date"], "1700000000")
        self.assertEqual(parsed["query_id"], "q1")

    def test_verify_success_for_valid_payload(self) -> None:
        bot_token = "123:ABC"
        now = int(time.time())
        user_json = json.dumps({"id": 101, "username": "admin101"}, ensure_ascii=False)
        init_data = _build_init_data(
            {
                "auth_date": str(now),
                "query_id": "AAEAAAE",
                "user": user_json,
            },
            bot_token=bot_token,
        )
        result = verify_telegram_webapp_init_data(
            init_data=init_data,
            bot_token=bot_token,
            now_ts=now,
        )
        self.assertTrue(result.ok)
        self.assertIsNone(result.reason)
        self.assertEqual(result.user_id, 101)
        self.assertEqual(result.user["username"], "admin101")

    def test_verify_fails_on_invalid_hash(self) -> None:
        bot_token = "123:ABC"
        now = int(time.time())
        init_data = (
            f"auth_date={now}&query_id=AAEAAAE&user=%7B%22id%22%3A101%7D&hash=invalid"
        )
        result = verify_telegram_webapp_init_data(
            init_data=init_data,
            bot_token=bot_token,
            now_ts=now,
        )
        self.assertFalse(result.ok)
        self.assertEqual(result.reason, "invalid_hash")

    def test_verify_fails_on_expired_auth_date(self) -> None:
        bot_token = "123:ABC"
        now = int(time.time())
        old = now - 200_000
        init_data = _build_init_data(
            {
                "auth_date": str(old),
                "query_id": "AAEAAAE",
                "user": json.dumps({"id": 101}, ensure_ascii=False),
            },
            bot_token=bot_token,
        )
        result = verify_telegram_webapp_init_data(
            init_data=init_data,
            bot_token=bot_token,
            now_ts=now,
            max_age_seconds=86_400,
        )
        self.assertFalse(result.ok)
        self.assertEqual(result.reason, "expired_auth_date")

    def test_verify_fails_when_bot_token_missing(self) -> None:
        result = verify_telegram_webapp_init_data(
            init_data="auth_date=1700000000&hash=x",
            bot_token="",
        )
        self.assertFalse(result.ok)
        self.assertEqual(result.reason, "missing_bot_token")


if __name__ == "__main__":
    unittest.main()
