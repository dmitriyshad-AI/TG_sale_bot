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

    def test_verify_fails_when_hash_or_auth_date_missing(self) -> None:
        no_hash = verify_telegram_webapp_init_data(
            init_data="auth_date=1700000000&query_id=q",
            bot_token="123:ABC",
        )
        self.assertFalse(no_hash.ok)
        self.assertEqual(no_hash.reason, "missing_hash")

        no_auth_date = verify_telegram_webapp_init_data(
            init_data="query_id=q&hash=abc",
            bot_token="123:ABC",
        )
        self.assertFalse(no_auth_date.ok)
        self.assertEqual(no_auth_date.reason, "missing_auth_date")

    def test_verify_fails_when_auth_date_invalid_or_in_future(self) -> None:
        invalid_auth = verify_telegram_webapp_init_data(
            init_data="auth_date=notanumber&query_id=q&hash=abc",
            bot_token="123:ABC",
        )
        self.assertFalse(invalid_auth.ok)
        self.assertEqual(invalid_auth.reason, "invalid_auth_date")

        bot_token = "123:ABC"
        now = int(time.time())
        future = now + 600
        init_data = _build_init_data(
            {"auth_date": str(future), "query_id": "AAEAAAE", "user": json.dumps({"id": 101}, ensure_ascii=False)},
            bot_token=bot_token,
        )
        future_result = verify_telegram_webapp_init_data(
            init_data=init_data,
            bot_token=bot_token,
            now_ts=now,
        )
        self.assertFalse(future_result.ok)
        self.assertEqual(future_result.reason, "future_auth_date")

    def test_verify_fails_when_bot_token_missing(self) -> None:
        result = verify_telegram_webapp_init_data(
            init_data="auth_date=1700000000&hash=x",
            bot_token="",
        )
        self.assertFalse(result.ok)
        self.assertEqual(result.reason, "missing_bot_token")

    def test_verify_accepts_payload_with_invalid_user_json(self) -> None:
        bot_token = "123:ABC"
        now = int(time.time())
        init_data = _build_init_data(
            {
                "auth_date": str(now),
                "query_id": "AAEAAAE",
                "user": "not-json",
            },
            bot_token=bot_token,
        )
        result = verify_telegram_webapp_init_data(
            init_data=init_data,
            bot_token=bot_token,
            now_ts=now,
        )
        self.assertTrue(result.ok)
        self.assertIsNone(result.user)
        self.assertIsNone(result.user_id)

    def test_verify_accepts_payload_when_user_id_is_not_int(self) -> None:
        bot_token = "123:ABC"
        now = int(time.time())
        init_data = _build_init_data(
            {
                "auth_date": str(now),
                "query_id": "AAEAAAE",
                "user": json.dumps({"id": "abc", "username": "x"}, ensure_ascii=False),
            },
            bot_token=bot_token,
        )
        result = verify_telegram_webapp_init_data(
            init_data=init_data,
            bot_token=bot_token,
            now_ts=now,
        )
        self.assertTrue(result.ok)
        self.assertIsNone(result.user_id)
        self.assertIsNone(result.user)


if __name__ == "__main__":
    unittest.main()
