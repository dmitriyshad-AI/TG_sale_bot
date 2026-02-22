import unittest
from types import SimpleNamespace

try:
    from sales_agent.sales_api.main import (
        AssistantCriteriaPayload,
        _assistant_mode,
        _criteria_from_payload,
        _extract_bearer_token,
        _extract_tg_init_data,
        _missing_criteria_fields,
        _request_client_ip,
        _request_id_from_request,
        _safe_user_payload,
    )
    from sales_agent.sales_core.catalog import SearchCriteria

    HAS_API = True
except ModuleNotFoundError:
    HAS_API = False


@unittest.skipUnless(HAS_API, "api dependencies are not installed")
class ApiHelpersTests(unittest.TestCase):
    def test_assistant_mode_routes_questions(self) -> None:
        criteria = SearchCriteria(brand="kmipt", grade=None, goal=None, subject=None, format=None)
        self.assertEqual(_assistant_mode("Какие документы нужны для договора?", criteria), "knowledge")
        self.assertEqual(_assistant_mode("Как поступить в МФТИ в 10 классе?", criteria), "consultative")
        self.assertEqual(_assistant_mode("Что такое косинус?", criteria), "general")

    def test_criteria_from_payload_normalizes_values(self) -> None:
        payload = AssistantCriteriaPayload(
            brand="  KMIPT ",
            grade=10,
            goal=" EGE ",
            subject=" Physics ",
            format=" ONLINE ",
        )
        criteria = _criteria_from_payload(payload, brand_default="kmipt")
        self.assertEqual(criteria.brand, "kmipt")
        self.assertEqual(criteria.grade, 10)
        self.assertEqual(criteria.goal, "ege")
        self.assertEqual(criteria.subject, "physics")
        self.assertEqual(criteria.format, "online")

    def test_missing_criteria_fields_lists_required_keys(self) -> None:
        criteria = SearchCriteria(brand="kmipt", grade=None, goal=None, subject=None, format=None)
        self.assertEqual(_missing_criteria_fields(criteria), ["grade", "goal", "subject", "format"])

    def test_extract_tg_init_data_prefers_direct_header(self) -> None:
        request = SimpleNamespace(
            headers={
                "X-Tg-Init-Data": "direct",
                "X-Telegram-Init-Data": "legacy",
                "Authorization": "tma auth-value",
            }
        )
        self.assertEqual(_extract_tg_init_data(request), "direct")

    def test_extract_tg_init_data_uses_legacy_and_auth(self) -> None:
        request_legacy = SimpleNamespace(headers={"X-Telegram-Init-Data": "legacy"})
        request_auth = SimpleNamespace(headers={"Authorization": "tma auth-value"})
        request_empty = SimpleNamespace(headers={})
        self.assertEqual(_extract_tg_init_data(request_legacy), "legacy")
        self.assertEqual(_extract_tg_init_data(request_auth), "auth-value")
        self.assertEqual(_extract_tg_init_data(request_empty), "")

    def test_safe_user_payload_handles_non_dict(self) -> None:
        self.assertEqual(_safe_user_payload(None), {})
        payload = _safe_user_payload({"id": 42, "first_name": "Ivan"})
        self.assertEqual(payload["id"], 42)
        self.assertEqual(payload["first_name"], "Ivan")

    def test_request_id_and_bearer_and_client_ip(self) -> None:
        request_with_id = SimpleNamespace(state=SimpleNamespace(request_id="abc123"))
        request_without_id = SimpleNamespace(state=SimpleNamespace(request_id=""))
        self.assertEqual(_request_id_from_request(request_with_id), "abc123")
        self.assertEqual(_request_id_from_request(request_without_id), "unknown")

        request_bearer = SimpleNamespace(headers={"Authorization": "Bearer token-1"})
        request_no_bearer = SimpleNamespace(headers={"Authorization": "Basic x"})
        self.assertEqual(_extract_bearer_token(request_bearer), "token-1")
        self.assertEqual(_extract_bearer_token(request_no_bearer), "")

        request_forwarded = SimpleNamespace(
            headers={"X-Forwarded-For": "203.0.113.10, 10.0.0.1"},
            client=SimpleNamespace(host="127.0.0.1"),
        )
        request_client = SimpleNamespace(headers={}, client=SimpleNamespace(host="127.0.0.2"))
        request_unknown = SimpleNamespace(headers={}, client=None)
        self.assertEqual(_request_client_ip(request_forwarded), "203.0.113.10")
        self.assertEqual(_request_client_ip(request_client), "127.0.0.2")
        self.assertEqual(_request_client_ip(request_unknown), "unknown")


if __name__ == "__main__":
    unittest.main()
