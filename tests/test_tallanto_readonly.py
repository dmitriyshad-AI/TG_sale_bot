import io
import unittest
from urllib.error import HTTPError
from unittest.mock import patch

from sales_agent.sales_core.tallanto_readonly import (
    TallantoReadOnlyClient,
    normalize_tallanto_fields,
    normalize_tallanto_modules,
    sanitize_tallanto_lookup_context,
)


class _MockHTTPResponse:
    def __init__(self, body: str) -> None:
        self._body = body

    def read(self) -> bytes:
        return self._body.encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class TallantoReadOnlyClientTests(unittest.TestCase):
    def test_call_rejects_write_method(self) -> None:
        client = TallantoReadOnlyClient(base_url="https://crm.example/api", token="token")
        with self.assertRaises(RuntimeError):
            client.call("set_entry", {"module": "leads"})

    def test_call_rejects_unknown_method(self) -> None:
        client = TallantoReadOnlyClient(base_url="https://crm.example/api", token="token")
        with self.assertRaises(RuntimeError):
            client.call("unknown_method", {})

    def test_call_requires_configuration(self) -> None:
        client = TallantoReadOnlyClient(base_url="", token="")
        with self.assertRaises(RuntimeError):
            client.call("list_possible_modules", {})

    @patch("sales_agent.sales_core.tallanto_readonly.urlopen")
    def test_call_sends_allowed_method_and_parses_json(self, mock_urlopen) -> None:
        mock_urlopen.return_value = _MockHTTPResponse('{"result":["contacts","leads"]}')
        client = TallantoReadOnlyClient(base_url="https://crm.example/api", token="token")

        result = client.call("list_possible_modules", {})

        self.assertEqual(result["result"], ["contacts", "leads"])
        sent_request = mock_urlopen.call_args.args[0]
        payload = sent_request.data.decode("utf-8")
        self.assertIn('"method": "list_possible_modules"', payload)
        self.assertIn('"api_key": "token"', payload)

    @patch("sales_agent.sales_core.tallanto_readonly.urlopen")
    def test_call_raises_runtime_error_on_http(self, mock_urlopen) -> None:
        mock_urlopen.side_effect = HTTPError(
            url="https://crm.example/api",
            code=401,
            msg="Unauthorized",
            hdrs=None,
            fp=io.BytesIO(b""),
        )
        client = TallantoReadOnlyClient(base_url="https://crm.example/api", token="token")
        with self.assertRaises(RuntimeError) as exc:
            client.call("list_possible_modules", {})
        self.assertIn("401", str(exc.exception))

    def test_sanitize_lookup_context_extracts_safe_fields(self) -> None:
        context = sanitize_tallanto_lookup_context(
            {
                "result": [
                    {
                        "tags": "vip,retarget",
                        "interests": ["camp", "ege"],
                        "updated_at": "2026-02-15T10:00:00Z",
                        "phone": "+79990000000",
                    }
                ]
            }
        )
        self.assertTrue(context["found"])
        self.assertEqual(context["tags"], ["vip", "retarget"])
        self.assertEqual(context["interests"], ["camp", "ege"])
        self.assertIsInstance(context["last_touch_days"], int)
        self.assertGreaterEqual(context["last_touch_days"], 0)
        self.assertNotIn("phone", context)

    def test_normalizers_extract_values(self) -> None:
        modules = normalize_tallanto_modules({"result": [{"module": "contacts"}, {"name": "leads"}]})
        fields = normalize_tallanto_fields({"result": [{"field": "phone"}, {"name": "email"}]})
        self.assertEqual(modules, ["contacts", "leads"])
        self.assertEqual(fields, ["phone", "email"])


if __name__ == "__main__":
    unittest.main()
