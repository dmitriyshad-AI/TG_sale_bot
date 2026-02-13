import io
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

try:
    from sales_agent.sales_core.tallanto_client import TallantoClient

    HAS_TALLANTO_DEPS = True
except ModuleNotFoundError:
    HAS_TALLANTO_DEPS = False


class _MockHTTPResponse:
    def __init__(self, body: str) -> None:
        self._body = body

    def read(self) -> bytes:
        return self._body.encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


@unittest.skipUnless(HAS_TALLANTO_DEPS, "tallanto dependencies are not installed")
class TallantoClientTests(unittest.TestCase):
    def test_set_entry_returns_error_when_not_configured(self) -> None:
        client = TallantoClient(base_url="", api_key="", mock_mode=False)
        result = client.set_entry(module="leads", fields_values={"phone": "+70000000000"})
        self.assertFalse(result.success)
        self.assertIsNone(result.entry_id)
        self.assertIn("not configured", result.error or "")

    def test_set_entry_returns_mock_id_in_mock_mode(self) -> None:
        client = TallantoClient(base_url="https://crm.example/api", api_key="key", mock_mode=True)
        result = client.set_entry(module="leads", fields_values={"phone": "+70000000000"})
        self.assertTrue(result.success)
        self.assertIsNotNone(result.entry_id)
        self.assertTrue((result.entry_id or "").startswith("mock-"))

    @patch("sales_agent.sales_core.tallanto_client.urlopen")
    def test_set_entry_parses_success_json_response(self, mock_urlopen) -> None:
        mock_urlopen.return_value = _MockHTTPResponse('{"success": true, "result": {"id": "abc-123"}}')
        client = TallantoClient(base_url="https://crm.example/api", api_key="key", mock_mode=False)

        result = client.set_entry(module="leads", fields_values={"phone": "+70000000000"})

        self.assertTrue(result.success)
        self.assertEqual(result.entry_id, "abc-123")

    @patch("sales_agent.sales_core.tallanto_client.urlopen")
    def test_set_entry_handles_http_error(self, mock_urlopen) -> None:
        mock_urlopen.side_effect = HTTPError(
            url="https://crm.example/api",
            code=500,
            msg="Internal Server Error",
            hdrs=None,
            fp=io.BytesIO(b""),
        )
        client = TallantoClient(base_url="https://crm.example/api", api_key="key", mock_mode=False)

        result = client.set_entry(module="leads", fields_values={"phone": "+70000000000"})

        self.assertFalse(result.success)
        self.assertIsNone(result.entry_id)
        self.assertIn("HTTP error", result.error or "")

    def test_create_lead_passes_expected_module(self) -> None:
        client = TallantoClient(base_url="https://crm.example/api", api_key="key", mock_mode=True)
        with patch.object(client, "set_entry", wraps=client.set_entry) as mock_set_entry:
            client.create_lead(phone="+70000000000", brand="kmipt", name="Alice")

            self.assertEqual(mock_set_entry.call_count, 1)
            kwargs = mock_set_entry.call_args.kwargs
            self.assertEqual(kwargs["module"], "leads")
            self.assertEqual(kwargs["fields_values"]["phone"], "+70000000000")
            self.assertEqual(kwargs["fields_values"]["brand"], "kmipt")


if __name__ == "__main__":
    unittest.main()
