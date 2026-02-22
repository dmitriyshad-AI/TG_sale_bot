import io
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError, URLError

import httpx

try:
    from sales_agent.sales_core.config import Settings
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


class _MockAsyncResponse:
    def __init__(self, status_code: int, payload: dict, text: str = "{}") -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        return self._payload


class _MockAsyncClient:
    def __init__(self, response: _MockAsyncResponse) -> None:
        self.response = response

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, *args, **kwargs):
        return self.response


class _MockAsyncClientRaises:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, *args, **kwargs):
        raise httpx.RequestError("network down")


@unittest.skipUnless(HAS_TALLANTO_DEPS, "tallanto dependencies are not installed")
class TallantoClientTests(unittest.TestCase):
    def test_from_settings_uses_mock_mode_env(self) -> None:
        settings = Settings(
            telegram_bot_token="",
            openai_api_key="",
            openai_model="gpt-4.1",
            tallanto_api_url="https://crm.example/api",
            tallanto_api_key="secret",
            brand_default="kmipt",
            database_path=Path("/tmp/sales_agent.db"),
            catalog_path=Path("/tmp/catalog.yaml"),
            knowledge_path=Path("/tmp/knowledge"),
            vector_store_meta_path=Path("/tmp/vector_store.json"),
            openai_vector_store_id="",
            admin_user="",
            admin_pass="",
        )
        with patch.dict("os.environ", {"TALLANTO_MOCK_MODE": "true"}, clear=True):
            client = TallantoClient.from_settings(settings)

        self.assertEqual(client.base_url, "https://crm.example/api")
        self.assertEqual(client.api_key, "secret")
        self.assertTrue(client.mock_mode)

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

    @patch("sales_agent.sales_core.tallanto_client.urlopen")
    def test_set_entry_handles_url_error(self, mock_urlopen) -> None:
        mock_urlopen.side_effect = URLError("connection refused")
        client = TallantoClient(base_url="https://crm.example/api", api_key="key", mock_mode=False)

        result = client.set_entry(module="leads", fields_values={"phone": "+70000000000"})

        self.assertFalse(result.success)
        self.assertIn("connection error", result.error or "")

    @patch("sales_agent.sales_core.tallanto_client.urlopen")
    def test_set_entry_handles_invalid_json_response(self, mock_urlopen) -> None:
        mock_urlopen.return_value = _MockHTTPResponse("not-json")
        client = TallantoClient(base_url="https://crm.example/api", api_key="key", mock_mode=False)

        result = client.set_entry(module="leads", fields_values={"phone": "+70000000000"})

        self.assertFalse(result.success)
        self.assertEqual(result.error, "Tallanto response is not valid JSON.")

    @patch("sales_agent.sales_core.tallanto_client.urlopen")
    def test_set_entry_fails_when_id_not_found(self, mock_urlopen) -> None:
        mock_urlopen.return_value = _MockHTTPResponse('{"success": false, "result": {}}')
        client = TallantoClient(base_url="https://crm.example/api", api_key="key", mock_mode=False)

        result = client.set_entry(module="leads", fields_values={"phone": "+70000000000"})

        self.assertFalse(result.success)
        self.assertEqual(result.error, "Tallanto returned no entry id.")

    def test_create_lead_passes_expected_module(self) -> None:
        client = TallantoClient(base_url="https://crm.example/api", api_key="key", mock_mode=True)
        with patch.object(client, "set_entry", wraps=client.set_entry) as mock_set_entry:
            client.create_lead(phone="+70000000000", brand="kmipt", name="Alice")

            self.assertEqual(mock_set_entry.call_count, 1)
            kwargs = mock_set_entry.call_args.kwargs
            self.assertEqual(kwargs["module"], "leads")
            self.assertEqual(kwargs["fields_values"]["phone"], "+70000000000")
            self.assertEqual(kwargs["fields_values"]["brand"], "kmipt")

    def test_upsert_contact_passes_contact_module_and_id(self) -> None:
        client = TallantoClient(base_url="https://crm.example/api", api_key="key", mock_mode=True)
        with patch.object(client, "set_entry", wraps=client.set_entry) as mock_set_entry:
            client.upsert_contact(phone="+70000000000", name="Alice", email="a@b.c", contact_id="c-1")
        kwargs = mock_set_entry.call_args.kwargs
        self.assertEqual(kwargs["module"], "contacts")
        self.assertEqual(kwargs["id"], "c-1")
        self.assertEqual(kwargs["fields_values"]["email"], "a@b.c")


@unittest.skipUnless(HAS_TALLANTO_DEPS, "tallanto dependencies are not installed")
class TallantoClientAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_set_entry_async_returns_error_when_not_configured(self) -> None:
        client = TallantoClient(base_url="", api_key="", mock_mode=False)
        result = await client.set_entry_async("leads", {"phone": "+70000000000"})
        self.assertFalse(result.success)
        self.assertIn("not configured", result.error or "")

    async def test_set_entry_async_parses_success(self) -> None:
        client = TallantoClient(base_url="https://crm.example/api", api_key="key", mock_mode=False)
        response = _MockAsyncResponse(200, {"success": True, "result": {"id": "async-1"}})
        with patch(
            "sales_agent.sales_core.tallanto_client.httpx.AsyncClient",
            return_value=_MockAsyncClient(response),
        ):
            result = await client.set_entry_async("leads", {"phone": "+70000000000"})
        self.assertTrue(result.success)
        self.assertEqual(result.entry_id, "async-1")

    async def test_create_lead_async_in_mock_mode(self) -> None:
        client = TallantoClient(base_url="https://crm.example/api", api_key="key", mock_mode=True)
        result = await client.create_lead_async(phone="+70000000000", brand="kmipt")
        self.assertTrue(result.success)
        self.assertTrue((result.entry_id or "").startswith("mock-"))

    async def test_set_entry_async_handles_request_error(self) -> None:
        client = TallantoClient(base_url="https://crm.example/api", api_key="key", mock_mode=False)
        with patch(
            "sales_agent.sales_core.tallanto_client.httpx.AsyncClient",
            return_value=_MockAsyncClientRaises(),
        ):
            result = await client.set_entry_async("leads", {"phone": "+70000000000"})
        self.assertFalse(result.success)
        self.assertIn("connection error", result.error or "")

    async def test_set_entry_async_handles_http_error_status(self) -> None:
        client = TallantoClient(base_url="https://crm.example/api", api_key="key", mock_mode=False)
        response = _MockAsyncResponse(503, {}, text="{}")
        with patch(
            "sales_agent.sales_core.tallanto_client.httpx.AsyncClient",
            return_value=_MockAsyncClient(response),
        ):
            result = await client.set_entry_async("leads", {"phone": "+70000000000"})
        self.assertFalse(result.success)
        self.assertIn("HTTP error", result.error or "")

    async def test_set_entry_async_handles_invalid_json(self) -> None:
        class _BadJsonResponse(_MockAsyncResponse):
            def json(self):
                raise ValueError("invalid json")

        client = TallantoClient(base_url="https://crm.example/api", api_key="key", mock_mode=False)
        response = _BadJsonResponse(200, {}, text="{")
        with patch(
            "sales_agent.sales_core.tallanto_client.httpx.AsyncClient",
            return_value=_MockAsyncClient(response),
        ):
            result = await client.set_entry_async("leads", {"phone": "+70000000000"})
        self.assertFalse(result.success)
        self.assertEqual(result.error, "Tallanto response is not valid JSON.")

    async def test_upsert_contact_async_passes_contact_module_and_id(self) -> None:
        client = TallantoClient(base_url="https://crm.example/api", api_key="key", mock_mode=True)
        with patch.object(client, "set_entry_async", wraps=client.set_entry_async) as mock_set_entry:
            await client.upsert_contact_async(
                phone="+70000000000",
                name="Alice",
                email="a@b.c",
                contact_id="c-1",
            )
        kwargs = mock_set_entry.call_args.kwargs
        self.assertEqual(kwargs["module"], "contacts")
        self.assertEqual(kwargs["id"], "c-1")


if __name__ == "__main__":
    unittest.main()
