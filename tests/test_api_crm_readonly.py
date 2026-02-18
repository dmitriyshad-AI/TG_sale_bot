import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    from fastapi.testclient import TestClient

    from sales_agent.sales_api.main import create_app
    from sales_agent.sales_core.config import Settings

    HAS_FASTAPI = True
except ModuleNotFoundError:
    HAS_FASTAPI = False


def _settings(db_path: Path, *, read_only: bool = True) -> Settings:
    return Settings(
        telegram_bot_token="",
        openai_api_key="",
        openai_model="gpt-4.1",
        tallanto_api_url="https://crm.example/api",
        tallanto_api_key="legacy-key",
        tallanto_api_token="token-1",
        tallanto_read_only=read_only,
        tallanto_default_contact_module="contacts",
        brand_default="kmipt",
        database_path=db_path,
        catalog_path=Path("catalog/products.yaml"),
        knowledge_path=Path("knowledge"),
        vector_store_meta_path=Path("data/vector_store.json"),
        openai_vector_store_id="",
        admin_user="admin",
        admin_pass="secret",
    )


@unittest.skipUnless(HAS_FASTAPI, "fastapi dependencies are not installed")
class ApiCrmReadOnlyTests(unittest.TestCase):
    def test_crm_endpoints_require_readonly_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app(_settings(Path(tmpdir) / "app.db", read_only=False))
            client = TestClient(app)
            response = client.get("/api/crm/meta/modules")
        self.assertEqual(response.status_code, 503)
        self.assertIn("read-only mode is disabled", response.json()["detail"].lower())

    def test_modules_endpoint_uses_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app(_settings(Path(tmpdir) / "app.db", read_only=True))
            client = TestClient(app)
            with patch(
                "sales_agent.sales_api.main.TallantoReadOnlyClient.call",
                return_value={"result": [{"module": "contacts"}, {"module": "leads"}]},
            ) as mock_call:
                first = client.get("/api/crm/meta/modules")
                second = client.get("/api/crm/meta/modules")

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertFalse(first.json()["cached"])
        self.assertTrue(second.json()["cached"])
        self.assertEqual(first.json()["items"], ["contacts", "leads"])
        self.assertEqual(mock_call.call_count, 1)

    def test_fields_endpoint_maps_tallanto_401_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app(_settings(Path(tmpdir) / "app.db", read_only=True))
            client = TestClient(app)
            with patch(
                "sales_agent.sales_api.main.TallantoReadOnlyClient.call",
                side_effect=RuntimeError("Tallanto HTTP error: 401"),
            ):
                response = client.get("/api/crm/meta/fields", params={"module": "contacts"})
        self.assertEqual(response.status_code, 401)

    def test_modules_endpoint_maps_tallanto_400_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app(_settings(Path(tmpdir) / "app.db", read_only=True))
            client = TestClient(app)
            with patch(
                "sales_agent.sales_api.main.TallantoReadOnlyClient.call",
                side_effect=RuntimeError("Tallanto HTTP error: 400"),
            ):
                response = client.get("/api/crm/meta/modules")
        self.assertEqual(response.status_code, 400)

    def test_lookup_endpoint_returns_sanitized_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app(_settings(Path(tmpdir) / "app.db", read_only=True))
            client = TestClient(app)
            with patch(
                "sales_agent.sales_api.main.TallantoReadOnlyClient.call",
                return_value={
                    "result": [
                        {
                            "tags": "vip,parent",
                            "interests": ["camp", "physics"],
                            "updated_at": "2026-02-15T10:00:00Z",
                            "phone": "+79990000000",
                        }
                    ]
                },
            ):
                response = client.get(
                    "/api/crm/lookup",
                    params={"module": "contacts", "field": "phone", "value": "+79990000000"},
                )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["found"])
        self.assertEqual(payload["tags"], ["vip", "parent"])
        self.assertEqual(payload["interests"], ["camp", "physics"])
        self.assertNotIn("phone", payload)
        self.assertIsInstance(payload["last_touch_days"], int)

    def test_lookup_endpoint_uses_fallback_when_primary_is_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app(_settings(Path(tmpdir) / "app.db", read_only=True))
            client = TestClient(app)
            with patch(
                "sales_agent.sales_api.main.TallantoReadOnlyClient.call",
                side_effect=[
                    {"result": []},
                    {"result": [{"tags": "retarget", "interests": "ege"}]},
                ],
            ) as mock_call:
                response = client.get(
                    "/api/crm/lookup",
                    params={"module": "contacts", "field": "phone", "value": "+79990000000"},
                )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["found"])
        self.assertTrue(payload["fallback_used"])
        self.assertEqual(payload["tags"], ["retarget"])
        self.assertEqual(payload["interests"], ["ege"])
        self.assertEqual(mock_call.call_count, 2)


if __name__ == "__main__":
    unittest.main()
