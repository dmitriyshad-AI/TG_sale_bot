import hashlib
import hmac
import json
import tempfile
import time
import unittest
from pathlib import Path
from urllib.parse import urlencode

try:
    from fastapi.testclient import TestClient

    from sales_agent.sales_api.main import create_app
    from sales_agent.sales_core import db
    from sales_agent.sales_core.config import Settings

    HAS_MINIAPP_DEPS = True
except ModuleNotFoundError:
    HAS_MINIAPP_DEPS = False


def _build_init_data(payload: dict, bot_token: str) -> str:
    data = {key: value for key, value in payload.items() if key != "hash"}
    check_lines = [f"{key}={value}" for key, value in sorted(data.items())]
    data_check_string = "\n".join(check_lines)
    secret_key = hmac.new(b"WebAppData", bot_token.encode("utf-8"), hashlib.sha256).digest()
    digest = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    data["hash"] = digest
    return urlencode(data)


@unittest.skipUnless(HAS_MINIAPP_DEPS, "fastapi dependencies are not installed")
class ApiMiniAppTests(unittest.TestCase):
    def _settings(self, db_path: Path, *, enabled: bool = True) -> Settings:
        return Settings(
            telegram_bot_token="123:ABC",
            openai_api_key="",
            openai_model="gpt-4.1",
            tallanto_api_url="",
            tallanto_api_key="",
            brand_default="kmipt",
            database_path=db_path,
            catalog_path=Path("catalog/products.yaml"),
            knowledge_path=Path("knowledge"),
            vector_store_meta_path=Path("data/vector_store.json"),
            openai_vector_store_id="",
            admin_user="admin",
            admin_pass="secret",
            admin_miniapp_enabled=enabled,
            admin_telegram_ids=(101,),
            admin_webapp_url="https://example.com/admin/miniapp",
        )

    def _headers(self, user_id: int = 101, bot_token: str = "123:ABC") -> dict:
        now = int(time.time())
        init_data = _build_init_data(
            {
                "auth_date": str(now),
                "query_id": "AAEAAAE",
                "user": json.dumps({"id": user_id, "username": "admin101"}, ensure_ascii=False),
            },
            bot_token=bot_token,
        )
        return {"X-Telegram-Init-Data": init_data}

    def test_miniapp_page_disabled_returns_404(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app(self._settings(Path(tmpdir) / "miniapp.db", enabled=False))
            client = TestClient(app)
            response = client.get("/admin/miniapp")
            self.assertEqual(response.status_code, 404)

    def test_miniapp_page_enabled_returns_html(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app(self._settings(Path(tmpdir) / "miniapp.db", enabled=True))
            client = TestClient(app)
            response = client.get("/admin/miniapp")
            self.assertEqual(response.status_code, 200)
            self.assertIn("Admin Mini App", response.text)

    def test_miniapp_api_requires_init_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app(self._settings(Path(tmpdir) / "miniapp.db", enabled=True))
            client = TestClient(app)
            response = client.get("/admin/miniapp/api/me")
            self.assertEqual(response.status_code, 401)

    def test_miniapp_api_rejects_user_not_in_allowlist(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app(self._settings(Path(tmpdir) / "miniapp.db", enabled=True))
            client = TestClient(app)
            response = client.get("/admin/miniapp/api/me", headers=self._headers(user_id=777))
            self.assertEqual(response.status_code, 403)

    def test_miniapp_api_returns_data_for_allowed_admin(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "miniapp.db"
            app = create_app(self._settings(db_path, enabled=True))
            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(
                    conn,
                    channel="telegram",
                    external_id="u-1",
                    username="manager",
                    first_name="Manager",
                    last_name="One",
                )
                db.create_lead_record(
                    conn=conn,
                    user_id=user_id,
                    status="created",
                    tallanto_entry_id="lead-1",
                    contact={"phone": "+79990000001", "source": "telegram"},
                )
                db.log_message(conn, user_id=user_id, direction="inbound", text="hello", meta={"k": 1})
            finally:
                conn.close()

            client = TestClient(app)
            headers = self._headers(user_id=101)

            me_response = client.get("/admin/miniapp/api/me", headers=headers)
            self.assertEqual(me_response.status_code, 200)
            self.assertEqual(me_response.json()["user_id"], 101)

            leads_response = client.get("/admin/miniapp/api/leads", headers=headers)
            self.assertEqual(leads_response.status_code, 200)
            self.assertEqual(len(leads_response.json()["items"]), 1)

            conv_response = client.get("/admin/miniapp/api/conversations", headers=headers)
            self.assertEqual(conv_response.status_code, 200)
            items = conv_response.json()["items"]
            self.assertEqual(len(items), 1)
            target_user_id = int(items[0]["user_id"])

            history_response = client.get(f"/admin/miniapp/api/conversations/{target_user_id}", headers=headers)
            self.assertEqual(history_response.status_code, 200)
            self.assertEqual(len(history_response.json()["messages"]), 1)


if __name__ == "__main__":
    unittest.main()
