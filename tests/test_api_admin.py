import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    from fastapi.testclient import TestClient

    from sales_agent.sales_api.main import create_app
    from sales_agent.sales_core import db
    from sales_agent.sales_core.config import Settings

    HAS_ADMIN_DEPS = True
except ModuleNotFoundError:
    HAS_ADMIN_DEPS = False


@unittest.skipUnless(HAS_ADMIN_DEPS, "fastapi dependencies are not installed")
class ApiAdminTests(unittest.TestCase):
    def _settings(self, db_path: Path, admin_user: str = "admin", admin_pass: str = "secret") -> Settings:
        return Settings(
            telegram_bot_token="",
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
            admin_user=admin_user,
            admin_pass=admin_pass,
        )

    def test_admin_endpoints_require_auth(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(self._settings(db_path))
            client = TestClient(app)

            response = client.get("/admin/leads")
            self.assertEqual(response.status_code, 401)
            response_ui = client.get("/admin")
            self.assertEqual(response_ui.status_code, 401)

    def test_admin_returns_503_when_not_configured(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(self._settings(db_path, admin_user="", admin_pass=""))
            client = TestClient(app)

            response = client.get("/admin/leads", auth=("x", "y"))
            self.assertEqual(response.status_code, 503)

    def test_admin_leads_and_conversations(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(self._settings(db_path))
            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(
                    conn,
                    channel="telegram",
                    external_id="user-1",
                    username="alice",
                    first_name="Alice",
                    last_name="Doe",
                )
                db.create_lead_record(
                    conn=conn,
                    user_id=user_id,
                    status="created",
                    tallanto_entry_id="lead-1",
                    contact={"phone": "+79990000001"},
                )
                db.log_message(conn, user_id=user_id, direction="inbound", text="hi", meta={"k": 1})
            finally:
                conn.close()

            client = TestClient(app)
            auth = ("admin", "secret")

            leads_response = client.get("/admin/leads", auth=auth)
            self.assertEqual(leads_response.status_code, 200)
            leads_items = leads_response.json()["items"]
            self.assertEqual(len(leads_items), 1)
            self.assertEqual(leads_items[0]["contact"]["phone"], "+79990000001")

            conv_response = client.get("/admin/conversations", auth=auth)
            self.assertEqual(conv_response.status_code, 200)
            conv_items = conv_response.json()["items"]
            self.assertEqual(len(conv_items), 1)
            self.assertEqual(conv_items[0]["messages_count"], 1)

            history_response = client.get(f"/admin/conversations/{conv_items[0]['user_id']}", auth=auth)
            self.assertEqual(history_response.status_code, 200)
            history = history_response.json()["messages"]
            self.assertEqual(len(history), 1)
            self.assertEqual(history[0]["text"], "hi")

            dashboard_ui = client.get("/admin", auth=auth)
            self.assertEqual(dashboard_ui.status_code, 200)
            self.assertIn("Sales Agent Admin", dashboard_ui.text)

            leads_ui = client.get("/admin/ui/leads", auth=auth)
            self.assertEqual(leads_ui.status_code, 200)
            self.assertIn("Leads", leads_ui.text)
            self.assertIn("+79990000001", leads_ui.text)

            conv_ui = client.get("/admin/ui/conversations", auth=auth)
            self.assertEqual(conv_ui.status_code, 200)
            self.assertIn("Conversations", conv_ui.text)

            conv_detail_ui = client.get(f"/admin/ui/conversations/{conv_items[0]['user_id']}", auth=auth)
            self.assertEqual(conv_detail_ui.status_code, 200)
            self.assertIn("Conversation #", conv_detail_ui.text)
            self.assertIn("hi", conv_detail_ui.text)

    def test_admin_copilot_import_returns_summary_and_draft(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(self._settings(db_path))
            client = TestClient(app)
            auth = ("admin", "secret")

            payload = (
                "12/02/2026, 10:00 - Клиент: 10 класс, ЕГЭ по математике\n"
                "12/02/2026, 10:05 - Менеджер: Добрый день\n"
            )
            response = client.post(
                "/admin/copilot/import",
                auth=auth,
                files={"file": ("dialog.txt", payload.encode("utf-8"), "text/plain")},
            )
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertIn("summary", data)
            self.assertIn("draft_reply", data)
            self.assertFalse(data["auto_send"])

            form_response = client.get("/admin/ui/copilot", auth=auth)
            self.assertEqual(form_response.status_code, 200)
            self.assertIn("Copilot Import", form_response.text)

            ui_response = client.post(
                "/admin/ui/copilot/import",
                auth=auth,
                files={"file": ("dialog.txt", payload.encode("utf-8"), "text/plain")},
            )
            self.assertEqual(ui_response.status_code, 200)
            self.assertIn("Copilot Result", ui_response.text)
            self.assertIn("Summary", ui_response.text)

    def test_admin_copilot_import_rejects_invalid_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(self._settings(db_path))
            client = TestClient(app)
            auth = ("admin", "secret")

            response = client.post(
                "/admin/copilot/import",
                auth=auth,
                files={"file": ("dialog.json", b"{not-valid-json", "application/json")},
            )
            self.assertEqual(response.status_code, 400)
            self.assertIn("Invalid Telegram JSON", response.json()["detail"])

    def test_admin_copilot_import_rejects_empty_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(self._settings(db_path))
            client = TestClient(app)
            auth = ("admin", "secret")

            response = client.post(
                "/admin/copilot/import",
                auth=auth,
                files={"file": ("dialog.txt", b"", "text/plain")},
            )
            self.assertEqual(response.status_code, 400)
            self.assertIn("empty", response.json()["detail"].lower())

    @patch("sales_agent.sales_api.main.create_tallanto_copilot_task")
    def test_admin_copilot_import_with_create_task(self, mock_create_task) -> None:
        mock_create_task.return_value = type(
            "Result",
            (),
            {"success": True, "entry_id": "task-1", "error": None},
        )()
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "admin.db"
            app = create_app(self._settings(db_path))
            client = TestClient(app)
            auth = ("admin", "secret")

            payload = (
                "12/02/2026, 10:00 - Клиент: 10 класс, ЕГЭ по математике\n"
                "12/02/2026, 10:05 - Менеджер: Добрый день\n"
            )
            response = client.post(
                "/admin/copilot/import?create_task=true",
                auth=auth,
                files={"file": ("dialog.txt", payload.encode("utf-8"), "text/plain")},
            )
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertIn("task", data)
            self.assertTrue(data["task"]["success"])
            self.assertEqual(data["task"]["entry_id"], "task-1")


if __name__ == "__main__":
    unittest.main()
