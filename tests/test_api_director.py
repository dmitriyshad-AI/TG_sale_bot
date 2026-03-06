import tempfile
import unittest
from pathlib import Path

try:
    from sales_agent.sales_api.main import create_app
    from sales_agent.sales_core import db
    from sales_agent.sales_core.config import Settings
    from tests.test_client_compat import build_test_client

    HAS_DEPS = True
except ModuleNotFoundError:
    HAS_DEPS = False


@unittest.skipUnless(HAS_DEPS, "fastapi dependencies are not installed")
class ApiDirectorTests(unittest.TestCase):
    def _settings(self, db_path: Path) -> Settings:
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
            admin_user="admin",
            admin_pass="secret",
            enable_director_agent=True,
        )

    def test_director_flow(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "director_api.db"
            app = create_app(self._settings(db_path))

            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(conn, channel="telegram", external_id="director-u1")
                db.log_message(conn, user_id, "inbound", "Нужна стратегия ЕГЭ по информатике", {})
            finally:
                conn.close()

            client = build_test_client(app)
            auth = ("admin", "secret")

            unauth = client.get("/admin/director")
            self.assertEqual(unauth.status_code, 401)

            create_plan = client.post(
                "/admin/director/plan",
                auth=auth,
                json={"goal_text": "Верни 10 теплых лидов по ЕГЭ информатика", "max_actions": 10},
            )
            self.assertEqual(create_plan.status_code, 200)
            payload = create_plan.json()
            self.assertTrue(payload["ok"])
            goal_id = int(payload["goal_id"])
            plan_id = int(payload["plan_id"])

            apply_without_approve = client.post(
                f"/admin/director/plans/{plan_id}/apply",
                auth=auth,
                json={},
            )
            self.assertEqual(apply_without_approve.status_code, 409)

            approve = client.post(f"/admin/director/plans/{plan_id}/approve", auth=auth)
            self.assertEqual(approve.status_code, 200)
            self.assertEqual(approve.json()["plan"]["status"], "approved")

            apply = client.post(
                f"/admin/director/plans/{plan_id}/apply",
                auth=auth,
                json={"max_actions": 5},
            )
            self.assertEqual(apply.status_code, 200)
            apply_payload = apply.json()
            self.assertTrue(apply_payload["ok"])
            self.assertEqual(apply_payload["plan"]["status"], "applied")
            self.assertGreaterEqual(len(apply_payload["actions"]), 1)

            goal_detail = client.get(f"/admin/director/goals/{goal_id}", auth=auth)
            self.assertEqual(goal_detail.status_code, 200)
            self.assertGreaterEqual(len(goal_detail.json()["plans"]), 1)

            plan_detail = client.get(f"/admin/director/plans/{plan_id}", auth=auth)
            self.assertEqual(plan_detail.status_code, 200)
            self.assertGreaterEqual(len(plan_detail.json()["actions"]), 1)

            overview = client.get("/admin/director", auth=auth)
            self.assertEqual(overview.status_code, 200)
            self.assertGreaterEqual(len(overview.json()["goals"]), 1)

            ui = client.get("/admin/ui/director", auth=auth)
            self.assertEqual(ui.status_code, 200)
            self.assertIn("Director Agent", ui.text)

            ui_create = client.post(
                "/admin/ui/director/plan",
                auth=auth,
                data={"goal_text": "Верни warm лиды по ОГЭ", "max_actions": 5},
            )
            self.assertIn(ui_create.status_code, {200, 303})

    def test_director_disabled_returns_404(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "director_disabled.db"
            settings = self._settings(db_path)
            settings.enable_director_agent = False
            app = create_app(settings)
            client = build_test_client(app)
            response = client.get("/admin/director", auth=("admin", "secret"))
            self.assertEqual(response.status_code, 404)


if __name__ == "__main__":
    unittest.main()
