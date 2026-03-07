import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    from sales_agent.sales_api.main import create_app
    from sales_agent.sales_core import db
    from sales_agent.sales_core.config import Settings
    from tests.test_client_compat import build_test_client

    HAS_DEPS = True
except ModuleNotFoundError:
    HAS_DEPS = False


@unittest.skipUnless(HAS_DEPS, "fastapi dependencies are not installed")
class ApiFaqLabTests(unittest.TestCase):
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
            enable_faq_lab=True,
            faq_lab_scheduler_enabled=False,
            faq_lab_interval_seconds=86400,
            faq_lab_window_days=30,
            faq_lab_min_question_count=1,
            faq_lab_max_items_per_run=50,
        )

    def test_admin_faq_lab_flow(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "faq_api.db"
            app = create_app(self._settings(db_path))

            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(conn, channel="telegram", external_id="faq-api-user")
                db.log_message(conn, user_id, "inbound", "Как поступить в МФТИ?", {})
            finally:
                conn.close()

            client = build_test_client(app)
            auth = ("admin", "secret")

            unauth = client.get("/admin/faq-lab")
            self.assertEqual(unauth.status_code, 401)

            run_resp = client.post("/admin/faq-lab/run", auth=auth, json={"limit": 20})
            self.assertEqual(run_resp.status_code, 200)
            self.assertTrue(run_resp.json()["ok"])

            snapshot = client.get("/admin/faq-lab", auth=auth, params={"refresh": "true", "limit": 20})
            self.assertEqual(snapshot.status_code, 200)
            payload = snapshot.json()
            self.assertTrue(payload["ok"])
            self.assertGreaterEqual(payload["metrics"]["candidate_count"], 1)

            candidate_id = int(payload["candidates"][0]["id"])
            promote_resp = client.post(
                f"/admin/faq-lab/candidates/{candidate_id}/promote",
                auth=auth,
                json={"answer_text": "Сначала уточняем цель и предмет, затем строим план."},
            )
            self.assertEqual(promote_resp.status_code, 200)
            self.assertTrue(promote_resp.json()["ok"])

            conn = db.get_connection(db_path)
            try:
                thread_id = f"tg:{user_id}"
                rejected_id = db.create_reply_draft(
                    conn,
                    user_id=user_id,
                    thread_id=thread_id,
                    draft_text="Отклоненный вариант",
                    model_name="faq_lab_v1",
                )
                db.update_reply_draft_status(
                    conn,
                    draft_id=rejected_id,
                    status="rejected",
                    actor="moderator",
                )
            finally:
                conn.close()

            ui_resp = client.get("/admin/ui/faq-lab", auth=auth)
            self.assertEqual(ui_resp.status_code, 200)
            self.assertIn("FAQ Lab", ui_resp.text)
            self.assertIn("Top New Questions", ui_resp.text)

            ui_run = client.post("/admin/ui/faq-lab/run", auth=auth, data={"limit": 10})
            self.assertIn(ui_run.status_code, {200, 303})

            ui_promote = client.post(
                f"/admin/ui/faq-lab/candidates/{candidate_id}/promote",
                auth=auth,
                data={"answer_text": "Уточняем цель, класс и предмет."},
            )
            self.assertIn(ui_promote.status_code, {200, 303})

    def test_faq_lab_disabled_returns_404(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "faq_api_disabled.db"
            settings = self._settings(db_path)
            settings.enable_faq_lab = False
            app = create_app(settings)
            client = build_test_client(app)

            response = client.get("/admin/faq-lab", auth=("admin", "secret"))
            self.assertEqual(response.status_code, 404)

    def test_promote_candidate_not_found_returns_404(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "faq_api_not_found.db"
            app = create_app(self._settings(db_path))
            client = build_test_client(app)
            auth = ("admin", "secret")

            response = client.post(
                "/admin/faq-lab/candidates/9999/promote",
                auth=auth,
                json={"answer_text": "Уточняем цель ученика и даем персональный план следующего шага."},
            )
            self.assertEqual(response.status_code, 404)

            ui_response = client.post(
                "/admin/ui/faq-lab/candidates/9999/promote",
                auth=auth,
                data={"answer_text": "Уточняем цель ученика и даем персональный план следующего шага."},
            )
            self.assertEqual(ui_response.status_code, 404)

    def test_promote_candidate_handles_internal_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "faq_api_internal.db"
            app = create_app(self._settings(db_path))

            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(conn, channel="telegram", external_id="faq-api-internal")
                db.log_message(conn, user_id, "inbound", "Как поступить в МФТИ?", {})
            finally:
                conn.close()

            client = build_test_client(app)
            auth = ("admin", "secret")
            snapshot = client.get("/admin/faq-lab", auth=auth, params={"refresh": "true"})
            candidate_id = int(snapshot.json()["candidates"][0]["id"])

            with patch(
                "sales_agent.sales_api.routers.faq_lab.promote_faq_candidate_to_canonical",
                return_value=None,
            ):
                response = client.post(
                    f"/admin/faq-lab/candidates/{candidate_id}/promote",
                    auth=auth,
                    json={"answer_text": "Уточняем цель ученика и даем персональный план следующего шага."},
                )
            self.assertEqual(response.status_code, 500)

    def test_promote_candidate_rejects_too_short_answer(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "faq_api_short_answer.db"
            app = create_app(self._settings(db_path))

            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(conn, channel="telegram", external_id="faq-api-short")
                db.log_message(conn, user_id, "inbound", "Как поступить в МФТИ?", {})
            finally:
                conn.close()

            client = build_test_client(app)
            auth = ("admin", "secret")
            snapshot = client.get("/admin/faq-lab", auth=auth, params={"refresh": "true"})
            candidate_id = int(snapshot.json()["candidates"][0]["id"])

            response = client.post(
                f"/admin/faq-lab/candidates/{candidate_id}/promote",
                auth=auth,
                json={"answer_text": "слишком коротко"},
            )
            self.assertEqual(response.status_code, 422)

    def test_faq_lab_scheduler_runs_on_startup(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "faq_scheduler.db"
            settings = self._settings(db_path)
            settings.faq_lab_scheduler_enabled = True
            settings.faq_lab_interval_seconds = 300
            app = create_app(settings)

            with patch(
                "sales_agent.sales_api.main.faq_lab_service.refresh_faq_lab",
                return_value={
                    "ok": True,
                    "trigger": "scheduler",
                    "candidates_upserted": 0,
                    "canonical_synced": 0,
                },
            ) as refresh_mock:
                with build_test_client(app) as client:
                    response = client.get("/api/health")
                    self.assertEqual(response.status_code, 200)
                    time.sleep(0.05)

            self.assertGreaterEqual(refresh_mock.call_count, 1)

    def test_faq_lab_ui_post_requires_origin_when_csrf_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "faq_csrf.db"
            settings = self._settings(db_path)
            settings.admin_ui_csrf_enabled = True
            app = create_app(settings)

            conn = db.get_connection(db_path)
            try:
                user_id = db.get_or_create_user(conn, channel="telegram", external_id="faq-csrf-user")
                db.log_message(conn, user_id, "inbound", "Как поступить в МФТИ?", {})
            finally:
                conn.close()

            client = build_test_client(app)
            auth = ("admin", "secret")
            no_origin = client.post("/admin/ui/faq-lab/run", auth=auth, data={"limit": 10})
            self.assertEqual(no_origin.status_code, 403)

            with_origin = client.post(
                "/admin/ui/faq-lab/run",
                auth=auth,
                data={"limit": 10},
                headers={"Origin": "http://testserver"},
            )
            self.assertIn(with_origin.status_code, {200, 303})


if __name__ == "__main__":
    unittest.main()
