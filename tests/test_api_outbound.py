import io
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
class ApiOutboundTests(unittest.TestCase):
    def _settings(self, db_path: Path, *, enabled: bool = True) -> Settings:
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
            enable_outbound_copilot=enabled,
        )

    def test_outbound_requires_auth_and_can_be_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "outbound.db"
            app = create_app(self._settings(db_path, enabled=True))
            client = build_test_client(app)
            unauth = client.get("/admin/outbound")
            self.assertEqual(unauth.status_code, 401)

            disabled_app = create_app(self._settings(Path(tmpdir) / "outbound_disabled.db", enabled=False))
            disabled_client = build_test_client(disabled_app)
            disabled = disabled_client.get("/admin/outbound", auth=("admin", "secret"))
            self.assertEqual(disabled.status_code, 404)

    def test_outbound_api_flow(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "outbound_api.db"
            app = create_app(self._settings(db_path, enabled=True))
            client = build_test_client(app)
            auth = ("admin", "secret")

            create_response = client.post(
                "/admin/outbound/companies",
                auth=auth,
                json={
                    "company_name": "Школа 444",
                    "website": "https://school444.example",
                    "city": "Москва",
                    "segment": "school",
                    "note": "интерес к ЕГЭ",
                },
            )
            self.assertEqual(create_response.status_code, 200)
            company = create_response.json()["company"]
            company_id = int(company["id"])
            self.assertEqual(company["company_name"], "Школа 444")

            list_response = client.get("/admin/outbound", auth=auth)
            self.assertEqual(list_response.status_code, 200)
            self.assertTrue(list_response.json()["ok"])
            self.assertGreaterEqual(len(list_response.json()["items"]), 1)

            score_response = client.post(
                f"/admin/outbound/companies/{company_id}/score",
                auth=auth,
                json={"campaign_tags": ["school", "ege"]},
            )
            self.assertEqual(score_response.status_code, 200)
            self.assertGreaterEqual(float(score_response.json()["fit"]["score"]), 30.0)

            proposal_response = client.post(
                f"/admin/outbound/companies/{company_id}/proposal",
                auth=auth,
                json={"offer_focus": "ОГЭ/ЕГЭ и олимпиады"},
            )
            self.assertEqual(proposal_response.status_code, 200)
            proposal = proposal_response.json()["proposal"]
            proposal_id = int(proposal["id"])
            self.assertEqual(proposal["status"], "draft")

            approve_response = client.post(
                f"/admin/outbound/proposals/{proposal_id}/approve",
                auth=auth,
                json={"actor": "manager-1"},
            )
            self.assertEqual(approve_response.status_code, 200)
            self.assertEqual(approve_response.json()["proposal"]["status"], "approved")

            status_response = client.patch(
                f"/admin/outbound/companies/{company_id}/status",
                auth=auth,
                json={"status": "in_progress", "actor": "manager-1"},
            )
            self.assertEqual(status_response.status_code, 200)
            self.assertEqual(status_response.json()["company"]["status"], "in_progress")

            detail_response = client.get(f"/admin/outbound/companies/{company_id}", auth=auth)
            self.assertEqual(detail_response.status_code, 200)
            detail_payload = detail_response.json()
            self.assertTrue(detail_payload["ok"])
            self.assertGreaterEqual(len(detail_payload["proposals"]), 1)
            self.assertGreaterEqual(len(detail_payload["events"]), 1)

    def test_outbound_dedup_and_antispam_guards(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "outbound_guard.db"
            app = create_app(self._settings(db_path, enabled=True))
            client = build_test_client(app)
            auth = ("admin", "secret")

            create_first = client.post(
                "/admin/outbound/companies",
                auth=auth,
                json={
                    "company_name": "Лицей 17",
                    "website": "https://lyceum17.example",
                    "city": "Москва",
                    "segment": "school",
                },
            )
            self.assertEqual(create_first.status_code, 200)
            first_payload = create_first.json()
            company_id = int(first_payload["company"]["id"])
            self.assertFalse(first_payload["deduplicated"])

            create_duplicate = client.post(
                "/admin/outbound/companies",
                auth=auth,
                json={
                    "company_name": "Лицей 17",
                    "website": "http://lyceum17.example/",
                    "city": "Москва",
                    "segment": "school",
                },
            )
            self.assertEqual(create_duplicate.status_code, 200)
            duplicate_payload = create_duplicate.json()
            self.assertTrue(duplicate_payload["deduplicated"])
            self.assertEqual(int(duplicate_payload["company"]["id"]), company_id)

            first_proposal = client.post(
                f"/admin/outbound/companies/{company_id}/proposal",
                auth=auth,
                json={"offer_focus": "Пилот ЕГЭ/олимп"},
            )
            self.assertEqual(first_proposal.status_code, 200)

            second_proposal = client.post(
                f"/admin/outbound/companies/{company_id}/proposal",
                auth=auth,
                json={"offer_focus": "Повторный пилот"},
            )
            self.assertEqual(second_proposal.status_code, 409)
            self.assertEqual(second_proposal.json()["detail"]["code"], "open_proposal_exists")

    def test_outbound_status_transitions_are_guarded(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "outbound_status.db"
            app = create_app(self._settings(db_path, enabled=True))
            client = build_test_client(app)
            auth = ("admin", "secret")

            create_response = client.post(
                "/admin/outbound/companies",
                auth=auth,
                json={
                    "company_name": "Компания XYZ",
                    "website": "https://xyz.example",
                    "city": "Москва",
                    "segment": "corporate",
                },
            )
            self.assertEqual(create_response.status_code, 200)
            company_id = int(create_response.json()["company"]["id"])

            move_to_won = client.patch(
                f"/admin/outbound/companies/{company_id}/status",
                auth=auth,
                json={"status": "won"},
            )
            self.assertEqual(move_to_won.status_code, 409)
            self.assertEqual(move_to_won.json()["detail"]["code"], "invalid_company_status_transition")

    def test_outbound_proposal_approve_rejected_for_invalid_transition(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "outbound_proposal_transition.db"
            app = create_app(self._settings(db_path, enabled=True))
            client = build_test_client(app)
            auth = ("admin", "secret")

            create_response = client.post(
                "/admin/outbound/companies",
                auth=auth,
                json={
                    "company_name": "Transition Co",
                    "website": "https://transition.example",
                    "city": "Москва",
                    "segment": "school",
                },
            )
            self.assertEqual(create_response.status_code, 200)
            company_id = int(create_response.json()["company"]["id"])

            proposal_response = client.post(
                f"/admin/outbound/companies/{company_id}/proposal",
                auth=auth,
                json={"offer_focus": "Pilot"},
            )
            self.assertEqual(proposal_response.status_code, 200)
            proposal_id = int(proposal_response.json()["proposal"]["id"])

            conn = db.get_connection(db_path)
            try:
                db.update_outbound_proposal_status(conn, proposal_id=proposal_id, status="approved", actor="mgr")
                db.update_outbound_proposal_status(conn, proposal_id=proposal_id, status="sent", actor="mgr")
            finally:
                conn.close()

            approve_again = client.post(
                f"/admin/outbound/proposals/{proposal_id}/approve",
                auth=auth,
                json={"actor": "mgr"},
            )
            self.assertEqual(approve_again.status_code, 409)
            self.assertEqual(approve_again.json()["detail"]["code"], "invalid_proposal_status_transition")

    def test_outbound_proposal_recomputes_fit_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "outbound_fit_missing.db"
            app = create_app(self._settings(db_path, enabled=True))
            client = build_test_client(app)
            auth = ("admin", "secret")

            conn = db.get_connection(db_path)
            try:
                company_id = db.create_outbound_company(
                    conn,
                    company_name="No Fit Co",
                    website="https://nofit.example",
                    city="Москва",
                    segment="school",
                    fit_score=None,
                    fit_tags=[],
                    fit_reason="",
                )
                db.update_outbound_company(conn, company_id=company_id, fit_score=0.0, fit_tags=[], fit_reason="")
            finally:
                conn.close()

            proposal_response = client.post(
                f"/admin/outbound/companies/{company_id}/proposal",
                auth=auth,
                json={"offer_focus": "Pilot"},
            )
            self.assertEqual(proposal_response.status_code, 200)
            company = proposal_response.json()["company"]
            self.assertGreater(float(company.get("fit_score") or 0), 0)

    def test_outbound_csv_import_and_ui(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "outbound_csv.db"
            app = create_app(self._settings(db_path, enabled=True))
            client = build_test_client(app)
            auth = ("admin", "secret")

            csv_content = (
                "company_name,website,city,segment,note,owner\n"
                "Школа 179,https://179.example,Москва,school,Олимпиадный интерес,owner-1\n"
                "Колледж Тест,https://college.example,Москва,college,Нужен пилот,owner-2\n"
            )
            files = {"file": ("companies.csv", io.BytesIO(csv_content.encode("utf-8")), "text/csv")}
            import_response = client.post("/admin/outbound/import-csv", auth=auth, files=files)
            self.assertEqual(import_response.status_code, 200)
            self.assertEqual(import_response.json()["imported"], 2)
            self.assertEqual(import_response.json()["skipped"], 0)

            import_duplicate_response = client.post("/admin/outbound/import-csv", auth=auth, files=files)
            self.assertEqual(import_duplicate_response.status_code, 200)
            self.assertEqual(import_duplicate_response.json()["imported"], 0)
            self.assertGreaterEqual(import_duplicate_response.json()["skipped"], 2)

            empty_files = {"file": ("empty.csv", io.BytesIO(b""), "text/csv")}
            empty_import_response = client.post("/admin/outbound/import-csv", auth=auth, files=empty_files)
            self.assertEqual(empty_import_response.status_code, 400)

            invalid_utf_files = {"file": ("bad.csv", io.BytesIO(b"\xff\xfe\xfd"), "text/csv")}
            invalid_utf_import_response = client.post("/admin/outbound/import-csv", auth=auth, files=invalid_utf_files)
            self.assertEqual(invalid_utf_import_response.status_code, 200)
            self.assertEqual(invalid_utf_import_response.json()["imported"], 0)

            ui_response = client.get("/admin/ui/outbound", auth=auth)
            self.assertEqual(ui_response.status_code, 200)
            self.assertIn("Outbound Copilot", ui_response.text)
            self.assertIn("Школа 179", ui_response.text)

    def test_outbound_not_found_and_ui_actions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "outbound_ui_actions.db"
            app = create_app(self._settings(db_path, enabled=True))
            client = build_test_client(app)
            auth = ("admin", "secret")

            for method, path, kwargs in (
                ("get", "/admin/outbound/companies/999", {}),
                ("patch", "/admin/outbound/companies/999/status", {"json": {"status": "qualified"}}),
                ("post", "/admin/outbound/companies/999/score", {"json": {"campaign_tags": []}}),
                ("post", "/admin/outbound/companies/999/proposal", {"json": {"offer_focus": "x"}}),
                ("post", "/admin/outbound/proposals/999/approve", {"json": {"actor": "u"}}),
            ):
                response = getattr(client, method)(path, auth=auth, **kwargs)
                self.assertEqual(response.status_code, 404)

            create_response = client.post(
                "/admin/outbound/companies",
                auth=auth,
                json={
                    "company_name": "UI Компания",
                    "website": "https://ui-company.example",
                    "city": "Москва",
                    "segment": "school",
                },
            )
            self.assertEqual(create_response.status_code, 200)
            company_id = int(create_response.json()["company"]["id"])

            ui_create = client.post(
                "/admin/ui/outbound/companies/create",
                auth=auth,
                data={
                    "company_name": "UI Компания 2",
                    "website": "https://ui-company-2.example",
                    "city": "Москва",
                    "segment": "school",
                    "owner": "owner",
                    "note": "note",
                },
            )
            self.assertIn(ui_create.status_code, {200, 303})

            ui_score = client.post(f"/admin/ui/outbound/companies/{company_id}/score", auth=auth)
            self.assertIn(ui_score.status_code, {200, 303})

            ui_proposal = client.post(f"/admin/ui/outbound/companies/{company_id}/proposal", auth=auth)
            self.assertIn(ui_proposal.status_code, {200, 303})

            details = client.get(f"/admin/outbound/companies/{company_id}", auth=auth)
            self.assertEqual(details.status_code, 200)
            proposals = details.json()["proposals"]
            self.assertGreaterEqual(len(proposals), 1)
            proposal_id = int(proposals[0]["id"])

            ui_status = client.post(
                f"/admin/ui/outbound/companies/{company_id}/status",
                auth=auth,
                data={"new_status": "in_progress"},
            )
            self.assertIn(ui_status.status_code, {200, 303})

            ui_approve = client.post(f"/admin/ui/outbound/proposals/{proposal_id}/approve", auth=auth)
            self.assertIn(ui_approve.status_code, {200, 303})

            ui_import_files = {
                "file": (
                    "ui_companies.csv",
                    io.BytesIO("company_name,city\nUI Import,Москва\n".encode("utf-8")),
                    "text/csv",
                )
            }
            ui_import = client.post("/admin/ui/outbound/import-csv", auth=auth, files=ui_import_files)
            self.assertIn(ui_import.status_code, {200, 303})


if __name__ == "__main__":
    unittest.main()
