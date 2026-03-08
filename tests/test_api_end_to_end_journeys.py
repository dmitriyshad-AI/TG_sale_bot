import hashlib
import hmac
import json
import tempfile
import time
import unittest
from pathlib import Path
from urllib.parse import urlencode

try:
    from tests.test_client_compat import build_test_client

    from sales_agent.sales_api.main import create_app
    from sales_agent.sales_core import db as db_module
    from sales_agent.sales_core.config import Settings

    HAS_FASTAPI = True
except ModuleNotFoundError:
    HAS_FASTAPI = False


def _settings(db_path: Path, catalog_path: Path) -> Settings:
    return Settings(
        telegram_bot_token="123:ABC",
        openai_api_key="",
        openai_model="gpt-5.1",
        tallanto_api_url="",
        tallanto_api_key="",
        brand_default="kmipt",
        database_path=db_path,
        catalog_path=catalog_path,
        knowledge_path=Path("knowledge"),
        vector_store_meta_path=Path("data/vector_store.json"),
        openai_vector_store_id="",
        admin_user="admin",
        admin_pass="secret",
        assistant_api_token="assistant-e2e",
    )


def _settings_revenue(db_path: Path, catalog_path: Path) -> Settings:
    cfg = _settings(db_path, catalog_path)
    cfg.enable_lead_radar = True
    cfg.enable_director_agent = True
    cfg.enable_call_copilot = True
    cfg.enable_outbound_copilot = True
    cfg.lead_radar_no_reply_hours = 1
    cfg.lead_radar_call_no_next_step_hours = 1
    cfg.lead_radar_stale_warm_days = 1
    cfg.lead_radar_max_items_per_run = 20
    cfg.lead_radar_thread_cooldown_hours = 0
    cfg.lead_radar_daily_cap_per_thread = 5
    return cfg


def _write_catalog(path: Path) -> None:
    path.write_text(
        """
products:
  - id: kmipt-ege-math
    brand: kmipt
    title: Подготовка к ЕГЭ по математике
    url: https://kmipt.ru/courses/EGE/matematika_ege/
    category: ege
    grade_min: 10
    grade_max: 11
    subjects: [math]
    format: online
    sessions:
      - name: Осенний поток
        start_date: 2026-09-15
        end_date: 2027-05-20
        price_rub: 98000
    usp:
      - Мини-группы
      - Персональная проверка ДЗ
      - Разбор вариантов ЕГЭ
  - id: kmipt-olymp-physics
    brand: kmipt
    title: Олимпиадная физика
    url: https://kmipt.ru/courses/olymp/physics/
    category: olympiad
    grade_min: 8
    grade_max: 11
    subjects: [physics]
    format: offline
    usp:
      - Олимпиадные задачи
      - Практика в мини-группах
      - Тренировка нестандартного мышления
""".strip(),
        encoding="utf-8",
    )


def _build_init_data(payload: dict, bot_token: str) -> str:
    data = {key: value for key, value in payload.items() if key != "hash"}
    check_lines = [f"{key}={value}" for key, value in sorted(data.items())]
    data_check_string = "\n".join(check_lines)
    secret_key = hmac.new(b"WebAppData", bot_token.encode("utf-8"), hashlib.sha256).digest()
    digest = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    data["hash"] = digest
    return urlencode(data)


def _telegram_headers(user_id: int, bot_token: str = "123:ABC") -> dict[str, str]:
    init_data = _build_init_data(
        {
            "auth_date": str(int(time.time())),
            "query_id": f"AAE{user_id}AAE",
            "user": json.dumps(
                {
                    "id": user_id,
                    "first_name": "E2E",
                    "username": f"e2e_{user_id}",
                },
                ensure_ascii=False,
            ),
        },
        bot_token,
    )
    return {"X-Tg-Init-Data": init_data}


@unittest.skipUnless(HAS_FASTAPI, "fastapi dependencies are not installed")
class ApiEndToEndJourneyTests(unittest.TestCase):
    def test_e2e_assistant_general_with_service_token(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            catalog_path = root / "products.yaml"
            _write_catalog(catalog_path)
            app = create_app(_settings(root / "app.db", catalog_path))
            client = build_test_client(app)

            response = client.post(
                "/api/assistant/ask",
                json={"question": "Что такое косинус?", "criteria": {"brand": "kmipt"}},
                headers={"X-Assistant-Token": "assistant-e2e"},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertIn(payload["mode"], {"general", "consultative"})
        self.assertTrue(str(payload.get("answer_text", "")).strip())

    def test_e2e_assistant_persists_user_context_between_turns(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            db_path = root / "app.db"
            catalog_path = root / "products.yaml"
            _write_catalog(catalog_path)
            app = create_app(_settings(db_path, catalog_path))
            client = build_test_client(app)
            headers = _telegram_headers(user_id=777)

            first = client.post(
                "/api/assistant/ask",
                json={
                    "question": "Ученик 10 класса, как готовиться к ЕГЭ по математике для МФТИ?",
                    "criteria": {"brand": "kmipt", "grade": 10, "goal": "ege", "subject": "math", "format": "online"},
                },
                headers=headers,
            )
            self.assertEqual(first.status_code, 200)

            second = client.post(
                "/api/assistant/ask",
                json={
                    "question": "Продолжим: какой темп лучше?",
                    "criteria": {"brand": "kmipt"},
                },
                headers=headers,
            )
            self.assertEqual(second.status_code, 200)

            conn = db_module.get_connection(db_path)
            try:
                user_id = db_module.get_or_create_user(
                    conn,
                    channel="telegram",
                    external_id="777",
                    username="e2e_777",
                    first_name="E2E",
                    last_name="",
                )
                context = db_module.get_conversation_context(conn, user_id=user_id)
            finally:
                conn.close()

        self.assertTrue(context)
        self.assertIn("summary_text", context)
        self.assertIn("10 класс", str(context["summary_text"]))
        requests = context.get("recent_user_requests", [])
        self.assertTrue(any("какой темп лучше" in item.lower() for item in requests))

    def test_e2e_catalog_search_strong_match(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            catalog_path = root / "products.yaml"
            _write_catalog(catalog_path)
            app = create_app(_settings(root / "app.db", catalog_path))
            client = build_test_client(app)

            response = client.get(
                "/api/catalog/search",
                params={
                    "brand": "kmipt",
                    "grade": 11,
                    "goal": "ege",
                    "subject": "math",
                    "format": "online",
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["match_quality"], "strong")
        self.assertGreaterEqual(payload["count"], 1)
        self.assertEqual(payload["items"][0]["id"], "kmipt-ege-math")

    def test_e2e_catalog_search_no_match_recommends_manager(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            catalog_path = root / "products.yaml"
            _write_catalog(catalog_path)
            app = create_app(_settings(root / "app.db", catalog_path))
            client = build_test_client(app)

            response = client.get(
                "/api/catalog/search",
                params={
                    "brand": "kmipt",
                    "grade": 5,
                    "goal": "camp",
                    "subject": "informatics",
                    "format": "online",
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["count"], 0)
        self.assertEqual(payload["match_quality"], "none")
        self.assertTrue(payload["manager_recommended"])
        self.assertIn("Оставьте контакт", payload["manager_call_to_action"])

    def test_e2e_assistant_rate_limit_for_telegram_user(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            catalog_path = root / "products.yaml"
            _write_catalog(catalog_path)
            cfg = _settings(root / "app.db", catalog_path)
            cfg.assistant_rate_limit_window_seconds = 60
            cfg.assistant_rate_limit_user_requests = 1
            cfg.assistant_rate_limit_ip_requests = 100
            app = create_app(cfg)
            client = build_test_client(app)
            headers = _telegram_headers(user_id=999)

            first = client.post(
                "/api/assistant/ask",
                json={"question": "Что такое синус?", "criteria": {"brand": "kmipt"}},
                headers=headers,
            )
            second = client.post(
                "/api/assistant/ask",
                json={"question": "Что такое косинус?", "criteria": {"brand": "kmipt"}},
                headers=headers,
            )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 429)
        detail = second.json().get("detail", {})
        self.assertEqual(detail.get("code"), "rate_limited")
        self.assertEqual(detail.get("scope"), "assistant_user")

    def test_e2e_admin_approval_send_flow_manual_confirmation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            db_path = root / "app.db"
            catalog_path = root / "products.yaml"
            _write_catalog(catalog_path)
            cfg = _settings(db_path, catalog_path)
            app = create_app(cfg)
            client = build_test_client(app)
            auth = ("admin", "secret")

            conn = db_module.get_connection(db_path)
            try:
                user_id = db_module.get_or_create_user(
                    conn,
                    channel="telegram",
                    external_id="e2e-admin-1",
                    username="e2e_admin_1",
                )
                db_module.log_message(
                    conn,
                    user_id=user_id,
                    direction="inbound",
                    text="Нужна консультация по ЕГЭ",
                    meta={},
                )
            finally:
                conn.close()

            create_draft = client.post(
                f"/admin/inbox/{user_id}/drafts",
                auth=auth,
                json={"draft_text": "Предлагаю короткую консультацию", "model_name": "e2e-model"},
            )
            self.assertEqual(create_draft.status_code, 200)
            draft = create_draft.json()["draft"]
            draft_id = int(draft["id"])

            approve = client.post(f"/admin/inbox/drafts/{draft_id}/approve", auth=auth)
            self.assertEqual(approve.status_code, 200)
            self.assertEqual(approve.json()["draft"]["status"], "approved")

            send = client.post(
                f"/admin/inbox/drafts/{draft_id}/send",
                auth=auth,
                json={"sent_message_id": "tg-manual-1001"},
            )
            self.assertEqual(send.status_code, 200)
            sent = send.json()["draft"]
            self.assertEqual(sent["status"], "sent")
            self.assertEqual(sent["sent_message_id"], "tg-manual-1001")

    def test_e2e_revenue_followup_run_creates_tasks_for_stale_inbound(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            db_path = root / "app.db"
            catalog_path = root / "products.yaml"
            _write_catalog(catalog_path)
            cfg = _settings_revenue(db_path, catalog_path)
            app = create_app(cfg)
            client = build_test_client(app)
            auth = ("admin", "secret")

            conn = db_module.get_connection(db_path)
            try:
                user_id = db_module.get_or_create_user(conn, channel="telegram", external_id="radar-user-1")
                db_module.log_message(conn, user_id=user_id, direction="inbound", text="Хочу готовиться к ЕГЭ", meta={})
                conn.execute(
                    "UPDATE messages SET created_at = datetime('now', '-2 hours') WHERE user_id = ?",
                    (user_id,),
                )
                conn.commit()
            finally:
                conn.close()

            run_response = client.post("/admin/followups/run", auth=auth)
            self.assertEqual(run_response.status_code, 200)
            result = run_response.json()
            self.assertTrue(result)

            followups = client.get("/admin/followups", auth=auth)
            self.assertEqual(followups.status_code, 200)
            items = followups.json()["items"]
            self.assertGreaterEqual(len(items), 1)
            self.assertTrue(any(str(item.get("reason") or "").startswith("radar:no_reply") for item in items))

    def test_e2e_revenue_call_upload_materializes_summary_and_followup(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            db_path = root / "app.db"
            catalog_path = root / "products.yaml"
            _write_catalog(catalog_path)
            cfg = _settings_revenue(db_path, catalog_path)
            app = create_app(cfg)
            client = build_test_client(app)
            auth = ("admin", "secret")

            conn = db_module.get_connection(db_path)
            try:
                user_id = db_module.get_or_create_user(conn, channel="telegram", external_id="call-user-1")
            finally:
                conn.close()

            upload = client.post(
                "/admin/calls/upload",
                auth=auth,
                data={
                    "user_id": str(user_id),
                    "transcript_hint": "Родитель спросил про план ЕГЭ по математике, договорились о консультации.",
                },
            )
            self.assertEqual(upload.status_code, 200)
            call_item = upload.json()["item"]
            self.assertEqual(call_item["status"], "done")

            calls = client.get("/admin/calls", auth=auth)
            self.assertEqual(calls.status_code, 200)
            self.assertGreaterEqual(len(calls.json()["items"]), 1)

            followups = client.get("/admin/followups", auth=auth)
            self.assertEqual(followups.status_code, 200)
            self.assertTrue(any("call_copilot" in str(item.get("reason") or "") for item in followups.json()["items"]))

    def test_e2e_revenue_director_plan_to_actions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            db_path = root / "app.db"
            catalog_path = root / "products.yaml"
            _write_catalog(catalog_path)
            cfg = _settings_revenue(db_path, catalog_path)
            app = create_app(cfg)
            client = build_test_client(app)
            auth = ("admin", "secret")

            conn = db_module.get_connection(db_path)
            try:
                user_id = db_module.get_or_create_user(conn, channel="telegram", external_id="director-user-1")
                db_module.log_message(conn, user_id=user_id, direction="inbound", text="ОГЭ информатика, нужен план", meta={})
            finally:
                conn.close()

            plan_create = client.post(
                "/admin/director/plan",
                auth=auth,
                json={"goal_text": "Вернуть 5 теплых лидов по ОГЭ информатика", "max_actions": 5},
            )
            self.assertEqual(plan_create.status_code, 200)
            plan_id = int(plan_create.json()["plan_id"])

            approve = client.post(f"/admin/director/plans/{plan_id}/approve", auth=auth)
            self.assertEqual(approve.status_code, 200)

            apply = client.post(f"/admin/director/plans/{plan_id}/apply", auth=auth, json={"max_actions": 5})
            self.assertEqual(apply.status_code, 200)
            self.assertGreaterEqual(len(apply.json()["actions"]), 1)

            followups = client.get("/admin/followups", auth=auth)
            self.assertEqual(followups.status_code, 200)
            self.assertGreaterEqual(len(followups.json()["items"]), 1)

    def test_e2e_revenue_outbound_guard_prevents_double_proposal(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            db_path = root / "app.db"
            catalog_path = root / "products.yaml"
            _write_catalog(catalog_path)
            cfg = _settings_revenue(db_path, catalog_path)
            app = create_app(cfg)
            client = build_test_client(app)
            auth = ("admin", "secret")

            create_company = client.post(
                "/admin/outbound/companies",
                auth=auth,
                json={
                    "company_name": "Школа Revenue",
                    "website": "https://revenue-school.example",
                    "city": "Москва",
                    "segment": "school",
                },
            )
            self.assertEqual(create_company.status_code, 200)
            company_id = int(create_company.json()["company"]["id"])

            first = client.post(
                f"/admin/outbound/companies/{company_id}/proposal",
                auth=auth,
                json={"offer_focus": "Пилотная программа ОГЭ/ЕГЭ"},
            )
            self.assertEqual(first.status_code, 200)

            second = client.post(
                f"/admin/outbound/companies/{company_id}/proposal",
                auth=auth,
                json={"offer_focus": "Повторное предложение"},
            )
            self.assertEqual(second.status_code, 409)
            self.assertEqual(second.json()["detail"]["code"], "open_proposal_exists")


if __name__ == "__main__":
    unittest.main()
