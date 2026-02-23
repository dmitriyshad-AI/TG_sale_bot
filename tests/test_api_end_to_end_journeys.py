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
            client = TestClient(app)

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
            client = TestClient(app)
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
            client = TestClient(app)

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
            client = TestClient(app)

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
            client = TestClient(app)
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


if __name__ == "__main__":
    unittest.main()
