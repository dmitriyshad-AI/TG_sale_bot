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
    from sales_agent.sales_core.config import Settings

    HAS_FASTAPI = True
except ModuleNotFoundError:
    HAS_FASTAPI = False


def _settings(db_path: Path, webapp_dist: Path) -> Settings:
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
        webapp_dist_path=webapp_dist,
    )


def _build_init_data(payload: dict, bot_token: str) -> str:
    data = {key: value for key, value in payload.items() if key != "hash"}
    check_lines = [f"{key}={value}" for key, value in sorted(data.items())]
    data_check_string = "\n".join(check_lines)
    secret_key = hmac.new(b"WebAppData", bot_token.encode("utf-8"), hashlib.sha256).digest()
    digest = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    data["hash"] = digest
    return urlencode(data)


@unittest.skipUnless(HAS_FASTAPI, "fastapi dependencies are not installed")
class ApiUserWebappTests(unittest.TestCase):
    def test_user_webapp_placeholder_when_dist_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            app = create_app(_settings(root / "app.db", root / "missing_dist"))
            client = TestClient(app)

            root_response = client.get("/")
            self.assertEqual(root_response.status_code, 200)
            self.assertEqual(root_response.json()["user_miniapp"]["status"], "build-required")

            placeholder = client.get("/app")
            self.assertEqual(placeholder.status_code, 200)
            self.assertIn("User Mini App is not built yet", placeholder.text)

    def test_user_webapp_served_when_dist_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            dist = root / "dist"
            dist.mkdir(parents=True, exist_ok=True)
            (dist / "index.html").write_text("<!doctype html><html><body>miniapp-ready</body></html>", encoding="utf-8")

            app = create_app(_settings(root / "app.db", dist))
            client = TestClient(app)

            root_response = client.get("/")
            self.assertEqual(root_response.status_code, 200)
            self.assertEqual(root_response.json()["user_miniapp"]["status"], "ready")

            webapp_response = client.get("/app", follow_redirects=True)
            self.assertEqual(webapp_response.status_code, 200)
            self.assertIn("miniapp-ready", webapp_response.text)

    def test_whoami_returns_not_in_telegram_without_init_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            app = create_app(_settings(root / "app.db", root / "missing_dist"))
            client = TestClient(app)
            response = client.get("/api/auth/whoami")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["ok"], False)
        self.assertEqual(response.json()["reason"], "not_in_telegram")

    def test_miniapp_meta_returns_brand_advisor_and_manager_links(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cfg = _settings(root / "app.db", root / "missing_dist")
            cfg.miniapp_brand_name = "УНПК МФТИ"
            cfg.miniapp_advisor_name = "Гид"
            cfg.sales_manager_label = "Старший менеджер"
            cfg.sales_manager_chat_url = "https://t.me/kmipt_sales_manager"
            cfg.user_webapp_url = "https://example.com/app"
            app = create_app(cfg)
            client = TestClient(app)
            response = client.get("/api/miniapp/meta")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["brand_name"], "УНПК МФТИ")
        self.assertEqual(payload["advisor_name"], "Гид")
        self.assertEqual(payload["manager_label"], "Старший менеджер")
        self.assertEqual(payload["manager_chat_url"], "https://t.me/kmipt_sales_manager")
        self.assertEqual(payload["user_miniapp_url"], "https://example.com/app")

    def test_whoami_accepts_header_and_authorization_tma(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            app = create_app(_settings(root / "app.db", root / "missing_dist"))
            client = TestClient(app)
            init_data = _build_init_data(
                {
                    "auth_date": str(int(time.time())),
                    "query_id": "AAEAAAE",
                    "user": json.dumps({"id": 42, "first_name": "Dmitriy", "username": "dmitriy"}, ensure_ascii=False),
                },
                "123:ABC",
            )

            via_header = client.get("/api/auth/whoami", headers={"X-Tg-Init-Data": init_data})
            via_auth = client.get("/api/auth/whoami", headers={"Authorization": f"tma {init_data}"})

        self.assertEqual(via_header.status_code, 200)
        self.assertTrue(via_header.json()["ok"])
        self.assertEqual(via_header.json()["user"]["id"], 42)
        self.assertEqual(via_auth.status_code, 200)
        self.assertTrue(via_auth.json()["ok"])
        self.assertEqual(via_auth.json()["user"]["username"], "dmitriy")

    def test_whoami_rejects_invalid_init_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            app = create_app(_settings(root / "app.db", root / "missing_dist"))
            client = TestClient(app)
            response = client.get(
                "/api/auth/whoami",
                headers={
                    "X-Tg-Init-Data": "auth_date=1700000000&query_id=AAEAAAE&user=%7B%22id%22%3A1%7D&hash=broken"
                },
            )

        self.assertEqual(response.status_code, 401)
        self.assertIn("invalid telegram miniapp auth", response.json()["detail"].lower())

    def test_catalog_search_returns_top_items(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            catalog_path = root / "products.yaml"
            catalog_path.write_text(
                """
products:
  - id: kmipt-ege-math
    brand: kmipt
    title: Подготовка к ЕГЭ по математике
    url: https://example.com/math
    category: ege
    grade_min: 10
    grade_max: 11
    subjects: [math]
    format: online
    sessions:
      - name: Осень
        start_date: 2026-09-15
        end_date: 2027-05-20
        price_rub: 98000
    usp:
      - Мини-группа
      - Практика по заданиям ФИПИ
      - Персональная обратная связь
  - id: kmipt-ege-physics
    brand: kmipt
    title: Подготовка к ЕГЭ по физике
    url: https://example.com/physics
    category: ege
    grade_min: 10
    grade_max: 11
    subjects: [physics]
    format: hybrid
    sessions:
      - name: Осень
        start_date: 2026-09-20
        end_date: 2027-05-25
        price_rub: 102000
    usp:
      - Мини-группа
      - Разбор второй части
      - Домашние задания с проверкой
""".strip(),
                encoding="utf-8",
            )
            cfg = _settings(root / "app.db", root / "missing_dist")
            cfg.catalog_path = catalog_path
            app = create_app(cfg)
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
        self.assertEqual(payload["count"], 1)
        first = payload["items"][0]
        self.assertEqual(first["id"], "kmipt-ege-math")
        self.assertIn("подходит", first["why_match"])
        self.assertEqual(first["price_text"], "98 000 ₽")
        self.assertEqual(first["next_start_text"], "15.09.2026")
        self.assertEqual(len(first["usp"]), 3)
        self.assertEqual(payload["match_quality"], "strong")
        self.assertFalse(payload["manager_recommended"])

    def test_catalog_search_without_match_promotes_manager_contact(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            catalog_path = root / "products.yaml"
            catalog_path.write_text(
                """
products:
  - id: kmipt-ege-math
    brand: kmipt
    title: Подготовка к ЕГЭ по математике
    url: https://example.com/math
    category: ege
    grade_min: 10
    grade_max: 11
    subjects: [math]
    format: online
    sessions:
      - name: Осень
        start_date: 2026-09-15
        end_date: 2027-05-20
        price_rub: 98000
    usp:
      - Мини-группа
      - Практика по заданиям ФИПИ
      - Персональная обратная связь
""".strip(),
                encoding="utf-8",
            )
            cfg = _settings(root / "app.db", root / "missing_dist")
            cfg.catalog_path = catalog_path
            app = create_app(cfg)
            client = TestClient(app)
            response = client.get(
                "/api/catalog/search",
                params={
                    "brand": "kmipt",
                    "grade": 6,
                    "goal": "camp",
                    "subject": "informatics",
                    "format": "offline",
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["count"], 0)
        self.assertEqual(payload["match_quality"], "none")
        self.assertTrue(payload["manager_recommended"])
        self.assertIn("Оставьте контакт", payload["manager_call_to_action"])

    def test_assistant_ask_returns_general_help_in_fallback_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            app = create_app(_settings(root / "app.db", root / "missing_dist"))
            client = TestClient(app)
            response = client.post(
                "/api/assistant/ask",
                json={
                    "question": "Что такое косинус?",
                    "criteria": {"brand": "kmipt"},
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertIn(payload["mode"], {"general", "consultative"})
        self.assertIn("косинус", payload["answer_text"].lower())
        self.assertIn("manager_offer", payload)
        self.assertIn("processing_note", payload)
        self.assertTrue(response.headers.get("X-Request-ID"))
        self.assertEqual(payload.get("request_id"), response.headers.get("X-Request-ID"))

    def test_assistant_ask_returns_consultative_with_recommendation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            catalog_path = root / "products.yaml"
            catalog_path.write_text(
                """
products:
  - id: kmipt-ege-math
    brand: kmipt
    title: Подготовка к ЕГЭ по математике
    url: https://example.com/math
    category: ege
    grade_min: 10
    grade_max: 11
    subjects: [math]
    format: online
    usp:
      - Мини-группа
      - Практика по заданиям ФИПИ
      - Персональная обратная связь
""".strip(),
                encoding="utf-8",
            )
            cfg = _settings(root / "app.db", root / "missing_dist")
            cfg.catalog_path = catalog_path
            app = create_app(cfg)
            client = TestClient(app)
            response = client.post(
                "/api/assistant/ask",
                json={
                    "question": "Ученик 11 класса, как лучше подготовиться к ЕГЭ по математике для поступления?",
                    "criteria": {"brand": "kmipt", "grade": 11, "goal": "ege", "subject": "math", "format": "online"},
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["mode"], "consultative")
        self.assertIn("manager_offer", payload)
        self.assertGreaterEqual(len(payload["recommended_products"]), 1)
        self.assertEqual(payload["recommended_products"][0]["id"], "kmipt-ege-math")
        self.assertTrue(response.headers.get("X-Request-ID"))
        self.assertEqual(payload.get("request_id"), response.headers.get("X-Request-ID"))

    def test_assistant_ask_empty_question_returns_user_message_and_request_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            app = create_app(_settings(root / "app.db", root / "missing_dist"))
            client = TestClient(app)
            response = client.post(
                "/api/assistant/ask",
                json={
                    "question": "   ",
                    "criteria": {"brand": "kmipt"},
                },
            )

        self.assertEqual(response.status_code, 400)
        payload = response.json()
        detail = payload.get("detail")
        self.assertIsInstance(detail, dict)
        self.assertEqual(detail.get("code"), "empty_question")
        self.assertIn("Напишите вопрос", detail.get("user_message", ""))
        self.assertEqual(detail.get("request_id"), response.headers.get("X-Request-ID"))


if __name__ == "__main__":
    unittest.main()
