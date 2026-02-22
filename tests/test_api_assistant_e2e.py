import tempfile
import unittest
from pathlib import Path

try:
    from fastapi.testclient import TestClient

    from sales_agent.sales_api.main import create_app
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
        assistant_api_token="assistant-e2e-token",
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
      - Разбор реальных вариантов
  - id: kmipt-ege-physics
    brand: kmipt
    title: Подготовка к ЕГЭ по физике
    url: https://kmipt.ru/courses/EGE/fizika_ege/
    category: ege
    grade_min: 10
    grade_max: 11
    subjects: [physics]
    format: offline
    sessions:
      - name: Осенний поток
        start_date: 2026-09-20
        end_date: 2027-05-25
        price_rub: 102000
    usp:
      - Практика второй части
      - Малые группы
      - Индивидуальные рекомендации
""".strip(),
        encoding="utf-8",
    )


@unittest.skipUnless(HAS_FASTAPI, "fastapi dependencies are not installed")
class AssistantApiE2ETests(unittest.TestCase):
    def test_multiturn_flow_general_then_consultative(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            catalog_path = root / "products.yaml"
            _write_catalog(catalog_path)

            app = create_app(_settings(root / "app.db", catalog_path))
            client = TestClient(app)
            headers = {"X-Assistant-Token": "assistant-e2e-token"}

            first = client.post(
                "/api/assistant/ask",
                json={
                    "question": "Что такое косинус?",
                    "criteria": {"brand": "kmipt"},
                },
                headers=headers,
            )
            self.assertEqual(first.status_code, 200)
            first_payload = first.json()
            self.assertTrue(first_payload["ok"])
            self.assertIn(first_payload["mode"], {"general", "consultative"})
            self.assertTrue(str(first_payload.get("answer_text", "")).strip())

            second = client.post(
                "/api/assistant/ask",
                json={
                    "question": "Ученик 10 класса, как выстроить стратегию ЕГЭ по математике для поступления в МФТИ?",
                    "criteria": {
                        "brand": "kmipt",
                        "grade": 10,
                        "goal": "ege",
                        "subject": "math",
                        "format": "online",
                    },
                    "recent_history": [
                        {"role": "user", "text": "Что такое косинус?"},
                        {"role": "assistant", "text": first_payload["answer_text"]},
                    ],
                },
                headers=headers,
            )
            self.assertEqual(second.status_code, 200)
            second_payload = second.json()
            self.assertTrue(second_payload["ok"])
            self.assertEqual(second_payload["mode"], "consultative")
            self.assertTrue(str(second_payload.get("answer_text", "")).strip())
            self.assertTrue(str(second_payload.get("processing_note", "")).strip())
            self.assertTrue(second_payload["recommended_products"])
            self.assertEqual(second_payload["recommended_products"][0]["id"], "kmipt-ege-math")
            self.assertIn("manager_offer", second_payload)

    def test_knowledge_mode_fallback_without_vector_store_is_user_friendly(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            catalog_path = root / "products.yaml"
            _write_catalog(catalog_path)

            app = create_app(_settings(root / "app.db", catalog_path))
            client = TestClient(app)
            headers = {"X-Assistant-Token": "assistant-e2e-token"}

            response = client.post(
                "/api/assistant/ask",
                json={
                    "question": "Какие документы нужны для договора и оплаты?",
                    "criteria": {"brand": "kmipt"},
                },
                headers=headers,
            )
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["mode"], "knowledge")
            self.assertTrue(str(payload.get("answer_text", "")).strip())
            answer_text = payload["answer_text"].lower()
            self.assertTrue("llm не настроен" in answer_text or "база знаний" in answer_text)


if __name__ == "__main__":
    unittest.main()
