import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

try:
    from fastapi.testclient import TestClient

    from sales_agent.sales_api.main import create_app
    from sales_agent.sales_core.config import Settings

    HAS_WEBHOOK_DEPS = True
except ModuleNotFoundError:
    HAS_WEBHOOK_DEPS = False


class _MockTelegramApplication:
    def __init__(self) -> None:
        self.bot = SimpleNamespace()
        self.initialize = AsyncMock()
        self.start = AsyncMock()
        self.stop = AsyncMock()
        self.shutdown = AsyncMock()
        self.process_update = AsyncMock()


@unittest.skipUnless(HAS_WEBHOOK_DEPS, "fastapi dependencies are not installed")
class ApiWebhookTests(unittest.TestCase):
    def _settings(
        self,
        db_path: Path,
        *,
        telegram_mode: str,
        webhook_secret: str = "",
        webhook_path: str = "/telegram/webhook",
    ) -> Settings:
        return Settings(
            telegram_bot_token="tg-token",
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
            telegram_mode=telegram_mode,
            telegram_webhook_secret=webhook_secret,
            telegram_webhook_path=webhook_path,
        )

    def test_webhook_returns_409_when_mode_is_polling(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "webhook.db"
            app = create_app(self._settings(db_path, telegram_mode="polling"))
            client = TestClient(app)
            response = client.post("/telegram/webhook", json={"update_id": 1})
            self.assertEqual(response.status_code, 409)
            self.assertIn("disabled", response.json()["detail"].lower())

    def test_webhook_rejects_invalid_secret(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "webhook.db"
            mock_tg_app = _MockTelegramApplication()
            with patch("sales_agent.sales_api.main.bot_runtime.build_application", return_value=mock_tg_app):
                app = create_app(
                    self._settings(
                        db_path,
                        telegram_mode="webhook",
                        webhook_secret="secret-1",
                    )
                )
                with TestClient(app) as client:
                    response = client.post("/telegram/webhook", json={"update_id": 1})
                    self.assertEqual(response.status_code, 403)
                    mock_tg_app.process_update.assert_not_awaited()

    def test_webhook_processes_update_when_secret_matches(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "webhook.db"
            mock_tg_app = _MockTelegramApplication()
            with patch("sales_agent.sales_api.main.bot_runtime.build_application", return_value=mock_tg_app), patch(
                "sales_agent.sales_api.main.Update.de_json", return_value=SimpleNamespace(update_id=1)
            ):
                app = create_app(
                    self._settings(
                        db_path,
                        telegram_mode="webhook",
                        webhook_secret="secret-2",
                        webhook_path="tg/webhook",
                    )
                )
                with TestClient(app) as client:
                    response = client.post(
                        "/tg/webhook",
                        json={"update_id": 1},
                        headers={"X-Telegram-Bot-Api-Secret-Token": "secret-2"},
                    )
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.json(), {"ok": True})

            mock_tg_app.initialize.assert_awaited_once()
            mock_tg_app.start.assert_awaited_once()
            mock_tg_app.process_update.assert_awaited_once()
            mock_tg_app.stop.assert_awaited_once()
            mock_tg_app.shutdown.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
