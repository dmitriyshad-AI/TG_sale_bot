import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from sales_agent.sales_bot import bot
from sales_agent.sales_core import db as db_module
from sales_agent.sales_core.config import Settings


def _make_user() -> SimpleNamespace:
    return SimpleNamespace(id=9001, username="e2e_user", first_name="E2E", last_name="User")


def _make_chat() -> SimpleNamespace:
    return SimpleNamespace(id=7001)


def _make_message_update(text: str, user: SimpleNamespace, chat: SimpleNamespace) -> SimpleNamespace:
    message = AsyncMock()
    message.text = text
    message.reply_text = AsyncMock()
    return SimpleNamespace(
        message=message,
        callback_query=None,
        effective_user=user,
        effective_chat=chat,
    )


def _make_callback_update(callback_data: str, user: SimpleNamespace, chat: SimpleNamespace) -> SimpleNamespace:
    callback_message = AsyncMock()
    callback_message.reply_text = AsyncMock()
    callback_query = SimpleNamespace(
        data=callback_data,
        answer=AsyncMock(),
        message=callback_message,
    )
    return SimpleNamespace(
        message=None,
        callback_query=callback_query,
        effective_user=user,
        effective_chat=chat,
    )


class BotFunnelE2ETests(unittest.IsolatedAsyncioTestCase):
    def _settings(self, db_path: Path) -> Settings:
        return Settings(
            telegram_bot_token="token",
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
            admin_user="",
            admin_pass="",
            crm_provider="none",
            amo_api_url="",
            amo_access_token="",
        )

    def _read_state(self, db_path: Path, user: SimpleNamespace) -> dict:
        conn = db_module.get_connection(db_path)
        try:
            user_id = db_module.get_or_create_user(
                conn=conn,
                channel="telegram",
                external_id=str(user.id),
                username=user.username,
                first_name=user.first_name,
                last_name=user.last_name,
            )
            return db_module.get_session(conn, user_id)["state"]
        finally:
            conn.close()

    async def test_full_funnel_creates_lead_record(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "e2e.db"
            db_module.init_db(db_path)
            settings = self._settings(db_path)

            user = _make_user()
            chat = _make_chat()
            context = SimpleNamespace(user_data={}, args=[])

            llm_reply = SimpleNamespace(
                answer_text="Рекомендую 2-3 программы",
                next_question="Удобно обсудить детали по телефону?",
                call_to_action="Нажмите оставить контакт",
            )
            llm_client = SimpleNamespace(build_sales_reply_async=AsyncMock(return_value=llm_reply))
            crm_result = SimpleNamespace(success=True, entry_id="crm-lead-1", raw={}, error=None)
            crm_client = SimpleNamespace(create_lead_async=AsyncMock(return_value=crm_result))

            with patch.object(bot, "settings", settings), patch.object(
                bot, "LLMClient", return_value=llm_client
            ), patch.object(bot, "build_crm_client", return_value=crm_client):
                await bot.start(_make_message_update("/start", user, chat), context)
                self.assertEqual(self._read_state(db_path, user).get("state"), "ask_grade")

                await bot.on_callback_query(_make_callback_update("grade:10", user, chat), context)
                self.assertEqual(self._read_state(db_path, user).get("state"), "ask_goal")

                await bot.on_callback_query(_make_callback_update("goal:ege", user, chat), context)
                self.assertEqual(self._read_state(db_path, user).get("state"), "ask_subject")

                await bot.on_callback_query(_make_callback_update("subject:math", user, chat), context)
                self.assertEqual(self._read_state(db_path, user).get("state"), "ask_format")

                await bot.on_callback_query(_make_callback_update("format:online", user, chat), context)
                self.assertEqual(self._read_state(db_path, user).get("state"), "suggest_products")

                await bot.on_callback_query(_make_callback_update("contact:start", user, chat), context)
                self.assertEqual(self._read_state(db_path, user).get("state"), "ask_contact")

                await bot.on_text_message(_make_message_update("+79991234567", user, chat), context)
                self.assertEqual(self._read_state(db_path, user).get("state"), "done")

            conn = db_module.get_connection(db_path)
            try:
                leads = db_module.list_recent_leads(conn, limit=10)
                self.assertEqual(len(leads), 1)
                self.assertEqual(leads[0]["status"], "created")
                self.assertEqual(leads[0]["tallanto_entry_id"], "crm-lead-1")
                self.assertEqual(leads[0]["contact"]["source"], "telegram_flow_contact")
                messages = db_module.list_conversation_messages(conn, user_id=leads[0]["user_id"], limit=200)
                self.assertGreaterEqual(len(messages), 10)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
