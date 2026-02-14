import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

try:
    from sales_agent.sales_bot import bot

    HAS_BOT_DEPS = True
except ModuleNotFoundError:
    HAS_BOT_DEPS = False


class _DummyConn:
    def close(self) -> None:
        return None


def _update_with_text(text: str):
    user = SimpleNamespace(id=1, username="user", first_name="A", last_name="B")
    chat = SimpleNamespace(id=10)
    message = AsyncMock()
    message.text = text
    return SimpleNamespace(
        message=message,
        callback_query=None,
        effective_user=user,
        effective_chat=chat,
    )


def _context_with_flags(**flags):
    return SimpleNamespace(user_data=dict(flags), args=[])


@unittest.skipUnless(HAS_BOT_DEPS, "bot dependencies are not installed")
class BotRoutingTests(unittest.IsolatedAsyncioTestCase):
    async def test_on_text_message_routes_to_kbtest_when_waiting_flag_set(self) -> None:
        update = _update_with_text("Какие документы нужны?")
        context = _context_with_flags(**{bot.KBTEST_WAITING_QUESTION_KEY: True})

        with patch.object(bot.db_module, "get_connection", return_value=_DummyConn()), patch.object(
            bot, "_get_or_create_user_id", return_value=1
        ), patch.object(bot.db_module, "log_message"), patch.object(
            bot, "_answer_knowledge_question", new_callable=AsyncMock
        ) as mock_answer:
            await bot.on_text_message(update, context)

        self.assertNotIn(bot.KBTEST_WAITING_QUESTION_KEY, context.user_data)
        mock_answer.assert_awaited_once()

    async def test_on_text_message_routes_to_leadtest_when_waiting_flag_set(self) -> None:
        update = _update_with_text("+79991234567")
        context = _context_with_flags(**{bot.LEADTEST_WAITING_PHONE_KEY: True})

        with patch.object(bot.db_module, "get_connection", return_value=_DummyConn()), patch.object(
            bot, "_get_or_create_user_id", return_value=1
        ), patch.object(bot.db_module, "log_message"), patch.object(
            bot, "_create_lead_from_phone", new_callable=AsyncMock
        ) as mock_create_lead:
            await bot.on_text_message(update, context)

        self.assertNotIn(bot.LEADTEST_WAITING_PHONE_KEY, context.user_data)
        mock_create_lead.assert_awaited_once()

    async def test_on_text_message_routes_to_kb_auto(self) -> None:
        update = _update_with_text("Какие условия возврата оплаты?")
        context = _context_with_flags()

        with patch.object(bot.db_module, "get_connection", return_value=_DummyConn()), patch.object(
            bot, "_get_or_create_user_id", return_value=1
        ), patch.object(bot.db_module, "log_message"), patch.object(
            bot, "_answer_knowledge_question", new_callable=AsyncMock
        ) as mock_answer, patch.object(bot, "_handle_flow_step", new_callable=AsyncMock) as mock_flow:
            await bot.on_text_message(update, context)

        mock_answer.assert_awaited_once()
        mock_flow.assert_not_awaited()

    async def test_on_text_message_routes_to_flow_for_regular_text(self) -> None:
        update = _update_with_text("10 класс")
        context = _context_with_flags()

        with patch.object(bot, "_handle_flow_step", new_callable=AsyncMock) as mock_flow, patch.object(
            bot, "_answer_knowledge_question", new_callable=AsyncMock
        ) as mock_answer, patch.object(
            bot, "_handle_consultative_query", new_callable=AsyncMock, return_value=False
        ) as mock_consult:
            await bot.on_text_message(update, context)

        mock_flow.assert_awaited_once()
        mock_answer.assert_not_awaited()
        mock_consult.assert_awaited_once()

    async def test_on_text_message_routes_to_consultative_when_detected(self) -> None:
        update = _update_with_text("У меня ребенок в 11 классе, хочу поступить в МФТИ, что делать?")
        context = _context_with_flags()

        with patch.object(
            bot, "_handle_consultative_query", new_callable=AsyncMock, return_value=True
        ) as mock_consult, patch.object(bot, "_handle_flow_step", new_callable=AsyncMock) as mock_flow:
            await bot.on_text_message(update, context)

        mock_consult.assert_awaited_once()
        mock_flow.assert_not_awaited()

    async def test_on_callback_query_noop_without_query(self) -> None:
        update = SimpleNamespace(callback_query=None)
        context = _context_with_flags()
        with patch.object(bot, "_handle_flow_step", new_callable=AsyncMock) as mock_flow:
            await bot.on_callback_query(update, context)
        mock_flow.assert_not_awaited()

    async def test_on_callback_query_handles_data(self) -> None:
        callback_query = SimpleNamespace(
            data="goal:ege",
            answer=AsyncMock(),
            message=AsyncMock(),
        )
        update = SimpleNamespace(callback_query=callback_query)
        context = _context_with_flags()
        with patch.object(bot, "_handle_flow_step", new_callable=AsyncMock) as mock_flow:
            await bot.on_callback_query(update, context)
        callback_query.answer.assert_awaited_once()
        mock_flow.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
