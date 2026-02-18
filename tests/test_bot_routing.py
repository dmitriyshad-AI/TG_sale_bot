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
        ) as mock_answer, patch.object(
            bot, "_load_current_state_payload", return_value={"state": "ask_grade", "criteria": {}, "contact": None}
        ), patch.object(
            bot, "_prepare_effective_text_and_context", return_value=("Какие условия возврата оплаты?", {})
        ), patch.object(bot, "_handle_flow_step", new_callable=AsyncMock) as mock_flow:
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
        ) as mock_consult, patch.object(
            bot, "_load_current_state_payload", return_value={"state": "ask_grade", "criteria": {}, "contact": None}
        ), patch.object(
            bot, "_prepare_effective_text_and_context", return_value=("10 класс", {})
        ):
            await bot.on_text_message(update, context)

        mock_flow.assert_awaited_once()
        mock_answer.assert_not_awaited()
        mock_consult.assert_awaited_once()

    async def test_on_text_message_uses_raw_text_for_routing_not_stitched_text(self) -> None:
        update = _update_with_text("11")
        context = _context_with_flags()

        with patch.object(
            bot, "_load_current_state_payload", return_value={"state": "ask_grade", "criteria": {}, "contact": None}
        ), patch.object(
            bot, "_prepare_effective_text_and_context",
            return_value=("Что ты знаешь про IT лагерь УНПК МФТИ?", {}),
        ), patch.object(
            bot, "_answer_knowledge_question", new_callable=AsyncMock
        ) as mock_kb, patch.object(
            bot, "_handle_consultative_query", new_callable=AsyncMock, return_value=False
        ) as mock_consult, patch.object(
            bot, "_handle_flow_step", new_callable=AsyncMock
        ) as mock_flow:
            await bot.on_text_message(update, context)

        mock_kb.assert_not_awaited()
        mock_consult.assert_awaited_once()
        mock_flow.assert_awaited_once()

    async def test_on_text_message_routes_to_general_help_for_education_question(self) -> None:
        update = _update_with_text("Что такое косинус?")
        context = _context_with_flags()

        with patch.object(
            bot, "_handle_consultative_query", new_callable=AsyncMock, return_value=False
        ) as mock_consult, patch.object(
            bot, "_answer_knowledge_question", new_callable=AsyncMock
        ) as mock_kb, patch.object(
            bot, "_load_current_state_payload", return_value={"state": "ask_subject", "criteria": {}, "contact": None}
        ), patch.object(
            bot, "_prepare_effective_text_and_context", return_value=("Что такое косинус?", {})
        ), patch.object(
            bot, "_answer_general_education_question", new_callable=AsyncMock, return_value=True
        ) as mock_general, patch.object(
            bot, "_handle_flow_step", new_callable=AsyncMock
        ) as mock_flow:
            await bot.on_text_message(update, context)

        mock_consult.assert_awaited_once()
        mock_kb.assert_not_awaited()
        mock_general.assert_awaited_once()
        mock_flow.assert_not_awaited()

    async def test_on_text_message_routes_to_small_talk_handler(self) -> None:
        update = _update_with_text("Спасибо")
        context = _context_with_flags()

        with patch.object(
            bot, "_handle_consultative_query", new_callable=AsyncMock, return_value=False
        ) as mock_consult, patch.object(
            bot, "_answer_knowledge_question", new_callable=AsyncMock
        ) as mock_kb, patch.object(
            bot, "_load_current_state_payload", return_value={"state": "ask_goal", "criteria": {}, "contact": None}
        ), patch.object(
            bot, "_prepare_effective_text_and_context", return_value=("Спасибо", {})
        ), patch.object(
            bot, "_answer_small_talk", new_callable=AsyncMock, return_value=True
        ) as mock_small, patch.object(
            bot, "_handle_flow_step", new_callable=AsyncMock
        ) as mock_flow:
            await bot.on_text_message(update, context)

        mock_consult.assert_awaited_once()
        mock_kb.assert_not_awaited()
        mock_small.assert_awaited_once()
        mock_flow.assert_not_awaited()

    async def test_on_text_message_routes_flow_interrupt_question_to_general_help(self) -> None:
        update = _update_with_text("Можно ли сначала разобраться со стратегией подготовки?")
        context = _context_with_flags()

        with patch.object(
            bot, "_load_current_state_payload", return_value={"state": "ask_subject", "criteria": {}, "contact": None}
        ), patch.object(
            bot, "_answer_presence_ping", new_callable=AsyncMock, return_value=False
        ), patch.object(
            bot, "_prepare_effective_text_and_context",
            return_value=("Можно ли сначала разобраться со стратегией подготовки?", {}),
        ), patch.object(
            bot, "_handle_consultative_query", new_callable=AsyncMock, return_value=False
        ), patch.object(
            bot, "_answer_knowledge_question", new_callable=AsyncMock
        ) as mock_kb, patch.object(
            bot, "_answer_general_education_question", new_callable=AsyncMock, return_value=True
        ) as mock_general, patch.object(
            bot, "_handle_flow_step", new_callable=AsyncMock
        ) as mock_flow:
            await bot.on_text_message(update, context)

        mock_kb.assert_not_awaited()
        mock_general.assert_awaited_once()
        mock_flow.assert_not_awaited()

    async def test_on_text_message_routes_presence_ping_before_flow(self) -> None:
        update = _update_with_text("ты тут?")
        context = _context_with_flags()

        with patch.object(
            bot, "_load_current_state_payload", return_value={"state": "ask_goal", "criteria": {}, "contact": None}
        ), patch.object(
            bot, "_answer_presence_ping", new_callable=AsyncMock, return_value=True
        ) as mock_ping, patch.object(
            bot, "_handle_consultative_query", new_callable=AsyncMock
        ) as mock_consult, patch.object(
            bot, "_handle_flow_step", new_callable=AsyncMock
        ) as mock_flow:
            await bot.on_text_message(update, context)

        mock_ping.assert_awaited_once()
        mock_consult.assert_not_awaited()
        mock_flow.assert_not_awaited()

    async def test_on_text_message_uses_forced_consultative_for_fragmented_thought(self) -> None:
        update = _update_with_text("Ты лучше понял, что мне нужно")
        context = _context_with_flags()

        with patch.object(
            bot, "_load_current_state_payload", return_value={"state": "ask_goal", "criteria": {}, "contact": None}
        ), patch.object(
            bot, "_answer_presence_ping", new_callable=AsyncMock, return_value=False
        ), patch.object(
            bot, "_prepare_effective_text_and_context", return_value=("Ты лучше понял, что мне нужно", {})
        ), patch.object(
            bot, "_handle_consultative_query", new_callable=AsyncMock, side_effect=[False, True]
        ) as mock_consult, patch.object(
            bot, "_handle_flow_step", new_callable=AsyncMock
        ) as mock_flow:
            await bot.on_text_message(update, context)

        self.assertEqual(mock_consult.await_count, 2)
        first_call = mock_consult.await_args_list[0]
        self.assertEqual(first_call.kwargs.get("text"), "Ты лучше понял, что мне нужно")
        self.assertEqual(first_call.kwargs.get("llm_text"), "Ты лучше понял, что мне нужно")
        forced_call = mock_consult.await_args_list[1]
        self.assertEqual(forced_call.kwargs.get("force"), True)
        mock_flow.assert_not_awaited()

    async def test_on_text_message_routes_to_consultative_when_detected(self) -> None:
        update = _update_with_text("У меня ребенок в 11 классе, хочу поступить в МФТИ, что делать?")
        context = _context_with_flags()

        with patch.object(
            bot, "_load_current_state_payload", return_value={"state": "ask_grade", "criteria": {}, "contact": None}
        ), patch.object(
            bot, "_prepare_effective_text_and_context",
            return_value=("У меня ребенок в 11 классе, хочу поступить в МФТИ, что делать?", {}),
        ), patch.object(
            bot, "_handle_consultative_query", new_callable=AsyncMock, return_value=True
        ) as mock_consult, patch.object(bot, "_handle_flow_step", new_callable=AsyncMock) as mock_flow:
            await bot.on_text_message(update, context)

        mock_consult.assert_awaited_once()
        mock_flow.assert_not_awaited()

    async def test_on_text_message_routes_program_info_to_knowledge_before_consultative(self) -> None:
        update = _update_with_text("Что ты знаешь про it лагерь УНПК МФТИ?")
        context = _context_with_flags()

        with patch.object(
            bot, "_load_current_state_payload", return_value={"state": "ask_grade", "criteria": {}, "contact": None}
        ), patch.object(
            bot, "_prepare_effective_text_and_context",
            return_value=("Что ты знаешь про it лагерь УНПК МФТИ?", {}),
        ), patch.object(
            bot, "_answer_knowledge_question", new_callable=AsyncMock
        ) as mock_answer, patch.object(
            bot, "_handle_consultative_query", new_callable=AsyncMock
        ) as mock_consult, patch.object(
            bot.db_module, "get_connection", return_value=_DummyConn()
        ), patch.object(
            bot, "_get_or_create_user_id", return_value=1
        ), patch.object(
            bot.db_module, "log_message"
        ), patch.object(
            bot, "_handle_flow_step", new_callable=AsyncMock
        ) as mock_flow:
            await bot.on_text_message(update, context)

        mock_answer.assert_awaited_once()
        mock_consult.assert_not_awaited()
        mock_flow.assert_not_awaited()

    async def test_on_text_message_prioritizes_consultative_for_mixed_query(self) -> None:
        update = _update_with_text("Хочу поступить в МФТИ, подскажите условия оплаты и что делать дальше?")
        context = _context_with_flags()

        with patch.object(
            bot, "_load_current_state_payload", return_value={"state": "ask_grade", "criteria": {}, "contact": None}
        ), patch.object(
            bot, "_prepare_effective_text_and_context",
            return_value=("Хочу поступить в МФТИ, подскажите условия оплаты и что делать дальше?", {}),
        ), patch.object(
            bot, "_handle_consultative_query", new_callable=AsyncMock, return_value=True
        ) as mock_consult, patch.object(
            bot, "_answer_knowledge_question", new_callable=AsyncMock
        ) as mock_answer, patch.object(
            bot, "_handle_flow_step", new_callable=AsyncMock
        ) as mock_flow:
            await bot.on_text_message(update, context)

        mock_consult.assert_awaited_once()
        mock_answer.assert_not_awaited()
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

    async def test_on_text_message_skips_duplicate_updates(self) -> None:
        update = _update_with_text("Что такое косинус?")
        update.update_id = 1001
        context = _context_with_flags()

        with patch.object(bot, "_is_duplicate_update", return_value=True), patch.object(
            bot, "_handle_consultative_query", new_callable=AsyncMock
        ) as mock_consult, patch.object(
            bot, "_handle_flow_step", new_callable=AsyncMock
        ) as mock_flow:
            await bot.on_text_message(update, context)

        mock_consult.assert_not_awaited()
        mock_flow.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
