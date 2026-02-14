import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

try:
    from sales_agent.sales_bot import bot
    from sales_agent.sales_core.catalog import SearchCriteria, parse_catalog
    from sales_agent.sales_core.flow import FlowStep

    HAS_BOT_DEPS = True
except ModuleNotFoundError:
    HAS_BOT_DEPS = False


class _DummyConn:
    def close(self) -> None:
        return None


def _make_user():
    return SimpleNamespace(id=101, username="user101", first_name="Ivan", last_name="Petrov")


def _make_update_with_message(text: str = "text"):
    message = AsyncMock()
    message.text = text
    message.reply_text = AsyncMock()
    return SimpleNamespace(
        message=message,
        callback_query=None,
        effective_user=_make_user(),
        effective_chat=SimpleNamespace(id=5001),
    )


def _make_callback_update(callback_data: str = "goal:ege"):
    callback_query = SimpleNamespace(
        data=callback_data,
        answer=AsyncMock(),
        message=AsyncMock(),
    )
    return SimpleNamespace(
        message=None,
        callback_query=callback_query,
        effective_user=_make_user(),
        effective_chat=SimpleNamespace(id=5001),
    )


def _sample_products():
    catalog = parse_catalog(
        {
            "products": [
                {
                    "id": "prod-1",
                    "brand": "kmipt",
                    "title": "Подготовка к ЕГЭ по математике",
                    "url": "https://example.com/p1",
                    "category": "ege",
                    "grade_min": 10,
                    "grade_max": 11,
                    "subjects": ["math"],
                    "format": "online",
                    "usp": ["u1", "u2", "u3"],
                }
            ]
        },
        Path("memory://catalog.yaml"),
    )
    return catalog.products


@unittest.skipUnless(HAS_BOT_DEPS, "bot dependencies are not installed")
class BotSyncCoverageTests(unittest.TestCase):
    def test_get_or_create_user_id_calls_db_layer(self) -> None:
        update = _make_update_with_message("hello")
        with patch.object(bot.db_module, "get_or_create_user", return_value=77) as mock_get:
            user_id = bot._get_or_create_user_id(update, conn=object())
        self.assertEqual(user_id, 77)
        self.assertEqual(mock_get.call_args.kwargs["external_id"], str(update.effective_user.id))
        self.assertEqual(mock_get.call_args.kwargs["channel"], "telegram")

    def test_select_products_uses_settings_paths(self) -> None:
        criteria = SearchCriteria(brand="kmipt", grade=10, goal="ege", subject="math", format="online")
        fake_settings = SimpleNamespace(catalog_path=Path("/tmp/catalog.yaml"), brand_default="foton")
        with patch.object(bot, "settings", fake_settings), patch.object(
            bot, "select_top_products", return_value=[]
        ) as mock_select:
            bot._select_products(criteria)
        self.assertEqual(mock_select.call_args.kwargs["path"], Path("/tmp/catalog.yaml"))
        self.assertEqual(mock_select.call_args.kwargs["brand_default"], "foton")

    def test_main_raises_when_token_missing(self) -> None:
        with patch.object(bot, "settings", SimpleNamespace(telegram_bot_token="")):
            with self.assertRaises(RuntimeError):
                bot.main()

    def test_main_builds_application_and_starts_polling(self) -> None:
        app_mock = MagicMock()
        builder = MagicMock()
        builder.token.return_value = builder
        builder.build.return_value = app_mock

        with patch.object(bot, "settings", SimpleNamespace(telegram_bot_token="tg-token")), patch.object(
            bot, "ApplicationBuilder", return_value=builder
        ):
            bot.main()

        builder.token.assert_called_once_with("tg-token")
        self.assertEqual(app_mock.add_handler.call_count, 5)
        app_mock.run_polling.assert_called_once()


@unittest.skipUnless(HAS_BOT_DEPS, "bot dependencies are not installed")
class BotAsyncCoverageTests(unittest.IsolatedAsyncioTestCase):
    async def test_reply_sends_keyboard_markup_when_layout_provided(self) -> None:
        update = _make_update_with_message("hello")
        await bot._reply(update, "reply", keyboard_layout=[[("Кнопка", "cb:data")]])
        update.message.reply_text.assert_awaited_once()
        kwargs = update.message.reply_text.call_args.kwargs
        self.assertIn("reply_markup", kwargs)
        self.assertIsNotNone(kwargs["reply_markup"])

    async def test_reply_noop_when_no_target_message(self) -> None:
        update = SimpleNamespace(message=None, callback_query=None)
        await bot._reply(update, "hello")

    async def test_create_lead_from_phone_rejects_invalid_phone(self) -> None:
        update = _make_update_with_message("bad")
        await bot._create_lead_from_phone(update=update, raw_phone="123", command_source="test")
        update.message.reply_text.assert_awaited_once()
        self.assertIn("Не удалось распознать номер", update.message.reply_text.call_args.args[0])

    async def test_create_lead_from_phone_no_target_message(self) -> None:
        update = SimpleNamespace(message=None, callback_query=None)
        await bot._create_lead_from_phone(update=update, raw_phone="+79991234567", command_source="test")

    async def test_create_lead_from_phone_success_path(self) -> None:
        update = _make_update_with_message("ok")
        crm_result = SimpleNamespace(success=True, entry_id="lead-42", error=None)
        crm_client = SimpleNamespace(create_lead_async=AsyncMock(return_value=crm_result))

        with patch.object(bot.db_module, "get_connection", return_value=_DummyConn()), patch.object(
            bot, "_get_or_create_user_id", return_value=1
        ), patch.object(bot, "_build_user_name", return_value="Ivan Petrov"), patch.object(
            bot.db_module, "create_lead_record"
        ) as mock_create_record, patch.object(bot.db_module, "log_message") as mock_log, patch.object(
            bot, "build_crm_client", return_value=crm_client
        ):
            await bot._create_lead_from_phone(
                update=update,
                raw_phone="+79991234567",
                command_source="telegram_leadtest_command",
            )

        update.message.reply_text.assert_awaited_once()
        self.assertIn("Лид создан", update.message.reply_text.call_args.args[0])
        mock_create_record.assert_called_once()
        mock_log.assert_called_once()

    async def test_create_lead_from_phone_failure_path(self) -> None:
        update = _make_update_with_message("ok")
        crm_result = SimpleNamespace(success=False, entry_id=None, error="bad request")
        crm_client = SimpleNamespace(create_lead_async=AsyncMock(return_value=crm_result))

        with patch.object(bot.db_module, "get_connection", return_value=_DummyConn()), patch.object(
            bot, "_get_or_create_user_id", return_value=1
        ), patch.object(bot, "_build_user_name", return_value="Ivan Petrov"), patch.object(
            bot.db_module, "create_lead_record"
        ), patch.object(bot.db_module, "log_message"), patch.object(
            bot, "build_crm_client", return_value=crm_client
        ):
            await bot._create_lead_from_phone(
                update=update,
                raw_phone="+79991234567",
                command_source="telegram_leadtest_command",
            )

        update.message.reply_text.assert_awaited_once()
        self.assertIn("Не удалось создать лид", update.message.reply_text.call_args.args[0])

    async def test_answer_knowledge_question_appends_sources(self) -> None:
        update = _make_update_with_message("kb")
        kb_result = SimpleNamespace(
            answer_text="Ответ из базы знаний",
            sources=["payments.md", "faq_general.md"],
            used_fallback=False,
            error=None,
        )
        llm_client = SimpleNamespace(answer_knowledge_question_async=AsyncMock(return_value=kb_result))

        with patch.object(bot, "LLMClient", return_value=llm_client), patch.object(
            bot.db_module, "get_connection", return_value=_DummyConn()
        ), patch.object(bot, "_get_or_create_user_id", return_value=1), patch.object(
            bot.db_module, "log_message"
        ):
            await bot._answer_knowledge_question(update=update, question="Как оплатить?")

        update.message.reply_text.assert_awaited_once()
        text = update.message.reply_text.call_args.args[0]
        self.assertIn("Ответ из базы знаний", text)
        self.assertIn("Источники:", text)

    async def test_answer_knowledge_question_no_target_message(self) -> None:
        update = SimpleNamespace(message=None, callback_query=None)
        await bot._answer_knowledge_question(update=update, question="Какие документы?")

    async def test_handle_flow_step_regular_prompt(self) -> None:
        update = _make_update_with_message("10")
        session_state = {"state": "ask_grade", "criteria": {}, "contact": None}
        step = FlowStep(
            message="Следующий шаг",
            next_state="ask_goal",
            state_data={"state": "ask_goal", "criteria": {}, "contact": None},
            keyboard=[],
        )

        with patch.object(bot.db_module, "get_connection", return_value=_DummyConn()), patch.object(
            bot, "_get_or_create_user_id", return_value=1
        ), patch.object(bot.db_module, "get_session", return_value={"state": session_state, "meta": {}}), patch.object(
            bot.db_module, "log_message"
        ), patch.object(bot.db_module, "upsert_session_state"), patch.object(
            bot, "advance_flow", return_value=step
        ), patch.object(bot, "_reply", new_callable=AsyncMock) as mock_reply:
            await bot._handle_flow_step(update=update, message_text="10")

        mock_reply.assert_awaited_once()
        self.assertEqual(mock_reply.call_args.args[1], "Следующий шаг")

    async def test_handle_flow_step_suggests_products_with_llm(self) -> None:
        update = _make_update_with_message("online")
        session_state = {
            "state": "ask_format",
            "criteria": {"brand": "kmipt", "grade": 10, "goal": "ege", "subject": "math", "format": "online"},
            "contact": None,
        }
        step = FlowStep(
            message="Подбираю",
            next_state="suggest_products",
            state_data=session_state,
            keyboard=[],
            should_suggest_products=True,
        )
        llm_reply = SimpleNamespace(
            answer_text="LLM текст",
            next_question="Удобно заниматься вечером?",
            call_to_action="Оставьте телефон",
        )
        llm_client = SimpleNamespace(build_sales_reply_async=AsyncMock(return_value=llm_reply))

        with patch.object(bot.db_module, "get_connection", return_value=_DummyConn()), patch.object(
            bot, "_get_or_create_user_id", return_value=1
        ), patch.object(bot.db_module, "get_session", return_value={"state": session_state, "meta": {}}), patch.object(
            bot.db_module, "log_message"
        ), patch.object(bot.db_module, "upsert_session_state"), patch.object(
            bot, "advance_flow", return_value=step
        ), patch.object(bot, "_select_products", return_value=_sample_products()), patch.object(
            bot, "_format_product_blurb", return_value="BLURB"
        ), patch.object(bot, "LLMClient", return_value=llm_client), patch.object(
            bot, "_reply", new_callable=AsyncMock
        ) as mock_reply:
            await bot._handle_flow_step(update=update, message_text="online")

        mock_reply.assert_awaited_once()
        text = mock_reply.call_args.args[1]
        self.assertIn("LLM текст", text)
        self.assertIn("BLURB", text)
        self.assertIn("Оставьте телефон", text)

    async def test_handle_flow_step_llm_exception_fallback(self) -> None:
        update = _make_update_with_message("online")
        session_state = {"state": "ask_format", "criteria": {"brand": "kmipt"}, "contact": None}
        step = FlowStep(
            message="Подбираю",
            next_state="suggest_products",
            state_data=session_state,
            keyboard=[],
            should_suggest_products=True,
        )

        with patch.object(bot.db_module, "get_connection", return_value=_DummyConn()), patch.object(
            bot, "_get_or_create_user_id", return_value=1
        ), patch.object(bot.db_module, "get_session", return_value={"state": session_state, "meta": {}}), patch.object(
            bot.db_module, "log_message"
        ), patch.object(bot.db_module, "upsert_session_state"), patch.object(
            bot, "advance_flow", return_value=step
        ), patch.object(bot, "_select_products", side_effect=RuntimeError("boom")), patch.object(
            bot, "_reply", new_callable=AsyncMock
        ) as mock_reply:
            await bot._handle_flow_step(update=update, message_text="online")

        text = mock_reply.call_args.args[1]
        self.assertIn("Подбор временно недоступен", text)

    async def test_handle_flow_step_creates_lead_after_contact_completion(self) -> None:
        update = _make_update_with_message("+79991234567")
        previous_state = {"state": "ask_contact", "criteria": {"brand": "kmipt"}, "contact": None}
        step = FlowStep(
            message="Спасибо! Заявка сохранена.",
            next_state="done",
            state_data={"state": "done", "criteria": {"brand": "kmipt"}, "contact": "+79991234567"},
            keyboard=[],
            completed=True,
        )

        with patch.object(bot.db_module, "get_connection", return_value=_DummyConn()), patch.object(
            bot, "_get_or_create_user_id", return_value=1
        ), patch.object(bot.db_module, "get_session", return_value={"state": previous_state, "meta": {}}), patch.object(
            bot.db_module, "log_message"
        ), patch.object(bot.db_module, "upsert_session_state"), patch.object(
            bot, "advance_flow", return_value=step
        ), patch.object(bot, "_reply", new_callable=AsyncMock), patch.object(
            bot, "_create_lead_from_phone", new_callable=AsyncMock
        ) as mock_create_lead:
            await bot._handle_flow_step(update=update, message_text="+79991234567")

        mock_create_lead.assert_awaited_once()
        self.assertEqual(mock_create_lead.call_args.kwargs["raw_phone"], "+79991234567")

    async def test_start_resets_flags_and_saves_session_meta(self) -> None:
        update = _make_update_with_message("/start")
        context = SimpleNamespace(
            user_data={
                bot.LEADTEST_WAITING_PHONE_KEY: True,
                bot.KBTEST_WAITING_QUESTION_KEY: True,
            },
            args=["payload-token"],
        )
        prompt = FlowStep(message="Укажите класс", next_state="ask_grade", state_data={}, keyboard=[])
        base_state = {"state": "ask_grade", "criteria": {"brand": "kmipt"}, "contact": None}

        with patch.object(bot, "ensure_state", return_value=base_state), patch.object(
            bot, "parse_start_payload", return_value={"source": "site", "page": "/courses/camp", "brand": "foton"}
        ), patch.object(bot, "build_prompt", return_value=prompt), patch.object(
            bot, "build_greeting_hint", return_value="HINT"
        ), patch.object(bot.db_module, "get_connection", return_value=_DummyConn()), patch.object(
            bot, "_get_or_create_user_id", return_value=1
        ), patch.object(bot.db_module, "log_message"), patch.object(
            bot.db_module, "upsert_session_state"
        ) as mock_upsert, patch.object(
            bot, "_reply", new_callable=AsyncMock
        ) as mock_reply:
            await bot.start(update, context)

        self.assertNotIn(bot.LEADTEST_WAITING_PHONE_KEY, context.user_data)
        self.assertNotIn(bot.KBTEST_WAITING_QUESTION_KEY, context.user_data)
        mock_upsert.assert_called_once()
        saved_meta = mock_upsert.call_args.kwargs["meta"]
        self.assertEqual(saved_meta["source"], "site")
        self.assertEqual(saved_meta["brand"], "foton")
        self.assertIn("HINT", mock_reply.call_args.args[1])

    async def test_start_noop_without_message(self) -> None:
        update = SimpleNamespace(message=None)
        context = SimpleNamespace(user_data={}, args=[])
        await bot.start(update, context)

    async def test_leadtest_with_args_calls_create_lead(self) -> None:
        update = _make_update_with_message("/leadtest")
        context = SimpleNamespace(user_data={}, args=["+79991234567"])
        with patch.object(bot.db_module, "get_connection", return_value=_DummyConn()), patch.object(
            bot, "_get_or_create_user_id", return_value=1
        ), patch.object(bot.db_module, "log_message"), patch.object(
            bot, "_create_lead_from_phone", new_callable=AsyncMock
        ) as mock_create:
            await bot.leadtest(update, context)
        mock_create.assert_awaited_once()

    async def test_leadtest_noop_without_message(self) -> None:
        update = SimpleNamespace(message=None)
        context = SimpleNamespace(user_data={}, args=[])
        await bot.leadtest(update, context)

    async def test_leadtest_without_args_sets_waiting_flag(self) -> None:
        update = _make_update_with_message("/leadtest")
        context = SimpleNamespace(user_data={}, args=[])
        with patch.object(bot.db_module, "get_connection", return_value=_DummyConn()), patch.object(
            bot, "_get_or_create_user_id", return_value=1
        ), patch.object(bot.db_module, "log_message"):
            await bot.leadtest(update, context)
        self.assertTrue(context.user_data.get(bot.LEADTEST_WAITING_PHONE_KEY))
        update.message.reply_text.assert_awaited_once()

    async def test_kbtest_with_args_calls_answer(self) -> None:
        update = _make_update_with_message("/kbtest")
        context = SimpleNamespace(user_data={}, args=["Какие документы нужны?"])
        with patch.object(bot, "_answer_knowledge_question", new_callable=AsyncMock) as mock_answer:
            await bot.kbtest(update, context)
        mock_answer.assert_awaited_once()

    async def test_kbtest_without_args_sets_waiting_flag(self) -> None:
        update = _make_update_with_message("/kbtest")
        context = SimpleNamespace(user_data={}, args=[])
        await bot.kbtest(update, context)
        self.assertTrue(context.user_data.get(bot.KBTEST_WAITING_QUESTION_KEY))
        update.message.reply_text.assert_awaited_once()

    async def test_kbtest_noop_without_message(self) -> None:
        update = SimpleNamespace(message=None)
        context = SimpleNamespace(user_data={}, args=[])
        await bot.kbtest(update, context)

    async def test_on_text_message_noop_without_message(self) -> None:
        update = SimpleNamespace(message=None)
        context = SimpleNamespace(user_data={}, args=[])
        await bot.on_text_message(update, context)

    async def test_handle_consultative_query_avoids_repeating_long_pitch(self) -> None:
        update = _make_update_with_message("поступить в МФТИ")
        session_state = {
            "state": "ask_subject",
            "criteria": {
                "brand": "kmipt",
                "grade": 11,
                "goal": "ege",
                "subject": "math",
                "format": None,
            },
            "contact": None,
            "consultative": {"last_text": "поступить в мфти", "turns": 1},
        }
        prompt = FlowStep(
            message="Какой формат удобнее?",
            next_state="ask_format",
            state_data=session_state,
            keyboard=[],
        )

        with patch.object(bot.db_module, "get_connection", return_value=_DummyConn()), patch.object(
            bot, "_get_or_create_user_id", return_value=1
        ), patch.object(
            bot.db_module, "get_session", return_value={"state": session_state, "meta": {}}
        ), patch.object(
            bot.db_module, "log_message"
        ), patch.object(
            bot.db_module, "upsert_session_state"
        ), patch.object(
            bot, "_select_products", return_value=_sample_products()
        ), patch.object(
            bot, "build_prompt", return_value=prompt
        ), patch.object(
            bot, "_reply", new_callable=AsyncMock
        ) as mock_reply:
            handled = await bot._handle_consultative_query(update=update, text="поступить в МФТИ")

        self.assertTrue(handled)
        mock_reply.assert_awaited_once()
        response_text = mock_reply.call_args.args[1]
        self.assertIn("Понял, цель поступить в МФТИ", response_text)
        self.assertIn("Какой формат удобнее", response_text)
        self.assertNotIn("Вот 2 направления", response_text)
        self.assertNotIn("По математике", response_text)


if __name__ == "__main__":
    unittest.main()
