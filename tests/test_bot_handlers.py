import unittest
import json
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


def _make_update_with_web_app_data(data: str):
    message = AsyncMock()
    message.text = None
    message.web_app_data = SimpleNamespace(data=data)
    message.reply_text = AsyncMock()
    return SimpleNamespace(
        message=message,
        callback_query=None,
        effective_user=_make_user(),
        effective_chat=SimpleNamespace(id=5001),
        update_id=9001,
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
        with patch.object(bot, "settings", SimpleNamespace(telegram_bot_token="", telegram_mode="polling")):
            with self.assertRaises(RuntimeError):
                bot.main()

    def test_main_builds_application_and_starts_polling(self) -> None:
        app_mock = MagicMock()
        builder = MagicMock()
        builder.token.return_value = builder
        builder.build.return_value = app_mock

        with patch.object(
            bot,
            "settings",
            SimpleNamespace(telegram_bot_token="tg-token", telegram_mode="polling"),
        ), patch.object(
            bot, "ApplicationBuilder", return_value=builder
        ):
            bot.main()

        builder.token.assert_called_once_with("tg-token")
        self.assertEqual(app_mock.add_handler.call_count, 8)
        app_mock.run_polling.assert_called_once()

    def test_main_raises_in_webhook_mode(self) -> None:
        with patch.object(
            bot,
            "settings",
            SimpleNamespace(telegram_bot_token="tg-token", telegram_mode="webhook"),
        ):
            with self.assertRaises(RuntimeError):
                bot.main()

    def test_resolve_user_webapp_url_uses_admin_url_fallback(self) -> None:
        with patch.object(
            bot,
            "settings",
            SimpleNamespace(user_webapp_url="", admin_webapp_url="https://example.com/admin/miniapp"),
        ):
            self.assertEqual(bot._resolve_user_webapp_url(), "https://example.com/app")

    def test_build_user_miniapp_markup_returns_none_when_url_not_configured(self) -> None:
        with patch.object(
            bot,
            "settings",
            SimpleNamespace(user_webapp_url="", admin_webapp_url=""),
        ):
            self.assertIsNone(bot._build_user_miniapp_markup())

    def test_build_user_miniapp_markup_returns_button_when_url_configured(self) -> None:
        with patch.object(
            bot,
            "settings",
            SimpleNamespace(user_webapp_url="https://example.com/app", admin_webapp_url=""),
        ):
            markup = bot._build_user_miniapp_markup()
        self.assertIsNotNone(markup)
        inline = getattr(markup, "inline_keyboard", [])
        self.assertTrue(inline and inline[0])
        button = inline[0][0]
        self.assertEqual(getattr(getattr(button, "web_app", None), "url", None), "https://example.com/app")


@unittest.skipUnless(HAS_BOT_DEPS, "bot dependencies are not installed")
class BotAsyncCoverageTests(unittest.IsolatedAsyncioTestCase):
    async def test_reply_sends_keyboard_markup_when_layout_provided(self) -> None:
        update = _make_update_with_message("hello")
        bot._OUTBOUND_REPLY_DEDUP_CACHE.clear()
        await bot._reply(update, "reply", keyboard_layout=[[("Кнопка", "cb:data")]])
        update.message.reply_text.assert_awaited_once()
        kwargs = update.message.reply_text.call_args.kwargs
        self.assertIn("reply_markup", kwargs)
        self.assertIsNotNone(kwargs["reply_markup"])

    async def test_reply_noop_when_no_target_message(self) -> None:
        update = SimpleNamespace(message=None, callback_query=None)
        await bot._reply(update, "hello")

    async def test_reply_suppresses_duplicate_for_same_update(self) -> None:
        update = _make_update_with_message("hello")
        update.update_id = 777
        bot._OUTBOUND_REPLY_DEDUP_CACHE.clear()

        await bot._reply(update, "Один и тот же ответ")
        await bot._reply(update, "Один и тот же ответ")

        update.message.reply_text.assert_awaited_once()

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

    async def test_answer_general_education_question_replies_and_logs(self) -> None:
        update = _make_update_with_message("что такое косинус?")
        llm_result = SimpleNamespace(
            answer_text="Косинус — отношение прилежащего катета к гипотенузе.",
            used_fallback=False,
            error=None,
        )
        llm_client = SimpleNamespace(build_general_help_reply_async=AsyncMock(return_value=llm_result))

        with patch.object(bot, "LLMClient", return_value=llm_client), patch.object(
            bot.db_module, "get_connection", return_value=_DummyConn()
        ), patch.object(bot, "_get_or_create_user_id", return_value=1), patch.object(
            bot.db_module, "list_recent_messages", return_value=[]
        ), patch.object(
            bot.db_module, "log_message"
        ):
            handled = await bot._answer_general_education_question(
                update=update,
                question="что такое косинус?",
                current_state="ask_subject",
            )

        self.assertTrue(handled)
        update.message.reply_text.assert_awaited_once()
        text = update.message.reply_text.call_args.args[0]
        self.assertIn("Косинус", text)
        self.assertIn("вернемся к вашему плану", text)

    async def test_answer_general_education_question_returns_false_without_target(self) -> None:
        update = SimpleNamespace(message=None, callback_query=None)
        handled = await bot._answer_general_education_question(
            update=update,
            question="что такое косинус?",
            current_state="ask_subject",
        )
        self.assertFalse(handled)

    async def test_answer_small_talk_replies_with_prompt_for_active_state(self) -> None:
        update = _make_update_with_message("Спасибо")
        current_state_payload = {
            "state": "ask_grade",
            "criteria": {"brand": "kmipt", "grade": None, "goal": None, "subject": None, "format": None},
            "contact": None,
        }
        llm_result = SimpleNamespace(
            answer_text="Пожалуйста, рад помочь.",
            used_fallback=False,
            error=None,
        )
        llm_client = SimpleNamespace(build_general_help_reply_async=AsyncMock(return_value=llm_result))

        with patch.object(bot.db_module, "get_connection", return_value=_DummyConn()), patch.object(
            bot, "_get_or_create_user_id", return_value=1
        ), patch.object(bot.db_module, "list_recent_messages", return_value=[]), patch.object(
            bot.db_module, "log_message"
        ), patch.object(bot, "LLMClient", return_value=llm_client):
            handled = await bot._answer_small_talk(
                update=update,
                text="Спасибо",
                current_state_payload=current_state_payload,
            )

        self.assertTrue(handled)
        update.message.reply_text.assert_awaited_once()
        reply_text = update.message.reply_text.call_args.args[0]
        self.assertIn("Пожалуйста", reply_text)

    async def test_adminapp_returns_disabled_message_when_feature_is_off(self) -> None:
        update = _make_update_with_message("/adminapp")
        with patch.object(bot.db_module, "get_connection", return_value=_DummyConn()), patch.object(
            bot, "_get_or_create_user_id", return_value=1
        ), patch.object(bot.db_module, "log_message"), patch.object(
            bot,
            "settings",
            SimpleNamespace(
                database_path=Path("/tmp/test.db"),
                admin_miniapp_enabled=False,
                admin_webapp_url="https://example.com/admin/miniapp",
                admin_telegram_ids=(101,),
            ),
        ), patch.object(bot, "_reply", new_callable=AsyncMock) as mock_reply:
            await bot.adminapp(update=update, context=SimpleNamespace(args=[], user_data={}))

        mock_reply.assert_awaited()
        self.assertIn("выключен", mock_reply.await_args_list[0].args[1].lower())

    async def test_app_returns_no_url_message_when_not_configured(self) -> None:
        update = _make_update_with_message("/app")
        with patch.object(bot.db_module, "get_connection", return_value=_DummyConn()), patch.object(
            bot, "_get_or_create_user_id", return_value=1
        ), patch.object(bot.db_module, "log_message"), patch.object(
            bot,
            "settings",
            SimpleNamespace(
                database_path=Path("/tmp/test.db"),
                user_webapp_url="",
                admin_webapp_url="",
            ),
        ), patch.object(bot, "_reply", new_callable=AsyncMock) as mock_reply:
            await bot.app(update=update, context=SimpleNamespace(args=[], user_data={}))

        mock_reply.assert_awaited()
        self.assertIn("не настроен", mock_reply.await_args_list[0].args[1].lower())

    async def test_app_returns_webapp_button_when_url_configured(self) -> None:
        update = _make_update_with_message("/app")
        with patch.object(bot.db_module, "get_connection", return_value=_DummyConn()), patch.object(
            bot, "_get_or_create_user_id", return_value=1
        ), patch.object(bot.db_module, "log_message"), patch.object(
            bot,
            "settings",
            SimpleNamespace(
                database_path=Path("/tmp/test.db"),
                user_webapp_url="https://example.com/app",
                admin_webapp_url="",
            ),
        ):
            await bot.app(update=update, context=SimpleNamespace(args=[], user_data={}))

        update.message.reply_text.assert_awaited_once()
        kwargs = update.message.reply_text.call_args.kwargs
        self.assertIn("reply_markup", kwargs)
        self.assertIsNotNone(kwargs["reply_markup"])

    async def test_adminapp_returns_forbidden_for_non_admin(self) -> None:
        update = _make_update_with_message("/adminapp")
        with patch.object(bot.db_module, "get_connection", return_value=_DummyConn()), patch.object(
            bot, "_get_or_create_user_id", return_value=1
        ), patch.object(bot.db_module, "log_message"), patch.object(
            bot,
            "settings",
            SimpleNamespace(
                database_path=Path("/tmp/test.db"),
                admin_miniapp_enabled=True,
                admin_webapp_url="https://example.com/admin/miniapp",
                admin_telegram_ids=(999,),
            ),
        ), patch.object(bot, "_reply", new_callable=AsyncMock) as mock_reply:
            await bot.adminapp(update=update, context=SimpleNamespace(args=[], user_data={}))

        mock_reply.assert_awaited()
        self.assertIn("ограничен", mock_reply.await_args_list[0].args[1].lower())

    async def test_adminapp_returns_webapp_button_for_admin(self) -> None:
        update = _make_update_with_message("/adminapp")
        with patch.object(bot.db_module, "get_connection", return_value=_DummyConn()), patch.object(
            bot, "_get_or_create_user_id", return_value=1
        ), patch.object(bot.db_module, "log_message"), patch.object(
            bot,
            "settings",
            SimpleNamespace(
                database_path=Path("/tmp/test.db"),
                admin_miniapp_enabled=True,
                admin_webapp_url="https://example.com/admin/miniapp",
                admin_telegram_ids=(101,),
            ),
        ):
            await bot.adminapp(update=update, context=SimpleNamespace(args=[], user_data={}))

        update.message.reply_text.assert_awaited_once()
        kwargs = update.message.reply_text.call_args.kwargs
        self.assertIn("reply_markup", kwargs)
        self.assertIsNotNone(kwargs["reply_markup"])

    async def test_on_web_app_data_handles_catalog_payload(self) -> None:
        payload = {
            "flow": "catalog",
            "criteria": {"grade": 11, "goal": "ege", "subject": "physics", "format": "online"},
            "top": [{"id": "p-1", "title": "ЕГЭ по физике", "url": "https://kmipt.ru/courses/EGE/fizika_ege/"}],
        }
        update = _make_update_with_web_app_data(json.dumps(payload, ensure_ascii=False))
        context = SimpleNamespace(user_data={})

        with patch.object(bot, "_is_duplicate_update", return_value=False), patch.object(
            bot.db_module, "get_connection", return_value=_DummyConn()
        ), patch.object(bot, "_get_or_create_user_id", return_value=1), patch.object(
            bot.db_module, "log_message"
        ) as mock_log:
            await bot.on_web_app_data(update=update, context=context)

        update.message.reply_text.assert_awaited_once()
        reply_text = update.message.reply_text.call_args.args[0]
        self.assertIn("Mini App", reply_text)
        self.assertIn("ЕГЭ по физике", reply_text)
        self.assertIn("Класс: 11", reply_text)
        self.assertGreaterEqual(mock_log.call_count, 2)

    async def test_on_web_app_data_handles_invalid_payload(self) -> None:
        update = _make_update_with_web_app_data("{invalid")
        context = SimpleNamespace(user_data={})

        with patch.object(bot, "_is_duplicate_update", return_value=False), patch.object(
            bot.db_module, "get_connection", return_value=_DummyConn()
        ), patch.object(bot, "_get_or_create_user_id", return_value=1), patch.object(
            bot.db_module, "log_message"
        ):
            await bot.on_web_app_data(update=update, context=context)

        update.message.reply_text.assert_awaited_once()
        reply_text = update.message.reply_text.call_args.args[0]
        self.assertIn("не смог их распознать", reply_text)

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
        ), patch.object(
            bot.db_module, "get_conversation_context", return_value={}
        ), patch.object(bot.db_module, "upsert_session_state"), patch.object(
            bot, "advance_flow", return_value=step
        ), patch.object(
            bot, "_humanize_flow_message", new_callable=AsyncMock, return_value="Следующий шаг"
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
        ), patch.object(
            bot.db_module, "get_conversation_context", return_value={}
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
        self.assertNotIn("Оставьте телефон", text)

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
        ), patch.object(
            bot.db_module, "get_conversation_context", return_value={}
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
        ), patch.object(
            bot.db_module, "get_conversation_context", return_value={}
        ), patch.object(bot.db_module, "upsert_session_state"), patch.object(
            bot, "advance_flow", return_value=step
        ), patch.object(
            bot, "_humanize_flow_message", new_callable=AsyncMock, return_value="Спасибо! Заявка сохранена."
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

    async def test_start_sends_miniapp_button_when_user_webapp_url_configured(self) -> None:
        update = _make_update_with_message("/start")
        context = SimpleNamespace(user_data={}, args=[])
        prompt = FlowStep(message="Укажите класс", next_state="ask_grade", state_data={}, keyboard=[])
        base_state = {"state": "ask_grade", "criteria": {"brand": "kmipt"}, "contact": None}

        with patch.object(
            bot,
            "settings",
            SimpleNamespace(
                database_path=Path("/tmp/test.db"),
                brand_default="kmipt",
                user_webapp_url="https://example.com/app",
                admin_webapp_url="",
            ),
        ), patch.object(bot, "ensure_state", return_value=base_state), patch.object(
            bot, "parse_start_payload", return_value={}
        ), patch.object(bot, "build_prompt", return_value=prompt), patch.object(
            bot, "build_greeting_hint", return_value=""
        ), patch.object(bot.db_module, "get_connection", return_value=_DummyConn()), patch.object(
            bot, "_get_or_create_user_id", return_value=1
        ), patch.object(bot.db_module, "log_message"), patch.object(
            bot.db_module, "upsert_session_state"
        ), patch.object(
            bot, "_reply", new_callable=AsyncMock
        ):
            await bot.start(update, context)

        update.message.reply_text.assert_awaited_once()
        kwargs = update.message.reply_text.call_args.kwargs
        self.assertIn("reply_markup", kwargs)
        self.assertIsNotNone(kwargs["reply_markup"])

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
            bot.db_module, "list_recent_messages", return_value=[]
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
        self.assertIn("Понял вас, цель поступить в МФТИ", response_text)
        self.assertIn("Как удобнее заниматься", response_text)
        self.assertNotIn("Вот 2 направления", response_text)
        self.assertNotIn("профильный трек", response_text)

    async def test_handle_consultative_query_uses_llm_text_for_semantics(self) -> None:
        update = _make_update_with_message("Ты лучше понял?")
        session_state = {
            "state": "ask_goal",
            "criteria": {"brand": "kmipt", "grade": None, "goal": None, "subject": None, "format": None},
            "contact": None,
        }
        prompt = FlowStep(
            message="Какая цель подготовки?",
            next_state="ask_goal",
            state_data=session_state,
            keyboard=[],
        )
        llm_reply = SimpleNamespace(
            answer_text="Соберу аккуратный план, чтобы без перегруза прийти к цели.",
            next_question="Какая цель подготовки?",
            call_to_action="",
            recommended_product_ids=[],
            used_fallback=False,
            error=None,
        )
        llm_client = SimpleNamespace(build_consultative_reply_async=AsyncMock(return_value=llm_reply))
        semantic_text = "У меня ученик 10 класса. Хочу стратегию поступления в МФТИ."

        with patch.object(bot.db_module, "get_connection", return_value=_DummyConn()), patch.object(
            bot, "_get_or_create_user_id", return_value=1
        ), patch.object(
            bot.db_module, "list_recent_messages", return_value=[]
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
            bot, "LLMClient", return_value=llm_client
        ), patch.object(
            bot, "_reply", new_callable=AsyncMock
        ):
            handled = await bot._handle_consultative_query(
                update=update,
                text="Ты лучше понял?",
                force=True,
                llm_text=semantic_text,
            )

        self.assertTrue(handled)
        llm_client.build_consultative_reply_async.assert_awaited_once()
        self.assertEqual(
            llm_client.build_consultative_reply_async.await_args.kwargs["user_message"],
            semantic_text,
        )


if __name__ == "__main__":
    unittest.main()
