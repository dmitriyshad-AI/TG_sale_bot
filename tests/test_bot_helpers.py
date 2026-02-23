import unittest
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

try:
    from telegram import InlineKeyboardMarkup

    from sales_agent.sales_bot import bot
    from sales_agent.sales_bot.bot import (
        _apply_start_meta_to_state,
        _build_stitched_user_text,
        _build_consultative_fallback_text,
        _build_consultative_question,
        _build_webapp_data_reply_text,
        _build_inline_keyboard,
        _build_user_name,
        _criteria_from_state,
        _extract_webapp_top,
        _extract_goal_hint,
        _extract_grade_hint,
        _extract_subject_hint,
        _format_product_blurb,
        _format_soft_picks,
        _is_duplicate_outbound_reply,
        _is_consultative_query,
        _is_duplicate_update,
        _is_flow_interrupt_question,
        _is_general_education_query,
        _is_presence_ping,
        _is_program_info_query,
        _is_small_talk_message,
        _is_structured_flow_input,
        _looks_like_fragmented_context_message,
        _load_current_state_name,
        _missing_criteria_fields,
        _next_state_for_consultative,
        _normalize_webapp_grade,
        _normalize_webapp_label,
        _normalize_webapp_payload,
        _outbound_dedup_cache_key,
        _parse_db_timestamp,
        _recent_dialogue_for_llm,
        _resolve_vector_store_id,
        _select_recommended_products,
        _shorten_text,
        _should_offer_products,
        _target_message,
        _update_user_context_summary,
    )
    from sales_agent.sales_core import db as db_module
    from sales_agent.sales_core.catalog import SearchCriteria, parse_catalog

    HAS_BOT_DEPS = True
except ModuleNotFoundError:
    HAS_BOT_DEPS = False


def _sample_products():
    catalog = parse_catalog(
        {
            "products": [
                {
                    "id": "kmipt-ege-math",
                    "brand": "kmipt",
                    "title": "Математика ЕГЭ",
                    "url": "https://example.com/ege-math",
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
class BotHelpersTests(unittest.TestCase):
    def test_apply_start_meta_sets_kmipt_brand(self) -> None:
        state = {"state": "ask_grade", "criteria": {}, "contact": None}
        updated = _apply_start_meta_to_state(state, {"brand": "KMIPT"})
        self.assertEqual(updated["criteria"]["brand"], "kmipt")

    def test_apply_start_meta_ignores_non_kmipt_brand(self) -> None:
        state = {"state": "ask_grade", "criteria": {"brand": "kmipt"}, "contact": None}
        updated = _apply_start_meta_to_state(state, {"brand": "foton", "source": "site"})
        self.assertEqual(updated["criteria"]["brand"], "kmipt")

    def test_apply_start_meta_ignores_unknown_brand(self) -> None:
        state = {"state": "ask_grade", "criteria": {"brand": "kmipt"}, "contact": None}
        updated = _apply_start_meta_to_state(state, {"brand": "unknown"})
        self.assertEqual(updated["criteria"]["brand"], "kmipt")

    def test_criteria_from_state(self) -> None:
        state = {
            "criteria": {
                "brand": "kmipt",
                "grade": 10,
                "goal": "ege",
                "subject": "math",
                "format": "online",
            }
        }
        criteria = _criteria_from_state(state)
        self.assertEqual(
            criteria,
            SearchCriteria(brand="kmipt", grade=10, goal="ege", subject="math", format="online"),
        )

    def test_build_inline_keyboard(self) -> None:
        keyboard = _build_inline_keyboard([[("Кнопка", "cb:data")]])
        self.assertIsInstance(keyboard, InlineKeyboardMarkup)
        self.assertIsNone(_build_inline_keyboard(None))

    def test_format_product_blurb_for_empty_list(self) -> None:
        text = _format_product_blurb(SearchCriteria(brand="kmipt"), [])
        self.assertIn("не найдены", text.lower())

    def test_format_product_blurb_contains_link_and_reason(self) -> None:
        products = _sample_products()
        text = _format_product_blurb(
            SearchCriteria(brand="kmipt", grade=10, goal="ege", subject="math", format="online"),
            products,
        )
        self.assertIn("Математика ЕГЭ", text)
        self.assertIn("https://example.com/ege-math", text)
        self.assertIn("подходит для 10 класса", text)

    def test_resolve_vector_store_prefers_env_setting(self) -> None:
        fake_settings = SimpleNamespace(
            openai_vector_store_id="vs_env_123",
            vector_store_meta_path=Path("/tmp/missing.json"),
        )
        with patch.object(bot, "settings", fake_settings):
            self.assertEqual(_resolve_vector_store_id(), "vs_env_123")

    def test_resolve_vector_store_reads_from_meta_when_env_missing(self) -> None:
        fake_settings = SimpleNamespace(
            openai_vector_store_id="",
            vector_store_meta_path=Path("/tmp/vector_store_meta.json"),
        )
        with patch.object(bot, "settings", fake_settings), patch.object(
            bot, "load_vector_store_id", return_value="vs_meta_456"
        ):
            self.assertEqual(_resolve_vector_store_id(), "vs_meta_456")

    def test_target_message_prefers_callback_message(self) -> None:
        callback_message = SimpleNamespace()
        update = SimpleNamespace(callback_query=SimpleNamespace(message=callback_message), message=None)
        self.assertIs(_target_message(update), callback_message)

    def test_target_message_falls_back_to_plain_message(self) -> None:
        message = SimpleNamespace()
        update = SimpleNamespace(callback_query=None, message=message)
        self.assertIs(_target_message(update), message)

    def test_consultative_detection_and_hint_extractors(self) -> None:
        text = "У меня ребенок в 11 классе, хочу поступить в МФТИ, что делать?"
        self.assertTrue(_is_consultative_query(text))
        self.assertEqual(_extract_grade_hint(text), 11)
        self.assertEqual(_extract_goal_hint(text), "ege")
        self.assertIsNone(_extract_subject_hint(text))

    def test_consultative_detection_supports_hotel_wording(self) -> None:
        text = "Я хотел бы поступить в МФТИ, помогите с планом подготовки."
        self.assertTrue(_is_consultative_query(text))

    def test_consultative_query_is_not_knowledge_query(self) -> None:
        text = "Какие условия возврата и оплаты?"
        self.assertFalse(_is_consultative_query(text))

    def test_program_info_query_detection(self) -> None:
        text = "Что ты знаешь про it лагерь УНПК МФТИ?"
        self.assertTrue(_is_program_info_query(text))
        self.assertFalse(_is_consultative_query(text))

    def test_program_info_query_does_not_catch_strategy_question(self) -> None:
        text = "У меня ребенок в 11 классе, хочу поступить в МФТИ, что делать?"
        self.assertFalse(_is_program_info_query(text))
        self.assertTrue(_is_consultative_query(text))

    def test_general_education_query_detection(self) -> None:
        self.assertTrue(_is_general_education_query("Что такое косинус?"))
        self.assertTrue(_is_general_education_query("Объясни, как решать уравнения?"))
        self.assertFalse(_is_general_education_query("10"))
        self.assertFalse(_is_general_education_query("Онлайн"))
        self.assertFalse(_is_general_education_query("Хочу поступить в МФТИ, что делать?"))

    def test_small_talk_detection(self) -> None:
        self.assertTrue(_is_small_talk_message("Спасибо"))
        self.assertTrue(_is_small_talk_message("Понял"))
        self.assertFalse(_is_small_talk_message("Онлайн"))
        self.assertFalse(_is_small_talk_message("Хочу поступить в МФТИ"))

    def test_presence_ping_detection(self) -> None:
        self.assertTrue(_is_presence_ping("ты тут?"))
        self.assertTrue(_is_presence_ping("на связи"))
        self.assertFalse(_is_presence_ping("хочу курс по физике"))

    def test_structured_flow_input_accepts_punctuation_variants(self) -> None:
        self.assertTrue(_is_structured_flow_input("11?"))
        self.assertTrue(_is_structured_flow_input("ЕГЭ!"))
        self.assertTrue(_is_structured_flow_input("онлайн."))
        self.assertFalse(_is_structured_flow_input("  "))

    def test_flow_interrupt_question_detection(self) -> None:
        self.assertTrue(_is_flow_interrupt_question("Можно ли совмещать школу и подготовку?"))
        self.assertTrue(_is_flow_interrupt_question("Объясните, как распределить время между школой и кружками"))
        self.assertFalse(_is_flow_interrupt_question("11"))
        self.assertFalse(_is_flow_interrupt_question("онлайн"))

    def test_fragmented_context_message_detection(self) -> None:
        state = {"state": "ask_goal", "criteria": {"brand": "kmipt"}, "contact": None}
        self.assertTrue(
            _looks_like_fragmented_context_message(
                "Ты лучше понял, что мне нужно для поступления в МФТИ",
                state,
            )
        )
        self.assertFalse(_looks_like_fragmented_context_message("11", state))

    def test_build_stitched_user_text_combines_recent_inbound_fragments(self) -> None:
        class _DummyConn:
            pass

        now = datetime.utcnow()
        recent = [
            {
                "direction": "inbound",
                "text": "У меня ученик 10 класса",
                "created_at": (now - timedelta(seconds=35)).strftime("%Y-%m-%d %H:%M:%S"),
            },
            {
                "direction": "inbound",
                "text": "Хочу стратегию поступления в МФТИ",
                "created_at": (now - timedelta(seconds=15)).strftime("%Y-%m-%d %H:%M:%S"),
            },
        ]
        with patch.object(bot.db_module, "list_recent_messages", return_value=recent):
            stitched = _build_stitched_user_text(_DummyConn(), user_id=1, current_text="Ты лучше понял, что нужно?")

        self.assertIn("У меня ученик 10 класса", stitched)
        self.assertIn("Хочу стратегию поступления в МФТИ", stitched)
        self.assertIn("Ты лучше понял", stitched)

    def test_update_user_context_summary_persists_profile_and_summary_text(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "helper_context.db"
            db_module.init_db(db_path)
            conn = db_module.get_connection(db_path)
            try:
                user_id = db_module.get_or_create_user(conn, channel="telegram", external_id="1001")
                state = {
                    "state": "ask_subject",
                    "criteria": {"brand": "kmipt", "grade": 10, "goal": "ege", "subject": "physics", "format": None},
                    "contact": None,
                }
                summary = _update_user_context_summary(
                    conn,
                    user_id=user_id,
                    message_text="Хочу поступить в МФТИ, нужна стратегия",
                    state_payload=state,
                )

                self.assertEqual(summary.get("profile", {}).get("grade"), 10)
                self.assertIn("МФТИ", summary.get("profile", {}).get("target", ""))
                self.assertTrue(summary.get("summary_text"))
                stored = db_module.get_conversation_context(conn, user_id=user_id)
                self.assertEqual(stored.get("profile", {}).get("subject"), "Физика")
            finally:
                conn.close()

    def test_recent_dialogue_for_llm_filters_and_truncates(self) -> None:
        class _DummyConn:
            pass

        history = [
            {"direction": "inbound", "text": "/start"},
            {"direction": "unknown", "text": "ignored"},
            {"direction": "inbound", "text": ""},
            {"direction": "inbound", "text": "A" * 430},
            {"direction": "outbound", "text": "Ответ менеджера"},
        ]
        with patch.object(bot.db_module, "list_recent_messages", return_value=history):
            result = _recent_dialogue_for_llm(_DummyConn(), user_id=1, limit=8)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["role"], "user")
        self.assertEqual(len(result[0]["text"]), 400)
        self.assertEqual(result[1]["role"], "assistant")

    def test_parse_db_timestamp_returns_none_for_invalid(self) -> None:
        self.assertIsNone(_parse_db_timestamp(None))

    def test_update_user_context_summary_handles_non_dict_context(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "helper_context_invalid.db"
            db_module.init_db(db_path)
            conn = db_module.get_connection(db_path)
            try:
                user_id = db_module.get_or_create_user(conn, channel="telegram", external_id="2002")
                with patch.object(bot.db_module, "get_conversation_context", return_value="bad-context"):
                    summary = _update_user_context_summary(
                        conn,
                        user_id=user_id,
                        message_text="Хочу очно и цель поступить в МГУ",
                        state_payload={"criteria": {"format": "hybrid"}},
                    )
                self.assertEqual(summary["profile"]["target"], "МГУ")
                self.assertEqual(summary["profile"]["format"], "Гибрид")
            finally:
                conn.close()

    def test_next_state_for_consultative_does_not_loop_to_goal_when_complete(self) -> None:
        state = _next_state_for_consultative(
            {
                "brand": "kmipt",
                "grade": 11,
                "goal": "ege",
                "subject": "math",
                "format": "online",
            }
        )
        self.assertEqual(state, "suggest_products")

    def test_missing_criteria_fields_marks_empty_subject(self) -> None:
        missing = _missing_criteria_fields({"grade": 11, "goal": "ege", "subject": None, "format": "online"})
        self.assertEqual(missing, ["subject"])

    def test_format_soft_picks_and_consultative_question_branches(self) -> None:
        products = [
            SimpleNamespace(title="Курс 1", usp=["Фокус на задачах"]),
            SimpleNamespace(title="Курс 2", usp=[]),
        ]
        picks = _format_soft_picks(products)
        self.assertIn("Что уже может подойти", picks)
        self.assertIn("Курс 2", picks)
        self.assertIn("подходит для текущей цели", picks)
        self.assertEqual(_format_soft_picks([]), "")

        self.assertIn(
            "какой сейчас класс",
            _build_consultative_question({"grade": None, "goal": None, "subject": None, "format": None}, "fallback").lower(),
        )
        self.assertIn(
            "в приоритете",
            _build_consultative_question({"grade": 10, "goal": None, "subject": None, "format": None}, "fallback").lower(),
        )
        self.assertIn(
            "какой предмет",
            _build_consultative_question({"grade": 10, "goal": "ege", "subject": None, "format": None}, "fallback").lower(),
        )
        self.assertIn(
            "как удобнее",
            _build_consultative_question({"grade": 10, "goal": "ege", "subject": "math", "format": None}, "fallback").lower(),
        )
        self.assertIn(
            "2-3 программы",
            _build_consultative_question({"grade": 10, "goal": "ege", "subject": "math", "format": "online"}, "fallback"),
        )
        with patch.object(bot, "_missing_criteria_fields", return_value=["custom"]):
            self.assertEqual(_build_consultative_question({}, "fallback-question"), "fallback-question")

    def test_should_offer_products_branches(self) -> None:
        self.assertTrue(_should_offer_products(state_name="suggest_products", missing_fields=["grade"], user_text="x"))
        self.assertTrue(_should_offer_products(state_name="ask_grade", missing_fields=[], user_text="x"))
        self.assertTrue(
            _should_offer_products(state_name="ask_format", missing_fields=["format"], user_text="подберите курс")
        )
        self.assertFalse(
            _should_offer_products(state_name="ask_goal", missing_fields=["goal", "format"], user_text="привет")
        )

    def test_load_current_state_name_and_select_recommended_products(self) -> None:
        with patch.object(bot, "_load_current_state_payload", return_value={"state": "ask_subject"}):
            self.assertEqual(_load_current_state_name(SimpleNamespace()), "ask_subject")
        with patch.object(bot, "_load_current_state_payload", return_value={"state": 123}):
            self.assertIsNone(_load_current_state_name(SimpleNamespace()))

        p1 = SimpleNamespace(id="a", title="A")
        p2 = SimpleNamespace(id="b", title="B")
        self.assertEqual(_select_recommended_products([p1, p2], ["b", "b", "c"]), [p2])
        self.assertEqual(_select_recommended_products([p1, p2], ["zzz"]), [p1, p2])
        self.assertEqual(_select_recommended_products([], ["a"]), [])

    def test_build_consultative_fallback_text_repeated_and_regular(self) -> None:
        products = [SimpleNamespace(title="Курс", usp=["План поступления"])]
        repeated = _build_consultative_fallback_text(
            text="Цель поступить в МФТИ",
            criteria={"grade": 10, "goal": "ege"},
            products=products,
            next_question="Какой сейчас класс у ученика?",
            show_picks=True,
            repeated_without_new_info=True,
            repeat_count=3,
        )
        self.assertIn("Понял вас, цель поступить в МФТИ.", repeated)
        self.assertIn("Например: «11 класс».", repeated)

        repeated_mgu = _build_consultative_fallback_text(
            text="Хочу в МГУ",
            criteria={"grade": None, "goal": None},
            products=products,
            next_question="Какой предмет сейчас основной?",
            show_picks=False,
            repeated_without_new_info=True,
            repeat_count=2,
        )
        self.assertIn("цель поступить в МГУ", repeated_mgu)
        self.assertIn("Например: «математика».", repeated_mgu)

        regular_ege = _build_consultative_fallback_text(
            text="",
            criteria={"grade": 10, "goal": "ege"},
            products=products,
            next_question="Какой предмет в приоритете?",
            show_picks=False,
            repeated_without_new_info=False,
            repeat_count=0,
        )
        self.assertIn("Для 10 класса", regular_ege)
        self.assertIn("ЕГЭ", regular_ege)

        regular_olymp = _build_consultative_fallback_text(
            text="хочу поступить в МФТИ",
            criteria={"grade": None, "goal": "olympiad"},
            products=products,
            next_question="Уточните формат занятий",
            show_picks=True,
            repeated_without_new_info=False,
            repeat_count=0,
        )
        self.assertIn("олимпиадный трек", regular_olymp.lower())
        self.assertIn("Что уже может подойти", regular_olymp)

    def test_webapp_helpers_and_reply_text(self) -> None:
        self.assertEqual(_shorten_text("abcdef", max_len=5), "ab...")
        self.assertEqual(_shorten_text("abc", max_len=5), "abc")

        self.assertEqual(_normalize_webapp_payload("{bad-json"), {})
        self.assertEqual(_normalize_webapp_payload("[]"), {})
        self.assertEqual(_normalize_webapp_payload('{"flow":"catalog"}'), {"flow": "catalog"})

        self.assertEqual(_normalize_webapp_grade(7), 7)
        self.assertEqual(_normalize_webapp_grade("11"), 11)
        self.assertIsNone(_normalize_webapp_grade("12"))
        self.assertIsNone(_normalize_webapp_grade("abc"))
        self.assertIsNone(_normalize_webapp_grade(None))

        self.assertEqual(_normalize_webapp_label(" ege ", mapping={"ege": "ЕГЭ"}), "ЕГЭ")
        self.assertEqual(_normalize_webapp_label(" custom ", mapping={"ege": "ЕГЭ"}), "custom")
        self.assertIsNone(_normalize_webapp_label(" ", mapping={}))
        self.assertIsNone(_normalize_webapp_label(None, mapping={}))

        self.assertEqual(_extract_webapp_top({"top": "bad"}), [])
        top = _extract_webapp_top(
            {
                "top": [
                    "bad",
                    {"id": "2", "title": "Программа", "url": ""},
                    {"id": "3", "title": "", "url": "https://example.com"},
                    {"id": "4", "title": "Лишнее", "url": "https://example.com/4"},
                ]
            }
        )
        self.assertEqual(
            top,
            [
                {"id": "2", "title": "Программа", "url": ""},
                {"id": "3", "title": "Программа без названия", "url": "https://example.com"},
            ],
        )

        text_invalid, flow_invalid = _build_webapp_data_reply_text("{bad-json")
        self.assertEqual(flow_invalid, "unknown")
        self.assertIn("не смог их распознать", text_invalid)

        raw = (
            '{"flow":"catalog","criteria":{"grade":"10","goal":"ege","subject":"math","format":"online"},'
            '"top":[{"id":"p1","title":"Очень длинное название ' + ("x" * 200) + '","url":"https://example.com/p1"}]}'
        )
        text_valid, flow_valid = _build_webapp_data_reply_text(raw)
        self.assertEqual(flow_valid, "catalog")
        self.assertIn("Класс: 10", text_valid)
        self.assertIn("Варианты из Mini App:", text_valid)
        self.assertIn("Ссылка: https://example.com/p1", text_valid)

        no_criteria_text, _ = _build_webapp_data_reply_text('{"flow":"catalog","criteria":{},"top":[]}')
        self.assertIn("Параметры не переданы", no_criteria_text)

    def test_outbound_dedup_helpers(self) -> None:
        cache = bot._OUTBOUND_REPLY_DEDUP_CACHE
        cache.clear()
        try:
            update = SimpleNamespace(update_id=10, effective_chat=SimpleNamespace(id=100))
            self.assertIsNone(_outbound_dedup_cache_key(update, "   "))
            self.assertIsNone(_outbound_dedup_cache_key(SimpleNamespace(update_id=None, effective_chat=None), "x"))

            key = _outbound_dedup_cache_key(update, "Привет")
            self.assertEqual(key, "100:10:привет")

            self.assertFalse(_is_duplicate_outbound_reply(update, "Привет"))
            cache[key] = 0.0
            with patch.object(bot.time, "monotonic", return_value=bot.OUTBOUND_REPLY_DEDUP_WINDOW_SECONDS + 5):
                self.assertFalse(_is_duplicate_outbound_reply(update, "Привет"))

            with patch.object(bot.time, "monotonic", return_value=100.0):
                cache[key] = 99.5
                self.assertTrue(_is_duplicate_outbound_reply(update, "Привет"))
        finally:
            cache.clear()

    def test_build_user_name_from_first_and_last_name(self) -> None:
        update = SimpleNamespace(effective_user=SimpleNamespace(first_name="Ivan", last_name="Petrov"))
        self.assertEqual(_build_user_name(update), "Ivan Petrov")

    def test_is_duplicate_update_returns_false_without_update_id(self) -> None:
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=1),
            effective_chat=SimpleNamespace(id=100),
            update_id=None,
        )
        self.assertFalse(_is_duplicate_update(update))

    def test_is_duplicate_update_detects_repeat(self) -> None:
        class _DummyConn:
            def close(self):
                return None

        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=1),
            effective_chat=SimpleNamespace(id=100),
            update_id=5001,
        )
        state_without_runtime = {
            "state": "ask_grade",
            "criteria": {"brand": "kmipt", "grade": None, "goal": None, "subject": None, "format": None},
            "contact": None,
        }
        state_with_runtime = {
            **state_without_runtime,
            "_runtime": {"last_update_id": 5001},
        }

        with patch.object(bot.db_module, "get_connection", return_value=_DummyConn()), patch.object(
            bot, "_get_or_create_user_id", return_value=1
        ), patch.object(
            bot.db_module,
            "get_session",
            side_effect=[{"state": state_without_runtime, "meta": {}}, {"state": state_with_runtime, "meta": {}}],
        ), patch.object(bot.db_module, "upsert_session_state") as mock_upsert:
            self.assertFalse(_is_duplicate_update(update))
            self.assertTrue(_is_duplicate_update(update))

        mock_upsert.assert_called_once()


if __name__ == "__main__":
    unittest.main()
