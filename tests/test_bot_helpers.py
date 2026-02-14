import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

try:
    from telegram import InlineKeyboardMarkup

    from sales_agent.sales_bot import bot
    from sales_agent.sales_bot.bot import (
        _apply_start_meta_to_state,
        _build_inline_keyboard,
        _build_user_name,
        _criteria_from_state,
        _extract_goal_hint,
        _extract_grade_hint,
        _extract_subject_hint,
        _format_product_blurb,
        _is_consultative_query,
        _is_duplicate_update,
        _is_general_education_query,
        _is_small_talk_message,
        _next_state_for_consultative,
        _resolve_vector_store_id,
        _target_message,
    )
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

    def test_consultative_query_is_not_knowledge_query(self) -> None:
        text = "Какие условия возврата и оплаты?"
        self.assertFalse(_is_consultative_query(text))

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
