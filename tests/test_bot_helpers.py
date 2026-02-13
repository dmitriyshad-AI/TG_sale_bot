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
        _format_product_blurb,
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
    def test_apply_start_meta_sets_valid_brand(self) -> None:
        state = {"state": "ask_grade", "criteria": {"brand": "kmipt"}, "contact": None}
        updated = _apply_start_meta_to_state(state, {"brand": "foton", "source": "site"})
        self.assertEqual(updated["criteria"]["brand"], "foton")

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

    def test_build_user_name_from_first_and_last_name(self) -> None:
        update = SimpleNamespace(effective_user=SimpleNamespace(first_name="Ivan", last_name="Petrov"))
        self.assertEqual(_build_user_name(update), "Ivan Petrov")


if __name__ == "__main__":
    unittest.main()
