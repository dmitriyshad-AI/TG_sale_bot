import unittest
from datetime import datetime, timedelta

from sales_agent.sales_bot.context_engine import (
    build_context_summary_text,
    build_stitched_user_text,
    extract_intent_tags,
    merge_unique_texts,
    parse_db_timestamp,
)


class ContextEngineTests(unittest.TestCase):
    def test_parse_db_timestamp_handles_common_formats(self) -> None:
        self.assertIsNotNone(parse_db_timestamp("2026-02-18 12:30:45"))
        self.assertIsNotNone(parse_db_timestamp("2026-02-18T12:30:45"))
        self.assertIsNotNone(parse_db_timestamp("2026-02-18T12:30:45.123456"))
        self.assertIsNone(parse_db_timestamp("bad-ts"))

    def test_build_stitched_user_text_combines_recent_fragments(self) -> None:
        now = datetime.utcnow()
        stitched = build_stitched_user_text(
            current_text="Ты лучше понял, что мне нужно?",
            recent_messages=[
                {
                    "direction": "inbound",
                    "text": "У меня ученик 10 класса",
                    "created_at": (now - timedelta(seconds=40)).strftime("%Y-%m-%d %H:%M:%S"),
                },
                {
                    "direction": "inbound",
                    "text": "Хочу стратегию поступления в МФТИ",
                    "created_at": (now - timedelta(seconds=20)).strftime("%Y-%m-%d %H:%M:%S"),
                },
            ],
            normalize_text_fn=lambda value: " ".join(value.lower().split()),
            is_structured_flow_input_fn=lambda value: False,
            now_utc=now,
        )
        self.assertIn("У меня ученик 10 класса", stitched)
        self.assertIn("Хочу стратегию поступления в МФТИ", stitched)
        self.assertIn("Ты лучше понял", stitched)

    def test_build_stitched_user_text_does_not_expand_structured_input(self) -> None:
        now = datetime.utcnow()
        stitched = build_stitched_user_text(
            current_text="11",
            recent_messages=[
                {
                    "direction": "inbound",
                    "text": "Хочу стратегию поступления",
                    "created_at": (now - timedelta(seconds=10)).strftime("%Y-%m-%d %H:%M:%S"),
                },
            ],
            normalize_text_fn=lambda value: value.lower(),
            is_structured_flow_input_fn=lambda value: value.strip().isdigit(),
            now_utc=now,
        )
        self.assertEqual(stitched, "11")

    def test_merge_unique_texts_and_extract_intent_tags(self) -> None:
        merged = merge_unique_texts(
            [" Поступление  ", "поступление", "ЕГЭ", "егэ", "  "],
            normalize_text_fn=lambda value: value.strip().lower(),
            limit=10,
        )
        self.assertEqual(merged, ["Поступление", "ЕГЭ"])

        intents = extract_intent_tags(
            "Хочу поступить в МФТИ и уточнить оплату с рассрочкой",
            normalize_text_fn=lambda value: value.lower(),
        )
        self.assertIn("поступление", intents)
        self.assertIn("оплата", intents)

    def test_build_context_summary_text(self) -> None:
        summary = build_context_summary_text(
            {
                "profile": {"grade": 10, "goal": "ЕГЭ", "subject": "Физика", "target": "МФТИ"},
                "intents": ["поступление", "стратегия"],
                "recent_user_requests": ["Хочу план", "Как распределить нагрузку?"],
            }
        )
        self.assertIn("10 класс", summary)
        self.assertIn("Интересы", summary)
        self.assertIn("Последние запросы", summary)


if __name__ == "__main__":
    unittest.main()
