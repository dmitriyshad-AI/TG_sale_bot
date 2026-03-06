import tempfile
import unittest
from pathlib import Path

from sales_agent.sales_core.call_copilot import (
    build_call_insights,
    build_transcript_fallback,
    extract_transcript_from_file,
)


class CallCopilotTests(unittest.TestCase):
    def test_build_call_insights_hot(self) -> None:
        text = (
            "Добрый день. Хотим записаться на курс по математике ЕГЭ. "
            "Подскажите, когда старт и как оплатить?"
        )
        insights = build_call_insights(text)
        self.assertEqual(insights.warmth, "hot")
        self.assertGreaterEqual(insights.score, 80.0)
        self.assertIn("математика", insights.interests)
        self.assertIn("ЕГЭ", insights.interests)
        self.assertIn("Связаться", insights.next_best_action)

    def test_build_call_insights_cold(self) -> None:
        text = "Спасибо, но пока не актуально. Дорого, подумаем позже."
        insights = build_call_insights(text)
        self.assertEqual(insights.warmth, "cold")
        self.assertLessEqual(insights.score, 40.0)
        self.assertIn("цена", insights.objections)

    def test_extract_transcript_from_txt_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "call.txt"
            file_path.write_text("Привет, интересует лагерь по физике.", encoding="utf-8")
            transcript = extract_transcript_from_file(file_path)
            self.assertIn("интересует лагерь", transcript)

    def test_extract_transcript_returns_empty_for_binary_extension(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "call.m4a"
            file_path.write_bytes(b"\x00\x01\x02")
            transcript = extract_transcript_from_file(file_path)
            self.assertEqual(transcript, "")

    def test_build_transcript_fallback_prefers_hint(self) -> None:
        transcript = build_transcript_fallback(
            source_type="url",
            source_ref="https://example.com/recording/1",
            transcript_hint="Клиент интересуется подготовкой к ОГЭ.",
        )
        self.assertIn("ОГЭ", transcript)
        self.assertNotIn("Автотранскрипт", transcript)

    def test_build_transcript_fallback_for_url_without_hint(self) -> None:
        transcript = build_transcript_fallback(
            source_type="url",
            source_ref="https://example.com/recording/1",
            transcript_hint=None,
        )
        self.assertIn("example.com", transcript)
        self.assertIn("Автотранскрипт", transcript)


if __name__ == "__main__":
    unittest.main()
