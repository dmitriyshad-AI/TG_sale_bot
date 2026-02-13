import unittest

from sales_agent.sales_core.copilot import (
    import_dialogue,
    parse_telegram_export,
    parse_whatsapp_export,
    propose_reply,
    run_copilot_from_file,
    summarize_dialogue,
)


class CopilotTests(unittest.TestCase):
    def test_parse_whatsapp_export(self) -> None:
        raw = (
            "12/02/2026, 10:00 - Клиент: Добрый день, нужен курс ЕГЭ по математике для 10 класс\n"
            "12/02/2026, 10:05 - Менеджер: Здравствуйте! Подскажите удобный формат?\n"
        )
        messages = parse_whatsapp_export(raw)
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0]["role"], "client")
        self.assertIn("ЕГЭ", messages[0]["text"])

    def test_parse_telegram_export(self) -> None:
        payload = {
            "messages": [
                {"from": "Client", "date": "2026-02-12T10:00:00", "text": "Нужна олимпиада по физике"},
                {
                    "from": "Manager",
                    "date": "2026-02-12T10:05:00",
                    "text": [{"type": "plain", "text": "Есть несколько вариантов"}],
                },
            ]
        }
        messages = parse_telegram_export(payload)
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[1]["text"], "Есть несколько вариантов")

    def test_summarize_dialogue_extracts_profile(self) -> None:
        messages = [
            {
                "source": "whatsapp",
                "created_at": "12/02/2026 10:00",
                "sender": "Клиент",
                "role": "client",
                "text": "10 класс, интересует ЕГЭ по математике, но дорого",
            }
        ]
        summary, profile = summarize_dialogue(messages)
        self.assertIn("Класс: 10", summary)
        self.assertEqual(profile["goal"], "ege")
        self.assertEqual(profile["subject"], "math")
        self.assertIn("price", profile["objections"])

    def test_import_dialogue_detects_format(self) -> None:
        source_format, messages = import_dialogue(
            "export.json",
            b'{"messages":[{"from":"Client","date":"2026-01-01","text":"Hello"}]}',
        )
        self.assertEqual(source_format, "telegram_json")
        self.assertEqual(len(messages), 1)

    def test_import_dialogue_raises_on_invalid_telegram_json(self) -> None:
        with self.assertRaises(ValueError):
            import_dialogue("export.json", b"{broken-json")

    def test_run_copilot_returns_draft(self) -> None:
        content = (
            "12/02/2026, 10:00 - Клиент: 9 класс, ОГЭ по физике\n"
            "12/02/2026, 10:02 - Менеджер: Добрый день\n"
        ).encode("utf-8")
        result = run_copilot_from_file("chat.txt", content)
        self.assertGreater(result.message_count, 0)
        self.assertIn("класс", result.summary.lower())
        self.assertIn("Здравствуйте", result.draft_reply)

    def test_propose_reply_uses_catalog_context(self) -> None:
        draft = propose_reply(
            summary="summary",
            customer_profile={"grade": 8, "goal": "camp", "subject": "math", "objections": []},
            catalog_context=[{"title": "Курс 1"}],
        )
        self.assertIn("2-3 подходящие программы", draft)


if __name__ == "__main__":
    unittest.main()
