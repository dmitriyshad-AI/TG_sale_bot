import unittest
from unittest.mock import Mock

from sales_agent.sales_core.copilot import (
    create_crm_copilot_task,
    create_tallanto_copilot_task,
    detect_source_format,
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

    def test_parse_whatsapp_export_merges_multiline_message(self) -> None:
        raw = (
            "12/02/2026, 10:00 - Клиент: Добрый день\n"
            "Нужна стратегия поступления в МФТИ\n"
            "12/02/2026, 10:05 - Менеджер: Понял, давайте уточним детали\n"
        )
        messages = parse_whatsapp_export(raw)
        self.assertEqual(len(messages), 2)
        self.assertIn("стратегия поступления", messages[0]["text"])
        self.assertIn("\n", messages[0]["text"])

    def test_parse_telegram_export_skips_invalid_items_and_empty_text(self) -> None:
        payload = {
            "messages": [
                42,
                {"from": "Client", "date": "2026-02-12T10:00:00", "text": ""},
                {
                    "from": "Client",
                    "date": "2026-02-12T10:02:00",
                    "text": ["Первая часть ", {"text": "и продолжение"}],
                },
                {"from": "Client", "date": "2026-02-12T10:05:00", "text": [{"type": "plain"}]},
            ]
        }
        messages = parse_telegram_export(payload)
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]["text"], "Первая часть и продолжение")

    def test_parse_telegram_export_returns_empty_when_messages_not_list(self) -> None:
        self.assertEqual(parse_telegram_export({"messages": "not-a-list"}), [])

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

    def test_summarize_dialogue_empty_input_returns_default_profile(self) -> None:
        summary, profile = summarize_dialogue([])
        self.assertIn("пустой", summary.lower())
        self.assertIsNone(profile["grade"])
        self.assertEqual(profile["objections"], [])

    def test_detect_source_format_by_filename_and_content(self) -> None:
        self.assertEqual(detect_source_format("dialog.json", "[]"), "telegram_json")
        self.assertEqual(detect_source_format("dialog.txt", "{}"), "whatsapp_txt")
        self.assertEqual(detect_source_format("dialog.bin", '{"messages":[{"text":"ok"}]}'), "telegram_json")
        self.assertEqual(detect_source_format("dialog.bin", "plain text"), "whatsapp_txt")

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

    def test_import_dialogue_raises_on_non_object_json(self) -> None:
        with self.assertRaises(ValueError):
            import_dialogue("export.json", b"[]")

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

    def test_propose_reply_mentions_budget_for_price_objection(self) -> None:
        draft = propose_reply(
            summary="summary",
            customer_profile={"grade": 10, "goal": "ege", "subject": "physics", "objections": ["price"]},
            catalog_context=None,
        )
        self.assertIn("бюджете", draft)

    def test_create_tallanto_copilot_task_uses_tasks_module(self) -> None:
        tallanto = Mock()
        tallanto.set_entry.return_value = {"ok": True}
        result = create_tallanto_copilot_task(
            tallanto=tallanto,
            summary="summary",
            draft_reply="draft",
            contact=None,
        )
        self.assertEqual(result, {"ok": True})
        tallanto.set_entry.assert_called_once()
        call = tallanto.set_entry.call_args.kwargs
        self.assertEqual(call["module"], "tasks")
        self.assertEqual(call["fields_values"]["contact"], "")

    def test_create_crm_copilot_task_proxies_to_client(self) -> None:
        crm = Mock()
        crm.create_copilot_task.return_value = {"ok": True}
        result = create_crm_copilot_task(
            crm=crm,
            summary="sum",
            draft_reply="reply",
            contact="+79990000000",
        )
        self.assertEqual(result, {"ok": True})
        crm.create_copilot_task.assert_called_once_with(
            summary="sum",
            draft_reply="reply",
            contact="+79990000000",
        )


if __name__ == "__main__":
    unittest.main()
