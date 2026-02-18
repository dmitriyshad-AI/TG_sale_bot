import unittest
from io import BytesIO
from pathlib import Path
from unittest.mock import AsyncMock, patch
from urllib.error import HTTPError, URLError

import httpx

try:
    from sales_agent.sales_core.catalog import SearchCriteria, parse_catalog
    from sales_agent.sales_core.llm_client import KnowledgeReply, LLMClient

    HAS_LLM_DEPS = True
except ModuleNotFoundError:
    HAS_LLM_DEPS = False


class _MockHTTPResponse:
    def __init__(self, body: str) -> None:
        self.body = body

    def read(self) -> bytes:
        return self.body.encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _MockAsyncResponse:
    def __init__(self, status_code: int, payload: dict) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = "{}"

    def json(self):
        return self._payload


class _MockAsyncInvalidJsonResponse:
    status_code = 200
    text = "{not-json"

    def json(self):
        raise ValueError("bad json")


class _MockAsyncClient:
    def __init__(self, response: _MockAsyncResponse) -> None:
        self.response = response

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, *args, **kwargs):
        return self.response


def _products():
    catalog = parse_catalog(
        {
            "products": [
                {
                    "id": "p01",
                    "brand": "kmipt",
                    "title": "Course 1",
                    "url": "https://example.com/p1",
                    "category": "ege",
                    "grade_min": 10,
                    "grade_max": 11,
                    "subjects": ["math"],
                    "format": "online",
                    "usp": ["u1", "u2", "u3"],
                },
                {
                    "id": "p02",
                    "brand": "kmipt",
                    "title": "Course 2",
                    "url": "https://example.com/p2",
                    "category": "ege",
                    "grade_min": 10,
                    "grade_max": 11,
                    "subjects": ["math"],
                    "format": "hybrid",
                    "usp": ["u1", "u2", "u3"],
                },
            ]
        },
        Path("memory://catalog.yaml"),
    )
    return catalog.products


@unittest.skipUnless(HAS_LLM_DEPS, "llm dependencies are not installed")
class LLMClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.criteria = SearchCriteria(brand="kmipt", grade=10, goal="ege", subject="math", format="online")
        self.top_products = _products()

    def test_fallback_when_no_api_key(self) -> None:
        client = LLMClient(api_key="", model="gpt-4.1")
        result = client.build_sales_reply(self.criteria, self.top_products)
        self.assertTrue(result.used_fallback)
        self.assertGreaterEqual(len(result.recommended_product_ids), 1)

    def test_knowledge_fallback_when_vector_store_missing(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        result = client.answer_knowledge_question("Как проходит оплата?", vector_store_id=None)
        self.assertTrue(result.used_fallback)
        self.assertIn("синхронизацию", result.answer_text.lower())

    @patch("sales_agent.sales_core.llm_client.urlopen")
    def test_parses_structured_response(self, mock_urlopen) -> None:
        mock_urlopen.return_value = _MockHTTPResponse(
            '{"output_text":"{\\"answer_text\\":\\"Подойдет вариант 1\\",\\"next_question\\":\\"Удобно ли онлайн?\\",\\"call_to_action\\":\\"Оставьте телефон\\",\\"recommended_product_ids\\":[\\"p01\\"]}"}'
        )
        client = LLMClient(api_key="sk-test", model="gpt-4.1")

        result = client.build_sales_reply(self.criteria, self.top_products)

        self.assertFalse(result.used_fallback)
        self.assertEqual(result.answer_text, "Подойдет вариант 1")
        self.assertEqual(result.recommended_product_ids, ["p01"])

    @patch("sales_agent.sales_core.llm_client.urlopen")
    def test_ignores_recommended_ids_outside_context(self, mock_urlopen) -> None:
        mock_urlopen.return_value = _MockHTTPResponse(
            '{"output_text":"{\\"answer_text\\":\\"Ответ\\",\\"next_question\\":null,\\"call_to_action\\":\\"Оставьте телефон\\",\\"recommended_product_ids\\":[\\"p01\\",\\"x999\\"]}"}'
        )
        client = LLMClient(api_key="sk-test", model="gpt-4.1")

        result = client.build_sales_reply(self.criteria, self.top_products)

        self.assertFalse(result.used_fallback)
        self.assertEqual(result.recommended_product_ids, ["p01"])

    @patch("sales_agent.sales_core.llm_client.urlopen")
    def test_fallback_on_invalid_llm_payload(self, mock_urlopen) -> None:
        mock_urlopen.return_value = _MockHTTPResponse('{"output_text":"not-json"}')
        client = LLMClient(api_key="sk-test", model="gpt-4.1")

        result = client.build_sales_reply(self.criteria, self.top_products)

        self.assertTrue(result.used_fallback)
        self.assertIsNotNone(result.error)

    @patch("sales_agent.sales_core.llm_client.urlopen")
    def test_knowledge_response_with_sources(self, mock_urlopen) -> None:
        mock_urlopen.return_value = _MockHTTPResponse(
            '{'
            '"output":[{"content":[{"text":"Оплата подтверждается после выставления счета.",'
            '"annotations":[{"filename":"payments.md"}]}]}]'
            '}'
        )
        client = LLMClient(api_key="sk-test", model="gpt-4.1")

        result = client.answer_knowledge_question(
            "Как происходит оплата?",
            vector_store_id="vs_test_123",
        )

        self.assertFalse(result.used_fallback)
        self.assertIn("счета", result.answer_text)
        self.assertEqual(result.sources, ["payments.md"])

    def test_sales_payload_uses_input_text_type(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        payload = client._build_sales_payload(self.criteria, self.top_products)
        self.assertEqual(payload["input"][0]["content"][0]["type"], "input_text")
        self.assertEqual(payload["input"][1]["content"][0]["type"], "input_text")
        self.assertIn("уважительный", payload["input"][0]["content"][0]["text"])

    def test_knowledge_payload_uses_input_text_type(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        payload = client._build_knowledge_payload(
            question="Как оплатить?",
            vector_store_id="vs_test_123",
        )
        self.assertEqual(payload["input"][0]["content"][0]["type"], "input_text")
        self.assertEqual(payload["input"][1]["content"][0]["type"], "input_text")

    def test_site_search_payload_uses_web_search_tool(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        payload = client._build_site_search_payload(
            question="Что известно про IT лагерь?",
            site_domain="kmipt.ru",
            user_context={"summary_text": "Родитель интересуется летней программой."},
        )
        self.assertEqual(payload["tools"][0]["type"], "web_search_preview")
        self.assertEqual(payload["input"][0]["content"][0]["type"], "input_text")
        self.assertIn("kmipt.ru", payload["input"][0]["content"][0]["text"])

    def test_consultative_payload_uses_input_text_type(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        payload = client._build_consultative_payload(
            user_message="Ребенок в 11 классе, как поступить в МФТИ?",
            criteria=self.criteria,
            top_products=self.top_products,
            missing_fields=["format"],
            repeat_count=0,
            product_offer_allowed=True,
        )
        self.assertEqual(payload["input"][0]["content"][0]["type"], "input_text")
        self.assertEqual(payload["input"][1]["content"][0]["type"], "input_text")
        self.assertIn("квалифицированного сотрудника отдела продаж", payload["input"][0]["content"][0]["text"])

    def test_payloads_include_user_context_summary(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        context = {"summary_text": "Ученик 10 класса, цель ЕГЭ, интерес к МФТИ."}
        sales_payload = client._build_sales_payload(self.criteria, self.top_products, user_context=context)
        consult_payload = client._build_consultative_payload(
            user_message="Хочу поступить в МФТИ",
            criteria=self.criteria,
            top_products=self.top_products,
            missing_fields=["format"],
            repeat_count=0,
            product_offer_allowed=True,
            recent_history=[],
            user_context=context,
        )
        general_payload = client._build_general_help_payload(
            user_message="Как составить план?",
            dialogue_state="ask_goal",
            recent_history=[],
            user_context=context,
        )
        flow_payload = client._build_flow_followup_payload(
            user_message="Спасибо",
            base_message="Укажите класс ученика (1-11):",
            current_state="ask_grade",
            next_state="ask_grade",
            criteria={"brand": "kmipt"},
            recent_history=[],
            user_context=context,
        )
        knowledge_payload = client._build_knowledge_payload(
            question="Как оплатить?",
            vector_store_id="vs_test_123",
            user_context=context,
        )
        self.assertIn("Законспектированный контекст клиента", sales_payload["input"][1]["content"][0]["text"])
        self.assertIn("Законспектированный контекст клиента", consult_payload["input"][1]["content"][0]["text"])
        self.assertIn("Законспектированный контекст клиента", general_payload["input"][1]["content"][0]["text"])
        self.assertIn("Законспектированный контекст клиента", flow_payload["input"][1]["content"][0]["text"])
        self.assertIn("Законспектированный контекст клиента", knowledge_payload["input"][1]["content"][0]["text"])

    def test_consultative_payload_includes_recent_history(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        payload = client._build_consultative_payload(
            user_message="Хочу поступить в МФТИ",
            criteria=self.criteria,
            top_products=self.top_products,
            missing_fields=["format"],
            repeat_count=0,
            product_offer_allowed=False,
            recent_history=[{"role": "user", "text": "Ранее: 11 класс"}],
        )
        prompt_text = payload["input"][1]["content"][0]["text"]
        self.assertIn("Краткая история последних сообщений", prompt_text)
        self.assertIn("11 класс", prompt_text)

    def test_general_help_payload_includes_recent_history(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        payload = client._build_general_help_payload(
            user_message="Что такое косинус?",
            dialogue_state="ask_subject",
            recent_history=[{"role": "assistant", "text": "Обсуждали тригонометрию"}],
        )
        prompt_text = payload["input"][1]["content"][0]["text"]
        self.assertIn("Краткая история последних сообщений", prompt_text)
        self.assertIn("тригонометрию", prompt_text)

    def test_flow_followup_payload_includes_base_message(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        payload = client._build_flow_followup_payload(
            user_message="Спасибо",
            base_message="Укажите класс ученика (1-11):",
            current_state="ask_grade",
            next_state="ask_grade",
            criteria={"brand": "kmipt"},
            recent_history=[{"role": "user", "text": "Хочу поступить в МФТИ"}],
        )
        self.assertEqual(payload["input"][0]["content"][0]["type"], "input_text")
        prompt_text = payload["input"][1]["content"][0]["text"]
        self.assertIn("Базовое сообщение бота", prompt_text)
        self.assertIn("Укажите класс ученика", prompt_text)

    @patch("sales_agent.sales_core.llm_client.urlopen")
    def test_send_request_includes_http_error_details(self, mock_urlopen) -> None:
        mock_urlopen.side_effect = HTTPError(
            url="https://api.openai.com/v1/responses",
            code=400,
            msg="Bad Request",
            hdrs=None,
            fp=BytesIO(b'{"error":{"message":"bad payload"}}'),
        )
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        raw, error = client._send_request({"model": "gpt-4.1", "input": "ping"})
        self.assertIsNone(raw)
        self.assertIn("OpenAI HTTP error: 400", error or "")
        self.assertIn("bad payload", error or "")

    def test_extract_text_from_output_chunks(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        text = client._extract_text(
            {
                "output": [
                    {"content": [{"text": "Первая часть"}, {"text": "Вторая часть"}]},
                ]
            }
        )
        self.assertIn("Первая часть", text)
        self.assertIn("Вторая часть", text)

    def test_extract_json_object_from_code_fence(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        parsed = client._extract_json_object(
            "```json\n{\"answer_text\":\"ok\",\"call_to_action\":\"cta\"}\n```"
        )
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["answer_text"], "ok")

    def test_extract_json_object_from_embedded_fragment(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        parsed = client._extract_json_object(
            "Преамбула\n{\"answer_text\":\"ok\",\"call_to_action\":\"cta\"}\nПостамбула"
        )
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["call_to_action"], "cta")

    def test_parse_openai_sales_reply_returns_none_when_required_fields_missing(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        parsed = client._parse_openai_sales_reply(
            {"output_text": '{"answer_text":"ok","recommended_product_ids":["p01"]}'},
            allowed_ids=["p01"],
        )
        self.assertIsNone(parsed)

    def test_source_label_from_annotation_supports_multiple_formats(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        self.assertEqual(
            client._source_label_from_annotation({"file_citation": {"filename": "faq.md"}}),
            "faq.md",
        )
        self.assertEqual(
            client._source_label_from_annotation({"url_citation": {"title": "FAQ page"}}),
            "FAQ page",
        )
        self.assertEqual(
            client._source_label_from_annotation({"url_citation": {"url": "https://kmipt.ru/page"}}),
            "https://kmipt.ru/page",
        )

    def test_extract_source_names_collects_unique_annotations(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        sources = client._extract_source_names(
            {
                "output": [
                    {
                        "content": [
                            {
                                "annotations": [
                                    {"filename": "a.md"},
                                    {"file_citation": {"filename": "a.md"}},
                                    {"title": "Program page"},
                                    {"url": "https://kmipt.ru/camp"},
                                ]
                            }
                        ]
                    }
                ]
            }
        )
        self.assertEqual(sources, ["a.md", "Program page", "https://kmipt.ru/camp"])

    def test_send_request_handles_url_error(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        with patch("sales_agent.sales_core.llm_client.urlopen", side_effect=URLError("timed out")):
            raw, error = client._send_request({"model": "gpt-4.1", "input": "ping"})
        self.assertIsNone(raw)
        self.assertIn("connection error", error or "")

    @patch("sales_agent.sales_core.llm_client.urlopen")
    def test_send_request_handles_invalid_json_response(self, mock_urlopen) -> None:
        mock_urlopen.return_value = _MockHTTPResponse("{bad-json")
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        raw, error = client._send_request({"model": "gpt-4.1", "input": "ping"})
        self.assertIsNone(raw)
        self.assertIn("not valid json", (error or "").lower())

    def test_apply_site_fallback_returns_primary_when_domain_not_set(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        primary = KnowledgeReply(answer_text="OK", sources=[], used_fallback=False)
        resolved = client._apply_site_fallback(
            question="Что с оплатой?",
            primary_reply=primary,
            user_context={},
            site_domain="",
        )
        self.assertIs(resolved, primary)

    def test_apply_site_fallback_uses_site_result_when_primary_has_gap_marker(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        primary = KnowledgeReply(
            answer_text="Недостаточно информации в базе знаний, нужно уточнить.",
            sources=[],
            used_fallback=False,
        )
        site = KnowledgeReply(
            answer_text="Нашел данные на сайте.",
            sources=["https://kmipt.ru/page"],
            used_fallback=False,
        )
        with patch.object(client, "_answer_knowledge_via_site_search", return_value=site):
            resolved = client._apply_site_fallback(
                question="Что с оплатой?",
                primary_reply=primary,
                user_context={"summary_text": "..."},
                site_domain="kmipt.ru",
            )
        self.assertEqual(resolved.answer_text, "Нашел данные на сайте.")

    def test_should_use_site_fallback_true_on_explicit_fallback(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        self.assertTrue(
            client._should_use_site_fallback(
                KnowledgeReply(answer_text="Ок", sources=[], used_fallback=True)
            )
        )

    def test_should_use_site_fallback_false_for_confident_reply(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        self.assertFalse(
            client._should_use_site_fallback(
                KnowledgeReply(answer_text="Нашел точный ответ по документам.", sources=["docs.md"], used_fallback=False)
            )
        )

    def test_apply_site_fallback_keeps_primary_when_site_search_fails(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        primary = KnowledgeReply(answer_text="Недостаточно данных.", sources=[], used_fallback=False)
        failed_site = KnowledgeReply(answer_text="Ошибка поиска.", sources=[], used_fallback=True, error="timeout")
        with patch.object(client, "_answer_knowledge_via_site_search", return_value=failed_site):
            resolved = client._apply_site_fallback(
                question="Какие документы?",
                primary_reply=primary,
                user_context={},
                site_domain="kmipt.ru",
            )
        self.assertIs(resolved, primary)

    def test_answer_knowledge_via_site_search_handles_error(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        with patch.object(client, "_send_request", return_value=(None, "timeout")):
            result = client._answer_knowledge_via_site_search(
                question="Что известно про лагерь?",
                user_context={"summary_text": "интерес к лагерю"},
                site_domain="kmipt.ru",
            )
        self.assertTrue(result.used_fallback)
        self.assertIn("не удалось", result.answer_text.lower())
        self.assertIn("timeout", result.error or "")

    def test_answer_knowledge_via_site_search_handles_empty_text(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        with patch.object(client, "_send_request", return_value=({"output": []}, None)):
            result = client._answer_knowledge_via_site_search(
                question="Что известно про лагерь?",
                user_context={"summary_text": "интерес к лагерю"},
                site_domain="kmipt.ru",
            )
        self.assertTrue(result.used_fallback)
        self.assertIn("не удалось получить факты", result.answer_text.lower())

    def test_extract_text_prefers_output_text(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        text = client._extract_text(
            {"output_text": "  Готовый ответ  ", "output": [{"content": [{"text": "Лишний"}]}]}
        )
        self.assertEqual(text, "Готовый ответ")

    def test_fallback_consultative_reply_without_product_offer(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        reply = client._fallback_consultative_reply(
            criteria=SearchCriteria(brand="kmipt", grade=None, goal=None, subject=None, format=None),
            top_products=self.top_products,
            missing_fields=["grade"],
            product_offer_allowed=False,
        )
        self.assertTrue(reply.used_fallback)
        self.assertNotIn("Course 1", reply.answer_text)
        self.assertIn("класс", reply.next_question or "")

    def test_fallback_general_help_reply_and_flow_followup_variants(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")

        sinus = client._fallback_general_help_reply(user_message="Что такое синус?")
        self.assertIn("Синус", sinus.answer_text)

        mgu = client._fallback_general_help_reply(user_message="Как поступить в МГУ?")
        self.assertIn("МГУ", mgu.answer_text)

        generic = client._fallback_general_help_reply(user_message="Объясни тему", dialogue_state="ask_goal")
        self.assertIn("можем вернуться", generic.answer_text)

        followup = client._fallback_flow_followup_reply(
            base_message="Укажите класс ученика",
            next_state="ask_subject",
            criteria={"grade": 10},
        )
        self.assertIn("Для 10 класса", followup.answer_text)

        followup_goal = client._fallback_flow_followup_reply(
            base_message="Какая цель подготовки?",
            next_state="ask_goal",
            criteria={},
        )
        self.assertIn("Нужен еще один ориентир", followup_goal.answer_text)

        followup_format = client._fallback_flow_followup_reply(
            base_message="Какой формат удобнее?",
            next_state="ask_format",
            criteria={},
        )
        self.assertIn("последний организационный вопрос", followup_format.answer_text)

        followup_contact = client._fallback_flow_followup_reply(
            base_message="Оставьте телефон",
            next_state="ask_contact",
            criteria={},
        )
        self.assertIn("передадим запрос менеджеру", followup_contact.answer_text)

    def test_answer_knowledge_question_sync_edge_cases(self) -> None:
        configured = LLMClient(api_key="sk-test", model="gpt-4.1")
        empty = configured.answer_knowledge_question("   ", vector_store_id="vs_test")
        self.assertTrue(empty.used_fallback)
        self.assertIn("задайте вопрос", empty.answer_text.lower())

        unconfigured = LLMClient(api_key="", model="gpt-4.1")
        no_key = unconfigured.answer_knowledge_question("Как оплатить?", vector_store_id="vs_test")
        self.assertTrue(no_key.used_fallback)
        self.assertIn("OPENAI_API_KEY", no_key.error or "")

    def test_answer_knowledge_question_sync_handles_request_error_and_empty_text(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        with patch.object(client, "_send_request", return_value=(None, "upstream error")):
            errored = client.answer_knowledge_question("Как оплатить?", vector_store_id="vs_test")
        self.assertTrue(errored.used_fallback)
        self.assertIn("upstream error", errored.error or "")

        with patch.object(client, "_send_request", return_value=({"output": []}, None)):
            empty = client.answer_knowledge_question("Как оплатить?", vector_store_id="vs_test")
        self.assertTrue(empty.used_fallback)
        self.assertIn("переформулировать", empty.answer_text.lower())

    def test_send_request_http_error_without_details(self) -> None:
        class _NoBodyHTTPError(HTTPError):
            def read(self):
                raise RuntimeError("cannot read body")

        err = _NoBodyHTTPError(
            url="https://api.openai.com/v1/responses",
            code=503,
            msg="Service Unavailable",
            hdrs=None,
            fp=None,
        )
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        with patch("sales_agent.sales_core.llm_client.urlopen", side_effect=err):
            raw, error = client._send_request({"model": "gpt-4.1", "input": "ping"})
        self.assertIsNone(raw)
        self.assertEqual(error, "OpenAI HTTP error: 503")

    def test_extract_helpers_cover_non_happy_paths(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        self.assertEqual(client._extract_text({"output": "wrong-type"}), "")
        self.assertEqual(client._extract_text({"output": [{"content": "wrong"}]}), "")
        self.assertEqual(client._extract_source_names({"output": "wrong-type"}), [])
        self.assertEqual(client._source_label_from_annotation({"unknown": "value"}), "")
        self.assertIsNone(client._extract_json_object(""))
        self.assertIsNone(client._extract_json_object("no json here"))

    def test_product_payload_includes_sessions(self) -> None:
        catalog = parse_catalog(
            {
                "products": [
                    {
                        "id": "p-s",
                        "brand": "kmipt",
                        "title": "Session Product",
                        "url": "https://example.com/s",
                        "category": "camp",
                        "grade_min": 8,
                        "grade_max": 11,
                        "subjects": ["math"],
                        "format": "offline",
                        "sessions": [
                            {
                                "name": "Лето",
                                "start_date": "2026-06-10",
                                "end_date": "2026-06-20",
                                "price_rub": 59000,
                            }
                        ],
                        "usp": ["u1", "u2", "u3"],
                    }
                ]
            },
            Path("memory://catalog-with-sessions.yaml"),
        )
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        payload = client._product_payload(catalog.products[0])
        self.assertEqual(payload["sessions"][0]["name"], "Лето")
        self.assertEqual(payload["sessions"][0]["price_rub"], 59000)


@unittest.skipUnless(HAS_LLM_DEPS, "llm dependencies are not installed")
class LLMClientAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_build_sales_reply_async_parses_response(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        response = _MockAsyncResponse(
            200,
            {
                "output_text": (
                    '{"answer_text":"Асинхронный ответ","next_question":"Уточнить формат?",'
                    '"call_to_action":"Оставьте телефон","recommended_product_ids":["p01"]}'
                )
            },
        )
        with patch(
            "sales_agent.sales_core.llm_client.httpx.AsyncClient",
            return_value=_MockAsyncClient(response),
        ):
            result = await client.build_sales_reply_async(
                SearchCriteria(brand="kmipt", grade=10, goal="ege", subject="math", format="online"),
                _products(),
            )
        self.assertFalse(result.used_fallback)
        self.assertEqual(result.recommended_product_ids, ["p01"])

    async def test_answer_knowledge_question_async_with_sources(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        response = _MockAsyncResponse(
            200,
            {
                "output": [
                    {
                        "content": [
                            {
                                "text": "Оплата подтверждается по счету.",
                                "annotations": [{"filename": "payments.md"}],
                            }
                        ]
                    }
                ]
            },
        )
        with patch(
            "sales_agent.sales_core.llm_client.httpx.AsyncClient",
            return_value=_MockAsyncClient(response),
        ):
            result = await client.answer_knowledge_question_async(
                "Как подтвердить оплату?",
                vector_store_id="vs_test_123",
            )
        self.assertFalse(result.used_fallback)
        self.assertIn("счету", result.answer_text)
        self.assertEqual(result.sources, ["payments.md"])

    async def test_answer_knowledge_question_async_web_fallback_when_vector_store_missing(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        response = _MockAsyncResponse(
            200,
            {
                "output": [
                    {
                        "content": [
                            {
                                "text": "IT лагерь проходит летом на базе кампуса МФТИ.",
                                "annotations": [{"url": "https://kmipt.ru/camps/it"}],
                            }
                        ]
                    }
                ]
            },
        )
        with patch(
            "sales_agent.sales_core.llm_client.httpx.AsyncClient",
            return_value=_MockAsyncClient(response),
        ):
            result = await client.answer_knowledge_question_async(
                "Что известно про IT лагерь?",
                vector_store_id=None,
                allow_web_fallback=True,
                site_domain="kmipt.ru",
            )
        self.assertFalse(result.used_fallback)
        self.assertIn("лагерь", result.answer_text.lower())
        self.assertIn("https://kmipt.ru/camps/it", result.sources)

    async def test_answer_knowledge_question_async_web_fallback_when_file_search_uncertain(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        raw_file = {"output_text": "Недостаточно информации в базе знаний, лучше уточнить у менеджера."}
        raw_web = {
            "output": [
                {
                    "content": [
                        {
                            "text": "По данным kmipt.ru, IT лагерь включает проектные треки и командную работу.",
                            "annotations": [{"url": "https://kmipt.ru/camps/it-program"}],
                        }
                    ]
                }
            ]
        }
        with patch.object(
            client,
            "_send_request_async",
            new=AsyncMock(side_effect=[(raw_file, None), (raw_web, None)]),
        ):
            result = await client.answer_knowledge_question_async(
                "Расскажи про IT лагерь",
                vector_store_id="vs_test_123",
                allow_web_fallback=True,
                site_domain="kmipt.ru",
            )

        self.assertFalse(result.used_fallback)
        self.assertIn("kmipt.ru", result.answer_text.lower())
        self.assertEqual(result.sources, ["https://kmipt.ru/camps/it-program"])

    async def test_send_request_async_includes_http_error_details(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        response = _MockAsyncResponse(400, {})
        response.text = '{"error":{"message":"bad async payload"}}'
        with patch(
            "sales_agent.sales_core.llm_client.httpx.AsyncClient",
            return_value=_MockAsyncClient(response),
        ):
            raw, error = await client._send_request_async({"model": "gpt-4.1", "input": "ping"})

        self.assertIsNone(raw)
        self.assertIn("OpenAI HTTP error: 400", error or "")
        self.assertIn("bad async payload", error or "")

    async def test_build_consultative_reply_async_parses_response(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        response = _MockAsyncResponse(
            200,
            {
                "output_text": (
                    '{"answer_text":"План понятен: фиксируем предмет и темп.",'
                    '"next_question":"Как удобнее заниматься: онлайн или очно?",'
                    '"call_to_action":"После этого подберу 2 программы без навязчивых продаж.",'
                    '"recommended_product_ids":["p01"]}'
                )
            },
        )
        with patch(
            "sales_agent.sales_core.llm_client.httpx.AsyncClient",
            return_value=_MockAsyncClient(response),
        ):
            result = await client.build_consultative_reply_async(
                user_message="Хочу поступить в МФТИ, что делать?",
                criteria=SearchCriteria(brand="kmipt", grade=11, goal="ege", subject="math", format=None),
                top_products=_products(),
                missing_fields=["format"],
                repeat_count=0,
                product_offer_allowed=True,
            )

        self.assertFalse(result.used_fallback)
        self.assertIn("План понятен", result.answer_text)
        self.assertEqual(result.recommended_product_ids, ["p01"])

    async def test_build_general_help_reply_async_parses_text(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        response = _MockAsyncResponse(
            200,
            {"output_text": "Косинус — отношение прилежащего катета к гипотенузе."},
        )
        with patch(
            "sales_agent.sales_core.llm_client.httpx.AsyncClient",
            return_value=_MockAsyncClient(response),
        ):
            result = await client.build_general_help_reply_async(
                user_message="что такое косинус?",
                dialogue_state="ask_subject",
            )
        self.assertFalse(result.used_fallback)
        self.assertIn("Косинус", result.answer_text)

    async def test_build_general_help_reply_async_uses_fallback_without_key(self) -> None:
        client = LLMClient(api_key="", model="gpt-4.1")
        result = await client.build_general_help_reply_async(
            user_message="Что такое косинус?",
            dialogue_state="ask_subject",
        )
        self.assertTrue(result.used_fallback)
        self.assertIn("косинус", result.answer_text.lower())

    async def test_build_flow_followup_reply_async_parses_text(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        response = _MockAsyncResponse(
            200,
            {"output_text": "Понял вас. Подскажите, пожалуйста, какой сейчас класс ученика?"},
        )
        with patch(
            "sales_agent.sales_core.llm_client.httpx.AsyncClient",
            return_value=_MockAsyncClient(response),
        ):
            result = await client.build_flow_followup_reply_async(
                user_message="Спасибо",
                base_message="Укажите класс ученика (1-11):",
                current_state="ask_grade",
                next_state="ask_grade",
                criteria={"brand": "kmipt"},
                recent_history=[],
            )

        self.assertFalse(result.used_fallback)
        self.assertIn("класс", result.answer_text.lower())

    async def test_build_flow_followup_reply_async_uses_fallback_without_key(self) -> None:
        client = LLMClient(api_key="", model="gpt-4.1")
        result = await client.build_flow_followup_reply_async(
            user_message="Спасибо",
            base_message="Укажите класс ученика (1-11):",
            current_state="ask_grade",
            next_state="ask_grade",
            criteria={"brand": "kmipt"},
            recent_history=[],
        )
        self.assertTrue(result.used_fallback)
        self.assertIn("класс", result.answer_text.lower())

    async def test_send_request_async_handles_invalid_json_response(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        with patch(
            "sales_agent.sales_core.llm_client.httpx.AsyncClient",
            return_value=_MockAsyncClient(_MockAsyncInvalidJsonResponse()),
        ):
            raw, error = await client._send_request_async({"model": "gpt-4.1", "input": "ping"})
        self.assertIsNone(raw)
        self.assertIn("not valid json", (error or "").lower())

    async def test_send_request_async_handles_request_error_and_http_without_body(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")

        class _FailingAsyncClient:
            async def __aenter__(self):
                raise httpx.RequestError("network down")

            async def __aexit__(self, exc_type, exc, tb):
                return False

        with patch("sales_agent.sales_core.llm_client.httpx.AsyncClient", return_value=_FailingAsyncClient()):
            raw, error = await client._send_request_async({"model": "gpt-4.1", "input": "ping"})
        self.assertIsNone(raw)
        self.assertIn("connection error", (error or "").lower())

        response = _MockAsyncResponse(502, {})
        response.text = ""
        with patch(
            "sales_agent.sales_core.llm_client.httpx.AsyncClient",
            return_value=_MockAsyncClient(response),
        ):
            raw, error = await client._send_request_async({"model": "gpt-4.1", "input": "ping"})
        self.assertIsNone(raw)
        self.assertEqual(error, "OpenAI HTTP error: 502")

    async def test_build_consultative_reply_async_uses_fallback_without_key(self) -> None:
        client = LLMClient(api_key="", model="gpt-4.1")
        result = await client.build_consultative_reply_async(
            user_message="Хочу план поступления в МФТИ",
            criteria=SearchCriteria(brand="kmipt", grade=11, goal="ege", subject="math", format=None),
            top_products=_products(),
            missing_fields=["format"],
            repeat_count=0,
            product_offer_allowed=True,
        )
        self.assertTrue(result.used_fallback)
        self.assertIn("OPENAI_API_KEY", result.error or "")

    async def test_build_general_help_reply_async_handles_empty_message(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        result = await client.build_general_help_reply_async(user_message="   ")
        self.assertTrue(result.used_fallback)
        self.assertIn("сформулируйте", result.answer_text.lower())

    async def test_build_flow_followup_reply_async_handles_empty_base_message(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        result = await client.build_flow_followup_reply_async(
            user_message="Спасибо",
            base_message="   ",
            current_state="ask_goal",
            next_state="ask_goal",
            criteria={"brand": "kmipt"},
        )
        self.assertTrue(result.used_fallback)
        self.assertIn("как лучше вам помочь", result.answer_text.lower())

    async def test_build_consultative_reply_async_handles_error_and_parse_fallback(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        with patch.object(client, "_send_request_async", new=AsyncMock(return_value=(None, "downstream"))):
            errored = await client.build_consultative_reply_async(
                user_message="Хочу стратегию поступления",
                criteria=SearchCriteria(brand="kmipt", grade=11, goal="ege", subject="math", format=None),
                top_products=_products(),
                missing_fields=["format"],
                product_offer_allowed=True,
            )
        self.assertTrue(errored.used_fallback)
        self.assertEqual(errored.error, "downstream")

        with patch.object(client, "_send_request_async", new=AsyncMock(return_value=({"output_text": "not-json"}, None))):
            parsed_fail = await client.build_consultative_reply_async(
                user_message="Хочу стратегию поступления",
                criteria=SearchCriteria(brand="kmipt", grade=11, goal="ege", subject="math", format=None),
                top_products=_products(),
                missing_fields=["format"],
                product_offer_allowed=True,
            )
        self.assertTrue(parsed_fail.used_fallback)
        self.assertIn("parse structured", parsed_fail.error or "")

    async def test_build_general_and_flow_async_handle_error_and_empty_text(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        with patch.object(client, "_send_request_async", new=AsyncMock(return_value=(None, "upstream"))):
            general_error = await client.build_general_help_reply_async(user_message="что такое косинус?")
        self.assertTrue(general_error.used_fallback)
        self.assertEqual(general_error.error, "upstream")

        with patch.object(client, "_send_request_async", new=AsyncMock(return_value=({"output": []}, None))):
            general_empty = await client.build_general_help_reply_async(user_message="что такое косинус?")
        self.assertTrue(general_empty.used_fallback)
        self.assertEqual(general_empty.error, "empty response text")

        with patch.object(client, "_send_request_async", new=AsyncMock(return_value=(None, "downstream"))):
            flow_error = await client.build_flow_followup_reply_async(
                user_message="спасибо",
                base_message="Укажите класс",
                current_state="ask_grade",
                next_state="ask_grade",
                criteria={"brand": "kmipt"},
            )
        self.assertTrue(flow_error.used_fallback)
        self.assertEqual(flow_error.error, "downstream")

        with patch.object(client, "_send_request_async", new=AsyncMock(return_value=({"output": []}, None))):
            flow_empty = await client.build_flow_followup_reply_async(
                user_message="спасибо",
                base_message="Укажите класс",
                current_state="ask_grade",
                next_state="ask_grade",
                criteria={"brand": "kmipt"},
            )
        self.assertTrue(flow_empty.used_fallback)
        self.assertEqual(flow_empty.error, "empty response text")

    async def test_answer_knowledge_question_async_edge_cases(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        empty = await client.answer_knowledge_question_async("   ", vector_store_id="vs")
        self.assertTrue(empty.used_fallback)

        no_key_client = LLMClient(api_key="", model="gpt-4.1")
        no_key = await no_key_client.answer_knowledge_question_async("Как оплатить?", vector_store_id="vs")
        self.assertTrue(no_key.used_fallback)
        self.assertIn("OPENAI_API_KEY", no_key.error or "")

        with patch.object(client, "_send_request_async", new=AsyncMock(return_value=(None, "err"))):
            errored = await client.answer_knowledge_question_async("Как оплатить?", vector_store_id="vs")
        self.assertTrue(errored.used_fallback)
        self.assertEqual(errored.error, "err")

        with patch.object(client, "_send_request_async", new=AsyncMock(return_value=({"output": []}, None))):
            empty_text = await client.answer_knowledge_question_async("Как оплатить?", vector_store_id="vs")
        self.assertTrue(empty_text.used_fallback)
        self.assertEqual(empty_text.error, "empty response text")


if __name__ == "__main__":
    unittest.main()
