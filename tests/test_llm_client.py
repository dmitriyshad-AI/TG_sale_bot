import unittest
from io import BytesIO
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

try:
    from sales_agent.sales_core.catalog import SearchCriteria, parse_catalog
    from sales_agent.sales_core.llm_client import LLMClient

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

    def test_knowledge_payload_uses_input_text_type(self) -> None:
        client = LLMClient(api_key="sk-test", model="gpt-4.1")
        payload = client._build_knowledge_payload(
            question="Как оплатить?",
            vector_store_id="vs_test_123",
        )
        self.assertEqual(payload["input"][0]["content"][0]["type"], "input_text")
        self.assertEqual(payload["input"][1]["content"][0]["type"], "input_text")

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


if __name__ == "__main__":
    unittest.main()
