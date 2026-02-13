import unittest
from pathlib import Path
from unittest.mock import patch

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


if __name__ == "__main__":
    unittest.main()
