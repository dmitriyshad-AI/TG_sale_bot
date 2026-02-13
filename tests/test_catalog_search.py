import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

try:
    from sales_agent.sales_core.catalog import (
        SearchCriteria,
        explain_match,
        filter_products,
        parse_catalog,
        rank_products,
        select_top_products,
    )

    HAS_CATALOG_DEPS = True
except ModuleNotFoundError:
    HAS_CATALOG_DEPS = False


def _build_products():
    catalog = parse_catalog(
        {
            "products": [
                {
                    "id": "kmipt-ege-math",
                    "brand": "kmipt",
                    "title": "KMIPT EGE Math",
                    "url": "https://example.com/kmipt-ege-math",
                    "category": "ege",
                    "grade_min": 10,
                    "grade_max": 11,
                    "subjects": ["math"],
                    "format": "online",
                    "usp": ["u1", "u2", "u3"],
                },
                {
                    "id": "kmipt-ege-physics",
                    "brand": "kmipt",
                    "title": "KMIPT EGE Physics",
                    "url": "https://example.com/kmipt-ege-physics",
                    "category": "ege",
                    "grade_min": 10,
                    "grade_max": 11,
                    "subjects": ["physics"],
                    "format": "hybrid",
                    "usp": ["u1", "u2", "u3"],
                },
                {
                    "id": "foton-ege-math",
                    "brand": "foton",
                    "title": "FOTON EGE Math",
                    "url": "https://example.com/foton-ege-math",
                    "category": "ege",
                    "grade_min": 10,
                    "grade_max": 11,
                    "subjects": ["math"],
                    "format": "online",
                    "usp": ["u1", "u2", "u3"],
                },
                {
                    "id": "foton-base-math",
                    "brand": "foton",
                    "title": "FOTON Base Math",
                    "url": "https://example.com/foton-base-math",
                    "category": "base",
                    "grade_min": 8,
                    "grade_max": 10,
                    "subjects": ["math"],
                    "format": "offline",
                    "usp": ["u1", "u2", "u3"],
                },
            ]
        },
        Path("memory://catalog.yaml"),
    )
    return catalog.products


@unittest.skipUnless(HAS_CATALOG_DEPS, "catalog dependencies are not installed")
class CatalogSearchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.products = _build_products()

    def test_filter_uses_brand_default_when_brand_is_missing(self) -> None:
        with patch(
            "sales_agent.sales_core.catalog.get_settings",
            return_value=SimpleNamespace(brand_default="foton"),
        ):
            filtered = filter_products(
                products=self.products,
                brand=None,
                grade=10,
                goal="ege",
                subject="math",
                format="online",
            )

        self.assertEqual([item.id for item in filtered], ["foton-ege-math"])

    def test_filter_keeps_hybrid_when_online_requested(self) -> None:
        filtered = filter_products(
            products=self.products,
            brand="kmipt",
            grade=10,
            goal="ege",
            subject=None,
            format="online",
        )

        self.assertEqual([item.id for item in filtered], ["kmipt-ege-math", "kmipt-ege-physics"])

    def test_rank_prioritizes_exact_subject(self) -> None:
        filtered = filter_products(
            products=self.products,
            brand="kmipt",
            grade=10,
            goal="ege",
            subject=None,
            format="online",
        )
        ranked = rank_products(
            filtered,
            SearchCriteria(
                brand="kmipt",
                grade=10,
                goal="ege",
                subject="math",
                format="online",
            ),
        )

        self.assertEqual(ranked[0].id, "kmipt-ege-math")

    def test_explain_match_contains_human_readable_reasons(self) -> None:
        product = self.products[0]
        message = explain_match(
            product,
            SearchCriteria(
                brand="kmipt",
                grade=10,
                goal="ege",
                subject="math",
                format="online",
            ),
        )
        self.assertIn("подходит для 10 класса", message)
        self.assertIn("цель совпадает", message)
        self.assertIn("предмету", message)

    def test_select_top_products_limits_result_count(self) -> None:
        with patch("sales_agent.sales_core.catalog.load_products", return_value=self.products):
            result = select_top_products(
                SearchCriteria(
                    brand="kmipt",
                    grade=10,
                    goal="ege",
                    subject="math",
                    format="online",
                ),
                top_k=1,
            )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, "kmipt-ege-math")


if __name__ == "__main__":
    unittest.main()
