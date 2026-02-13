import argparse
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

from sales_agent.sales_core.catalog_draft import ProductCandidate
from scripts import build_catalog_draft


def _product_stub(candidate: ProductCandidate, idx: int = 1) -> dict:
    return {
        "id": f"{candidate.brand}-{idx}",
        "brand": candidate.brand,
        "title": candidate.title,
        "url": candidate.url,
        "category": "base",
        "grade_min": 8,
        "grade_max": 11,
        "subjects": ["math"],
        "format": candidate.format_hint or "online",
        "sessions": [],
        "usp": ["stub 1", "stub 2", "stub 3"],
    }


class BuildCatalogDraftScriptTests(unittest.TestCase):
    def test_main_returns_error_when_no_products_collected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "products.auto_draft.yaml"
            args = argparse.Namespace(output=output, limit_per_brand=5, timeout=1.0)

            with patch.object(build_catalog_draft, "parse_args", return_value=args), patch.object(
                build_catalog_draft, "collect_candidates_for_brand", return_value=[]
            ), patch.object(build_catalog_draft, "KMIPT_FALLBACK_CANDIDATES", []):
                result = build_catalog_draft.main()

            self.assertEqual(result, 1)
            self.assertFalse(output.exists())

    def test_main_uses_kmipt_fallback_and_writes_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "products.auto_draft.yaml"
            args = argparse.Namespace(output=output, limit_per_brand=2, timeout=1.0)
            kmipt_fallback = [
                ProductCandidate(
                    brand="kmipt",
                    title="Математика ЕГЭ",
                    url="https://kmipt.ru/courses/ege/math",
                    format_hint="offline",
                )
            ]
            foton_candidate = ProductCandidate(
                brand="foton",
                title="ФОТОН онлайн",
                url="https://cdpofoton.ru/courses/online",
                format_hint="online",
            )

            def collect_side_effect(*, brand, listing_urls, timeout):
                if brand == "kmipt":
                    return []
                return [foton_candidate]

            def build_side_effect(candidate, detail_html, used_ids):
                return _product_stub(candidate, idx=len(used_ids) + 1)

            with patch.object(build_catalog_draft, "parse_args", return_value=args), patch.object(
                build_catalog_draft, "collect_candidates_for_brand", side_effect=collect_side_effect
            ), patch.object(build_catalog_draft, "KMIPT_FALLBACK_CANDIDATES", kmipt_fallback), patch.object(
                build_catalog_draft, "fetch_html", return_value="<html>ok</html>"
            ), patch.object(
                build_catalog_draft, "build_product_from_candidate", side_effect=build_side_effect
            ):
                result = build_catalog_draft.main()

            self.assertEqual(result, 0)
            self.assertTrue(output.exists())
            payload = yaml.safe_load(output.read_text(encoding="utf-8"))
            products = payload.get("products", [])
            self.assertEqual(len(products), 2)
            brands = {item["brand"] for item in products}
            self.assertEqual(brands, {"kmipt", "foton"})

    def test_main_deduplicates_products_by_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "products.auto_draft.yaml"
            args = argparse.Namespace(output=output, limit_per_brand=3, timeout=1.0)
            candidate_a = ProductCandidate(
                brand="kmipt",
                title="Математика ЕГЭ",
                url="https://kmipt.ru/courses/ege/math",
                format_hint="offline",
            )
            candidate_b = ProductCandidate(
                brand="kmipt",
                title="Математика ЕГЭ",
                url="https://kmipt.ru/courses/ege/math-dup",
                format_hint="offline",
            )

            def collect_side_effect(*, brand, listing_urls, timeout):
                return [candidate_a, candidate_b] if brand == "kmipt" else []

            def build_side_effect(candidate, detail_html, used_ids):
                product = _product_stub(candidate, idx=1)
                product["id"] = f"unique-{candidate.url.rsplit('/', 1)[-1]}"
                return product

            with patch.object(build_catalog_draft, "parse_args", return_value=args), patch.object(
                build_catalog_draft, "collect_candidates_for_brand", side_effect=collect_side_effect
            ), patch.object(build_catalog_draft, "KMIPT_FALLBACK_CANDIDATES", []), patch.object(
                build_catalog_draft, "fetch_html", return_value="<html>ok</html>"
            ), patch.object(
                build_catalog_draft, "build_product_from_candidate", side_effect=build_side_effect
            ):
                result = build_catalog_draft.main()

            self.assertEqual(result, 0)
            payload = yaml.safe_load(output.read_text(encoding="utf-8"))
            products = payload.get("products", [])
            self.assertEqual(len(products), 1)


if __name__ == "__main__":
    unittest.main()
