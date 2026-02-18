import tempfile
import unittest
from pathlib import Path

try:
    import yaml  # noqa: F401
    from sales_agent.sales_core.catalog import (
        CatalogValidationError,
        default_catalog_path,
        load_catalog,
        parse_catalog,
    )

    HAS_CATALOG_DEPS = True
except ModuleNotFoundError:
    HAS_CATALOG_DEPS = False


@unittest.skipUnless(HAS_CATALOG_DEPS, "catalog dependencies are not installed")
class CatalogTests(unittest.TestCase):
    def test_load_default_catalog_success(self) -> None:
        catalog = load_catalog()
        self.assertGreaterEqual(len(catalog.products), 10)

    def test_default_catalog_file_exists(self) -> None:
        self.assertTrue(default_catalog_path().exists())

    def test_parse_catalog_rejects_duplicate_product_ids(self) -> None:
        raw_catalog = {
            "products": [
                {
                    "id": "dup-product",
                    "brand": "kmipt",
                    "title": "Product One",
                    "url": "https://example.com/p1",
                    "category": "ege",
                    "grade_min": 10,
                    "grade_max": 11,
                    "subjects": ["math"],
                    "format": "online",
                    "usp": ["u1", "u2", "u3"],
                },
                {
                    "id": "dup-product",
                    "brand": "foton",
                    "title": "Product Two",
                    "url": "https://example.com/p2",
                    "category": "ege",
                    "grade_min": 10,
                    "grade_max": 11,
                    "subjects": ["physics"],
                    "format": "online",
                    "usp": ["u1", "u2", "u3"],
                },
            ]
        }

        with self.assertRaises(CatalogValidationError) as exc:
            parse_catalog(raw_catalog, Path("memory://catalog.yaml"))

        self.assertIn("duplicate product ids", str(exc.exception))

    def test_parse_catalog_allows_camp_without_sessions(self) -> None:
        raw_catalog = {
            "products": [
                {
                    "id": "camp-no-session",
                    "brand": "kmipt",
                    "title": "Camp without session",
                    "url": "https://example.com/camp",
                    "category": "camp",
                    "grade_min": 7,
                    "grade_max": 9,
                    "subjects": ["math"],
                    "format": "offline",
                    "sessions": [],
                    "usp": ["u1", "u2", "u3"],
                }
            ]
        }

        parsed = parse_catalog(raw_catalog, Path("memory://catalog.yaml"))
        self.assertEqual(parsed.products[0].sessions, [])

    def test_parse_catalog_rejects_short_usp(self) -> None:
        raw_catalog = {
            "products": [
                {
                    "id": "short-usp",
                    "brand": "kmipt",
                    "title": "Short usp product",
                    "url": "https://example.com/short-usp",
                    "category": "base",
                    "grade_min": 5,
                    "grade_max": 6,
                    "subjects": ["math"],
                    "format": "online",
                    "usp": ["only one"],
                }
            ]
        }

        with self.assertRaises(CatalogValidationError) as exc:
            parse_catalog(raw_catalog, Path("memory://catalog.yaml"))

        self.assertIn("usp", str(exc.exception))

    def test_parse_catalog_rejects_invalid_grade_range(self) -> None:
        raw_catalog = {
            "products": [
                {
                    "id": "bad-grade-range",
                    "brand": "foton",
                    "title": "Bad grade product",
                    "url": "https://example.com/bad-grade",
                    "category": "base",
                    "grade_min": 9,
                    "grade_max": 6,
                    "subjects": ["math"],
                    "format": "online",
                    "usp": ["u1", "u2", "u3"],
                }
            ]
        }

        with self.assertRaises(CatalogValidationError) as exc:
            parse_catalog(raw_catalog, Path("memory://catalog.yaml"))

        self.assertIn("grade_min must be <=", str(exc.exception))

    def test_parse_catalog_normalizes_subjects(self) -> None:
        raw_catalog = {
            "products": [
                {
                    "id": "normalize-subjects",
                    "brand": "foton",
                    "title": "Normalize subjects product",
                    "url": "https://example.com/normalize",
                    "category": "base",
                    "grade_min": 5,
                    "grade_max": 6,
                    "subjects": [" Math ", "math", "PHYSICS"],
                    "format": "online",
                    "usp": ["u1", "u2", "u3"],
                }
            ]
        }

        catalog = parse_catalog(raw_catalog, Path("memory://catalog.yaml"))
        self.assertEqual(catalog.products[0].subjects, ["math", "physics"])

    def test_load_catalog_rejects_non_mapping_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "invalid.yaml"
            path.write_text("- not-a-mapping\n", encoding="utf-8")
            with self.assertRaises(CatalogValidationError):
                load_catalog(path)


if __name__ == "__main__":
    unittest.main()
