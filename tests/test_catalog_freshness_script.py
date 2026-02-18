import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

try:
    import yaml  # noqa: F401
    import pydantic  # noqa: F401

    HAS_CATALOG_DEPS = True
except ModuleNotFoundError:
    HAS_CATALOG_DEPS = False


def _product(
    *,
    product_id: str,
    category: str,
    sessions: list[dict],
    format_value: str = "hybrid",
    subject: str = "math",
) -> dict:
    return {
        "id": product_id,
        "brand": "kmipt",
        "title": f"Product {product_id}",
        "url": f"https://example.com/{product_id}",
        "category": category,
        "grade_min": 8,
        "grade_max": 11,
        "subjects": [subject],
        "format": format_value,
        "sessions": sessions,
        "usp": ["u1 enough text", "u2 enough text", "u3 enough text"],
    }


@unittest.skipUnless(HAS_CATALOG_DEPS, "catalog dependencies are not installed")
class CatalogFreshnessScriptTests(unittest.TestCase):
    def _run(self, catalog_path: Path, *extra_args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "scripts/check_catalog_freshness.py", "--path", str(catalog_path), *extra_args],
            capture_output=True,
            text=True,
            check=False,
        )

    def test_script_passes_for_fresh_catalog(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "catalog.yaml"
            path.write_text(
                yaml.safe_dump(
                    {
                        "products": [
                            _product(
                                product_id="kmipt-camp-summer",
                                category="camp",
                                sessions=[
                                    {
                                        "name": "summer",
                                        "start_date": "2026-06-10",
                                        "end_date": "2026-06-20",
                                        "price_rub": 55000,
                                    }
                                ],
                                format_value="offline",
                            )
                        ]
                    },
                    sort_keys=False,
                    allow_unicode=True,
                ),
                encoding="utf-8",
            )

            result = self._run(path, "--today", "2026-02-13")
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertIn("[OK] Catalog freshness check passed", result.stdout)

    def test_script_skips_camp_rule_when_camp_has_no_sessions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "catalog.yaml"
            path.write_text(
                yaml.safe_dump(
                    {
                        "products": [
                            _product(
                                product_id="kmipt-camp-without-sessions",
                                category="camp",
                                sessions=[],
                                format_value="offline",
                            )
                        ]
                    },
                    sort_keys=False,
                    allow_unicode=True,
                ),
                encoding="utf-8",
            )

            result = self._run(path, "--today", "2026-02-13")
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertIn("[OK] Catalog freshness check passed", result.stdout)

    def test_script_fails_for_past_camp_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "catalog.yaml"
            path.write_text(
                yaml.safe_dump(
                    {
                        "products": [
                            _product(
                                product_id="kmipt-camp-old",
                                category="camp",
                                sessions=[
                                    {
                                        "name": "winter",
                                        "start_date": "2025-01-01",
                                        "end_date": "2025-01-07",
                                        "price_rub": 50000,
                                    }
                                ],
                                format_value="offline",
                            )
                        ]
                    },
                    sort_keys=False,
                    allow_unicode=True,
                ),
                encoding="utf-8",
            )

            result = self._run(path, "--today", "2026-02-13")
            self.assertEqual(result.returncode, 1)
            self.assertIn("catalog: no camp has upcoming sessions", result.stdout)

    def test_script_fails_for_stale_and_price_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "catalog.yaml"
            path.write_text(
                yaml.safe_dump(
                    {
                        "products": [
                            _product(
                                product_id="kmipt-ege-stale",
                                category="ege",
                                sessions=[
                                    {
                                        "name": "2024 year",
                                        "start_date": "2024-01-10",
                                        "end_date": "2024-05-31",
                                        "price_rub": None,
                                    }
                                ],
                            )
                        ]
                    },
                    sort_keys=False,
                    allow_unicode=True,
                ),
                encoding="utf-8",
            )

            result = self._run(path, "--today", "2026-02-13", "--stale-days", "30")
            self.assertEqual(result.returncode, 1)
            self.assertIn("older than 30 days", result.stdout)
            self.assertIn("all sessions have empty price_rub", result.stdout)


if __name__ == "__main__":
    unittest.main()
