import subprocess
import sys
import unittest

try:
    import yaml  # noqa: F401
    import pydantic  # noqa: F401

    HAS_CATALOG_DEPS = True
except ModuleNotFoundError:
    HAS_CATALOG_DEPS = False


@unittest.skipUnless(HAS_CATALOG_DEPS, "catalog dependencies are not installed")
class ValidateCatalogScriptTests(unittest.TestCase):
    def test_script_success_for_default_catalog(self) -> None:
        result = subprocess.run(
            [sys.executable, "scripts/validate_catalog.py"],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("[OK] Catalog is valid", result.stdout)

    def test_script_fails_for_missing_file(self) -> None:
        result = subprocess.run(
            [sys.executable, "scripts/validate_catalog.py", "--path", "missing_file.yaml"],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("[ERROR]", result.stderr)


if __name__ == "__main__":
    unittest.main()
