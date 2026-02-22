import json
import os
import subprocess
import sys
import tempfile
import unittest
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts import preflight_audit


def _write_catalog(path: Path) -> None:
    path.write_text(
        """
products:
  - id: kmipt-ege-physics
    brand: kmipt
    title: Подготовка к ЕГЭ по физике
    url: https://example.com/ege-physics
    category: ege
    grade_min: 10
    grade_max: 11
    subjects: [physics]
    format: online
    usp:
      - Мини-группа
      - Домашние задания с разбором
      - Трекинг прогресса
""".strip(),
        encoding="utf-8",
    )


class PreflightAuditScriptTests(unittest.TestCase):
    def test_script_returns_fail_when_critical_env_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            catalog_path = root / "catalog.yaml"
            _write_catalog(catalog_path)
            db_path = root / "data" / "sales_agent.db"
            db_path.parent.mkdir(parents=True, exist_ok=True)

            env = {
                "PATH": os.environ.get("PATH", ""),
                "PYTHONPATH": str(Path.cwd()),
                "TELEGRAM_BOT_TOKEN": "",
                "OPENAI_API_KEY": "",
                "DATABASE_PATH": str(db_path),
                "CATALOG_PATH": str(catalog_path),
                "KNOWLEDGE_PATH": str(root / "knowledge"),
            }
            result = subprocess.run(
                [sys.executable, "scripts/preflight_audit.py", "--json"],
                capture_output=True,
                text=True,
                check=False,
                env=env,
            )

        self.assertEqual(result.returncode, 1)
        payload = json.loads(result.stdout)
        self.assertEqual(payload.get("status"), "fail")

    def test_script_returns_ok_with_ready_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            catalog_path = root / "catalog.yaml"
            _write_catalog(catalog_path)
            db_path = root / "data" / "sales_agent.db"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            knowledge_path = root / "knowledge"
            knowledge_path.mkdir(parents=True, exist_ok=True)
            (knowledge_path / "faq_general.md").write_text("FAQ", encoding="utf-8")

            env = {
                "PATH": os.environ.get("PATH", ""),
                "PYTHONPATH": str(Path.cwd()),
                "TELEGRAM_BOT_TOKEN": "tg-token",
                "OPENAI_API_KEY": "sk-test",
                "OPENAI_VECTOR_STORE_ID": "vs_123",
                "DATABASE_PATH": str(db_path),
                "CATALOG_PATH": str(catalog_path),
                "KNOWLEDGE_PATH": str(knowledge_path),
                "TELEGRAM_MODE": "webhook",
                "TELEGRAM_WEBHOOK_SECRET": "secret",
            }
            result = subprocess.run(
                [sys.executable, "scripts/preflight_audit.py", "--json"],
                capture_output=True,
                text=True,
                check=False,
                env=env,
            )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        payload = json.loads(result.stdout)
        self.assertIn(payload.get("status"), {"ok", "warn"})
        runtime = payload.get("runtime", {})
        self.assertTrue(runtime.get("catalog_ok"))
        self.assertTrue(runtime.get("vector_store_id_set"))
        if payload.get("status") == "warn":
            codes = {item.get("code") for item in payload.get("issues", []) if isinstance(item, dict)}
            self.assertIn("ptb_business_features_unavailable", codes)

    def test_main_returns_zero_for_warn_and_prints_summary(self) -> None:
        diagnostics = {
            "status": "warn",
            "runtime": {
                "telegram_mode": "polling",
                "openai_model": "gpt-5.1",
                "catalog_ok": True,
                "catalog_products_count": 10,
                "knowledge_files_count": 5,
                "vector_store_id_set": True,
            },
            "issues": [{"severity": "warning", "code": "demo", "message": "demo warning"}],
        }

        with patch.object(preflight_audit, "get_settings", return_value=SimpleNamespace()), patch.object(
            preflight_audit, "build_runtime_diagnostics", return_value=diagnostics
        ), patch("sys.stdout", new_callable=StringIO) as stdout:
            result = preflight_audit.main([])

        self.assertEqual(result, 0)
        output = stdout.getvalue()
        self.assertIn("Preflight status: WARN", output)
        self.assertIn("[warning] demo", output)

    def test_main_json_mode_returns_fail_code(self) -> None:
        diagnostics = {"status": "fail", "runtime": {}, "issues": []}
        with patch.object(preflight_audit, "get_settings", return_value=SimpleNamespace()), patch.object(
            preflight_audit, "build_runtime_diagnostics", return_value=diagnostics
        ), patch("sys.stdout", new_callable=StringIO) as stdout:
            result = preflight_audit.main(["--json"])

        self.assertEqual(result, 1)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["status"], "fail")

    def test_main_returns_fail_when_get_settings_raises_value_error(self) -> None:
        with patch.object(preflight_audit, "get_settings", side_effect=ValueError("bad webhook config")), patch(
            "sys.stdout", new_callable=StringIO
        ) as stdout:
            result = preflight_audit.main(["--json"])

        self.assertEqual(result, 1)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["status"], "fail")
        self.assertEqual(payload["issues"][0]["code"], "invalid_configuration")
        self.assertIn("webhook", payload["issues"][0]["message"])


if __name__ == "__main__":
    unittest.main()
