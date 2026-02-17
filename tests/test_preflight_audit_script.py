import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


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
        self.assertEqual(payload.get("status"), "ok")
        runtime = payload.get("runtime", {})
        self.assertTrue(runtime.get("catalog_ok"))
        self.assertTrue(runtime.get("vector_store_id_set"))


if __name__ == "__main__":
    unittest.main()
