import json
import tempfile
import unittest
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from sales_agent.sales_core import db
from scripts import db_migrations_status


class DbMigrationsStatusScriptTests(unittest.TestCase):
    def test_main_json_reports_all_migrations_applied(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "status.db"
            db.init_db(db_path)
            settings = SimpleNamespace(database_path=db_path)
            with patch.object(db_migrations_status, "get_settings", return_value=settings), patch(
                "sys.stdout", new_callable=StringIO
            ) as stdout:
                code = db_migrations_status.main(["--json"])

        self.assertEqual(code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["pending_versions"], [])
        self.assertEqual(
            payload["known_versions"],
            [version for version, _ in db.SCHEMA_MIGRATION_STEPS],
        )

    def test_main_text_mode_with_explicit_db_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "status_explicit.db"
            with patch("sys.stdout", new_callable=StringIO) as stdout:
                code = db_migrations_status.main(["--db-path", str(db_path)])

        self.assertEqual(code, 0)
        output = stdout.getvalue()
        self.assertIn("Known migrations", output)
        self.assertIn("All migrations are applied.", output)


if __name__ == "__main__":
    unittest.main()

