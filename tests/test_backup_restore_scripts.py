import sqlite3
import tempfile
import unittest
from pathlib import Path

from scripts import backup_sqlite, restore_sqlite


def _create_sample_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT)")
        conn.execute("INSERT INTO events (text) VALUES ('hello')")
        conn.commit()
    finally:
        conn.close()


class BackupRestoreScriptsTests(unittest.TestCase):
    def test_backup_and_restore_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            db_path = root / "source.db"
            backups_dir = root / "backups"
            restored_path = root / "restored.db"
            _create_sample_db(db_path)

            backup_code = backup_sqlite.main(
                [
                    "--db-path",
                    str(db_path),
                    "--output-dir",
                    str(backups_dir),
                    "--prefix",
                    "test-db",
                    "--keep-last",
                    "2",
                ]
            )
            self.assertEqual(backup_code, 0)

            backups = sorted(backups_dir.glob("test-db-*.db.gz"))
            self.assertEqual(len(backups), 1)

            restore_code = restore_sqlite.main(
                [
                    "--backup-path",
                    str(backups[0]),
                    "--db-path",
                    str(restored_path),
                ]
            )
            self.assertEqual(restore_code, 0)

            conn = sqlite3.connect(restored_path)
            try:
                row = conn.execute("SELECT COUNT(*) FROM events").fetchone()
            finally:
                conn.close()
            self.assertEqual(int(row[0]), 1)

    def test_backup_fails_when_source_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            code = backup_sqlite.main(
                [
                    "--db-path",
                    str(root / "missing.db"),
                    "--output-dir",
                    str(root / "backups"),
                ]
            )
        self.assertEqual(code, 1)

    def test_restore_requires_force_for_existing_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source_db = root / "source.db"
            target_db = root / "target.db"
            backups_dir = root / "backups"
            _create_sample_db(source_db)
            _create_sample_db(target_db)

            backup_code = backup_sqlite.main(
                [
                    "--db-path",
                    str(source_db),
                    "--output-dir",
                    str(backups_dir),
                    "--prefix",
                    "force-test",
                ]
            )
            self.assertEqual(backup_code, 0)
            backup_path = sorted(backups_dir.glob("force-test-*.db.gz"))[0]

            refused = restore_sqlite.main(
                [
                    "--backup-path",
                    str(backup_path),
                    "--db-path",
                    str(target_db),
                ]
            )
            self.assertEqual(refused, 1)

            forced = restore_sqlite.main(
                [
                    "--backup-path",
                    str(backup_path),
                    "--db-path",
                    str(target_db),
                    "--force",
                ]
            )
            self.assertEqual(forced, 0)
            bak_files = list(root.glob("target.db.bak-*"))
            self.assertTrue(bak_files)


if __name__ == "__main__":
    unittest.main()
