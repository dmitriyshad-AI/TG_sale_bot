import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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

    def test_backup_no_compress_creates_plain_db_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            db_path = root / "source.db"
            backups_dir = root / "backups"
            _create_sample_db(db_path)

            backup_code = backup_sqlite.main(
                [
                    "--db-path",
                    str(db_path),
                    "--output-dir",
                    str(backups_dir),
                    "--prefix",
                    "plain",
                    "--no-compress",
                ]
            )
            self.assertEqual(backup_code, 0)
            self.assertEqual(len(list(backups_dir.glob("plain-*.db"))), 1)
            self.assertEqual(len(list(backups_dir.glob("plain-*.db.gz"))), 0)

    def test_backup_returns_error_when_snapshot_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            db_path = root / "source.db"
            backups_dir = root / "backups"
            _create_sample_db(db_path)

            with patch.object(backup_sqlite, "_snapshot_sqlite", side_effect=RuntimeError("boom")):
                code = backup_sqlite.main(
                    [
                        "--db-path",
                        str(db_path),
                        "--output-dir",
                        str(backups_dir),
                    ]
                )
            self.assertEqual(code, 1)

    def test_backup_returns_error_when_compression_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            db_path = root / "source.db"
            backups_dir = root / "backups"
            _create_sample_db(db_path)

            with patch.object(backup_sqlite.gzip, "open", side_effect=RuntimeError("gzip broken")):
                code = backup_sqlite.main(
                    [
                        "--db-path",
                        str(db_path),
                        "--output-dir",
                        str(backups_dir),
                    ]
                )
            self.assertEqual(code, 1)
            self.assertEqual(len(list(backups_dir.glob("*.db"))), 0)

    def test_prune_old_backups_handles_non_positive_keep_last(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_dir = root / "backups"
            output_dir.mkdir(parents=True, exist_ok=True)
            stale = output_dir / "sales-agent-20260101.db"
            stale.write_text("x", encoding="utf-8")
            backup_sqlite._prune_old_backups(output_dir, prefix="sales-agent", keep_last=0)
            self.assertTrue(stale.exists())

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

    def test_restore_fails_when_backup_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            target_db = root / "target.db"
            code = restore_sqlite.main(
                [
                    "--backup-path",
                    str(root / "missing.db"),
                    "--db-path",
                    str(target_db),
                ]
            )
            self.assertEqual(code, 1)

    def test_restore_from_plain_db_backup(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source_db = root / "source.db"
            plain_backup = root / "plain_backup.db"
            target_db = root / "target.db"
            _create_sample_db(source_db)
            plain_backup.write_bytes(source_db.read_bytes())

            code = restore_sqlite.main(
                [
                    "--backup-path",
                    str(plain_backup),
                    "--db-path",
                    str(target_db),
                ]
            )
            self.assertEqual(code, 0)
            conn = sqlite3.connect(target_db)
            try:
                row = conn.execute("SELECT COUNT(*) FROM events").fetchone()
            finally:
                conn.close()
            self.assertEqual(int(row[0]), 1)

    def test_restore_fails_when_backup_is_not_valid_sqlite(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            bad_backup = root / "broken.db"
            bad_backup.write_text("not-a-sqlite-file", encoding="utf-8")
            target_db = root / "target.db"

            code = restore_sqlite.main(
                [
                    "--backup-path",
                    str(bad_backup),
                    "--db-path",
                    str(target_db),
                ]
            )
            self.assertEqual(code, 1)


if __name__ == "__main__":
    unittest.main()
