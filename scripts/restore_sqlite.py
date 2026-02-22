#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import os
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Restore SQLite DB from backup file.")
    parser.add_argument("--backup-path", required=True, help="Path to .db or .db.gz backup.")
    parser.add_argument(
        "--db-path",
        default=os.getenv("DATABASE_PATH", "data/sales_agent.db"),
        help="Target SQLite DB path.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow overwrite of existing target DB (creates timestamped .bak copy first).",
    )
    return parser


def _validate_sqlite(path: Path) -> None:
    conn = sqlite3.connect(str(path), timeout=5.0)
    try:
        row = conn.execute("PRAGMA integrity_check").fetchone()
        result = str(row[0]) if row else "failed"
    finally:
        conn.close()
    if result.lower() != "ok":
        raise RuntimeError(f"Integrity check failed: {result}")


def _copy_backup_to_target(backup_path: Path, target_path: Path) -> None:
    if backup_path.suffix == ".gz":
        with gzip.open(backup_path, "rb") as source, target_path.open("wb") as destination:
            shutil.copyfileobj(source, destination)
        return
    shutil.copy2(backup_path, target_path)


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    backup_path = Path(args.backup_path)
    target_path = Path(args.db_path)

    if not backup_path.exists():
        print(f"[FAIL] Backup file not found: {backup_path}")
        return 1

    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists():
        if not args.force:
            print(f"[FAIL] Target DB already exists: {target_path}. Use --force to overwrite.")
            return 1
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%SZ")
        backup_current = target_path.with_suffix(target_path.suffix + f".bak-{timestamp}")
        shutil.copy2(target_path, backup_current)

    try:
        _copy_backup_to_target(backup_path, target_path)
        _validate_sqlite(target_path)
    except Exception as exc:
        print(f"[FAIL] Restore failed: {exc}")
        return 1

    print(f"[OK] restore_completed: {target_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
