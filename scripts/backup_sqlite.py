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
    parser = argparse.ArgumentParser(description="Create a consistent SQLite backup.")
    parser.add_argument(
        "--db-path",
        default=os.getenv("DATABASE_PATH", "data/sales_agent.db"),
        help="Path to source SQLite DB.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/backups",
        help="Directory for backup artifacts.",
    )
    parser.add_argument(
        "--prefix",
        default="sales-agent",
        help="Backup file prefix.",
    )
    parser.add_argument(
        "--no-compress",
        action="store_true",
        help="Disable gzip compression and keep plain .db backup.",
    )
    parser.add_argument(
        "--keep-last",
        type=int,
        default=14,
        help="How many most recent backups to keep in output-dir (default: 14).",
    )
    return parser


def _prune_old_backups(output_dir: Path, prefix: str, keep_last: int) -> None:
    if keep_last <= 0:
        return
    files = sorted(
        [path for path in output_dir.glob(f"{prefix}-*.db*") if path.is_file()],
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    for stale in files[keep_last:]:
        stale.unlink(missing_ok=True)


def _snapshot_sqlite(source_path: Path, destination_path: Path) -> None:
    source = sqlite3.connect(str(source_path), timeout=8.0)
    try:
        destination = sqlite3.connect(str(destination_path), timeout=8.0)
        try:
            source.backup(destination)
            destination.commit()
        finally:
            destination.close()
    finally:
        source.close()


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    source_path = Path(args.db_path)
    output_dir = Path(args.output_dir)
    prefix = args.prefix.strip() or "sales-agent"

    if not source_path.exists():
        print(f"[FAIL] Source DB does not exist: {source_path}")
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%SZ")
    raw_backup_path = output_dir / f"{prefix}-{timestamp}.db"

    try:
        _snapshot_sqlite(source_path, raw_backup_path)
    except Exception as exc:
        print(f"[FAIL] Backup snapshot failed: {exc}")
        return 1

    final_path = raw_backup_path
    if not args.no_compress:
        compressed_path = raw_backup_path.with_suffix(raw_backup_path.suffix + ".gz")
        try:
            with raw_backup_path.open("rb") as source, gzip.open(compressed_path, "wb") as target:
                shutil.copyfileobj(source, target)
        except Exception as exc:
            raw_backup_path.unlink(missing_ok=True)
            print(f"[FAIL] Compression failed: {exc}")
            return 1
        raw_backup_path.unlink(missing_ok=True)
        final_path = compressed_path

    _prune_old_backups(output_dir, prefix=prefix, keep_last=max(1, int(args.keep_last)))

    print(f"[OK] backup_created: {final_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
