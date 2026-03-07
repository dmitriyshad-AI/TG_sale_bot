#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sales_agent.sales_core import db
from sales_agent.sales_core.config import get_settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Show SQLite schema migration status.")
    parser.add_argument("--db-path", default="", help="Optional SQLite path override.")
    parser.add_argument("--json", action="store_true", help="Output JSON.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    settings = get_settings()
    db_path = Path(args.db_path).expanduser() if str(args.db_path).strip() else Path(settings.database_path)

    db.init_db(db_path)
    conn = db.get_connection(db_path)
    try:
        applied = db.list_applied_migrations(conn)
    finally:
        conn.close()

    known_versions = [version for version, _ in db.SCHEMA_MIGRATION_STEPS]
    pending = [version for version in known_versions if version not in set(applied)]

    if args.json:
        import json

        print(
            json.dumps(
                {
                    "db_path": str(db_path),
                    "known_versions": known_versions,
                    "applied_versions": applied,
                    "pending_versions": pending,
                    "ok": len(pending) == 0,
                },
                ensure_ascii=False,
            )
        )
    else:
        print(f"DB path: {db_path}")
        print(f"Known migrations: {len(known_versions)}")
        print(f"Applied migrations: {len(applied)}")
        print(f"Pending migrations: {len(pending)}")
        if pending:
            print("Pending:")
            for version in pending:
                print(f"- {version}")
        else:
            print("All migrations are applied.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
