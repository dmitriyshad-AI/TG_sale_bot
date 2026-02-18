#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sales_agent.sales_core.config import get_settings
from sales_agent.sales_core.runtime_diagnostics import build_runtime_diagnostics


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Local preflight audit for sales-agent runtime configuration.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print full diagnostics JSON only.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        settings = get_settings()
    except ValueError as exc:
        payload = {
            "status": "fail",
            "runtime": {},
            "issues": [
                {
                    "severity": "error",
                    "code": "invalid_configuration",
                    "message": str(exc),
                }
            ],
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print("Preflight status: FAIL")
            print(f"- configuration error: {exc}")
        return 1

    diagnostics = build_runtime_diagnostics(settings)
    status = str(diagnostics.get("status") or "fail").lower()

    if args.json:
        print(json.dumps(diagnostics, ensure_ascii=False, indent=2))
    else:
        runtime = diagnostics.get("runtime", {})
        issues = diagnostics.get("issues", [])
        print(f"Preflight status: {status.upper()}")
        print(f"- telegram_mode: {runtime.get('telegram_mode')}")
        print(f"- openai_model: {runtime.get('openai_model')}")
        print(f"- catalog_ok: {runtime.get('catalog_ok')} ({runtime.get('catalog_products_count')} products)")
        print(f"- knowledge_files_count: {runtime.get('knowledge_files_count')}")
        print(f"- vector_store_id_set: {runtime.get('vector_store_id_set')}")
        if issues:
            print("- issues:")
            for item in issues:
                print(f"  [{item.get('severity')}] {item.get('code')}: {item.get('message')}")

    if status == "fail":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
