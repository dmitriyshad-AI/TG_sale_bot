#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import uvicorn

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sales_agent.sales_core.config import get_settings
from sales_agent.sales_core.runtime_diagnostics import enforce_startup_preflight


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Start FastAPI with mandatory preflight checks.")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8000, help="Bind port (default: 8000)")
    parser.add_argument("--reload", action="store_true", help="Enable uvicorn reload mode")
    parser.add_argument("--log-level", default="info", help="Uvicorn log level (default: info)")
    parser.add_argument(
        "--preflight-mode",
        choices=("off", "fail", "strict"),
        default=None,
        help="Override STARTUP_PREFLIGHT_MODE for this launch.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    settings = get_settings()
    diagnostics = enforce_startup_preflight(settings, mode=args.preflight_mode)
    status = str(diagnostics.get("status") or "unknown").upper()
    mode = args.preflight_mode or settings.startup_preflight_mode
    print(f"[start_api] preflight={status} mode={mode}")

    uvicorn.run(
        "sales_agent.sales_api.main:create_app",
        factory=True,
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level=args.log_level,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

