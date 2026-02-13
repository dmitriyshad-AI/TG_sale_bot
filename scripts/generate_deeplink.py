#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sales_agent.sales_core.deeplink import DeepLinkMeta, encode_start_payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate Telegram deep-link payload")
    parser.add_argument("--bot-username", required=True, help="Telegram bot username without @")
    parser.add_argument("--brand", choices=["kmipt", "foton"], default="kmipt")
    parser.add_argument("--source", default="site")
    parser.add_argument("--page", default="/")
    parser.add_argument("--utm-source", default=None)
    parser.add_argument("--utm-medium", default=None)
    parser.add_argument("--utm-campaign", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = encode_start_payload(
        DeepLinkMeta(
            brand=args.brand,
            source=args.source,
            page=args.page,
            utm_source=args.utm_source,
            utm_medium=args.utm_medium,
            utm_campaign=args.utm_campaign,
        )
    )
    print(f"https://t.me/{args.bot_username}?start={payload}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
