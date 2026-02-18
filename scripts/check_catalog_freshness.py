#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sales_agent.sales_core.catalog import CatalogValidationError, Product, load_catalog


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check catalog freshness and operational quality rules")
    parser.add_argument(
        "--path",
        type=Path,
        default=None,
        help="Path to catalog yaml (default: catalog/products.yaml)",
    )
    parser.add_argument(
        "--today",
        type=str,
        default=None,
        help="Override current date in ISO format (YYYY-MM-DD) for deterministic checks",
    )
    parser.add_argument(
        "--stale-days",
        type=int,
        default=365,
        help="Fail if latest session is older than this threshold in days",
    )
    return parser.parse_args()


def _session_boundary(product: Product) -> date | None:
    if not product.sessions:
        return None
    boundaries = [(session.end_date or session.start_date) for session in product.sessions]
    return max(boundaries) if boundaries else None


def _has_future_or_active_session(product: Product, today: date) -> bool:
    for session in product.sessions:
        boundary = session.end_date or session.start_date
        if boundary >= today:
            return True
    return False


def _check_product(product: Product, today: date, stale_days: int) -> list[str]:
    issues: list[str] = []
    boundary = _session_boundary(product)
    if boundary is None:
        return issues

    if boundary < (today - timedelta(days=stale_days)):
        issues.append(
            f"{product.id}: latest session date {boundary.isoformat()} is older than {stale_days} days"
        )

    if all(session.price_rub is None for session in product.sessions):
        issues.append(f"{product.id}: all sessions have empty price_rub")

    return issues


def main() -> int:
    args = parse_args()
    today = date.today()
    if args.today:
        try:
            today = date.fromisoformat(args.today)
        except ValueError:
            print(f"[ERROR] Invalid --today date: {args.today}", file=sys.stderr)
            return 1

    try:
        catalog = load_catalog(args.path)
    except FileNotFoundError as exc:
        print(f"[ERROR] Catalog file not found: {exc.filename}", file=sys.stderr)
        return 1
    except CatalogValidationError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1

    all_issues: list[str] = []
    for product in catalog.products:
        all_issues.extend(_check_product(product=product, today=today, stale_days=max(1, args.stale_days)))

    camp_products = [product for product in catalog.products if product.category == "camp"]
    camp_products_with_sessions = [product for product in camp_products if product.sessions]
    if camp_products_with_sessions:
        has_future_camp = any(_has_future_or_active_session(product, today) for product in camp_products_with_sessions)
        if not has_future_camp:
            all_issues.append(f"catalog: no camp has upcoming sessions after {today.isoformat()}")

    if all_issues:
        print(f"[ERROR] Catalog freshness check failed: {len(all_issues)} issue(s)")
        for issue in all_issues:
            print(f"  - {issue}")
        return 1

    print(
        f"[OK] Catalog freshness check passed for {len(catalog.products)} products "
        f"(today={today.isoformat()}, stale_days={max(1, args.stale_days)})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
