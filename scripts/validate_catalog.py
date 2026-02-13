#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

from sales_agent.sales_core.catalog import CatalogValidationError, load_catalog


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate product catalog schema")
    parser.add_argument(
        "--path",
        type=Path,
        default=None,
        help="Path to catalog yaml (default: catalog/products.yaml)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        catalog = load_catalog(args.path)
    except FileNotFoundError as exc:
        print(f"[ERROR] Catalog file not found: {exc.filename}", file=sys.stderr)
        return 1
    except CatalogValidationError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1

    products = catalog.products
    by_brand = Counter(item.brand for item in products)
    by_category = Counter(item.category for item in products)

    print(f"[OK] Catalog is valid: {len(products)} products")
    print("[INFO] Products by brand:")
    for brand, count in sorted(by_brand.items()):
        print(f"  - {brand}: {count}")

    print("[INFO] Products by category:")
    for category, count in sorted(by_category.items()):
        print(f"  - {category}: {count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
