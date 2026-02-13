#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sales_agent.sales_core.catalog_draft import (  # noqa: E402
    ProductCandidate,
    build_product_from_candidate,
    collect_candidates_for_brand,
    fetch_html,
)


KMIPT_LISTINGS = [
    "https://kmipt.ru/courses/EGE/",
    "https://kmipt.ru/courses/EGE_10/",
    "https://kmipt.ru/courses/oge/",
    "https://kmipt.ru/courses/School_5_8/",
    "https://kmipt.ru/courses/1_4_klass/",
    "https://kmipt.ru/courses/online/",
    "https://kmipt.ru/courses/Kanikuly/",
]

FOTON_LISTINGS = [
    "https://cdpofoton.ru/courses/",
    "https://cdpofoton.ru/courses/filter/directions-is-podgotovka-k-ege/apply/",
    "https://cdpofoton.ru/courses/filter/directions-is-podgotovka-k-oge/apply/",
    "https://cdpofoton.ru/courses/filter/directions-is-olimpiada-fiztekh/apply/",
    "https://cdpofoton.ru/courses/filter/directions-is-na-kanikuly/apply/",
]

KMIPT_FALLBACK_CANDIDATES = [
    ProductCandidate(
        brand="kmipt",
        title="Математика ЕГЭ",
        url="https://kmipt.ru/courses/EGE/Matematika_EGE/",
        format_hint="offline",
    ),
    ProductCandidate(
        brand="kmipt",
        title="Физика ЕГЭ",
        url="https://kmipt.ru/courses/EGE/Fizika_EGE/",
        format_hint="offline",
    ),
    ProductCandidate(
        brand="kmipt",
        title="Информатика ЕГЭ",
        url="https://kmipt.ru/courses/EGE/Informatika_EGE/",
        format_hint="offline",
    ),
    ProductCandidate(
        brand="kmipt",
        title="Русский язык ЕГЭ",
        url="https://kmipt.ru/courses/EGE/Russkiy_yazyk_EGE/",
        format_hint="offline",
    ),
    ProductCandidate(
        brand="kmipt",
        title="Математика 10 класс",
        url="https://kmipt.ru/courses/EGE_10/EGE_Matematika_10/",
        format_hint="offline",
    ),
    ProductCandidate(
        brand="kmipt",
        title="Физика 10 класс",
        url="https://kmipt.ru/courses/EGE_10/Fizika_10/",
        format_hint="offline",
    ),
    ProductCandidate(
        brand="kmipt",
        title="Информатика 10 класс",
        url="https://kmipt.ru/courses/EGE_10/Informatika_10/",
        format_hint="offline",
    ),
    ProductCandidate(
        brand="kmipt",
        title="Олимпиадная подготовка (математика и физика)",
        url="https://kmipt.ru/courses/nabor_online/olimp_math_phys/",
        format_hint="offline",
    ),
    ProductCandidate(
        brand="kmipt",
        title="Летняя выездная школа",
        url="https://kmipt.ru/courses/Kanikuly/Letnyaya_vyezdnaya_fizikomatematicheskaya_shkola_8__11_kl/",
        format_hint="offline",
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build auto-draft catalog from kmipt.ru and cdpofoton.ru public pages"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=PROJECT_ROOT / "catalog" / "products.auto_draft.yaml",
        help="Path to output YAML file",
    )
    parser.add_argument(
        "--limit-per-brand",
        type=int,
        default=20,
        help="Maximum number of products per brand in output",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=25.0,
        help="Network timeout for page requests",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    used_ids: set[str] = set()
    products: List[Dict[str, object]] = []

    per_brand = {
        "kmipt": KMIPT_LISTINGS,
        "foton": FOTON_LISTINGS,
    }
    for brand, listings in per_brand.items():
        print(f"[INFO] collecting candidates for {brand} from {len(listings)} listing pages")
        candidates = collect_candidates_for_brand(brand=brand, listing_urls=listings, timeout=args.timeout)
        if not candidates:
            if brand == "kmipt":
                print("[WARN] no candidates collected for kmipt listings, using fallback URLs")
                candidates = KMIPT_FALLBACK_CANDIDATES
            else:
                print(f"[WARN] no candidates collected for {brand}")
                continue

        added = 0
        for candidate in candidates:
            if added >= max(1, args.limit_per_brand):
                break
            try:
                detail_html = fetch_html(candidate.url, timeout=args.timeout)
                product = build_product_from_candidate(candidate, detail_html=detail_html, used_ids=used_ids)
                products.append(product)
                added += 1
                print(f"[OK] {brand}: {product['title']}")
            except Exception as exc:
                print(f"[WARN] skip {candidate.url}: {exc}")

    if not products:
        print("[ERROR] no products were collected")
        return 1

    deduped: List[Dict[str, object]] = []
    seen_keys = set()
    for item in products:
        dedupe_key = (
            item.get("brand"),
            str(item.get("title", "")).strip().lower(),
            item.get("category"),
            item.get("grade_min"),
            item.get("grade_max"),
            item.get("format"),
        )
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        deduped.append(item)
    products = deduped

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as fh:
        yaml.safe_dump({"products": products}, fh, allow_unicode=True, sort_keys=False)
    print(f"[OK] saved auto-draft catalog: {args.output} ({len(products)} products)")
    print("[INFO] next: run python3 scripts/validate_catalog.py --path", args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
