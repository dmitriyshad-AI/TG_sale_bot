#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from html import unescape
from pathlib import Path
from typing import Dict, List, Sequence, Tuple
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sales_agent.sales_core.catalog import Catalog, CatalogValidationError, parse_catalog

SITEMAP_URL = "https://kmipt.ru/sitemap.xml"
BASE_URL = "https://kmipt.ru"

KNOWN_SUBJECTS = (
    ("math", ("математ", "math")),
    ("physics", ("физик", "physics")),
    ("informatics", ("информат", "programming", "code")),
    ("russian", ("русск", "russian")),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync KMIPT catalog from public sitemap and course pages")
    parser.add_argument(
        "--output",
        type=Path,
        default=PROJECT_ROOT / "catalog" / "products.yaml",
        help="Path to write resulting catalog YAML",
    )
    parser.add_argument(
        "--sitemap-url",
        type=str,
        default=SITEMAP_URL,
        help="Sitemap URL with KMIPT courses",
    )
    parser.add_argument(
        "--base-url",
        type=str,
        default=BASE_URL,
        help="Base site URL used for filtering course links",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=25.0,
        help="HTTP timeout in seconds per request",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=6,
        help="Retries per request before failing",
    )
    parser.add_argument(
        "--check-catalog",
        type=Path,
        default=None,
        help="Only validate existing catalog against website (URLs and titles) and exit",
    )
    return parser.parse_args()


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def strip_html(raw_html: str) -> str:
    text = re.sub(r"<script[^>]*>.*?</script>", " ", raw_html, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    return _normalize_space(text)


def fetch_url(url: str, timeout: float, retries: int) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        },
    )

    last_error: Exception | None = None
    for attempt in range(max(1, retries)):
        try:
            with urlopen(request, timeout=timeout) as response:
                return response.read().decode("utf-8", errors="ignore")
        except Exception as exc:  # pragma: no cover - tested via retry behavior in caller
            last_error = exc
            time.sleep(min(2.5, 0.35 * (attempt + 1)))

    curl_result = subprocess.run(
        [
            "curl",
            "-L",
            "--retry",
            str(max(1, retries)),
            "--retry-all-errors",
            "--connect-timeout",
            str(max(5, int(timeout / 2))),
            "--max-time",
            str(max(10, int(timeout))),
            "-s",
            url,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if curl_result.returncode == 0 and curl_result.stdout.strip():
        return curl_result.stdout

    if last_error is not None:
        raise RuntimeError(f"failed to fetch {url}: {last_error}") from last_error
    raise RuntimeError(f"failed to fetch {url}: curl exit={curl_result.returncode}")


def extract_h1(html: str) -> str:
    match = re.search(r"<h1[^>]*>(.*?)</h1>", html, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    return strip_html(match.group(1))


def extract_meta_description(html: str) -> str:
    match = re.search(r'<meta\s+name="description"\s+content="([^"]*)"', html, flags=re.IGNORECASE)
    if not match:
        return ""
    return _normalize_space(unescape(match.group(1)))


def extract_price_rub(html: str) -> int | None:
    match = re.search(r'class="price_value">\s*([^<]+)<', html, flags=re.IGNORECASE)
    if not match:
        return None
    digits = re.sub(r"\D", "", match.group(1))
    return int(digits) if digits else None


def _xml_tag_name(tag: str) -> str:
    return tag.split("}", 1)[-1].lower()


def _extract_xml_locs(root: ET.Element) -> List[str]:
    locs: List[str] = []
    for node in root.iter():
        if _xml_tag_name(node.tag) != "loc":
            continue
        if not node.text:
            continue
        value = node.text.strip()
        if value:
            locs.append(value)
    return locs


def _filter_course_urls(urls: Sequence[str], base_url: str) -> List[str]:
    seen: set[str] = set()
    result: List[str] = []
    prefix = base_url.rstrip("/") + "/courses/"
    for url in urls:
        if not url.startswith(prefix):
            continue
        path_parts = [part for part in urlparse(url).path.split("/") if part]
        if len(path_parts) < 3:
            continue
        if url in seen:
            continue
        seen.add(url)
        result.append(url)
    return result


def parse_sitemap_urls(sitemap_xml: str, base_url: str) -> List[str]:
    try:
        root = ET.fromstring(sitemap_xml)
    except ET.ParseError as exc:
        raise RuntimeError("invalid sitemap XML") from exc
    if _xml_tag_name(root.tag) != "urlset":
        raise RuntimeError("parse_sitemap_urls expects XML <urlset>")
    return _filter_course_urls(_extract_xml_locs(root), base_url=base_url)


def collect_course_urls_from_sitemaps(
    sitemap_url: str,
    base_url: str,
    timeout: float,
    retries: int,
) -> List[str]:
    sitemap_queue: List[str] = [sitemap_url]
    seen_sitemaps: set[str] = set()
    course_urls: List[str] = []
    seen_courses: set[str] = set()

    while sitemap_queue:
        current_sitemap = sitemap_queue.pop(0)
        if current_sitemap in seen_sitemaps:
            continue
        seen_sitemaps.add(current_sitemap)

        raw_xml = fetch_url(current_sitemap, timeout=timeout, retries=retries)
        try:
            root = ET.fromstring(raw_xml)
        except ET.ParseError as exc:
            raise RuntimeError(f"invalid sitemap XML: {current_sitemap}") from exc

        root_name = _xml_tag_name(root.tag)
        locs = _extract_xml_locs(root)

        if root_name == "sitemapindex":
            for loc in locs:
                if loc.endswith(".xml"):
                    sitemap_queue.append(loc)
            continue

        if root_name != "urlset":
            continue

        for url in _filter_course_urls(locs, base_url=base_url):
            if url in seen_courses:
                continue
            seen_courses.add(url)
            course_urls.append(url)

    if not course_urls:
        raise RuntimeError("no course URLs found in sitemap index")
    return course_urls


def _match_grade_range(text: str) -> Tuple[int, int] | None:
    dash = re.search(r"\b(1[01]|[1-9])\s*[-–—]\s*(1[01]|[1-9])\s*класс", text)
    if dash:
        left = int(dash.group(1))
        right = int(dash.group(2))
        return min(left, right), max(left, right)
    single = re.search(r"\b(1[01]|[1-9])\s*класс", text)
    if single:
        value = int(single.group(1))
        return value, value
    return None


def infer_category(url: str, title: str, description: str) -> str:
    source = f"{url}\n{title}\n{description}".lower()
    if "огэ" in source or "/oge/" in source:
        return "oge"
    if "егэ" in source or "/ege/" in source or "/ege_10/" in source:
        return "ege"
    if "олимп" in source:
        return "olympiad"
    if "лагер" in source or "каникул" in source:
        return "camp"
    if "интенсив" in source:
        return "intensive"
    return "base"


def infer_grades(url: str, title: str, description: str, category: str) -> Tuple[int, int]:
    source = f"{url}\n{title}\n{description}".lower()
    matched = _match_grade_range(source)
    if matched:
        return matched

    if "/online_5_8/courses_8/" in url:
        return 8, 8
    if "/online_5_8/courses_7/" in url.lower():
        return 7, 7
    if "/online_5_8/courses_6/" in url:
        return 6, 6
    if "/online_5_8/courses_5/" in url:
        return 5, 5
    if "/online_5_8/courses_online_3_4/" in url:
        return 3, 4
    if "/ege_10/" in url or "10 класс" in source:
        return 10, 10
    if "/ege/" in url and category == "ege":
        return 11, 11
    if "/oge/" in url and category == "oge":
        return 9, 9
    if "/school_5_8/" in url.lower() or "/online_5_8/" in url:
        return 5, 8
    if "/1_4_klass/" in url:
        return 1, 4
    if category == "camp":
        camp_match = re.search(r"_(1[01]|[1-9])__([1-9]|1[01])_kl", url.lower())
        if camp_match:
            left = int(camp_match.group(1))
            right = int(camp_match.group(2))
            return min(left, right), max(left, right)
    if category == "ege":
        return 10, 11
    if category == "oge":
        return 8, 9
    return 5, 11


def infer_subjects(url: str, title: str, description: str) -> List[str]:
    source = f"{url}\n{title}\n{description}".lower()
    subjects: List[str] = []
    for canonical, patterns in KNOWN_SUBJECTS:
        if any(pattern in source for pattern in patterns):
            subjects.append(canonical)
    if subjects:
        return list(dict.fromkeys(subjects))
    return ["general"]


def infer_format(url: str, title: str, description: str, category: str) -> str:
    source = f"{url}\n{title}\n{description}".lower()
    is_online = "онлайн" in source or "/online/" in source or "/online_5_8/" in source
    is_offline = (
        "очно" in source
        or "офлайн" in source
        or "выезд" in source
        or "жуковск" in source
        or category == "camp"
    )
    if is_online and is_offline:
        return "hybrid"
    if is_online:
        return "online"
    if is_offline:
        return "offline"
    if "/ege/" in source or "/ege_10/" in source or "/oge/" in source or "/school_5_8/" in source:
        return "offline"
    return "hybrid"


def make_product_id(url: str) -> str:
    path = urlparse(url).path.strip("/").lower()
    slug = re.sub(r"[^a-z0-9/_-]+", "-", path).replace("/", "-")
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    base = f"kmipt-{slug}"[:63]
    return base if len(base) >= 3 else "kmipt-product"


def build_usp(url: str, title: str, description: str, grade_min: int, grade_max: int, fmt: str, price: int | None) -> List[str]:
    format_label = {"online": "онлайн", "offline": "очно", "hybrid": "смешанный"}[fmt]
    bullets = [
        f"Название на странице: {title}",
        f"Источник: {url}",
        f"Уровень по разделу сайта: {grade_min}-{grade_max} класс, формат {format_label}",
    ]
    if description:
        bullets.append(f"Описание страницы: {description[:180]}")
    if price is not None:
        bullets.append(f"Цена на странице: {price} руб.")
    return bullets[:7]


def build_product(url: str, html: str) -> Dict[str, object]:
    title = extract_h1(html)
    if not title:
        raise RuntimeError(f"missing h1 title: {url}")
    description = extract_meta_description(html)
    price = extract_price_rub(html)
    category = infer_category(url, title, description)
    grade_min, grade_max = infer_grades(url, title, description, category)
    subjects = infer_subjects(url, title, description)
    fmt = infer_format(url, title, description, category)
    product: Dict[str, object] = {
        "id": make_product_id(url),
        "brand": "kmipt",
        "title": title,
        "url": url,
        "category": category,
        "grade_min": grade_min,
        "grade_max": grade_max,
        "subjects": subjects,
        "format": fmt,
        "usp": build_usp(url, title, description, grade_min, grade_max, fmt, price),
    }
    return product


def _dedupe_ids(products: Sequence[Dict[str, object]]) -> List[Dict[str, object]]:
    used: set[str] = set()
    result: List[Dict[str, object]] = []
    for item in products:
        base_id = str(item["id"])
        candidate = base_id
        index = 2
        while candidate in used:
            suffix = f"-{index}"
            candidate = (base_id[: 63 - len(suffix)] + suffix).strip("-")
            index += 1
        item_copy = dict(item)
        item_copy["id"] = candidate
        used.add(candidate)
        result.append(item_copy)
    return result


def scrape_products(
    sitemap_url: str,
    base_url: str,
    timeout: float,
    retries: int,
) -> List[Dict[str, object]]:
    course_urls = collect_course_urls_from_sitemaps(
        sitemap_url=sitemap_url,
        base_url=base_url,
        timeout=timeout,
        retries=retries,
    )

    products: List[Dict[str, object]] = []
    for index, url in enumerate(course_urls, start=1):
        html = fetch_url(url, timeout=timeout, retries=retries)
        product = build_product(url=url, html=html)
        products.append(product)
        print(f"[OK] {index:02d}/{len(course_urls)} {product['title']}")

    products = _dedupe_ids(products)
    products.sort(key=lambda item: str(item["url"]))
    return products


def _catalog_from_products(products: Sequence[Dict[str, object]]) -> Catalog:
    payload = {"products": list(products)}
    try:
        return parse_catalog(payload, Path("memory://kmipt-sync.yaml"))
    except CatalogValidationError as exc:
        raise RuntimeError(str(exc)) from exc


def _normalize_url(url: str) -> str:
    return url.rstrip("/") + "/"


def check_catalog_against_site(
    catalog_path: Path,
    products_from_site: Sequence[Dict[str, object]],
) -> int:
    if not catalog_path.exists():
        print(f"[ERROR] catalog file not found: {catalog_path}")
        return 1

    catalog_data = yaml.safe_load(catalog_path.read_text(encoding="utf-8")) or {}
    catalog = parse_catalog(catalog_data, catalog_path)

    local_by_url = {_normalize_url(str(item.url)): item for item in catalog.products}
    site_by_url = {_normalize_url(str(item["url"])): item for item in products_from_site}

    local_urls = set(local_by_url.keys())
    site_urls = set(site_by_url.keys())

    missing_in_catalog = sorted(site_urls - local_urls)
    extra_in_catalog = sorted(local_urls - site_urls)

    title_mismatch: List[Tuple[str, str, str]] = []
    for url in sorted(local_urls & site_urls):
        local_title = local_by_url[url].title.strip()
        site_title = str(site_by_url[url]["title"]).strip()
        if local_title != site_title:
            title_mismatch.append((url, local_title, site_title))

    print("[INFO] catalog check summary:")
    print(f"  - local products: {len(local_urls)}")
    print(f"  - site products: {len(site_urls)}")
    print(f"  - missing in catalog: {len(missing_in_catalog)}")
    print(f"  - extra in catalog: {len(extra_in_catalog)}")
    print(f"  - title mismatches: {len(title_mismatch)}")

    if missing_in_catalog:
        print("[DETAIL] missing in catalog:")
        for url in missing_in_catalog:
            print(f"  - {url}")
    if extra_in_catalog:
        print("[DETAIL] extra in catalog:")
        for url in extra_in_catalog:
            print(f"  - {url}")
    if title_mismatch:
        print("[DETAIL] title mismatches:")
        for url, local_title, site_title in title_mismatch:
            print(f"  - {url}")
            print(f"    local: {local_title}")
            print(f"    site:  {site_title}")

    return 1 if (missing_in_catalog or extra_in_catalog or title_mismatch) else 0


def write_catalog(output_path: Path, products: Sequence[Dict[str, object]]) -> None:
    payload = {"products": list(products)}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def main() -> int:
    args = parse_args()
    try:
        products = scrape_products(
            sitemap_url=args.sitemap_url,
            base_url=args.base_url,
            timeout=args.timeout,
            retries=max(1, args.retries),
        )
        _catalog_from_products(products)
    except (RuntimeError, URLError, OSError, CatalogValidationError) as exc:
        print(f"[ERROR] {exc}")
        return 1

    if args.check_catalog:
        return check_catalog_against_site(catalog_path=args.check_catalog, products_from_site=products)

    write_catalog(args.output, products)
    print(f"[OK] synced catalog: {args.output} ({len(products)} products)")
    print("[INFO] run check:")
    print(f"  python3 scripts/sync_kmipt_catalog.py --check-catalog {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
