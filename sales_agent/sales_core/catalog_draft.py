from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import date
from html import unescape
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin, urlparse
from urllib.request import HTTPCookieProcessor, Request, build_opener

from http.cookiejar import CookieJar
from urllib.error import HTTPError


MONTHS_RU = {
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
}


@dataclass(frozen=True)
class ProductCandidate:
    brand: str
    title: str
    url: str
    description: str = ""
    list_price: Optional[int] = None
    format_hint: Optional[str] = None


COOKIE_JAR = CookieJar()
OPENER = build_opener(HTTPCookieProcessor(COOKIE_JAR))


def fetch_html(url: str, timeout: float = 25.0) -> str:
    parsed = urlparse(url)
    host_base = f"{parsed.scheme}://{parsed.netloc}/"

    request = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
            "Referer": host_base,
        },
    )
    last_error: Optional[Exception] = None
    for attempt in range(3):
        try:
            with OPENER.open(request, timeout=timeout) as response:
                body = response.read()
            return body.decode("utf-8", errors="ignore")
        except HTTPError as exc:
            last_error = exc
            if exc.code != 503:
                raise
            # Warm up a cookie-enabled session and retry.
            warmup = Request(
                host_base,
                headers={
                    "User-Agent": request.headers.get("User-Agent", ""),
                    "Accept-Language": request.headers.get("Accept-Language", ""),
                },
            )
            with OPENER.open(warmup, timeout=timeout):
                pass
            time.sleep(0.8 + attempt * 0.6)

    if last_error:
        raise last_error

    raise RuntimeError(f"Failed to fetch URL: {url}")


def strip_html(text: str) -> str:
    cleaned = re.sub(r"<!--.*?-->", " ", text, flags=re.DOTALL)
    cleaned = re.sub(r"<script[^>]*>.*?</script>", " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(r"<style[^>]*>.*?</style>", " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(r"<br\s*/?>", "\n", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    cleaned = unescape(cleaned)
    cleaned = re.sub(r"[ \t\r\f\v]+", " ", cleaned)
    cleaned = re.sub(r"\n\s*", "\n", cleaned)
    return cleaned.strip()


def parse_number(value: str) -> Optional[int]:
    digits = "".join(ch for ch in value if ch.isdigit())
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def derive_category(text: str) -> str:
    lowered = text.lower()
    if "егэ" in lowered or "ege" in lowered:
        return "ege"
    if "огэ" in lowered or "oge" in lowered:
        return "oge"
    if "олимп" in lowered:
        return "olympiad"
    if "лагер" in lowered or "каникул" in lowered or "выезд" in lowered:
        return "camp"
    if "интенсив" in lowered:
        return "intensive"
    return "base"


def derive_subjects(text: str) -> List[str]:
    lowered = text.lower()
    subjects: List[str] = []
    if "матем" in lowered:
        subjects.append("math")
    if "физ" in lowered:
        subjects.append("physics")
    if "информ" in lowered or "программ" in lowered:
        subjects.append("informatics")
    if "русск" in lowered:
        subjects.append("russian")
    if "хим" in lowered:
        subjects.append("chemistry")
    if "биолог" in lowered:
        subjects.append("biology")
    if not subjects:
        subjects.append("general")
    return subjects


def derive_format(title_text: str, detail_text: str, format_hint: Optional[str] = None) -> str:
    hint = (format_hint or "").lower()
    combined = f"{title_text}\n{detail_text}".lower()

    online = "онлайн" in combined or "online" in combined or hint == "online"
    offline = (
        "очно" in combined
        or "офлайн" in combined
        or "выезд" in combined
        or "лагер" in combined
        or hint == "offline"
    )
    if online and offline:
        return "hybrid"
    if online:
        return "online"
    if offline:
        return "offline"
    return "offline"


def derive_grade_range(text: str, category: str) -> Tuple[int, int]:
    lowered = text.lower()

    dash_pattern = re.search(
        r"\b(1[01]|[1-9])\s*[-–—]\s*(1[01]|[1-9])\s*(?:класс(?:а|ов)?|кл\.?)\b",
        lowered,
    )
    if dash_pattern:
        left = int(dash_pattern.group(1))
        right = int(dash_pattern.group(2))
        return min(left, right), max(left, right)

    single_grade = re.search(r"\b(1[01]|[1-9])\s*(?:класс(?:а|ов)?|кл\.?)\b", lowered)
    if single_grade:
        value = int(single_grade.group(1))
        return value, value

    if category == "ege":
        return 10, 11
    if category == "oge":
        return 8, 9
    if category == "camp":
        return 5, 11
    return 5, 11


def extract_price_from_html(html: str) -> Optional[int]:
    by_value = re.search(
        r'class="price_value">\s*([0-9][0-9\s]{2,})\s*<',
        html,
        flags=re.IGNORECASE,
    )
    if by_value:
        return parse_number(by_value.group(1))

    for pattern in (
        r"'price'\s*:\s*'([0-9]+)'",
        r'"price"\s*:\s*"([0-9]+)"',
        r"([0-9][0-9\s]{3,})\s*(?:₽|руб\.?)",
    ):
        match = re.search(pattern, html, flags=re.IGNORECASE)
        if not match:
            continue
        parsed = parse_number(match.group(1))
        if parsed:
            return parsed
    return None


def extract_title_from_html(html: str) -> Optional[str]:
    match = re.search(r"<h1[^>]*>(.*?)</h1>", html, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    title = strip_html(match.group(1))
    return title or None


def extract_meta_description(html: str) -> str:
    match = re.search(
        r'<meta\s+name="description"\s+content="([^"]+)"',
        html,
        flags=re.IGNORECASE,
    )
    if not match:
        return ""
    return strip_html(match.group(1))


def extract_bullet_lines(raw_text: str) -> List[str]:
    if not raw_text:
        return []
    lines = [line.strip(" -\u2022\t") for line in raw_text.splitlines()]
    return [line for line in lines if line and len(line) >= 8]


def build_usp(
    title: str,
    description: str,
    detail_text: str,
    grade_min: int,
    grade_max: int,
) -> List[str]:
    def _usable_line(line: str) -> bool:
        lowered = line.lower()
        if len(line) < 8 or len(line) > 220:
            return False
        if "window.bx" in lowered or "bitrix" in lowered:
            return False
        if lowered.count("{") + lowered.count("}") >= 2:
            return False
        return True

    bullets: List[str] = []
    for candidate in extract_bullet_lines(description) + extract_bullet_lines(detail_text):
        if not _usable_line(candidate):
            continue
        if candidate not in bullets:
            bullets.append(candidate)
        if len(bullets) >= 4:
            break

    if not bullets:
        bullets.append(f"Программа: {title}")
    if len(bullets) < 2:
        bullets.append(f"Подходит для {grade_min}-{grade_max} классов")
    if len(bullets) < 3:
        bullets.append("Данные собраны автоматически с публичного сайта, нужна ручная проверка")
    return bullets[:7]


def parse_ru_date_range(text: str) -> Optional[Tuple[date, Optional[date]]]:
    lowered = text.lower()
    pattern = re.search(
        r"\bс\s*(\d{1,2})\s*по\s*(\d{1,2})\s*([а-я]+)\s*(\d{4})",
        lowered,
    )
    if pattern:
        day_from = int(pattern.group(1))
        day_to = int(pattern.group(2))
        month = MONTHS_RU.get(pattern.group(3))
        year = int(pattern.group(4))
        if month:
            try:
                return date(year, month, day_from), date(year, month, day_to)
            except ValueError:
                return None

    pattern = re.search(
        r"\b(\d{1,2})\s*[-–]\s*(\d{1,2})\s*([а-я]+)\s*(\d{4})",
        lowered,
    )
    if pattern:
        day_from = int(pattern.group(1))
        day_to = int(pattern.group(2))
        month = MONTHS_RU.get(pattern.group(3))
        year = int(pattern.group(4))
        if month:
            try:
                return date(year, month, day_from), date(year, month, day_to)
            except ValueError:
                return None

    return None


def build_sessions(category: str, title: str, detail_text: str, price_rub: Optional[int]) -> List[Dict[str, object]]:
    if category != "camp":
        return []

    parsed = parse_ru_date_range(detail_text)
    if parsed:
        start_date, end_date = parsed
        return [
            {
                "name": "Смена",
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat() if end_date else None,
                "price_rub": price_rub,
            }
        ]

    # Placeholder for mandatory schema field in camp category.
    return [
        {
            "name": "Даты уточняются",
            "start_date": date.today().isoformat(),
            "end_date": None,
            "price_rub": price_rub,
        }
    ]


def _slugify_path(path: str) -> str:
    lowered = path.strip("/").lower().replace("/", "-")
    slug = re.sub(r"[^a-z0-9_-]+", "-", lowered).strip("-_")
    return slug or "product"


def make_product_id(brand: str, url: str, used: set[str]) -> str:
    parsed = urlparse(url)
    base = f"{brand}-{_slugify_path(parsed.path)}"
    base = re.sub(r"-{2,}", "-", base).strip("-")
    base = base[:63] if len(base) > 63 else base
    if len(base) < 3:
        base = f"{brand}-product"

    if base not in used:
        used.add(base)
        return base

    idx = 2
    while True:
        suffix = f"-{idx}"
        candidate = (base[: 63 - len(suffix)] + suffix).strip("-")
        if candidate not in used:
            used.add(candidate)
            return candidate
        idx += 1


def extract_foton_candidates(listing_html: str, base_url: str = "https://cdpofoton.ru") -> List[ProductCandidate]:
    candidates: List[ProductCandidate] = []
    pattern = re.compile(
        r'<a href="(?P<href>/courses/[^"]+/)" class="cart-course__name">\s*(?P<title>.*?)\s*</a>',
        flags=re.IGNORECASE | re.DOTALL,
    )

    for match in pattern.finditer(listing_html):
        href = match.group("href")
        title = strip_html(match.group("title"))
        if not title:
            continue
        block = listing_html[max(0, match.start() - 600) : min(len(listing_html), match.end() + 2200)]
        desc_match = re.search(
            r'<div class="cart-course__desc[^"]*">(.*?)</div>',
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        desc = strip_html(desc_match.group(1)) if desc_match else ""
        price = extract_price_from_html(block)

        format_hint = None
        if "_online" in block.lower():
            format_hint = "online"
        elif "_full_time" in block.lower() or "очный" in block.lower():
            format_hint = "offline"

        candidates.append(
            ProductCandidate(
                brand="foton",
                title=title,
                url=urljoin(base_url, href),
                description=desc,
                list_price=price,
                format_hint=format_hint,
            )
        )
    return candidates


def extract_kmipt_candidates(listing_html: str, source_url: str, base_url: str = "https://kmipt.ru") -> List[ProductCandidate]:
    candidates: List[ProductCandidate] = []
    pattern = re.compile(
        r'<a class="name_title" href="(?P<href>/courses/[^"]+/)">(?P<title>.*?)</a>',
        flags=re.IGNORECASE | re.DOTALL,
    )
    source_lower = source_url.lower()
    source_hint: Optional[str] = "online" if "/online" in source_lower else None

    for match in pattern.finditer(listing_html):
        href = match.group("href")
        title = strip_html(match.group("title"))
        if not title:
            continue

        block = listing_html[max(0, match.start() - 200) : min(len(listing_html), match.end() + 1000)]
        desc_match = re.search(
            r'<p class="desc">(.*?)</p>',
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        desc = strip_html(desc_match.group(1)) if desc_match else ""

        candidates.append(
            ProductCandidate(
                brand="kmipt",
                title=title,
                url=urljoin(base_url, href),
                description=desc,
                list_price=extract_price_from_html(block),
                format_hint=source_hint,
            )
        )
    return candidates


def unique_candidates(candidates: Sequence[ProductCandidate]) -> List[ProductCandidate]:
    seen: set[Tuple[str, str]] = set()
    unique: List[ProductCandidate] = []
    for candidate in candidates:
        key = (candidate.brand, candidate.url)
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
    return unique


def build_product_from_candidate(
    candidate: ProductCandidate,
    detail_html: str,
    used_ids: set[str],
) -> Dict[str, object]:
    title = extract_title_from_html(detail_html) or candidate.title
    detail_text = strip_html(detail_html)
    meta_description = extract_meta_description(detail_html)
    merged_text = f"{title}\n{candidate.description}\n{meta_description}".strip()

    category = derive_category(f"{title}\n{candidate.url}")
    grade_min, grade_max = derive_grade_range(merged_text, category)
    price = candidate.list_price or extract_price_from_html(detail_html)
    product_format = derive_format(
        title_text=title,
        detail_text=f"{candidate.description}\n{meta_description}",
        format_hint=candidate.format_hint,
    )
    subjects = derive_subjects(merged_text)
    sessions = build_sessions(
        category=category,
        title=title,
        detail_text=f"{candidate.description}\n{meta_description}\n{detail_text[:3000]}",
        price_rub=price,
    )
    usp = build_usp(
        title=title,
        description=candidate.description,
        detail_text=meta_description,
        grade_min=grade_min,
        grade_max=grade_max,
    )

    return {
        "id": make_product_id(candidate.brand, candidate.url, used=used_ids),
        "brand": candidate.brand,
        "title": title,
        "url": candidate.url,
        "category": category,
        "grade_min": grade_min,
        "grade_max": grade_max,
        "subjects": subjects,
        "format": product_format,
        "sessions": sessions,
        "usp": usp,
    }


def collect_candidates_for_brand(
    brand: str,
    listing_urls: Iterable[str],
    timeout: float = 25.0,
) -> List[ProductCandidate]:
    collected: List[ProductCandidate] = []
    for listing_url in listing_urls:
        try:
            html = fetch_html(listing_url, timeout=timeout)
        except Exception:
            continue
        if brand == "kmipt":
            collected.extend(extract_kmipt_candidates(html, source_url=listing_url))
        elif brand == "foton":
            collected.extend(extract_foton_candidates(html))
    return unique_candidates(collected)
