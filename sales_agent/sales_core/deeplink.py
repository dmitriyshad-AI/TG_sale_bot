from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Dict, Optional
from urllib.parse import parse_qs


@dataclass
class DeepLinkMeta:
    brand: Optional[str] = None
    source: Optional[str] = None
    page: Optional[str] = None
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None

    def to_dict(self) -> Dict[str, str]:
        payload: Dict[str, str] = {}
        if self.brand:
            payload["brand"] = self.brand
        if self.source:
            payload["source"] = self.source
        if self.page:
            payload["page"] = self.page
        if self.utm_source:
            payload["utm_source"] = self.utm_source
        if self.utm_medium:
            payload["utm_medium"] = self.utm_medium
        if self.utm_campaign:
            payload["utm_campaign"] = self.utm_campaign
        return payload


def _shorten(value: Optional[str], limit: int) -> Optional[str]:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    return cleaned[:limit]


def encode_start_payload(meta: DeepLinkMeta, max_len: int = 64) -> str:
    compact = {
        "b": _shorten(meta.brand, 10),
        "s": _shorten(meta.source, 14),
        "p": _shorten(meta.page, 24),
        "us": _shorten(meta.utm_source, 14),
        "um": _shorten(meta.utm_medium, 14),
        "uc": _shorten(meta.utm_campaign, 14),
    }
    compact = {key: value for key, value in compact.items() if value}

    def _encode(payload: Dict[str, str]) -> str:
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        encoded = base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")
        return f"dl_{encoded}"

    token = _encode(compact)
    if len(token) <= max_len:
        return token

    compact.pop("uc", None)
    compact.pop("um", None)
    compact.pop("us", None)
    token = _encode(compact)

    if len(token) <= max_len:
        return token

    compact["p"] = _shorten(compact.get("p"), 12)
    token = _encode(compact)
    if len(token) <= max_len:
        return token

    compact.pop("p", None)
    token = _encode(compact)
    if len(token) <= max_len:
        return token

    compact = {}
    return _encode(compact)


def _decode_payload_token(token: str) -> Dict[str, str]:
    body = token[3:]
    padding = "=" * ((4 - len(body) % 4) % 4)
    decoded = base64.urlsafe_b64decode((body + padding).encode("ascii")).decode("utf-8")
    data = json.loads(decoded)
    if not isinstance(data, dict):
        return {}
    result = {
        "brand": str(data.get("b", "")).strip(),
        "source": str(data.get("s", "")).strip(),
        "page": str(data.get("p", "")).strip(),
        "utm_source": str(data.get("us", "")).strip(),
        "utm_medium": str(data.get("um", "")).strip(),
        "utm_campaign": str(data.get("uc", "")).strip(),
    }
    return {key: value for key, value in result.items() if value}


def parse_start_payload(payload: Optional[str]) -> Dict[str, str]:
    if not payload:
        return {}

    token = payload.strip()
    if not token:
        return {}

    if token.startswith("dl_"):
        try:
            return _decode_payload_token(token)
        except Exception:
            return {}

    parsed = parse_qs(token, keep_blank_values=False)
    result: Dict[str, str] = {}
    for key in ("brand", "source", "page", "utm_source", "utm_medium", "utm_campaign"):
        values = parsed.get(key)
        if values:
            value = values[0].strip()
            if value:
                result[key] = value
    return result


def build_greeting_hint(meta: Dict[str, str]) -> Optional[str]:
    page = meta.get("page", "").lower()
    source = meta.get("source")

    if "camp" in page or "kanikul" in page or "лагер" in page:
        hint = "Вижу, что вы пришли со страницы лагеря. Помогу быстро подобрать смену."
    elif "ege" in page:
        hint = "Вижу, что вы смотрели ЕГЭ-направление. Подберу подходящий курс."
    elif "oge" in page:
        hint = "Вижу интерес к ОГЭ. Подберу программу по вашему предмету."
    else:
        hint = None

    if source == "site":
        source_hint = "Вы пришли с сайта — можно сразу перейти к короткой квалификации."
        if hint:
            return f"{hint} {source_hint}"
        return source_hint

    return hint
