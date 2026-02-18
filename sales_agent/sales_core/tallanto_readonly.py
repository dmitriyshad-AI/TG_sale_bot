from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ALLOWED_METHODS = {
    "list_possible_modules",
    "list_possible_fields",
    "list_possible_fields_doc",
    "list_enum_values",
    "get_entry_by_id",
    "entry_by_fields",
    "get_entry_list",
}


class TallantoReadOnlyClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        timeout_seconds: float = 10.0,
    ) -> None:
        self.base_url = base_url.strip()
        self.token = token.strip()
        self.timeout_seconds = timeout_seconds

    def is_configured(self) -> bool:
        return bool(self.base_url and self.token)

    def call(self, method: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        normalized_method = (method or "").strip()
        if normalized_method == "set_entry" or normalized_method not in ALLOWED_METHODS:
            raise RuntimeError("Tallanto is read-only")
        if not self.is_configured():
            raise RuntimeError("Tallanto read-only client is not configured.")

        payload: Dict[str, Any] = {
            "method": normalized_method,
            "api_key": self.token,
        }
        if params:
            payload.update(params)

        request = Request(
            self.base_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                response_body = response.read().decode("utf-8")
        except HTTPError as exc:
            raise RuntimeError(f"Tallanto HTTP error: {exc.code}") from exc
        except URLError as exc:
            raise RuntimeError(f"Tallanto connection error: {exc.reason}") from exc

        try:
            parsed = json.loads(response_body) if response_body else {}
        except json.JSONDecodeError as exc:
            raise RuntimeError("Tallanto response is not valid JSON.") from exc
        if isinstance(parsed, dict):
            return parsed
        return {"result": parsed}


def _to_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if value is None:
        return []
    return [value]


def extract_tallanto_items(payload: Dict[str, Any]) -> List[Any]:
    for key in ("result", "data", "items", "entries", "records"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            nested = extract_tallanto_items(value)
            if nested:
                return nested
            return [value]
    if isinstance(payload, list):
        return payload
    return []


def _flatten_text_values(value: Any) -> Iterable[str]:
    if value is None:
        return []
    if isinstance(value, str):
        for chunk in value.replace("|", ",").replace(";", ",").split(","):
            item = chunk.strip()
            if item:
                yield item
        return
    if isinstance(value, (list, tuple, set)):
        for item in value:
            if isinstance(item, (str, int, float)):
                text = str(item).strip()
                if text:
                    yield text
        return
    if isinstance(value, dict):
        for item in value.values():
            if isinstance(item, (str, int, float)):
                text = str(item).strip()
                if text:
                    yield text
        return
    if isinstance(value, (int, float)):
        text = str(value).strip()
        if text:
            yield text


def _extract_first_record(payload: Dict[str, Any]) -> Dict[str, Any]:
    items = extract_tallanto_items(payload)
    for item in items:
        if isinstance(item, dict):
            return item
    if isinstance(payload.get("result"), dict):
        return payload["result"]
    return {}


def _parse_datetime(value: Any) -> Optional[datetime]:
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    normalized = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        pass

    for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(raw, pattern)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def sanitize_tallanto_lookup_context(payload: Dict[str, Any]) -> Dict[str, Any]:
    record = _extract_first_record(payload if isinstance(payload, dict) else {})
    if not record:
        return {
            "found": False,
            "tags": [],
            "interests": [],
            "last_touch_days": None,
        }

    tags: List[str] = []
    interests: List[str] = []
    for key in ("tags", "tag", "segment", "segments", "labels", "categories"):
        tags.extend(_flatten_text_values(record.get(key)))
    for key in ("interests", "interest", "subjects", "goals", "intent", "focus"):
        interests.extend(_flatten_text_values(record.get(key)))

    dedup_tags = list(dict.fromkeys(tags))[:20]
    dedup_interests = list(dict.fromkeys(interests))[:20]

    last_touch_days: Optional[int] = None
    for key in ("last_touch_days", "days_since_last_touch"):
        value = record.get(key)
        if isinstance(value, int):
            last_touch_days = max(0, value)
            break
        if isinstance(value, str) and value.isdigit():
            last_touch_days = max(0, int(value))
            break

    if last_touch_days is None:
        now = datetime.now(timezone.utc)
        for key in ("last_touch_at", "last_contact_at", "updated_at", "date_modified", "modified_at"):
            parsed = _parse_datetime(record.get(key))
            if parsed is None:
                continue
            delta = now - parsed
            last_touch_days = max(0, int(delta.total_seconds() // 86_400))
            break

    return {
        "found": True,
        "tags": dedup_tags,
        "interests": dedup_interests,
        "last_touch_days": last_touch_days,
    }


def normalize_tallanto_modules(payload: Dict[str, Any]) -> List[str]:
    items = extract_tallanto_items(payload if isinstance(payload, dict) else {})
    modules: List[str] = []
    for item in _to_list(items):
        if isinstance(item, str):
            modules.append(item.strip())
            continue
        if isinstance(item, dict):
            value = item.get("module") or item.get("name") or item.get("id")
            if value:
                modules.append(str(value).strip())
    return [item for item in dict.fromkeys(modules) if item]


def normalize_tallanto_fields(payload: Dict[str, Any]) -> List[str]:
    items = extract_tallanto_items(payload if isinstance(payload, dict) else {})
    fields: List[str] = []
    for item in _to_list(items):
        if isinstance(item, str):
            fields.append(item.strip())
            continue
        if isinstance(item, dict):
            value = item.get("field") or item.get("name") or item.get("id")
            if value:
                fields.append(str(value).strip())
    return [item for item in dict.fromkeys(fields) if item]
