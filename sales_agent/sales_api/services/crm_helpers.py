from __future__ import annotations

import json
from typing import Any, Callable, Dict, Optional
from urllib.parse import quote_plus

from fastapi import HTTPException, status

from sales_agent.sales_core.tallanto_readonly import TallantoReadOnlyClient, sanitize_tallanto_lookup_context


def crm_cache_key(prefix: str, params: Dict[str, Any]) -> str:
    serialized = json.dumps(params, ensure_ascii=False, sort_keys=True)
    return f"crm:{prefix}:{quote_plus(serialized)}"


def read_crm_cache(
    *,
    database_path: Any,
    key: str,
    max_age_seconds: int,
    get_connection: Callable[[Any], Any],
    get_crm_cache: Callable[..., Optional[Dict[str, Any]]],
) -> Optional[Dict[str, Any]]:
    conn = get_connection(database_path)
    try:
        return get_crm_cache(conn, key=key, max_age_seconds=max_age_seconds)
    finally:
        conn.close()


def write_crm_cache(
    *,
    database_path: Any,
    key: str,
    payload: Dict[str, Any],
    get_connection: Callable[[Any], Any],
    upsert_crm_cache: Callable[..., None],
) -> None:
    conn = get_connection(database_path)
    try:
        upsert_crm_cache(conn, key=key, value=payload)
    finally:
        conn.close()


def map_tallanto_error(exc: RuntimeError) -> HTTPException:
    message = str(exc)
    lowered = message.lower()
    if "401" in message or "unauthorized" in lowered:
        code = status.HTTP_401_UNAUTHORIZED
    elif "400" in message:
        code = status.HTTP_400_BAD_REQUEST
    elif "403" in message:
        code = status.HTTP_403_FORBIDDEN
    else:
        code = status.HTTP_502_BAD_GATEWAY
    return HTTPException(status_code=code, detail=message)


def build_thread_crm_context(
    user_item: Dict[str, Any],
    *,
    settings: Any,
    read_cache: Callable[[str], Optional[Dict[str, Any]]],
    write_cache: Callable[[str, Dict[str, Any]], None],
) -> Dict[str, Any]:
    context: Dict[str, Any] = {
        "enabled": bool(settings.enable_tallanto_enrichment and settings.crm_provider == "tallanto"),
        "found": False,
        "tags": [],
        "interests": [],
        "last_touch_days": None,
    }
    if not context["enabled"]:
        return context

    if not settings.tallanto_read_only:
        context["error"] = "tallanto_read_only_disabled"
        return context
    token = settings.tallanto_api_token or settings.tallanto_api_key
    if not settings.tallanto_api_url or not token:
        context["error"] = "tallanto_not_configured"
        return context

    module = (settings.tallanto_default_contact_module or "contacts").strip() or "contacts"
    external_id = str(user_item.get("external_id") or "").strip()
    username = str(user_item.get("username") or "").strip()
    lookup_candidates: list[tuple[str, str]] = []
    if external_id:
        lookup_candidates.extend(
            [
                ("telegram_id", external_id),
                ("telegram", external_id),
                ("external_id", external_id),
            ]
        )
    if username:
        lookup_candidates.extend(
            [
                ("username", username),
                ("telegram_username", username),
                ("telegram_username", f"@{username}"),
            ]
        )

    deduped: list[tuple[str, str]] = []
    seen_pairs: set[tuple[str, str]] = set()
    for field_name, field_value in lookup_candidates:
        key = (field_name.strip(), field_value.strip())
        if not key[0] or not key[1] or key in seen_pairs:
            continue
        seen_pairs.add(key)
        deduped.append(key)

    if not deduped:
        context["error"] = "lookup_candidates_empty"
        return context

    client = TallantoReadOnlyClient(base_url=settings.tallanto_api_url, token=token)
    for field_name, field_value in deduped:
        cache_key = crm_cache_key(
            "thread_context",
            {"module": module, "field": field_name, "value": field_value},
        )
        cached = read_cache(cache_key)
        if isinstance(cached, dict):
            cached_context = {
                "enabled": True,
                "found": bool(cached.get("found")),
                "tags": list(cached.get("tags") or []),
                "interests": list(cached.get("interests") or []),
                "last_touch_days": cached.get("last_touch_days"),
                "lookup_field": field_name,
            }
            if cached_context["found"]:
                return cached_context

        try:
            primary = client.call(
                "entry_by_fields",
                {"module": module, "fields_values": {field_name: field_value}},
            )
            payload = sanitize_tallanto_lookup_context(primary)
            if not payload.get("found"):
                fallback = client.call(
                    "get_entry_list",
                    {"module": module, "fields_values": {field_name: field_value}},
                )
                payload = sanitize_tallanto_lookup_context(fallback)
        except RuntimeError:
            continue

        response_payload = {
            "found": bool(payload.get("found")),
            "tags": list(payload.get("tags") or []),
            "interests": list(payload.get("interests") or []),
            "last_touch_days": payload.get("last_touch_days"),
        }
        write_cache(cache_key, response_payload)
        if response_payload["found"]:
            return {
                "enabled": True,
                **response_payload,
                "lookup_field": field_name,
            }

    return context

