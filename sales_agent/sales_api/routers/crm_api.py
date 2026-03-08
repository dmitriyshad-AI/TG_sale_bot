from __future__ import annotations

from typing import Any, Callable, Dict, Optional

from fastapi import APIRouter, Depends, Query

from sales_agent.sales_core.tallanto_readonly import (
    normalize_tallanto_fields,
    normalize_tallanto_modules,
    sanitize_tallanto_lookup_context,
)


def build_crm_api_router(
    *,
    require_crm_api_access: Callable[..., str],
    require_tallanto_readonly_client: Callable[[], Any],
    crm_cache_key: Callable[[str, Dict[str, Any]], str],
    read_crm_cache: Callable[[str], Optional[Dict[str, Any]]],
    write_crm_cache: Callable[[str, Dict[str, Any]], None],
    map_tallanto_error: Callable[[RuntimeError], Exception],
) -> APIRouter:
    router = APIRouter()

    @router.get("/api/crm/meta/modules")
    async def crm_meta_modules(_: str = Depends(require_crm_api_access)):
        client = require_tallanto_readonly_client()
        cache_key = crm_cache_key("modules", {})
        cached = read_crm_cache(cache_key)
        if cached is not None:
            return {"ok": True, "cached": True, **cached}

        try:
            payload = client.call("list_possible_modules", {})
        except RuntimeError as exc:
            raise map_tallanto_error(exc) from exc

        response_payload = {
            "items": normalize_tallanto_modules(payload),
        }
        write_crm_cache(cache_key, response_payload)
        return {"ok": True, "cached": False, **response_payload}

    @router.get("/api/crm/meta/fields")
    async def crm_meta_fields(
        module: str = Query(..., min_length=1, max_length=128),
        _: str = Depends(require_crm_api_access),
    ):
        client = require_tallanto_readonly_client()
        params = {"module": module.strip()}
        cache_key = crm_cache_key("fields", params)
        cached = read_crm_cache(cache_key)
        if cached is not None:
            return {"ok": True, "cached": True, **cached}

        try:
            payload = client.call("list_possible_fields", params)
        except RuntimeError as exc:
            raise map_tallanto_error(exc) from exc

        response_payload = {
            "module": params["module"],
            "items": normalize_tallanto_fields(payload),
        }
        write_crm_cache(cache_key, response_payload)
        return {"ok": True, "cached": False, **response_payload}

    @router.get("/api/crm/lookup")
    async def crm_lookup(
        module: str = Query(..., min_length=1, max_length=128),
        field: str = Query(..., min_length=1, max_length=128),
        value: str = Query(..., min_length=1, max_length=512),
        _: str = Depends(require_crm_api_access),
    ):
        client = require_tallanto_readonly_client()
        normalized_module = module.strip()
        normalized_field = field.strip()
        normalized_value = value.strip()
        params = {
            "module": normalized_module,
            "field": normalized_field,
            "value": normalized_value,
        }
        cache_key = crm_cache_key("lookup", params)
        cached = read_crm_cache(cache_key)
        if cached is not None:
            return {"ok": True, "cached": True, **cached}

        try:
            primary = client.call(
                "entry_by_fields",
                {
                    "module": normalized_module,
                    "fields_values": {normalized_field: normalized_value},
                },
            )
        except RuntimeError as exc:
            raise map_tallanto_error(exc) from exc

        context = sanitize_tallanto_lookup_context(primary)
        fallback_used = False
        if not context.get("found"):
            fallback_used = True
            try:
                fallback = client.call(
                    "get_entry_list",
                    {
                        "module": normalized_module,
                        "fields_values": {normalized_field: normalized_value},
                    },
                )
            except RuntimeError as exc:
                raise map_tallanto_error(exc) from exc
            context = sanitize_tallanto_lookup_context(fallback)

        response_payload = {
            "module": normalized_module,
            "lookup_field": normalized_field,
            "found": bool(context.get("found")),
            "tags": list(context.get("tags") or []),
            "interests": list(context.get("interests") or []),
            "last_touch_days": context.get("last_touch_days"),
            "fallback_used": fallback_used,
        }
        write_crm_cache(cache_key, response_payload)
        return {"ok": True, "cached": False, **response_payload}

    return router
