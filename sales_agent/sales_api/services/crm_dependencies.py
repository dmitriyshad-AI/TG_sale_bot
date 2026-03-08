from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from fastapi import HTTPException, status

from sales_agent.sales_api.services.crm_helpers import (
    build_thread_crm_context,
    crm_cache_key,
    map_tallanto_error,
    read_crm_cache,
    write_crm_cache,
)
from sales_agent.sales_core.tallanto_readonly import TallantoReadOnlyClient


@dataclass
class CrmDependencyService:
    settings: Any
    database_path: Any
    cache_ttl_seconds: int
    get_connection: Callable[[Any], Any]
    get_crm_cache: Callable[..., Optional[Dict[str, Any]]]
    upsert_crm_cache: Callable[..., None]
    client_cls: Callable[..., Any] = TallantoReadOnlyClient
    crm_cache_key_fn: Callable[[str, Dict[str, Any]], str] = crm_cache_key
    read_crm_cache_fn: Callable[..., Optional[Dict[str, Any]]] = read_crm_cache
    write_crm_cache_fn: Callable[..., None] = write_crm_cache
    map_tallanto_error_fn: Callable[[RuntimeError], HTTPException] = map_tallanto_error
    build_thread_crm_context_fn: Callable[..., Dict[str, Any]] = build_thread_crm_context

    def require_tallanto_readonly_client(self) -> Any:
        if not self.settings.tallanto_read_only:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Tallanto read-only mode is disabled. Set TALLANTO_READ_ONLY=1.",
            )
        token = self.settings.tallanto_api_token or self.settings.tallanto_api_key
        if not self.settings.tallanto_api_url or not token:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Tallanto read-only config is incomplete. Fill TALLANTO_API_URL and TALLANTO_API_TOKEN.",
            )
        return self.client_cls(base_url=self.settings.tallanto_api_url, token=token)

    def crm_cache_key(self, prefix: str, params: Dict[str, Any]) -> str:
        return self.crm_cache_key_fn(prefix, params)

    def read_crm_cache(self, key: str) -> Optional[Dict[str, Any]]:
        return self.read_crm_cache_fn(
            database_path=self.database_path,
            key=key,
            max_age_seconds=self.cache_ttl_seconds,
            get_connection=self.get_connection,
            get_crm_cache=self.get_crm_cache,
        )

    def write_crm_cache(self, key: str, payload: Dict[str, Any]) -> None:
        self.write_crm_cache_fn(
            database_path=self.database_path,
            key=key,
            payload=payload,
            get_connection=self.get_connection,
            upsert_crm_cache=self.upsert_crm_cache,
        )

    def map_tallanto_error(self, exc: RuntimeError) -> HTTPException:
        return self.map_tallanto_error_fn(exc)

    def build_thread_crm_context(self, user_item: Dict[str, Any]) -> Dict[str, Any]:
        return self.build_thread_crm_context_fn(
            user_item,
            settings=self.settings,
            read_cache=self.read_crm_cache,
            write_cache=self.write_crm_cache,
        )
