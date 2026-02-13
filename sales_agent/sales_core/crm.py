from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol

from sales_agent.sales_core.config import Settings, get_settings
from sales_agent.sales_core.tallanto_client import TallantoClient


@dataclass
class CRMResult:
    success: bool
    entry_id: Optional[str]
    raw: Dict[str, Any]
    error: Optional[str] = None


class CRMClient(Protocol):
    provider: str

    async def create_lead_async(
        self,
        phone: str,
        brand: str,
        name: Optional[str] = None,
        source: str = "telegram",
        note: Optional[str] = None,
    ) -> CRMResult:
        ...

    def create_copilot_task(
        self,
        summary: str,
        draft_reply: str,
        contact: Optional[str] = None,
    ) -> CRMResult:
        ...


class TallantoCRMClient:
    provider = "tallanto"

    def __init__(self, client: TallantoClient) -> None:
        self._client = client

    async def create_lead_async(
        self,
        phone: str,
        brand: str,
        name: Optional[str] = None,
        source: str = "telegram",
        note: Optional[str] = None,
    ) -> CRMResult:
        result = await self._client.create_lead_async(
            phone=phone,
            brand=brand,
            name=name,
            source=source,
            note=note,
        )
        return CRMResult(
            success=result.success,
            entry_id=result.entry_id,
            raw=result.raw,
            error=result.error,
        )

    def create_copilot_task(
        self,
        summary: str,
        draft_reply: str,
        contact: Optional[str] = None,
    ) -> CRMResult:
        payload = {
            "title": "Copilot: реактивация диалога",
            "summary": summary,
            "draft_reply": draft_reply,
            "contact": contact or "",
        }
        result = self._client.set_entry(module="tasks", fields_values=payload)
        return CRMResult(
            success=result.success,
            entry_id=result.entry_id,
            raw=result.raw,
            error=result.error,
        )


class AmoCRMClient:
    provider = "amo"

    def __init__(self, base_url: str, access_token: str) -> None:
        self.base_url = base_url.strip()
        self.access_token = access_token.strip()

    async def create_lead_async(
        self,
        phone: str,
        brand: str,
        name: Optional[str] = None,
        source: str = "telegram",
        note: Optional[str] = None,
    ) -> CRMResult:
        if not self.base_url or not self.access_token:
            return CRMResult(
                success=False,
                entry_id=None,
                raw={},
                error="AMO CRM is not configured. Fill AMO_API_URL and AMO_ACCESS_TOKEN.",
            )
        return CRMResult(
            success=False,
            entry_id=None,
            raw={},
            error="AMO CRM adapter is not implemented yet.",
        )

    def create_copilot_task(
        self,
        summary: str,
        draft_reply: str,
        contact: Optional[str] = None,
    ) -> CRMResult:
        if not self.base_url or not self.access_token:
            return CRMResult(
                success=False,
                entry_id=None,
                raw={},
                error="AMO CRM is not configured. Fill AMO_API_URL and AMO_ACCESS_TOKEN.",
            )
        return CRMResult(
            success=False,
            entry_id=None,
            raw={},
            error="AMO CRM adapter is not implemented yet.",
        )


class NoopCRMClient:
    provider = "none"

    def __init__(self, reason: Optional[str] = None) -> None:
        self.reason = reason or "CRM integration is disabled (CRM_PROVIDER=none)."

    async def create_lead_async(
        self,
        phone: str,
        brand: str,
        name: Optional[str] = None,
        source: str = "telegram",
        note: Optional[str] = None,
    ) -> CRMResult:
        return CRMResult(success=False, entry_id=None, raw={}, error=self.reason)

    def create_copilot_task(
        self,
        summary: str,
        draft_reply: str,
        contact: Optional[str] = None,
    ) -> CRMResult:
        return CRMResult(success=False, entry_id=None, raw={}, error=self.reason)


def build_crm_client(settings: Optional[Settings] = None) -> CRMClient:
    cfg = settings or get_settings()
    provider = (cfg.crm_provider or "tallanto").strip().lower()
    if provider == "tallanto":
        return TallantoCRMClient(TallantoClient.from_settings(cfg))
    if provider == "amo":
        return AmoCRMClient(base_url=cfg.amo_api_url, access_token=cfg.amo_access_token)
    if provider == "none":
        return NoopCRMClient()
    return NoopCRMClient(reason=f"Unsupported CRM_PROVIDER: {provider}")
