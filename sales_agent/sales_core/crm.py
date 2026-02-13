from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol

import httpx

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

    def __init__(self, base_url: str, access_token: str, timeout_seconds: float = 10.0) -> None:
        self.base_url = base_url.strip().rstrip("/")
        self.access_token = access_token.strip()
        self.timeout_seconds = timeout_seconds

    def _is_configured(self) -> bool:
        return bool(self.base_url and self.access_token)

    def _api_url(self, path: str) -> str:
        if self.base_url.endswith("/api/v4"):
            return f"{self.base_url}{path}"
        return f"{self.base_url}/api/v4{path}"

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    @staticmethod
    def _extract_entity_id(payload: Dict[str, Any], entity: str) -> Optional[str]:
        embedded = payload.get("_embedded")
        if isinstance(embedded, dict):
            items = embedded.get(entity)
            if isinstance(items, list) and items:
                head = items[0]
                if isinstance(head, dict) and head.get("id") is not None:
                    return str(head["id"])
        if payload.get("id") is not None:
            return str(payload["id"])
        return None

    @staticmethod
    def _safe_error_message(exc: Exception) -> str:
        if isinstance(exc, httpx.HTTPStatusError):
            status = exc.response.status_code
            body = exc.response.text.strip()
            if body:
                body = body[:300]
                return f"AMO CRM HTTP error: {status} ({body})"
            return f"AMO CRM HTTP error: {status}"
        if isinstance(exc, httpx.RequestError):
            return f"AMO CRM connection error: {exc}"
        return f"AMO CRM error: {exc}"

    def _build_lead_name(self, brand: str, source: str, name: Optional[str]) -> str:
        human_name = (name or "").strip()
        if human_name:
            return f"{human_name} ({brand.upper()}, {source})"
        return f"Lead {brand.upper()} ({source})"

    def _build_lead_payload(
        self,
        brand: str,
        source: str,
        name: Optional[str],
    ) -> list[Dict[str, Any]]:
        return [
            {
                "name": self._build_lead_name(brand=brand, source=source, name=name),
                "_embedded": {
                    "tags": [
                        {"name": f"brand:{brand}"},
                        {"name": f"source:{source}"},
                        {"name": "sales-agent"},
                    ]
                },
            }
        ]

    @staticmethod
    def _build_note_payload(text: str) -> list[Dict[str, Any]]:
        return [{"note_type": "common", "params": {"text": text.strip()}}]

    async def _post_json_async(self, path: str, payload: Any) -> Dict[str, Any]:
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.post(
                self._api_url(path),
                headers=self._headers(),
                json=payload,
            )
        response.raise_for_status()
        data = response.json() if response.text else {}
        return data if isinstance(data, dict) else {}

    def _post_json(self, path: str, payload: Any) -> Dict[str, Any]:
        with httpx.Client(timeout=self.timeout_seconds) as client:
            response = client.post(
                self._api_url(path),
                headers=self._headers(),
                json=payload,
            )
        response.raise_for_status()
        data = response.json() if response.text else {}
        return data if isinstance(data, dict) else {}

    async def create_lead_async(
        self,
        phone: str,
        brand: str,
        name: Optional[str] = None,
        source: str = "telegram",
        note: Optional[str] = None,
    ) -> CRMResult:
        if not self._is_configured():
            return CRMResult(
                success=False,
                entry_id=None,
                raw={},
                error="AMO CRM is not configured. Fill AMO_API_URL and AMO_ACCESS_TOKEN.",
            )

        note_text_parts = [f"Телефон: {phone}"]
        if note and note.strip():
            note_text_parts.append(note.strip())
        note_payload = self._build_note_payload("\n".join(note_text_parts))

        lead_response: Dict[str, Any] = {}
        note_response: Dict[str, Any] = {}
        try:
            lead_response = await self._post_json_async(
                "/leads",
                self._build_lead_payload(brand=brand, source=source, name=name),
            )
            lead_id = self._extract_entity_id(lead_response, "leads")
            if not lead_id:
                return CRMResult(
                    success=False,
                    entry_id=None,
                    raw={"lead_response": lead_response},
                    error="AMO CRM returned no lead id.",
                )

            note_response = await self._post_json_async(
                f"/leads/{lead_id}/notes",
                note_payload,
            )
            return CRMResult(
                success=True,
                entry_id=lead_id,
                raw={"lead_response": lead_response, "note_response": note_response},
                error=None,
            )
        except Exception as exc:
            return CRMResult(
                success=False,
                entry_id=self._extract_entity_id(lead_response, "leads"),
                raw={"lead_response": lead_response, "note_response": note_response},
                error=self._safe_error_message(exc),
            )

    def create_copilot_task(
        self,
        summary: str,
        draft_reply: str,
        contact: Optional[str] = None,
    ) -> CRMResult:
        if not self._is_configured():
            return CRMResult(
                success=False,
                entry_id=None,
                raw={},
                error="AMO CRM is not configured. Fill AMO_API_URL and AMO_ACCESS_TOKEN.",
            )

        contact_line = f"\nКонтакт: {contact.strip()}" if contact and contact.strip() else ""
        note_payload = self._build_note_payload(
            "Copilot draft:\n"
            f"{draft_reply.strip()}\n\n"
            "Summary:\n"
            f"{summary.strip()}"
            f"{contact_line}"
        )

        lead_response: Dict[str, Any] = {}
        note_response: Dict[str, Any] = {}
        try:
            lead_response = self._post_json(
                "/leads",
                self._build_lead_payload(
                    brand="copilot",
                    source="copilot",
                    name="Copilot: реактивация диалога",
                ),
            )
            lead_id = self._extract_entity_id(lead_response, "leads")
            if not lead_id:
                return CRMResult(
                    success=False,
                    entry_id=None,
                    raw={"lead_response": lead_response},
                    error="AMO CRM returned no lead id.",
                )

            note_response = self._post_json(f"/leads/{lead_id}/notes", note_payload)
            return CRMResult(
                success=True,
                entry_id=lead_id,
                raw={"lead_response": lead_response, "note_response": note_response},
                error=None,
            )
        except Exception as exc:
            return CRMResult(
                success=False,
                entry_id=self._extract_entity_id(lead_response, "leads"),
                raw={"lead_response": lead_response, "note_response": note_response},
                error=self._safe_error_message(exc),
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
