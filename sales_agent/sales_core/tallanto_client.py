from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from sales_agent.sales_core.config import Settings, get_settings


@dataclass
class TallantoResult:
    success: bool
    entry_id: Optional[str]
    raw: Dict[str, Any]
    error: Optional[str] = None


class TallantoClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        timeout_seconds: float = 10.0,
        mock_mode: bool = False,
    ) -> None:
        self.base_url = base_url.strip()
        self.api_key = api_key.strip()
        self.timeout_seconds = timeout_seconds
        self.mock_mode = mock_mode

    @classmethod
    def from_settings(cls, settings: Optional[Settings] = None) -> "TallantoClient":
        config = settings or get_settings()
        mock_mode = os.getenv("TALLANTO_MOCK_MODE", "").strip().lower() in {"1", "true", "yes"}
        return cls(
            base_url=config.tallanto_api_url,
            api_key=config.tallanto_api_key,
            mock_mode=mock_mode,
        )

    def is_configured(self) -> bool:
        return bool(self.base_url and self.api_key)

    def _mock_result(self) -> TallantoResult:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        return TallantoResult(
            success=True,
            entry_id=f"mock-{timestamp}",
            raw={"mock_mode": True},
        )

    def _extract_entry_id(self, payload: Dict[str, Any]) -> Optional[str]:
        candidates = (
            payload.get("id"),
            payload.get("entry_id"),
            payload.get("result", {}).get("id") if isinstance(payload.get("result"), dict) else None,
            payload.get("data", {}).get("id") if isinstance(payload.get("data"), dict) else None,
        )
        for candidate in candidates:
            if candidate is not None:
                return str(candidate)
        return None

    def set_entry(
        self,
        module: str,
        fields_values: Dict[str, Any],
        id: Optional[str] = None,
    ) -> TallantoResult:
        if self.mock_mode:
            return self._mock_result()

        if not self.is_configured():
            return TallantoResult(
                success=False,
                entry_id=None,
                raw={},
                error="Tallanto is not configured. Fill TALLANTO_API_URL and TALLANTO_API_KEY.",
            )

        payload: Dict[str, Any] = {
            "method": "set_entry",
            "api_key": self.api_key,
            "module": module,
            "fields_values": fields_values,
        }
        if id:
            payload["id"] = id

        request = Request(
            self.base_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                response_body = response.read().decode("utf-8")
                parsed: Dict[str, Any] = json.loads(response_body) if response_body else {}
        except HTTPError as exc:
            return TallantoResult(
                success=False,
                entry_id=None,
                raw={"status_code": exc.code},
                error=f"Tallanto HTTP error: {exc.code}",
            )
        except URLError as exc:
            return TallantoResult(
                success=False,
                entry_id=None,
                raw={},
                error=f"Tallanto connection error: {exc.reason}",
            )
        except json.JSONDecodeError:
            return TallantoResult(
                success=False,
                entry_id=None,
                raw={},
                error="Tallanto response is not valid JSON.",
            )

        entry_id = self._extract_entry_id(parsed)
        success = bool(entry_id) or bool(parsed.get("success") is True)
        return TallantoResult(
            success=success,
            entry_id=entry_id,
            raw=parsed,
            error=None if success else "Tallanto returned no entry id.",
        )

    def create_lead(
        self,
        phone: str,
        brand: str,
        name: Optional[str] = None,
        source: str = "telegram",
        note: Optional[str] = None,
    ) -> TallantoResult:
        fields = {
            "phone": phone,
            "brand": brand,
            "name": name or "",
            "source": source,
            "note": note or "",
        }
        return self.set_entry(module="leads", fields_values=fields)

    def upsert_contact(
        self,
        phone: str,
        name: Optional[str] = None,
        email: Optional[str] = None,
        contact_id: Optional[str] = None,
    ) -> TallantoResult:
        fields = {
            "phone": phone,
            "name": name or "",
            "email": email or "",
        }
        return self.set_entry(module="contacts", fields_values=fields, id=contact_id)
