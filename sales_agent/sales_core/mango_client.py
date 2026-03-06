from __future__ import annotations

from dataclasses import dataclass
import hashlib
import hmac
import json
from typing import Any, Dict, Iterable, Optional
from urllib import error, parse, request


class MangoClientError(RuntimeError):
    """Raised when Mango API interaction fails."""


@dataclass(frozen=True)
class MangoCallEvent:
    event_id: str
    call_id: str
    phone: str
    recording_url: str
    transcript_hint: str
    occurred_at: str
    payload: Dict[str, Any]


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _json_bytes(payload: Dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _extract_first(payload: Dict[str, Any], keys: Iterable[str]) -> str:
    for key in keys:
        value = payload.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _unwrap_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data")
    if isinstance(data, dict):
        return data
    call = payload.get("call")
    if isinstance(call, dict):
        return call
    return payload


class MangoClient:
    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        calls_path: str = "/calls",
        timeout_seconds: float = 10.0,
        webhook_secret: str = "",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token.strip()
        self.calls_path = calls_path if calls_path.startswith("/") else f"/{calls_path}"
        self.timeout_seconds = max(1.0, float(timeout_seconds))
        self.webhook_secret = webhook_secret.strip()

    def _build_request(
        self,
        *,
        path: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
    ) -> request.Request:
        if not self.base_url or not self.token:
            raise MangoClientError("Mango API is not configured.")
        normalized_path = path if path.startswith("/") else f"/{path}"
        url = f"{self.base_url}{normalized_path}"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        data = None
        if method.upper() == "GET":
            if params:
                query = parse.urlencode(
                    {k: v for k, v in params.items() if v is not None and str(v).strip() != ""},
                    doseq=True,
                )
                if query:
                    url = f"{url}?{query}"
        else:
            headers["Content-Type"] = "application/json"
            data = _json_bytes(params or {})
        return request.Request(url=url, method=method.upper(), headers=headers, data=data)

    def _request_json(
        self,
        *,
        path: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        req = self._build_request(path=path, method=method, params=params)
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                status_code = int(getattr(response, "status", 200))
                body = response.read().decode("utf-8")
        except error.HTTPError as exc:  # pragma: no cover - covered by unit mocks
            detail = exc.read().decode("utf-8", errors="replace")
            raise MangoClientError(f"Mango HTTP error: {exc.code} {detail}".strip()) from exc
        except error.URLError as exc:  # pragma: no cover - covered by unit mocks
            raise MangoClientError(f"Mango connection error: {exc.reason}") from exc

        if status_code < 200 or status_code >= 300:
            raise MangoClientError(f"Mango unexpected status: {status_code}")

        try:
            payload = json.loads(body) if body else {}
        except json.JSONDecodeError as exc:
            raise MangoClientError("Mango invalid JSON response.") from exc
        if not isinstance(payload, dict):
            raise MangoClientError("Mango response must be a JSON object.")
        return payload

    def verify_webhook_signature(self, *, raw_body: bytes, signature: str) -> bool:
        if not self.webhook_secret:
            return True
        provided = _safe_str(signature)
        if not provided:
            return False
        digest = hmac.new(
            key=self.webhook_secret.encode("utf-8"),
            msg=raw_body,
            digestmod=hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(digest, provided)

    def parse_call_event(self, payload: Dict[str, Any]) -> Optional[MangoCallEvent]:
        if not isinstance(payload, dict):
            return None
        event_type = _safe_str(payload.get("event") or payload.get("type")).lower()
        if event_type and not any(
            token in event_type
            for token in ("record", "call", "conversation", "voip")
        ):
            return None

        content = _unwrap_payload(payload)
        call_id = _extract_first(content, ("call_id", "callId", "id", "record_id", "recordId"))
        recording_url = _extract_first(
            content,
            (
                "recording_url",
                "recordingUrl",
                "record_url",
                "recordUrl",
                "record",
                "record_link",
                "recordLink",
                "audio_url",
            ),
        )
        phone = _extract_first(
            content,
            ("phone", "phone_number", "from", "from_number", "caller", "client_phone"),
        )
        transcript_hint = _extract_first(content, ("transcript", "summary", "note", "comment"))
        occurred_at = _extract_first(
            content,
            ("occurred_at", "occurredAt", "created_at", "createdAt", "timestamp"),
        )
        event_id = _extract_first(payload, ("event_id", "eventId", "id"))
        if not event_id:
            base = call_id or recording_url or json.dumps(content, ensure_ascii=False, sort_keys=True)
            event_id = hashlib.sha1(base.encode("utf-8")).hexdigest()  # nosec B324

        if not call_id and not recording_url:
            return None
        return MangoCallEvent(
            event_id=event_id,
            call_id=call_id or event_id,
            phone=phone,
            recording_url=recording_url,
            transcript_hint=transcript_hint,
            occurred_at=occurred_at,
            payload=payload,
        )

    def list_recent_calls(self, *, since_iso: str = "", limit: int = 50) -> list[MangoCallEvent]:
        payload = self._request_json(
            path=self.calls_path,
            method="GET",
            params={
                "since": since_iso or None,
                "limit": max(1, min(int(limit), 500)),
            },
        )
        items = payload.get("items")
        if not isinstance(items, list):
            return []
        events: list[MangoCallEvent] = []
        for item in items:
            event = self.parse_call_event(item if isinstance(item, dict) else {})
            if event is not None:
                events.append(event)
        return events
