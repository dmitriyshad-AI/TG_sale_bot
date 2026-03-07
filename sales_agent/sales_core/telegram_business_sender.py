from __future__ import annotations

import json
from typing import Any, Dict, Optional
from urllib import error, parse, request


class TelegramBusinessSendError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        response_body: str = "",
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


def send_business_message(
    *,
    bot_token: str,
    business_connection_id: str,
    chat_id: int,
    text: str,
    timeout_seconds: float = 20.0,
) -> Dict[str, Any]:
    token = (bot_token or "").strip()
    if not token:
        raise ValueError("bot_token is required")
    connection_id = (business_connection_id or "").strip()
    if not connection_id:
        raise ValueError("business_connection_id is required")
    message_text = (text or "").strip()
    if not message_text:
        raise ValueError("text is required")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = parse.urlencode(
        {
            "business_connection_id": connection_id,
            "chat_id": int(chat_id),
            "text": message_text,
        }
    ).encode("utf-8")

    req = request.Request(
        url=url,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    status_code: Optional[int] = None
    raw_body = ""
    try:
        with request.urlopen(req, timeout=float(timeout_seconds)) as resp:
            status_code = int(resp.getcode())
            raw_body = resp.read().decode("utf-8", errors="replace")
    except error.HTTPError as exc:
        response_body = exc.read().decode("utf-8", errors="replace")
        raise TelegramBusinessSendError(
            f"Telegram HTTP error: {exc.code}",
            status_code=int(exc.code),
            response_body=response_body,
        ) from exc
    except error.URLError as exc:
        raise TelegramBusinessSendError(f"Telegram connection error: {exc.reason}") from exc

    try:
        parsed = json.loads(raw_body or "{}")
    except json.JSONDecodeError as exc:
        raise TelegramBusinessSendError(
            "Telegram API returned invalid JSON.",
            status_code=status_code,
            response_body=raw_body,
        ) from exc

    if not isinstance(parsed, dict):
        raise TelegramBusinessSendError(
            "Telegram API returned unexpected response type.",
            status_code=status_code,
            response_body=raw_body,
        )

    if not bool(parsed.get("ok")):
        description = str(parsed.get("description") or "Telegram API error")
        raise TelegramBusinessSendError(
            description,
            status_code=status_code,
            response_body=raw_body,
        )

    result = parsed.get("result")
    if not isinstance(result, dict):
        raise TelegramBusinessSendError(
            "Telegram API response missing result object.",
            status_code=status_code,
            response_body=raw_body,
        )
    return result
