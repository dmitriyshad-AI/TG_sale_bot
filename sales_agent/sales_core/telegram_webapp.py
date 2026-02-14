from __future__ import annotations

import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib.parse import parse_qsl


@dataclass(frozen=True)
class WebAppAuthResult:
    ok: bool
    reason: Optional[str]
    payload: Dict[str, str]
    user: Optional[Dict[str, Any]]
    user_id: Optional[int]


def parse_init_data(init_data: str) -> Dict[str, str]:
    pairs = parse_qsl(init_data, keep_blank_values=True)
    parsed: Dict[str, str] = {}
    for key, value in pairs:
        parsed[key] = value
    return parsed


def _build_data_check_string(payload: Dict[str, str]) -> str:
    lines = [f"{key}={value}" for key, value in sorted(payload.items()) if key != "hash"]
    return "\n".join(lines)


def verify_telegram_webapp_init_data(
    *,
    init_data: str,
    bot_token: str,
    max_age_seconds: int = 86_400,
    now_ts: Optional[int] = None,
) -> WebAppAuthResult:
    raw = (init_data or "").strip()
    if not raw:
        return WebAppAuthResult(ok=False, reason="missing_init_data", payload={}, user=None, user_id=None)

    if not bot_token.strip():
        return WebAppAuthResult(ok=False, reason="missing_bot_token", payload={}, user=None, user_id=None)

    payload = parse_init_data(raw)
    their_hash = payload.get("hash", "")
    if not their_hash:
        return WebAppAuthResult(ok=False, reason="missing_hash", payload=payload, user=None, user_id=None)

    auth_date_raw = payload.get("auth_date")
    if not auth_date_raw:
        return WebAppAuthResult(ok=False, reason="missing_auth_date", payload=payload, user=None, user_id=None)

    try:
        auth_date = int(auth_date_raw)
    except ValueError:
        return WebAppAuthResult(ok=False, reason="invalid_auth_date", payload=payload, user=None, user_id=None)

    now_value = int(now_ts if now_ts is not None else time.time())
    if auth_date > now_value + 60:
        return WebAppAuthResult(ok=False, reason="future_auth_date", payload=payload, user=None, user_id=None)
    if max_age_seconds > 0 and now_value - auth_date > max_age_seconds:
        return WebAppAuthResult(ok=False, reason="expired_auth_date", payload=payload, user=None, user_id=None)

    data_check_string = _build_data_check_string(payload)
    secret_key = hmac.new(b"WebAppData", bot_token.encode("utf-8"), hashlib.sha256).digest()
    expected_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected_hash, their_hash):
        return WebAppAuthResult(ok=False, reason="invalid_hash", payload=payload, user=None, user_id=None)

    user_raw = payload.get("user")
    user: Optional[Dict[str, Any]] = None
    user_id: Optional[int] = None
    if user_raw:
        try:
            parsed_user = json.loads(user_raw)
            if isinstance(parsed_user, dict):
                user = parsed_user
                if "id" in parsed_user:
                    user_id = int(parsed_user["id"])
        except (ValueError, TypeError):
            user = None
            user_id = None

    return WebAppAuthResult(ok=True, reason=None, payload=payload, user=user, user_id=user_id)
