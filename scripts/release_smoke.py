#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Release smoke checks for running sales-agent API.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="API base URL")
    parser.add_argument("--timeout", type=float, default=5.0, help="HTTP timeout in seconds")
    parser.add_argument(
        "--require-miniapp-ready",
        action="store_true",
        help="Fail if user miniapp status is not ready.",
    )
    parser.add_argument(
        "--strict-runtime",
        action="store_true",
        help="Fail on runtime diagnostics status=warn.",
    )
    parser.add_argument(
        "--require-render-persistent",
        action="store_true",
        help="Fail if runtime is not on Render persistent storage (/tmp fallback is treated as failure).",
    )
    parser.add_argument(
        "--require-webhook-mode",
        action="store_true",
        help="Fail if runtime telegram mode is not webhook.",
    )
    parser.add_argument(
        "--check-telegram-webhook",
        action="store_true",
        help="Call Telegram getWebhookInfo and validate webhook state for configured bot token.",
    )
    parser.add_argument(
        "--telegram-token-env",
        default="TELEGRAM_BOT_TOKEN",
        help="Env var name that stores Telegram bot token (used with --check-telegram-webhook).",
    )
    return parser


def _fetch_json(base_url: str, path: str, timeout: float) -> dict:
    url = urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
    req = Request(url, headers={"Accept": "application/json"})
    with urlopen(req, timeout=timeout) as response:
        raw = response.read().decode("utf-8")
    return json.loads(raw)


def _fetch_status(base_url: str, path: str, timeout: float) -> int:
    url = urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
    req = Request(url)
    with urlopen(req, timeout=timeout) as response:
        return int(response.status)


def _fetch_telegram_webhook_info(bot_token: str, timeout: float) -> dict:
    url = f"https://api.telegram.org/bot{bot_token}/getWebhookInfo"
    req = Request(url, headers={"Accept": "application/json"})
    with urlopen(req, timeout=timeout) as response:
        raw = response.read().decode("utf-8")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("Unexpected Telegram response format.")
    return payload


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    checks: list[tuple[str, bool, str]] = []

    try:
        health = _fetch_json(args.base_url, "/api/health", args.timeout)
        ok = health.get("status") == "ok" and health.get("service") == "sales-agent"
        checks.append(("health", ok, f"status={health.get('status')} service={health.get('service')}"))
    except Exception as exc:
        checks.append(("health", False, f"error: {exc}"))

    diagnostics: dict = {}
    try:
        diagnostics = _fetch_json(args.base_url, "/api/runtime/diagnostics", args.timeout)
        runtime_status = str(diagnostics.get("status") or "fail").lower()
        runtime_ok = runtime_status == "ok" or (runtime_status == "warn" and not args.strict_runtime)
        checks.append(("runtime_diagnostics", runtime_ok, f"status={runtime_status}"))
    except Exception as exc:
        checks.append(("runtime_diagnostics", False, f"error: {exc}"))

    try:
        meta = _fetch_json(args.base_url, "/api/miniapp/meta", args.timeout)
        meta_ok = bool(meta.get("ok")) and bool(str(meta.get("advisor_name") or "").strip())
        checks.append(("miniapp_meta", meta_ok, f"advisor={meta.get('advisor_name')}"))
    except Exception as exc:
        checks.append(("miniapp_meta", False, f"error: {exc}"))

    root_payload: dict = {}
    try:
        root_payload = _fetch_json(args.base_url, "/", args.timeout)
        miniapp_status = ((root_payload.get("user_miniapp") or {}).get("status") or "").strip()
        root_ok = root_payload.get("status") == "ok" and miniapp_status in {"ready", "build-required"}
        if args.require_miniapp_ready:
            root_ok = root_ok and miniapp_status == "ready"
        checks.append(("root_status", root_ok, f"miniapp={miniapp_status or 'unknown'}"))
    except Exception as exc:
        checks.append(("root_status", False, f"error: {exc}"))

    try:
        app_status_code = _fetch_status(args.base_url, "/app", args.timeout)
        checks.append(("app_endpoint", app_status_code == 200, f"http={app_status_code}"))
    except URLError as exc:
        checks.append(("app_endpoint", False, f"error: {exc.reason}"))
    except Exception as exc:
        checks.append(("app_endpoint", False, f"error: {exc}"))

    # If webhook mode is enabled, webhook secret is optional but recommended.
    runtime = diagnostics.get("runtime") if isinstance(diagnostics, dict) else {}
    if isinstance(runtime, dict) and runtime.get("telegram_mode") == "webhook":
        secret_set = bool(runtime.get("telegram_webhook_secret_set"))
        checks.append(("webhook_secret", True, f"secret_set={secret_set} (recommended=true)"))

    if args.require_webhook_mode:
        telegram_mode = ""
        if isinstance(runtime, dict):
            telegram_mode = str(runtime.get("telegram_mode") or "").strip().lower()
        checks.append(
            (
                "require_webhook_mode",
                telegram_mode == "webhook",
                f"telegram_mode={telegram_mode or 'unknown'}",
            )
        )

    if args.require_render_persistent:
        running_on_render = bool(runtime.get("running_on_render")) if isinstance(runtime, dict) else False
        persistent_root = str(runtime.get("persistent_data_root") or "").strip() if isinstance(runtime, dict) else ""
        db_on_persistent = bool(runtime.get("database_on_persistent_storage")) if isinstance(runtime, dict) else False
        vector_on_persistent = bool(runtime.get("vector_meta_on_persistent_storage")) if isinstance(runtime, dict) else False
        persistent_ok = (
            running_on_render
            and persistent_root not in {"", "/tmp"}
            and db_on_persistent
            and vector_on_persistent
        )
        checks.append(
            (
                "render_persistent_storage",
                persistent_ok,
                (
                    f"running_on_render={running_on_render} "
                    f"persistent_data_root={persistent_root or 'missing'} "
                    f"database_on_persistent_storage={db_on_persistent} "
                    f"vector_meta_on_persistent_storage={vector_on_persistent}"
                ),
            )
        )

    if args.check_telegram_webhook:
        token = os.getenv(args.telegram_token_env, "").strip()
        if not token:
            checks.append(
                (
                    "telegram_webhook_info",
                    False,
                    f"{args.telegram_token_env} is empty",
                )
            )
        else:
            try:
                webhook_info_payload = _fetch_telegram_webhook_info(token, args.timeout)
                ok = bool(webhook_info_payload.get("ok"))
                result = webhook_info_payload.get("result") if isinstance(webhook_info_payload, dict) else {}
                result = result if isinstance(result, dict) else {}
                url = str(result.get("url") or "").strip()
                pending_count = int(result.get("pending_update_count") or 0)
                last_error_message = str(result.get("last_error_message") or "").strip()
                checks.append(
                    (
                        "telegram_webhook_info",
                        ok and bool(url),
                        f"url_set={bool(url)} pending_update_count={pending_count}",
                    )
                )
                checks.append(
                    (
                        "telegram_webhook_last_error",
                        not bool(last_error_message),
                        f"last_error_message={last_error_message or 'none'}",
                    )
                )
                if args.require_webhook_mode and isinstance(runtime, dict):
                    runtime_path = str(runtime.get("telegram_webhook_path") or "").strip()
                    if runtime_path:
                        expected_url = f"{args.base_url.rstrip('/')}{runtime_path}"
                        checks.append(
                            (
                                "telegram_webhook_expected_url",
                                url == expected_url,
                                f"configured={url or 'missing'} expected={expected_url}",
                            )
                        )
            except Exception as exc:
                checks.append(("telegram_webhook_info", False, f"error: {exc}"))

    failed = [item for item in checks if not item[1]]
    for name, ok, details in checks:
        mark = "OK" if ok else "FAIL"
        print(f"[{mark}] {name}: {details}")

    if failed:
        print(f"Smoke result: FAIL ({len(failed)} checks failed)")
        return 1

    print("Smoke result: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
