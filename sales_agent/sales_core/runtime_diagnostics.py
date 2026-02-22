from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
from uuid import uuid4
import re

from sales_agent.sales_core.catalog import CatalogValidationError, load_catalog
from sales_agent.sales_core.config import Settings
from sales_agent.sales_core.vector_store import load_vector_store_id

try:
    from telegram import __version__ as TELEGRAM_LIBRARY_VERSION
except Exception:  # pragma: no cover - import failure depends on runtime env
    TELEGRAM_LIBRARY_VERSION = ""

PTB_BUSINESS_MIN_MAJOR = 21
PTB_BUSINESS_MIN_MINOR = 1


@dataclass(frozen=True)
class DiagnosticIssue:
    severity: str
    code: str
    message: str


def normalize_preflight_mode(value: object) -> str:
    if not isinstance(value, str):
        return "off"
    normalized = value.strip().lower()
    if normalized in {"off", "fail", "strict"}:
        return normalized
    return "off"


def _summarize_issues(issues: List[dict], limit: int = 3) -> str:
    if not issues:
        return "no issues reported"
    parts: List[str] = []
    for item in issues[: max(1, limit)]:
        code = str(item.get("code") or "unknown")
        message = str(item.get("message") or "").strip()
        if message:
            parts.append(f"{code}: {message}")
        else:
            parts.append(code)
    suffix = "" if len(issues) <= limit else f" (+{len(issues) - limit} more)"
    return "; ".join(parts) + suffix


def enforce_startup_preflight(settings: Settings, mode: str | None = None) -> Dict[str, object]:
    configured_mode = getattr(settings, "startup_preflight_mode", "off")
    preflight_mode = normalize_preflight_mode(mode if mode is not None else configured_mode)

    if preflight_mode == "off":
        return {"status": "off", "runtime": {}, "issues": []}

    diagnostics = build_runtime_diagnostics(settings)
    status = str(diagnostics.get("status") or "fail").lower()

    issues = diagnostics.get("issues")
    issue_items = issues if isinstance(issues, list) else []
    summary = _summarize_issues(issue_items)

    if status == "fail":
        raise RuntimeError(f"Startup preflight failed ({preflight_mode}): {summary}")

    if status == "warn" and preflight_mode == "strict":
        raise RuntimeError(f"Startup preflight blocked by warnings (strict): {summary}")

    return diagnostics


def _safe_md_count(path: Path) -> int:
    if not path.exists() or not path.is_dir():
        return 0
    return len([item for item in path.rglob("*") if item.is_file() and item.suffix.lower() in {".md", ".txt", ".pdf"}])


def _can_write_parent(path: Path) -> bool:
    parent = path.parent
    if not parent.exists() or not parent.is_dir():
        return False
    probe = parent / f".write_probe_{uuid4().hex}"
    try:
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
        return True
    except Exception:
        return False


def _is_path_within(child: Path, parent: Path) -> bool:
    try:
        child_resolved = child.resolve()
        parent_resolved = parent.resolve()
    except Exception:
        return False
    try:
        return child_resolved.is_relative_to(parent_resolved)
    except AttributeError:
        try:
            child_resolved.relative_to(parent_resolved)
            return True
        except ValueError:
            return False


def _parse_major_minor(version: str) -> tuple[int, int]:
    if not isinstance(version, str):
        return (0, 0)
    parts = version.strip().split(".")
    if len(parts) < 2:
        return (0, 0)

    def _num(value: str) -> int:
        match = re.match(r"(\d+)", value.strip())
        if not match:
            return 0
        return int(match.group(1))

    return (_num(parts[0]), _num(parts[1]))


def _ptb_business_ready(version: str) -> bool:
    major, minor = _parse_major_minor(version)
    if major > PTB_BUSINESS_MIN_MAJOR:
        return True
    if major == PTB_BUSINESS_MIN_MAJOR and minor >= PTB_BUSINESS_MIN_MINOR:
        return True
    return False


def build_runtime_diagnostics(settings: Settings) -> Dict[str, object]:
    issues: List[DiagnosticIssue] = []
    database_parent_writable = _can_write_parent(settings.database_path)
    ptb_version = TELEGRAM_LIBRARY_VERSION.strip() if isinstance(TELEGRAM_LIBRARY_VERSION, str) else ""
    ptb_business_ready = _ptb_business_ready(ptb_version)

    if not settings.telegram_bot_token:
        issues.append(
            DiagnosticIssue(
                severity="error",
                code="telegram_token_missing",
                message="TELEGRAM_BOT_TOKEN is empty.",
            )
        )
    if not settings.openai_api_key:
        issues.append(
            DiagnosticIssue(
                severity="error",
                code="openai_key_missing",
                message="OPENAI_API_KEY is empty.",
            )
        )

    if not ptb_business_ready:
        issues.append(
            DiagnosticIssue(
                severity="warning",
                code="ptb_business_features_unavailable",
                message=(
                    "python-telegram-bot version does not support Telegram Business API features. "
                    f"Upgrade to >= {PTB_BUSINESS_MIN_MAJOR}.{PTB_BUSINESS_MIN_MINOR} "
                    f"(detected: {ptb_version or 'unknown'})."
                ),
            )
        )

    if settings.telegram_mode == "webhook" and not settings.telegram_webhook_secret:
        issues.append(
            DiagnosticIssue(
                severity="warning",
                code="webhook_secret_missing",
                message="TELEGRAM_MODE=webhook without TELEGRAM_WEBHOOK_SECRET.",
            )
        )

    tallanto_token_present = bool((settings.tallanto_api_token or settings.tallanto_api_key).strip())
    if settings.tallanto_read_only and (not settings.tallanto_api_url or not tallanto_token_present):
        issues.append(
            DiagnosticIssue(
                severity="warning",
                code="tallanto_readonly_incomplete",
                message="TALLANTO_READ_ONLY=1 but Tallanto URL/token is not fully configured.",
            )
        )

    if not database_parent_writable:
        issues.append(
            DiagnosticIssue(
                severity="error",
                code="database_parent_not_writable",
                message=f"Database parent is not writable: {settings.database_path.parent}",
            )
        )

    database_on_persistent_storage = False
    vector_meta_on_persistent_storage = False
    if settings.running_on_render:
        persistent_root = settings.persistent_data_root
        if persistent_root == Path():
            issues.append(
                DiagnosticIssue(
                    severity="warning",
                    code="persistent_data_root_missing",
                    message=(
                        "Render environment detected but persistent data root is not configured. "
                        "Set PERSISTENT_DATA_PATH (for example /var/data)."
                    ),
                )
            )
        elif persistent_root == Path("/tmp"):
            issues.append(
                DiagnosticIssue(
                    severity="warning",
                    code="render_ephemeral_storage_fallback",
                    message=(
                        "Render persistent storage is not configured, using /tmp fallback. "
                        "Database and vector metadata will reset after redeploy/restart."
                    ),
                )
            )
        else:
            database_on_persistent_storage = _is_path_within(settings.database_path, persistent_root)
            vector_meta_on_persistent_storage = _is_path_within(settings.vector_store_meta_path, persistent_root)
            if not database_on_persistent_storage:
                issues.append(
                    DiagnosticIssue(
                        severity="warning",
                        code="render_database_not_persistent",
                        message=(
                            "DATABASE_PATH is outside persistent storage. "
                            "Sessions/leads/context may be lost after redeploy."
                        ),
                    )
                )
            if not vector_meta_on_persistent_storage:
                issues.append(
                    DiagnosticIssue(
                        severity="warning",
                        code="render_vector_meta_not_persistent",
                        message=(
                            "VECTOR_STORE_META_PATH is outside persistent storage. "
                            "Vector store metadata may be lost after redeploy."
                        ),
                    )
                )

    catalog_ok = True
    catalog_products_count = 0
    catalog_error: Optional[str] = None
    try:
        catalog = load_catalog(settings.catalog_path)
        catalog_products_count = len(catalog.products)
    except (FileNotFoundError, CatalogValidationError, OSError) as exc:
        catalog_ok = False
        catalog_error = str(exc)
        issues.append(
            DiagnosticIssue(
                severity="error",
                code="catalog_invalid",
                message=f"Catalog is unavailable or invalid: {exc}",
            )
        )

    vector_store_id_env = settings.openai_vector_store_id.strip() if settings.openai_vector_store_id else ""
    vector_store_id_meta = load_vector_store_id(settings.vector_store_meta_path)
    vector_store_id = vector_store_id_env or vector_store_id_meta
    if vector_store_id_env:
        vector_store_source = "env"
    elif vector_store_id_meta:
        vector_store_source = "meta_file"
    else:
        vector_store_source = "missing"

    if not vector_store_id:
        issues.append(
            DiagnosticIssue(
                severity="warning",
                code="vector_store_not_configured",
                message="OpenAI vector store id is missing (knowledge fallback may be limited).",
            )
        )
    elif vector_store_source == "meta_file":
        issues.append(
            DiagnosticIssue(
                severity="warning",
                code="vector_store_env_recommended",
                message=(
                    "Vector store id is loaded from local metadata file. "
                    "For Render/production set OPENAI_VECTOR_STORE_ID explicitly."
                ),
            )
        )

    knowledge_files_count = _safe_md_count(settings.knowledge_path)
    if knowledge_files_count == 0:
        issues.append(
            DiagnosticIssue(
                severity="warning",
                code="knowledge_files_missing",
                message=f"Knowledge directory is empty or missing: {settings.knowledge_path}",
            )
        )

    has_errors = any(item.severity == "error" for item in issues)
    has_warnings = any(item.severity == "warning" for item in issues)

    return {
        "status": "fail" if has_errors else ("warn" if has_warnings else "ok"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runtime": {
            "telegram_mode": settings.telegram_mode,
            "telegram_webhook_path": settings.telegram_webhook_path,
            "telegram_token_set": bool(settings.telegram_bot_token),
            "telegram_webhook_secret_set": bool(settings.telegram_webhook_secret),
            "openai_key_set": bool(settings.openai_api_key),
            "openai_model": settings.openai_model,
            "python_telegram_bot_version": ptb_version or "unknown",
            "python_telegram_bot_business_ready": ptb_business_ready,
            "assistant_api_token_set": bool(settings.assistant_api_token),
            "assistant_rate_limit_window_seconds": settings.assistant_rate_limit_window_seconds,
            "assistant_rate_limit_user_requests": settings.assistant_rate_limit_user_requests,
            "assistant_rate_limit_ip_requests": settings.assistant_rate_limit_ip_requests,
            "vector_store_id_set": bool(vector_store_id),
            "vector_store_id_source": vector_store_source,
            "vector_store_meta_path": str(settings.vector_store_meta_path),
            "crm_provider": settings.crm_provider,
            "crm_api_exposed": settings.crm_api_exposed,
            "crm_rate_limit_window_seconds": settings.crm_rate_limit_window_seconds,
            "crm_rate_limit_ip_requests": settings.crm_rate_limit_ip_requests,
            "tallanto_read_only": settings.tallanto_read_only,
            "tallanto_token_set": tallanto_token_present,
            "tallanto_default_contact_module": settings.tallanto_default_contact_module,
            "database_path": str(settings.database_path),
            "database_parent_writable": database_parent_writable,
            "running_on_render": settings.running_on_render,
            "persistent_data_root": (
                str(settings.persistent_data_root)
                if settings.persistent_data_root != Path()
                else ""
            ),
            "database_on_persistent_storage": database_on_persistent_storage,
            "vector_meta_on_persistent_storage": vector_meta_on_persistent_storage,
            "catalog_path": str(settings.catalog_path),
            "catalog_ok": catalog_ok,
            "catalog_products_count": catalog_products_count,
            "catalog_error": catalog_error,
            "knowledge_path": str(settings.knowledge_path),
            "knowledge_files_count": knowledge_files_count,
        },
        "issues": [{"severity": item.severity, "code": item.code, "message": item.message} for item in issues],
    }
