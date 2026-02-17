from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from sales_agent.sales_core.catalog import CatalogValidationError, load_catalog
from sales_agent.sales_core.config import Settings
from sales_agent.sales_core.vector_store import load_vector_store_id


@dataclass(frozen=True)
class DiagnosticIssue:
    severity: str
    code: str
    message: str


def _safe_md_count(path: Path) -> int:
    if not path.exists() or not path.is_dir():
        return 0
    return len([item for item in path.rglob("*") if item.is_file() and item.suffix.lower() in {".md", ".txt", ".pdf"}])


def _can_write_parent(path: Path) -> bool:
    parent = path.parent
    if not parent.exists() or not parent.is_dir():
        return False
    probe = parent / ".write_probe"
    try:
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
        return True
    except Exception:
        return False


def build_runtime_diagnostics(settings: Settings) -> Dict[str, object]:
    issues: List[DiagnosticIssue] = []

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

    if settings.telegram_mode == "webhook" and not settings.telegram_webhook_secret:
        issues.append(
            DiagnosticIssue(
                severity="warning",
                code="webhook_secret_missing",
                message="TELEGRAM_MODE=webhook without TELEGRAM_WEBHOOK_SECRET.",
            )
        )

    if not _can_write_parent(settings.database_path):
        issues.append(
            DiagnosticIssue(
                severity="error",
                code="database_parent_not_writable",
                message=f"Database parent is not writable: {settings.database_path.parent}",
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

    vector_store_id = settings.openai_vector_store_id or load_vector_store_id(settings.vector_store_meta_path)
    if not vector_store_id:
        issues.append(
            DiagnosticIssue(
                severity="warning",
                code="vector_store_not_configured",
                message="OpenAI vector store id is missing (knowledge fallback may be limited).",
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
            "vector_store_id_set": bool(vector_store_id),
            "crm_provider": settings.crm_provider,
            "database_path": str(settings.database_path),
            "database_parent_writable": _can_write_parent(settings.database_path),
            "catalog_path": str(settings.catalog_path),
            "catalog_ok": catalog_ok,
            "catalog_products_count": catalog_products_count,
            "catalog_error": catalog_error,
            "knowledge_path": str(settings.knowledge_path),
            "knowledge_files_count": knowledge_files_count,
        },
        "issues": [{"severity": item.severity, "code": item.code, "message": item.message} for item in issues],
    }
