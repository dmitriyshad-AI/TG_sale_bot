from __future__ import annotations

from typing import Any, Callable, Dict

from sales_agent.sales_core.db import (
    count_call_records,
    count_campaign_plans,
    count_faq_lab_runs,
    count_mango_events,
    count_webhook_updates_by_status,
    get_latest_faq_lab_run,
    get_oldest_call_record_created_at,
    get_oldest_campaign_plan_created_at,
    get_oldest_mango_event_created_at,
)


def build_runtime_enrichment(
    *,
    conn: Any,
    settings: Any,
    mango_webhook_path: str,
    mango_ingest_enabled: Callable[[], bool],
) -> Dict[str, Any]:
    latest_faq_run = get_latest_faq_lab_run(conn)
    faq_runs_failed = count_faq_lab_runs(conn, status="failed")
    latest_faq_status = str((latest_faq_run or {}).get("status") or "").strip()
    if faq_runs_failed > 0 and latest_faq_status.lower() in {"", "success"}:
        latest_faq_status = "failed"
    return {
        "webhook_queue": {
            "pending": count_webhook_updates_by_status(conn, "pending"),
            "retry": count_webhook_updates_by_status(conn, "retry"),
            "processing": count_webhook_updates_by_status(conn, "processing"),
            "failed": count_webhook_updates_by_status(conn, "failed"),
        },
        "mango": {
            "enabled": mango_ingest_enabled(),
            "webhook_path": mango_webhook_path,
            "polling_enabled": settings.mango_polling_enabled,
            "poll_interval_seconds": settings.mango_poll_interval_seconds,
            "poll_limit_per_run": settings.mango_poll_limit_per_run,
            "poll_retry_attempts": settings.mango_poll_retry_attempts,
            "poll_retry_backoff_seconds": settings.mango_poll_retry_backoff_seconds,
            "retry_failed_limit_per_run": settings.mango_retry_failed_limit_per_run,
            "recording_ttl_hours": settings.mango_call_recording_ttl_hours,
            "calls_path": settings.mango_calls_path,
            "events_total": count_mango_events(conn),
            "events_queued": count_mango_events(conn, status="queued"),
            "events_processing": count_mango_events(conn, status="processing"),
            "events_failed": count_mango_events(conn, status="failed"),
            "oldest_failed_created_at": get_oldest_mango_event_created_at(conn, status="failed"),
        },
        "calls": {
            "enabled": settings.enable_call_copilot,
            "records_total": count_call_records(conn),
            "records_queued": count_call_records(conn, status="queued"),
            "records_processing": count_call_records(conn, status="processing"),
            "records_done": count_call_records(conn, status="done"),
            "records_failed": count_call_records(conn, status="failed"),
            "oldest_failed_created_at": get_oldest_call_record_created_at(conn, status="failed"),
        },
        "faq_lab": {
            "enabled": settings.enable_faq_lab,
            "scheduler_enabled": settings.faq_lab_scheduler_enabled,
            "interval_seconds": settings.faq_lab_interval_seconds,
            "window_days": settings.faq_lab_window_days,
            "min_question_count": settings.faq_lab_min_question_count,
            "max_items_per_run": settings.faq_lab_max_items_per_run,
            "runs_total": count_faq_lab_runs(conn),
            "runs_failed": faq_runs_failed,
            "latest_run_status": latest_faq_status,
            "latest_run_started_at": str((latest_faq_run or {}).get("started_at") or "").strip(),
            "latest_run_finished_at": str((latest_faq_run or {}).get("finished_at") or "").strip(),
            "latest_run_error_set": bool(str((latest_faq_run or {}).get("error_text") or "").strip()),
        },
        "director": {
            "enabled": settings.enable_director_agent,
            "plans_total": count_campaign_plans(conn),
            "plans_draft": count_campaign_plans(conn, status="draft"),
            "plans_approved": count_campaign_plans(conn, status="approved"),
            "plans_applied": count_campaign_plans(conn, status="applied"),
            "plans_completed": count_campaign_plans(conn, status="completed"),
            "plans_archived": count_campaign_plans(conn, status="archived"),
            "oldest_draft_created_at": get_oldest_campaign_plan_created_at(conn, status="draft"),
        },
    }
