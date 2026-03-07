from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict

from fastapi import HTTPException, status

from sales_agent.sales_core.db import (
    claim_reply_draft_for_send,
    create_approval_action,
    get_reply_draft,
    update_reply_draft_status,
)


@dataclass(frozen=True)
class DraftSendResult:
    draft: Dict[str, Any]
    delivery: Dict[str, Any]
    already_sent: bool = False


async def send_approved_draft(
    conn: Any,
    *,
    draft_id: int,
    actor: str,
    manual_sent_message_id: str,
    is_business_thread: Callable[[str], bool],
    business_sender: Callable[[Dict[str, Any], str], Awaitable[Dict[str, Any]]],
) -> DraftSendResult:
    draft = get_reply_draft(conn, draft_id)
    if draft is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Draft {draft_id} not found.")

    current_status = str(draft.get("status") or "")
    if current_status == "sent":
        return DraftSendResult(draft=draft, delivery={}, already_sent=True)
    if current_status == "sending":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Draft {draft_id} is already being sent by another request.",
        )
    if current_status != "approved":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Draft {draft_id} must be approved before send.",
        )

    claimed = claim_reply_draft_for_send(conn, draft_id=draft_id, actor=actor)
    if claimed is None:
        latest = get_reply_draft(conn, draft_id)
        if latest and str(latest.get("status") or "") == "sent":
            return DraftSendResult(draft=latest, delivery={}, already_sent=True)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Draft {draft_id} was modified by another request. Retry.",
        )

    if not is_business_thread(str(claimed.get("thread_id") or "")):
        if not manual_sent_message_id:
            update_reply_draft_status(
                conn,
                draft_id=draft_id,
                status="approved",
                actor=actor,
                last_error="manual_confirmation_required",
            )
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Manual send requires sent_message_id for non-business threads.",
            )
        delivery = {
            "transport": "manual_confirmed",
            "sent_message_id": manual_sent_message_id,
            "message_ids": [],
        }
    else:
        try:
            delivery = await business_sender(claimed, actor)
        except HTTPException as exc:
            update_reply_draft_status(
                conn,
                draft_id=draft_id,
                status="approved",
                actor=actor,
                last_error=str(exc.detail),
            )
            raise

    resolved_sent_message_id = (
        (delivery.get("sent_message_id") if isinstance(delivery, dict) else None)
        or manual_sent_message_id
        or None
    )
    update_reply_draft_status(
        conn,
        draft_id=draft_id,
        status="sent",
        actor=actor,
        sent_message_id=resolved_sent_message_id,
    )
    create_approval_action(
        conn,
        draft_id=draft_id,
        user_id=int(draft["user_id"]),
        thread_id=str(draft["thread_id"]),
        action="draft_sent",
        actor=actor,
        payload={
            "sent_message_id": resolved_sent_message_id,
            "delivery": delivery,
        },
    )
    updated_draft = get_reply_draft(conn, draft_id) or claimed
    return DraftSendResult(
        draft=updated_draft,
        delivery=delivery if isinstance(delivery, dict) else {},
        already_sent=False,
    )

