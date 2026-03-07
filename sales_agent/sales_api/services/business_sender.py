from __future__ import annotations

import asyncio
from typing import Any, Callable, Dict, Optional

from fastapi import HTTPException, status


def split_telegram_text_chunks(text: str, *, max_len: int) -> list[str]:
    normalized = " ".join((text or "").split()).strip()
    if not normalized:
        return []
    if len(normalized) <= max_len:
        return [normalized]

    chunks: list[str] = []
    start = 0
    while start < len(normalized):
        hard_end = min(start + max_len, len(normalized))
        end = hard_end
        if hard_end < len(normalized):
            split_at = normalized.rfind(" ", start, hard_end)
            if split_at > start + max_len // 3:
                end = split_at
        chunk = normalized[start:end].strip()
        if not chunk:
            chunk = normalized[start:hard_end].strip()
            end = hard_end
        chunks.append(chunk)
        start = end
        while start < len(normalized) and normalized[start].isspace():
            start += 1
    return chunks


async def send_business_draft_and_log(
    *,
    conn: Any,
    draft: Dict[str, Any],
    actor: str,
    telegram_bot_token: str,
    parse_business_thread_key: Callable[[str], Optional[tuple[str, int]]],
    get_business_connection: Callable[..., Any],
    send_business_message: Callable[..., Dict[str, Any]],
    send_error_type: type[Exception],
    set_reply_draft_last_error: Callable[..., Any],
    create_approval_action: Callable[..., Any],
    log_business_message: Callable[..., Any],
    max_text_chars: int,
) -> Dict[str, Any]:
    parsed_thread = parse_business_thread_key(str(draft.get("thread_id") or ""))
    if parsed_thread is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Draft thread is not Telegram Business. Use manual confirmation with sent_message_id.",
        )

    if not telegram_bot_token:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="TELEGRAM_BOT_TOKEN is empty. Business send is unavailable.",
        )

    business_connection_id, chat_id = parsed_thread
    connection = get_business_connection(conn, business_connection_id=business_connection_id)
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Business connection not found: {business_connection_id}",
        )
    if not bool(connection.get("is_enabled")):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Business connection is disabled: {business_connection_id}",
        )
    if not bool(connection.get("can_reply")):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Business connection cannot reply: {business_connection_id}",
        )

    message_text = str(draft.get("draft_text") or "").strip()
    chunks = split_telegram_text_chunks(message_text, max_len=max_text_chars)
    if not chunks:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Draft text is empty after normalization.",
        )

    message_ids: list[int] = []
    total_chunks = len(chunks)
    for chunk in chunks:
        try:
            message_result = await asyncio.to_thread(
                send_business_message,
                bot_token=telegram_bot_token,
                business_connection_id=business_connection_id,
                chat_id=chat_id,
                text=chunk,
            )
        except send_error_type as exc:
            if message_ids:
                sent_ids = ",".join(str(item) for item in message_ids)
                partial_error = f"partial_delivery|sent_message_ids={sent_ids}|error={str(exc)}"
                set_reply_draft_last_error(conn, draft_id=int(draft["id"]), last_error=partial_error)
                create_approval_action(
                    conn,
                    draft_id=int(draft["id"]),
                    user_id=int(draft["user_id"]),
                    thread_id=str(draft["thread_id"]),
                    action="draft_send_partial",
                    actor=actor,
                    payload={
                        "error": str(exc),
                        "business_connection_id": business_connection_id,
                        "chat_id": chat_id,
                        "sent_message_ids": message_ids,
                        "delivered_chunks": len(message_ids),
                        "total_chunks": total_chunks,
                    },
                )
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=(
                        f"Partial Telegram Business delivery detected: sent_message_ids={sent_ids}. "
                        "Проверьте чат и подтвердите отправку через sent_message_id, чтобы избежать дублей. "
                        f"Original error: {exc}"
                    ),
                ) from exc

            set_reply_draft_last_error(conn, draft_id=int(draft["id"]), last_error=str(exc))
            create_approval_action(
                conn,
                draft_id=int(draft["id"]),
                user_id=int(draft["user_id"]),
                thread_id=str(draft["thread_id"]),
                action="draft_send_failed",
                actor=actor,
                payload={"error": str(exc), "business_connection_id": business_connection_id, "chat_id": chat_id},
            )
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Telegram Business send failed: {exc}",
            ) from exc

        message_id = int(message_result.get("message_id") or 0)
        if message_id > 0:
            message_ids.append(message_id)
        log_business_message(
            conn,
            business_connection_id=business_connection_id,
            chat_id=chat_id,
            telegram_message_id=message_id if message_id > 0 else None,
            direction="outbound",
            text=chunk,
            user_id=int(draft["user_id"]),
            payload={
                "event_type": "business_message",
                "source": "admin_inbox_send",
                "draft_id": int(draft["id"]),
                "actor": actor,
            },
        )

    sent_message_id = ",".join(str(item) for item in message_ids) if message_ids else None
    return {
        "transport": "telegram_business",
        "business_connection_id": business_connection_id,
        "chat_id": chat_id,
        "message_ids": message_ids,
        "sent_message_id": sent_message_id,
    }
