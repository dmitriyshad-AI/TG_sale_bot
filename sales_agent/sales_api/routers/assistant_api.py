from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional, Type

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ValidationError

from sales_agent.sales_core.catalog import CatalogValidationError, explain_match, select_top_products

logger = logging.getLogger(__name__)
ASSISTANT_TIMEOUT_SECONDS = 36.0
ASSISTANT_CONTEXT_SUMMARY_MAX = 1200


def build_assistant_api_router(
    *,
    settings: Any,
    assistant_payload_model: Type[BaseModel],
    request_id_from_request: Callable[[Request], str],
    require_assistant_access: Callable[[Request], Dict[str, Any]],
    request_client_ip: Callable[[Request], str],
    enforce_rate_limit: Callable[..., None],
    assistant_rate_limiter: Any,
    criteria_from_payload: Callable[..., Any],
    evaluate_match_quality: Callable[[Any, list[Any]], str],
    build_manager_offer: Callable[[str, bool], Dict[str, object]],
    assistant_mode: Callable[[str, Any], str],
    missing_criteria_fields: Callable[[Any], list[str]],
    sanitize_recent_history: Callable[[Any], list[Dict[str, str]]],
    compact_text: Callable[[object], str],
    merge_assistant_context: Callable[..., Dict[str, Any]],
    llm_client_factory: Callable[[], Any],
    load_vector_store_id: Callable[[Any], str],
    get_connection: Callable[[Any], Any],
    get_or_create_user: Callable[..., int],
    get_conversation_context: Callable[..., Dict[str, Any]],
    upsert_conversation_context: Callable[..., None],
) -> APIRouter:
    router = APIRouter()

    @router.post("/api/assistant/ask")
    async def assistant_ask(payload: Dict[str, Any], request: Request):
        try:
            parsed_payload = assistant_payload_model.model_validate(payload)
        except ValidationError as exc:
            raise RequestValidationError(exc.errors()) from exc

        request_id = request_id_from_request(request)
        assistant_access = require_assistant_access(request)
        client_ip = request_client_ip(request)

        telegram_user_id = assistant_access.get("user_id")
        telegram_user_id_int = telegram_user_id if isinstance(telegram_user_id, int) else None
        if telegram_user_id_int is not None:
            enforce_rate_limit(
                request=request,
                limiter=assistant_rate_limiter,
                key=f"assistant:user:{telegram_user_id_int}",
                limit=settings.assistant_rate_limit_user_requests,
                scope="assistant_user",
            )

        enforce_rate_limit(
            request=request,
            limiter=assistant_rate_limiter,
            key=f"assistant:ip:{client_ip}",
            limit=settings.assistant_rate_limit_ip_requests,
            scope="assistant_ip",
        )

        question = parsed_payload.question.strip()
        if not question:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "code": "empty_question",
                    "message": "Question is empty.",
                    "user_message": "Сообщение получилось пустым. Напишите вопрос текстом.",
                    "request_id": request_id,
                },
            )

        try:
            criteria = criteria_from_payload(parsed_payload.criteria, brand_default=settings.brand_default)
            try:
                top_products = select_top_products(
                    criteria,
                    path=settings.catalog_path,
                    top_k=3,
                    brand_default=settings.brand_default,
                )
            except (CatalogValidationError, FileNotFoundError, OSError):
                top_products = []

            match_quality = evaluate_match_quality(criteria, top_products)
            manager_offer = build_manager_offer(match_quality, has_results=bool(top_products))
            mode = assistant_mode(question, criteria)

            llm_client = llm_client_factory()
            persisted_context: Dict[str, Any] = {}
            context_user_id: Optional[int] = None
            if telegram_user_id_int is not None:
                conn_context = get_connection(settings.database_path)
                try:
                    context_user_id = get_or_create_user(
                        conn_context,
                        channel="telegram",
                        external_id=str(telegram_user_id_int),
                    )
                    persisted_context = get_conversation_context(conn_context, user_id=context_user_id)
                finally:
                    conn_context.close()

            user_context: Dict[str, Any] = {}
            recent_history = sanitize_recent_history(parsed_payload.recent_history)
            summary_text = ""
            if isinstance(persisted_context, dict):
                summary_text = compact_text(
                    persisted_context.get("summary_text"),
                    limit=ASSISTANT_CONTEXT_SUMMARY_MAX,
                )

            if isinstance(parsed_payload.context_summary, str) and parsed_payload.context_summary.strip():
                summary_text = compact_text(parsed_payload.context_summary, limit=ASSISTANT_CONTEXT_SUMMARY_MAX)
            elif not summary_text and recent_history:
                history_excerpt = " | ".join(
                    f"{item['role']}: {item['text']}" for item in recent_history[-4:]
                )
                if history_excerpt:
                    summary_text = compact_text(history_excerpt, limit=ASSISTANT_CONTEXT_SUMMARY_MAX)

            if summary_text:
                user_context["summary_text"] = summary_text

            answer_text = ""
            sources: list[str] = []
            used_fallback = False
            recommended_ids: list[str] = []

            if mode == "knowledge":
                vector_store_id = settings.openai_vector_store_id or load_vector_store_id(settings.vector_store_meta_path)
                knowledge_reply = await llm_client.answer_knowledge_question_async(
                    question=question,
                    vector_store_id=vector_store_id,
                    user_context=user_context,
                    allow_web_fallback=settings.openai_web_fallback_enabled,
                    site_domain=settings.openai_web_fallback_domain,
                )
                answer_text = knowledge_reply.answer_text
                sources = list(knowledge_reply.sources)
                used_fallback = knowledge_reply.used_fallback
            elif mode == "consultative":
                consult_reply = await llm_client.build_consultative_reply_async(
                    user_message=question,
                    criteria=criteria,
                    top_products=top_products,
                    missing_fields=missing_criteria_fields(criteria),
                    repeat_count=0,
                    product_offer_allowed=match_quality != "none",
                    recent_history=recent_history,
                    user_context=user_context,
                )
                answer_text = consult_reply.answer_text
                used_fallback = consult_reply.used_fallback
                recommended_ids = list(consult_reply.recommended_product_ids)
            else:
                general_reply = await llm_client.build_general_help_reply_async(
                    user_message=question,
                    dialogue_state=None,
                    recent_history=recent_history,
                    user_context=user_context,
                )
                answer_text = general_reply.answer_text
                used_fallback = general_reply.used_fallback

            recommended_products: list[Dict[str, str]] = []
            if top_products:
                allowed_ids = {item.id for item in top_products}
                filtered_ids = [item_id for item_id in recommended_ids if item_id in allowed_ids]
                if mode == "consultative" and not filtered_ids:
                    filtered_ids = [top_products[0].id]
                for product in top_products:
                    if product.id not in filtered_ids:
                        continue
                    recommended_products.append(
                        {
                            "id": product.id,
                            "title": product.title,
                            "url": str(product.url),
                            "why_match": explain_match(product, criteria),
                        }
                    )

            if context_user_id is not None:
                merged_context = merge_assistant_context(
                    persisted_context if isinstance(persisted_context, dict) else {},
                    question=question,
                    criteria=criteria,
                    recent_history=recent_history,
                    context_summary=summary_text,
                )
                conn_update = get_connection(settings.database_path)
                try:
                    upsert_conversation_context(conn_update, user_id=context_user_id, summary=merged_context)
                finally:
                    conn_update.close()

            return {
                "ok": True,
                "request_id": request_id,
                "mode": mode,
                "answer_text": answer_text,
                "sources": sources,
                "used_fallback": used_fallback,
                "criteria": {
                    "brand": criteria.brand or settings.brand_default,
                    "grade": criteria.grade,
                    "goal": criteria.goal,
                    "subject": criteria.subject,
                    "format": criteria.format,
                },
                "match_quality": match_quality,
                "recommended_products": recommended_products,
                "manager_offer": manager_offer,
                "processing_note": "Спасибо за ожидание. Запрос проработан подробно, можно продолжать диалог.",
            }
        except HTTPException:
            raise
        except Exception:
            logger.exception("Assistant ask failed (request_id=%s)", request_id)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "code": "assistant_unavailable",
                    "message": "Assistant request failed.",
                    "user_message": "Сервис ответа временно недоступен. Повторите вопрос через минуту.",
                    "request_id": request_id,
                },
            )

    return router
