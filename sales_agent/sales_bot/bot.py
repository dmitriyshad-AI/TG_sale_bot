import logging
import re
from typing import Dict, List, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from sales_agent.sales_core import db as db_module
from sales_agent.sales_core.catalog import SearchCriteria, explain_match, select_top_products
from sales_agent.sales_core.config import get_settings
from sales_agent.sales_core.crm import build_crm_client
from sales_agent.sales_core.deeplink import build_greeting_hint, parse_start_payload
from sales_agent.sales_core.flow import (
    STATE_ASK_CONTACT,
    STATE_ASK_FORMAT,
    STATE_ASK_GOAL,
    STATE_ASK_GRADE,
    STATE_ASK_SUBJECT,
    STATE_DONE,
    advance_flow,
    build_prompt,
    ensure_state,
)
from sales_agent.sales_core.llm_client import LLMClient
from sales_agent.sales_core.vector_store import load_vector_store_id


logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

settings = get_settings()
db_module.init_db(settings.database_path)
LEADTEST_WAITING_PHONE_KEY = "leadtest_waiting_phone"
KBTEST_WAITING_QUESTION_KEY = "kbtest_waiting_question"

KNOWLEDGE_QUERY_KEYWORDS = {
    "договор",
    "документ",
    "оплата",
    "оплат",
    "рассрочка",
    "рассроч",
    "возврат",
    "вычет",
    "маткапитал",
    "безопасность",
    "проживание",
    "питание",
    "условия",
    "как оплатить",
    "перенос",
}

CONSULTATIVE_QUERY_KEYWORDS = {
    "поступить",
    "мфти",
    "что делать",
    "как подготов",
    "нужен план",
    "план подготовки",
    "как лучше",
    "куда поступ",
    "помогите выбрать",
}

GOAL_HINTS = {
    "еге": "ege",
    "огэ": "oge",
    "олимп": "olympiad",
    "лагер": "camp",
    "успеваем": "base",
}

SUBJECT_HINTS = {
    "матем": "math",
    "физ": "physics",
    "информ": "informatics",
}

GRADE_PATTERN = re.compile(r"\b(1[01]|[1-9])\s*[- ]?класс", flags=re.IGNORECASE)


def _user_meta(update: Update) -> Dict[str, Optional[str]]:
    user = update.effective_user
    return {
        "username": getattr(user, "username", None),
        "first_name": getattr(user, "first_name", None),
        "last_name": getattr(user, "last_name", None),
        "chat_id": update.effective_chat.id if update.effective_chat else None,
    }


def _sanitize_phone(raw_value: str) -> Optional[str]:
    digits = "".join(ch for ch in raw_value if ch.isdigit())
    if len(digits) < 10:
        return None
    if len(digits) == 10:
        return f"+7{digits}"
    if len(digits) == 11 and digits.startswith("8"):
        return f"+7{digits[1:]}"
    if len(digits) == 11 and digits.startswith("7"):
        return f"+{digits}"
    return f"+{digits}"


def _get_or_create_user_id(update: Update, conn) -> int:
    user = update.effective_user
    return db_module.get_or_create_user(
        conn=conn,
        channel="telegram",
        external_id=str(user.id),
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
    )


def _build_user_name(update: Update) -> str:
    user = update.effective_user
    chunks = [user.first_name or "", user.last_name or ""]
    return " ".join(chunk for chunk in chunks if chunk).strip()


def _resolve_vector_store_id() -> Optional[str]:
    if settings.openai_vector_store_id:
        return settings.openai_vector_store_id
    return load_vector_store_id(settings.vector_store_meta_path)


def _is_knowledge_query(text: str) -> bool:
    normalized = text.strip().lower()
    if not normalized:
        return False
    return any(keyword in normalized for keyword in KNOWLEDGE_QUERY_KEYWORDS)


def _is_consultative_query(text: str) -> bool:
    normalized = text.strip().lower()
    if len(normalized) < 12:
        return False
    if _is_knowledge_query(normalized):
        return False
    return any(keyword in normalized for keyword in CONSULTATIVE_QUERY_KEYWORDS)


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip().lower())


def _extract_grade_hint(text: str) -> Optional[int]:
    normalized = _normalize_text(text)
    matched = GRADE_PATTERN.search(normalized)
    if matched:
        return int(matched.group(1))

    digits = "".join(ch for ch in normalized if ch.isdigit())
    if digits in {str(value) for value in range(1, 12)}:
        return int(digits)
    return None


def _extract_goal_hint(text: str) -> Optional[str]:
    normalized = _normalize_text(text)
    for keyword, goal in GOAL_HINTS.items():
        if keyword in normalized:
            return goal
    if "поступить" in normalized and "мфти" in normalized:
        return "ege"
    return None


def _extract_subject_hint(text: str) -> Optional[str]:
    normalized = _normalize_text(text)
    for keyword, subject in SUBJECT_HINTS.items():
        if keyword in normalized:
            return subject
    return None


def _next_state_for_consultative(criteria: Dict[str, object]) -> str:
    if not criteria.get("grade"):
        return STATE_ASK_GRADE
    if not criteria.get("goal"):
        return STATE_ASK_GOAL
    if criteria.get("subject") is None:
        return STATE_ASK_SUBJECT
    if not criteria.get("format"):
        return STATE_ASK_FORMAT
    return STATE_ASK_GOAL


def _format_soft_picks(products: List[object]) -> str:
    if not products:
        return ""
    lines = ["Вот 2 направления, которые уже похожи на ваш запрос:"]
    for product in products[:2]:
        usp = product.usp[0] if getattr(product, "usp", None) else "подходит для текущей цели"
        lines.append(f"• {product.title} — {usp}.")
    return "\n".join(lines)


def _build_consultative_text(
    text: str,
    criteria: Dict[str, object],
    products: List[object],
    next_question: str,
    *,
    subject_from_user: bool,
    show_picks: bool,
    repeated_without_new_info: bool,
) -> str:
    grade = criteria.get("grade")
    goal = criteria.get("goal")
    subject = criteria.get("subject")
    normalized = _normalize_text(text)

    if repeated_without_new_info:
        return (
            "Понял, цель поступить в МФТИ.\n\n"
            "Чтобы дать действительно полезный план без лишних шагов, уточню один пункт:\n"
            f"{next_question}\n\n"
            "Можно ответить коротко, в 1-2 словах."
        )

    if grade:
        intro = (
            f"Отличная цель. Для {grade} класса поступление в МФТИ обычно строят через сильный профиль "
            "по ЕГЭ и, по возможности, олимпиадные результаты."
        )
    else:
        intro = (
            "Отличная цель. Для поступления в МФТИ обычно строят персональный маршрут: "
            "ЕГЭ + олимпиады + регулярный контроль прогресса."
        )

    if goal == "ege":
        focus = "Сейчас логично сделать акцент на ЕГЭ-план и зафиксировать приоритетный предмет."
    elif goal == "olympiad":
        focus = "Тогда делаем акцент на олимпиадный трек и параллельно держим базу под экзамены."
    elif "поступить" in normalized and "мфти" in normalized:
        focus = "Если цель именно МФТИ, обычно начинаем с ЕГЭ-трека и добавляем олимпиадную стратегию."
    else:
        focus = "Соберу точный план под ваш кейс, чтобы без перегруза и с понятными этапами."

    subject_hint = ""
    if subject_from_user and subject == "math":
        subject_hint = "По математике можем сразу предложить профильный трек."
    elif subject_from_user and subject == "physics":
        subject_hint = "По физике можем сразу собрать траекторию от базы до сложных задач."
    elif subject_from_user and subject == "informatics":
        subject_hint = "По информатике подберем трек под формат задач ЕГЭ/олимпиад."

    picks = _format_soft_picks(products) if show_picks else ""
    cta = (
        "Если хотите, продолжим коротко: после следующего уточнения дам 2-3 программы "
        "и объясню, какая лучше под вашу цель."
    )

    chunks = [intro, focus]
    if subject_hint:
        chunks.append(subject_hint)
    if picks:
        chunks.append(picks)
    chunks.extend([next_question, cta])
    return "\n\n".join(chunks)


def _build_inline_keyboard(layout):
    if not layout:
        return None
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(text=text, callback_data=callback_data) for text, callback_data in row]
            for row in layout
        ]
    )


def _criteria_from_state(state_data: Dict[str, object]) -> SearchCriteria:
    criteria = state_data.get("criteria") if isinstance(state_data.get("criteria"), dict) else {}
    return SearchCriteria(
        brand=criteria.get("brand") if isinstance(criteria, dict) else None,
        grade=criteria.get("grade") if isinstance(criteria, dict) else None,
        goal=criteria.get("goal") if isinstance(criteria, dict) else None,
        subject=criteria.get("subject") if isinstance(criteria, dict) else None,
        format=criteria.get("format") if isinstance(criteria, dict) else None,
    )


def _apply_start_meta_to_state(state_data: Dict[str, object], meta: Dict[str, str]) -> Dict[str, object]:
    criteria = state_data.get("criteria") if isinstance(state_data.get("criteria"), dict) else {}
    brand = meta.get("brand", "").strip().lower()
    if brand == "kmipt":
        criteria["brand"] = brand
    state_data["criteria"] = criteria
    return state_data


def _select_products(criteria: SearchCriteria):
    return select_top_products(
        criteria=criteria,
        path=settings.catalog_path,
        top_k=3,
        brand_default=settings.brand_default,
    )


def _format_product_blurb(criteria: SearchCriteria, products) -> str:
    if not products:
        return "Подходящие продукты пока не найдены. Оставьте контакт, и менеджер подберет вручную."

    lines = ["Подобрал варианты:"]
    for idx, product in enumerate(products, start=1):
        reason = explain_match(product, criteria)
        lines.append(
            f"{idx}. {product.title}\n"
            f"{reason}\n"
            f"Ссылка: {str(product.url)}"
        )
    return "\n\n".join(lines)


def _target_message(update: Update) -> Optional[Message]:
    if update.callback_query and update.callback_query.message:
        return update.callback_query.message
    return update.message


async def _reply(update: Update, text: str, keyboard_layout=None) -> None:
    target = _target_message(update)
    if not target:
        return
    markup = _build_inline_keyboard(keyboard_layout)
    await target.reply_text(text, reply_markup=markup)


async def _create_lead_from_phone(
    update: Update,
    raw_phone: str,
    command_source: str,
) -> None:
    target = _target_message(update)
    if not target:
        return

    phone = _sanitize_phone(raw_phone)
    if not phone:
        msg = "Не удалось распознать номер. Отправьте номер в формате +79991234567."
        await target.reply_text(msg)
        return

    conn = db_module.get_connection(settings.database_path)
    try:
        user_id = _get_or_create_user_id(update, conn)
        crm = build_crm_client(settings)
        result = await crm.create_lead_async(
            phone=phone,
            brand=settings.brand_default,
            name=_build_user_name(update),
            source=command_source,
            note=f"telegram_user_id={update.effective_user.id}",
        )
        status = "created" if result.success else "failed"
        db_module.create_lead_record(
            conn=conn,
            user_id=user_id,
            status=status,
            tallanto_entry_id=result.entry_id,
            contact={
                "phone": phone,
                "source": command_source,
                "brand": settings.brand_default,
                "error": result.error,
            },
        )

        if result.success:
            reply = f"Лид создан: {result.entry_id or 'без id в ответе'}"
        else:
            reply = (
                "Не удалось создать лид в CRM. "
                f"Причина: {result.error or 'неизвестная ошибка'}."
            )

        await target.reply_text(reply)
        db_module.log_message(
            conn,
            user_id,
            "outbound",
            reply,
            {"handler": "leadtest", "status": status, **_user_meta(update)},
        )
    finally:
        conn.close()


async def _answer_knowledge_question(update: Update, question: str) -> None:
    target = _target_message(update)
    if not target:
        return

    llm_client = LLMClient(api_key=settings.openai_api_key, model=settings.openai_model)
    knowledge_reply = await llm_client.answer_knowledge_question_async(
        question=question,
        vector_store_id=_resolve_vector_store_id(),
    )

    text = knowledge_reply.answer_text
    if knowledge_reply.sources:
        sources = ", ".join(knowledge_reply.sources)
        text = f"{text}\n\nИсточники: {sources}"

    await target.reply_text(text)

    conn = db_module.get_connection(settings.database_path)
    try:
        user_id = _get_or_create_user_id(update, conn)
        db_module.log_message(
            conn,
            user_id,
            "outbound",
            text,
            {
                "handler": "kb",
                "used_fallback": knowledge_reply.used_fallback,
                "error": knowledge_reply.error,
                **_user_meta(update),
            },
        )
    finally:
        conn.close()


async def _handle_consultative_query(update: Update, text: str) -> bool:
    if not _is_consultative_query(text):
        return False

    conn = db_module.get_connection(settings.database_path)
    try:
        user_id = _get_or_create_user_id(update, conn)
        session = db_module.get_session(conn, user_id)
        state = ensure_state(session.get("state"), brand_default=settings.brand_default)
        if state.get("state") == STATE_ASK_CONTACT:
            return False

        db_module.log_message(
            conn,
            user_id,
            "inbound",
            text,
            {"type": "message", "handler": "consultative", **_user_meta(update)},
        )

        criteria = state.get("criteria") if isinstance(state.get("criteria"), dict) else {}
        previous_criteria = dict(criteria)

        grade_hint = _extract_grade_hint(text)
        goal_hint = _extract_goal_hint(text)
        subject_hint = _extract_subject_hint(text)

        if grade_hint and criteria.get("grade") != grade_hint:
            criteria["grade"] = grade_hint
        if goal_hint and criteria.get("goal") != goal_hint:
            criteria["goal"] = goal_hint
        if subject_hint and criteria.get("subject") != subject_hint:
            criteria["subject"] = subject_hint

        changed_grade = previous_criteria.get("grade") != criteria.get("grade")
        changed_goal = previous_criteria.get("goal") != criteria.get("goal")
        changed_subject = previous_criteria.get("subject") != criteria.get("subject")
        has_new_info = changed_grade or changed_goal or changed_subject

        criteria["brand"] = "kmipt"
        state["criteria"] = criteria
        state["state"] = _next_state_for_consultative(criteria)
        if state["state"] == STATE_DONE:
            state["state"] = STATE_ASK_GRADE

        normalized_text = _normalize_text(text)
        consultative = state.get("consultative") if isinstance(state.get("consultative"), dict) else {}
        last_text = str(consultative.get("last_text") or "")
        previous_turns = int(consultative.get("turns") or 0)
        repeated_without_new_info = (last_text == normalized_text) and (not has_new_info)
        state["consultative"] = {
            "last_text": normalized_text,
            "turns": previous_turns + 1,
        }

        db_module.upsert_session_state(conn, user_id=user_id, state=state)
    finally:
        conn.close()

    criteria_obj = _criteria_from_state(state)
    products = _select_products(criteria_obj)
    prompt = build_prompt(state)
    response_text = _build_consultative_text(
        text=text,
        criteria=state["criteria"],
        products=products,
        next_question=prompt.message,
        subject_from_user=bool(subject_hint),
        show_picks=has_new_info or int(state.get("consultative", {}).get("turns", 0)) <= 1,
        repeated_without_new_info=repeated_without_new_info,
    )
    await _reply(update, response_text, keyboard_layout=prompt.keyboard)

    conn = db_module.get_connection(settings.database_path)
    try:
        user_id = _get_or_create_user_id(update, conn)
        db_module.log_message(
            conn,
            user_id,
            "outbound",
            response_text,
            {
                "handler": "consultative",
                "next_state": state.get("state"),
                "products_count": len(products),
                **_user_meta(update),
            },
        )
    finally:
        conn.close()

    return True


async def _handle_flow_step(
    update: Update,
    message_text: Optional[str] = None,
    callback_data: Optional[str] = None,
) -> None:
    conn = db_module.get_connection(settings.database_path)
    try:
        user_id = _get_or_create_user_id(update, conn)
        session = db_module.get_session(conn, user_id)
        previous_state = session["state"].get("state") if isinstance(session["state"], dict) else None

        inbound_text = callback_data or message_text or ""
        inbound_type = "callback" if callback_data else "message"
        db_module.log_message(
            conn,
            user_id,
            "inbound",
            inbound_text,
            {"type": inbound_type, "handler": "flow", **_user_meta(update)},
        )

        step = advance_flow(
            state_data=session["state"],
            brand_default=settings.brand_default,
            message_text=message_text,
            callback_data=callback_data,
        )
        db_module.upsert_session_state(conn, user_id=user_id, state=step.state_data)
    finally:
        conn.close()

    response_text = step.message
    if step.should_suggest_products:
        try:
            criteria = _criteria_from_state(step.state_data)
            products = _select_products(criteria)
            products_block = _format_product_blurb(criteria, products)

            llm_client = LLMClient(api_key=settings.openai_api_key, model=settings.openai_model)
            sales_reply = await llm_client.build_sales_reply_async(
                criteria=criteria,
                top_products=products,
            )

            extra = []
            if sales_reply.next_question:
                extra.append(sales_reply.next_question)
            if sales_reply.call_to_action:
                extra.append(sales_reply.call_to_action)

            response_text = f"{sales_reply.answer_text}\n\n{products_block}"
            if extra:
                response_text = f"{response_text}\n\n" + "\n".join(extra)
        except Exception as exc:  # defensive fallback
            logger.exception("Failed to prepare product suggestions")
            response_text = (
                "Подбор временно недоступен. "
                "Оставьте контакт, и менеджер поможет вручную."
            )

    await _reply(update, response_text, keyboard_layout=step.keyboard)

    conn = db_module.get_connection(settings.database_path)
    try:
        user_id = _get_or_create_user_id(update, conn)
        db_module.log_message(
            conn,
            user_id,
            "outbound",
            response_text,
            {"handler": "flow", "next_state": step.next_state, **_user_meta(update)},
        )
    finally:
        conn.close()

    if previous_state == STATE_ASK_CONTACT and step.completed and step.state_data.get("contact"):
        await _create_lead_from_phone(
            update=update,
            raw_phone=str(step.state_data["contact"]),
            command_source="telegram_flow_contact",
        )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    context.user_data.pop(LEADTEST_WAITING_PHONE_KEY, None)
    context.user_data.pop(KBTEST_WAITING_QUESTION_KEY, None)
    state = ensure_state(None, brand_default=settings.brand_default)
    start_payload = context.args[0] if context.args else ""
    start_meta = parse_start_payload(start_payload)
    state = _apply_start_meta_to_state(state, start_meta)
    prompt = build_prompt(state)

    conn = db_module.get_connection(settings.database_path)
    try:
        user_id = _get_or_create_user_id(update, conn)
        incoming_text = update.message.text or "/start"
        db_module.log_message(
            conn, user_id, "inbound", incoming_text, {"type": "command", **_user_meta(update)}
        )
        db_module.upsert_session_state(conn, user_id=user_id, state=state, meta=start_meta or None)
    finally:
        conn.close()

    hint = build_greeting_hint(start_meta)
    hint_block = f"{hint}\n\n" if hint else ""
    greeting = (
        "Привет! Я помогу подобрать курс или лагерь УНПК МФТИ.\n\n"
        f"{hint_block}{prompt.message}"
    )
    await _reply(update, greeting, keyboard_layout=prompt.keyboard)

    conn = db_module.get_connection(settings.database_path)
    try:
        user_id = _get_or_create_user_id(update, conn)
        db_module.log_message(
            conn, user_id, "outbound", greeting, {"handler": "start", **_user_meta(update)}
        )
    finally:
        conn.close()


async def leadtest(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    conn = db_module.get_connection(settings.database_path)
    try:
        user_id = _get_or_create_user_id(update, conn)
        incoming_text = update.message.text or "/leadtest"
        db_module.log_message(
            conn,
            user_id,
            "inbound",
            incoming_text,
            {"type": "command", "handler": "leadtest", **_user_meta(update)},
        )
    finally:
        conn.close()

    phone_from_args = " ".join(context.args).strip() if context.args else ""
    if phone_from_args:
        await _create_lead_from_phone(
            update=update,
            raw_phone=phone_from_args,
            command_source="telegram_leadtest_command",
        )
        return

    context.user_data[LEADTEST_WAITING_PHONE_KEY] = True
    reply = (
        "Команда /leadtest запущена. "
        "Отправьте номер телефона отдельным сообщением, например: +79991234567"
    )
    await update.message.reply_text(reply)


async def kbtest(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    question_from_args = " ".join(context.args).strip() if context.args else ""
    if question_from_args:
        await _answer_knowledge_question(update, question=question_from_args)
        return

    context.user_data[KBTEST_WAITING_QUESTION_KEY] = True
    await update.message.reply_text(
        "Команда /kbtest запущена. Напишите вопрос по условиям, оплате или документам."
    )


async def on_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    text = update.message.text or ""
    if context.user_data.get(KBTEST_WAITING_QUESTION_KEY):
        context.user_data.pop(KBTEST_WAITING_QUESTION_KEY, None)

        conn = db_module.get_connection(settings.database_path)
        try:
            user_id = _get_or_create_user_id(update, conn)
            db_module.log_message(
                conn,
                user_id,
                "inbound",
                text,
                {"type": "message", "handler": "kbtest", **_user_meta(update)},
            )
        finally:
            conn.close()

        await _answer_knowledge_question(update, question=text)
        return

    if context.user_data.get(LEADTEST_WAITING_PHONE_KEY):
        context.user_data.pop(LEADTEST_WAITING_PHONE_KEY, None)

        conn = db_module.get_connection(settings.database_path)
        try:
            user_id = _get_or_create_user_id(update, conn)
            db_module.log_message(
                conn,
                user_id,
                "inbound",
                text,
                {"type": "message", "handler": "leadtest", **_user_meta(update)},
            )
        finally:
            conn.close()

        await _create_lead_from_phone(
            update=update,
            raw_phone=text,
            command_source="telegram_leadtest_message",
        )
        return

    if _is_knowledge_query(text):
        conn = db_module.get_connection(settings.database_path)
        try:
            user_id = _get_or_create_user_id(update, conn)
            db_module.log_message(
                conn,
                user_id,
                "inbound",
                text,
                {"type": "message", "handler": "kb-auto", **_user_meta(update)},
            )
        finally:
            conn.close()

        await _answer_knowledge_question(update, question=text)
        return

    handled_consultative = await _handle_consultative_query(update=update, text=text)
    if handled_consultative:
        return

    await _handle_flow_step(update=update, message_text=text)


async def on_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.callback_query:
        return
    await update.callback_query.answer()
    await _handle_flow_step(update=update, callback_data=update.callback_query.data)


def main() -> None:
    if not settings.telegram_bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set. Fill .env before running.")
    application = ApplicationBuilder().token(settings.telegram_bot_token).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("leadtest", leadtest))
    application.add_handler(CommandHandler("kbtest", kbtest))
    application.add_handler(CallbackQueryHandler(on_callback_query))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text_message))
    logger.info("Starting Telegram bot polling...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
