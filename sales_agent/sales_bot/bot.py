import logging
from typing import Dict, Optional

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from sales_agent.sales_core.config import get_settings
from sales_agent.sales_core import db as db_module


logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

settings = get_settings()
db_module.init_db(settings.database_path)


def _user_meta(update: Update) -> Dict[str, Optional[str]]:
    user = update.effective_user
    return {
        "username": getattr(user, "username", None),
        "first_name": getattr(user, "first_name", None),
        "last_name": getattr(user, "last_name", None),
        "chat_id": update.effective_chat.id if update.effective_chat else None,
    }


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    user = update.effective_user
    conn = db_module.get_connection(settings.database_path)
    user_id = db_module.get_or_create_user(
        conn=conn,
        channel="telegram",
        external_id=str(user.id),
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
    )
    incoming_text = update.message.text or "/start"
    db_module.log_message(
        conn, user_id, "inbound", incoming_text, {"type": "command", **_user_meta(update)}
    )
    greeting = (
        "Привет! Я помогу подобрать курс или лагерь. "
        "Напишите класс и цель (ЕГЭ/ОГЭ/олимпиада/каникулы), чтобы я предложил варианты."
    )
    await update.message.reply_text(greeting)
    db_module.log_message(
        conn, user_id, "outbound", greeting, {"handler": "start", **_user_meta(update)}
    )
    conn.close()


async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    user = update.effective_user
    text = update.message.text or ""
    conn = db_module.get_connection(settings.database_path)
    user_id = db_module.get_or_create_user(
        conn=conn,
        channel="telegram",
        external_id=str(user.id),
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
    )
    db_module.log_message(
        conn, user_id, "inbound", text, {"type": "message", **_user_meta(update)}
    )
    reply = f"Вы написали: {text}"
    await update.message.reply_text(reply)
    db_module.log_message(
        conn, user_id, "outbound", reply, {"handler": "echo", **_user_meta(update)}
    )
    conn.close()


def main() -> None:
    if not settings.telegram_bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set. Fill .env before running.")
    application = ApplicationBuilder().token(settings.telegram_bot_token).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))
    logger.info("Starting Telegram bot polling...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

