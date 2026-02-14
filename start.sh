#!/usr/bin/env sh
set -eu

PORT="${PORT:-8000}"
TELEGRAM_MODE="${TELEGRAM_MODE:-polling}"

if [ "${TELEGRAM_MODE}" = "webhook" ]; then
  echo "[start] Starting API in webhook mode on 0.0.0.0:${PORT}"
  exec uvicorn sales_agent.sales_api.main:app --host 0.0.0.0 --port "${PORT}"
fi

echo "[start] Starting API on 0.0.0.0:${PORT} (polling mode)"
uvicorn sales_agent.sales_api.main:app --host 0.0.0.0 --port "${PORT}" &
API_PID=$!

cleanup() {
  if kill -0 "${API_PID}" 2>/dev/null; then
    kill "${API_PID}" 2>/dev/null || true
    wait "${API_PID}" 2>/dev/null || true
  fi
}
trap cleanup INT TERM EXIT

echo "[start] Starting Telegram bot in polling mode"
python -m sales_agent.sales_bot.bot
BOT_STATUS=$?

cleanup
exit "${BOT_STATUS}"
