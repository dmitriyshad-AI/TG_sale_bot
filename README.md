# sales-agent (MVP каркас)

Минимальный каркас для sales-бота (KMIPT + ФОТОН): FastAPI healthcheck, Telegram-бот (echo) и SQLite-логирование.

## Быстрый старт (локально)

1. Создать окружение и установить зависимости:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
2. Создать `.env` из примера и заполнить токен Telegram:
   ```bash
   cp .env.example .env
   # TELEGRAM_BOT_TOKEN=...
   ```
3. Запустить API:
   ```bash
   uvicorn sales_agent.sales_api.main:app --reload
   # health: http://127.0.0.1:8000/api/health
   ```
4. Запустить Telegram-бота (в другом терминале):
   ```bash
   python -m sales_agent.sales_bot.bot
   ```

База `data/sales_agent.db` создаётся автоматически при первом запуске.

## Docker / Compose

```bash
docker compose up --build
```

API будет доступен на `localhost:8000`, бот использует тот же образ.

## Структура

```
/sales_agent
  sales_api/       # FastAPI
  sales_bot/       # Telegram-бот
  sales_core/      # конфиг, БД
data/              # SQLite (игнорируется в git)
tests/             # пока пусто
```

## Настройка окружения

`.env.example`:
```
TELEGRAM_BOT_TOKEN=
OPENAI_API_KEY=
OPENAI_MODEL=gpt-4.1
TALLANTO_API_URL=
TALLANTO_API_KEY=
BRAND_DEFAULT=kmipt
DATABASE_PATH= # опционально
```

## Команды обслуживания

- Инициализация/создание БД выполняется автоматически при старте API или бота.
- Проверка статуса API: `curl http://127.0.0.1:8000/api/health`

## Следующие шаги (по плану)

1. Каталог продуктов + валидатор
2. Поиск/ранжирование продуктов
3. Tallanto клиент и лиды
4. Воронка квалификации в Telegram
