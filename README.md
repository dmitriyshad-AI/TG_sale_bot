# sales-agent (MVP)

MVP-каркас для sales-бота (KMIPT + ФОТОН): FastAPI API, Telegram-бот, SQLite-логирование, каталог продуктов и строгая валидация каталога.

## Быстрый старт (локально)

1. Создать окружение и установить runtime-зависимости:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
2. Для разработки и тестов установить доп. зависимости:
   ```bash
   pip install -r requirements-dev.txt
   ```
3. Создать `.env` из примера и заполнить токен Telegram:
   ```bash
   cp .env.example .env
   # TELEGRAM_BOT_TOKEN=...
   ```
4. Запустить API:
   ```bash
   uvicorn sales_agent.sales_api.main:app --reload
   # health: http://127.0.0.1:8000/api/health
   ```
5. Запустить Telegram-бота (в другом терминале):
   ```bash
   python3 -m sales_agent.sales_bot.bot
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
  sales_core/      # конфиг, БД, каталог
catalog/           # products.yaml
scripts/           # служебные скрипты
data/              # SQLite (игнорируется в git)
tests/             # unit tests
```

## Настройка окружения

`.env.example`:
```
TELEGRAM_BOT_TOKEN=
OPENAI_API_KEY=
OPENAI_MODEL=gpt-4.1
TALLANTO_API_URL=
TALLANTO_API_KEY=
TALLANTO_MOCK_MODE=false
BRAND_DEFAULT=kmipt

# Optional overrides
DATABASE_PATH=
CATALOG_PATH=
```

## Команды обслуживания

- Инициализация/создание БД выполняется автоматически при старте API или бота.
- Проверка статуса API: `curl http://127.0.0.1:8000/api/health`
- Основной сценарий в Telegram:
  - `/start` запускает воронку квалификации с inline-кнопками (класс → цель → предмет → формат).
  - После подбора 2-3 продуктов бот формирует ответ через LLM (или через fallback без LLM) и предлагает оставить контакт.
- Проверка создания лида в Telegram:
  - `/leadtest +79991234567` (одной командой), или
  - `/leadtest`, затем отправить номер отдельным сообщением.
- Для локальной проверки без CRM включить mock-режим:
  - `TALLANTO_MOCK_MODE=true`
- Если `OPENAI_API_KEY` не задан, бот использует детерминированный fallback для текста рекомендаций.
- Валидация каталога:
  ```bash
  python3 scripts/validate_catalog.py
  ```
- Запуск тестов:
  ```bash
  python3 -m unittest discover -s tests -v
  ```
- Запуск pytest (если установлен):
  ```bash
  pytest -q
  ```

## Следующие шаги (по плану)

1. LLM-ответы через OpenAI Responses API
2. RAG через File Search (knowledge base)
3. Сайт → Telegram deep-links + UTM
4. Мини-админка лидов/диалогов
5. Copilot для старых диалогов
