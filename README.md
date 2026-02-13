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
docker compose -f docker-compose.dev.yml up --build
```

API будет доступен на `localhost:8000`, бот использует тот же образ.

Для production-профиля:

```bash
docker compose -f docker-compose.prod.yml up -d --build
```

## Структура

```
/sales_agent
  sales_api/       # FastAPI
  sales_bot/       # Telegram-бот
  sales_core/      # конфиг, БД, каталог, LLM/RAG
catalog/           # products.yaml
knowledge/         # документы базы знаний
scripts/           # служебные скрипты
docs/              # HTML snippets для сайта
data/              # SQLite (игнорируется в git)
tests/             # unit tests
```

## Настройка окружения

`.env.example`:
```
TELEGRAM_BOT_TOKEN=
OPENAI_API_KEY=
OPENAI_MODEL=gpt-4.1
OPENAI_VECTOR_STORE_ID=
TALLANTO_API_URL=
TALLANTO_API_KEY=
TALLANTO_MOCK_MODE=false
CRM_PROVIDER=tallanto
AMO_API_URL=
AMO_ACCESS_TOKEN=
BRAND_DEFAULT=kmipt
ADMIN_USER=
ADMIN_PASS=

# Optional overrides
DATABASE_PATH=
CATALOG_PATH=
KNOWLEDGE_PATH=
VECTOR_STORE_META_PATH=
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
- Проверка knowledge-базы в Telegram:
  - `/kbtest Какие условия возврата?`, или
  - `/kbtest`, затем отправить вопрос отдельным сообщением.
- Генерация deep-link для сайта:
  ```bash
  python3 scripts/generate_deeplink.py --bot-username YOUR_BOT --brand kmipt --page /courses/ege --utm-source google --utm-medium cpc
  ```
  - Готовый HTML-фрагмент: `docs/widget_snippet.html`
  - В `/start` бот распознаёт payload и сохраняет `source/page/utm_*` в `sessions.meta_json`.
  - При превышении лимита 64 символа бот старается сохранить хотя бы категорию страницы (`camp/ege/oge/olymp`), чтобы не терять контекст приветствия.
- Для локальной проверки без CRM включить mock-режим:
  - `TALLANTO_MOCK_MODE=true`
- Выбор CRM-провайдера:
  - `CRM_PROVIDER=tallanto` — текущая рабочая интеграция.
  - `CRM_PROVIDER=amo` — минимальная рабочая AMO-интеграция (`/api/v4/leads` + заметка с контактом).
  - `CRM_PROVIDER=none` — отключить запись лидов/задач в CRM.
- Мини-админка (FastAPI, Basic Auth):
  - Заполнить в `.env`: `ADMIN_USER` и `ADMIN_PASS`.
  - `GET /admin` — HTML dashboard.
  - `GET /admin/ui/leads` — HTML список лидов.
  - `GET /admin/ui/conversations` — HTML список диалогов.
  - `GET /admin/ui/conversations/{user_id}` — HTML история пользователя.
  - `GET /admin/ui/copilot` — HTML форма импорта диалога.
  - `POST /admin/ui/copilot/import` — HTML результат (`summary + draft_reply`).
  - `GET /admin/leads` — последние лиды.
  - `GET /admin/conversations` — последние диалоги.
  - `GET /admin/conversations/{user_id}` — история пользователя.
  - `POST /admin/copilot/import` — импорт WhatsApp `.txt` или Telegram `.json`,
    возврат `summary + customer_profile + draft_reply` (без автоотправки).
    Опционально: `?create_task=true` для создания задачи в Tallanto.
- Если `OPENAI_API_KEY` не задан, бот использует детерминированный fallback для текста рекомендаций.
- Синхронизация knowledge-файлов в OpenAI Vector Store:
  ```bash
  python3 scripts/sync_vector_store.py
  ```
  - Скрипт сохранит `vector_store_id` в `data/vector_store.json`.
  - Повторный запуск идемпотентный: неизменённые файлы будут переиспользованы без повторной загрузки.
  - Режим предпросмотра: `python3 scripts/sync_vector_store.py --dry-run`
  - Очистка устаревших файлов в vector store:
    `python3 scripts/sync_vector_store.py --prune-missing`
    (удаляет как файлы, удаленные локально, так и старые версии после перезагрузки обновленного файла)
  - Можно зафиксировать ID вручную через `OPENAI_VECTOR_STORE_ID`.
- Валидация каталога:
  ```bash
  python3 scripts/validate_catalog.py
  ```
- Проверка свежести каталога (обязательная в CI):
  ```bash
  python3 scripts/check_catalog_freshness.py
  ```
- Автогенерация чернового каталога с публичных страниц kmipt.ru + cdpofoton.ru:
  ```bash
  python3 scripts/build_catalog_draft.py
  python3 scripts/validate_catalog.py --path catalog/products.auto_draft.yaml
  ```
  - Важно: это черновик, перед продом нужна ручная верификация цен/дат/условий.
- Запуск тестов:
  ```bash
  python3 -m unittest discover -s tests -v
  ```
- Запуск pytest (если установлен):
  ```bash
  pytest -q
  ```

- Точечные тесты CLI-утилит:
  ```bash
  python3 -m pytest tests/test_sync_vector_store_script.py tests/test_generate_deeplink_script.py tests/test_catalog_freshness_script.py -q
  ```

## Следующие шаги (по плану)

1. UI-слой для админки (HTML/Jinja) и метрики конверсии
2. Полевая валидация каталога и юридических условий перед продом
