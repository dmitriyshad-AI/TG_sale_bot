# sales-agent (MVP)

MVP-каркас для sales-бота УНПК МФТИ (kmipt.ru): FastAPI API, Telegram-бот, SQLite-логирование, каталог продуктов и строгая валидация каталога.

Текущая версия Telegram SDK: `python-telegram-bot==21.11.1` (ветка с поддержкой Telegram Business API, `>=21.1`).

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
   python3 scripts/start_api.py --host 127.0.0.1 --port 8000 --reload
   # health: http://127.0.0.1:8000/api/health
   ```
5. Запустить Telegram-бота (в другом терминале):
   ```bash
   python3 -m sales_agent.sales_bot.bot
   ```

6. Запустить пользовательский Mini App (опционально, для фронтенд-разработки):
   ```bash
   cd webapp
   npm install
   npm run dev
   # http://127.0.0.1:5173
   ```

База `data/sales_agent.db` создаётся автоматически при первом запуске.

Сборка Mini App для FastAPI (`/app`):
```bash
cd webapp
npm install
npm run build
# далее FastAPI отдаёт webapp/dist по пути /app
```

Примечание: при запуске через Docker `webapp` собирается автоматически в `Dockerfile`.

## Docker / Compose

```bash
docker compose -f docker-compose.dev.yml up --build
```

API будет доступен на `localhost:8000`, бот использует тот же образ.

Для production-профиля:

```bash
docker compose -f docker-compose.prod.yml up -d --build
```

Пошаговая инструкция внедрения: `docs/deployment_runbook.md`.
Чек-лист ручного UAT: `docs/uat_checklist.md`.
Чек-лист мониторинга и алертов: `docs/ops_monitoring.md`.

## Render (Free plan, polling)

На Render Free у `Background Worker` нет бесплатного тарифа, поэтому для polling используется один `Web Service`.
В этом репозитории Docker-старт по умолчанию запускает:
- FastAPI на `PORT` (Render переменная окружения),
- Telegram-бота в polling-режиме.

Минимальные шаги:
1. Создать отдельного бота для Render в `@BotFather` (другой токен, не локальный).
2. Перед polling удалить webhook для этого токена:
   ```bash
   curl -s "https://api.telegram.org/bot<RENDER_BOT_TOKEN>/deleteWebhook?drop_pending_updates=true"
   ```
3. Создать `Web Service` в Render (Runtime: Docker, Plan: Free).
4. В `Environment` задать минимум:
   - `TELEGRAM_BOT_TOKEN`
   - `OPENAI_API_KEY`
   - `OPENAI_MODEL=gpt-4.1`
   - `BRAND_DEFAULT=kmipt`
   - `CRM_PROVIDER=none`
5. `Health Check Path`: `/api/health`.
6. Deploy и проверить логи: должна появиться строка `Starting Telegram bot polling...`.

Важно:
- Один токен нельзя запускать одновременно локально и в Render.
- Free Web Service может уходить в sleep при простое; для стабильного 24/7 обычно переходят на paid или webhook-архитектуру.
- Если persistent disk не подключен и `/var/data` недоступен, сервис автоматически использует `/tmp`
  для SQLite и metadata vector store (стартует без ручной настройки, но данные будут временными).

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
TELEGRAM_MODE=polling
TELEGRAM_WEBHOOK_PATH=/telegram/webhook
TELEGRAM_WEBHOOK_SECRET=
OPENAI_API_KEY=
OPENAI_MODEL=gpt-4.1
OPENAI_VECTOR_STORE_ID=
OPENAI_WEB_FALLBACK_ENABLED=true
OPENAI_WEB_FALLBACK_DOMAIN=kmipt.ru
ASSISTANT_API_TOKEN=
ASSISTANT_RATE_LIMIT_WINDOW_SECONDS=60
ASSISTANT_RATE_LIMIT_USER_REQUESTS=24
ASSISTANT_RATE_LIMIT_IP_REQUESTS=72
STARTUP_PREFLIGHT_MODE=fail
TALLANTO_API_URL=
TALLANTO_API_KEY=
TALLANTO_API_TOKEN=
TALLANTO_READ_ONLY=0
TALLANTO_DEFAULT_CONTACT_MODULE=
TALLANTO_MOCK_MODE=false
CRM_API_EXPOSED=false
CRM_RATE_LIMIT_WINDOW_SECONDS=300
CRM_RATE_LIMIT_IP_REQUESTS=180
CRM_PROVIDER=none
AMO_API_URL=
AMO_ACCESS_TOKEN=
BRAND_DEFAULT=kmipt
ADMIN_USER=
ADMIN_PASS=
ADMIN_MINIAPP_ENABLED=false
ADMIN_TELEGRAM_IDS=
ADMIN_WEBAPP_URL=
USER_WEBAPP_URL=

# Optional overrides
PERSISTENT_DATA_PATH=
RENDER_DISK_MOUNT_PATH=
DATABASE_PATH=
CATALOG_PATH=
KNOWLEDGE_PATH=
VECTOR_STORE_META_PATH=
WEBAPP_DIST_PATH=
SALES_TONE_PATH=
```

## Тон общения бота

- Базовый профиль тона хранится в `config/sales_tone.yaml`.
- По умолчанию бот использует этот файл для:
  - системных промптов LLM,
  - мягкой пост-обработки ответов,
  - оценки качества текста в логах (`helpfulness/friendliness/pressure`).
- Можно указать другой файл через `SALES_TONE_PATH`.
- Метрики качества добавляются в `messages.meta_json.quality` для исходящих сообщений.

## Режим Telegram: polling vs webhook

- По умолчанию: `TELEGRAM_MODE=polling`.
- Для локальной разработки проще polling (`python3 -m sales_agent.sales_bot.bot`).
- Для Render/Web Service можно использовать webhook:
  1. Установить в env: `TELEGRAM_MODE=webhook`.
  2. Опционально задать `TELEGRAM_WEBHOOK_SECRET` (рекомендуется для проверки заголовка Telegram).
  3. После деплоя выставить webhook:
     ```bash
     curl -s "https://api.telegram.org/bot<RENDER_BOT_TOKEN>/setWebhook" \
       -d "url=https://<your-render-domain>${TELEGRAM_WEBHOOK_PATH}" \
       -d "secret_token=<TELEGRAM_WEBHOOK_SECRET>"
     ```
  4. Проверка:
     ```bash
     curl -s "https://api.telegram.org/bot<RENDER_BOT_TOKEN>/getWebhookInfo"
     ```
- В webhook-режиме endpoint создается по пути `TELEGRAM_WEBHOOK_PATH` (по умолчанию `/telegram/webhook`).
- Webhook endpoint отвечает быстро (`{"ok":true,"queued":true}`), а обработка апдейта идёт через durable SQLite-очередь с retry.

## Команды обслуживания

- Инициализация/создание БД выполняется автоматически при старте API или бота.
- Проверка статуса API: `curl http://127.0.0.1:8000/api/health`
- Runtime-диагностика (без секретов): `curl http://127.0.0.1:8000/api/runtime/diagnostics`
- Проверка user Mini App auth:
  - без Telegram: `curl http://127.0.0.1:8000/api/auth/whoami` -> `{"ok":false,"reason":"not_in_telegram",...}`
  - в Telegram Mini App: передать `initData` в `X-Tg-Init-Data` или `Authorization: tma <initData>`.
- API подбора каталога для Mini App:
  - `GET /api/catalog/search?brand=kmipt&grade=11&goal=ege&subject=math&format=online`
- API ассистента (`POST /api/assistant/ask`) защищен:
  - доступ из Telegram Mini App через `X-Tg-Init-Data`,
  - или сервисный токен `X-Assistant-Token` / `Authorization: Bearer ...` (`ASSISTANT_API_TOKEN`).
  - включены мягкие rate-limit (по Telegram user и IP) с ответом `429` и `Retry-After`.
- Tallanto read-only API (для miniapp/бэкофиса, без записи в CRM):
  - Требует `TALLANTO_READ_ONLY=1`.
  - Дополнительно отключен по умолчанию: `CRM_API_EXPOSED=false`.
  - Для включения нужен `CRM_API_EXPOSED=true` и Basic Auth (`ADMIN_USER`/`ADMIN_PASS`).
  - `GET /api/crm/meta/modules`
  - `GET /api/crm/meta/fields?module=contacts`
  - `GET /api/crm/lookup?module=contacts&field=phone&value=%2B79990000000`
  - Ответ `lookup` всегда обезличен: `found/tags/interests/last_touch_days` (без телефонов и заметок).
- Проверка пользовательского Mini App:
  - `GET /` — статус API и Mini App (`ready`/`build-required`).
  - `GET /app` — собранный пользовательский Mini App или инструкция по сборке.
- Предстартовый аудит окружения:
  ```bash
  python3 scripts/preflight_audit.py
  python3 scripts/preflight_audit.py --json
  ```
- Smoke-проверка уже запущенного API (локально/Render):
  ```bash
  python3 scripts/release_smoke.py --base-url http://127.0.0.1:8000
  # строгий режим по runtime warning:
  python3 scripts/release_smoke.py --base-url https://<your-render-domain> --strict-runtime
  # production-проверка Render + webhook + persistent disk:
  python3 scripts/release_smoke.py \
    --base-url https://<your-render-domain> \
    --strict-runtime \
    --require-webhook-mode \
    --require-render-persistent \
    --check-telegram-webhook
  ```
  - Регулярный remote smoke можно включить через GitHub Actions workflow `Release Smoke`:
    1. Добавьте GitHub Secret `RELEASE_SMOKE_BASE_URL=https://<your-domain>`.
    2. Опционально добавьте `TELEGRAM_BOT_TOKEN` (для проверки webhook в Telegram API).
    3. Workflow запускается вручную и по cron (каждые 30 минут).
- Резервное копирование SQLite:
  ```bash
  # Создать backup (по умолчанию gzip + ротация последних 14 файлов)
  python3 scripts/backup_sqlite.py --db-path data/sales_agent.db --output-dir data/backups

  # Восстановить из backup
  python3 scripts/restore_sqlite.py --backup-path data/backups/sales-agent-<timestamp>.db.gz --db-path data/sales_agent.db --force
  ```
- Легкий нагрузочный smoke (без внешних библиотек):
  ```bash
  # health / catalog / assistant
  python3 scripts/load_smoke.py --base-url http://127.0.0.1:8000 --target health --requests 60 --concurrency 10
  python3 scripts/load_smoke.py --base-url http://127.0.0.1:8000 --target catalog --requests 60 --concurrency 10
  python3 scripts/load_smoke.py --base-url http://127.0.0.1:8000 --target assistant --assistant-token <ASSISTANT_API_TOKEN> --requests 30 --concurrency 6
  ```
- Режим стартового preflight:
  - `STARTUP_PREFLIGHT_MODE=off` — выключить блокировку старта.
  - `STARTUP_PREFLIGHT_MODE=fail` — блокировать только при критических ошибках (рекомендуется).
  - `STARTUP_PREFLIGHT_MODE=strict` — блокировать и при предупреждениях.
- Основной сценарий в Telegram:
  - `/start` запускает воронку квалификации с inline-кнопками (класс → цель → предмет → формат).
  - После подбора 2-3 продуктов бот формирует ответ через LLM (или через fallback без LLM) и предлагает оставить контакт.
- Проверка создания лида в Telegram:
  - `/leadtest +79991234567` (одной командой), или
  - `/leadtest`, затем отправить номер отдельным сообщением.
- Открытие admin miniapp из Telegram:
  - `/adminapp` (команда доступна только ID из `ADMIN_TELEGRAM_IDS`).
- Открытие клиентского miniapp из Telegram:
  - `/app` (доступно всем пользователям, нужен `USER_WEBAPP_URL`).
- Проверка knowledge-базы в Telegram:
  - `/kbtest Какие условия возврата?`, или
  - `/kbtest`, затем отправить вопрос отдельным сообщением.
- Для вопросов о конкретных программах (`"Что ты знаешь про IT лагерь?"`) бот сначала пробует File Search.
  Если данных не хватает, может автоматически сделать web fallback по домену `OPENAI_WEB_FALLBACK_DOMAIN`.
- Генерация deep-link для сайта:
  ```bash
  python3 scripts/generate_deeplink.py --bot-username YOUR_BOT --brand kmipt --page /courses/ege --utm-source google --utm-medium cpc
  ```
  - Готовый HTML-фрагмент: `docs/widget_snippet.html`
  - В `/start` бот распознаёт payload и сохраняет `source/page/utm_*` в `sessions.meta_json`.
  - При превышении лимита 64 символа бот старается сохранить хотя бы категорию страницы (`camp/ege/oge/olymp`), чтобы не терять контекст приветствия.
- Для локальной проверки без CRM включить mock-режим:
  - `TALLANTO_MOCK_MODE=true`
- Для Render с SQLite храните БД и metadata на persistent disk:
  - по умолчанию используется `/var/data`, если путь доступен для записи;
  - если mount path другой, задайте `PERSISTENT_DATA_PATH=<mount-path>`;
  - также поддерживается auto-detect через `RENDER_DISK_MOUNT_PATH`.
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
- Admin Mini App (Telegram WebApp, без Basic Auth):
  - Включить в env:
    - `ADMIN_MINIAPP_ENABLED=true`
    - `ADMIN_TELEGRAM_IDS=123456789,987654321`
    - `ADMIN_WEBAPP_URL=https://<your-domain>/admin/miniapp`
    - `USER_WEBAPP_URL=https://<your-domain>/app` (клиентский miniapp, команда `/app`)
  - UI:
    - `GET /admin/miniapp`
    - стиль интерфейса: glass/liquid в голубых тонах, адаптивен для mobile/desktop.
  - API (используют Telegram `initData` в заголовке `X-Telegram-Init-Data`):
    - `GET /admin/miniapp/api/me`
    - `GET /admin/miniapp/api/leads`
    - `GET /admin/miniapp/api/conversations`
    - `GET /admin/miniapp/api/conversations/{user_id}`
- Если `OPENAI_API_KEY` не задан, бот использует детерминированный fallback для текста рекомендаций.
- Синхронизация knowledge-файлов в OpenAI Vector Store:
  ```bash
  python3 scripts/sync_vector_store.py
  ```
  - Скрипт сохранит `vector_store_id` в `data/vector_store.json`.
  - Повторный запуск идемпотентный: неизменённые файлы будут переиспользованы без повторной загрузки.
  - Для Render/production обязательно продублируйте ID в env: `OPENAI_VECTOR_STORE_ID=vs_...`
    (локальный `data/vector_store.json` в облаке может быть недолговечным).
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
- Полная синхронизация каталога KMIPT с сайта (sitemap + страницы курсов):
  ```bash
  python3 scripts/sync_kmipt_catalog.py
  ```
  - Проверка соответствия текущего каталога живому сайту:
    ```bash
    python3 scripts/sync_kmipt_catalog.py --check-catalog catalog/products.yaml
    ```
- Автогенерация чернового каталога с публичных страниц kmipt.ru:
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
- Локальная quality-проверка (как в CI):
  ```bash
  pytest --cov=sales_agent --cov=scripts --cov-report=term-missing --cov-fail-under=85 -q
  cd webapp && npm ci && npm run typecheck && npm run build
  ```

- Точечные тесты CLI-утилит:
  ```bash
  python3 -m pytest tests/test_sync_vector_store_script.py tests/test_generate_deeplink_script.py tests/test_catalog_freshness_script.py -q
  ```

## Следующие шаги (по плану)

1. UI-слой для админки (HTML/Jinja) и метрики конверсии
2. Полевая валидация каталога и юридических условий перед продом
