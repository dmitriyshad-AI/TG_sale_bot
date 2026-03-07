# Deployment Runbook (Staging -> Production)

Ниже короткая и практичная инструкция для запуска проекта в боевом режиме через Docker Compose.

## 1) Подготовка сервера

1. Установить Docker + Docker Compose.
2. Клонировать репозиторий:
   ```bash
   git clone <your-repo-url> TG_sale_bot
   cd TG_sale_bot
   ```
3. Создать файл окружения:
   ```bash
   cp .env.example .env
   ```

## 2) Заполнить `.env`

Обязательные поля для запуска:

```dotenv
TELEGRAM_BOT_TOKEN=...
TELEGRAM_MODE=polling
TELEGRAM_WEBHOOK_PATH=/telegram/webhook
TELEGRAM_WEBHOOK_SECRET=   # опционально, но рекомендуется при TELEGRAM_MODE=webhook
OPENAI_API_KEY=...
OPENAI_MODEL=gpt-4.1
OPENAI_WEB_FALLBACK_ENABLED=true
OPENAI_WEB_FALLBACK_DOMAIN=kmipt.ru
ASSISTANT_API_TOKEN=
ASSISTANT_RATE_LIMIT_WINDOW_SECONDS=60
ASSISTANT_RATE_LIMIT_USER_REQUESTS=24
ASSISTANT_RATE_LIMIT_IP_REQUESTS=72
STARTUP_PREFLIGHT_MODE=fail
APP_ENV=development
RATE_LIMIT_BACKEND=memory
REDIS_URL=
ADMIN_UI_CSRF_ENABLED=false
CRM_PROVIDER=none
CRM_API_EXPOSED=false
CRM_RATE_LIMIT_WINDOW_SECONDS=300
CRM_RATE_LIMIT_IP_REQUESTS=180
BRAND_DEFAULT=kmipt
ADMIN_USER=admin
ADMIN_PASS=strong_password
ADMIN_MINIAPP_ENABLED=false
ADMIN_TELEGRAM_IDS=
ADMIN_WEBAPP_URL=
USER_WEBAPP_URL=
ENABLE_MANGO_AUTO_INGEST=false
ENABLE_BUSINESS_INBOX=false
ENABLE_CALL_COPILOT=false
ENABLE_TALLANTO_ENRICHMENT=false
ENABLE_DIRECTOR_AGENT=false
ENABLE_LEAD_RADAR=false
ENABLE_FAQ_LAB=false
LEAD_RADAR_SCHEDULER_ENABLED=true
LEAD_RADAR_INTERVAL_SECONDS=3600
LEAD_RADAR_NO_REPLY_HOURS=6
LEAD_RADAR_CALL_NO_NEXT_STEP_HOURS=24
LEAD_RADAR_STALE_WARM_DAYS=7
LEAD_RADAR_MAX_ITEMS_PER_RUN=50
LEAD_RADAR_THREAD_COOLDOWN_HOURS=24
LEAD_RADAR_DAILY_CAP_PER_THREAD=2
FAQ_LAB_SCHEDULER_ENABLED=true
FAQ_LAB_INTERVAL_SECONDS=21600
FAQ_LAB_WINDOW_DAYS=90
FAQ_LAB_MIN_QUESTION_COUNT=2
FAQ_LAB_MAX_ITEMS_PER_RUN=120
MANGO_API_BASE_URL=
MANGO_API_TOKEN=
MANGO_CALLS_PATH=/calls
MANGO_WEBHOOK_PATH=/integrations/mango/webhook
MANGO_WEBHOOK_SECRET=
MANGO_POLLING_ENABLED=false
MANGO_POLL_INTERVAL_SECONDS=300
MANGO_POLL_LIMIT_PER_RUN=50
MANGO_POLL_RETRY_ATTEMPTS=3
MANGO_POLL_RETRY_BACKOFF_SECONDS=2
MANGO_RETRY_FAILED_LIMIT_PER_RUN=25
MANGO_CALL_RECORDING_TTL_HOURS=48
SALES_TONE_PATH=
WEBAPP_DIST_PATH=
PERSISTENT_DATA_PATH=
RENDER_DISK_MOUNT_PATH=
```

CRM:

1. Tallanto:
   ```dotenv
   CRM_PROVIDER=tallanto
   TALLANTO_API_URL=...
   TALLANTO_API_KEY=...
   TALLANTO_API_TOKEN=...   # предпочтительно отдельный токен для read-only
   TALLANTO_READ_ONLY=1     # обязательно literal "1" для /api/crm/*
   TALLANTO_DEFAULT_CONTACT_MODULE=contacts
   ```
2. AMO:
   ```dotenv
   CRM_PROVIDER=amo
   AMO_API_URL=...
   AMO_ACCESS_TOKEN=...
   ```
3. Без CRM на пилоте:
   ```dotenv
   CRM_PROVIDER=none
   ```

## 3) Предстартовая проверка

```bash
docker compose -f docker-compose.prod.yml config
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q
python3 scripts/validate_catalog.py
python3 scripts/preflight_audit.py
python3 scripts/db_migrations_status.py
```

Смысл `STARTUP_PREFLIGHT_MODE`:
- `off` — не блокировать старт по preflight.
- `fail` — блокировать старт только при критических ошибках.
- `strict` — блокировать старт и при warning.

Сборка пользовательского Mini App (если запускаете без Docker):

```bash
cd webapp
npm install
npm run build
cd ..
```

Примечание: в Docker-сборке проекта miniapp собирается автоматически в `Dockerfile`.

Если используете knowledge-base через File Search:

```bash
python3 scripts/sync_vector_store.py --dry-run
python3 scripts/sync_vector_store.py
```

После синхронизации обязательно скопируйте `vector_store_id` в env:

```dotenv
OPENAI_VECTOR_STORE_ID=vs_...
```

Для Render это критично: локальный файл `data/vector_store.json` не должен быть единственным источником ID.

## 4) Запуск

```bash
docker compose -f docker-compose.prod.yml up -d --build
```

Проверка:

```bash
curl http://127.0.0.1:8000/api/health
curl http://127.0.0.1:8000/
curl http://127.0.0.1:8000/app
curl http://127.0.0.1:8000/api/runtime/diagnostics
python3 scripts/release_smoke.py --base-url http://127.0.0.1:8000
docker compose -f docker-compose.prod.yml ps
docker compose -f docker-compose.prod.yml logs --tail=100 api
docker compose -f docker-compose.prod.yml logs --tail=100 bot
```

## 5) Smoke test после запуска

1. В Telegram отправить `/start`.
2. Пройти короткую квалификацию до подбора продуктов.
3. Отправить `/kbtest Какие условия оплаты?`.
4. Отправить `/leadtest +79991234567` (если CRM настроена).
5. Проверить админ API:
   - `GET /admin/leads`
   - `GET /admin/conversations`
6. Если включен Mango auto-ingest:
   - `GET /admin/calls/mango/events`
   - `POST /admin/calls/mango/poll`
   - `POST /admin/calls/mango/retry-failed`
7. Если включен FAQ Lab:
   - `GET /admin/faq-lab`
   - `POST /admin/faq-lab/run`
   - `GET /admin/ui/faq-lab`

## 6) Обновление версии

```bash
git pull
docker compose -f docker-compose.prod.yml up -d --build
```

## 7) Быстрый откат

1. Переключиться на предыдущий commit/tag.
2. Пересобрать контейнеры:
   ```bash
   docker compose -f docker-compose.prod.yml up -d --build
   ```

`data/` вынесена в volume-монтирование, поэтому SQLite и метаданные vector store сохраняются между релизами.

## 8) Render Free (один Web Service, без Worker)

На бесплатном плане Render `Background Worker` недоступен. Для polling-режима используйте только `Web Service`.

В проекте уже есть `start.sh`:
1. `TELEGRAM_MODE=polling` (по умолчанию): поднимает API + polling-бот.
2. `TELEGRAM_MODE=webhook`: поднимает только API, Telegram обновления приходят в webhook endpoint.

### Шаги на Render

1. Создайте отдельного Telegram-бота для Render (отдельный токен).
2. Удалите webhook для этого токена (иначе polling не получит апдейты):
   ```bash
   curl -s "https://api.telegram.org/bot<RENDER_BOT_TOKEN>/deleteWebhook?drop_pending_updates=true"
   ```
3. Создайте `Web Service`:
   - Runtime: `Docker`
   - Plan: `Free`
4. Добавьте env-переменные:
   ```dotenv
   TELEGRAM_BOT_TOKEN=...
   OPENAI_API_KEY=...
   OPENAI_MODEL=gpt-4.1
   BRAND_DEFAULT=kmipt
   CRM_PROVIDER=none
   ```
   Если persistent disk на Render не подключен, сервис теперь автоматически использует `/tmp`
   для SQLite и метаданных vector store (работает, но данные будут сбрасываться после redeploy/restart).
   Если disk смонтирован не в `/var/data`, задайте `PERSISTENT_DATA_PATH=<mount-path>`
   (или используйте `RENDER_DISK_MOUNT_PATH`, если он уже задан в окружении Render).
5. Health Check Path: `/api/health`.
6. Запустите deploy и проверьте логи сервиса:
   - `Starting API on 0.0.0.0...`
   - `Starting Telegram bot polling...`

### Persistent Disk на Render: полный чек-лист

1. Откройте ваш Render `Web Service` -> вкладка `Disks`.
2. Нажмите `Add Disk`.
3. Выберите параметры:
   - Name: `data` (любое понятное имя);
   - Mount path: `/var/data` (рекомендуемый путь для этого проекта);
   - Size: минимум `1 GB` для MVP.
4. Сохраните диск и дождитесь, пока Render применит изменения.
5. В `Environment` проверьте переменные:
   - `PERSISTENT_DATA_PATH=/var/data` (рекомендуется явно задать даже при стандартном mount path).
   - Если mount path другой, укажите ваш путь:
     - `PERSISTENT_DATA_PATH=<your-mount-path>`, или
     - `RENDER_DISK_MOUNT_PATH=<your-mount-path>`.
6. Убедитесь, что не переопределены конфликтующие пути:
   - `DATABASE_PATH` должен быть внутри mount path (или пустой);
   - `VECTOR_STORE_META_PATH` должен быть внутри mount path (или пустой).
7. Нажмите `Manual Deploy` -> `Deploy latest commit`.
8. После деплоя проверьте runtime-диагностику:
   ```bash
   curl -s "https://<your-render-domain>/api/runtime/diagnostics"
   ```
   Должно быть:
   - `runtime.running_on_render = true`
   - `runtime.persistent_data_root != "/tmp"`
   - `runtime.database_on_persistent_storage = true`
   - `runtime.vector_meta_on_persistent_storage = true`
9. Прогоните автоматическую post-deploy проверку:
   ```bash
   python3 scripts/release_smoke.py \
     --base-url https://<your-render-domain> \
     --strict-runtime \
     --require-render-persistent
   ```
10. Проверка на устойчивость данных:
   - отправьте пару сообщений боту;
   - выполните `Manual Deploy` (или restart);
   - убедитесь в админке/БД, что история и сессии сохранились.

### Резервные копии SQLite на Render (рекомендуется даже с Persistent Disk)

1. Создайте папку backup-файлов на persistent disk (если нужно):
   ```bash
   mkdir -p /var/data/backups
   ```
2. Снимите backup вручную:
   ```bash
   python3 scripts/backup_sqlite.py \
     --db-path /var/data/sales_agent.db \
     --output-dir /var/data/backups \
     --prefix render-sales-agent \
     --keep-last 30
   ```
3. Проверка восстановления (в staging path):
   ```bash
   python3 scripts/restore_sqlite.py \
     --backup-path /var/data/backups/render-sales-agent-<timestamp>.db.gz \
     --db-path /var/data/sales_agent.restore.db
   ```
4. Если нужно восстановить боевую БД:
   ```bash
   python3 scripts/restore_sqlite.py \
     --backup-path /var/data/backups/render-sales-agent-<timestamp>.db.gz \
     --db-path /var/data/sales_agent.db \
     --force
   ```
5. После восстановления выполните smoke:
   ```bash
python3 scripts/release_smoke.py \
  --base-url https://<your-render-domain> \
  --strict-runtime \
  --require-webhook-mode \
  --require-render-persistent \
  --check-mango-runtime \
  --check-revenue-runtime
   ```

Примечание: для регулярного контроля можно включить GitHub Actions workflow `Release Smoke`:
- Secret `RELEASE_SMOKE_BASE_URL=https://<your-render-domain>`
- Optional Secret `TELEGRAM_BOT_TOKEN` (для проверки `getWebhookInfo`)
- Optional Secrets для Telegram-алерта при падении smoke:
  - `RELEASE_SMOKE_ALERT_TG_BOT_TOKEN`
  - `RELEASE_SMOKE_ALERT_TG_CHAT_ID`
- cron каждые 30 минут + ручной запуск.

### Offline Mango smoke (локально и в CI)

Для проверки Mango контура без реального Mango API:

```bash
python3 scripts/mango_offline_smoke.py
```

Скрипт проверяет:
1. webhook ingest + signature verify;
2. дедупликацию webhook событий;
3. ручной poll endpoint (`/admin/calls/mango/poll`);
4. повторный прогон failed событий (`/admin/calls/mango/retry-failed`);
5. Mango runtime-метрики в `/api/runtime/diagnostics`.

### Важно по эксплуатации

- Не запускайте один и тот же токен одновременно локально и в Render.
- Free Web Service может засыпать при простое. Для стабильного 24/7 лучше paid plan или webhook-архитектура.
- Для smoke-нагрузки перед релизом используйте:
  ```bash
  python3 scripts/load_smoke.py --base-url https://<your-render-domain> --target health --requests 60 --concurrency 10
  python3 scripts/load_smoke.py --base-url https://<your-render-domain> --target catalog --requests 60 --concurrency 10
  python3 scripts/load_smoke.py --base-url https://<your-render-domain> --target assistant --assistant-token <ASSISTANT_API_TOKEN> --requests 30 --concurrency 6
  ```

### Webhook режим на Render (рекомендуется для single web service)

1. В env Render задайте:
   ```dotenv
   TELEGRAM_MODE=webhook
   TELEGRAM_WEBHOOK_PATH=/telegram/webhook
   TELEGRAM_WEBHOOK_SECRET=<long-random-secret>  # опционально, но рекомендуется
   ```
2. После деплоя выставьте webhook:
   ```bash
   curl -s "https://api.telegram.org/bot<RENDER_BOT_TOKEN>/setWebhook" \
     -d "url=https://<your-render-domain>/telegram/webhook" \
     -d "secret_token=<long-random-secret>"
   ```
   Если не задаете `TELEGRAM_WEBHOOK_SECRET`, можно вызвать `setWebhook` только с параметром `url`.
3. Проверьте состояние:
   ```bash
   curl -s "https://api.telegram.org/bot<RENDER_BOT_TOKEN>/getWebhookInfo"
   ```
   Ожидаемый ответ от вашего backend на входящий webhook: `{"ok":true,"queued":true}`.
   Обработка апдейтов идет через SQLite-очередь с retry, поэтому кратковременный сбой обработчика не теряет сообщения.
4. Если нужно вернуться к polling:
   - `TELEGRAM_MODE=polling`
   - удалить webhook:
     ```bash
     curl -s "https://api.telegram.org/bot<RENDER_BOT_TOKEN>/deleteWebhook?drop_pending_updates=true"
     ```

## 9) Admin Mini App (опционально)

1. В env включите miniapp:
   ```dotenv
   ADMIN_MINIAPP_ENABLED=true
   ADMIN_TELEGRAM_IDS=123456789,987654321
   ADMIN_WEBAPP_URL=https://<your-domain>/admin/miniapp
   ```
2. Перезапустите сервис.
3. В Telegram от админ-аккаунта выполните `/adminapp`.
4. Бот отправит кнопку открытия miniapp.
5. Miniapp API защищен:
   - Telegram WebApp `initData` (проверка подписи),
   - allowlist `ADMIN_TELEGRAM_IDS`.

## 10) User Mini App API (для webapp `/app`)

- `GET /api/auth/whoami`
  - в браузере без Telegram: `{"ok":false,"reason":"not_in_telegram"}`
  - в Telegram Mini App: передавать `initData` в `X-Tg-Init-Data` или `Authorization: tma <initData>`
- `GET /api/catalog/search?brand=kmipt&grade=11&goal=ege&subject=math&format=online`
  - возвращает top-3 программ с `why_match`, `price_text`, `next_start_text`, `usp`
- Клиентский запуск из Telegram:
  - задайте `USER_WEBAPP_URL=https://<your-domain>/app`
  - пользователь открывает miniapp командой `/app`

## 11) Tallanto Read-Only API

- Включается только при `TALLANTO_READ_ONLY=1`.
- Endpoints:
  - `GET /api/crm/meta/modules`
  - `GET /api/crm/meta/fields?module=contacts`
  - `GET /api/crm/lookup?module=contacts&field=phone&value=%2B79990000000`
- Требуется Basic Auth (`ADMIN_USER` / `ADMIN_PASS`) и `CRM_API_EXPOSED=true`.
- `lookup` возвращает только обезличенный контекст:
  - `found`, `tags`, `interests`, `last_touch_days`
  - персональные поля (телефоны, адреса, внутренние заметки) не выдаются.
