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
TELEGRAM_WEBHOOK_SECRET=
OPENAI_API_KEY=...
OPENAI_MODEL=gpt-4.1
OPENAI_WEB_FALLBACK_ENABLED=true
OPENAI_WEB_FALLBACK_DOMAIN=kmipt.ru
BRAND_DEFAULT=kmipt
ADMIN_USER=admin
ADMIN_PASS=strong_password
ADMIN_MINIAPP_ENABLED=false
ADMIN_TELEGRAM_IDS=
ADMIN_WEBAPP_URL=
SALES_TONE_PATH=
WEBAPP_DIST_PATH=
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
```

Сборка пользовательского Mini App (если используете `/app`):

```bash
cd webapp
npm install
npm run build
cd ..
```

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
5. Health Check Path: `/api/health`.
6. Запустите deploy и проверьте логи сервиса:
   - `Starting API on 0.0.0.0...`
   - `Starting Telegram bot polling...`

### Важно по эксплуатации

- Не запускайте один и тот же токен одновременно локально и в Render.
- Free Web Service может засыпать при простое. Для стабильного 24/7 лучше paid plan или webhook-архитектура.

### Webhook режим на Render (рекомендуется для single web service)

1. В env Render задайте:
   ```dotenv
   TELEGRAM_MODE=webhook
   TELEGRAM_WEBHOOK_PATH=/telegram/webhook
   TELEGRAM_WEBHOOK_SECRET=<long-random-secret>
   ```
2. После деплоя выставьте webhook:
   ```bash
   curl -s "https://api.telegram.org/bot<RENDER_BOT_TOKEN>/setWebhook" \
     -d "url=https://<your-render-domain>/telegram/webhook" \
     -d "secret_token=<long-random-secret>"
   ```
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

## 11) Tallanto Read-Only API

- Включается только при `TALLANTO_READ_ONLY=1`.
- Endpoints:
  - `GET /api/crm/meta/modules`
  - `GET /api/crm/meta/fields?module=contacts`
  - `GET /api/crm/lookup?module=contacts&field=phone&value=%2B79990000000`
- `lookup` возвращает только обезличенный контекст:
  - `found`, `tags`, `interests`, `last_touch_days`
  - персональные поля (телефоны, адреса, внутренние заметки) не выдаются.
