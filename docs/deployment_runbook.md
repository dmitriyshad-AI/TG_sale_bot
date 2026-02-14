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
OPENAI_API_KEY=...
OPENAI_MODEL=gpt-4.1
BRAND_DEFAULT=kmipt
ADMIN_USER=admin
ADMIN_PASS=strong_password
```

CRM:

1. Tallanto:
   ```dotenv
   CRM_PROVIDER=tallanto
   TALLANTO_API_URL=...
   TALLANTO_API_KEY=...
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
```

Если используете knowledge-base через File Search:

```bash
python3 scripts/sync_vector_store.py --dry-run
python3 scripts/sync_vector_store.py
```

## 4) Запуск

```bash
docker compose -f docker-compose.prod.yml up -d --build
```

Проверка:

```bash
curl http://127.0.0.1:8000/api/health
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
