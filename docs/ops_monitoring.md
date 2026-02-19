# Ops Monitoring Checklist

Минимальный набор наблюдаемости для MVP в Render.

## 1) Что мониторить

1. HTTP 5xx rate (`/api/*`).
2. Error spikes в webhook-обработке.
3. HTTP 429 rate (проверка, что rate-limit не слишком агрессивный).
4. Latency p95/p99 на `/api/assistant/ask`.
5. Состояние persistent storage через `/api/runtime/diagnostics`.

## 2) Что настроить сразу

1. Render Alerts на:
   - service down / failed deploy;
   - high error rate (если доступно на плане).
2. Внешний uptime-check на:
   - `GET /api/health`
   - `GET /api/runtime/diagnostics`
3. Ночной smoke-job (локально или CI runner):
   ```bash
   python3 scripts/release_smoke.py \
     --base-url https://<your-render-domain> \
     --strict-runtime \
     --require-webhook-mode \
     --require-render-persistent \
     --check-telegram-webhook
   ```

## 3) Минимальные пороги для MVP

1. `5xx` < 1% на интервале 15 минут.
2. `429` < 5% на интервале 15 минут (если выше — пересмотреть лимиты).
3. `assistant ask p95` < 12 сек.
4. `pending_update_count` в Telegram webhook не растёт длительно.

## 4) Реакция на инциденты

1. Проверить последние deploy logs.
2. Проверить `/api/runtime/diagnostics`.
3. Проверить `getWebhookInfo` и `last_error_message`.
4. При проблеме с хранилищем:
   - убедиться, что смонтирован persistent disk;
   - проверить `PERSISTENT_DATA_PATH`/`RENDER_DISK_MOUNT_PATH`;
   - redeploy и повторить smoke-check.
