# UAT Checklist (Client + Admin + Manager)

Ниже короткий чек-лист ручного приёмочного теста перед пилотом/боем.

## 1) Client Flow (Telegram + Mini App)

1. Открыть бота и отправить `/start`.
2. Пройти подбор курса до экрана рекомендаций.
3. Проверить, что кнопки и навигация работают без “залипаний”.
4. Открыть `/app` и отправить вопрос в чат “Гид”.
5. Проверить, что ответ пришёл, форматирование читаемое, нет дублей сообщений.
6. Нажать “Связаться с менеджером” и убедиться в корректном переходе.
7. Проверить fallback-сценарий: при временной ошибке пользователь видит понятное сообщение.

## 2) Admin Flow

1. Открыть `/admin` (или admin miniapp, если включен).
2. Проверить список лидов и список диалогов.
3. Открыть карточку диалога пользователя.
4. Убедиться, что виден контекст последних сообщений.
5. Проверить, что защищённые endpoints требуют авторизацию.

## 3) Webhook + Runtime

1. Проверить webhook:
   ```bash
   curl -s "https://api.telegram.org/bot<RENDER_BOT_TOKEN>/getWebhookInfo"
   ```
2. Проверить runtime diagnostics:
   ```bash
   curl -s "https://<your-render-domain>/api/runtime/diagnostics"
   ```
3. Проверить post-deploy smoke:
   ```bash
   python3 scripts/release_smoke.py \
     --base-url https://<your-render-domain> \
     --strict-runtime \
     --require-webhook-mode \
     --require-render-persistent \
     --check-telegram-webhook
   ```

## 4) Acceptance Criteria

1. Нет критических ошибок 5xx в API.
2. Нет падений webhook и массовых 429.
3. Данные диалогов сохраняются после redeploy.
4. Ответы ассистента релевантны, без “выдуманных фактов” о продуктах.
5. Менеджер может подхватить диалог без потери контекста.
