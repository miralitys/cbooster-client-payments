# Credit Booster Client Payments

Мини-приложение для учёта платежей клиентов:
- таблица со всеми полями по клиентам;
- добавление нового клиента через форму;
- поиск по `Client name` и `Company Name`;
- локальный кэш в `localStorage` + синхронизация в PostgreSQL (Supabase) через API.

## Локальный запуск

```bash
cd "/Users/ramisyaparov/Desktop/Project/CBooster Client Payments"
npm install
cp .env.example .env
```

В `.env` заполните `DATABASE_URL` от Supabase и запустите:

```bash
npm start
```

После запуска откройте: `http://localhost:10000`

Если `DATABASE_URL` не задан, интерфейс продолжит работать только с `localStorage`.

Если вы подключаетесь к локальному Postgres без TLS, задайте `PGSSLMODE=disable`.

## Деплой на Render + Supabase

1. Создайте проект в Supabase.
2. Возьмите строку подключения Postgres (`DATABASE_URL`) в `Supabase -> Project Settings -> Database`.
3. В Render создайте `Web Service` из этого репозитория (можно по `render.yaml`).
4. В переменных Render добавьте:
   - `WEB_AUTH_USERNAME` = логин для входа на веб-сайт;
   - `WEB_AUTH_PASSWORD_HASH` = bcrypt-хеш пароля для входа на веб-сайт;
   - `WEB_AUTH_PASSWORD` = plaintext-пароль только для локальной разработки (в production отключен);
   - `WEB_AUTH_OWNER_USERNAME` = username владельца с полными правами (по умолчанию `owner`);
   - `WEB_AUTH_USERS_JSON` = JSON-массив пользователей с ролями/департаментами (опционально);
   - `WEB_AUTH_SESSION_SECRET` = длинный случайный секрет для подписи cookie-сессии;
   - `WEB_AUTH_SESSION_TTL_SEC` = TTL сессии в секундах (по умолчанию `43200` = 12 часов);
   - `WEB_AUTH_COOKIE_SECURE` = `true`/`false` (опционально, принудительный secure-флаг cookie);
   - `WEB_AUTH_TOTP_ISSUER` = issuer-имя для Authenticator-приложений (по умолчанию `Credit Booster`);
   - `WEB_AUTH_TOTP_PERIOD_SEC` = шаг TOTP в секундах (по умолчанию `30`);
   - `WEB_AUTH_TOTP_WINDOW_STEPS` = допустимое окно шагов TOTP (по умолчанию `1`, т.е. ±1 шаг);
   - `TRUST_PROXY` = настройка Express `trust proxy` (по умолчанию `1` для Render/Cloudflare);
   - `WEB_AUTH_TOTP_ISSUER` = issuer-имя для Authenticator-приложений (по умолчанию `Credit Booster`);
   - `WEB_AUTH_TOTP_PERIOD_SEC` = шаг TOTP в секундах (по умолчанию `30`);
   - `WEB_AUTH_TOTP_WINDOW_STEPS` = допустимое окно шагов TOTP (по умолчанию `1`, т.е. ±1 шаг);
   - `RATE_LIMIT_ENABLED` = `true`/`false` (по умолчанию `true`, защита от brute-force и burst-запросов);
   - `RATE_LIMIT_STORE_MODE` = `postgres` или `memory` (по умолчанию `postgres`, если задан `DATABASE_URL`);
   - `RATE_LIMIT_STORE_MAX_KEYS` = лимит in-memory ключей rate-limit (по умолчанию `60000`);
   - `RATE_LIMIT_DB_ERROR_COOLDOWN_MS` = cooldown при ошибках shared rate-limit store (по умолчанию `30000`);
   - `QUICKBOOKS_CLIENT_ID` = QuickBooks OAuth Client ID;
   - `QUICKBOOKS_CLIENT_SECRET` = QuickBooks OAuth Client Secret;
   - `QUICKBOOKS_REFRESH_TOKEN` = QuickBooks OAuth Refresh Token;
   - `QUICKBOOKS_REFRESH_TOKEN_ENCRYPTION_KEY` = 32-byte ключ для шифрования refresh token в БД (hex-64, base64, или plain 32-char);
   - `QUICKBOOKS_REFRESH_TOKEN_ENCRYPTION_KEY_ID` = key id для зашифрованного payload (по умолчанию `default`);
   - `QUICKBOOKS_REALM_ID` = QuickBooks Company Realm ID;
   - `QUICKBOOKS_REDIRECT_URI` = Redirect URI (опционально, но рекомендовано хранить рядом с OAuth-настройками);
   - `QUICKBOOKS_API_BASE_URL` = API base URL (по умолчанию `https://quickbooks.api.intuit.com`);
   - `OPENAI_API_KEY` = API key для LLM-ответов онлайн-помощника (опционально; без ключа работает rule-based fallback);
   - `OPENAI_MODEL` = модель OpenAI для ассистента (по умолчанию `gpt-4.1-mini`);
   - `OPENAI_API_BASE_URL` = base URL OpenAI API (по умолчанию `https://api.openai.com`);
   - `OPENAI_ASSISTANT_TIMEOUT_MS` = timeout запроса к OpenAI в ms (по умолчанию `15000`);
   - `OPENAI_ASSISTANT_MAX_OUTPUT_TOKENS` = лимит токенов ответа (по умолчанию `420`);
   - `ASSISTANT_REVIEW_PII_MODE` = режим хранения вопросов/ответов в очереди ревью (`minimal` по умолчанию; варианты: `minimal`, `redact`, `full`);
   - `ASSISTANT_REVIEW_RETENTION_SWEEP_ENABLED` = `true`/`false` (по умолчанию `true`, автоочистка старых записей review-очереди);
   - `ASSISTANT_REVIEW_RETENTION_DAYS` = срок хранения записей review-очереди в днях (по умолчанию `90`);
   - `ASSISTANT_REVIEW_RETENTION_SWEEP_INTERVAL_MS` = интервал sweep review-очереди в ms (по умолчанию `14400000`, т.е. 4 часа);
   - `ASSISTANT_REVIEW_RETENTION_SWEEP_BATCH_LIMIT` = сколько старых review-записей удалять за один sweep (по умолчанию `500`);
   - `ELEVENLABS_API_KEY` = API key ElevenLabs для озвучки ответов ассистента голосом;
   - `ELEVENLABS_VOICE_ID` = voice id в ElevenLabs (по умолчанию `ARyC2bwXA7I797b7vxmB`);
   - `ELEVENLABS_MODEL_ID` = модель ElevenLabs для TTS (по умолчанию `eleven_multilingual_v2`);
   - `ELEVENLABS_API_BASE_URL` = base URL ElevenLabs API (по умолчанию `https://api.elevenlabs.io`);
   - `ELEVENLABS_OUTPUT_FORMAT` = формат аудио ответа (по умолчанию `mp3_44100_128`);
   - `ELEVENLABS_TTS_TIMEOUT_MS` = timeout TTS-запроса к ElevenLabs в ms (по умолчанию `15000`);
   - `DATABASE_URL` = строка подключения Supabase;
   - `PGSSLMODE` = `disable` для локального Postgres без SSL, иначе SSL включается со строгой валидацией сертификата;
   - `PGSSLROOTCERT` = абсолютный путь к CA-файлу (опционально, если нужен кастомный CA);
   - `PGSSL_CA_CERT` = CA сертификат в PEM (строкой, `\n` поддерживается) — опционально;
   - `PGSSL_CA_CERT_BASE64` = тот же CA в base64 — опционально;
   - `DB_TABLE_NAME` = `client_records_state`;
   - `DB_MODERATION_TABLE_NAME` = `mini_client_submissions`;
   - `DB_MODERATION_FILES_TABLE_NAME` = `mini_submission_files`;
   - `DB_RATE_LIMIT_BUCKETS_TABLE_NAME` = `web_rate_limit_buckets`;
   - `DB_LOGIN_FAILURES_TABLE_NAME` = `web_login_failure_state`;
   - `TELEGRAM_BOT_TOKEN` = токен бота (для Mini App);
   - `TELEGRAM_ALLOWED_USER_IDS` = список Telegram user id через запятую (опционально);
   - `TELEGRAM_REQUIRED_CHAT_ID` = id группы, где пользователь должен состоять, чтобы пользоваться Mini App (опционально);
   - `TELEGRAM_INIT_DATA_TTL_SEC` = TTL сессии Mini App в секундах (по умолчанию `86400`).
   - `TELEGRAM_NOTIFY_CHAT_ID` = id чата/группы для уведомлений о новых заявках из Mini App (опционально);
   - `TELEGRAM_NOTIFY_THREAD_ID` = id топика в группе (опционально, только для group topics).
5. Deploy.

### PostgreSQL TLS (строгий режим)

- По умолчанию при `PGSSLMODE != disable` используется SSL с проверкой сертификата сервера (`rejectUnauthorized=true`).
- Insecure-режим с `rejectUnauthorized=false` не используется.
- Если провайдер требует доверенный CA, передайте его через `PGSSLROOTCERT` или `PGSSL_CA_CERT`/`PGSSL_CA_CERT_BASE64`.

### QuickBooks refresh token encryption

- Refresh token в таблице `quickbooks_auth_state.refresh_token` сохраняется в формате `enc:v1:...` (AES-256-GCM).
- Для QuickBooks в production переменная `QUICKBOOKS_REFRESH_TOKEN_ENCRYPTION_KEY` обязательна.
- Legacy plaintext токены читаются для обратной совместимости, но при следующем `persist` записываются уже в encrypted-формате.
- Не храните `QUICKBOOKS_REFRESH_TOKEN_ENCRYPTION_KEY` в репозитории или логах.

Сервис поднимает API:
- `GET /api/auth/session`
- `GET /api/auth/access-model`
- `GET /api/auth/users`
- `POST /api/auth/users`
- `GET /api/quickbooks/payments/recent?from=YYYY-MM-DD&to=YYYY-MM-DD`
- `POST /api/quickbooks/payments/recent/sync` (body: `{ from, to, fullSync? }`, создает async sync job)
- `GET /api/quickbooks/payments/recent/sync-jobs/:jobId`
- `GET /api/ghl/client-managers`
- `POST /api/ghl/client-managers/refresh` (body: `{ refresh: "incremental" | "full" }`)
- `POST /api/ghl/client-contracts/archive` (ingest auth only via `x-ghl-contract-archive-token` header or `Authorization: Bearer <token>`)
- `GET /api/ghl/client-basic-note?clientName=...` (cache read-only)
- `POST /api/ghl/client-basic-note/refresh` (body: `{ clientName, writtenOff? }`)
- `GET /api/health` (анонимно возвращает минимальный `{ ok: true }`; подробный DB-статус только при `HEALTH_CHECK_API_KEY` через `x-health-check-key` или `Authorization: Bearer ...`)
- `GET /api/records`
- `PUT /api/records`
- `POST /api/assistant/chat`
- `POST /api/assistant/tts`
- `POST /api/mini/access`
- `POST /api/mini/clients`
- `GET /api/moderation/submissions`
- `GET /api/moderation/submissions/:id/files`
- `GET /api/moderation/submissions/:id/files/:fileId`
- `POST /api/moderation/submissions/:id/approve`
- `POST /api/moderation/submissions/:id/reject`

Для cookie-сессий state-changing API (`POST/PUT` на `/api/*`) требуют CSRF-заголовок `X-CSRF-Token` (значение из cookie `cbooster_auth_csrf`).

Таблица в Supabase создается автоматически при первом обращении:
- `client_records_state(id, records, updated_at)`.
- `mini_client_submissions(id, record, mini_data, submitted_by, status, submitted_at, reviewed_at, reviewed_by, review_note)`.
- `mini_submission_files(id, submission_id, file_name, mime_type, size_bytes, content, created_at)`.

## Доступ к страницам

- Весь веб-интерфейс защищен авторизацией через `/login`.
- Для пользователей с включенным 2FA на `/login` дополнительно требуется 6-значный код из Authenticator-приложения (TOTP).
- После входа доступны:
  - главная страница `/` (Dashboard: overview + таблица заявок на модерацию);
  - страница полной таблицы клиентов `/Client_Payments`;
  - отдельная тестовая страница QuickBooks `/quickbooks-payments`.
  - страница модели прав `/access-control` (включая регистрацию пользователей для Owner через кнопку `Add New User`).
- Выход: только `POST /logout` (`GET /logout` возвращает `405 Method Not Allowed`).
- Mini App маршруты (`/mini`, `/api/mini/*`) защищаются подписью Telegram `initData`.

## Global Access Control (RBAC)

- Главный аккаунт (`Owner`) задается через `WEB_AUTH_OWNER_USERNAME` и имеет полный доступ ко всем разделам.
- Дополнительные аккаунты можно задать через `WEB_AUTH_USERS_JSON`:
  - `username`, `passwordHash` (bcrypt), `displayName` (опционально),
  - `department` (`accounting`, `client_service`, `sales`, `collection`),
  - `role` (`department_head`, `middle_manager`, `manager`),
  - `teamUsernames` (опционально, массив или строка через запятую; для `middle_manager` в `client_service`),
  - `isOwner` (`true/false`, опционально),
  - `totpSecret` (опционально, Base32-секрет для Authenticator/TOTP),
  - `totpEnabled` (`true/false`, опционально; при `true` вход требует код TOTP).
- В production plaintext-пароли в конфиге (`WEB_AUTH_PASSWORD`, `WEB_AUTH_USERS_JSON[].password`) запрещены: используйте только bcrypt-хеши.
- Для генерации TOTP-секрета и `otpauth://` URI:
  ```bash
  cd "/Users/ramisyaparov/Desktop/Project/CBooster Client Payments"
  npm run auth:totp-generate -- --username owner
  ```
- Быстрая миграция `WEB_AUTH_USERS_JSON` (чтобы заменить все `password` на `passwordHash`):
  1. Скопируйте текущее значение `WEB_AUTH_USERS_JSON` из Render.
  2. Выполните:
     ```bash
     cd "/Users/ramisyaparov/Desktop/Project/CBooster Client Payments"
     pbpaste | npm run auth:hash-users-json -- --stdin --compact > /tmp/users.hashed.json
     cat /tmp/users.hashed.json | pbcopy
     ```
  3. Вставьте результат обратно в Render в переменную `WEB_AUTH_USERS_JSON`.
  4. Удалите (или оставьте пустой) `WEB_AUTH_PASSWORD` в production.
  5. Сделайте redeploy.
  6. Если `WEB_AUTH_USERS_JSON` у вас не используется, миграция этого поля не требуется.
- Департаменты и роли (на английском):
  - `Accounting Department`: `Department Head`, `Manager`
  - `Client Service Department`: `Department Head`, `Middle Manager`, `Manager`
  - `Sales Department`: `Department Head`, `Manager`
  - `Collection Department`: `Department Head`, `Manager`
- Страница `/access-control` показывает текущую модель доступа, роли и назначенных пользователей.
- На странице `/access-control` (Owner only) доступна кнопка `Add New User` для создания нового пользователя и назначения департамента/роли.
- В блоке `Current Users` на `/access-control` можно нажать на имя пользователя и открыть окно редактирования (username, пароль, роль, департамент, команда).
- В формах создания/редактирования пользователя на `/access-control` доступно включение `2FA (Authenticator)`, генерация `TOTP Secret` и QR-preview для сканирования в приложении Authenticator.
- В форме создания пользователя на `/access-control` поля `Username` и `Password` необязательны: можно завести сотрудника только по `Display Name + Department + Role`.
- Если `Username/Password` не переданы, система создаст временные технические credentials автоматически (только для внутренней записи пользователя).
- Для текущей структуры также автоматически добавляются пользователи без обязательного email/password:
  - `Client Service Department`: `Nataly Regush` (`Department Head`), `Marina Urvanceva` (`Middle Manager`, username: `marynau@creditbooster.com`), `Natasha Grek` (`Middle Manager`), managers: `Anastasiia Lopatina` (username: `anastasiial@creditbooster.com`), `Vadim Kozorezov`, `Liudmyla Sidachenko`, `Ihor Syrovatka`, `Arina Alekhina`, `Arslan Utiaganov` (username: `arslanu@creditbooster.com`), `Ruanna Ordukhanova-Aslanyan`, `Kristina Troinova`.
  - `Accounting Department`: `Alla Havrysh` (`Department Head`, username: `allah@urbansa.us`), `Nataliia Poliakova` (`Manager`).
  - `Sales Department`: `Maryna Shuliatytska` (`Department Head`, username: `garbarmarina13@gmail.com`), managers: `Vlad Burnis`, `Yurii Kis`, `Kateryna Shuliatytska` (`username: katyash957@gmail.com`).
  - `Collection Department`: `Dmitriy Polanski` (`Department Head`).
- Новый пользователь добавляется в текущую runtime-директорию авторизации и доступен сразу после создания.
- Правила доступа к клиентам:
  - `Accounting Department`: `Department Head` и `Manager` видят всех клиентов и могут редактировать/создавать.
  - `Client Service Department`: `Department Head` видит всех и может редактировать; `Middle Manager` видит только своих клиентов и клиентов своей команды (`teamUsernames`), редактировать не может; `Manager` видит только своих клиентов, редактировать не может.
  - `Sales Department`: только просмотр клиентов, закрепленных за пользователем (`closedBy`), без редактирования.
  - `Collection Department`: только просмотр всех клиентов, без редактирования.

## QuickBooks тест (отдельно)

- Откройте `/quickbooks-payments`.
- Страница показывает транзакции из QuickBooks за период `2026-01-01` -> текущая дата.
- Колонки: `Client Name`, `Phone`, `Email`, `Payment Amount`, `Payment Date`.
- При открытии страницы читаются только сохраненные данные из базы (без запроса в QuickBooks).
- Фоновый авто-sync работает каждый час только в окне `08:00-22:00` по времени `America/Chicago` (вне этого времени автоматических обновлений нет).
- `GET /api/quickbooks/payments/recent` теперь строго read-only (только кеш).
- Для ручного обновления нажмите `Refresh`: UI вызывает `POST /api/quickbooks/payments/recent/sync`, получает `jobId`, опрашивает `GET /api/quickbooks/payments/recent/sync-jobs/:jobId` и после завершения job перечитывает кеш через `GET /api/quickbooks/payments/recent`.
- Кнопка `Total Refresh` делает тот же async flow, но с `fullSync=true` за весь диапазон `2026-01-01` -> текущая дата.
- Для поиска введите имя клиента в поле `Search by client` (поиск выполняется по подстроке).
- Чекбокс `Only refunds` показывает только транзакции с возвратами (`RefundReceipt`).
- Интеграция строго read-only: мы только читаем данные из QuickBooks и не отправляем туда изменения.
- Если `Payment.TotalAmt = 0`, но у записи есть linked `Deposit`, система интерпретирует сумму депозита как полученные деньги (берется модуль суммы linked deposit line).
- Если `Payment.TotalAmt = 0`, но у записи есть linked `CreditMemo`, система интерпретирует это как списание долга и показывает сумму как отрицательную (write-off).
- `RefundReceipt` также включается в выдачу и отображается как отрицательная сумма (`-amount`), т.к. это возврат денег клиенту.
- Нулевые транзакции (`amount = 0`) в таблице не отображаются.

## Telegram Mini App

1. В BotFather создайте/настройте бота и получите токен.
2. Добавьте `TELEGRAM_BOT_TOKEN` в Render environment.
3. В BotFather настройте кнопку/меню Web App на URL:
   - `https://<ваш-домен>.onrender.com/mini`
4. (Опционально) ограничьте доступ к Mini App:
   - `TELEGRAM_ALLOWED_USER_IDS` для явного списка user id;
   - `TELEGRAM_REQUIRED_CHAT_ID=-100...`, чтобы пользоваться Mini App могли только участники конкретной группы.
5. Убедитесь, что бот добавлен в эту группу (иначе проверка членства работать не будет).
6. Откройте Mini App из Telegram и добавьте клиента (заявка попадет в очередь модерации).
7. Откройте `https://<ваш-домен>.onrender.com/`, проверьте заявку в таблице модерации и откройте карточку клиента.
8. Поставьте галочку "Добавить в общую базу данных" и нажмите "Применить" для одобрения.
9. После approve клиент появится на странице `https://<ваш-домен>.onrender.com/Client_Payments`.

## Вложения в Mini App

- В Mini App можно прикрепить до 10 файлов за одну отправку.
- Разрешены любые файлы, кроме скриптов и HTML (`.js`, `.ts`, `.py`, `.sh`, `.html`, и т.д.).
- Ограничение: до 10 MB на файл и до 40 MB суммарно на заявку.
- На этапе премодерации вложения доступны в карточке заявки: просмотр (для изображений/PDF) и скачивание.

## Mobile Audit Gate Policy (CI)

- Workflow: `.github/workflows/mobile-security-audit.yml`
- Gate script: `scripts/mobile-audit-gate.js`
- По умолчанию CI падает, если:
  - `critical > 0`, или
  - `high > 0`.
- Временный override только для `high`:
  - `MOBILE_AUDIT_ALLOW_HIGH=true`
  - при override скрипт пишет явный warning в лог.
- `critical` уязвимости override не обходит.
- Если `npm audit --json` не удалось распарсить, gate работает в fail-closed режиме (job падает).

## Дополнительные поля Mini App

В Mini App добавлены отдельные поля, которые сохраняются только в модерационных заявках (`mini_client_submissions.mini_data`) и не попадают в основную таблицу клиентов (`client_records_state`):

- `leadSource` (Источник лида)
- `ssn` (SSN)
- `clientPhoneNumber` (Телефон клиента)
- `futurePayment` (Future payment)
- `identityIq` (IdentityIQ)
- `clientEmailAddress` (Email клиента)

## Уведомления в группу о новых заявках

Если хотите получать сообщение в Telegram-группу сразу после отправки формы из Mini App:

1. Добавьте в Render:
   - `TELEGRAM_NOTIFY_CHAT_ID=-100...` (id нужной группы/канала).
   - `TELEGRAM_NOTIFY_THREAD_ID=123` (опционально, если используете топики).
2. Убедитесь, что бот состоит в группе и имеет право отправлять сообщения.
3. Redeploy сервиса.

После этого при каждом `POST /api/mini/clients` бот отправит в группу сообщение с автором заявки и заполненными полями клиента.

## Миграция текущих данных

При первом старте с настроенной БД:
- если в Supabase уже есть данные, они загрузятся в UI;
- если БД пустая, локальные данные браузера будут отправлены в Supabase автоматически.

## Records v2 Cutover Runbook

Флаги миграции (`.env` / Render):
- `DUAL_WRITE_V2` — при legacy-write зеркалит запись в `client_records_v2` (подготовительный этап).
- `DUAL_READ_COMPARE` — при legacy-read асинхронно сравнивает `legacy` и `v2`, логирует mismatch.
- `READ_V2` — чтение `/api/records` из `client_records_v2` с controlled fallback на `legacy` при ошибке v2.
- `WRITE_V2` — запись `/api/records` source-of-truth в `client_records_v2`.
- `LEGACY_MIRROR` — при `WRITE_V2=true` включает best-effort зеркальную запись `records` в `client_records_state`.

Рекомендуемая последовательность cutover:
1. Backfill `client_records_v2` и сверка:
   ```bash
   npm run records:v2:backfill
   npm run records:v2:verify
   ```
2. Включить `DUAL_WRITE_V2=true`, оставить `READ_V2=false`, проверить логи/диагностику.
3. Включить `DUAL_READ_COMPARE=true`, убедиться, что mismatch нет или они объяснимы.
4. Переключить чтение: `READ_V2=true` (fallback на legacy останется автоматически).
5. Переключить запись: `WRITE_V2=true`.
6. На переходный период держать `LEGACY_MIRROR=true` (если нужны legacy-совместимые readers).
7. После стабилизации отключить `DUAL_WRITE_V2` и `DUAL_READ_COMPARE`; при необходимости выключить `LEGACY_MIRROR`.

Быстрый rollback только флагами:
1. Если проблемы с чтением v2: `READ_V2=false`.
2. Если проблемы с записью v2: `WRITE_V2=false`.
3. Если mirror создает нагрузку/ошибки: `LEGACY_MIRROR=false`.
4. Сохранить `DUAL_READ_COMPARE=true` для диагностики после отката (опционально).

Важно:
- сервис не выполняет auto-drop legacy таблиц;
- переключение выполняется только через feature flags.

### Manual Cleanup Plan (SQL, не выполняется автоматически)

Запускать только после стабилизации проде и подтвержденного cutover.

1) Проверить, что legacy и v2 синхронны:
```sql
SELECT COUNT(*) AS legacy_count
FROM jsonb_array_elements((SELECT records FROM public.client_records_state WHERE id = 1));

SELECT COUNT(*) AS v2_count
FROM public.client_records_v2
WHERE source_state_row_id = 1;
```

2) Зафиксировать backup legacy row:
```sql
CREATE TABLE IF NOT EXISTS public.client_records_state_backup AS
SELECT *
FROM public.client_records_state
WHERE id = 1;
```

3) После финального подтверждения (ручное решение команды):
```sql
-- Опция A: оставить таблицу, но очистить JSON payload:
UPDATE public.client_records_state
SET records = '[]'::jsonb
WHERE id = 1;

-- Опция B: удалить legacy таблицу полностью (только если точно не нужна):
-- DROP TABLE public.client_records_state;
```

## iPhone приложение

Добавлен отдельный мобильный клиент на Expo:  
`/Users/ramisyaparov/Desktop/Project/CBooster Client Payments/mobile-app`

Быстрый старт:

```bash
cd "/Users/ramisyaparov/Desktop/Project/CBooster Client Payments/mobile-app"
npm install
cp .env.example .env
# в .env укажите EXPO_PUBLIC_APP_URL=https://<ваш-домен>.onrender.com
npm run start
```

Далее откройте `Expo Go` на iPhone и отсканируйте QR из терминала.
