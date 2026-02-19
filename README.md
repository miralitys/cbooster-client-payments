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

## Деплой на Render + Supabase

1. Создайте проект в Supabase.
2. Возьмите строку подключения Postgres (`DATABASE_URL`) в `Supabase -> Project Settings -> Database`.
3. В Render создайте `Web Service` из этого репозитория (можно по `render.yaml`).
4. В переменных Render добавьте:
   - `WEB_AUTH_USERNAME` = логин для входа на веб-сайт;
   - `WEB_AUTH_PASSWORD` = пароль для входа на веб-сайт;
   - `WEB_AUTH_OWNER_USERNAME` = username владельца с полными правами (по умолчанию `ramisi@creditbooster.com`);
   - `WEB_AUTH_USERS_JSON` = JSON-массив пользователей с ролями/департаментами (опционально);
   - `WEB_AUTH_SESSION_SECRET` = длинный случайный секрет для подписи cookie-сессии;
   - `WEB_AUTH_SESSION_TTL_SEC` = TTL сессии в секундах (по умолчанию `43200` = 12 часов);
   - `WEB_AUTH_COOKIE_SECURE` = `true`/`false` (опционально, принудительный secure-флаг cookie);
   - `QUICKBOOKS_CLIENT_ID` = QuickBooks OAuth Client ID;
   - `QUICKBOOKS_CLIENT_SECRET` = QuickBooks OAuth Client Secret;
   - `QUICKBOOKS_REFRESH_TOKEN` = QuickBooks OAuth Refresh Token;
   - `QUICKBOOKS_REALM_ID` = QuickBooks Company Realm ID;
   - `QUICKBOOKS_REDIRECT_URI` = Redirect URI (опционально, но рекомендовано хранить рядом с OAuth-настройками);
   - `QUICKBOOKS_API_BASE_URL` = API base URL (по умолчанию `https://quickbooks.api.intuit.com`);
   - `DATABASE_URL` = строка подключения Supabase;
   - `DB_TABLE_NAME` = `client_records_state`;
   - `DB_MODERATION_TABLE_NAME` = `mini_client_submissions`;
   - `DB_MODERATION_FILES_TABLE_NAME` = `mini_submission_files`;
   - `TELEGRAM_BOT_TOKEN` = токен бота (для Mini App);
   - `TELEGRAM_ALLOWED_USER_IDS` = список Telegram user id через запятую (опционально);
   - `TELEGRAM_REQUIRED_CHAT_ID` = id группы, где пользователь должен состоять, чтобы пользоваться Mini App (опционально);
   - `TELEGRAM_INIT_DATA_TTL_SEC` = TTL сессии Mini App в секундах (по умолчанию `86400`).
   - `TELEGRAM_NOTIFY_CHAT_ID` = id чата/группы для уведомлений о новых заявках из Mini App (опционально);
   - `TELEGRAM_NOTIFY_THREAD_ID` = id топика в группе (опционально, только для group topics).
5. Deploy.

Сервис поднимает API:
- `GET /api/auth/session`
- `GET /api/auth/access-model`
- `GET /api/auth/users`
- `POST /api/auth/users`
- `GET /api/quickbooks/payments/recent?from=YYYY-MM-DD&to=YYYY-MM-DD[&sync=1][&fullSync=1]`
- `GET /api/health`
- `GET /api/records`
- `PUT /api/records`
- `POST /api/mini/access`
- `POST /api/mini/clients`
- `GET /api/moderation/submissions`
- `GET /api/moderation/submissions/:id/files`
- `GET /api/moderation/submissions/:id/files/:fileId`
- `POST /api/moderation/submissions/:id/approve`
- `POST /api/moderation/submissions/:id/reject`

Таблица в Supabase создается автоматически при первом обращении:
- `client_records_state(id, records, updated_at)`.
- `mini_client_submissions(id, record, mini_data, submitted_by, status, submitted_at, reviewed_at, reviewed_by, review_note)`.
- `mini_submission_files(id, submission_id, file_name, mime_type, size_bytes, content, created_at)`.

## Доступ к страницам

- Весь веб-интерфейс защищен авторизацией через `/login`.
- После входа доступны:
  - главная страница `/` (Dashboard: overview + таблица заявок на модерацию);
  - страница полной таблицы клиентов `/Client_Payments`;
  - отдельная тестовая страница QuickBooks `/quickbooks-payments`.
  - страница модели прав `/access-control` (включая регистрацию пользователей для Owner через кнопку `Add New User`).
- Выход: `/logout`.
- Mini App маршруты (`/mini`, `/api/mini/*`) защищаются подписью Telegram `initData`.

## Global Access Control (RBAC)

- Главный аккаунт (`Owner`) задается через `WEB_AUTH_OWNER_USERNAME` и имеет полный доступ ко всем разделам.
- Дополнительные аккаунты можно задать через `WEB_AUTH_USERS_JSON`:
  - `username`, `password`, `displayName` (опционально),
  - `department` (`accounting`, `client_service`, `sales`, `collection`),
  - `role` (`department_head`, `middle_manager`, `manager`),
  - `teamUsernames` (опционально, массив или строка через запятую; для `middle_manager` в `client_service`),
  - `isOwner` (`true/false`, опционально).
- Департаменты и роли (на английском):
  - `Accounting Department`: `Department Head`, `Manager`
  - `Client Service Department`: `Department Head`, `Middle Manager`, `Manager`
  - `Sales Department`: `Department Head`, `Manager`
  - `Collection Department`: `Department Head`, `Manager`
- Страница `/access-control` показывает текущую модель доступа, роли и назначенных пользователей.
- На странице `/access-control` (Owner only) доступна кнопка `Add New User` для создания нового пользователя и назначения департамента/роли.
- В форме создания пользователя на `/access-control` поля `Username` и `Password` необязательны: можно завести сотрудника только по `Display Name + Department + Role`.
- Если `Username/Password` не переданы, система создаст временные технические credentials автоматически (только для внутренней записи пользователя).
- Для текущей структуры также автоматически добавляются пользователи без обязательного email/password:
  - `Client Service Department`: `Nataly Regush` (`Department Head`), `Anastasiia Lopatina`, `Arslan Utiaganov`, `Liudmyla Sydachenko`, `Dmitrii Kabanov`, `Arina Alekhina` (`Manager`).
  - `Accounting Department`: `Alla Havrysh` (`Department Head`).
  - `Sales Department`: `Maryna Shuliatytska` (`Department Head`, username: `garbarmarina13@gmail.com`).
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
- Для ручного обновления нажмите `Refresh`: выполняется sync с QuickBooks только от последней сохраненной даты и добавляются новые транзакции.
- Кнопка `Total Refresh` выполняет полный sync за весь диапазон `2026-01-01` -> текущая дата и пересчитывает кеш целиком.
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
