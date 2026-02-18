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
   - `DATABASE_URL` = строка подключения Supabase;
   - `DB_TABLE_NAME` = `client_records_state`;
   - `DB_MODERATION_TABLE_NAME` = `mini_client_submissions`;
   - `TELEGRAM_BOT_TOKEN` = токен бота (для Mini App);
   - `TELEGRAM_ALLOWED_USER_IDS` = список Telegram user id через запятую (опционально);
   - `TELEGRAM_REQUIRED_CHAT_ID` = id группы, где пользователь должен состоять, чтобы пользоваться Mini App (опционально);
   - `TELEGRAM_INIT_DATA_TTL_SEC` = TTL сессии Mini App в секундах (по умолчанию `86400`).
   - `TELEGRAM_NOTIFY_CHAT_ID` = id чата/группы для уведомлений о новых заявках из Mini App (опционально);
   - `TELEGRAM_NOTIFY_THREAD_ID` = id топика в группе (опционально, только для group topics).
5. Deploy.

Сервис поднимает API:
- `GET /api/health`
- `GET /api/records`
- `PUT /api/records`
- `POST /api/mini/access`
- `POST /api/mini/clients`
- `GET /api/moderation/submissions`
- `POST /api/moderation/submissions/:id/approve`
- `POST /api/moderation/submissions/:id/reject`

Таблица в Supabase создается автоматически при первом обращении:
- `client_records_state(id, records, updated_at)`.
- `mini_client_submissions(id, record, submitted_by, status, submitted_at, reviewed_at, reviewed_by, review_note)`.

## Доступ к страницам

- Главная страница `/` — это Dashboard (overview + таблица заявок на модерацию).
- Страница полной таблицы клиентов находится по адресу `/Client_Payments`.
- Mini App маршруты (`/mini`, `/api/mini/*`) защищаются подписью Telegram `initData`.

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

## Уведомления в группу о новых заявках

Если хотите получать сообщение в Telegram-группу сразу после отправки формы из Mini App:

1. Добавьте в Render:
   - `TELEGRAM_NOTIFY_CHAT_ID=-100...` (id нужной группы/канала).
   - `TELEGRAM_NOTIFY_THREAD_ID=123` (опционально, если используете топики).
2. Убедитесь, что бот состоит в группе и имеет право отправлять сообщения.
3. Redeploy сервиса.

После этого при каждом `POST /api/mini/clients` бот отправит в группу сообщение с `submissionId`, автором заявки и заполненными полями клиента.

## Миграция текущих данных

При первом старте с настроенной БД:
- если в Supabase уже есть данные, они загрузятся в UI;
- если БД пустая, локальные данные браузера будут отправлены в Supabase автоматически.
