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
   - `BASIC_AUTH_USER` = логин для входа;
   - `BASIC_AUTH_PASSWORD` = пароль для входа;
   - `TELEGRAM_BOT_TOKEN` = токен бота (для Mini App);
   - `TELEGRAM_ALLOWED_USER_IDS` = список Telegram user id через запятую (опционально);
   - `TELEGRAM_INIT_DATA_TTL_SEC` = TTL сессии Mini App в секундах (по умолчанию `86400`).
5. Deploy.

Сервис поднимает API:
- `GET /api/health`
- `GET /api/records`
- `PUT /api/records`
- `POST /api/mini/clients`

Таблица в Supabase создается автоматически при первом обращении:
- `client_records_state(id, records, updated_at)`.

## Базовая авторизация (HTTP Basic)

- Если заданы `BASIC_AUTH_USER` и `BASIC_AUTH_PASSWORD`, сайт и API требуют логин/пароль.
- `GET /api/health` остается открытым для health check Render.
- Mini App маршруты (`/mini`, `/api/mini/*`) не используют Basic Auth и защищаются подписью Telegram `initData`.

## Telegram Mini App

1. В BotFather создайте/настройте бота и получите токен.
2. Добавьте `TELEGRAM_BOT_TOKEN` в Render environment.
3. В BotFather настройте кнопку/меню Web App на URL:
   - `https://<ваш-домен>.onrender.com/mini`
4. (Опционально) ограничьте доступ к Mini App, задав `TELEGRAM_ALLOWED_USER_IDS`.
5. Откройте Mini App из Telegram и добавьте клиента.

## Миграция текущих данных

При первом старте с настроенной БД:
- если в Supabase уже есть данные, они загрузятся в UI;
- если БД пустая, локальные данные браузера будут отправлены в Supabase автоматически.
