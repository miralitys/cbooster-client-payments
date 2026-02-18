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
   - (опционально) `DB_TABLE_NAME` = `client_records_state`.
5. Deploy.

Сервис поднимает API:
- `GET /api/health`
- `GET /api/records`
- `PUT /api/records`

Таблица в Supabase создается автоматически при первом обращении:
- `client_records_state(id, records, updated_at)`.

## Миграция текущих данных

При первом старте с настроенной БД:
- если в Supabase уже есть данные, они загрузятся в UI;
- если БД пустая, локальные данные браузера будут отправлены в Supabase автоматически.
# cbooster-client-payments
