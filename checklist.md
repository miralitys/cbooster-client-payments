# Checklist: Prompts For Development Agent

This checklist contains ready-to-use prompts for the development agent.
Use them in order from top to bottom.

---

- [ ] **1. Scoring tests (P0)**

```text
Ты агент разработки в репозитории /Users/ramisyaparov/Desktop/Project/CBooster Client Payments.

Задача: усилить тесты для /Users/ramisyaparov/Desktop/Project/CBooster Client Payments/webapp/src/features/client-score/domain/scoring.ts.
Сфокусируйся на границах: задержки 3/4/29/30/59/60/89/90 дней, bonus/recovery, open milestones, clamp score (0..110), display score округление, month-end и timezone cases.
Используй текущий стек vitest, без snapshot.
Не меняй продуктовый код, если это не критично для тестируемости; если находишь баг — зафиксируй failing test и опиши bug.

Что сделать:
1) Добавь тесты в /Users/ramisyaparov/Desktop/Project/CBooster Client Payments/webapp/src/features/client-score/domain/scoring.test.ts.
2) Сделай тесты детерминированными (фиксированные даты).
3) Запусти:
   cd "/Users/ramisyaparov/Desktop/Project/CBooster Client Payments"
   npm --prefix webapp run test -- src/features/client-score/domain/scoring.test.ts
   npm --prefix webapp run test

Формат отчета:
1) Test plan
2) Added tests (paths)
3) Run results
4) Remaining gaps
```

- [ ] **2. useClientPayments hook tests (P0)**

```text
Ты агент разработки в репозитории /Users/ramisyaparov/Desktop/Project/CBooster Client Payments.

Задача: покрыть тестами хук /Users/ramisyaparov/Desktop/Project/CBooster Client Payments/webapp/src/features/client-payments/hooks/useClientPayments.ts.
Критические сценарии: debounce save, retry/backoff, PATCH->PUT fallback, 409 conflict, cleanup timers on unmount, beforeunload warning, assistant open-client event.
Используй vitest и легкие моки; без флейков, с fake timers.
Не меняй бизнес-логику без крайней необходимости.

Что сделать:
1) Создай тестовый файл для хука (в той же фиче).
2) Замокай API-модуль и таймеры.
3) Проверь ветки success/error/retry/give-up.
4) Запусти:
   cd "/Users/ramisyaparov/Desktop/Project/CBooster Client Payments"
   npm --prefix webapp run test

Формат отчета:
1) Test plan
2) Added tests (paths)
3) Моки/фикстуры
4) Run results
5) Remaining gaps
```

- [ ] **3. fetcher API tests (P0)**

```text
Ты агент разработки в репозитории /Users/ramisyaparov/Desktop/Project/CBooster Client Payments.

Задача: добавить тесты для /Users/ramisyaparov/Desktop/Project/CBooster Client Payments/webapp/src/shared/api/fetcher.ts.
Проверь: timeout, abort, network_error, 401 redirect, обработка не-JSON ответа, CSRF header attach только для mutating методов.

Требования:
1) Используй vitest, мокай global fetch, window.location.assign, document.cookie, AbortController поведение.
2) Тесты должны быть детерминированы, без сети.
3) Не менять продуктовый код без необходимости.

Запуск:
cd "/Users/ramisyaparov/Desktop/Project/CBooster Client Payments"
npm --prefix webapp run test -- src/shared/api
npm --prefix webapp run test

Отчет:
1) Added tests (paths)
2) Что проверено
3) Run results
4) Remaining gaps
```

- [ ] **4. /api/records integration tests (P0)**

```text
Ты агент разработки в репозитории /Users/ramisyaparov/Desktop/Project/CBooster Client Payments.

Задача: добавить минимальные интеграционные тесты API для /Users/ramisyaparov/Desktop/Project/CBooster Client Payments/server.js на сценарии /api/records.
Покрыть: PUT/PATCH happy path, конфликт 409 по expectedUpdatedAt, ошибка валидации payload, auth/csrf guard (если включен в текущей архитектуре).
Используй node:test и моки/стабы зависимостей, без реальной БД/сети.

Требования:
1) Если сервер не экспортирует тестопригодный entrypoint, сделай минимальный безопасный рефакторинг только для тестируемости.
2) Не добавляй тяжелые зависимости без необходимости.
3) Запусти backend тесты.

Команды:
cd "/Users/ramisyaparov/Desktop/Project/CBooster Client Payments"
npm run test:backend

Отчет:
1) Какие интеграционные тесты добавлены
2) Как изолированы внешние зависимости
3) Run results
4) Какие риски еще не закрыты
```

- [ ] **5. Enable webapp coverage**

```text
Ты агент разработки в репозитории /Users/ramisyaparov/Desktop/Project/CBooster Client Payments.

Задача: включить coverage для vitest в webapp.
Сейчас команда npm --prefix webapp run test -- --coverage падает из-за отсутствия @vitest/coverage-v8.

Что сделать:
1) Добавь devDependency @vitest/coverage-v8 в /Users/ramisyaparov/Desktop/Project/CBooster Client Payments/webapp/package.json.
2) При необходимости добавь минимальный coverage config (без усложнения).
3) Убедись, что команда работает:
   cd "/Users/ramisyaparov/Desktop/Project/CBooster Client Payments"
   npm --prefix webapp run test -- --coverage

Отчет:
1) Измененные файлы
2) Итог команды coverage
3) Если есть блокеры — точная причина
```

- [ ] **6. Add backend tests to CI**

```text
Ты агент разработки в репозитории /Users/ramisyaparov/Desktop/Project/CBooster Client Payments.

Задача: добавить запуск backend тестов в CI workflow.
Файл: /Users/ramisyaparov/Desktop/Project/CBooster Client Payments/.github/workflows/quality-gates.yml.

Что сделать:
1) Добавь шаг после установки зависимостей: npm run test:backend.
2) Не ломай текущие шаги webapp lint/test/build.
3) Убедись локально, что команды проходят:
   cd "/Users/ramisyaparov/Desktop/Project/CBooster Client Payments"
   npm run test:backend
   npm --prefix webapp run test

Отчет:
1) Что изменено в workflow
2) Локальные результаты
3) Возможные риски в CI
```

- [ ] **7. Extend client-records-v2-utils tests (P1)**

```text
Ты агент разработки в репозитории /Users/ramisyaparov/Desktop/Project/CBooster Client Payments.

Задача: поднять покрытие для /Users/ramisyaparov/Desktop/Project/CBooster Client Payments/client-records-v2-utils.js.
Добавь тесты на:
- resolveSchemaName/resolveTableName (валидные/невалидные имена),
- buildRecordsV2TableRefsFromEnv defaults/override,
- buildSafeIndexName (длина >63),
- normalizeIsoTimestamp invalid values,
- normalizeSourceStateRowId edge cases,
- ensureClientRecordsV2Schema через mock queryable (проверка SQL-вызовов, без реальной БД).

Редактируй: /Users/ramisyaparov/Desktop/Project/CBooster Client Payments/tests/client-records-v2-utils.test.js.

Запуск:
cd "/Users/ramisyaparov/Desktop/Project/CBooster Client Payments"
node --test tests/client-records-v2-utils.test.js
npm run test:backend

Отчет:
1) Added tests
2) Coverage effect (если измеримо)
3) Remaining gaps
```

- [ ] **8. recordsPatch feature-flag tests (P1)**

```text
Ты агент разработки в репозитории /Users/ramisyaparov/Desktop/Project/CBooster Client Payments.

Задача: покрыть ветки feature-flag логики в /Users/ramisyaparov/Desktop/Project/CBooster Client Payments/webapp/src/features/client-payments/domain/recordsPatch.ts.
Особенно: resolveRecordsPatchEnabled + parseBooleanLike + readSessionPatchFlag.
Проверь комбинации:
- VITE_RECORDS_PATCH выключен/включен,
- featureFlags RECORDS_PATCH/records_patch/recordsPatch,
- string/number/boolean значения.

Редактируй тесты: /Users/ramisyaparov/Desktop/Project/CBooster Client Payments/webapp/src/features/client-payments/domain/recordsPatch.test.ts.

Запуск:
cd "/Users/ramisyaparov/Desktop/Project/CBooster Client Payments"
npm --prefix webapp run test -- src/features/client-payments/domain/recordsPatch.test.ts
npm --prefix webapp run test

Отчет:
1) Added tests
2) Какие ветки закрыты
3) Remaining gaps
```

- [ ] **9. calculations.ts additional tests (P1)**

```text
Ты агент разработки в репозитории /Users/ramisyaparov/Desktop/Project/CBooster Client Payments.

Задача: дополнить тесты для /Users/ramisyaparov/Desktop/Project/CBooster Client Payments/webapp/src/features/client-payments/domain/calculations.ts.
Добавь кейсы на:
- normalizeRecord/normalizeRecords defaults и sanitation,
- matchesStatusFilter по всем status и overdueRange,
- getClosedByOptions dedupe + case-insensitive sorting,
- formatDateTime invalid/valid values,
- getOverdueRanges контракт.

Редактируй: /Users/ramisyaparov/Desktop/Project/CBooster Client Payments/webapp/src/features/client-payments/domain/calculations.test.ts.

Запуск:
cd "/Users/ramisyaparov/Desktop/Project/CBooster Client Payments"
npm --prefix webapp run test -- src/features/client-payments/domain/calculations.test.ts
npm --prefix webapp run test

Отчет:
1) Added tests
2) Что покрыто дополнительно
3) Remaining gaps
```

- [ ] **10. export.ts security tests (P1)**

```text
Ты агент разработки в репозитории /Users/ramisyaparov/Desktop/Project/CBooster Client Payments.

Задача: добавить тесты для export-безопасности в /Users/ramisyaparov/Desktop/Project/CBooster Client Payments/webapp/src/features/client-payments/domain/export.ts.
Покрыть:
- sanitizeSpreadsheetCell для = + - @,
- escapeHtml для спецсимволов,
- корректное форматирование money/date/status в formatCell,
- защиту от formula injection в export HTML.

Создай/обнови тесты рядом с доменным файлом (vitest).
Не используй snapshot без необходимости.

Запуск:
cd "/Users/ramisyaparov/Desktop/Project/CBooster Client Payments"
npm --prefix webapp run test
```

- [ ] **11. uiState storage tests (P2)**

```text
Ты агент разработки в репозитории /Users/ramisyaparov/Desktop/Project/CBooster Client Payments.

Задача: покрыть /Users/ramisyaparov/Desktop/Project/CBooster Client Payments/webapp/src/shared/storage/uiState.ts.
Проверь:
- SSR fallback (window undefined),
- corrupted JSON,
- normalizePeriod на неизвестные значения,
- sortDirection normalization,
- write/read roundtrip.

Добавь unit-тесты на vitest с моками localStorage/window.

Запуск:
cd "/Users/ramisyaparov/Desktop/Project/CBooster Client Payments"
npm --prefix webapp run test -- src/shared/storage
npm --prefix webapp run test
```

- [ ] **12. records API adapter tests (P2)**

```text
Ты агент разработки в репозитории /Users/ramisyaparov/Desktop/Project/CBooster Client Payments.

Задача: добавить тесты для /Users/ramisyaparov/Desktop/Project/CBooster Client Payments/webapp/src/shared/api/records.ts.
Проверь нормализацию payload:
- getRecords: records not array -> [],
- updatedAt not string -> null,
- patchRecords: appliedOperations валиден только для finite number,
- putRecords/patchRecords корректно отправляют expectedUpdatedAt и body.

Мокай apiRequest, сеть не использовать.

Запуск:
cd "/Users/ramisyaparov/Desktop/Project/CBooster Client Payments"
npm --prefix webapp run test -- src/shared/api
npm --prefix webapp run test
```

