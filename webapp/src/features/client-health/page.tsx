import { useCallback, useEffect, useMemo, useState } from "react";

import { apiRequest, getClientHealth, getGhlClientBasicNote } from "@/shared/api";
import type { GhlClientCommunicationsPayload } from "@/shared/types/ghlCommunications";
import type { ClientRecord } from "@/shared/types/records";
import {
  Badge,
  Button,
  EmptyState,
  ErrorState,
  LoadingSkeleton,
  PageHeader,
  PageShell,
  Panel,
  Table,
} from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";
import {
  buildClientHealthRows,
  CLIENT_HEALTH_FLAGS_LOGIC,
  CLIENT_HEALTH_SCORING_PSEUDOCODE,
  CLIENT_HEALTH_SQL_EXAMPLE,
  type ClientHealthRow,
  type ClientHealthSource,
} from "@/features/client-health/domain/health";

const SAFE_LIMIT = 5;
const CLIENT_COMMUNICATION_TIMEOUT_MS = 8_000;
const CURRENCY_FORMATTER = new Intl.NumberFormat("ru-RU", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

type LoadPhase = "idle" | "loading" | "refreshing";

interface ClientHealthMeta {
  updatedAt: string;
  source: string;
  sampleMode: string;
  limit: number;
}

export default function ClientHealthPage() {
  const [rows, setRows] = useState<ClientHealthRow[]>([]);
  const [loadPhase, setLoadPhase] = useState<LoadPhase>("loading");
  const [loadError, setLoadError] = useState("");
  const [expandedClientId, setExpandedClientId] = useState<string | null>(null);
  const [meta, setMeta] = useState<ClientHealthMeta>({
    updatedAt: "",
    source: "",
    sampleMode: "",
    limit: SAFE_LIMIT,
  });

  const isLoading = loadPhase === "loading";
  const isRefreshing = loadPhase === "refreshing";

  const loadDashboard = useCallback(async (mode: LoadPhase = "loading") => {
    setLoadPhase(mode);
    setLoadError("");

    try {
      const payload = await getClientHealth();
      const records = Array.isArray(payload.records) ? payload.records.slice(0, SAFE_LIMIT) : [];
      const sources = await mapWithConcurrency(records, 2, loadClientSourceSafeMode);
      const calculatedRows = buildClientHealthRows(sources);

      setRows(calculatedRows);
      setMeta({
        updatedAt: typeof payload.updatedAt === "string" ? payload.updatedAt : "",
        source: typeof payload.source === "string" ? payload.source : "",
        sampleMode: typeof payload.sampleMode === "string" ? payload.sampleMode : "",
        limit: typeof payload.limit === "number" && Number.isFinite(payload.limit) ? Math.max(1, Math.trunc(payload.limit)) : SAFE_LIMIT,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Не удалось загрузить дашборд здоровья клиента.";
      setRows([]);
      setLoadError(message);
    } finally {
      setLoadPhase("idle");
    }
  }, []);

  useEffect(() => {
    void loadDashboard("loading");
  }, [loadDashboard]);

  const updatedAtLabel = useMemo(() => {
    if (!meta.updatedAt) {
      return "—";
    }
    const timestamp = Date.parse(meta.updatedAt);
    if (!Number.isFinite(timestamp)) {
      return "—";
    }
    return new Date(timestamp).toLocaleString("ru-RU");
  }, [meta.updatedAt]);

  useEffect(() => {
    setExpandedClientId((current) => {
      if (!rows.length) {
        return null;
      }
      if (current && rows.some((row) => row.clientId === current)) {
        return current;
      }
      return rows[0].clientId;
    });
  }, [rows]);

  const toggleExpandedClient = useCallback((clientId: string) => {
    setExpandedClientId((current) => (current === clientId ? null : clientId));
  }, []);

  const overviewColumns = useMemo<TableColumn<ClientHealthRow>[]>(
    () => [
      {
        key: "client",
        label: "Клиент",
        cell: (row) => <span className="client-health-client-name">{row.clientName}</span>,
      },
      {
        key: "status",
        label: "Статус",
        cell: (row) => <Badge tone={resolveStatusTone(row.overview.status)}>{row.overview.status}</Badge>,
      },
      {
        key: "health-index",
        label: "Индекс здоровья",
        align: "right",
        cell: (row) => (
          <span className={`client-health-index client-health-index--${row.overview.tone}`}>{row.overview.healthIndex}</span>
        ),
      },
      {
        key: "churn",
        label: "Вероятность отмены",
        align: "right",
        cell: (row) => <span>{formatPercent(row.overview.churnProbability)}</span>,
      },
      {
        key: "work-days",
        label: "Дней в работе",
        align: "right",
        cell: (row) => <span>{formatDays(row.overview.daysInWork)}</span>,
      },
      {
        key: "revenue",
        label: "Общая выручка",
        align: "right",
        cell: (row) => <span>{formatMoney(row.overview.totalRevenue)}</span>,
      },
      {
        key: "monthly",
        label: "Ежемесячный платёж",
        align: "right",
        cell: (row) => <span>{formatMoney(row.overview.monthlyPayment)}</span>,
      },
    ],
    [],
  );

  const timelineColumns = useMemo<TableColumn<ClientHealthRow>[]>(
    () => [
      {
        key: "client",
        label: "Клиент",
        cell: (row) => <span className="client-health-client-name">{row.clientName}</span>,
      },
      {
        key: "sale-date",
        label: "Дата продажи",
        cell: (row) => row.timeline.saleDate,
      },
      {
        key: "start-date",
        label: "Дата старта",
        cell: (row) => row.timeline.startDate,
      },
      {
        key: "sale-to-start",
        label: "Дней от продажи до старта",
        align: "right",
        cell: (row) => formatOptionalDays(row.timeline.daysSaleToStart),
      },
      {
        key: "last-activity",
        label: "Дней с последней активности",
        align: "right",
        cell: (row) => formatOptionalDays(row.timeline.daysSinceLastActivity),
      },
      {
        key: "last-contact",
        label: "Дней с последнего контакта",
        align: "right",
        cell: (row) => formatOptionalDays(row.timeline.daysSinceLastContact),
      },
      {
        key: "last-payment",
        label: "Дней с последнего платежа",
        align: "right",
        cell: (row) => formatOptionalDays(row.timeline.daysSinceLastPayment),
      },
      {
        key: "flags",
        label: "Флаги",
        cell: (row) => (
          <div className="client-health-flags-cell">
            {row.timeline.launchDelayFlag ? <Badge tone="warning">Задержка запуска {">"} 7 дней</Badge> : null}
            {row.timeline.inactivityFlag ? <Badge tone="danger">Нет активности {">"} 14 дней</Badge> : null}
            {!row.timeline.launchDelayFlag && !row.timeline.inactivityFlag ? <span>—</span> : null}
          </div>
        ),
      },
    ],
    [],
  );

  const paymentColumns = useMemo<TableColumn<ClientHealthRow>[]>(
    () => [
      {
        key: "client",
        label: "Клиент",
        cell: (row) => <span className="client-health-client-name">{row.clientName}</span>,
      },
      {
        key: "expected",
        label: "Ожидаемый ежемесячный платёж",
        align: "right",
        cell: (row) => formatMoney(row.payments.expectedMonthlyPayment),
      },
      {
        key: "last-payment",
        label: "Дата последнего платежа",
        cell: (row) => row.payments.lastPaymentDate,
      },
      {
        key: "avg-delay",
        label: "Средняя задержка (дни)",
        align: "right",
        cell: (row) => row.payments.averageDelayDays.toFixed(1),
      },
      {
        key: "on-time",
        label: "% оплат вовремя",
        align: "right",
        cell: (row) => formatPercent(row.payments.onTimePercent),
      },
      {
        key: "overdue",
        label: "Количество просрочек",
        align: "right",
        cell: (row) => String(row.payments.overdueCount),
      },
      {
        key: "discipline",
        label: "Платёжная дисциплина (0-10)",
        align: "right",
        cell: (row) => row.payments.disciplineScore.toFixed(1),
      },
      {
        key: "flags",
        label: "Риски",
        cell: (row) => (
          <div className="client-health-flags-cell">
            {row.payments.riskFlags.length ? row.payments.riskFlags.map((flag) => <Badge key={flag} tone="danger">{flag}</Badge>) : <span>—</span>}
          </div>
        ),
      },
    ],
    [],
  );

  const executionColumns = useMemo<TableColumn<ClientHealthRow>[]>(
    () => [
      {
        key: "client",
        label: "Клиент",
        cell: (row) => <span className="client-health-client-name">{row.clientName}</span>,
      },
      {
        key: "volume",
        label: "Обещанный объём работ",
        cell: (row) => row.execution.promisedWorkVolume,
      },
      {
        key: "term",
        label: "Ожидаемый срок",
        align: "right",
        cell: (row) => `${row.execution.expectedTermDays} дн.`,
      },
      {
        key: "work-days",
        label: "Дней в работе",
        align: "right",
        cell: (row) => formatDays(row.execution.daysInWork),
      },
      {
        key: "term-elapsed",
        label: "% срока истекло",
        align: "right",
        cell: (row) => formatPercent(row.execution.elapsedTermPercent),
      },
      {
        key: "activity-count",
        label: "Количество активностей",
        align: "right",
        cell: (row) => String(row.execution.activityCount),
      },
      {
        key: "activity-30",
        label: "Активность за 30 дней",
        align: "right",
        cell: (row) => String(row.execution.activity30d),
      },
      {
        key: "score",
        label: "Оценка исполнения (0-10)",
        align: "right",
        cell: (row) => row.execution.score.toFixed(1),
      },
      {
        key: "flag",
        label: "Флаг",
        cell: (row) => (row.execution.riskFlag ? <Badge tone="warning">Срок {">"} 50% и низкая активность</Badge> : "—"),
      },
    ],
    [],
  );

  const communicationColumns = useMemo<TableColumn<ClientHealthRow>[]>(
    () => [
      {
        key: "client",
        label: "Клиент",
        cell: (row) => <span className="client-health-client-name">{row.clientName}</span>,
      },
      {
        key: "contacts-30",
        label: "Контактов за 30 дней",
        align: "right",
        cell: (row) => String(row.communication.contacts30d),
      },
      {
        key: "in-out",
        label: "% входящих / исходящих",
        align: "right",
        cell: (row) => `${row.communication.inboundPercent}% / ${row.communication.outboundPercent}%`,
      },
      {
        key: "response",
        label: "Среднее время ответа клиента",
        align: "right",
        cell: (row) => `${row.communication.avgResponseHours.toFixed(1)} ч`,
      },
      {
        key: "sentiment",
        label: "Индекс тональности (-1..+1)",
        align: "right",
        cell: (row) => row.communication.sentimentIndex.toFixed(2),
      },
      {
        key: "score",
        label: "Оценка коммуникации (0-10)",
        align: "right",
        cell: (row) => row.communication.score.toFixed(1),
      },
      {
        key: "risk-phrases",
        label: "Риск-фразы",
        cell: (row) => (
          <div className="client-health-flags-cell">
            {row.communication.riskPhrases.length
              ? row.communication.riskPhrases.map((phrase) => <Badge key={phrase} tone="danger">{phrase}</Badge>)
              : "—"}
          </div>
        ),
      },
      {
        key: "negative-streak",
        label: "Негативных подряд",
        align: "right",
        cell: (row) => String(row.communication.negativeStreak),
      },
      {
        key: "flags",
        label: "Флаги",
        cell: (row) => (
          <div className="client-health-flags-cell">
            {row.communication.flags.length ? row.communication.flags.map((flag) => <Badge key={flag} tone="warning">{flag}</Badge>) : "—"}
          </div>
        ),
      },
    ],
    [],
  );

  const riskColumns = useMemo<TableColumn<ClientHealthRow>[]>(
    () => [
      {
        key: "client",
        label: "Клиент",
        cell: (row) => <span className="client-health-client-name">{row.clientName}</span>,
      },
      {
        key: "warnings",
        label: "Активные предупреждения",
        cell: (row) => (
          <div className="client-health-flags-cell">
            {row.risks.warnings.length ? row.risks.warnings.map((warning) => <Badge key={warning} tone="warning">{warning}</Badge>) : "—"}
          </div>
        ),
      },
      {
        key: "churn-probability",
        label: "Вероятность отмены",
        align: "right",
        cell: (row) => formatPercent(row.overview.churnProbability),
      },
      {
        key: "category",
        label: "Категория",
        cell: (row) => <Badge tone={resolveRiskCategoryTone(row.risks.churnCategory)}>{row.risks.churnCategory}</Badge>,
      },
    ],
    [],
  );

  return (
    <PageShell className="client-health-page">
      <PageHeader
        title="Здоровье клиента"
        subtitle="Безопасный тестовый режим: выборка ограничена 5 клиентами."
        actions={(
          <Button type="button" variant="secondary" onClick={() => void loadDashboard("refreshing")} disabled={isLoading || isRefreshing}>
            {isRefreshing ? "Обновление..." : "Обновить"}
          </Button>
        )}
        meta={(
          <div className="client-health-meta">
            <span><strong>SAFE MODE:</strong> ограничение до 5 клиентов</span>
            <span><strong>Клиентов:</strong> {meta.limit}</span>
            <span><strong>Источник:</strong> {meta.source || "—"}</span>
            <span><strong>Выборка:</strong> {meta.sampleMode || "—"}</span>
            <span><strong>Обновлено:</strong> {updatedAtLabel}</span>
          </div>
        )}
      />

      {loadError ? (
        <Panel title="Ошибка загрузки">
          <ErrorState
            title="Не удалось загрузить данные дашборда"
            description={loadError}
            actionLabel="Повторить"
            onAction={() => void loadDashboard("loading")}
          />
        </Panel>
      ) : null}

      {isLoading ? (
        <Panel title="Загрузка данных безопасного режима">
          <LoadingSkeleton rows={8} />
        </Panel>
      ) : null}

      {!isLoading && !loadError && !rows.length ? (
        <Panel title="Нет данных">
          <EmptyState
            title="Клиенты не найдены"
            description="В SAFE MODE выборка ограничена до 5 клиентов. Проверьте наличие активных записей."
          />
        </Panel>
      ) : null}

      {!isLoading && !loadError && rows.length ? (
        <>
          <Panel title="1. Обзор клиента">
            <Table columns={overviewColumns} rows={rows} rowKey={(row) => row.clientId} />
          </Panel>

          <Panel title="2. Хронология">
            <Table columns={timelineColumns} rows={rows} rowKey={(row) => row.clientId} />
          </Panel>

          <Panel title="3. Платежи">
            <Table
              columns={paymentColumns}
              rows={rows}
              rowKey={(row) => row.clientId}
              rowClassName={(row) => (row.payments.riskFlags.length ? "client-health-row--danger" : undefined)}
            />
          </Panel>

          <Panel title="4. Исполнение обязательств">
            <Table
              columns={executionColumns}
              rows={rows}
              rowKey={(row) => row.clientId}
              rowClassName={(row) => (row.execution.riskFlag ? "client-health-row--warning" : undefined)}
            />
          </Panel>

          <Panel title="5. Коммуникация">
            <Table
              columns={communicationColumns}
              rows={rows}
              rowKey={(row) => row.clientId}
              rowClassName={(row) => (row.communication.flags.length ? "client-health-row--warning" : undefined)}
            />
          </Panel>

          <Panel title="6. Панель рисков">
            <Table columns={riskColumns} rows={rows} rowKey={(row) => row.clientId} />
          </Panel>

          <Panel title="7. Подробное саммари по клиенту (по фамилии)">
            <div className="client-health-summary-list">
              {rows.map((row) => {
                const panelId = `client-health-summary-${toDomId(row.clientId)}`;
                const isExpanded = expandedClientId === row.clientId;
                const score = row.explanation.scoreBreakdown;

                return (
                  <article key={row.clientId} className={`client-health-summary-item${isExpanded ? " is-expanded" : ""}`}>
                    <button
                      type="button"
                      className="client-health-summary-toggle"
                      aria-expanded={isExpanded}
                      aria-controls={panelId}
                      onClick={() => toggleExpandedClient(row.clientId)}
                    >
                      <span className="client-health-summary-surname">{row.clientSurname}</span>
                      <span className="client-health-summary-name">{row.clientName}</span>
                      <span className="client-health-summary-status">
                        <Badge tone={resolveStatusTone(row.overview.status)}>{row.overview.status}</Badge>
                      </span>
                    </button>
                    <div id={panelId} className="client-health-summary-panel">
                      <div className="client-health-summary-panel-inner">
                        <div className="client-health-summary-grid">
                          <section>
                            <h3>Что</h3>
                            <ul>
                              {row.explanation.what.map((line) => (
                                <li key={line}>{line}</li>
                              ))}
                            </ul>
                          </section>
                          <section>
                            <h3>Когда</h3>
                            <ul>
                              {row.explanation.when.map((line) => (
                                <li key={line}>{line}</li>
                              ))}
                            </ul>
                          </section>
                          <section>
                            <h3>Зачем и почему</h3>
                            <ul>
                              {row.explanation.why.map((line) => (
                                <li key={line}>{line}</li>
                              ))}
                            </ul>
                          </section>
                          <section>
                            <h3>Логика оценки запуска</h3>
                            <ul>
                              {row.explanation.launch.map((line) => (
                                <li key={line}>{line}</li>
                              ))}
                            </ul>
                          </section>
                          <section>
                            <h3>Логика оценки платежей</h3>
                            <ul>
                              {row.explanation.payments.map((line) => (
                                <li key={line}>{line}</li>
                              ))}
                            </ul>
                          </section>
                          <section>
                            <h3>Логика оценки исполнения</h3>
                            <ul>
                              {row.explanation.execution.map((line) => (
                                <li key={line}>{line}</li>
                              ))}
                            </ul>
                          </section>
                          <section>
                            <h3>Логика оценки коммуникации</h3>
                            <ul>
                              {row.explanation.communication.map((line) => (
                                <li key={line}>{line}</li>
                              ))}
                            </ul>
                          </section>
                          <section>
                            <h3>Активные риски</h3>
                            <ul>
                              {row.explanation.risks.map((line) => (
                                <li key={line}>{line}</li>
                              ))}
                            </ul>
                          </section>
                        </div>
                        <section className="client-health-summary-score">
                          <h3>Разбор формулы индекса здоровья</h3>
                          <dl>
                            <div>
                              <dt>Скорость запуска</dt>
                              <dd>{score.launchSpeed} × 0.15 = {score.launchContribution}</dd>
                            </div>
                            <div>
                              <dt>Исполнение</dt>
                              <dd>{score.execution} × 0.30 = {score.executionContribution}</dd>
                            </div>
                            <div>
                              <dt>Платежи</dt>
                              <dd>{score.payments} × 0.25 = {score.paymentsContribution}</dd>
                            </div>
                            <div>
                              <dt>Вовлечённость</dt>
                              <dd>{score.engagement} × 0.15 = {score.engagementContribution}</dd>
                            </div>
                            <div>
                              <dt>Коммуникация</dt>
                              <dd>{score.communication} × 0.15 = {score.communicationContribution}</dd>
                            </div>
                            <div>
                              <dt>Итоговый индекс</dt>
                              <dd>{score.total}</dd>
                            </div>
                          </dl>
                        </section>
                      </div>
                    </div>
                  </article>
                );
              })}
            </div>
          </Panel>

          <Panel title="Безопасный режим / Техническая спецификация" className="client-health-tech-panel">
            <div className="client-health-tech-grid">
              <article>
                <h3>SQL (LIMIT 5)</h3>
                <pre>{CLIENT_HEALTH_SQL_EXAMPLE}</pre>
              </article>
              <article>
                <h3>Псевдокод скоринга</h3>
                <ul>
                  {CLIENT_HEALTH_SCORING_PSEUDOCODE.map((line) => (
                    <li key={line}>{line}</li>
                  ))}
                </ul>
              </article>
              <article>
                <h3>Логика флагов</h3>
                <ul>
                  {CLIENT_HEALTH_FLAGS_LOGIC.map((line) => (
                    <li key={line}>{line}</li>
                  ))}
                </ul>
              </article>
            </div>
          </Panel>
        </>
      ) : null}
    </PageShell>
  );
}

async function loadClientSourceSafeMode(record: ClientRecord): Promise<ClientHealthSource> {
  const clientName = String(record?.clientName || "").trim();
  if (!clientName) {
    return {
      record,
      memo: null,
      communications: null,
    };
  }

  const [memoResult, communicationsResult] = await Promise.allSettled([
    getGhlClientBasicNote(clientName, { refresh: false }),
    getClientCommunicationsSafe(clientName),
  ]);

  return {
    record,
    memo: memoResult.status === "fulfilled" ? memoResult.value : null,
    communications: communicationsResult.status === "fulfilled" ? communicationsResult.value : null,
  };
}

async function getClientCommunicationsSafe(clientName: string): Promise<GhlClientCommunicationsPayload | null> {
  const normalizedClientName = String(clientName || "").trim();
  if (!normalizedClientName) {
    return null;
  }

  const query = new URLSearchParams({
    clientName: normalizedClientName,
  });

  try {
    return await apiRequest<GhlClientCommunicationsPayload>(`/api/ghl/client-communications?${query.toString()}`, {
      timeoutMs: CLIENT_COMMUNICATION_TIMEOUT_MS,
    });
  } catch {
    return null;
  }
}

async function mapWithConcurrency<TInput, TOutput>(
  items: TInput[],
  concurrency: number,
  worker: (item: TInput, index: number) => Promise<TOutput>,
): Promise<TOutput[]> {
  const normalizedItems = Array.isArray(items) ? items : [];
  if (!normalizedItems.length) {
    return [];
  }

  const limit = Math.max(1, Math.floor(concurrency));
  const results = new Array<TOutput>(normalizedItems.length);
  let cursor = 0;

  async function runWorker() {
    while (cursor < normalizedItems.length) {
      const currentIndex = cursor;
      cursor += 1;
      results[currentIndex] = await worker(normalizedItems[currentIndex], currentIndex);
    }
  }

  await Promise.all(Array.from({ length: Math.min(limit, normalizedItems.length) }, () => runWorker()));
  return results;
}

function formatMoney(value: number): string {
  return CURRENCY_FORMATTER.format(Number.isFinite(value) ? value : 0);
}

function formatPercent(value: number): string {
  const safeValue = Number.isFinite(value) ? value : 0;
  return `${safeValue}%`;
}

function formatDays(value: number): string {
  const safeValue = Number.isFinite(value) ? Math.max(0, Math.trunc(value)) : 0;
  return `${safeValue}`;
}

function formatOptionalDays(value: number | null): string {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "—";
  }
  return formatDays(value);
}

function resolveStatusTone(status: ClientHealthRow["overview"]["status"]): "success" | "warning" | "danger" {
  if (status === "Здоровый") {
    return "success";
  }
  if (status === "Предупреждение") {
    return "warning";
  }
  return "danger";
}

function resolveRiskCategoryTone(category: ClientHealthRow["risks"]["churnCategory"]): "success" | "warning" | "danger" {
  if (category === "Стабильно") {
    return "success";
  }
  if (category === "Наблюдать") {
    return "warning";
  }
  return "danger";
}

function toDomId(value: string): string {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "-")
    .replace(/^-+|-+$/g, "");
}
