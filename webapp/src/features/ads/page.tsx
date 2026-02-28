import { useCallback, useEffect, useMemo, useState } from "react";

import { getAdsOverview } from "@/shared/api";
import type { MetaAdsAdRow, MetaAdsCampaignRow, MetaAdsOverviewPayload } from "@/shared/types/ads";
import { Badge, Button, EmptyState, ErrorState, Field, Input, LoadingSkeleton, PageHeader, PageShell, Panel, Table } from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

export default function AdsPage() {
  const [since, setSince] = useState(() => formatDateIsoDaysAgo(29));
  const [until, setUntil] = useState(() => formatDateIsoDaysAgo(0));
  const [data, setData] = useState<MetaAdsOverviewPayload | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [loadError, setLoadError] = useState("");

  const loadOverview = useCallback(
    async (manual = false) => {
      if (manual) {
        setIsRefreshing(true);
      } else {
        setIsLoading(true);
      }
      setLoadError("");

      try {
        const payload = await getAdsOverview({ since, until });
        setData(payload);
      } catch (error) {
        setData(null);
        setLoadError(error instanceof Error ? error.message : "Failed to load ads data.");
      } finally {
        setIsLoading(false);
        setIsRefreshing(false);
      }
    },
    [since, until],
  );

  useEffect(() => {
    void loadOverview(false);
  }, [loadOverview]);

  const campaignColumns = useMemo<TableColumn<MetaAdsCampaignRow>[]>(
    () => [
      {
        key: "campaign",
        label: "CAMPAIGN",
        align: "left",
        cell: (row) => row.campaignName || "-",
      },
      {
        key: "account",
        label: "ACCOUNT",
        align: "left",
        cell: (row) => row.accountName || "-",
      },
      {
        key: "spend",
        label: "SPEND",
        align: "right",
        cell: (row) => formatCurrency(row.spend),
      },
      {
        key: "impressions",
        label: "IMPRESSIONS",
        align: "right",
        cell: (row) => formatInteger(row.impressions),
      },
      {
        key: "clicks",
        label: "CLICKS",
        align: "right",
        cell: (row) => formatInteger(row.clicks),
      },
      {
        key: "ctr",
        label: "CTR",
        align: "right",
        cell: (row) => formatPercent(row.ctr),
      },
      {
        key: "cpc",
        label: "CPC",
        align: "right",
        cell: (row) => formatCurrencyNullable(row.cpc),
      },
    ],
    [],
  );

  const adsColumns = useMemo<TableColumn<MetaAdsAdRow>[]>(
    () => [
      {
        key: "ad",
        label: "AD",
        align: "left",
        cell: (row) => row.adName || "-",
      },
      {
        key: "campaign",
        label: "CAMPAIGN",
        align: "left",
        cell: (row) => row.campaignName || "-",
      },
      {
        key: "adset",
        label: "AD SET",
        align: "left",
        cell: (row) => row.adSetName || "-",
      },
      {
        key: "date",
        label: "DATE",
        align: "center",
        cell: (row) => `${row.dateStart} -> ${row.dateStop}`,
      },
      {
        key: "spend",
        label: "SPEND",
        align: "right",
        cell: (row) => formatCurrency(row.spend),
      },
      {
        key: "impressions",
        label: "IMPRESSIONS",
        align: "right",
        cell: (row) => formatInteger(row.impressions),
      },
      {
        key: "clicks",
        label: "CLICKS",
        align: "right",
        cell: (row) => formatInteger(row.clicks),
      },
      {
        key: "ctr",
        label: "CTR",
        align: "right",
        cell: (row) => formatPercent(row.ctr),
      },
    ],
    [],
  );

  const summary = data?.summary;

  return (
    <PageShell className="ads-page">
      <PageHeader
        title="Реклама"
        subtitle="Meta Ads"
        meta={
          data?.range ? (
            <span className="client-payments-page-header-meta">
              Range: {data.range.since}
              {" -> "}
              {data.range.until}
            </span>
          ) : null
        }
        actions={
          <Button type="button" variant="secondary" onClick={() => void loadOverview(true)} disabled={isLoading || isRefreshing}>
            {isRefreshing ? "Refreshing..." : "Refresh"}
          </Button>
        }
      />

      <Panel title="Filters" className="table-panel">
        <div className="ads-filters-grid">
          <Field label="FROM" htmlFor="ads-filter-since">
            <Input
              id="ads-filter-since"
              type="date"
              value={since}
              max={until || undefined}
              onChange={(event) => setSince(String(event.target.value || "").trim())}
            />
          </Field>
          <Field label="TO" htmlFor="ads-filter-until">
            <Input
              id="ads-filter-until"
              type="date"
              value={until}
              min={since || undefined}
              onChange={(event) => setUntil(String(event.target.value || "").trim())}
            />
          </Field>
          <div className="ads-filters-actions">
            <Button type="button" variant="primary" onClick={() => void loadOverview(true)} disabled={isLoading || isRefreshing}>
              Apply
            </Button>
          </div>
        </div>
      </Panel>

      {isLoading ? (
        <Panel title="Ads Data" className="table-panel">
          <LoadingSkeleton rows={8} />
        </Panel>
      ) : loadError ? (
        <Panel title="Ads Data" className="table-panel">
          <ErrorState title="Failed to load Meta Ads data" description={loadError} actionLabel="Retry" onAction={() => void loadOverview(true)} />
        </Panel>
      ) : !data?.configured ? (
        <Panel title="Ads Data" className="table-panel">
          <EmptyState title="Meta Ads is not configured" description="Set META_ADS_ACCESS_TOKEN and META_ADS_ACCOUNT_IDS in environment variables." />
        </Panel>
      ) : (
        <>
          <Panel title="Overview" className="table-panel">
            <div className="ads-overview-grid">
              <MetricCard label="Spend" value={formatCurrency(summary?.spend || 0)} tone="success" />
              <MetricCard label="Impressions" value={formatInteger(summary?.impressions || 0)} />
              <MetricCard label="Clicks" value={formatInteger(summary?.clicks || 0)} />
              <MetricCard label="Reach" value={formatInteger(summary?.reach || 0)} />
              <MetricCard label="CTR" value={formatPercent(summary?.ctr)} />
              <MetricCard label="CPC" value={formatCurrencyNullable(summary?.cpc)} />
            </div>
            <div className="ads-overview-badges">
              <Badge tone="info">Accounts: {summary?.accountCount || 0}</Badge>
              <Badge tone="info">Campaigns: {summary?.campaignCount || 0}</Badge>
              <Badge tone="info">Ad Sets: {summary?.adSetCount || 0}</Badge>
              <Badge tone="info">Ads: {summary?.adCount || 0}</Badge>
            </div>
          </Panel>

          <Panel title="Top Campaigns" className="table-panel">
            {Array.isArray(data.campaigns) && data.campaigns.length ? (
              <Table
                columns={campaignColumns}
                rows={data.campaigns}
                rowKey={(row) => `${row.campaignId}::${row.campaignName}`}
                className="ads-table-wrap"
                emptyState="No campaigns found for selected range"
              />
            ) : (
              <EmptyState title="No campaigns found" description="Try a wider date range." />
            )}
          </Panel>

          <Panel title="Ads" className="table-panel">
            {Array.isArray(data.ads) && data.ads.length ? (
              <Table
                columns={adsColumns}
                rows={data.ads}
                rowKey={(row) => `${row.adId}::${row.dateStart}::${row.dateStop}`}
                className="ads-table-wrap"
                emptyState="No ads found for selected range"
              />
            ) : (
              <EmptyState title="No ads found" description="Try a wider date range." />
            )}
          </Panel>
        </>
      )}
    </PageShell>
  );
}

function MetricCard(props: { label: string; value: string; tone?: "default" | "success" }) {
  const { label, value, tone = "default" } = props;
  return (
    <div className={`ads-metric-card ${tone === "success" ? "is-success" : ""}`.trim()}>
      <p className="ads-metric-card__label">{label}</p>
      <strong className="ads-metric-card__value">{value}</strong>
    </div>
  );
}

function formatDateIsoDaysAgo(daysAgo: number): string {
  const safeDays = Number.isFinite(daysAgo) ? Math.max(0, Math.floor(daysAgo)) : 0;
  const now = new Date();
  const date = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - safeDays));
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function formatCurrency(value: number): string {
  const amount = Number.isFinite(value) ? value : 0;
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(amount);
}

function formatCurrencyNullable(value: number | null | undefined): string {
  if (!Number.isFinite(value ?? Number.NaN)) {
    return "-";
  }
  return formatCurrency(value as number);
}

function formatInteger(value: number): string {
  const safeNumber = Number.isFinite(value) ? Math.max(0, Math.round(value)) : 0;
  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: 0,
  }).format(safeNumber);
}

function formatPercent(value: number | null | undefined): string {
  if (!Number.isFinite(value ?? Number.NaN)) {
    return "-";
  }
  return `${(value as number).toFixed(2)}%`;
}
