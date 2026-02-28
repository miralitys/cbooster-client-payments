import { EmptyState, PageHeader, PageShell, Panel } from "@/shared/ui";

export default function AdsPage() {
  return (
    <PageShell className="ads-page">
      <PageHeader
        title="Реклама"
        subtitle="Facebook Ads workspace"
      />
      <Panel title="Ads Data">
        <EmptyState
          title="Ads page is ready."
          description="Access is limited to Owner/Admin. Next step: connect Meta Business data sources."
        />
      </Panel>
    </PageShell>
  );
}
