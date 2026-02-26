import { Badge } from "@/shared/ui";
import { getRecordStatusFlags } from "@/features/client-payments/domain/calculations";
import type { ClientRecord } from "@/shared/types/records";

interface StatusBadgesProps {
  record: ClientRecord;
}

export function StatusBadges({ record }: StatusBadgesProps) {
  const status = getRecordStatusFlags(record);

  return (
    <div className="client-payments__badges">
      {status.isWrittenOff ? <Badge tone="danger">Written Off</Badge> : null}
      {status.isFullyPaid ? <Badge tone="success">Fully Paid</Badge> : null}
      {status.isOverdue ? <Badge tone="warning">Overdue {status.overdueRange}</Badge> : null}
      {status.isAfterResult ? <Badge tone="info">After Result</Badge> : null}
      {status.isActive ? <Badge tone="neutral">Active</Badge> : <Badge tone="neutral">Inactive</Badge>}
    </div>
  );
}
