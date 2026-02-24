import { Badge } from "@/shared/ui";
import { getRecordStatusFlags } from "@/features/client-payments/domain/calculations";
import type { ClientRecord } from "@/shared/types/records";

interface StatusBadgesProps {
  record: ClientRecord;
}

export function StatusBadges({ record }: StatusBadgesProps) {
  const status = getRecordStatusFlags(record);

  if (status.isContractCompleted) {
    return (
      <div className="client-payments__badges">
        <Badge tone="neutral">Inactive</Badge>
      </div>
    );
  }

  return (
    <div className="client-payments__badges">
      {status.isWrittenOff ? <Badge tone="danger">Written Off</Badge> : null}
      {status.isFullyPaid ? <Badge tone="success">Fully Paid</Badge> : null}
      {status.isOverdue ? <Badge tone="warning">Overdue {status.overdueRange}</Badge> : null}
      {status.isAfterResult ? <Badge tone="info">After Result</Badge> : null}
      {!status.isAfterResult && !status.isWrittenOff && !status.isFullyPaid && !status.isOverdue ? (
        <Badge tone="neutral">Active</Badge>
      ) : null}
    </div>
  );
}
