import { describe, expect, it } from "vitest";

import { createEmptyRecord } from "@/features/client-payments/domain/calculations";
import { buildClientHealthRows } from "@/features/client-health/domain/health";
import type { ClientRecord } from "@/shared/types/records";

function makeRecord(patch: Partial<ClientRecord>): ClientRecord {
  return {
    ...createEmptyRecord(),
    id: "client-1",
    clientName: "Иван Петров",
    active: "Yes",
    createdAt: "2026-01-01",
    payment1: "300",
    payment1Date: "01/05/2026",
    ...patch,
  };
}

describe("buildClientHealthRows", () => {
  it("returns only 5 clients in safe mode", () => {
    const sources = Array.from({ length: 7 }, (_, index) => ({
      record: makeRecord({
        id: `client-${index + 1}`,
        clientName: `Клиент ${index + 1}`,
      }),
      memo: null,
      communications: null,
    }));

    const rows = buildClientHealthRows(sources, new Date("2026-02-20T00:00:00Z"));

    expect(rows).toHaveLength(5);
  });

  it("detects risk phrases and communication streak flags", () => {
    const row = buildClientHealthRows(
      [
        {
          record: makeRecord({ id: "risk-client", clientName: "Риск Клиент" }),
          memo: null,
          communications: {
            ok: true,
            status: "found",
            clientName: "Риск Клиент",
            contactName: "Риск Клиент",
            contactId: "c-1",
            source: "test",
            matchedContacts: 1,
            inspectedContacts: 1,
            smsCount: 2,
            callCount: 0,
            items: [
              {
                id: "m-1",
                messageId: "m-1",
                conversationId: "cv-1",
                kind: "sms",
                direction: "inbound",
                body: "I want cancel now",
                transcript: "",
                status: "delivered",
                createdAt: "2026-02-18T12:00:00Z",
                source: "test",
                recordingUrls: [],
                attachmentUrls: [],
              },
              {
                id: "m-2",
                messageId: "m-2",
                conversationId: "cv-1",
                kind: "sms",
                direction: "inbound",
                body: "Need refund because not working",
                transcript: "",
                status: "delivered",
                createdAt: "2026-02-19T12:00:00Z",
                source: "test",
                recordingUrls: [],
                attachmentUrls: [],
              },
            ],
          },
        },
      ],
      new Date("2026-02-20T12:00:00Z"),
    )[0];

    expect(row.communication.riskPhrases).toContain("cancel");
    expect(row.communication.riskPhrases).toContain("refund");
    expect(row.communication.negativeStreak).toBeGreaterThanOrEqual(2);
    expect(row.communication.flags.length).toBeGreaterThan(0);
    expect(row.clientSurname).toBe("Клиент");
    expect(row.explanation.why.length).toBeGreaterThan(0);
    expect(row.explanation.scoreBreakdown.total).toBe(row.overview.healthIndex);
  });
});
