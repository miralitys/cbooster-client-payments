import { useEffect, useMemo, useState } from "react";

import { getGhlClientBasicNote } from "@/shared/api";
import type { GhlClientBasicNotePayload } from "@/shared/types/ghlNotes";
import type { ClientRecord } from "@/shared/types/records";
import { FIELD_DEFINITIONS } from "@/features/client-payments/domain/constants";
import { formatDate, formatMoney, parseMoneyValue } from "@/features/client-payments/domain/calculations";

interface RecordDetailsProps {
  record: ClientRecord;
}

export function RecordDetails({ record }: RecordDetailsProps) {
  const [ghlBasicNote, setGhlBasicNote] = useState<GhlClientBasicNotePayload | null>(null);
  const [isLoadingGhlBasicNote, setIsLoadingGhlBasicNote] = useState(false);
  const [ghlBasicNoteError, setGhlBasicNoteError] = useState("");

  const normalizedClientName = useMemo(() => (record.clientName || "").trim(), [record.clientName]);

  useEffect(() => {
    if (!normalizedClientName) {
      setGhlBasicNote(null);
      setGhlBasicNoteError("");
      setIsLoadingGhlBasicNote(false);
      return;
    }

    const abortController = new AbortController();
    let isActive = true;

    async function loadGhlBasicNote() {
      setIsLoadingGhlBasicNote(true);
      setGhlBasicNoteError("");

      try {
        const payload = await getGhlClientBasicNote(normalizedClientName, {
          signal: abortController.signal,
        });
        if (!isActive) {
          return;
        }
        setGhlBasicNote(payload);
      } catch (error) {
        if (!isActive) {
          return;
        }
        setGhlBasicNote(null);
        setGhlBasicNoteError(error instanceof Error ? error.message : "Failed to load GoHighLevel BASIC note.");
      } finally {
        if (isActive) {
          setIsLoadingGhlBasicNote(false);
        }
      }
    }

    void loadGhlBasicNote();

    return () => {
      isActive = false;
      abortController.abort();
    };
  }, [normalizedClientName]);

  return (
    <div className="record-details-stack">
      <div className="record-details-grid">
        {FIELD_DEFINITIONS.map((field) => {
          const value = record[field.key] || "";
          if (!value && field.type !== "checkbox") {
            return null;
          }

          let displayValue = value;
          if (field.type === "checkbox") {
            displayValue = value ? "Yes" : "No";
          } else if (field.type === "date") {
            displayValue = formatDate(value);
          } else if (field.key === "contractTotals" || field.key === "totalPayments" || field.key === "futurePayments") {
            const amount = parseMoneyValue(value);
            displayValue = amount === null ? value : formatMoney(amount);
          }

          return (
            <div key={field.key} className="record-details-grid__item">
              <span className="record-details-grid__label">{field.label}</span>
              <strong className="record-details-grid__value">{displayValue || "-"}</strong>
            </div>
          );
        })}
      </div>

      <section className="record-details-ghl-note" aria-live="polite">
        <div className="record-details-ghl-note__header">
          <h4 className="record-details-ghl-note__title">GoHighLevel BASIC Note</h4>
          {ghlBasicNote?.contactName ? (
            <p className="react-user-footnote">
              Contact: {ghlBasicNote.contactName}
              {ghlBasicNote.contactId ? ` (${ghlBasicNote.contactId})` : ""}
            </p>
          ) : null}
        </div>

        {isLoadingGhlBasicNote ? <p className="react-user-footnote">Searching client in GoHighLevel...</p> : null}
        {!isLoadingGhlBasicNote && ghlBasicNoteError ? (
          <p className="record-details-ghl-note__error">{ghlBasicNoteError}</p>
        ) : null}
        {!isLoadingGhlBasicNote && !ghlBasicNoteError && (!ghlBasicNote || ghlBasicNote.status !== "found") ? (
          <p className="react-user-footnote">BASIC note was not found for this client in GoHighLevel.</p>
        ) : null}
        {!isLoadingGhlBasicNote && !ghlBasicNoteError && ghlBasicNote?.status === "found" ? (
          <>
            <pre className="record-details-ghl-note__body">{ghlBasicNote.noteBody}</pre>
            {ghlBasicNote.noteCreatedAt ? (
              <p className="react-user-footnote">Created: {formatDate(ghlBasicNote.noteCreatedAt)}</p>
            ) : null}
          </>
        ) : null}
      </section>
    </div>
  );
}
