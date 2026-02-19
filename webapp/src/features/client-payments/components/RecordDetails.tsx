import { useEffect, useMemo, useState } from "react";

import { getGhlClientBasicNote } from "@/shared/api";
import type { GhlClientBasicNotePayload } from "@/shared/types/ghlNotes";
import type { ClientRecord } from "@/shared/types/records";
import { FIELD_DEFINITIONS } from "@/features/client-payments/domain/constants";
import { formatDate, formatMoney, parseMoneyValue } from "@/features/client-payments/domain/calculations";

interface RecordDetailsProps {
  record: ClientRecord;
}

const HEADER_FIELD_KEYS = new Set<keyof ClientRecord>([
  "clientName",
  "contractTotals",
  "totalPayments",
  "futurePayments",
  "companyName",
]);

const EMAIL_PATTERN = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i;
const PHONE_PATTERN = /(?:\+?\d[\d\s().-]{7,}\d)/;

export function RecordDetails({ record }: RecordDetailsProps) {
  const [ghlBasicNote, setGhlBasicNote] = useState<GhlClientBasicNotePayload | null>(null);
  const [isLoadingGhlBasicNote, setIsLoadingGhlBasicNote] = useState(false);
  const [ghlBasicNoteError, setGhlBasicNoteError] = useState("");

  const normalizedClientName = useMemo(() => (record.clientName || "").trim(), [record.clientName]);
  const contractDisplay = useMemo(() => formatMoneyCell(record.contractTotals), [record.contractTotals]);
  const paidDisplay = useMemo(() => formatMoneyCell(record.totalPayments), [record.totalPayments]);
  const debtDisplay = useMemo(() => formatMoneyCell(record.futurePayments), [record.futurePayments]);
  const companyDisplay = useMemo(() => (record.companyName || "").trim() || "-", [record.companyName]);

  const detailsFields = useMemo(
    () => FIELD_DEFINITIONS.filter((field) => !HEADER_FIELD_KEYS.has(field.key)),
    [],
  );

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
          writtenOff: isWrittenOffRecord(record),
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
  }, [normalizedClientName, record]);

  const contactInfo = useMemo(() => resolveContactInfo(record, ghlBasicNote), [ghlBasicNote, record]);

  return (
    <div className="record-details-stack">
      <section className="record-profile-header">
        <h4 className="record-profile-header__name">{normalizedClientName || "Unnamed client"}</h4>
        <p className="record-profile-header__contract">
          Contract: <strong>{contractDisplay}</strong>
        </p>

        <div className="record-profile-header__money-row">
          <div className="record-profile-header__money-item">
            <span className="record-profile-header__money-label">Paid</span>
            <strong className="record-profile-header__money-value">{paidDisplay}</strong>
          </div>
          <div className="record-profile-header__money-item">
            <span className="record-profile-header__money-label">Debt</span>
            <strong className="record-profile-header__money-value">{debtDisplay}</strong>
          </div>
        </div>

        <div className="record-profile-header__contact-row">
          <div className="record-profile-header__contact-item">
            <span className="record-profile-header__contact-label">Phone</span>
            <strong className="record-profile-header__contact-value">{contactInfo.phone || "-"}</strong>
          </div>
          <div className="record-profile-header__contact-item">
            <span className="record-profile-header__contact-label">Email</span>
            <strong className="record-profile-header__contact-value">{contactInfo.email || "-"}</strong>
          </div>
          <div className="record-profile-header__contact-item">
            <span className="record-profile-header__contact-label">Company</span>
            <strong className="record-profile-header__contact-value">{companyDisplay}</strong>
          </div>
        </div>
      </section>

      <section className="record-details-grid">
        {detailsFields.map((field) => {
          const rawValue = record[field.key] || "";
          if (!rawValue && field.type !== "checkbox") {
            return null;
          }

          const displayValue = formatFieldValue(field.key, field.type, rawValue);
          return (
            <div key={field.key} className="record-details-grid__item">
              <span className="record-details-grid__label">{field.label}</span>
              <strong className="record-details-grid__value">{displayValue || "-"}</strong>
            </div>
          );
        })}
      </section>

      <section className="record-details-ghl-note" aria-live="polite">
        <div className="record-details-ghl-note__header">
          <h4 className="record-details-ghl-note__title">Basic Info</h4>
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
          <p className="react-user-footnote">Basic info was not found for this client in GoHighLevel notes.</p>
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

      <section className="record-details-ghl-note" aria-live="polite">
        <div className="record-details-ghl-note__header">
          <h4 className="record-details-ghl-note__title">MEMO</h4>
        </div>

        {isLoadingGhlBasicNote ? <p className="react-user-footnote">Loading memo...</p> : null}
        {!isLoadingGhlBasicNote && ghlBasicNoteError ? (
          <p className="record-details-ghl-note__error">{ghlBasicNoteError}</p>
        ) : null}
        {!isLoadingGhlBasicNote && !ghlBasicNoteError && !ghlBasicNote?.memoBody ? (
          <p className="react-user-footnote">Memo not found in GoHighLevel notes for this client.</p>
        ) : null}
        {!isLoadingGhlBasicNote && !ghlBasicNoteError && ghlBasicNote?.memoBody ? (
          <>
            <pre className="record-details-ghl-note__body">{ghlBasicNote.memoBody}</pre>
            {ghlBasicNote.memoCreatedAt ? (
              <p className="react-user-footnote">Created: {formatDate(ghlBasicNote.memoCreatedAt)}</p>
            ) : null}
          </>
        ) : null}
      </section>
    </div>
  );
}

function formatMoneyCell(rawValue: string): string {
  const amount = parseMoneyValue(rawValue);
  if (amount === null) {
    return rawValue || "-";
  }
  return formatMoney(amount);
}

function formatFieldValue(
  key: keyof ClientRecord,
  type: "text" | "textarea" | "checkbox" | "date",
  rawValue: string,
): string {
  if (type === "checkbox") {
    return rawValue ? "Yes" : "No";
  }

  if (type === "date") {
    return formatDate(rawValue);
  }

  if (key === "contractTotals" || key === "totalPayments" || key === "futurePayments") {
    return formatMoneyCell(rawValue);
  }

  return rawValue;
}

function resolveContactInfo(record: ClientRecord, ghlBasicNote: GhlClientBasicNotePayload | null): {
  phone: string;
  email: string;
} {
  const noteText = [ghlBasicNote?.noteBody || "", ghlBasicNote?.memoBody || ""].filter(Boolean).join("\n");

  const phoneFromRecord =
    getOptionalRecordText(record, "clientPhoneNumber") || getOptionalRecordText(record, "clientPhone") || getOptionalRecordText(record, "phone");
  const emailFromRecord =
    getOptionalRecordText(record, "clientEmailAddress") || getOptionalRecordText(record, "clientEmail") || getOptionalRecordText(record, "email");

  return {
    phone: phoneFromRecord || extractFirstMatch(noteText, PHONE_PATTERN),
    email: emailFromRecord || extractFirstMatch(noteText, EMAIL_PATTERN),
  };
}

function getOptionalRecordText(record: ClientRecord, key: string): string {
  const rawValue = (record as unknown as Record<string, unknown>)[key];
  return (rawValue || "").toString().trim();
}

function extractFirstMatch(value: string, pattern: RegExp): string {
  const text = (value || "").toString();
  if (!text) {
    return "";
  }

  const match = text.match(pattern);
  return (match?.[0] || "").trim();
}

function isWrittenOffRecord(record: ClientRecord): boolean {
  const value = (record.writtenOff || "").toString().trim().toLowerCase();
  return value === "yes" || value === "true" || value === "1" || value === "on";
}
