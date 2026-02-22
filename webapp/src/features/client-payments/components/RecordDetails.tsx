import { useEffect, useMemo, useState } from "react";

import { getGhlClientBasicNote, getGhlClientCommunications, postGhlClientCommunicationTranscript } from "@/shared/api";
import { evaluateClientScore } from "@/features/client-score/domain/scoring";
import type { GhlClientBasicNotePayload } from "@/shared/types/ghlNotes";
import type {
  GhlClientCommunicationDirection,
  GhlClientCommunicationItem,
  GhlClientCommunicationsPayload,
} from "@/shared/types/ghlCommunications";
import type { ClientRecord } from "@/shared/types/records";
import { FIELD_DEFINITIONS, PAYMENT_PAIRS } from "@/features/client-payments/domain/constants";
import { formatDate, formatMoney, getRecordStatusFlags, parseMoneyValue } from "@/features/client-payments/domain/calculations";
import { Badge, Button, Modal } from "@/shared/ui";

interface RecordDetailsProps {
  record: ClientRecord;
}

const HEADER_FIELD_KEYS = new Set<keyof ClientRecord>([
  "clientName",
  "serviceType",
  "contractTotals",
  "totalPayments",
  "futurePayments",
  "companyName",
]);
const PAYMENT_FIELD_KEYS = new Set<keyof ClientRecord>(
  PAYMENT_PAIRS.flatMap(([paymentKey, paymentDateKey]) => [paymentKey, paymentDateKey]),
);
const PROFILE_SUMMARY_FIELD_KEYS = new Set<keyof ClientRecord>([
  "address",
  "dateOfBirth",
  "ssn",
  "creditMonitoringLogin",
  "creditMonitoringPassword",
  "purchasedService",
  "clientPhoneNumber",
  "clientEmailAddress",
]);

interface RequestedClientField {
  label: string;
  value: string;
}

interface BadgeMeta {
  label: string;
  tone: "neutral" | "success" | "warning" | "danger" | "info";
}

const EMAIL_PATTERN = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i;
const PHONE_PATTERN = /(?:\+?\d[\d\s().-]{7,}\d)/;
const MAX_RENDERED_COMMUNICATION_ITEMS = 120;
const COMMUNICATIONS_PAGE_SIZE = 5;
type CommunicationFilter = "all" | "sms" | "calls" | "documents";

export function RecordDetails({ record }: RecordDetailsProps) {
  const [ghlBasicNote, setGhlBasicNote] = useState<GhlClientBasicNotePayload | null>(null);
  const [isLoadingGhlBasicNote, setIsLoadingGhlBasicNote] = useState(false);
  const [ghlBasicNoteError, setGhlBasicNoteError] = useState("");
  const [ghlCommunications, setGhlCommunications] = useState<GhlClientCommunicationsPayload | null>(null);
  const [isLoadingGhlCommunications, setIsLoadingGhlCommunications] = useState(false);
  const [ghlCommunicationsError, setGhlCommunicationsError] = useState("");
  const [visibleCommunicationCount, setVisibleCommunicationCount] = useState(COMMUNICATIONS_PAGE_SIZE);
  const [selectedCommunicationTranscript, setSelectedCommunicationTranscript] = useState<GhlClientCommunicationItem | null>(null);
  const [generatedTranscriptsByMessageId, setGeneratedTranscriptsByMessageId] = useState<Record<string, string>>({});
  const [isGeneratingTranscript, setIsGeneratingTranscript] = useState(false);
  const [transcriptGenerationError, setTranscriptGenerationError] = useState("");
  const [activeCommunicationFilter, setActiveCommunicationFilter] = useState<CommunicationFilter>("all");

  const normalizedClientName = useMemo(() => (record.clientName || "").trim(), [record.clientName]);
  const contractDisplay = useMemo(() => formatMoneyCell(record.contractTotals), [record.contractTotals]);
  const paidDisplay = useMemo(() => formatMoneyCell(record.totalPayments), [record.totalPayments]);
  const debtDisplay = useMemo(() => formatMoneyCell(record.futurePayments), [record.futurePayments]);
  const managerDisplay = useMemo(() => {
    const candidates = [
      (record.closedBy || "").toString().trim(),
      getOptionalRecordText(record, "manager"),
      getOptionalRecordText(record, "assignedManager"),
      getOptionalRecordText(record, "clientManager"),
      getOptionalRecordText(record, "managerName"),
    ];
    for (const candidate of candidates) {
      if (candidate) {
        return candidate;
      }
    }
    return "-";
  }, [record]);
  const companyDisplay = useMemo(() => (record.companyName || "").trim() || "-", [record.companyName]);
  const avatarSource = useMemo(() => resolveAvatarSource(record, ghlBasicNote), [ghlBasicNote, record]);
  const avatarInitials = useMemo(() => buildAvatarInitials(normalizedClientName), [normalizedClientName]);
  const scoreResult = useMemo(() => evaluateClientScore(record), [record]);
  const statusBadge = useMemo(() => resolveStatusBadge(record), [record]);

  const detailsFields = useMemo(
    () =>
      FIELD_DEFINITIONS.filter(
        (field) =>
          !HEADER_FIELD_KEYS.has(field.key) &&
          !PAYMENT_FIELD_KEYS.has(field.key) &&
          !PROFILE_SUMMARY_FIELD_KEYS.has(field.key),
      ),
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

  useEffect(() => {
    if (!normalizedClientName) {
      setGhlCommunications(null);
      setGhlCommunicationsError("");
      setIsLoadingGhlCommunications(false);
      setVisibleCommunicationCount(COMMUNICATIONS_PAGE_SIZE);
      setSelectedCommunicationTranscript(null);
      setGeneratedTranscriptsByMessageId({});
      setIsGeneratingTranscript(false);
      setTranscriptGenerationError("");
      setActiveCommunicationFilter("all");
      return;
    }

    setVisibleCommunicationCount(COMMUNICATIONS_PAGE_SIZE);
    setSelectedCommunicationTranscript(null);
    setGeneratedTranscriptsByMessageId({});
    setIsGeneratingTranscript(false);
    setTranscriptGenerationError("");
    setActiveCommunicationFilter("all");

    const abortController = new AbortController();
    let isActive = true;

    async function loadCommunications() {
      setIsLoadingGhlCommunications(true);
      setGhlCommunicationsError("");

      try {
        const payload = await getGhlClientCommunications(normalizedClientName, {
          signal: abortController.signal,
        });
        if (!isActive) {
          return;
        }
        setGhlCommunications(payload);
      } catch (error) {
        if (!isActive) {
          return;
        }
        setGhlCommunications(null);
        setGhlCommunicationsError(error instanceof Error ? error.message : "Failed to load client communication history.");
      } finally {
        if (isActive) {
          setIsLoadingGhlCommunications(false);
        }
      }
    }

    void loadCommunications();

    return () => {
      isActive = false;
      abortController.abort();
    };
  }, [normalizedClientName]);

  const contactInfo = useMemo(() => resolveContactInfo(record, ghlBasicNote), [ghlBasicNote, record]);
  const communicationItemsLoaded = useMemo(
    () => (ghlCommunications?.items || []).slice(0, MAX_RENDERED_COMMUNICATION_ITEMS),
    [ghlCommunications?.items],
  );
  const communicationItemsFiltered = useMemo(
    () =>
      communicationItemsLoaded.filter((item) => {
        if (activeCommunicationFilter === "sms") {
          return normalizeCommunicationKind(item.kind) === "sms";
        }
        if (activeCommunicationFilter === "calls") {
          return normalizeCommunicationKind(item.kind) === "call";
        }
        if (activeCommunicationFilter === "documents") {
          return hasCommunicationDocuments(item);
        }
        return true;
      }),
    [activeCommunicationFilter, communicationItemsLoaded],
  );
  const communicationItemsVisible = useMemo(
    () => communicationItemsFiltered.slice(0, visibleCommunicationCount),
    [communicationItemsFiltered, visibleCommunicationCount],
  );
  const hiddenCommunicationCount = Math.max(0, communicationItemsFiltered.length - communicationItemsVisible.length);
  const truncatedByServerCount = Math.max(0, (ghlCommunications?.items?.length || 0) - communicationItemsLoaded.length);
  const nextBatchSize = Math.min(COMMUNICATIONS_PAGE_SIZE, hiddenCommunicationCount);
  const communicationEmptyMessage = useMemo(() => {
    if (activeCommunicationFilter === "sms") {
      return "No SMS history found for this client.";
    }
    if (activeCommunicationFilter === "calls") {
      return "No call history found for this client.";
    }
    if (activeCommunicationFilter === "documents") {
      return "No documents found for this client.";
    }
    return "No SMS or call history found for this client.";
  }, [activeCommunicationFilter]);
  const selectedCommunicationTranscriptText = useMemo(
    () => resolveCommunicationTranscript(selectedCommunicationTranscript, generatedTranscriptsByMessageId),
    [generatedTranscriptsByMessageId, selectedCommunicationTranscript],
  );
  const canTranscribeSelectedCommunication = useMemo(() => {
    if (!selectedCommunicationTranscript || normalizeCommunicationKind(selectedCommunicationTranscript.kind) !== "call") {
      return false;
    }
    if (selectedCommunicationTranscriptText) {
      return false;
    }
    return Boolean(normalizedClientName && getCommunicationTranscriptCacheKey(selectedCommunicationTranscript));
  }, [normalizedClientName, selectedCommunicationTranscript, selectedCommunicationTranscriptText]);

  async function handleGenerateTranscript() {
    if (!canTranscribeSelectedCommunication || !selectedCommunicationTranscript) {
      return;
    }

    const transcriptKey = getCommunicationTranscriptCacheKey(selectedCommunicationTranscript);
    if (!transcriptKey) {
      return;
    }

    setIsGeneratingTranscript(true);
    setTranscriptGenerationError("");
    try {
      const payload = await postGhlClientCommunicationTranscript(normalizedClientName, transcriptKey);
      const generatedTranscript = (payload.transcript || "").toString().trim();
      if (!generatedTranscript) {
        throw new Error("Transcription returned empty text.");
      }

      setGeneratedTranscriptsByMessageId((previous) => ({
        ...previous,
        [transcriptKey]: generatedTranscript,
      }));
      setSelectedCommunicationTranscript((previous) =>
        previous && getCommunicationTranscriptCacheKey(previous) === transcriptKey
          ? { ...previous, transcript: generatedTranscript }
          : previous,
      );
    } catch (error) {
      setTranscriptGenerationError(error instanceof Error ? error.message : "Failed to generate transcript.");
    } finally {
      setIsGeneratingTranscript(false);
    }
  }

  const requestedClientFields = useMemo<RequestedClientField[]>(
    () =>
      [
        {
          label: "Address",
          value:
            getOptionalRecordText(record, "address") ||
            getOptionalRecordText(record, "clientAddress") ||
            getOptionalRecordText(record, "mailingAddress"),
        },
        {
          label: "Date of Birth",
          value: formatOptionalDate(
            getOptionalRecordText(record, "dateOfBirth") ||
              getOptionalRecordText(record, "dob") ||
              getOptionalRecordText(record, "birthDate"),
          ),
        },
        {
          label: "SSN",
          value: getOptionalRecordText(record, "ssn") || getOptionalRecordText(record, "clientSsn"),
        },
        {
          label: "Credit Monitoring Login",
          value:
            getOptionalRecordText(record, "creditMonitoringLogin") ||
            getOptionalRecordText(record, "monitoringLogin") ||
            getOptionalRecordText(record, "monitoringServiceLogin"),
        },
        {
          label: "Credit Monitoring Password",
          value:
            getOptionalRecordText(record, "creditMonitoringPassword") ||
            getOptionalRecordText(record, "monitoringPassword") ||
            getOptionalRecordText(record, "monitoringServicePassword"),
        },
        {
          label: "Purchased Service",
          value:
            getOptionalRecordText(record, "purchasedService") ||
            getOptionalRecordText(record, "serviceDescription") ||
            getOptionalRecordText(record, "servicePurchased") ||
            (record.serviceType || "").trim(),
        },
      ].map((field) => ({
        ...field,
        value: field.value || "-",
      })),
    [record],
  );
  const paymentScheduleRows = useMemo(
    () =>
      PAYMENT_PAIRS.map(([paymentField, paymentDateField], index) => ({
        id: `payment-${index + 1}`,
        label: `Payment ${index + 1}`,
        amount: record[paymentField] ? formatMoneyCell(record[paymentField]) : "-",
        date: record[paymentDateField] ? formatDate(record[paymentDateField]) : "-",
      })),
    [record],
  );

  return (
    <div className="record-details-stack">
      <div className="record-details-layout">
        <div className="record-details-layout__main">
          <section className="record-profile-header">
            <div className="record-profile-header__identity">
              <div className="record-profile-header__avatar" aria-hidden="true">
                {avatarSource ? (
                  <img src={avatarSource} alt="" className="record-profile-header__avatar-image" />
                ) : (
                  <span className="record-profile-header__avatar-fallback">{avatarInitials}</span>
                )}
              </div>
              <div className="record-profile-header__identity-main">
                <div className="record-profile-header__title-row">
                  <h4 className="record-profile-header__name">{normalizedClientName || "Unnamed client"}</h4>
                  <div className="record-profile-header__chips">
                    <Badge tone={scoreResult.tone}>Score: {scoreResult.displayScore === null ? "N/A" : scoreResult.displayScore}</Badge>
                    <Badge tone={statusBadge.tone}>{statusBadge.label}</Badge>
                  </div>
                </div>
                <p className="record-profile-header__summary-line">
                  <span>
                    Contract: <strong>{contractDisplay}</strong>
                  </span>
                  <span>
                    Paid: <strong>{paidDisplay}</strong>
                  </span>
                  <span>
                    Debt: <strong>{debtDisplay}</strong>
                  </span>
                </p>
                <p className="record-profile-header__manager-line">
                  <span className="record-profile-header__manager-label">Manager:</span>
                  <strong>{managerDisplay}</strong>
                </p>
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

          <section className="record-details-grid record-details-grid--requested">
            {requestedClientFields.map((field) => (
              <div key={field.label} className="record-details-grid__item">
                <span className="record-details-grid__label">{field.label}</span>
                <strong className="record-details-grid__value">{field.value}</strong>
              </div>
            ))}
          </section>

          <section className="record-payments-panel">
            <div className="record-payments-panel__header">
              <h4 className="record-payments-panel__title">Payments</h4>
            </div>
            <div className="record-payments-panel__rows" role="list">
              {paymentScheduleRows.map((payment) => (
                <div key={payment.id} className="record-payments-panel__row" role="listitem">
                  <span className="record-payments-panel__label">{payment.label}</span>
                  <span className="record-payments-panel__amount">{payment.amount}</span>
                  <span className="record-payments-panel__date">{payment.date}</span>
                </div>
              ))}
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
        </div>

        <aside className="record-details-layout__sidebar">
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
              <h4 className="record-details-ghl-note__title">Memo</h4>
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

          <section className="record-details-ghl-note" aria-live="polite">
            <div className="record-details-ghl-note__header">
              <h4 className="record-details-ghl-note__title">SMS & Calls</h4>
              {ghlCommunications?.contactName ? (
                <p className="react-user-footnote">
                  Contact: {ghlCommunications.contactName}
                  {ghlCommunications.contactId ? ` (${ghlCommunications.contactId})` : ""}
                </p>
              ) : null}
            </div>

            {isLoadingGhlCommunications ? <p className="react-user-footnote">Loading communications...</p> : null}
            {!isLoadingGhlCommunications && ghlCommunicationsError ? (
              <p className="record-details-ghl-note__error">{ghlCommunicationsError}</p>
            ) : null}
            {!isLoadingGhlCommunications &&
            !ghlCommunicationsError &&
            (!ghlCommunications || ghlCommunications.status !== "found") ? (
              <p className="react-user-footnote">Client was not found in GoHighLevel communications.</p>
            ) : null}
            {!isLoadingGhlCommunications &&
            !ghlCommunicationsError &&
            ghlCommunications?.status === "found" &&
            communicationItemsLoaded.length === 0 ? (
              <p className="react-user-footnote">No SMS or call history found for this client.</p>
            ) : null}
            {!isLoadingGhlCommunications &&
            !ghlCommunicationsError &&
            ghlCommunications?.status === "found" &&
            communicationItemsLoaded.length > 0 ? (
              <>
                <p className="react-user-footnote">
                  SMS: {ghlCommunications.smsCount || 0} · Calls: {ghlCommunications.callCount || 0}
                </p>
                <div className="record-details-communications__filters">
                  <Button
                    type="button"
                    size="sm"
                    variant={activeCommunicationFilter === "sms" ? "primary" : "secondary"}
                    onClick={() => {
                      setVisibleCommunicationCount(COMMUNICATIONS_PAGE_SIZE);
                      setActiveCommunicationFilter((previous) => (previous === "sms" ? "all" : "sms"));
                    }}
                  >
                    SMS
                  </Button>
                  <Button
                    type="button"
                    size="sm"
                    variant={activeCommunicationFilter === "calls" ? "primary" : "secondary"}
                    onClick={() => {
                      setVisibleCommunicationCount(COMMUNICATIONS_PAGE_SIZE);
                      setActiveCommunicationFilter((previous) => (previous === "calls" ? "all" : "calls"));
                    }}
                  >
                    Звонки
                  </Button>
                  <Button
                    type="button"
                    size="sm"
                    variant={activeCommunicationFilter === "documents" ? "primary" : "secondary"}
                    onClick={() => {
                      setVisibleCommunicationCount(COMMUNICATIONS_PAGE_SIZE);
                      setActiveCommunicationFilter((previous) => (previous === "documents" ? "all" : "documents"));
                    }}
                  >
                    Документы
                  </Button>
                </div>
                {communicationItemsVisible.length === 0 ? (
                  <p className="react-user-footnote">{communicationEmptyMessage}</p>
                ) : (
                  <>
                    <div className="record-details-communications__list" role="list">
                      {communicationItemsVisible.map((item) => {
                        const normalizedKind = normalizeCommunicationKind(item.kind);
                        const transcriptText = resolveCommunicationTranscript(item, generatedTranscriptsByMessageId);
                        return (
                          <article key={item.id} className="record-details-communications__item" role="listitem">
                            <div className="record-details-communications__meta">
                              <span className={`record-details-communications__kind record-details-communications__kind--${normalizedKind}`}>
                                {formatCommunicationKind(item.kind)}
                              </span>
                              <span className="record-details-communications__direction">{formatCommunicationDirection(item.direction)}</span>
                              <span className="record-details-communications__date">{formatOptionalDateTime(item.createdAt)}</span>
                            </div>
                            <p className="record-details-communications__body">{item.body || "No text body."}</p>
                            {normalizedKind === "call" ? (
                              <div className="record-details-communications__actions">
                                <Button
                                  variant="secondary"
                                  size="sm"
                                  type="button"
                                  onClick={() => {
                                    setTranscriptGenerationError("");
                                    setSelectedCommunicationTranscript(item);
                                  }}
                                >
                                  {transcriptText ? "Transcript" : "Transcript (not available)"}
                                </Button>
                              </div>
                            ) : null}
                            {item.recordingUrls && item.recordingUrls.length > 0 ? (
                              <div className="record-details-communications__recordings">
                                {item.recordingUrls.map((recordingUrl, index) => (
                                  <div key={`${item.id}:${recordingUrl}:${index}`} className="record-details-communications__recording-item">
                                    <span className="record-details-communications__recording-label">Recording {index + 1}</span>
                                    <audio className="record-details-communications__audio" controls preload="none" src={recordingUrl}>
                                      Your browser does not support audio playback.
                                    </audio>
                                    <a
                                      href={recordingUrl}
                                      target="_blank"
                                      rel="noopener noreferrer"
                                      className="record-details-communications__recording-link"
                                    >
                                      Open in new tab
                                    </a>
                                  </div>
                                ))}
                              </div>
                            ) : null}
                            {item.attachmentUrls && item.attachmentUrls.length > 0 ? (
                              <div className="record-details-communications__recordings">
                                {item.attachmentUrls.map((attachmentUrl, index) => (
                                  <a
                                    key={`${item.id}:${attachmentUrl}:${index}`}
                                    href={attachmentUrl}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    className="record-details-communications__recording-link"
                                  >
                                    Attachment {index + 1}
                                  </a>
                                ))}
                              </div>
                            ) : null}
                          </article>
                        );
                      })}
                    </div>
                    {hiddenCommunicationCount > 0 ? (
                      <div className="record-details-communications__footer">
                        <Button
                          variant="secondary"
                          size="sm"
                          type="button"
                          onClick={() => setVisibleCommunicationCount((current) => current + COMMUNICATIONS_PAGE_SIZE)}
                        >
                          Show {nextBatchSize} more
                        </Button>
                      </div>
                    ) : null}
                    {hiddenCommunicationCount > 0 ? (
                      <p className="react-user-footnote">
                        Showing {communicationItemsVisible.length} of {communicationItemsFiltered.length}.
                      </p>
                    ) : null}
                  </>
                )}
                {truncatedByServerCount > 0 ? (
                  <p className="react-user-footnote">
                    Additional {truncatedByServerCount} messages are not displayed due to server cap.
                  </p>
                ) : null}
              </>
            ) : null}
          </section>
        </aside>
      </div>
      <Modal
        open={Boolean(selectedCommunicationTranscript)}
        title="Call Transcript"
        onClose={() => {
          setSelectedCommunicationTranscript(null);
          setTranscriptGenerationError("");
          setIsGeneratingTranscript(false);
        }}
        footer={
          <>
            {canTranscribeSelectedCommunication ? (
              <Button type="button" onClick={() => void handleGenerateTranscript()} isLoading={isGeneratingTranscript}>
                Transcribe
              </Button>
            ) : null}
            <Button
              type="button"
              variant="secondary"
              onClick={() => {
                setSelectedCommunicationTranscript(null);
                setTranscriptGenerationError("");
                setIsGeneratingTranscript(false);
              }}
            >
              Close
            </Button>
          </>
        }
      >
        {selectedCommunicationTranscript ? (
          <div className="record-details-communications__transcript-modal">
            <p className="react-user-footnote">
              {formatCommunicationDirection(selectedCommunicationTranscript.direction)} ·{" "}
              {formatOptionalDateTime(selectedCommunicationTranscript.createdAt)}
            </p>
            {selectedCommunicationTranscriptText ? (
              <pre className="record-details-communications__transcript">{selectedCommunicationTranscriptText}</pre>
            ) : (
              <p className="react-user-footnote">
                {isGeneratingTranscript
                  ? "Transcribing call recording..."
                  : "Transcript is not available for this call yet. Press Transcribe to generate it now."}
              </p>
            )}
            {!isGeneratingTranscript && transcriptGenerationError ? (
              <p className="record-details-ghl-note__error">{transcriptGenerationError}</p>
            ) : null}
          </div>
        ) : null}
      </Modal>
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

function formatOptionalDate(rawValue: string): string {
  if (!rawValue) {
    return "";
  }
  return formatDate(rawValue);
}

function formatOptionalDateTime(rawValue: string): string {
  const date = (rawValue || "").toString().trim();
  if (!date) {
    return "-";
  }
  const timestamp = Date.parse(date);
  if (!Number.isFinite(timestamp)) {
    return "-";
  }

  return new Intl.DateTimeFormat("en-US", {
    month: "2-digit",
    day: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(new Date(timestamp));
}

function normalizeCommunicationKind(rawKind: string): "sms" | "call" {
  const kind = (rawKind || "").toString().trim().toLowerCase();
  if (kind.includes("call") || kind.includes("voice") || kind.includes("voicemail")) {
    return "call";
  }
  return "sms";
}

function formatCommunicationKind(rawKind: string): string {
  return normalizeCommunicationKind(rawKind) === "call" ? "Call" : "SMS";
}

function formatCommunicationDirection(rawDirection: GhlClientCommunicationDirection): string {
  const direction = (rawDirection || "").toString().trim().toLowerCase();
  if (direction === "inbound" || direction === "in") {
    return "Inbound";
  }
  if (direction === "outbound" || direction === "out") {
    return "Outbound";
  }
  return "Unknown";
}

function getCommunicationTranscriptCacheKey(item: Pick<GhlClientCommunicationItem, "id" | "messageId"> | null): string {
  return ((item?.messageId || item?.id || "").toString().trim());
}

function resolveCommunicationTranscript(
  item: GhlClientCommunicationItem | null,
  generatedTranscriptsByMessageId: Record<string, string> = {},
): string {
  const transcriptKey = getCommunicationTranscriptCacheKey(item);
  if (transcriptKey) {
    const generatedTranscript = (generatedTranscriptsByMessageId?.[transcriptKey] || "").toString().trim();
    if (generatedTranscript) {
      return generatedTranscript;
    }
  }

  const transcript = (item?.transcript || "").toString().trim();
  if (transcript) {
    return transcript;
  }

  if (normalizeCommunicationKind(item?.kind || "") !== "call") {
    return "";
  }

  const body = (item?.body || "").toString().trim();
  if (!body || body.toLowerCase() === "no text body.") {
    return "";
  }
  return body;
}

function hasCommunicationDocuments(item: GhlClientCommunicationItem): boolean {
  return Array.isArray(item?.attachmentUrls) && item.attachmentUrls.length > 0;
}

function resolveStatusBadge(record: ClientRecord): BadgeMeta {
  const status = getRecordStatusFlags(record);

  if (status.isWrittenOff) {
    return { label: "Status: Written Off", tone: "danger" };
  }

  if (status.isFullyPaid) {
    return { label: "Status: Fully Paid", tone: "success" };
  }

  if (status.isAfterResult) {
    return { label: "Status: After Result", tone: "info" };
  }

  if (status.isOverdue) {
    return { label: `Status: Overdue ${status.overdueRange}`, tone: "warning" };
  }

  return { label: "Status: Active", tone: "neutral" };
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

function resolveAvatarSource(record: ClientRecord, ghlBasicNote: GhlClientBasicNotePayload | null): string {
  const recordMap = record as unknown as Record<string, unknown>;
  const noteMap = (ghlBasicNote || {}) as unknown as Record<string, unknown>;
  const candidates = [
    recordMap.avatarUrl,
    recordMap.photoUrl,
    recordMap.profilePhotoUrl,
    recordMap.clientPhotoUrl,
    recordMap.imageUrl,
    recordMap.photo,
    noteMap.contactAvatarUrl,
    noteMap.contactPhotoUrl,
    noteMap.contactImageUrl,
  ];

  for (const candidate of candidates) {
    const normalizedUrl = normalizeAvatarUrl(candidate);
    if (normalizedUrl) {
      return normalizedUrl;
    }
  }

  return "";
}

function normalizeAvatarUrl(rawValue: unknown): string {
  const value = (rawValue || "").toString().trim();
  if (!value) {
    return "";
  }

  if (/^https?:\/\/\S+$/i.test(value)) {
    return value;
  }

  if (/^data:image\//i.test(value)) {
    return value;
  }

  return "";
}

function buildAvatarInitials(clientName: string): string {
  const normalized = (clientName || "").trim();
  if (!normalized) {
    return "NA";
  }

  const tokens = normalized
    .split(/\s+/)
    .map((token) => token.replace(/[^A-Za-zА-Яа-яЁё0-9]/g, ""))
    .filter(Boolean);

  if (!tokens.length) {
    return "NA";
  }

  if (tokens.length === 1) {
    return tokens[0].slice(0, 2).toUpperCase();
  }

  return `${tokens[0][0]}${tokens[tokens.length - 1][0]}`.toUpperCase();
}
