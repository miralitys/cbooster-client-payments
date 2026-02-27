import { useEffect, useMemo, useRef, useState } from "react";

import {
  getGhlClientBasicNote,
  getGhlClientCommunications,
  getQuickBooksPendingConfirmations,
  postGhlClientCommunicationNormalizeTranscripts,
  postGhlClientCommunicationTranscript,
} from "@/shared/api";
import { evaluateClientScore } from "@/features/client-score/domain/scoring";
import type { GhlClientBasicNotePayload } from "@/shared/types/ghlNotes";
import type {
  GhlClientCommunicationDirection,
  GhlClientCommunicationItem,
  GhlClientCommunicationsPayload,
} from "@/shared/types/ghlCommunications";
import type { QuickBooksPendingConfirmationRow } from "@/shared/types/quickbooks";
import type { ClientRecord } from "@/shared/types/records";
import { FIELD_DEFINITIONS, PAYMENT_PAIRS } from "@/features/client-payments/domain/constants";
import { formatDate, formatMoney, getRecordStatusFlags, parseMoneyValue } from "@/features/client-payments/domain/calculations";
import { Badge, Button, Modal } from "@/shared/ui";

interface RecordDetailsProps {
  record: ClientRecord;
  clientManagerLabel?: string;
  canRefreshClientManager?: boolean;
  isRefreshingClientManager?: boolean;
  onRefreshClientManager?: (clientName: string) => Promise<void>;
  canRefreshClientPhone?: boolean;
  isRefreshingClientPhone?: boolean;
  onRefreshClientPhone?: (clientName: string) => Promise<void>;
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
const WORKFLOW_SUMMARY_FIELD_KEYS = new Set<keyof ClientRecord>([
  "closedBy",
  "active",
  "contractCompleted",
  "afterResult",
  "writtenOff",
  "dateWhenFullyPaid",
  "notes",
]);

interface RequestedClientField {
  label: string;
  value: string;
}

interface BadgeMeta {
  label: string;
  tone: "neutral" | "success" | "warning" | "danger" | "info";
}

interface WorkflowSummaryField {
  key: string;
  label: string;
  value: string;
}

const EMAIL_PATTERN = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i;
const PHONE_PATTERN = /(?:\+?\d[\d\s().-]{7,}\d)/;
const MAX_RENDERED_COMMUNICATION_ITEMS = 120;
const COMMUNICATIONS_PAGE_SIZE = 5;
const NOTE_PREVIEW_MAX_LINES = 7;
const TRANSCRIPT_NORMALIZE_BATCH_LIMIT = 240;
const DEFAULT_OWNER_COMPANY_LABEL = "Credit Booster";
const OWNER_COMPANY_FIELD_ALIASES = [
  "portfolioCompany",
  "boosterCompany",
  "agencyCompany",
  "companyBrand",
  "baseCompany",
];
type CommunicationFilter = "all" | "sms" | "calls" | "documents";
type SpeakerRole = "manager" | "client";

interface SpeakerTranscriptTurn {
  id: string;
  role: SpeakerRole;
  label: string;
  text: string;
}

export function RecordDetails({
  record,
  clientManagerLabel,
  canRefreshClientManager = false,
  isRefreshingClientManager = false,
  onRefreshClientManager,
  canRefreshClientPhone = false,
  isRefreshingClientPhone = false,
  onRefreshClientPhone,
}: RecordDetailsProps) {
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
  const [isNormalizingTranscripts, setIsNormalizingTranscripts] = useState(false);
  const [transcriptNormalizeError, setTranscriptNormalizeError] = useState("");
  const [activeCommunicationFilter, setActiveCommunicationFilter] = useState<CommunicationFilter>("all");
  const [pendingQuickBooksMatches, setPendingQuickBooksMatches] = useState<QuickBooksPendingConfirmationRow[]>([]);
  const [isBasicInfoExpanded, setIsBasicInfoExpanded] = useState(false);
  const [isMemoExpanded, setIsMemoExpanded] = useState(false);
  const [clientManagerRefreshError, setClientManagerRefreshError] = useState("");
  const [clientPhoneRefreshError, setClientPhoneRefreshError] = useState("");
  const transcriptNormalizationByClientRef = useRef<Record<string, boolean>>({});

  const normalizedClientName = useMemo(() => (record.clientName || "").trim(), [record.clientName]);
  const contractDisplay = useMemo(() => formatMoneyCell(record.contractTotals), [record.contractTotals]);
  const paidDisplay = useMemo(() => formatMoneyCell(record.totalPayments), [record.totalPayments]);
  const debtDisplay = useMemo(() => formatMoneyCell(record.futurePayments), [record.futurePayments]);
  const salesManagerDisplay = useMemo(() => {
    const candidates = [
      (record.closedBy || "").toString().trim(),
      getOptionalRecordText(record, "manager"),
      getOptionalRecordText(record, "assignedManager"),
      getOptionalRecordText(record, "managerName"),
    ];
    for (const candidate of candidates) {
      if (candidate) {
        return candidate;
      }
    }
    return "-";
  }, [record]);
  const clientManagerDisplay = useMemo(() => {
    const fromProp = (clientManagerLabel || "").toString().trim();
    if (fromProp) {
      return fromProp;
    }
    return getOptionalRecordText(record, "clientManager") || "-";
  }, [clientManagerLabel, record]);
  const companyDisplay = useMemo(() => (record.companyName || "").trim() || "-", [record.companyName]);
  const ownerCompanyDisplay = useMemo(() => resolveOwnerCompanyLabel(record), [record]);
  const avatarSource = useMemo(() => resolveAvatarSource(record, ghlBasicNote), [ghlBasicNote, record]);
  const avatarInitials = useMemo(() => buildAvatarInitials(normalizedClientName), [normalizedClientName]);
  const scoreResult = useMemo(() => evaluateClientScore(record), [record]);
  const statusBadge = useMemo(() => resolveStatusBadge(record), [record]);
  const canRefreshClientManagerForCurrentRecord = Boolean(
    canRefreshClientManager && normalizedClientName && typeof onRefreshClientManager === "function",
  );
  const canRefreshClientPhoneForCurrentRecord = Boolean(
    canRefreshClientPhone && normalizedClientName && typeof onRefreshClientPhone === "function",
  );
  const basicInfoPreview = useMemo(
    () => buildMultilinePreview(ghlBasicNote?.noteBody || "", NOTE_PREVIEW_MAX_LINES, isBasicInfoExpanded),
    [ghlBasicNote?.noteBody, isBasicInfoExpanded],
  );
  const memoPreview = useMemo(
    () => buildMultilinePreview(ghlBasicNote?.memoBody || "", NOTE_PREVIEW_MAX_LINES, isMemoExpanded),
    [ghlBasicNote?.memoBody, isMemoExpanded],
  );

  const detailsFields = useMemo(
    () =>
      FIELD_DEFINITIONS.filter(
        (field) =>
          !HEADER_FIELD_KEYS.has(field.key) &&
          !PAYMENT_FIELD_KEYS.has(field.key) &&
          !PROFILE_SUMMARY_FIELD_KEYS.has(field.key) &&
          !WORKFLOW_SUMMARY_FIELD_KEYS.has(field.key),
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
    setClientManagerRefreshError("");
    setClientPhoneRefreshError("");
    setIsBasicInfoExpanded(false);
    setIsMemoExpanded(false);
  }, [normalizedClientName]);

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
      setIsNormalizingTranscripts(false);
      setTranscriptNormalizeError("");
      setActiveCommunicationFilter("all");
      return;
    }

    setVisibleCommunicationCount(COMMUNICATIONS_PAGE_SIZE);
    setSelectedCommunicationTranscript(null);
    setGeneratedTranscriptsByMessageId({});
    setIsGeneratingTranscript(false);
    setTranscriptGenerationError("");
    setIsNormalizingTranscripts(false);
    setTranscriptNormalizeError("");
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

  useEffect(() => {
    if (!normalizedClientName || !ghlCommunications || ghlCommunications.status !== "found") {
      return;
    }
    if (isLoadingGhlCommunications || ghlCommunicationsError) {
      return;
    }
    if (transcriptNormalizationByClientRef.current[normalizedClientName]) {
      return;
    }

    const callItemsWithLegacyTranscript = (Array.isArray(ghlCommunications.items) ? ghlCommunications.items : []).filter((item) => {
      if (normalizeCommunicationKind(item.kind) !== "call") {
        return false;
      }
      const transcript = (item.transcript || "").toString().trim();
      return Boolean(transcript && !isSpeakerLabeledTranscript(transcript));
    });
    if (!callItemsWithLegacyTranscript.length) {
      return;
    }

    transcriptNormalizationByClientRef.current[normalizedClientName] = true;
    let isActive = true;
    setIsNormalizingTranscripts(true);
    setTranscriptNormalizeError("");

    async function normalizeExistingTranscripts() {
      try {
        const payload = await postGhlClientCommunicationNormalizeTranscripts(normalizedClientName, {
          limit: TRANSCRIPT_NORMALIZE_BATCH_LIMIT,
        });
        if (!isActive) {
          return;
        }

        const entries = Array.isArray(payload.entries) ? payload.entries : [];
        if (!entries.length) {
          return;
        }

        const nextTranscriptsByMessageId: Record<string, string> = {};
        for (const entry of entries) {
          const messageId = (entry?.messageId || "").toString().trim();
          const transcript = (entry?.transcript || entry?.formattedTranscript || entry?.rawTranscript || "").toString().trim();
          if (!messageId || !transcript) {
            continue;
          }
          nextTranscriptsByMessageId[messageId] = transcript;
        }
        if (!Object.keys(nextTranscriptsByMessageId).length) {
          return;
        }

        setGeneratedTranscriptsByMessageId((previous) => ({
          ...previous,
          ...nextTranscriptsByMessageId,
        }));
        setGhlCommunications((previous) => {
          if (!previous) {
            return previous;
          }
          let hasChanges = false;
          const nextItems = (Array.isArray(previous.items) ? previous.items : []).map((item) => {
            const transcriptKey = getCommunicationTranscriptCacheKey(item);
            const normalizedTranscript = transcriptKey ? nextTranscriptsByMessageId[transcriptKey] : "";
            if (!normalizedTranscript) {
              return item;
            }
            if ((item.transcript || "").toString().trim() === normalizedTranscript) {
              return item;
            }
            hasChanges = true;
            return {
              ...item,
              transcript: normalizedTranscript,
            };
          });
          if (!hasChanges) {
            return previous;
          }
          return {
            ...previous,
            items: nextItems,
          };
        });
        setSelectedCommunicationTranscript((previous) => {
          if (!previous) {
            return previous;
          }
          const transcriptKey = getCommunicationTranscriptCacheKey(previous);
          if (!transcriptKey) {
            return previous;
          }
          const normalizedTranscript = nextTranscriptsByMessageId[transcriptKey];
          if (!normalizedTranscript) {
            return previous;
          }
          return {
            ...previous,
            transcript: normalizedTranscript,
          };
        });
      } catch (error) {
        if (!isActive) {
          return;
        }
        setTranscriptNormalizeError(error instanceof Error ? error.message : "Failed to normalize existing transcripts.");
      } finally {
        if (isActive) {
          setIsNormalizingTranscripts(false);
        }
      }
    }

    void normalizeExistingTranscripts();

    return () => {
      isActive = false;
    };
  }, [ghlCommunications, ghlCommunicationsError, isLoadingGhlCommunications, normalizedClientName]);

  const contactInfo = useMemo(() => resolveContactInfo(record, ghlBasicNote), [ghlBasicNote, record]);
  const communicationItemsLoaded = useMemo(
    () => (ghlCommunications?.items || []).slice(0, MAX_RENDERED_COMMUNICATION_ITEMS),
    [ghlCommunications?.items],
  );
  const hasSmsItems = useMemo(
    () => communicationItemsLoaded.some((item) => normalizeCommunicationKind(item.kind) === "sms"),
    [communicationItemsLoaded],
  );
  const hasCallItems = useMemo(
    () => communicationItemsLoaded.some((item) => normalizeCommunicationKind(item.kind) === "call"),
    [communicationItemsLoaded],
  );
  const hasDocumentItems = useMemo(
    () => communicationItemsLoaded.some((item) => hasCommunicationDocuments(item)),
    [communicationItemsLoaded],
  );

  useEffect(() => {
    if (activeCommunicationFilter === "calls" && !hasCallItems) {
      setActiveCommunicationFilter("all");
      setVisibleCommunicationCount(COMMUNICATIONS_PAGE_SIZE);
      return;
    }
    if (activeCommunicationFilter === "documents" && !hasDocumentItems) {
      setActiveCommunicationFilter("all");
      setVisibleCommunicationCount(COMMUNICATIONS_PAGE_SIZE);
    }
  }, [activeCommunicationFilter, hasCallItems, hasDocumentItems]);

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
  const selectedCommunicationTranscriptTurns = useMemo(
    () => parseSpeakerTranscriptTurns(selectedCommunicationTranscriptText),
    [selectedCommunicationTranscriptText],
  );
  const selectedCommunicationNeedsFormatting = useMemo(
    () => Boolean(selectedCommunicationTranscriptText && !isSpeakerLabeledTranscript(selectedCommunicationTranscriptText)),
    [selectedCommunicationTranscriptText],
  );
  const canTranscribeSelectedCommunication = useMemo(() => {
    if (!selectedCommunicationTranscript || normalizeCommunicationKind(selectedCommunicationTranscript.kind) !== "call") {
      return false;
    }
    const transcriptKey = getCommunicationTranscriptCacheKey(selectedCommunicationTranscript);
    if (!normalizedClientName || !transcriptKey) {
      return false;
    }
    if (!selectedCommunicationTranscriptText) {
      return true;
    }
    return selectedCommunicationNeedsFormatting;
  }, [normalizedClientName, selectedCommunicationNeedsFormatting, selectedCommunicationTranscript, selectedCommunicationTranscriptText]);

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
      ].filter((field) => Boolean((field.value || "").trim())),
    [record],
  );
  const workflowSummaryFields = useMemo<WorkflowSummaryField[]>(
    () => [
      {
        key: "contract-completed",
        label: "Contract",
        value: formatFieldValue("contractCompleted", "checkbox", record.contractCompleted || ""),
      },
      {
        key: "after-result",
        label: "After Result",
        value: formatFieldValue("afterResult", "checkbox", record.afterResult || ""),
      },
      {
        key: "written-off",
        label: "Written Off",
        value: formatFieldValue("writtenOff", "checkbox", record.writtenOff || ""),
      },
      {
        key: "date-fully-paid",
        label: "Date When Fully Paid",
        value: formatDate(record.dateWhenFullyPaid || ""),
      },
    ],
    [record.afterResult, record.contractCompleted, record.dateWhenFullyPaid, record.writtenOff],
  );
  const workflowNotes = useMemo(() => getOptionalRecordText(record, "notes"), [record]);
  const pendingQuickBooksFields = useMemo(() => {
    const fields = new Set<string>();
    for (const item of pendingQuickBooksMatches) {
      const paymentField = String(item?.matchedPaymentField || "").trim();
      if (paymentField) {
        fields.add(paymentField);
      }
    }
    return fields;
  }, [pendingQuickBooksMatches]);
  const paymentScheduleRows = useMemo(
    () =>
      PAYMENT_PAIRS.map(([paymentField, paymentDateField], index) => ({
        id: `payment-${index + 1}`,
        paymentField: String(paymentField),
        label: `Payment ${index + 1}`,
        amount: record[paymentField] ? formatMoneyCell(record[paymentField]) : "-",
        date: record[paymentDateField] ? formatDate(record[paymentDateField]) : "-",
        pending: pendingQuickBooksFields.has(String(paymentField)),
      })),
    [pendingQuickBooksFields, record],
  );
  const visiblePaymentScheduleRows = useMemo(() => {
    const rowsWithValues = paymentScheduleRows.filter((payment) => payment.amount !== "-" || payment.date !== "-");
    if (rowsWithValues.length) {
      return rowsWithValues;
    }
    return paymentScheduleRows.slice(0, 1);
  }, [paymentScheduleRows]);
  const hiddenPaymentRowsCount = Math.max(0, paymentScheduleRows.length - visiblePaymentScheduleRows.length);
  const activeClientDisplay = useMemo(() => formatActiveClientValue(record.active), [record.active]);

  useEffect(() => {
    const recordId = String(record.id || "").trim();
    if (!recordId) {
      setPendingQuickBooksMatches([]);
      return;
    }

    let isActive = true;
    const abortController = new AbortController();

    void getQuickBooksPendingConfirmations(recordId)
      .then((payload) => {
        if (!isActive) {
          return;
        }
        setPendingQuickBooksMatches(Array.isArray(payload?.items) ? payload.items : []);
      })
      .catch(() => {
        if (!isActive) {
          return;
        }
        setPendingQuickBooksMatches([]);
      });

    return () => {
      isActive = false;
      abortController.abort();
    };
  }, [record.id]);

  async function handleRefreshClientManagerClick() {
    if (!canRefreshClientManagerForCurrentRecord || !onRefreshClientManager) {
      return;
    }

    setClientManagerRefreshError("");
    try {
      await onRefreshClientManager(normalizedClientName);
    } catch (error) {
      setClientManagerRefreshError(error instanceof Error ? error.message : "Failed to refresh Client Manager.");
    }
  }

  async function handleRefreshClientPhoneClick() {
    if (!canRefreshClientPhoneForCurrentRecord || !onRefreshClientPhone) {
      return;
    }

    setClientPhoneRefreshError("");
    try {
      await onRefreshClientPhone(normalizedClientName);
    } catch (error) {
      setClientPhoneRefreshError(error instanceof Error ? error.message : "Failed to refresh Phone.");
    }
  }

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
                    <Badge tone="info">{ownerCompanyDisplay} Clients</Badge>
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
                  <span className="record-profile-header__manager-entry">
                    <span className="record-profile-header__manager-label">Sales Manager:</span>
                    <strong>{salesManagerDisplay}</strong>
                  </span>
                  <span className="record-profile-header__manager-entry">
                    <span className="record-profile-header__manager-label">Client Manager:</span>
                    <strong>{clientManagerDisplay}</strong>
                    {canRefreshClientManagerForCurrentRecord ? (
                      <button
                        type="button"
                        className="record-profile-header__manager-refresh-btn"
                        onClick={() => void handleRefreshClientManagerClick()}
                        disabled={isRefreshingClientManager}
                        title="Refresh Client Manager from GoHighLevel"
                        aria-label="Refresh Client Manager from GoHighLevel"
                      >
                        {isRefreshingClientManager ? "..." : "↻"}
                      </button>
                    ) : null}
                  </span>
                </p>
                {clientManagerRefreshError ? (
                  <p className="record-profile-header__manager-error">{clientManagerRefreshError}</p>
                ) : null}
              </div>
            </div>

            <div className="record-profile-header__contact-row">
              <div className="record-profile-header__contact-item">
                <span className="record-profile-header__contact-label-row">
                  <span className="record-profile-header__contact-label">Phone</span>
                  {canRefreshClientPhoneForCurrentRecord ? (
                    <button
                      type="button"
                      className="record-profile-header__contact-refresh-btn"
                      onClick={() => void handleRefreshClientPhoneClick()}
                      disabled={isRefreshingClientPhone}
                      title="Refresh phone from GoHighLevel"
                      aria-label="Refresh phone from GoHighLevel"
                    >
                      {isRefreshingClientPhone ? "..." : "↻"}
                    </button>
                  ) : null}
                </span>
                <strong className="record-profile-header__contact-value">{contactInfo.phone || "-"}</strong>
                {clientPhoneRefreshError ? (
                  <p className="record-profile-header__contact-error">{clientPhoneRefreshError}</p>
                ) : null}
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

            <div className="record-workflow-summary">
              {workflowSummaryFields.map((field) => (
                <div key={field.key} className="record-workflow-summary__item">
                  <span className="record-workflow-summary__label">{field.label}</span>
                  <strong className="record-workflow-summary__value">{field.value || "-"}</strong>
                </div>
              ))}
              {workflowNotes ? (
                <div className="record-workflow-summary__item record-workflow-summary__item--note">
                  <span className="record-workflow-summary__label">Notes</span>
                  <strong className="record-workflow-summary__value">{workflowNotes}</strong>
                </div>
              ) : null}
            </div>
          </section>

          {requestedClientFields.length ? (
            <section className="record-details-grid record-details-grid--requested">
              {requestedClientFields.map((field) => (
                <div key={field.label} className="record-details-grid__item">
                  <span className="record-details-grid__label">{field.label}</span>
                  <strong className="record-details-grid__value">{field.value}</strong>
                </div>
              ))}
            </section>
          ) : null}

          <section className="record-payments-panel">
            <div className="record-payments-panel__header">
              <h4 className="record-payments-panel__title">Payments</h4>
            </div>
            <div className="record-payments-panel__rows" role="list">
              {visiblePaymentScheduleRows.map((payment) => (
                <div key={payment.id} className="record-payments-panel__row" role="listitem">
                  <span className="record-payments-panel__label">{payment.label}</span>
                  <span className="record-payments-panel__amount">{payment.amount}</span>
                  <span className="record-payments-panel__date">{payment.date}</span>
                  {payment.pending ? (
                    <span className="record-payments-panel__pending">Not Confirmed</span>
                  ) : null}
                </div>
              ))}
            </div>
            {hiddenPaymentRowsCount > 0 ? (
              <p className="react-user-footnote">Hidden empty payment rows: {hiddenPaymentRowsCount}</p>
            ) : null}
            <div className="record-details-grid__item">
              <span className="record-details-grid__label">Active Client</span>
              <strong className="record-details-grid__value">{activeClientDisplay}</strong>
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
                <pre className="record-details-ghl-note__body">{basicInfoPreview.text}</pre>
                {basicInfoPreview.hasMore && !isBasicInfoExpanded ? (
                  <div className="record-details-ghl-note__actions">
                    <Button type="button" variant="secondary" size="sm" onClick={() => setIsBasicInfoExpanded(true)}>
                      Show more
                    </Button>
                  </div>
                ) : null}
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
                <pre className="record-details-ghl-note__body">{memoPreview.text}</pre>
                {memoPreview.hasMore && !isMemoExpanded ? (
                  <div className="record-details-ghl-note__actions">
                    <Button type="button" variant="secondary" size="sm" onClick={() => setIsMemoExpanded(true)}>
                      Show more
                    </Button>
                  </div>
                ) : null}
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
                {isNormalizingTranscripts ? (
                  <p className="react-user-footnote">Formatting existing call transcripts...</p>
                ) : null}
                {!isNormalizingTranscripts && transcriptNormalizeError ? (
                  <p className="record-details-ghl-note__error">{transcriptNormalizeError}</p>
                ) : null}
                <div className="record-details-communications__filters">
                  {hasSmsItems ? (
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
                  ) : null}
                  {hasCallItems ? (
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
                  ) : null}
                  {hasDocumentItems ? (
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
                  ) : null}
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
                {selectedCommunicationNeedsFormatting ? "Format Transcript" : "Transcribe"}
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
              selectedCommunicationTranscriptTurns.length > 0 ? (
                <div className="record-details-communications__transcript-turns">
                  {selectedCommunicationTranscriptTurns.map((turn) => (
                    <article
                      key={turn.id}
                      className={`record-details-communications__transcript-turn record-details-communications__transcript-turn--${turn.role}`}
                    >
                      <header className="record-details-communications__transcript-turn-header">{turn.label}</header>
                      <p className="record-details-communications__transcript-turn-body">{turn.text}</p>
                    </article>
                  ))}
                </div>
              ) : (
                <pre className="record-details-communications__transcript">{selectedCommunicationTranscriptText}</pre>
              )
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

function formatActiveClientValue(rawValue: unknown): string {
  const normalized = (rawValue || "").toString().trim().toLowerCase();
  if (normalized === "yes" || normalized === "true" || normalized === "1" || normalized === "on" || normalized === "active") {
    return "Active";
  }
  return "Inactive";
}

function buildMultilinePreview(
  rawValue: string,
  maxLines: number,
  expanded: boolean,
): {
  text: string;
  hasMore: boolean;
} {
  const normalized = (rawValue || "").toString().replace(/\r\n/g, "\n");
  if (!normalized) {
    return {
      text: "",
      hasMore: false,
    };
  }

  const safeMaxLines = Math.max(1, Math.trunc(maxLines) || 1);
  const lines = normalized.split("\n");
  const hasMore = lines.length > safeMaxLines;
  if (!hasMore || expanded) {
    return {
      text: normalized,
      hasMore,
    };
  }

  return {
    text: lines.slice(0, safeMaxLines).join("\n"),
    hasMore: true,
  };
}

function resolveOwnerCompanyLabel(record: ClientRecord): string {
  const directValue = normalizeKnownOwnerCompanyName((record.ownerCompany || "").toString().trim());
  if (directValue) {
    return directValue;
  }

  for (const alias of OWNER_COMPANY_FIELD_ALIASES) {
    const candidate = normalizeKnownOwnerCompanyName(getOptionalRecordText(record, alias));
    if (candidate) {
      return candidate;
    }
  }

  return DEFAULT_OWNER_COMPANY_LABEL;
}

function normalizeKnownOwnerCompanyName(rawValue: string): string {
  const value = (rawValue || "").toString().trim();
  if (!value) {
    return "";
  }

  const normalized = value.toLowerCase().replace(/\s+/g, " ");
  if (normalized === "credit booster") {
    return "Credit Booster";
  }
  if (normalized === "ramis booster") {
    return "Ramis Booster";
  }
  if (normalized === "wolfowich" || normalized === "wolfovich") {
    return "Wolfowich";
  }

  return value;
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

function isSpeakerLabeledTranscript(rawTranscript: string): boolean {
  const transcript = (rawTranscript || "").toString().trim();
  if (!transcript) {
    return false;
  }
  return /(?:^|\n)\s*(?:manager|client|менеджер|клиент)\s*:/i.test(transcript);
}

function parseSpeakerTranscriptTurns(rawTranscript: string): SpeakerTranscriptTurn[] {
  const transcript = (rawTranscript || "").toString().replace(/\r\n/g, "\n").trim();
  if (!transcript) {
    return [];
  }

  const lines = transcript
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
  if (!lines.length) {
    return [];
  }

  const turns: SpeakerTranscriptTurn[] = [];
  let currentTurn: SpeakerTranscriptTurn | null = null;
  const speakerPattern = /^(?:\[(?:\d{1,2}:)?\d{1,2}:\d{2}\]\s*)?(manager|client|менеджер|клиент)\s*:\s*(.*)$/i;

  for (const line of lines) {
    const match = line.match(speakerPattern);
    if (match) {
      const rawSpeaker = (match[1] || "").toLowerCase();
      const role: SpeakerRole = rawSpeaker === "client" || rawSpeaker === "клиент" ? "client" : "manager";
      const label = role === "manager" ? "Manager" : "Client";
      const text = (match[2] || "").trim();
      currentTurn = {
        id: `${turns.length + 1}-${role}`,
        role,
        label,
        text: text || "...",
      };
      turns.push(currentTurn);
      continue;
    }

    if (currentTurn) {
      currentTurn.text = `${currentTurn.text} ${line}`.trim();
    } else {
      return [];
    }
  }

  return turns.filter((turn) => turn.text);
}

function hasCommunicationDocuments(item: GhlClientCommunicationItem): boolean {
  return Array.isArray(item?.attachmentUrls) && item.attachmentUrls.length > 0;
}

function resolveStatusBadge(record: ClientRecord): BadgeMeta {
  const status = getRecordStatusFlags(record);

  if (!status.isActive) {
    return { label: "Status: Inactive", tone: "neutral" };
  }

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
  if (key === "contractCompleted") {
    return rawValue ? "Completed" : "No completed";
  }

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
