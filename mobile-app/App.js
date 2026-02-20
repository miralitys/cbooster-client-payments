import React, { useCallback, useEffect, useMemo, useState } from "react";
import {
  ActivityIndicator,
  Alert,
  Linking,
  Modal,
  Pressable,
  SafeAreaView,
  ScrollView,
  StatusBar,
  StyleSheet,
  Switch,
  Text,
  TextInput,
  View,
} from "react-native";

const API_BASE_URL = (
  process.env.EXPO_PUBLIC_API_BASE_URL || "https://cbooster-client-payments.onrender.com"
).replace(/\/+$/, "");
const MOBILE_SESSION_HEADER_NAME = "X-CBooster-Session";
const AUTH_STATE_CHECKING = "checking";
const AUTH_STATE_SIGNED_OUT = "signedOut";
const AUTH_STATE_SIGNED_IN = "signedIn";

const DAY_IN_MS = 24 * 60 * 60 * 1000;
const ZERO_TOLERANCE = 0.005;
const DEFAULT_TAB = "clients";
const DEFAULT_STATUS_FILTER = "all";
const DEFAULT_PERIOD = "currentWeek";

const STATUS_FILTER_OPTIONS = [
  { key: "all", label: "All" },
  { key: "written-off", label: "Written Off" },
  { key: "fully-paid", label: "Fully Paid" },
  { key: "after-result", label: "After Result" },
  { key: "overdue", label: "Overdue" },
];

const OVERDUE_RANGE_FILTER_OPTIONS = [
  { key: "", label: "Any" },
  { key: "1-7", label: "1-7" },
  { key: "8-30", label: "8-30" },
  { key: "31-60", label: "31-60" },
  { key: "60+", label: "60+" },
];

const OVERVIEW_PERIOD_OPTIONS = [
  { key: "currentWeek", label: "Current Week" },
  { key: "previousWeek", label: "Previous Week" },
  { key: "currentMonth", label: "Current Month" },
  { key: "last30Days", label: "Last 30 Days" },
];

const PAYMENT_PAIRS = [
  ["payment1", "payment1Date"],
  ["payment2", "payment2Date"],
  ["payment3", "payment3Date"],
  ["payment4", "payment4Date"],
  ["payment5", "payment5Date"],
  ["payment6", "payment6Date"],
  ["payment7", "payment7Date"],
];

const PAYMENT_FIELDS = PAYMENT_PAIRS.map(([paymentKey]) => paymentKey);
const PAYMENT_DATE_FIELDS = PAYMENT_PAIRS.map(([, paymentDateKey]) => paymentDateKey);

const RECORD_TEXT_FIELDS = [
  "clientName",
  "closedBy",
  "companyName",
  "serviceType",
  "contractTotals",
  "totalPayments",
  ...PAYMENT_FIELDS,
  "futurePayments",
  "notes",
  "collection",
  "dateWhenFullyPaid",
  "leadSource",
  "ssn",
  "clientPhoneNumber",
  "futurePayment",
  "identityIq",
  "clientEmailAddress",
];

const RECORD_DATE_FIELDS = [...PAYMENT_DATE_FIELDS, "dateOfCollection", "dateWhenWrittenOff"];
const RECORD_CHECKBOX_FIELDS = ["afterResult", "writtenOff"];

const FIELD_LABELS = {
  clientName: "Client Name",
  closedBy: "Closed By",
  companyName: "Company Name",
  serviceType: "Service Type",
  contractTotals: "Contract Totals",
  totalPayments: "Total Payments",
  payment1: "Payment 1",
  payment1Date: "Payment 1 Date",
  payment2: "Payment 2",
  payment2Date: "Payment 2 Date",
  payment3: "Payment 3",
  payment3Date: "Payment 3 Date",
  payment4: "Payment 4",
  payment4Date: "Payment 4 Date",
  payment5: "Payment 5",
  payment5Date: "Payment 5 Date",
  payment6: "Payment 6",
  payment6Date: "Payment 6 Date",
  payment7: "Payment 7",
  payment7Date: "Payment 7 Date",
  futurePayments: "Future Payments",
  afterResult: "After Result",
  writtenOff: "Written Off",
  notes: "Notes",
  collection: "Collection",
  dateOfCollection: "Date of Collection",
  dateWhenWrittenOff: "Date When Written Off",
  dateWhenFullyPaid: "Date When Fully Paid",
  leadSource: "Lead Source",
  ssn: "SSN",
  clientPhoneNumber: "Client Phone",
  futurePayment: "Future Payment",
  identityIq: "IdentityIQ",
  clientEmailAddress: "Client Email",
};

const FIELD_CONFIG = {
  clientName: { type: "text", required: true, autoCapitalize: "words" },
  closedBy: { type: "text", autoCapitalize: "words" },
  companyName: { type: "text", autoCapitalize: "words" },
  serviceType: { type: "text", autoCapitalize: "words" },
  leadSource: { type: "text", autoCapitalize: "words" },
  clientPhoneNumber: { type: "phone" },
  clientEmailAddress: { type: "email", autoCapitalize: "none" },
  contractTotals: { type: "money" },
  afterResult: { type: "checkbox" },
  writtenOff: { type: "checkbox" },
  dateWhenWrittenOff: { type: "date" },
  payment1: { type: "money" },
  payment1Date: { type: "date" },
  payment2: { type: "money" },
  payment2Date: { type: "date" },
  payment3: { type: "money" },
  payment3Date: { type: "date" },
  payment4: { type: "money" },
  payment4Date: { type: "date" },
  payment5: { type: "money" },
  payment5Date: { type: "date" },
  payment6: { type: "money" },
  payment6Date: { type: "date" },
  payment7: { type: "money" },
  payment7Date: { type: "date" },
  collection: { type: "money" },
  dateOfCollection: { type: "date" },
  notes: { type: "textarea" },
  ssn: { type: "text" },
  identityIq: { type: "textarea" },
  futurePayment: { type: "text" },
};

const FORM_SECTIONS = [
  {
    title: "Client",
    fields: [
      "clientName",
      "closedBy",
      "companyName",
      "serviceType",
      "leadSource",
      "clientPhoneNumber",
      "clientEmailAddress",
    ],
  },
  {
    title: "Contract",
    fields: ["contractTotals", "afterResult", "writtenOff", "dateWhenWrittenOff"],
  },
  {
    title: "Payments",
    fields: [
      "payment1",
      "payment1Date",
      "payment2",
      "payment2Date",
      "payment3",
      "payment3Date",
      "payment4",
      "payment4Date",
      "payment5",
      "payment5Date",
      "payment6",
      "payment6Date",
      "payment7",
      "payment7Date",
    ],
  },
  {
    title: "Collections",
    fields: ["collection", "dateOfCollection"],
  },
  {
    title: "Additional",
    fields: ["notes", "ssn", "identityIq", "futurePayment"],
  },
];

const WRITTEN_OFF_CLIENT_NAMES = new Set(
  [
    "Ghenadie Nipomici",
    "Andrii Kuziv",
    "Alina Seiitbek Kyzy",
    "Syimyk Alymov",
    "Urmatbek Aliman Adi",
    "Maksatbek Nadyrov",
    "Ismayil Hajiyev",
    "Artur Maltsev",
    "Maksim Burlaev",
    "Serhii Vasylchuk",
    "Denys Vatsyk",
    "Rinat Kadirmetov",
    "Pavlo Mykhailov",
  ].map(normalizeClientName),
);

const SUBMISSION_PRIMARY_FIELDS = [
  "clientName",
  "closedBy",
  "companyName",
  "serviceType",
  "contractTotals",
  "payment1",
  "payment1Date",
  "notes",
  "afterResult",
  "writtenOff",
  "leadSource",
  "clientPhoneNumber",
  "clientEmailAddress",
  "futurePayment",
  "identityIq",
];

const MONEY_FORMATTER = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

const KPI_MONEY_FORMATTER = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 0,
  maximumFractionDigits: 0,
});

const DATE_TIME_FORMATTER = new Intl.DateTimeFormat("en-US", {
  year: "numeric",
  month: "short",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
});

export default function App() {
  const [activeTab, setActiveTab] = useState(DEFAULT_TAB);
  const [authState, setAuthState] = useState(AUTH_STATE_CHECKING);
  const [authUser, setAuthUser] = useState("");
  const [mobileSessionToken, setMobileSessionToken] = useState("");
  const [authError, setAuthError] = useState("");
  const [loginUsername, setLoginUsername] = useState("");
  const [loginPassword, setLoginPassword] = useState("");
  const [isSigningIn, setIsSigningIn] = useState(false);

  const [records, setRecords] = useState([]);
  const [recordsError, setRecordsError] = useState("");
  const [isLoadingRecords, setIsLoadingRecords] = useState(false);
  const [isSavingRecords, setIsSavingRecords] = useState(false);
  const [recordsUpdatedAt, setRecordsUpdatedAt] = useState("");

  const [searchQuery, setSearchQuery] = useState("");
  const [statusFilter, setStatusFilter] = useState(DEFAULT_STATUS_FILTER);
  const [overdueRangeFilter, setOverdueRangeFilter] = useState("");
  const [overviewPeriod, setOverviewPeriod] = useState(DEFAULT_PERIOD);

  const [editingRecordId, setEditingRecordId] = useState("");

  const [pendingSubmissions, setPendingSubmissions] = useState([]);
  const [isLoadingModeration, setIsLoadingModeration] = useState(false);
  const [moderationError, setModerationError] = useState("");
  const [activeSubmissionId, setActiveSubmissionId] = useState("");
  const [submissionFiles, setSubmissionFiles] = useState([]);
  const [isLoadingSubmissionFiles, setIsLoadingSubmissionFiles] = useState(false);
  const [submissionFilesError, setSubmissionFilesError] = useState("");
  const [isModerationActionRunning, setIsModerationActionRunning] = useState(false);

  const isAuthenticated = authState === AUTH_STATE_SIGNED_IN;

  const requestApiJson = useCallback(
    (path, options = {}) => {
      return requestJson(path, options, mobileSessionToken);
    },
    [mobileSessionToken],
  );

  const activeSubmission = useMemo(
    () => pendingSubmissions.find((item) => item.id === activeSubmissionId) || null,
    [pendingSubmissions, activeSubmissionId],
  );

  const editingRecord = useMemo(
    () => records.find((item) => item.id === editingRecordId) || null,
    [records, editingRecordId],
  );

  const resetAppDataForSignedOut = useCallback(() => {
    setActiveTab(DEFAULT_TAB);
    setRecords([]);
    setRecordsUpdatedAt("");
    setRecordsError("");
    setPendingSubmissions([]);
    setModerationError("");
    setActiveSubmissionId("");
    setSubmissionFiles([]);
    setSubmissionFilesError("");
    setEditingRecordId("");
    setIsLoadingRecords(false);
    setIsLoadingModeration(false);
    setIsLoadingSubmissionFiles(false);
  }, []);

  const applyUnauthorizedState = useCallback(
    (message = "Session expired. Sign in again.") => {
      setAuthState(AUTH_STATE_SIGNED_OUT);
      setAuthUser("");
      setMobileSessionToken("");
      setAuthError(message);
      resetAppDataForSignedOut();
    },
    [resetAppDataForSignedOut],
  );

  const loadAuthSession = useCallback(async () => {
    setAuthError("");
    setAuthState(AUTH_STATE_CHECKING);
    try {
      const body = await requestJson("/api/auth/session", {}, mobileSessionToken);
      const username = (body?.user?.username || "").toString();
      setAuthUser(username);
      setAuthState(AUTH_STATE_SIGNED_IN);
    } catch (error) {
      setAuthUser("");
      setMobileSessionToken("");
      setAuthState(AUTH_STATE_SIGNED_OUT);
      if (error?.status !== 401) {
        setAuthError(error.message || "Failed to connect to server.");
      }
      resetAppDataForSignedOut();
    }
  }, [mobileSessionToken, resetAppDataForSignedOut]);

  const handleSignIn = useCallback(async () => {
    const username = loginUsername.trim();
    const password = loginPassword;
    if (!username || !password) {
      setAuthError("Enter username and password.");
      return;
    }

    setIsSigningIn(true);
    setAuthError("");

    try {
      const body = await requestJson("/api/auth/login", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          username,
          password,
        }),
      });

      const sessionToken = (body?.sessionToken || "").toString();
      const nextUsername = (body?.user?.username || username).toString();
      setMobileSessionToken(sessionToken);
      setAuthUser(nextUsername);
      setAuthState(AUTH_STATE_SIGNED_IN);
      setLoginPassword("");
    } catch (error) {
      setAuthState(AUTH_STATE_SIGNED_OUT);
      setAuthUser("");
      setMobileSessionToken("");
      setAuthError(error.message || "Invalid login or password.");
      resetAppDataForSignedOut();
    } finally {
      setIsSigningIn(false);
    }
  }, [loginPassword, loginUsername, resetAppDataForSignedOut]);

  const handleSignOut = useCallback(async () => {
    try {
      await requestApiJson("/api/auth/logout", {
        method: "POST",
      });
    } catch {
      // Best effort logout; local session is always cleared.
    }

    setAuthState(AUTH_STATE_SIGNED_OUT);
    setAuthUser("");
    setMobileSessionToken("");
    setLoginPassword("");
    setAuthError("");
    resetAppDataForSignedOut();
  }, [requestApiJson, resetAppDataForSignedOut]);

  const loadRecords = useCallback(async () => {
    setRecordsError("");
    setIsLoadingRecords(true);

    try {
      const body = await requestApiJson("/api/records", {
        headers: {
          Accept: "application/json",
        },
      });

      const normalizedRecords = normalizeRecordsArray(body.records);
      setRecords(normalizedRecords);
      setRecordsUpdatedAt((body.updatedAt || "").toString());
    } catch (error) {
      if (error?.status === 401) {
        applyUnauthorizedState();
        return;
      }
      setRecordsError(error.message || "Failed to load records.");
    } finally {
      setIsLoadingRecords(false);
    }
  }, [applyUnauthorizedState, requestApiJson]);

  const saveRecords = useCallback(async (nextRecords) => {
    setIsSavingRecords(true);

    try {
      const body = await requestApiJson("/api/records", {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify({ records: nextRecords }),
      });

      setRecords(nextRecords);
      setRecordsUpdatedAt((body.updatedAt || new Date().toISOString()).toString());
      setRecordsError("");
      return { ok: true };
    } catch (error) {
      if (error?.status === 401) {
        applyUnauthorizedState();
        return {
          ok: false,
          error: "Session expired. Sign in again.",
        };
      }
      return {
        ok: false,
        error: error.message || "Failed to save records.",
      };
    } finally {
      setIsSavingRecords(false);
    }
  }, [applyUnauthorizedState, requestApiJson]);

  const loadPendingSubmissions = useCallback(async () => {
    setModerationError("");
    setIsLoadingModeration(true);

    try {
      const body = await requestApiJson("/api/moderation/submissions?status=pending&limit=200", {
        headers: {
          Accept: "application/json",
        },
      });

      const items = Array.isArray(body.items) ? body.items : [];
      setPendingSubmissions(items);
    } catch (error) {
      if (error?.status === 401) {
        applyUnauthorizedState();
        return;
      }
      setPendingSubmissions([]);
      setModerationError(error.message || "Failed to load moderation queue.");
    } finally {
      setIsLoadingModeration(false);
    }
  }, [applyUnauthorizedState, requestApiJson]);

  const loadSubmissionFiles = useCallback(async (submissionId) => {
    setIsLoadingSubmissionFiles(true);
    setSubmissionFilesError("");

    try {
      const body = await requestApiJson(
        `/api/moderation/submissions/${encodeURIComponent(submissionId)}/files`,
        {
          headers: {
            Accept: "application/json",
          },
        },
      );

      const items = Array.isArray(body.items) ? body.items : [];
      setSubmissionFiles(items);
    } catch (error) {
      if (error?.status === 401) {
        applyUnauthorizedState();
        return;
      }
      setSubmissionFiles([]);
      setSubmissionFilesError(error.message || "Failed to load attachments.");
    } finally {
      setIsLoadingSubmissionFiles(false);
    }
  }, [applyUnauthorizedState, requestApiJson]);

  useEffect(() => {
    void loadAuthSession();
  }, [loadAuthSession]);

  useEffect(() => {
    if (!isAuthenticated) {
      setRecords([]);
      setRecordsUpdatedAt("");
      setRecordsError("");
      setIsLoadingRecords(false);
      return;
    }

    void loadRecords();
  }, [isAuthenticated, loadRecords]);

  useEffect(() => {
    if (isAuthenticated && activeTab === "moderation") {
      void loadPendingSubmissions();
    }
  }, [activeTab, isAuthenticated, loadPendingSubmissions]);

  useEffect(() => {
    if (!isAuthenticated) {
      setSubmissionFiles([]);
      setSubmissionFilesError("");
      return;
    }

    if (!activeSubmissionId) {
      setSubmissionFiles([]);
      setSubmissionFilesError("");
      return;
    }

    void loadSubmissionFiles(activeSubmissionId);
  }, [activeSubmissionId, isAuthenticated, loadSubmissionFiles]);

  useEffect(() => {
    if (statusFilter !== "overdue" && overdueRangeFilter) {
      setOverdueRangeFilter("");
    }
  }, [statusFilter, overdueRangeFilter]);

  useEffect(() => {
    if (editingRecordId && !editingRecord) {
      setEditingRecordId("");
    }
  }, [editingRecordId, editingRecord]);

  const dashboardMetrics = useMemo(() => {
    return buildOverviewMetrics(records, overviewPeriod);
  }, [records, overviewPeriod]);

  const visibleRecords = useMemo(() => {
    const query = searchQuery.trim().toLowerCase();

    return [...records]
      .filter((record) => {
        if (!query) {
          return true;
        }

        const searchable = [
          record.clientName,
          record.companyName,
          record.closedBy,
          record.serviceType,
          record.clientEmailAddress,
        ]
          .map((value) => (value || "").toString().toLowerCase())
          .join(" ");

        return searchable.includes(query);
      })
      .filter((record) => matchesStatusFilter(record, statusFilter, overdueRangeFilter))
      .sort((left, right) => {
        const leftTime = Date.parse((left.createdAt || "").toString()) || 0;
        const rightTime = Date.parse((right.createdAt || "").toString()) || 0;
        return rightTime - leftTime;
      });
  }, [records, searchQuery, statusFilter, overdueRangeFilter]);

  const handleCreateRecord = useCallback(
    async (record) => {
      const nextRecords = [record, ...records];
      const result = await saveRecords(nextRecords);

      if (result.ok) {
        setActiveTab("clients");
      }

      return result;
    },
    [records, saveRecords],
  );

  const handleEditRecord = useCallback(
    async (updatedRecord) => {
      const nextRecords = records.map((record) => {
        return record.id === updatedRecord.id ? updatedRecord : record;
      });

      const result = await saveRecords(nextRecords);
      if (result.ok) {
        setEditingRecordId("");
      }

      return result;
    },
    [records, saveRecords],
  );

  const runModerationAction = useCallback(
    async (action) => {
      if (!activeSubmissionId) {
        return;
      }

      setIsModerationActionRunning(true);

      try {
        const endpoint = action === "approve" ? "approve" : "reject";
        await requestApiJson(
          `/api/moderation/submissions/${encodeURIComponent(activeSubmissionId)}/${endpoint}`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Accept: "application/json",
            },
            body: JSON.stringify({}),
          },
        );

        setActiveSubmissionId("");
        await Promise.all([loadPendingSubmissions(), loadRecords()]);
      } catch (error) {
        if (error?.status === 401) {
          applyUnauthorizedState();
          return;
        }
        Alert.alert("Action failed", error.message || "Failed to process moderation action.");
      } finally {
        setIsModerationActionRunning(false);
      }
    },
    [activeSubmissionId, applyUnauthorizedState, loadPendingSubmissions, loadRecords, requestApiJson],
  );

  const confirmModerationAction = useCallback(
    (action) => {
      const actionLabel = action === "approve" ? "Approve" : "Reject";
      const message =
        action === "approve"
          ? "Client will be added to the main records database."
          : "Submission will be removed from moderation queue.";

      Alert.alert(actionLabel, message, [
        {
          text: "Cancel",
          style: "cancel",
        },
        {
          text: actionLabel,
          style: action === "approve" ? "default" : "destructive",
          onPress: () => {
            void runModerationAction(action);
          },
        },
      ]);
    },
    [runModerationAction],
  );

  return (
    <SafeAreaView style={styles.safeArea}>
      <StatusBar barStyle="dark-content" />

      <View style={styles.headerShell}>
        <View style={styles.headerAccent} />
        <Text style={styles.headerTitle}>Credit Booster</Text>
        <Text style={styles.headerSubtitle}>Native app with direct server sync</Text>
        <Text style={styles.headerMeta}>API: {API_BASE_URL}</Text>
        <View style={styles.headerSessionRow}>
          <Text style={styles.headerMeta}>
            {isAuthenticated ? `Signed in as ${authUser || "user"}` : "Sign in required"}
          </Text>
          {isAuthenticated ? (
            <Pressable style={styles.headerSessionButton} onPress={() => void handleSignOut()}>
              <Text style={styles.headerSessionButtonText}>Sign out</Text>
            </Pressable>
          ) : null}
        </View>
      </View>

      {authState === AUTH_STATE_CHECKING ? (
        <View style={styles.authLoadingWrap}>
          <ActivityIndicator size="large" color="#102a56" />
          <Text style={styles.loadingText}>Checking session...</Text>
        </View>
      ) : null}

      {authState === AUTH_STATE_SIGNED_OUT ? (
        <AuthScreen
          authError={authError}
          username={loginUsername}
          password={loginPassword}
          isSigningIn={isSigningIn}
          onChangeUsername={setLoginUsername}
          onChangePassword={setLoginPassword}
          onSignIn={handleSignIn}
        />
      ) : null}

      {isAuthenticated ? (
        <>
          <View style={styles.tabsRow}>
            <TabButton
              label="Clients"
              isActive={activeTab === "clients"}
              onPress={() => setActiveTab("clients")}
            />
            <TabButton
              label="New"
              isActive={activeTab === "create"}
              onPress={() => setActiveTab("create")}
            />
            <TabButton
              label="Moderation"
              isActive={activeTab === "moderation"}
              onPress={() => setActiveTab("moderation")}
            />
          </View>

          {activeTab === "clients" ? (
            <ClientsScreen
              records={records}
              visibleRecords={visibleRecords}
              recordsUpdatedAt={recordsUpdatedAt}
              recordsError={recordsError}
              isLoadingRecords={isLoadingRecords}
              isSavingRecords={isSavingRecords}
              searchQuery={searchQuery}
              setSearchQuery={setSearchQuery}
              statusFilter={statusFilter}
              setStatusFilter={setStatusFilter}
              overdueRangeFilter={overdueRangeFilter}
              setOverdueRangeFilter={setOverdueRangeFilter}
              overviewPeriod={overviewPeriod}
              setOverviewPeriod={setOverviewPeriod}
              dashboardMetrics={dashboardMetrics}
              onRefresh={loadRecords}
              onEditRecord={(recordId) => setEditingRecordId(recordId)}
            />
          ) : null}

          {activeTab === "create" ? (
            <RecordForm
              mode="create"
              initialRecord={null}
              isSavingRecords={isSavingRecords}
              onCancel={() => setActiveTab("clients")}
              onSubmit={handleCreateRecord}
            />
          ) : null}

          {activeTab === "moderation" ? (
            <ModerationScreen
              isLoadingModeration={isLoadingModeration}
              moderationError={moderationError}
              pendingSubmissions={pendingSubmissions}
              onRefresh={loadPendingSubmissions}
              onOpenSubmission={(submissionId) => setActiveSubmissionId(submissionId)}
            />
          ) : null}

          <Modal
            visible={Boolean(editingRecord)}
            animationType="slide"
            presentationStyle="pageSheet"
            onRequestClose={() => setEditingRecordId("")}
          >
            <SafeAreaView style={styles.modalRoot}>
              <View style={styles.modalHeader}>
                <Text style={styles.modalTitle}>Client Details</Text>
                <Pressable style={styles.ghostButton} onPress={() => setEditingRecordId("")}>
                  <Text style={styles.ghostButtonText}>Close</Text>
                </Pressable>
              </View>

              {editingRecord ? <RecordDetails record={editingRecord} /> : null}
            </SafeAreaView>
          </Modal>

          <Modal
            visible={Boolean(activeSubmission)}
            animationType="slide"
            presentationStyle="pageSheet"
            onRequestClose={() => setActiveSubmissionId("")}
          >
            <SafeAreaView style={styles.modalRoot}>
              <View style={styles.modalHeader}>
                <Text style={styles.modalTitle}>Moderation Review</Text>
                <Pressable style={styles.ghostButton} onPress={() => setActiveSubmissionId("")}>
                  <Text style={styles.ghostButtonText}>Close</Text>
                </Pressable>
              </View>

              <ScrollView contentContainerStyle={styles.modalContent}>
                {activeSubmission ? (
                  <View style={styles.submissionCard}>
                    {buildSubmissionDetails(activeSubmission).map((item) => {
                      return (
                        <View style={styles.detailRow} key={`${activeSubmission.id}-${item.label}`}>
                          <Text style={styles.detailLabel}>{item.label}</Text>
                          <Text style={styles.detailValue}>{item.value || "-"}</Text>
                        </View>
                      );
                    })}
                  </View>
                ) : null}

                <View style={styles.attachmentsCard}>
                  <Text style={styles.sectionTitle}>Attachments</Text>

                  {isLoadingSubmissionFiles ? (
                    <View style={styles.inlineLoadingRow}>
                      <ActivityIndicator size="small" color="#102a56" />
                      <Text style={styles.inlineLoadingText}>Loading files...</Text>
                    </View>
                  ) : null}

                  {!isLoadingSubmissionFiles && submissionFilesError ? (
                    <Text style={styles.errorText}>{submissionFilesError}</Text>
                  ) : null}

                  {!isLoadingSubmissionFiles && !submissionFilesError && !submissionFiles.length ? (
                    <Text style={styles.mutedText}>No attachments.</Text>
                  ) : null}

                  {!isLoadingSubmissionFiles && submissionFiles.length
                    ? submissionFiles.map((item, index) => {
                        return (
                          <View
                            style={styles.attachmentRow}
                            key={item.id || `${item.fileName || "attachment"}-${index}`}
                          >
                            <View style={styles.attachmentInfo}>
                              <Text style={styles.attachmentName}>{item.fileName || "Attachment"}</Text>
                              <Text style={styles.attachmentMeta}>
                                {formatFileSize(item.sizeBytes)} | {(item.mimeType || "application/octet-stream").toString()}
                              </Text>
                            </View>

                            <View style={styles.attachmentActions}>
                              {item.canPreview && item.previewUrl ? (
                                <Pressable
                                  style={styles.secondaryActionButton}
                                  onPress={() => {
                                    const url = toAbsoluteApiUrl(item.previewUrl);
                                    void Linking.openURL(url);
                                  }}
                                >
                                  <Text style={styles.secondaryActionButtonText}>Preview</Text>
                                </Pressable>
                              ) : null}

                              {item.downloadUrl ? (
                                <Pressable
                                  style={styles.secondaryActionButton}
                                  onPress={() => {
                                    const url = toAbsoluteApiUrl(item.downloadUrl);
                                    void Linking.openURL(url);
                                  }}
                                >
                                  <Text style={styles.secondaryActionButtonText}>Download</Text>
                                </Pressable>
                              ) : null}
                            </View>
                          </View>
                        );
                      })
                    : null}
                </View>
              </ScrollView>

              <View style={styles.modalActionsBar}>
                <Pressable
                  style={[styles.rejectButton, isModerationActionRunning && styles.disabledButton]}
                  disabled={isModerationActionRunning}
                  onPress={() => confirmModerationAction("reject")}
                >
                  <Text style={styles.rejectButtonText}>Delete</Text>
                </Pressable>

                <Pressable
                  style={[styles.approveButton, isModerationActionRunning && styles.disabledButton]}
                  disabled={isModerationActionRunning}
                  onPress={() => confirmModerationAction("approve")}
                >
                  <Text style={styles.approveButtonText}>Add To Main DB</Text>
                </Pressable>
              </View>
            </SafeAreaView>
          </Modal>
        </>
      ) : null}
    </SafeAreaView>
  );
}

function ClientsScreen({
  records,
  visibleRecords,
  recordsUpdatedAt,
  recordsError,
  isLoadingRecords,
  isSavingRecords,
  searchQuery,
  setSearchQuery,
  statusFilter,
  setStatusFilter,
  overdueRangeFilter,
  setOverdueRangeFilter,
  overviewPeriod,
  setOverviewPeriod,
  dashboardMetrics,
  onRefresh,
  onEditRecord,
}) {
  const updatedAtLabel = recordsUpdatedAt ? formatDateTime(recordsUpdatedAt) : "-";

  return (
    <ScrollView contentContainerStyle={styles.screenContent}>
      <View style={styles.sectionCard}>
        <Text style={styles.sectionTitle}>Overview</Text>

        <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={styles.filterRow}>
          {OVERVIEW_PERIOD_OPTIONS.map((option) => {
            const isActive = option.key === overviewPeriod;
            return (
              <Pressable
                key={option.key}
                style={[styles.filterChip, isActive && styles.filterChipActive]}
                onPress={() => setOverviewPeriod(option.key)}
              >
                <Text style={[styles.filterChipText, isActive && styles.filterChipTextActive]}>
                  {option.label}
                </Text>
              </Pressable>
            );
          })}
        </ScrollView>

        <View style={styles.metricsRow}>
          <MetricCard label="Sales" value={formatKpiCurrency(dashboardMetrics.sales)} tone="blue" />
          <MetricCard label="Received" value={formatKpiCurrency(dashboardMetrics.received)} tone="green" />
          <MetricCard label="Debt" value={formatKpiCurrency(dashboardMetrics.debt)} tone="amber" />
        </View>
      </View>

      <View style={styles.sectionCard}>
        <View style={styles.sectionHeaderRow}>
          <Text style={styles.sectionTitle}>Records</Text>
          <Pressable style={styles.secondaryActionButton} onPress={() => void onRefresh()}>
            <Text style={styles.secondaryActionButtonText}>Refresh</Text>
          </Pressable>
        </View>

        <Text style={styles.metaText}>Last sync: {updatedAtLabel}</Text>

        <TextInput
          value={searchQuery}
          onChangeText={setSearchQuery}
          placeholder="Search by name, company, email"
          placeholderTextColor="#7b8ba5"
          style={styles.searchInput}
          autoCapitalize="none"
        />

        <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={styles.filterRow}>
          {STATUS_FILTER_OPTIONS.map((option) => {
            const isActive = option.key === statusFilter;
            return (
              <Pressable
                key={option.key}
                style={[styles.filterChip, isActive && styles.filterChipActive]}
                onPress={() => setStatusFilter(option.key)}
              >
                <Text style={[styles.filterChipText, isActive && styles.filterChipTextActive]}>
                  {option.label}
                </Text>
              </Pressable>
            );
          })}
        </ScrollView>

        {statusFilter === "overdue" ? (
          <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={styles.filterRow}>
            {OVERDUE_RANGE_FILTER_OPTIONS.map((option) => {
              const isActive = option.key === overdueRangeFilter;
              return (
                <Pressable
                  key={option.key || "any"}
                  style={[styles.filterChip, isActive && styles.filterChipActive]}
                  onPress={() => setOverdueRangeFilter(option.key)}
                >
                  <Text style={[styles.filterChipText, isActive && styles.filterChipTextActive]}>
                    {option.label}
                  </Text>
                </Pressable>
              );
            })}
          </ScrollView>
        ) : null}
      </View>

      {recordsError ? (
        <View style={styles.errorCard}>
          <Text style={styles.errorText}>{recordsError}</Text>
        </View>
      ) : null}

      {isLoadingRecords ? (
        <View style={styles.loadingCard}>
          <ActivityIndicator size="large" color="#102a56" />
          <Text style={styles.loadingText}>Loading records...</Text>
        </View>
      ) : null}

      {!isLoadingRecords ? (
        <View style={styles.sectionCard}>
          <View style={styles.sectionHeaderRow}>
            <Text style={styles.sectionTitle}>Clients ({visibleRecords.length})</Text>
            {isSavingRecords ? <Text style={styles.savingLabel}>Syncing...</Text> : null}
          </View>

          {!visibleRecords.length ? <Text style={styles.mutedText}>No records found.</Text> : null}

          {visibleRecords.map((record) => {
            const status = getRecordStatusFlags(record);
            const statusChips = getStatusChipConfig(status);

            return (
              <Pressable
                style={({ pressed }) => [styles.recordCard, pressed && styles.recordCardPressed]}
                key={record.id}
                onPress={() => onEditRecord(record.id)}
              >
                <View style={styles.recordCardHeader}>
                  <View style={styles.recordTitleWrap}>
                    <Text style={styles.recordTitle}>{record.clientName || "Unnamed client"}</Text>
                    <Text style={styles.recordSubtitle}>{record.companyName || "-"}</Text>
                  </View>
                </View>

                <Text style={styles.recordMetaLine}>Closed by: {record.closedBy || "-"}</Text>
                <Text style={styles.recordMetaLine}>Service: {record.serviceType || "-"}</Text>

                <View style={styles.statusChipRow}>
                  {statusChips.map((chip) => (
                    <StatusChip key={`${record.id}-${chip.label}`} label={chip.label} tone={chip.tone} />
                  ))}
                </View>

                <View style={styles.recordNumbersRow}>
                  <View style={styles.recordNumberCell}>
                    <Text style={styles.recordNumberLabel}>Contract</Text>
                    <Text style={styles.recordNumberValue}>{formatMoneyText(record.contractTotals)}</Text>
                  </View>
                  <View style={styles.recordNumberCell}>
                    <Text style={styles.recordNumberLabel}>Paid</Text>
                    <Text style={styles.recordNumberValue}>{formatMoneyText(record.totalPayments)}</Text>
                  </View>
                  <View style={styles.recordNumberCell}>
                    <Text style={styles.recordNumberLabel}>Balance</Text>
                    <Text style={styles.recordNumberValue}>{formatMoneyText(record.futurePayments)}</Text>
                  </View>
                </View>
              </Pressable>
            );
          })}

          <Text style={styles.metaText}>Total records in DB: {records.length}</Text>
        </View>
      ) : null}
    </ScrollView>
  );
}

function RecordDetails({ record }) {
  const details = useMemo(() => buildRecordDetails(record), [record]);

  return (
    <ScrollView contentContainerStyle={styles.modalContent}>
      <View style={styles.submissionCard}>
        {details.map((item, index) => (
          <View style={styles.detailRow} key={`${record.id || "record"}-${index}-${item.label}`}>
            <Text style={styles.detailLabel}>{item.label}</Text>
            <Text style={styles.detailValue}>{item.value || "-"}</Text>
          </View>
        ))}
      </View>
    </ScrollView>
  );
}

function AuthScreen({
  authError,
  username,
  password,
  isSigningIn,
  onChangeUsername,
  onChangePassword,
  onSignIn,
}) {
  return (
    <ScrollView contentContainerStyle={styles.screenContent}>
      <View style={styles.authCard}>
        <Text style={styles.authTitle}>Sign in to continue</Text>
        <Text style={styles.authText}>
          Use the same username and password as your web dashboard.
        </Text>

        {authError ? <Text style={styles.errorText}>{authError}</Text> : null}

        <View style={styles.fieldGroup}>
          <Text style={styles.fieldLabel}>Username</Text>
          <TextInput
            value={username}
            onChangeText={onChangeUsername}
            placeholder="Email or username"
            placeholderTextColor="#7b8ba5"
            style={styles.fieldInput}
            autoCapitalize="none"
            autoCorrect={false}
            keyboardType="email-address"
          />
        </View>

        <View style={styles.fieldGroup}>
          <Text style={styles.fieldLabel}>Password</Text>
          <TextInput
            value={password}
            onChangeText={onChangePassword}
            placeholder="Password"
            placeholderTextColor="#7b8ba5"
            style={styles.fieldInput}
            autoCapitalize="none"
            autoCorrect={false}
            secureTextEntry
          />
        </View>

        <Pressable
          style={[styles.submitButton, isSigningIn && styles.disabledButton]}
          disabled={isSigningIn}
          onPress={() => {
            void onSignIn();
          }}
        >
          <Text style={styles.submitButtonText}>{isSigningIn ? "Signing in..." : "Sign in"}</Text>
        </Pressable>
      </View>
    </ScrollView>
  );
}

function ModerationScreen({
  isLoadingModeration,
  moderationError,
  pendingSubmissions,
  onRefresh,
  onOpenSubmission,
}) {
  return (
    <ScrollView contentContainerStyle={styles.screenContent}>
      <View style={styles.sectionCard}>
        <View style={styles.sectionHeaderRow}>
          <Text style={styles.sectionTitle}>Moderation Queue</Text>
          <Pressable style={styles.secondaryActionButton} onPress={() => void onRefresh()}>
            <Text style={styles.secondaryActionButtonText}>Refresh</Text>
          </Pressable>
        </View>

        {isLoadingModeration ? (
          <View style={styles.inlineLoadingRow}>
            <ActivityIndicator size="small" color="#102a56" />
            <Text style={styles.inlineLoadingText}>Loading submissions...</Text>
          </View>
        ) : null}

        {moderationError ? <Text style={styles.errorText}>{moderationError}</Text> : null}

        {!isLoadingModeration && !pendingSubmissions.length ? (
          <Text style={styles.mutedText}>No pending submissions.</Text>
        ) : null}

        {pendingSubmissions.map((submission) => {
          const name = getSubmissionClientField(submission, "clientName") || "Unnamed";
          const company = getSubmissionClientField(submission, "companyName") || "-";
          const closedBy =
            getSubmissionClientField(submission, "closedBy") ||
            formatSubmittedBy(submission.submittedBy) ||
            "-";

          return (
            <View style={styles.recordCard} key={submission.id}>
              <Text style={styles.recordTitle}>{name}</Text>
              <Text style={styles.recordSubtitle}>{company}</Text>
              <Text style={styles.recordMetaLine}>Closed by: {closedBy}</Text>
              <Text style={styles.recordMetaLine}>Submitted: {formatDateTime(submission.submittedAt)}</Text>

              <View style={styles.recordCardActionsRow}>
                <Pressable
                  style={styles.primaryActionButton}
                  onPress={() => onOpenSubmission(submission.id)}
                >
                  <Text style={styles.primaryActionButtonText}>Open</Text>
                </Pressable>
              </View>
            </View>
          );
        })}
      </View>
    </ScrollView>
  );
}

function RecordForm({ mode, initialRecord, isSavingRecords, onSubmit, onCancel }) {
  const [draft, setDraft] = useState(() => {
    return toFormDraft(initialRecord);
  });
  const [formError, setFormError] = useState("");
  const [formSuccess, setFormSuccess] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);

  useEffect(() => {
    setDraft(toFormDraft(initialRecord));
    setFormError("");
    setFormSuccess("");
  }, [initialRecord, mode]);

  const previewRecord = useMemo(() => {
    const candidate = buildRecordFromDraftValues(draft);
    const previousRecord = initialRecord || null;
    applyDerivedRecordState(candidate, previousRecord);
    return candidate;
  }, [draft, initialRecord]);

  const busy = isSubmitting || isSavingRecords;

  const handleFieldChange = useCallback((fieldKey, value) => {
    setDraft((previous) => ({
      ...previous,
      [fieldKey]: value,
    }));
  }, []);

  const handleCheckboxChange = useCallback((fieldKey, checked) => {
    setDraft((previous) => {
      const nextDraft = {
        ...previous,
        [fieldKey]: checked ? "Yes" : "",
      };

      if (fieldKey === "writtenOff") {
        if (checked) {
          if (!nextDraft.dateWhenWrittenOff) {
            nextDraft.dateWhenWrittenOff = getTodayDateUs();
          }
        } else if (!isWrittenOffByList(nextDraft.clientName)) {
          nextDraft.dateWhenWrittenOff = "";
        }
      }

      return nextDraft;
    });
  }, []);

  const submit = useCallback(async () => {
    setFormError("");
    setFormSuccess("");

    const previousRecord = mode === "edit" ? initialRecord : null;
    const prepared = prepareRecordForSave({
      draft,
      mode,
      previousRecord,
    });

    if (prepared.error) {
      setFormError(prepared.error);
      return;
    }

    setIsSubmitting(true);

    try {
      const result = await onSubmit(prepared.record);
      if (!result?.ok) {
        setFormError(result?.error || "Failed to save record.");
        return;
      }

      setFormSuccess(mode === "edit" ? "Client updated." : "Client added.");
      if (mode === "create") {
        setDraft(toFormDraft(null));
      }
    } catch (error) {
      setFormError(error.message || "Failed to save record.");
    } finally {
      setIsSubmitting(false);
    }
  }, [draft, mode, initialRecord, onSubmit]);

  return (
    <ScrollView contentContainerStyle={styles.screenContent}>
      <View style={styles.sectionCard}>
        <View style={styles.sectionHeaderRow}>
          <Text style={styles.sectionTitle}>{mode === "edit" ? "Edit Client" : "New Client"}</Text>
          <Pressable style={styles.secondaryActionButton} onPress={onCancel}>
            <Text style={styles.secondaryActionButtonText}>Back</Text>
          </Pressable>
        </View>

        {formError ? <Text style={styles.errorText}>{formError}</Text> : null}
        {formSuccess ? <Text style={styles.successText}>{formSuccess}</Text> : null}

        {FORM_SECTIONS.map((section) => {
          return (
            <View style={styles.formSection} key={section.title}>
              <Text style={styles.formSectionTitle}>{section.title}</Text>

              {section.fields.map((fieldKey) => {
                const fieldConfig = FIELD_CONFIG[fieldKey] || { type: "text" };
                const fieldLabel = FIELD_LABELS[fieldKey] || humanizeKey(fieldKey);

                if (fieldConfig.type === "checkbox") {
                  const isEnabled = isCheckboxEnabled(draft[fieldKey]);

                  return (
                    <View style={styles.checkboxRow} key={fieldKey}>
                      <Text style={styles.fieldLabel}>{fieldLabel}</Text>
                      <Switch
                        value={isEnabled}
                        onValueChange={(checked) => handleCheckboxChange(fieldKey, checked)}
                        trackColor={{ false: "#c7d2e2", true: "#1b4f99" }}
                        thumbColor={isEnabled ? "#f8fafc" : "#f1f5f9"}
                      />
                    </View>
                  );
                }

                const value = (draft[fieldKey] || "").toString();

                return (
                  <View style={styles.fieldGroup} key={fieldKey}>
                    <Text style={styles.fieldLabel}>
                      {fieldLabel}
                      {fieldConfig.required ? " *" : ""}
                    </Text>
                    <TextInput
                      value={value}
                      onChangeText={(nextValue) => handleFieldChange(fieldKey, nextValue)}
                      placeholder={fieldConfig.type === "date" ? "MM/DD/YYYY" : fieldLabel}
                      placeholderTextColor="#7b8ba5"
                      style={[
                        styles.fieldInput,
                        fieldConfig.type === "textarea" ? styles.fieldInputMultiline : null,
                      ]}
                      keyboardType={resolveKeyboardType(fieldConfig.type)}
                      autoCapitalize={fieldConfig.autoCapitalize || "sentences"}
                      autoCorrect={false}
                      multiline={fieldConfig.type === "textarea"}
                      numberOfLines={fieldConfig.type === "textarea" ? 4 : 1}
                    />
                  </View>
                );
              })}
            </View>
          );
        })}

        <View style={styles.formSection}>
          <Text style={styles.formSectionTitle}>Computed</Text>

          <View style={styles.fieldGroup}>
            <Text style={styles.fieldLabel}>Total Payments</Text>
            <View style={styles.readOnlyValueWrap}>
              <Text style={styles.readOnlyValue}>{previewRecord.totalPayments || "$0.00"}</Text>
            </View>
          </View>

          <View style={styles.fieldGroup}>
            <Text style={styles.fieldLabel}>Future Payments</Text>
            <View style={styles.readOnlyValueWrap}>
              <Text style={styles.readOnlyValue}>{previewRecord.futurePayments || ""}</Text>
            </View>
          </View>

          <View style={styles.fieldGroup}>
            <Text style={styles.fieldLabel}>Date When Fully Paid</Text>
            <View style={styles.readOnlyValueWrap}>
              <Text style={styles.readOnlyValue}>{previewRecord.dateWhenFullyPaid || "-"}</Text>
            </View>
          </View>
        </View>

        <Pressable
          style={[styles.submitButton, busy && styles.disabledButton]}
          disabled={busy}
          onPress={() => {
            void submit();
          }}
        >
          <Text style={styles.submitButtonText}>{busy ? "Saving..." : mode === "edit" ? "Save" : "Add Client"}</Text>
        </Pressable>
      </View>
    </ScrollView>
  );
}

function TabButton({ label, isActive, onPress }) {
  return (
    <Pressable style={[styles.tabButton, isActive && styles.tabButtonActive]} onPress={onPress}>
      <Text style={[styles.tabButtonText, isActive && styles.tabButtonTextActive]}>{label}</Text>
    </Pressable>
  );
}

function MetricCard({ label, value, tone }) {
  return (
    <View style={[styles.metricCard, resolveMetricTone(tone)]}>
      <Text style={styles.metricLabel}>{label}</Text>
      <Text style={styles.metricValue}>{value}</Text>
    </View>
  );
}

function StatusChip({ label, tone }) {
  return (
    <View style={[styles.statusChip, resolveStatusTone(tone)]}>
      <Text style={styles.statusChipText}>{label}</Text>
    </View>
  );
}

function resolveKeyboardType(fieldType) {
  if (fieldType === "money") {
    return "decimal-pad";
  }

  if (fieldType === "phone") {
    return "phone-pad";
  }

  if (fieldType === "email") {
    return "email-address";
  }

  if (fieldType === "date") {
    return "numbers-and-punctuation";
  }

  return "default";
}

function resolveMetricTone(tone) {
  if (tone === "blue") {
    return styles.metricBlue;
  }

  if (tone === "green") {
    return styles.metricGreen;
  }

  if (tone === "amber") {
    return styles.metricAmber;
  }

  return null;
}

function resolveStatusTone(tone) {
  if (tone === "writtenOff") {
    return styles.statusWrittenOff;
  }

  if (tone === "fullyPaid") {
    return styles.statusFullyPaid;
  }

  if (tone === "overdue") {
    return styles.statusOverdue;
  }

  if (tone === "afterResult") {
    return styles.statusAfterResult;
  }

  return styles.statusActive;
}

function createEmptyRecord() {
  const record = {};

  for (const key of RECORD_TEXT_FIELDS) {
    record[key] = "";
  }

  for (const key of RECORD_DATE_FIELDS) {
    record[key] = "";
  }

  for (const key of RECORD_CHECKBOX_FIELDS) {
    record[key] = "";
  }

  record.id = "";
  record.createdAt = "";
  return record;
}

function toFormDraft(record) {
  const source = record && typeof record === "object" ? record : createEmptyRecord();
  const draft = createEmptyRecord();

  for (const key of RECORD_TEXT_FIELDS) {
    draft[key] = (source[key] || "").toString();
  }

  for (const key of RECORD_DATE_FIELDS) {
    draft[key] = (source[key] || "").toString();
  }

  for (const key of RECORD_CHECKBOX_FIELDS) {
    draft[key] = isCheckboxEnabled(source[key]) ? "Yes" : "";
  }

  draft.id = (source.id || "").toString();
  draft.createdAt = (source.createdAt || "").toString();

  return draft;
}

function buildRecordFromDraftValues(draft) {
  const source = draft && typeof draft === "object" ? draft : {};
  const record = createEmptyRecord();

  for (const key of RECORD_TEXT_FIELDS) {
    record[key] = (source[key] || "").toString().trim();
  }

  for (const key of RECORD_DATE_FIELDS) {
    record[key] = (source[key] || "").toString().trim();
  }

  for (const key of RECORD_CHECKBOX_FIELDS) {
    record[key] = isCheckboxEnabled(source[key]) ? "Yes" : "";
  }

  record.id = (source.id || "").toString().trim();
  record.createdAt = (source.createdAt || "").toString().trim();

  return record;
}

function prepareRecordForSave({ draft, mode, previousRecord }) {
  const record = createEmptyRecord();
  const source = draft && typeof draft === "object" ? draft : {};

  const clientName = (source.clientName || "").toString().trim();
  if (!clientName) {
    return {
      error: 'Field "Client Name" is required.',
    };
  }

  for (const key of RECORD_TEXT_FIELDS) {
    if (key === "totalPayments" || key === "futurePayments" || key === "dateWhenFullyPaid") {
      continue;
    }

    record[key] = (source[key] || "").toString().trim();
  }

  for (const key of RECORD_DATE_FIELDS) {
    const rawValue = (source[key] || "").toString().trim();
    const normalizedDate = normalizeDateForStorage(rawValue);

    if (rawValue && normalizedDate === null) {
      return {
        error: `Field "${FIELD_LABELS[key] || key}" must be in MM/DD/YYYY format.`,
      };
    }

    record[key] = normalizedDate || "";
  }

  for (const key of RECORD_CHECKBOX_FIELDS) {
    record[key] = isCheckboxEnabled(source[key]) ? "Yes" : "";
  }

  if (record.writtenOff === "Yes" && !record.dateWhenWrittenOff) {
    record.dateWhenWrittenOff = getTodayDateUs();
  } else if (!isWrittenOffByList(record.clientName)) {
    record.dateWhenWrittenOff = "";
  }

  const baseRecord = {
    ...record,
    id:
      mode === "edit"
        ? ((previousRecord?.id || source.id || "").toString().trim() || generateId())
        : generateId(),
    createdAt:
      mode === "edit"
        ? normalizeCreatedAt(previousRecord?.createdAt || source.createdAt)
        : new Date().toISOString(),
  };

  applyDerivedRecordState(baseRecord, previousRecord || null);

  return {
    record: baseRecord,
  };
}

function normalizeRecordsArray(rawRecords) {
  if (!Array.isArray(rawRecords)) {
    return [];
  }

  return rawRecords
    .map((item) => normalizeRecord(item))
    .filter((item) => item.clientName || item.companyName);
}

function normalizeRecord(rawRecord) {
  const source = rawRecord && typeof rawRecord === "object" ? rawRecord : {};
  const record = createEmptyRecord();

  for (const key of RECORD_TEXT_FIELDS) {
    record[key] = (source[key] || "").toString().trim();
  }

  for (const key of RECORD_DATE_FIELDS) {
    const normalizedDate = normalizeDateForStorage(source[key] || "");
    record[key] = normalizedDate || "";
  }

  for (const key of RECORD_CHECKBOX_FIELDS) {
    record[key] = isCheckboxEnabled(source[key]) ? "Yes" : "";
  }

  record.id = (source.id || "").toString().trim() || generateId();
  record.createdAt = normalizeCreatedAt(source.createdAt);

  applyDerivedRecordState(record, source);
  return record;
}

function normalizeCreatedAt(rawValue) {
  const value = (rawValue || "").toString().trim();
  if (!value) {
    return new Date().toISOString();
  }

  const timestamp = Date.parse(value);
  if (Number.isNaN(timestamp)) {
    return new Date().toISOString();
  }

  return new Date(timestamp).toISOString();
}

function buildOverviewMetrics(records, periodKey) {
  const ranges = getPeriodRanges();
  const selectedRange = ranges[periodKey] || ranges[DEFAULT_PERIOD];
  const metrics = calculatePeriodMetrics(records, selectedRange);

  return {
    sales: metrics.sales,
    received: metrics.received,
    debt: calculateOverallDebt(records),
  };
}

function getPeriodRanges() {
  const todayUtcStart = getCurrentUtcDayStart();
  const todayDate = new Date(todayUtcStart);
  const currentWeekStart = getCurrentWeekStartUtc(todayUtcStart);
  const previousWeekStart = currentWeekStart - 7 * DAY_IN_MS;
  const previousWeekEnd = currentWeekStart - DAY_IN_MS;
  const currentMonthStart = Date.UTC(todayDate.getUTCFullYear(), todayDate.getUTCMonth(), 1);
  const last30DaysStart = todayUtcStart - 29 * DAY_IN_MS;

  return {
    currentWeek: {
      from: currentWeekStart,
      to: todayUtcStart,
    },
    previousWeek: {
      from: previousWeekStart,
      to: previousWeekEnd,
    },
    currentMonth: {
      from: currentMonthStart,
      to: todayUtcStart,
    },
    last30Days: {
      from: last30DaysStart,
      to: todayUtcStart,
    },
  };
}

function calculatePeriodMetrics(records, range) {
  let sales = 0;
  let received = 0;

  for (const record of records) {
    const firstPaymentDate = parseDateValue(record.payment1Date);
    if (isTimestampWithinInclusiveRange(firstPaymentDate, range.from, range.to)) {
      const contractAmount = parseMoneyValue(record.contractTotals);
      if (contractAmount !== null) {
        sales += contractAmount;
      }
    }

    for (const [paymentKey, paymentDateKey] of PAYMENT_PAIRS) {
      const paymentDate = parseDateValue(record[paymentDateKey]);
      if (!isTimestampWithinInclusiveRange(paymentDate, range.from, range.to)) {
        continue;
      }

      const paymentAmount = parseMoneyValue(record[paymentKey]);
      if (paymentAmount !== null) {
        received += paymentAmount;
      }
    }
  }

  return {
    sales,
    received,
  };
}

function calculateOverallDebt(records) {
  let debt = 0;

  for (const record of records) {
    const futureAmount = computeFuturePaymentsAmount(record);
    if (futureAmount !== null && futureAmount > ZERO_TOLERANCE) {
      debt += futureAmount;
    }
  }

  return debt;
}

function applyDerivedRecordState(record, previousRecord = null) {
  if (!record) {
    return;
  }

  record.totalPayments = computeTotalPayments(record);
  record.futurePayments = computeFuturePayments(record);
  record.dateWhenFullyPaid = computeDateWhenFullyPaid(record, previousRecord);
}

function computeTotalPayments(record) {
  const total = PAYMENT_FIELDS.reduce((sum, key) => {
    return sum + (parseMoneyValue(record?.[key]) ?? 0);
  }, 0);

  return MONEY_FORMATTER.format(total);
}

function computeFuturePayments(record) {
  const future = computeFuturePaymentsAmount(record);
  if (future === null) {
    return "";
  }

  return MONEY_FORMATTER.format(future);
}

function computeFuturePaymentsAmount(record) {
  if (isRecordWrittenOff(record)) {
    return 0;
  }

  const contractTotal = parseMoneyValue(record?.contractTotals);
  if (contractTotal === null) {
    return null;
  }

  const paid = PAYMENT_FIELDS.reduce((sum, key) => {
    return sum + (parseMoneyValue(record?.[key]) ?? 0);
  }, 0);

  return contractTotal - paid;
}

function computeDateWhenFullyPaid(record, previousRecord = null) {
  if (!record || isRecordWrittenOff(record)) {
    return "";
  }

  const contractTotal = parseMoneyValue(record.contractTotals);
  if (contractTotal === null) {
    return "";
  }

  let runningSum = 0;

  for (const [paymentKey, paymentDateKey] of PAYMENT_PAIRS) {
    runningSum += parseMoneyValue(record[paymentKey]) ?? 0;

    if (runningSum >= contractTotal - ZERO_TOLERANCE) {
      const closureDate = parseDateValue(record[paymentDateKey]);
      if (closureDate !== null) {
        return formatDateTimestampUs(closureDate);
      }

      const previousDate = normalizeDateForStorage(previousRecord?.dateWhenFullyPaid || "");
      if (previousDate) {
        return previousDate;
      }

      const latestPaymentDate = getLatestPaymentDateTimestamp(record);
      if (latestPaymentDate !== null) {
        return formatDateTimestampUs(latestPaymentDate);
      }

      return getTodayDateUs();
    }
  }

  return "";
}

function getLatestPaymentDateTimestamp(record) {
  let latestDate = null;

  for (const paymentDateKey of PAYMENT_DATE_FIELDS) {
    const paymentDate = parseDateValue(record?.[paymentDateKey]);
    if (paymentDate === null) {
      continue;
    }

    if (latestDate === null || paymentDate > latestDate) {
      latestDate = paymentDate;
    }
  }

  return latestDate;
}

function matchesStatusFilter(record, statusFilter, overdueRangeFilter) {
  const status = getRecordStatusFlags(record);

  if (statusFilter === "all") {
    return true;
  }

  if (statusFilter === "written-off") {
    return status.isWrittenOff;
  }

  if (statusFilter === "fully-paid") {
    return status.isFullyPaid;
  }

  if (statusFilter === "after-result") {
    return status.isAfterResult;
  }

  if (statusFilter === "overdue") {
    if (!status.isOverdue) {
      return false;
    }

    if (!overdueRangeFilter) {
      return true;
    }

    return status.overdueRange === overdueRangeFilter;
  }

  return true;
}

function getRecordStatusFlags(record) {
  const futureAmount = computeFuturePaymentsAmount(record);
  const isAfterResult = isCheckboxEnabled(record?.afterResult);
  const isWrittenOff = isRecordWrittenOff(record);
  const isFullyPaid = !isWrittenOff && futureAmount !== null && Math.abs(futureAmount) <= ZERO_TOLERANCE;
  const hasOpenBalance = !isWrittenOff && futureAmount !== null && futureAmount > ZERO_TOLERANCE;

  const latestPaymentDate = getLatestPaymentDateTimestamp(record);
  const overdueDays =
    !isAfterResult && hasOpenBalance && latestPaymentDate !== null
      ? getDaysSinceDate(latestPaymentDate)
      : null;
  const overdueRange = getOverdueRangeLabel(overdueDays);

  return {
    isAfterResult,
    isWrittenOff,
    isFullyPaid,
    isOverdue: Boolean(overdueRange),
    overdueRange,
  };
}

function getStatusChipConfig(statusFlags) {
  if (statusFlags.isWrittenOff) {
    return [{ label: "Written Off", tone: "writtenOff" }];
  }

  const chips = [];

  if (statusFlags.isFullyPaid) {
    chips.push({ label: "Fully Paid", tone: "fullyPaid" });
  }

  if (statusFlags.isOverdue) {
    chips.push({ label: `Overdue ${statusFlags.overdueRange}`, tone: "overdue" });
  }

  if (statusFlags.isAfterResult) {
    chips.push({ label: "After Result", tone: "afterResult" });
  }

  if (!chips.length) {
    chips.push({ label: "Active", tone: "active" });
  }

  return chips;
}

function getDaysSinceDate(timestamp) {
  if (timestamp === null || !Number.isFinite(timestamp)) {
    return null;
  }

  const target = new Date(timestamp);
  const targetUtcStart = Date.UTC(target.getUTCFullYear(), target.getUTCMonth(), target.getUTCDate());

  const now = new Date();
  const todayUtcStart = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());

  return Math.floor((todayUtcStart - targetUtcStart) / DAY_IN_MS);
}

function getOverdueRangeLabel(daysOverdue) {
  if (daysOverdue === null || daysOverdue < 1) {
    return "";
  }

  if (daysOverdue <= 7) {
    return "1-7";
  }

  if (daysOverdue <= 30) {
    return "8-30";
  }

  if (daysOverdue <= 60) {
    return "31-60";
  }

  return "60+";
}

function parseMoneyValue(rawValue) {
  const value = (rawValue || "").toString().trim();
  if (!value) {
    return null;
  }

  const normalized = value
    .replace(/[−–—]/g, "-")
    .replace(/\(([^)]+)\)/g, "-$1")
    .replace(/[^0-9.-]/g, "");

  if (!normalized || normalized === "-" || normalized === "." || normalized === "-.") {
    return null;
  }

  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseDateValue(rawValue) {
  const value = (rawValue || "").toString().trim();
  if (!value) {
    return null;
  }

  const isoMatch = value.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (isoMatch) {
    const year = Number(isoMatch[1]);
    const month = Number(isoMatch[2]);
    const day = Number(isoMatch[3]);

    if (isValidDateParts(year, month, day)) {
      return Date.UTC(year, month - 1, day);
    }

    return null;
  }

  const usMatch = value.match(/^(\d{1,2})[\/.-](\d{1,2})[\/.-](\d{2}|\d{4})$/);
  if (usMatch) {
    const month = Number(usMatch[1]);
    const day = Number(usMatch[2]);
    let year = Number(usMatch[3]);

    if (usMatch[3].length === 2) {
      year += 2000;
    }

    if (isValidDateParts(year, month, day)) {
      return Date.UTC(year, month - 1, day);
    }

    return null;
  }

  return null;
}

function normalizeDateForStorage(rawValue) {
  const value = (rawValue || "").toString().trim();
  if (!value) {
    return "";
  }

  const timestamp = parseDateValue(value);
  if (timestamp === null) {
    return null;
  }

  return formatDateTimestampUs(timestamp);
}

function formatDateTimestampUs(timestamp) {
  const date = new Date(timestamp);
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  const year = String(date.getUTCFullYear());
  return `${month}/${day}/${year}`;
}

function isValidDateParts(year, month, day) {
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) {
    return false;
  }

  if (year < 1900 || year > 2100 || month < 1 || month > 12 || day < 1 || day > 31) {
    return false;
  }

  const date = new Date(Date.UTC(year, month - 1, day));
  return (
    date.getUTCFullYear() === year &&
    date.getUTCMonth() === month - 1 &&
    date.getUTCDate() === day
  );
}

function getCurrentUtcDayStart() {
  const now = new Date();
  return Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
}

function getCurrentWeekStartUtc(dayUtcStart) {
  const dayOfWeek = new Date(dayUtcStart).getUTCDay();
  const mondayOffset = dayOfWeek === 0 ? 6 : dayOfWeek - 1;
  return dayUtcStart - mondayOffset * DAY_IN_MS;
}

function isTimestampWithinInclusiveRange(timestamp, fromTimestamp, toTimestamp) {
  if (timestamp === null) {
    return false;
  }

  if (fromTimestamp !== null && timestamp < fromTimestamp) {
    return false;
  }

  if (toTimestamp !== null && timestamp > toTimestamp) {
    return false;
  }

  return true;
}

function getSubmissionClientField(submission, key) {
  const client = submission?.client;
  if (!client || typeof client !== "object") {
    return "";
  }

  return (client[key] || "").toString().trim();
}

function buildRecordDetails(record) {
  const source = record && typeof record === "object" ? record : {};
  const details = [];
  const shown = new Set();
  const requiredFields = new Set([
    "clientName",
    "closedBy",
    "companyName",
    "serviceType",
    "contractTotals",
    "totalPayments",
    "futurePayments",
    "afterResult",
    "writtenOff",
  ]);
  const orderedFields = [
    "clientName",
    "closedBy",
    "companyName",
    "serviceType",
    "leadSource",
    "clientPhoneNumber",
    "clientEmailAddress",
    "contractTotals",
    "totalPayments",
    ...PAYMENT_FIELDS,
    ...PAYMENT_DATE_FIELDS,
    "futurePayments",
    "afterResult",
    "writtenOff",
    "dateWhenWrittenOff",
    "dateWhenFullyPaid",
    "collection",
    "dateOfCollection",
    "notes",
    "ssn",
    "identityIq",
    "futurePayment",
  ];

  for (const key of orderedFields) {
    if (shown.has(key)) {
      continue;
    }

    const rawValue = source[key];
    let value = "";
    if (RECORD_CHECKBOX_FIELDS.includes(key)) {
      value = isCheckboxEnabled(rawValue) ? "Yes" : "No";
    } else {
      value = (rawValue || "").toString().trim();
    }

    if (!value && !requiredFields.has(key)) {
      shown.add(key);
      continue;
    }

    details.push({
      label: FIELD_LABELS[key] || humanizeKey(key),
      value: value || "-",
    });
    shown.add(key);
  }

  for (const [key, rawValue] of Object.entries(source)) {
    if (shown.has(key) || key === "id") {
      continue;
    }

    const value = (rawValue || "").toString().trim();
    if (!value) {
      continue;
    }

    details.push({
      label: FIELD_LABELS[key] || humanizeKey(key),
      value,
    });
  }

  return details;
}

function buildSubmissionDetails(submission) {
  const client = submission?.client && typeof submission.client === "object" ? submission.client : {};
  const details = [];
  const shown = new Set();

  for (const key of SUBMISSION_PRIMARY_FIELDS) {
    const value = (client[key] || "").toString().trim();
    details.push({
      label: FIELD_LABELS[key] || humanizeKey(key),
      value,
    });
    shown.add(key);
  }

  details.push({
    label: "Submitted By",
    value: formatSubmittedBy(submission?.submittedBy),
  });
  details.push({
    label: "Submitted At",
    value: formatDateTime(submission?.submittedAt),
  });

  for (const [key, value] of Object.entries(client)) {
    if (shown.has(key)) {
      continue;
    }

    const textValue = (value || "").toString().trim();
    if (!textValue) {
      continue;
    }

    details.push({
      label: FIELD_LABELS[key] || humanizeKey(key),
      value: textValue,
    });
  }

  return details;
}

function formatSubmittedBy(submittedBy) {
  if (!submittedBy || typeof submittedBy !== "object") {
    return "-";
  }

  const username = (submittedBy.username || "").toString().trim();
  if (username) {
    return `@${username}`;
  }

  const firstName = (submittedBy.first_name || "").toString().trim();
  const lastName = (submittedBy.last_name || "").toString().trim();
  const fullName = [firstName, lastName].filter(Boolean).join(" ").trim();

  if (fullName) {
    return fullName;
  }

  const userId = (submittedBy.id || "").toString().trim();
  return userId ? `tg:${userId}` : "-";
}

function isCheckboxEnabled(rawValue) {
  const normalized = (rawValue || "").toString().trim().toLowerCase();
  return normalized === "yes" || normalized === "true" || normalized === "1" || normalized === "on";
}

function normalizeClientName(value) {
  return (value || "")
    .toString()
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

function isWrittenOffByList(clientName) {
  const normalized = normalizeClientName(clientName);
  return normalized ? WRITTEN_OFF_CLIENT_NAMES.has(normalized) : false;
}

function isRecordWrittenOff(record) {
  if (!record) {
    return false;
  }

  return isCheckboxEnabled(record.writtenOff) || isWrittenOffByList(record.clientName);
}

function formatMoneyText(rawValue) {
  const parsed = parseMoneyValue(rawValue);
  return MONEY_FORMATTER.format(parsed ?? 0);
}

function formatKpiCurrency(value) {
  const parsed = Number(value);
  return KPI_MONEY_FORMATTER.format(Number.isFinite(parsed) ? parsed : 0);
}

function formatDateTime(rawValue) {
  const textValue = (rawValue || "").toString().trim();
  if (!textValue) {
    return "-";
  }

  const timestamp = Date.parse(textValue);
  if (Number.isNaN(timestamp)) {
    return textValue;
  }

  return DATE_TIME_FORMATTER.format(new Date(timestamp));
}

function getTodayDateUs() {
  const now = new Date();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  const day = String(now.getDate()).padStart(2, "0");
  const year = String(now.getFullYear());
  return `${month}/${day}/${year}`;
}

function formatFileSize(rawValue) {
  const bytes = Number.parseInt(rawValue, 10);
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return "0 B";
  }

  const kilobyte = 1024;
  const megabyte = 1024 * 1024;

  if (bytes >= megabyte) {
    return `${(bytes / megabyte).toFixed(bytes >= 10 * megabyte ? 0 : 1)} MB`;
  }

  if (bytes >= kilobyte) {
    return `${Math.round(bytes / kilobyte)} KB`;
  }

  return `${bytes} B`;
}

function humanizeKey(key) {
  return (key || "")
    .toString()
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/^./, (letter) => letter.toUpperCase());
}

function generateId() {
  return `${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;
}

function toAbsoluteApiUrl(pathOrUrl) {
  const value = (pathOrUrl || "").toString().trim();
  if (!value) {
    return API_BASE_URL;
  }

  if (/^https?:\/\//i.test(value)) {
    return value;
  }

  if (value.startsWith("/")) {
    return `${API_BASE_URL}${value}`;
  }

  return `${API_BASE_URL}/${value}`;
}

async function requestJson(path, options = {}, sessionToken = "") {
  const url = toAbsoluteApiUrl(path);
  const nextHeaders = {
    ...(options.headers || {}),
    [MOBILE_SESSION_HEADER_NAME]: sessionToken || "",
  };
  if (!sessionToken) {
    delete nextHeaders[MOBILE_SESSION_HEADER_NAME];
  }

  const response = await fetch(url, {
    ...options,
    credentials: "include",
    headers: nextHeaders,
  });
  const text = await response.text();

  let body = {};
  if (text) {
    try {
      body = JSON.parse(text);
    } catch {
      body = {};
    }
  }

  if (!response.ok) {
    const error = new Error(
      body.error || body.details || `Request failed (${response.status})`,
    );
    error.status = response.status;
    throw error;
  }

  return body;
}

const styles = StyleSheet.create({
  safeArea: {
    flex: 1,
    backgroundColor: "#edf2fa",
  },
  headerShell: {
    paddingHorizontal: 18,
    paddingVertical: 14,
    backgroundColor: "#f8faff",
    borderBottomWidth: 1,
    borderBottomColor: "#d5e3e3",
    gap: 2,
  },
  headerAccent: {
    width: 58,
    height: 6,
    borderRadius: 999,
    backgroundColor: "#1b4f99",
    marginBottom: 4,
  },
  headerTitle: {
    fontSize: 24,
    fontWeight: "800",
    color: "#0f172a",
  },
  headerSubtitle: {
    fontSize: 13,
    color: "#3f526f",
  },
  headerMeta: {
    fontSize: 11,
    color: "#64748b",
    marginTop: 2,
  },
  headerSessionRow: {
    marginTop: 4,
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 8,
  },
  headerSessionButton: {
    borderWidth: 1,
    borderColor: "#c4d3df",
    borderRadius: 999,
    paddingHorizontal: 10,
    paddingVertical: 4,
    backgroundColor: "#ffffff",
  },
  headerSessionButtonText: {
    fontSize: 11,
    fontWeight: "700",
    color: "#1f3b57",
  },
  tabsRow: {
    flexDirection: "row",
    gap: 8,
    paddingHorizontal: 14,
    paddingVertical: 10,
    backgroundColor: "#edf2fa",
  },
  tabButton: {
    flex: 1,
    borderRadius: 12,
    borderWidth: 1,
    borderColor: "#cbd9e6",
    backgroundColor: "#ffffff",
    paddingVertical: 10,
    alignItems: "center",
    justifyContent: "center",
  },
  tabButtonActive: {
    backgroundColor: "#102a56",
    borderColor: "#102a56",
  },
  tabButtonText: {
    color: "#26415f",
    fontWeight: "700",
    fontSize: 13,
  },
  tabButtonTextActive: {
    color: "#f8fafc",
  },
  screenContent: {
    padding: 14,
    gap: 12,
    paddingBottom: 30,
  },
  authLoadingWrap: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    gap: 8,
    paddingHorizontal: 24,
  },
  authCard: {
    backgroundColor: "#ffffff",
    borderRadius: 16,
    padding: 16,
    borderWidth: 1,
    borderColor: "#d6e1ec",
    gap: 10,
  },
  authTitle: {
    fontSize: 20,
    fontWeight: "800",
    color: "#0f172a",
  },
  authText: {
    fontSize: 13,
    color: "#526a86",
  },
  sectionCard: {
    backgroundColor: "#ffffff",
    borderRadius: 16,
    padding: 14,
    borderWidth: 1,
    borderColor: "#d6e1ec",
    gap: 10,
  },
  sectionHeaderRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: 10,
  },
  sectionTitle: {
    fontSize: 18,
    fontWeight: "800",
    color: "#0f172a",
  },
  metaText: {
    color: "#5b708b",
    fontSize: 12,
  },
  searchInput: {
    borderWidth: 1,
    borderColor: "#c8d5e3",
    borderRadius: 12,
    backgroundColor: "#f6fafc",
    paddingHorizontal: 12,
    paddingVertical: 10,
    fontSize: 14,
    color: "#0f172a",
  },
  filterRow: {
    flexDirection: "row",
    gap: 8,
  },
  filterChip: {
    paddingHorizontal: 12,
    paddingVertical: 8,
    borderRadius: 999,
    borderWidth: 1,
    borderColor: "#c6d3df",
    backgroundColor: "#f7fafc",
  },
  filterChipActive: {
    backgroundColor: "#102a56",
    borderColor: "#102a56",
  },
  filterChipText: {
    fontSize: 12,
    fontWeight: "700",
    color: "#29425d",
  },
  filterChipTextActive: {
    color: "#f8fafc",
  },
  metricsRow: {
    flexDirection: "row",
    gap: 8,
  },
  metricCard: {
    flex: 1,
    borderRadius: 12,
    padding: 10,
    borderWidth: 1,
  },
  metricBlue: {
    backgroundColor: "#eef4ff",
    borderColor: "#c6d7fb",
  },
  metricGreen: {
    backgroundColor: "#e8effa",
    borderColor: "#c8d8f7",
  },
  metricAmber: {
    backgroundColor: "#fff7ea",
    borderColor: "#f4deb3",
  },
  metricLabel: {
    fontSize: 12,
    color: "#52667f",
    fontWeight: "700",
  },
  metricValue: {
    marginTop: 6,
    fontSize: 16,
    fontWeight: "800",
    color: "#0f172a",
  },
  recordCard: {
    borderWidth: 1,
    borderColor: "#d4e0ea",
    borderRadius: 14,
    backgroundColor: "#fbfdff",
    padding: 12,
    gap: 8,
  },
  recordCardPressed: {
    backgroundColor: "#f4f8ff",
    borderColor: "#b8caec",
  },
  recordCardHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "flex-start",
    gap: 10,
  },
  recordTitleWrap: {
    flex: 1,
    gap: 2,
  },
  recordTitle: {
    fontSize: 16,
    fontWeight: "800",
    color: "#0f172a",
  },
  recordSubtitle: {
    fontSize: 13,
    color: "#4f647d",
  },
  recordMetaLine: {
    fontSize: 12,
    color: "#4c5f77",
  },
  recordCardActionsRow: {
    flexDirection: "row",
    justifyContent: "flex-end",
  },
  primaryActionButton: {
    borderRadius: 10,
    backgroundColor: "#102a56",
    paddingHorizontal: 12,
    paddingVertical: 8,
    alignItems: "center",
    justifyContent: "center",
  },
  primaryActionButtonText: {
    color: "#f8fafc",
    fontWeight: "700",
    fontSize: 12,
  },
  secondaryActionButton: {
    borderRadius: 10,
    borderWidth: 1,
    borderColor: "#b9c9d9",
    backgroundColor: "#f4f8fb",
    paddingHorizontal: 10,
    paddingVertical: 7,
    alignItems: "center",
    justifyContent: "center",
  },
  secondaryActionButtonText: {
    color: "#1d3a59",
    fontWeight: "700",
    fontSize: 12,
  },
  statusChipRow: {
    flexDirection: "row",
    gap: 6,
    flexWrap: "wrap",
  },
  statusChip: {
    borderRadius: 999,
    borderWidth: 1,
    paddingHorizontal: 9,
    paddingVertical: 5,
  },
  statusChipText: {
    fontSize: 11,
    fontWeight: "700",
    color: "#0f172a",
  },
  statusWrittenOff: {
    backgroundColor: "#fee2e2",
    borderColor: "#fca5a5",
  },
  statusFullyPaid: {
    backgroundColor: "#e8effa",
    borderColor: "#b6caf2",
  },
  statusOverdue: {
    backgroundColor: "#ffedd5",
    borderColor: "#fdba74",
  },
  statusAfterResult: {
    backgroundColor: "#e0e7ff",
    borderColor: "#a5b4fc",
  },
  statusActive: {
    backgroundColor: "#eef3ff",
    borderColor: "#c5d7f8",
  },
  recordNumbersRow: {
    flexDirection: "row",
    gap: 8,
  },
  recordNumberCell: {
    flex: 1,
    borderRadius: 10,
    backgroundColor: "#f3f8fc",
    borderWidth: 1,
    borderColor: "#d8e4ef",
    paddingVertical: 8,
    paddingHorizontal: 8,
    gap: 2,
  },
  recordNumberLabel: {
    fontSize: 11,
    color: "#5a6f86",
    fontWeight: "700",
  },
  recordNumberValue: {
    fontSize: 13,
    color: "#0f172a",
    fontWeight: "800",
  },
  loadingCard: {
    backgroundColor: "#ffffff",
    borderRadius: 16,
    borderWidth: 1,
    borderColor: "#d6e1ec",
    alignItems: "center",
    justifyContent: "center",
    gap: 8,
    paddingVertical: 20,
  },
  loadingText: {
    color: "#32506a",
    fontWeight: "600",
    fontSize: 13,
  },
  inlineLoadingRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  inlineLoadingText: {
    color: "#32506a",
    fontWeight: "600",
    fontSize: 13,
  },
  savingLabel: {
    fontSize: 12,
    fontWeight: "700",
    color: "#102a56",
  },
  mutedText: {
    color: "#5e738d",
    fontSize: 13,
  },
  errorCard: {
    backgroundColor: "#fff1f2",
    borderRadius: 12,
    borderWidth: 1,
    borderColor: "#fecdd3",
    padding: 12,
  },
  errorText: {
    color: "#b91c1c",
    fontSize: 13,
    fontWeight: "600",
  },
  successText: {
    color: "#102a56",
    fontSize: 13,
    fontWeight: "700",
  },
  formSection: {
    borderTopWidth: 1,
    borderTopColor: "#e0e8f0",
    paddingTop: 10,
    gap: 10,
  },
  formSectionTitle: {
    fontSize: 15,
    fontWeight: "800",
    color: "#0f172a",
  },
  fieldGroup: {
    gap: 6,
  },
  fieldLabel: {
    fontSize: 12,
    color: "#38506a",
    fontWeight: "700",
  },
  fieldInput: {
    borderWidth: 1,
    borderColor: "#c8d5e3",
    borderRadius: 10,
    backgroundColor: "#f7fbfd",
    paddingHorizontal: 11,
    paddingVertical: 9,
    fontSize: 14,
    color: "#0f172a",
  },
  fieldInputMultiline: {
    minHeight: 96,
    textAlignVertical: "top",
  },
  checkboxRow: {
    borderWidth: 1,
    borderColor: "#d0dcea",
    borderRadius: 10,
    backgroundColor: "#f7fbfd",
    paddingHorizontal: 10,
    paddingVertical: 8,
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  readOnlyValueWrap: {
    borderWidth: 1,
    borderColor: "#d2dfeb",
    borderRadius: 10,
    backgroundColor: "#eef4f8",
    paddingHorizontal: 11,
    paddingVertical: 10,
  },
  readOnlyValue: {
    fontSize: 14,
    color: "#0f172a",
    fontWeight: "700",
  },
  submitButton: {
    marginTop: 8,
    borderRadius: 12,
    backgroundColor: "#102a56",
    paddingVertical: 12,
    alignItems: "center",
    justifyContent: "center",
  },
  submitButtonText: {
    color: "#ffffff",
    fontSize: 15,
    fontWeight: "800",
  },
  disabledButton: {
    opacity: 0.6,
  },
  modalRoot: {
    flex: 1,
    backgroundColor: "#edf2fa",
  },
  modalHeader: {
    paddingHorizontal: 14,
    paddingVertical: 10,
    borderBottomWidth: 1,
    borderBottomColor: "#d4e0ea",
    backgroundColor: "#f8faff",
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: 10,
  },
  modalTitle: {
    fontSize: 18,
    fontWeight: "800",
    color: "#0f172a",
  },
  ghostButton: {
    borderRadius: 10,
    borderWidth: 1,
    borderColor: "#b8c8d8",
    backgroundColor: "#f7fbfd",
    paddingHorizontal: 10,
    paddingVertical: 7,
  },
  ghostButtonText: {
    color: "#1f3f5f",
    fontWeight: "700",
    fontSize: 12,
  },
  modalContent: {
    padding: 14,
    gap: 12,
    paddingBottom: 22,
  },
  submissionCard: {
    backgroundColor: "#ffffff",
    borderRadius: 14,
    borderWidth: 1,
    borderColor: "#d6e1ec",
    padding: 12,
    gap: 10,
  },
  detailRow: {
    borderBottomWidth: 1,
    borderBottomColor: "#edf2f7",
    paddingBottom: 8,
    gap: 3,
  },
  detailLabel: {
    fontSize: 12,
    fontWeight: "700",
    color: "#34506f",
  },
  detailValue: {
    fontSize: 14,
    color: "#0f172a",
  },
  attachmentsCard: {
    backgroundColor: "#ffffff",
    borderRadius: 14,
    borderWidth: 1,
    borderColor: "#d6e1ec",
    padding: 12,
    gap: 10,
  },
  attachmentRow: {
    borderWidth: 1,
    borderColor: "#d8e4ef",
    borderRadius: 11,
    padding: 10,
    gap: 9,
  },
  attachmentInfo: {
    gap: 3,
  },
  attachmentName: {
    fontSize: 14,
    fontWeight: "700",
    color: "#0f172a",
  },
  attachmentMeta: {
    fontSize: 12,
    color: "#5e738d",
  },
  attachmentActions: {
    flexDirection: "row",
    gap: 8,
  },
  modalActionsBar: {
    paddingHorizontal: 14,
    paddingVertical: 12,
    borderTopWidth: 1,
    borderTopColor: "#d4e0ea",
    backgroundColor: "#f8faff",
    flexDirection: "row",
    gap: 10,
  },
  rejectButton: {
    flex: 1,
    borderRadius: 12,
    borderWidth: 1,
    borderColor: "#fca5a5",
    backgroundColor: "#fff1f2",
    paddingVertical: 11,
    alignItems: "center",
  },
  rejectButtonText: {
    color: "#b91c1c",
    fontWeight: "800",
    fontSize: 13,
  },
  approveButton: {
    flex: 1,
    borderRadius: 12,
    backgroundColor: "#102a56",
    paddingVertical: 11,
    alignItems: "center",
  },
  approveButtonText: {
    color: "#ffffff",
    fontWeight: "800",
    fontSize: 13,
  },
});
