const STORAGE_KEY = "cbooster_client_payments_v1";
const CSV_IMPORT_VERSION = "2026-02-17-credit-booster-v2";
const CSV_IMPORT_MARKER_KEY = `cbooster_csv_import_${CSV_IMPORT_VERSION}`;
const REMOTE_RECORDS_ENDPOINT = "/api/records";
const REMOTE_SYNC_DEBOUNCE_MS = 900;
const REMOTE_SYNC_RETRY_MS = 5000;
const IS_HTTP_CONTEXT = window.location.protocol === "http:" || window.location.protocol === "https:";
const FILTERS_PANEL_COLLAPSED_KEY = "cbooster_filters_panel_collapsed_v1";
const AUTH_SESSION_ENDPOINT = "/api/auth/session";
const AUTH_LOGOUT_PATH = "/logout";
const AUTH_LOGIN_PATH = "/login";
const STATUS_FILTER_ALL = "all";
const STATUS_FILTER_WRITTEN_OFF = "written-off";
const STATUS_FILTER_FULLY_PAID = "fully-paid";
const STATUS_FILTER_AFTER_RESULT = "after-result";
const STATUS_FILTER_OVERDUE = "overdue";
const OVERDUE_RANGE_FILTER_ALL = "";
const OVERDUE_RANGE_FILTERS = new Set(["1-7", "8-30", "31-60", "60+"]);
const SORT_DIRECTION_ASC = "asc";
const SORT_DIRECTION_DESC = "desc";
const DAY_IN_MS = 24 * 60 * 60 * 1000;
const PAYMENT_FIELDS = ["payment1", "payment2", "payment3", "payment4", "payment5", "payment6", "payment7"];
const PAYMENT_FIELD_SET = new Set(PAYMENT_FIELDS);
const PAYMENT_DATE_FIELDS = [
  "payment1Date",
  "payment2Date",
  "payment3Date",
  "payment4Date",
  "payment5Date",
  "payment6Date",
  "payment7Date",
];
const PAYMENT_DATE_FIELD_SET = new Set(PAYMENT_DATE_FIELDS);
const SUMMABLE_FIELDS = new Set([
  "contractTotals",
  "totalPayments",
  ...PAYMENT_FIELDS,
  "futurePayments",
  "collection",
]);
const MONEY_SORT_FIELDS = new Set([
  "contractTotals",
  "totalPayments",
  ...PAYMENT_FIELDS,
  "futurePayments",
]);
const ZERO_TOLERANCE = 0.005;
const TEXT_COLLATOR = new Intl.Collator("en-US", {
  numeric: true,
  sensitivity: "base",
});
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
const AFTER_RESULT_CLIENT_NAMES = new Set(
  [
    "Liviu Gurin",
    "Volodymyr Kasprii",
    "Filip Cvetkov",
    "Mekan Gurbanbayev",
    "Atai Taalaibekov",
    "Maksim Lenin",
    "Anastasiia Dovhaniuk",
    "Telman Akipov",
    "Artur Pyrogov",
    "Dmytro Shakin",
    "Mahir Aliyev",
    "Vasyl Feduniak",
    "Dmytro Kovalchuk",
    "Ilyas Veliev",
    "Muyassar Tulaganova",
    "Rostyslav Khariuk",
    "Kanat Omuraliev",
  ].map(normalizeClientName),
);
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

const FIELDS = [
  { key: "clientName", label: "Client name", type: "text", required: true },
  { key: "closedBy", label: "Closed by", type: "text" },
  { key: "companyName", label: "Company Name", type: "text" },
  { key: "serviceType", label: "Service Type", type: "text" },
  { key: "contractTotals", label: "Contract Totals", type: "text" },
  { key: "totalPayments", label: "Total Payments", type: "text", computed: true },
  { key: "payment1", label: "Payment 1", type: "text" },
  { key: "payment1Date", label: "Payment 1 Date", type: "date" },
  { key: "payment2", label: "Payment 2", type: "text" },
  { key: "payment2Date", label: "Payment 2 Date", type: "date" },
  { key: "payment3", label: "Payment 3", type: "text" },
  { key: "payment3Date", label: "Payment 3 Date", type: "date" },
  { key: "payment4", label: "Payment 4", type: "text" },
  { key: "payment4Date", label: "Payment 4 Date", type: "date" },
  { key: "payment5", label: "Payment 5", type: "text" },
  { key: "payment5Date", label: "Payment 5 Date", type: "date" },
  { key: "payment6", label: "Payment 6", type: "text" },
  { key: "payment6Date", label: "Payment 6 Date", type: "date" },
  { key: "payment7", label: "Payment 7", type: "text" },
  { key: "payment7Date", label: "Payment 7 Date", type: "date" },
  { key: "futurePayments", label: "Future Payments", type: "text", computed: true },
  { key: "afterResult", label: "After Result", type: "checkbox" },
  { key: "writtenOff", label: "Written Off", type: "checkbox" },
  { key: "notes", label: "Notes", type: "textarea" },
  { key: "collection", label: "COLLECTION", type: "text" },
  { key: "dateOfCollection", label: "Date of collection", type: "date" },
  { key: "dateWhenWrittenOff", label: "Date when written off", type: "date" },
];
const TABLE_FIELDS = FIELDS.filter((field) => field.key !== "writtenOff");
const FIELD_BY_KEY = new Map(FIELDS.map((field) => [field.key, field]));
const PAYMENT_PAIRS = [
  ["payment1", "payment1Date"],
  ["payment2", "payment2Date"],
  ["payment3", "payment3Date"],
  ["payment4", "payment4Date"],
  ["payment5", "payment5Date"],
  ["payment6", "payment6Date"],
  ["payment7", "payment7Date"],
];
const PAYMENT_DATE_FIELD_BY_AMOUNT_FIELD = new Map(PAYMENT_PAIRS);
const FORM_SECTION_LAYOUT = [
  {
    title: "Client Info",
    gridClassName: "form-section__grid form-section__grid--client-info",
    fields: ["clientName", "closedBy", "companyName", "serviceType"],
  },
  {
    title: "Contract",
    gridClassName: "form-section__grid form-section__grid--contract",
    fields: ["contractTotals", "futurePayments", "afterResult"],
  },
  {
    title: "Payments",
    payments: PAYMENT_PAIRS,
  },
  {
    title: "Collections",
    gridClassName: "form-section__grid form-section__grid--collections",
    fields: ["collection", "dateOfCollection", "writtenOff", "dateWhenWrittenOff"],
  },
  {
    title: "Notes",
    gridClassName: "form-section__grid form-section__grid--notes",
    fields: ["notes"],
  },
];
const CREATE_FORM_EXCLUDE_KEYS = new Set([
  "writtenOff",
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
]);

const form = document.querySelector("#client-form");
const formFields = document.querySelector("#form-fields");
const formMessage = document.querySelector("#form-message");
const pageShell = document.querySelector(".page-shell");
const pageHeaderElement = document.querySelector(".page-header");
const accountMenu = document.querySelector("#account-menu");
const accountMenuToggleButton = document.querySelector("#account-menu-toggle");
const accountMenuPanel = document.querySelector("#account-menu-panel");
const accountMenuUser = document.querySelector("#account-menu-user");
const accountLoginActionButton = document.querySelector("#account-login-action");
const accountLogoutActionButton = document.querySelector("#account-logout-action");
const dashboardGrid = document.querySelector(".dashboard-grid");
const filtersPanel = document.querySelector(".filters-panel");
const searchInput = document.querySelector("#search-input");
const clearSearchButton = document.querySelector("#clear-search");
const newClientDateFromInput = document.querySelector("#payment-date-from");
const newClientDateToInput = document.querySelector("#payment-date-to");
const paymentsDateFromInput = document.querySelector("#payments-date-from");
const paymentsDateToInput = document.querySelector("#payments-date-to");
const writtenOffDateFromInput = document.querySelector("#written-off-date-from");
const writtenOffDateToInput = document.querySelector("#written-off-date-to");
const writtenOffDateFilterBlock = document.querySelector("#written-off-date-filter");
const fullyPaidDateFromInput = document.querySelector("#fully-paid-date-from");
const fullyPaidDateToInput = document.querySelector("#fully-paid-date-to");
const fullyPaidDateFilterBlock = document.querySelector("#fully-paid-date-filter");
const overdueRangeFilterBlock = document.querySelector("#overdue-range-filter");
const closedByFilterSelect = document.querySelector("#closed-by-filter");
const clearDateFilterButton = document.querySelector("#clear-date-filter");
const toggleFiltersPanelButton = document.querySelector("#toggle-filters-panel");
const clientFormSection = document.querySelector("#client-form-section");
const toggleClientFormButton = document.querySelector("#toggle-client-form");
const closeClientFormButton = document.querySelector("#close-client-form");
const exportDropdown = document.querySelector("#export-dropdown");
const exportMenuToggleButton = document.querySelector("#export-menu-toggle");
const exportMenu = document.querySelector("#export-menu");
const exportXlsButton = document.querySelector("#export-xls");
const exportPdfButton = document.querySelector("#export-pdf");
const overviewPanel = document.querySelector(".period-dashboard-shell");
const overviewContent = document.querySelector("#overview-content");
const toggleOverviewPanelButton = document.querySelector("#toggle-overview-panel");
const overviewCollapsedSummary = document.querySelector("#overview-collapsed-summary");
const overviewSummaryDebt = document.querySelector("#overview-summary-debt");
const overviewSummarySales = document.querySelector("#overview-summary-sales");
const overviewSummaryReceived = document.querySelector("#overview-summary-received");
const tablePanel = document.querySelector(".table-panel");
const tableWrap = document.querySelector(".table-wrap");
const tableHead = document.querySelector("#table-head");
const tableBody = document.querySelector("#table-body");
const tableFoot = document.querySelector("#table-foot");
const recordCount = document.querySelector("#record-count");
const filteredCount = document.querySelector("#filtered-count");
const writtenOffCount = document.querySelector("#written-off-count");
const fullyPaidCount = document.querySelector("#fully-paid-count");
const overdueCount = document.querySelector("#overdue-count");
const OVERVIEW_PERIOD_DEFAULT = "currentWeek";
const OVERVIEW_PERIOD_KEYS = {
  currentWeek: "Current Week",
  previousWeek: "Previous Week",
  currentMonth: "Current Month",
  last30Days: "Last 30 Days",
};
const overviewSalesValue = document.querySelector("#overview-sales-value");
const overviewDebtValue = document.querySelector("#overview-debt-value");
const overviewReceivedValue = document.querySelector("#overview-received-value");
const overviewSalesContext = document.querySelector("#overview-sales-context");
const overviewReceivedContext = document.querySelector("#overview-received-context");
const overviewPeriodButtons = [...document.querySelectorAll(".overview-period-toggle")];
const statusFilterButtons = [...document.querySelectorAll(".status-filter-group .table-filter-btn")];
const overdueRangeFilterButtons = [...document.querySelectorAll(".overdue-range-filter-btn")];
const editModal = document.querySelector("#edit-modal");
const editClientForm = document.querySelector("#edit-client-form");
const editFormFields = document.querySelector("#edit-form-fields");
const editFormMessage = document.querySelector("#edit-form-message");
const editCloseButtons = [...document.querySelectorAll("[data-close-edit-modal]")];
const enableEditModeButton = document.querySelector("#enable-edit-mode");
const saveEditChangesButton = document.querySelector("#save-edit-changes");
const cancelEditModeButton = document.querySelector("#cancel-edit-mode");
const editModalStatusChips = document.querySelector("#edit-modal-status-chips");
const editModalDialog = editModal?.querySelector(".modal-dialog") || null;
initializeDateInputFeatures(newClientDateFromInput);
initializeDateInputFeatures(newClientDateToInput);
initializeDateInputFeatures(paymentsDateFromInput);
initializeDateInputFeatures(paymentsDateToInput);
initializeDateInputFeatures(writtenOffDateFromInput);
initializeDateInputFeatures(writtenOffDateToInput);
initializeDateInputFeatures(fullyPaidDateFromInput);
initializeDateInputFeatures(fullyPaidDateToInput);

applyCsvImportOnce();
let records = loadRecords();
removeDeprecatedFieldsFromRecords();
let activeStatusFilter = STATUS_FILTER_ALL;
let activeOverdueRangeFilter = OVERDUE_RANGE_FILTER_ALL;
let activeOverviewPeriod = OVERVIEW_PERIOD_DEFAULT;
let activeSortKey = "";
let activeSortDirection = SORT_DIRECTION_ASC;
let currentPaymentsDateRange = { from: null, to: null };
let editingRecordId = "";
let isCardEditMode = false;
let lastFocusedElementBeforeModal = null;
let currentAuthUser = "";
let isRemoteSyncEnabled = IS_HTTP_CONTEXT;
let hasCompletedInitialRemoteSync = false;
let remoteSyncTimeoutId = null;
let isRemoteSyncInFlight = false;
let hasPendingRemoteSync = false;
applyAfterResultFlags();
recalculateFuturePaymentsForAllRecords();

renderFormFields();
renderEditFormFields();
bindWrittenOffAutomation(form);
bindWrittenOffAutomation(editClientForm);
bindFuturePaymentsPreview();
bindEditFuturePaymentsPreview();
collapseCreatePaymentsSection();
refreshClosedByFilterOptions();
initializeStatusFilterButtons();
initializeOverdueRangeFilterButtons();
initializeOverviewPeriodButtons();
initializeOverviewPanelToggle();
initializeExportDropdown();
initializeEditModal();
initializeAccountMenu();
initializeAuthGate();
initializeAuthSession();
renderTableHead();
renderTableLoadingState();
requestAnimationFrame(() => {
  renderTable();
});
syncFiltersStickyOffset();
initializeFiltersPanelToggle();
window.addEventListener("resize", syncFiltersStickyOffset);
setClientFormVisibility(false);
void hydrateRecordsFromRemote();

form.addEventListener("submit", (event) => {
  event.preventDefault();
  clearMessage();

  const formData = new FormData(form);
  const nextRecord = {};

  for (const field of FIELDS) {
    if (field.computed) {
      continue;
    }

    if (field.type === "checkbox") {
      const checkbox = form.querySelector(`[name=\"${field.key}\"]`);
      nextRecord[field.key] = checkbox?.checked ? "Yes" : "";
      continue;
    }

    const rawValue = formData.get(field.key);
    const value = (rawValue ?? "").toString().trim();

    if (field.type === "date") {
      const normalizedDate = normalizeDateForStorage(value);
      if (value && normalizedDate === null) {
        showMessage(`Field "${field.label}" must be in MM/DD/YYYY format`, "error");
        focusField(field.key);
        return;
      }

      nextRecord[field.key] = normalizedDate || "";
      continue;
    }

    if (field.required && !value) {
      showMessage(`Field "${field.label}" is required`, "error");
      focusField(field.key);
      return;
    }

    nextRecord[field.key] = value;
  }

  if (isWrittenOffEnabled(nextRecord.writtenOff)) {
    if (parseDateValue(nextRecord.dateWhenWrittenOff) === null) {
      nextRecord.dateWhenWrittenOff = getTodayDateUs();
    }
  } else if (!isWrittenOffByList(nextRecord.clientName)) {
    nextRecord.dateWhenWrittenOff = "";
  }

  applyDerivedRecordState(nextRecord);

  records.unshift({
    id: generateId(),
    createdAt: new Date().toISOString(),
    ...nextRecord,
  });

  persistRecords();
  refreshClosedByFilterOptions();
  renderTable();
  form.reset();
  updateFuturePaymentsPreview();
  collapseCreatePaymentsSection();
  showMessage("Client added", "success");
});

searchInput.addEventListener("input", () => {
  renderTable();
});

clearSearchButton.addEventListener("click", () => {
  searchInput.value = "";
  renderTable();
  searchInput.focus();
});

if (closedByFilterSelect) {
  closedByFilterSelect.addEventListener("change", () => {
    renderTable();
  });
}

for (const dateInput of [
  newClientDateFromInput,
  newClientDateToInput,
  paymentsDateFromInput,
  paymentsDateToInput,
  writtenOffDateFromInput,
  writtenOffDateToInput,
  fullyPaidDateFromInput,
  fullyPaidDateToInput,
]) {
  if (dateInput) {
    dateInput.addEventListener("input", () => {
      renderTable();
    });
  }
}

if (clearDateFilterButton) {
  clearDateFilterButton.addEventListener("click", () => {
    if (searchInput) {
      searchInput.value = "";
    }

    if (closedByFilterSelect) {
      closedByFilterSelect.value = "";
    }

    for (const dateInput of [
      newClientDateFromInput,
      newClientDateToInput,
      paymentsDateFromInput,
      paymentsDateToInput,
      writtenOffDateFromInput,
      writtenOffDateToInput,
      fullyPaidDateFromInput,
      fullyPaidDateToInput,
    ]) {
      if (dateInput) {
        dateInput.value = "";
      }
    }

    activeStatusFilter = STATUS_FILTER_ALL;
    activeOverdueRangeFilter = OVERDUE_RANGE_FILTER_ALL;
    syncStatusFilterButtons();
    renderTable();
  });
}

if (exportXlsButton) {
  exportXlsButton.addEventListener("click", () => {
    exportVisibleTableToXls();
    setExportMenuVisibility(false);
  });
}

if (exportPdfButton) {
  exportPdfButton.addEventListener("click", () => {
    exportVisibleTableToPdf();
    setExportMenuVisibility(false);
  });
}

form.addEventListener("reset", () => {
  requestAnimationFrame(() => {
    updateFuturePaymentsPreview();
    collapseCreatePaymentsSection();
  });
});

if (closeClientFormButton) {
  closeClientFormButton.addEventListener("click", () => {
    setClientFormVisibility(false);
    clearMessage();
    scrollPageToTop();
  });
}

toggleClientFormButton.addEventListener("click", () => {
  const isOpen = !clientFormSection.hidden;
  setClientFormVisibility(!isOpen);

  if (clientFormSection.hidden) {
    return;
  }

  focusField("clientName");
});

if (editClientForm) {
  editClientForm.addEventListener("submit", (event) => {
    event.preventDefault();
    clearEditMessage();

    if (!isCardEditMode) {
      return;
    }

    const recordIndex = records.findIndex((record) => record.id === editingRecordId);
    if (recordIndex === -1) {
      showEditMessage("Client not found", "error");
      return;
    }

    const formData = new FormData(editClientForm);
    const updatedRecord = { ...records[recordIndex] };

    for (const field of FIELDS) {
      if (field.computed) {
        continue;
      }

      const control = editClientForm.querySelector(`[name="${field.key}"]`);
      if (!control) {
        continue;
      }

      if (field.type === "checkbox") {
        updatedRecord[field.key] = control.checked ? "Yes" : "";
        continue;
      }

      const rawValue = formData.get(field.key);
      const value = (rawValue ?? "").toString().trim();

      if (field.type === "date") {
        const normalizedDate = normalizeDateForStorage(value);
        if (value && normalizedDate === null) {
          showEditMessage(`Field "${field.label}" must be in MM/DD/YYYY format`, "error");
          focusEditField(field.key);
          return;
        }

        updatedRecord[field.key] = normalizedDate || "";
        continue;
      }

      if (field.required && !value) {
        showEditMessage(`Field "${field.label}" is required`, "error");
        focusEditField(field.key);
        return;
      }

      updatedRecord[field.key] = value;
    }

    if (isWrittenOffEnabled(updatedRecord.writtenOff)) {
      if (parseDateValue(updatedRecord.dateWhenWrittenOff) === null) {
        updatedRecord.dateWhenWrittenOff = getTodayDateUs();
      }
    } else if (!isWrittenOffByList(updatedRecord.clientName)) {
      updatedRecord.dateWhenWrittenOff = "";
    }

    applyDerivedRecordState(updatedRecord, records[recordIndex]);
    records[recordIndex] = updatedRecord;

    persistRecords();
    refreshClosedByFilterOptions();
    renderTable();
    fillEditForm(updatedRecord);
    setCardEditMode(false);
    showEditMessage("Changes saved", "success");
  });
}

if (enableEditModeButton) {
  enableEditModeButton.addEventListener("click", () => {
    const currentRecord = getEditingRecord();
    if (!currentRecord) {
      return;
    }

    setCardEditMode(true);
    clearEditMessage();
    focusEditField("clientName");
  });
}

if (cancelEditModeButton) {
  cancelEditModeButton.addEventListener("click", () => {
    const currentRecord = getEditingRecord();
    if (!currentRecord) {
      return;
    }

    fillEditForm(currentRecord);
    setCardEditMode(false);
    clearEditMessage();
  });
}

function initializeEditModal() {
  if (!editModal || !editClientForm || !editFormFields || !editFormMessage) {
    return;
  }

  for (const element of editCloseButtons) {
    element.addEventListener("click", () => {
      setEditModalVisibility(false);
    });
  }

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !editModal.hidden) {
      setEditModalVisibility(false);
    }
  });

  if (editModalDialog) {
    editModalDialog.addEventListener("keydown", trapModalFocus);
  }
}

function openEditModal(recordId) {
  const record = records.find((item) => item.id === recordId);
  if (!record || !editClientForm) {
    return;
  }

  editingRecordId = recordId;
  fillEditForm(record);
  setCardEditMode(false);
  clearEditMessage();
  setEditModalVisibility(true);

  if (enableEditModeButton instanceof HTMLButtonElement) {
    enableEditModeButton.focus();
    return;
  }

  const closeButton = editModal?.querySelector(".modal-close");
  if (closeButton instanceof HTMLButtonElement) {
    closeButton.focus();
  }
}

function fillEditForm(record) {
  if (!editClientForm) {
    return;
  }

  for (const field of FIELDS) {
    const control = editClientForm.querySelector(`[name="${field.key}"]`);
    if (!control) {
      continue;
    }

    if (field.type === "checkbox") {
      if (field.key === "writtenOff") {
        control.checked = isRecordWrittenOff(record);
      } else {
        control.checked = isAfterResultEnabled(record[field.key]);
      }
      continue;
    }

    if (field.key === "futurePayments") {
      control.value = computeFuturePayments(record);
      continue;
    }

    if (field.type === "date") {
      control.value = formatDateValueUs(record[field.key]);
      continue;
    }

    control.value = (record[field.key] ?? "").toString().trim();
  }

  updateEditFuturePaymentsPreview();
  syncEditPaymentRowVisibility();
  syncEditCollectionsSectionVisibility();
  syncEditNotesSectionVisibility();
  syncEditAfterResultFieldVisibility();
  renderEditModalStatusChips(record);
}

function renderEditModalStatusChips(record) {
  if (!editModalStatusChips) {
    return;
  }

  editModalStatusChips.replaceChildren();

  if (!record) {
    return;
  }

  const statusFlags = getRecordStatusFlags(record);
  const statusChipList = buildStatusChipList(statusFlags);
  editModalStatusChips.append(statusChipList);
}

function getEditingRecord() {
  if (!editingRecordId) {
    return null;
  }

  return records.find((record) => record.id === editingRecordId) || null;
}

function setEditModalVisibility(isVisible) {
  if (!editModal) {
    return;
  }

  if (isVisible) {
    lastFocusedElementBeforeModal = document.activeElement;
  }

  editModal.hidden = !isVisible;
  document.body.classList.toggle("modal-open", isVisible);

  if (!isVisible) {
    setCardEditMode(false);
    editingRecordId = "";
    renderEditModalStatusChips(null);
    clearEditMessage();
    if (lastFocusedElementBeforeModal instanceof HTMLElement) {
      lastFocusedElementBeforeModal.focus();
    }
  }
}

function setCardEditMode(nextMode) {
  isCardEditMode = Boolean(nextMode);

  if (!editClientForm) {
    return;
  }

  for (const field of FIELDS) {
    if (field.computed) {
      continue;
    }

    const control = editClientForm.querySelector(`[name="${field.key}"]`);
    if (!control) {
      continue;
    }

    control.disabled = !isCardEditMode;
  }

  if (enableEditModeButton) {
    enableEditModeButton.hidden = isCardEditMode;
  }

  if (saveEditChangesButton) {
    saveEditChangesButton.hidden = !isCardEditMode;
  }

  if (cancelEditModeButton) {
    cancelEditModeButton.hidden = !isCardEditMode;
  }

  syncEditPaymentRowVisibility();
  syncEditCollectionsSectionVisibility();
  syncEditNotesSectionVisibility();
  syncEditAfterResultFieldVisibility();
}

function syncEditPaymentRowVisibility() {
  if (!editFormFields || !editClientForm) {
    return;
  }

  const paymentRows = editFormFields.querySelectorAll(".payment-row");
  for (const row of paymentRows) {
    const amountKey = row.dataset.paymentAmountKey;
    const dateKey = row.dataset.paymentDateKey;
    if (!amountKey || !dateKey) {
      continue;
    }

    const amountControl = editClientForm.querySelector(`[name="${amountKey}"]`);
    const dateControl = editClientForm.querySelector(`[name="${dateKey}"]`);
    if (!(amountControl instanceof HTMLInputElement) || !(dateControl instanceof HTMLInputElement)) {
      continue;
    }

    const paymentMatch = amountKey.match(/^payment(\d+)$/);
    const paymentNumber = paymentMatch ? Number(paymentMatch[1]) : 0;
    const isPaymentOne = paymentNumber === 1;

    if (isCardEditMode || isPaymentOne) {
      row.hidden = false;
      continue;
    }

    const hasAmount = Boolean(amountControl.value.trim());
    const hasDate = Boolean(dateControl.value.trim());
    row.hidden = !hasAmount && !hasDate;
  }
}

function syncEditCollectionsSectionVisibility() {
  if (!editFormFields || !editClientForm) {
    return;
  }

  const collectionsSection = editFormFields.querySelector('.form-section[data-section-key="collections"]');
  if (!collectionsSection) {
    return;
  }

  if (isCardEditMode) {
    collectionsSection.hidden = false;
    return;
  }

  const collectionText = (editClientForm.querySelector('[name="collection"]')?.value || "").toString().trim();
  const dateOfCollection = (editClientForm.querySelector('[name="dateOfCollection"]')?.value || "").toString().trim();
  const dateWhenWrittenOff = (editClientForm.querySelector('[name="dateWhenWrittenOff"]')?.value || "")
    .toString()
    .trim();
  const writtenOffChecked = Boolean(editClientForm.querySelector('[name="writtenOff"]')?.checked);

  const hasCollectionData =
    Boolean(collectionText) || Boolean(dateOfCollection) || Boolean(dateWhenWrittenOff) || writtenOffChecked;
  collectionsSection.hidden = !hasCollectionData;
}

function syncEditNotesSectionVisibility() {
  if (!editFormFields || !editClientForm) {
    return;
  }

  const notesSection = editFormFields.querySelector('.form-section[data-section-key="notes"]');
  if (!notesSection) {
    return;
  }

  if (isCardEditMode) {
    notesSection.hidden = false;
    return;
  }

  const notesValue = (editClientForm.querySelector('[name="notes"]')?.value || "").toString().trim();
  notesSection.hidden = !notesValue;
}

function syncEditAfterResultFieldVisibility() {
  if (!editClientForm) {
    return;
  }

  const afterResultControl = editClientForm.querySelector('[name="afterResult"]');
  if (!(afterResultControl instanceof HTMLInputElement)) {
    return;
  }

  const afterResultField = afterResultControl.closest(".field-checkbox");
  if (!afterResultField) {
    return;
  }

  if (isCardEditMode) {
    afterResultField.hidden = false;
    return;
  }

  afterResultField.hidden = !afterResultControl.checked;
}

function initializeStatusFilterButtons() {
  if (!statusFilterButtons.length) {
    return;
  }

  for (const button of statusFilterButtons) {
    button.addEventListener("click", () => {
      const nextFilter = button.dataset.statusFilter || STATUS_FILTER_ALL;
      if (nextFilter === activeStatusFilter) {
        return;
      }

      activeStatusFilter = nextFilter;
      if (activeStatusFilter !== STATUS_FILTER_OVERDUE) {
        activeOverdueRangeFilter = OVERDUE_RANGE_FILTER_ALL;
      }
      syncStatusFilterButtons();
      renderTable();
    });
  }

  syncStatusFilterButtons();
}

function initializeOverdueRangeFilterButtons() {
  if (!overdueRangeFilterButtons.length) {
    syncOverdueRangeFilterButtons();
    return;
  }

  for (const button of overdueRangeFilterButtons) {
    button.addEventListener("click", () => {
      const nextRange = (button.dataset.overdueRange || "").trim();
      if (!OVERDUE_RANGE_FILTERS.has(nextRange)) {
        return;
      }

      activeOverdueRangeFilter =
        activeOverdueRangeFilter === nextRange ? OVERDUE_RANGE_FILTER_ALL : nextRange;
      syncOverdueRangeFilterButtons();
      renderTable();
    });
  }

  syncOverdueRangeFilterButtons();
}

function initializeOverviewPeriodButtons() {
  if (!overviewPeriodButtons.length) {
    return;
  }

  for (const button of overviewPeriodButtons) {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      const nextPeriod = (button.dataset.overviewPeriod || "").trim();
      if (!Object.prototype.hasOwnProperty.call(OVERVIEW_PERIOD_KEYS, nextPeriod)) {
        return;
      }

      activeOverviewPeriod = nextPeriod;

      syncOverviewPeriodButtons();
      updatePeriodDashboard(records);
    });
  }

  syncOverviewPeriodButtons();
}

function syncOverviewPeriodButtons() {
  for (const button of overviewPeriodButtons) {
    const periodKey = (button.dataset.overviewPeriod || "").trim();
    const isActive = periodKey === activeOverviewPeriod;

    button.classList.toggle("is-active", isActive);
    button.setAttribute("aria-pressed", String(isActive));
  }
}

function syncStatusFilterButtons() {
  for (const button of statusFilterButtons) {
    const filterValue = button.dataset.statusFilter || STATUS_FILTER_ALL;
    const isActive = filterValue === activeStatusFilter;
    button.classList.toggle("is-active", isActive);
    button.setAttribute("aria-pressed", String(isActive));
  }

  syncStatusDateFiltersVisibility();
}

function setStatusDateFilterVisibility(block, isVisible) {
  if (!block) {
    return;
  }

  // Keep date-range blocks strictly tied to the active status filter.
  block.hidden = !isVisible;
  block.style.display = isVisible ? "" : "none";
  block.setAttribute("aria-hidden", String(!isVisible));
}

function syncStatusDateFiltersVisibility() {
  setStatusDateFilterVisibility(writtenOffDateFilterBlock, activeStatusFilter === STATUS_FILTER_WRITTEN_OFF);
  setStatusDateFilterVisibility(fullyPaidDateFilterBlock, activeStatusFilter === STATUS_FILTER_FULLY_PAID);
  syncOverdueRangeFilterButtons();
}

function syncOverdueRangeFilterButtons() {
  const isVisible = activeStatusFilter === STATUS_FILTER_OVERDUE;
  setStatusDateFilterVisibility(overdueRangeFilterBlock, isVisible);

  for (const button of overdueRangeFilterButtons) {
    const rangeValue = (button.dataset.overdueRange || "").trim();
    const isActive = isVisible && rangeValue === activeOverdueRangeFilter && activeOverdueRangeFilter !== "";
    button.classList.toggle("is-active", isActive);
    button.setAttribute("aria-pressed", String(isActive));
  }
}

function initializeExportDropdown() {
  if (!exportDropdown || !exportMenuToggleButton || !exportMenu) {
    return;
  }

  exportMenuToggleButton.addEventListener("click", (event) => {
    event.stopPropagation();
    setExportMenuVisibility(exportMenu.hidden);
  });

  exportMenu.addEventListener("click", (event) => {
    event.stopPropagation();
  });

  document.addEventListener("click", (event) => {
    if (!(event.target instanceof Node) || exportMenu.hidden) {
      return;
    }

    if (!exportDropdown.contains(event.target)) {
      setExportMenuVisibility(false);
    }
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      setExportMenuVisibility(false);
    }
  });

  setExportMenuVisibility(false);
}

function setExportMenuVisibility(isVisible) {
  if (!exportDropdown || !exportMenuToggleButton || !exportMenu) {
    return;
  }

  exportMenu.hidden = !isVisible;
  exportMenuToggleButton.setAttribute("aria-expanded", String(isVisible));
  exportDropdown.classList.toggle("is-open", isVisible);
}

function renderFormFields() {
  renderFieldsIntoContainer(formFields, "", {
    excludeKeys: CREATE_FORM_EXCLUDE_KEYS,
  });
}

function renderEditFormFields() {
  renderFieldsIntoContainer(editFormFields, "edit-", {
    excludeKeys: new Set(["futurePayments"]),
  });
}

function renderFieldsIntoContainer(container, idPrefix, options = {}) {
  if (!container) {
    return;
  }

  const excludeKeys = options.excludeKeys || new Set();
  const fragment = document.createDocumentFragment();

  for (const sectionConfig of FORM_SECTION_LAYOUT) {
    const section = document.createElement("section");
    section.className = "form-section";
    section.dataset.sectionKey = (sectionConfig.title || "")
      .toString()
      .trim()
      .toLowerCase()
      .replace(/\s+/g, "-");

    const sectionTitle = document.createElement("h3");
    sectionTitle.className = "form-section__title";
    sectionTitle.textContent = sectionConfig.title;

    let sectionBody;

    if (sectionConfig.payments) {
      sectionBody = document.createElement("div");
      sectionBody.className = "form-section__payments";
      const visiblePaymentCount = idPrefix === "" ? 1 : sectionConfig.payments.length;

      for (const [paymentIndex, [amountKey, dateKey]] of sectionConfig.payments.entries()) {
        if (paymentIndex >= visiblePaymentCount) {
          continue;
        }

        const pairRow = document.createElement("div");
        pairRow.className = "payment-row";
        pairRow.dataset.paymentAmountKey = amountKey;
        pairRow.dataset.paymentDateKey = dateKey;

        const amountField = FIELD_BY_KEY.get(amountKey);
        const dateField = FIELD_BY_KEY.get(dateKey);

        if (amountField && !excludeKeys.has(amountField.key)) {
          pairRow.append(createFieldControl(amountField, idPrefix));
        }

        if (dateField && !excludeKeys.has(dateField.key)) {
          pairRow.append(createFieldControl(dateField, idPrefix));
        }

        if (pairRow.childElementCount === 0) {
          continue;
        }

        sectionBody.append(pairRow);
      }
    } else {
      sectionBody = document.createElement("div");
      sectionBody.className = sectionConfig.gridClassName || "form-section__grid";

      for (const fieldKey of sectionConfig.fields || []) {
        if (excludeKeys.has(fieldKey)) {
          continue;
        }

        const field = FIELD_BY_KEY.get(fieldKey);
        if (!field) {
          continue;
        }

        sectionBody.append(createFieldControl(field, idPrefix));
      }
    }

    if (!sectionBody || sectionBody.childElementCount === 0) {
      continue;
    }

    section.append(sectionTitle, sectionBody);
    fragment.append(section);
  }

  container.replaceChildren(fragment);
}

function createFieldControl(field, idPrefix) {
  const wrapper = document.createElement("div");
  wrapper.className = "field";

  const label = document.createElement("label");
  const fieldId = `${idPrefix}${field.key}`;
  label.setAttribute("for", fieldId);
  label.textContent = field.required ? `${field.label} *` : field.label;

  let control;

  if (field.type === "textarea") {
    control = document.createElement("textarea");
  } else if (field.type === "select") {
    control = document.createElement("select");
    for (const option of field.options || []) {
      const optionElement = document.createElement("option");
      optionElement.value = option.value;
      optionElement.textContent = option.label;
      control.append(optionElement);
    }
  } else {
    control = document.createElement("input");
  }

  control.id = fieldId;
  control.name = field.key;
  control.required = Boolean(field.required);
  control.setAttribute("aria-label", field.label);

  if (field.type !== "textarea" && field.type !== "select") {
    control.type = field.type;
  }

  if (field.type === "date") {
    control.type = "text";
    control.inputMode = "numeric";
    control.placeholder = "MM/DD/YYYY";
  }

  if (field.type === "checkbox") {
    control.checked = false;
    control.value = "Yes";
    wrapper.classList.add("field-checkbox");

    const checkboxLabel = document.createElement("label");
    checkboxLabel.className = "checkbox-label";
    checkboxLabel.setAttribute("for", fieldId);
    checkboxLabel.append(control, document.createTextNode(field.label));

    wrapper.append(checkboxLabel);
    return wrapper;
  }

  if (field.computed) {
    control.readOnly = true;
    control.placeholder = "Calculated automatically";
  }

  wrapper.append(label, control);

  if (field.type === "date") {
    initializeDateInputFeatures(control);
  }

  return wrapper;
}

function renderTableHead() {
  const row = document.createElement("tr");

  for (const field of TABLE_FIELDS) {
    const cell = document.createElement("th");
    cell.scope = "col";

    const sortButton = document.createElement("button");
    sortButton.type = "button";
    sortButton.className = "th-sort-btn";
    sortButton.setAttribute("aria-label", `Sort by ${field.label}`);
    sortButton.addEventListener("click", () => {
      toggleSort(field.key);
    });

    const label = document.createElement("span");
    label.className = "th-sort-label";
    label.textContent = field.label;

    const indicator = document.createElement("span");
    indicator.className = "th-sort-indicator";
    indicator.setAttribute("aria-hidden", "true");

    const isActive = activeSortKey === field.key;
    if (isActive) {
      sortButton.classList.add("is-active");
      indicator.textContent = activeSortDirection === SORT_DIRECTION_ASC ? "▲" : "▼";
      cell.setAttribute("aria-sort", activeSortDirection === SORT_DIRECTION_ASC ? "ascending" : "descending");
    } else {
      indicator.textContent = "↕";
      cell.setAttribute("aria-sort", "none");
    }

    sortButton.append(label, indicator);
    cell.append(sortButton);
    row.append(cell);
  }

  tableHead.replaceChildren(row);
}

function toggleSort(fieldKey) {
  if (activeSortKey === fieldKey) {
    activeSortDirection = activeSortDirection === SORT_DIRECTION_ASC ? SORT_DIRECTION_DESC : SORT_DIRECTION_ASC;
  } else {
    activeSortKey = fieldKey;
    activeSortDirection = SORT_DIRECTION_ASC;
  }

  renderTableHead();
  renderTable();
}

function renderTable() {
  try {
    const query = searchInput.value.trim().toLowerCase();
    const newClientDateRange = getDateRangeBounds(newClientDateFromInput, newClientDateToInput);
    const paymentsDateRange = getDateRangeBounds(paymentsDateFromInput, paymentsDateToInput);
    const writtenOffDateRange = getDateRangeBounds(writtenOffDateFromInput, writtenOffDateToInput);
    const fullyPaidDateRange = getDateRangeBounds(fullyPaidDateFromInput, fullyPaidDateToInput);
    currentPaymentsDateRange = paymentsDateRange;
    const selectedClosedBy = normalizeClientName(closedByFilterSelect?.value || "");
    const hasNewClientDateRangeFilter = hasDateRangeValues(newClientDateRange);
    const hasPaymentsDateRangeFilter = hasDateRangeValues(paymentsDateRange);
    const hasWrittenOffDateRangeFilter = hasDateRangeValues(writtenOffDateRange);
    const hasFullyPaidDateRangeFilter = hasDateRangeValues(fullyPaidDateRange);
    const shouldApplyWrittenOffDateRangeFilter =
      activeStatusFilter === STATUS_FILTER_WRITTEN_OFF && hasWrittenOffDateRangeFilter;
    const shouldApplyFullyPaidDateRangeFilter =
      activeStatusFilter === STATUS_FILTER_FULLY_PAID && hasFullyPaidDateRangeFilter;
    const hasClosedByFilter = Boolean(selectedClosedBy);

    const queryMatchedRecords = query
      ? records.filter((record) => {
          const client = (record.clientName || "").toLowerCase();
          const company = (record.companyName || "").toLowerCase();
          return client.includes(query) || company.includes(query);
        })
      : records;
    const scopedRecords = queryMatchedRecords
      .filter((record) => matchesClosedByFilter(record, selectedClosedBy))
      .filter((record) => matchesNewClientDateRange(record, newClientDateRange.from, newClientDateRange.to))
      .filter((record) => matchesAnyPaymentDateRange(record, paymentsDateRange.from, paymentsDateRange.to))
      .filter((record) =>
        shouldApplyWrittenOffDateRangeFilter
          ? matchesWrittenOffDateRange(record, writtenOffDateRange.from, writtenOffDateRange.to)
          : true,
      )
      .filter((record) =>
        shouldApplyFullyPaidDateRangeFilter
          ? matchesFullyPaidDateRange(record, fullyPaidDateRange.from, fullyPaidDateRange.to)
          : true,
      );
    const filteredRecords = scopedRecords.filter((record) =>
      matchesStatusFilter(record, activeStatusFilter, activeOverdueRangeFilter),
    );
    const sortedRecords = getSortedRecords(filteredRecords);
    let totalWrittenOff = 0;
    let totalFullyPaid = 0;
    let totalOverdue = 0;
    for (const record of filteredRecords) {
      const statusFlags = getRecordStatusFlags(record);
      if (statusFlags.isWrittenOff) {
        totalWrittenOff += 1;
      }
      if (statusFlags.isFullyPaid) {
        totalFullyPaid += 1;
      }
      if (statusFlags.isOverdue) {
        totalOverdue += 1;
      }
    }

    if (recordCount) {
      recordCount.textContent = String(scopedRecords.length);
    }
    if (filteredCount) {
      filteredCount.textContent = String(sortedRecords.length);
    }
    if (writtenOffCount) {
      writtenOffCount.textContent = String(totalWrittenOff);
    }
    if (fullyPaidCount) {
      fullyPaidCount.textContent = String(totalFullyPaid);
    }
    if (overdueCount) {
      overdueCount.textContent = String(totalOverdue);
    }
    updatePeriodDashboard(records);

    if (!sortedRecords.length) {
      const row = document.createElement("tr");
      const cell = document.createElement("td");
      cell.colSpan = TABLE_FIELDS.length;
      cell.className = "empty-row";
      if (!records.length) {
        cell.textContent = "No data yet. Add your first client using the form above.";
      } else if (hasClosedByFilter) {
        cell.textContent = "No records found for the selected Closed By name.";
      } else if (shouldApplyWrittenOffDateRangeFilter) {
        cell.textContent = "No written off clients found in the selected write off date range.";
      } else if (shouldApplyFullyPaidDateRangeFilter) {
        cell.textContent = "No fully paid clients found in the selected fully paid date range.";
      } else if (hasNewClientDateRangeFilter && hasPaymentsDateRangeFilter) {
        cell.textContent = "No clients found for the selected New Client and Payments date ranges.";
      } else if (hasNewClientDateRangeFilter) {
        cell.textContent = "No clients found with Payment 1 Date in the selected date range.";
      } else if (hasPaymentsDateRangeFilter) {
        cell.textContent = "No clients found with any payment date in the selected Payments range.";
      } else if (activeStatusFilter !== STATUS_FILTER_ALL || query) {
        cell.textContent = "No records found for the selected filters.";
      } else {
        cell.textContent = "No records found for your search.";
      }
      row.append(cell);
      tableBody.replaceChildren(row);
      renderTableFoot([]);
      return;
    }

    const fragment = document.createDocumentFragment();

    for (const record of sortedRecords) {
      const row = document.createElement("tr");
      const statusFlags = getRecordStatusFlags(record);
      const shouldShowWarning = statusFlags.isAfterResult && statusFlags.isFullyPaid;
      const rowToneClass = getRowToneClass(statusFlags);

      if (rowToneClass) {
        row.classList.add(rowToneClass);
      }

      for (const field of TABLE_FIELDS) {
        const cell = document.createElement("td");

        if (field.key === "clientName") {
          const clientRow = document.createElement("div");
          clientRow.className = "client-name-wrapper";

          const clientButton = document.createElement("button");
          clientButton.type = "button";
          clientButton.className = "client-name-button";
          clientButton.textContent = formatFieldValue(field, record[field.key], record);
          clientButton.addEventListener("click", () => {
            openEditModal(record.id);
          });
          clientRow.append(clientButton);

          if (shouldShowWarning) {
            const warningBadge = document.createElement("span");
            warningBadge.className = "warning-badge";
            warningBadge.textContent = "!";
            warningBadge.title = "After Result + Future Payments = 0";
            warningBadge.setAttribute("aria-label", "After Result plus Future Payments equals zero");
            clientRow.append(warningBadge);
          }

          const statusChipList = buildStatusChipList(statusFlags);
          clientRow.append(statusChipList);
          cell.append(clientRow);
        } else {
          cell.textContent = formatFieldValue(field, record[field.key], record);
        }

        row.append(cell);
      }

      fragment.append(row);
    }

    tableBody.replaceChildren(fragment);
    renderTableFoot(sortedRecords);
  } catch (error) {
    updatePeriodDashboard([]);
    renderTableErrorState();
    renderTableFoot([]);
    console.error(error);
  }
}

function renderTableLoadingState() {
  if (!tableBody) {
    return;
  }

  const fragment = document.createDocumentFragment();
  const rowCount = 6;

  for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
    const row = document.createElement("tr");
    row.className = "skeleton-row";

    for (let colIndex = 0; colIndex < TABLE_FIELDS.length; colIndex += 1) {
      const cell = document.createElement("td");
      const line = document.createElement("span");
      line.className = "skeleton-line";
      cell.append(line);
      row.append(cell);
    }

    fragment.append(row);
  }

  tableBody.replaceChildren(fragment);
  renderTableFoot([]);
}

function renderTableErrorState() {
  if (!tableBody) {
    return;
  }

  const row = document.createElement("tr");
  const cell = document.createElement("td");
  cell.colSpan = TABLE_FIELDS.length;
  cell.className = "error-row";
  cell.textContent = "Failed to render table data. Refresh the page and try again.";
  row.append(cell);
  tableBody.replaceChildren(row);
}

function renderTableFoot(displayedRecords) {
  if (!tableFoot) {
    return;
  }

  if (!displayedRecords.length) {
    tableFoot.replaceChildren();
    return;
  }

  const row = document.createElement("tr");

  for (const field of TABLE_FIELDS) {
    const cell = document.createElement("td");

    if (field.key === "clientName") {
      cell.textContent = "Totals";
    } else if (field.key === "closedBy") {
      cell.textContent = `${displayedRecords.length} clients`;
    } else if (SUMMABLE_FIELDS.has(field.key)) {
      const fieldSum = sumFieldValues(displayedRecords, field.key);
      cell.textContent = fieldSum === null ? "-" : MONEY_FORMATTER.format(fieldSum);
    } else {
      cell.textContent = "-";
    }

    row.append(cell);
  }

  tableFoot.replaceChildren(row);
}

function updatePeriodDashboard(recordsToMeasure = []) {
  const ranges = getPeriodDashboardRanges();
  const metricsByPeriod = {
    currentWeek: calculatePeriodMetrics(recordsToMeasure, ranges.currentWeek),
    previousWeek: calculatePeriodMetrics(recordsToMeasure, ranges.previousWeek),
    currentMonth: calculatePeriodMetrics(recordsToMeasure, ranges.currentMonth),
    last30Days: calculatePeriodMetrics(recordsToMeasure, ranges.last30Days),
  };

  const selectedMetrics = metricsByPeriod[activeOverviewPeriod] || metricsByPeriod[OVERVIEW_PERIOD_DEFAULT];
  const selectedPeriodLabel = OVERVIEW_PERIOD_KEYS[activeOverviewPeriod] || OVERVIEW_PERIOD_KEYS[OVERVIEW_PERIOD_DEFAULT];

  if (overviewSalesValue) {
    overviewSalesValue.textContent = formatKpiCurrency(selectedMetrics?.sales ?? 0);
  }

  if (overviewSalesContext) {
    overviewSalesContext.textContent = selectedPeriodLabel;
  }

  if (overviewReceivedValue) {
    overviewReceivedValue.textContent = formatKpiCurrency(selectedMetrics?.received ?? 0);
  }

  if (overviewReceivedContext) {
    overviewReceivedContext.textContent = selectedPeriodLabel;
  }

  const overallDebt = calculateOverallDebt(recordsToMeasure);
  if (overviewDebtValue) {
    overviewDebtValue.textContent = formatKpiCurrency(overallDebt);
  }

  if (overviewSummaryDebt) {
    overviewSummaryDebt.textContent = formatKpiCurrency(overallDebt);
  }

  if (overviewSummarySales) {
    overviewSummarySales.textContent = formatKpiCurrency(metricsByPeriod.currentWeek?.sales ?? 0);
  }

  if (overviewSummaryReceived) {
    overviewSummaryReceived.textContent = formatKpiCurrency(metricsByPeriod.currentWeek?.received ?? 0);
  }
}

function formatKpiCurrency(value) {
  const parsed = Number(value);
  return KPI_MONEY_FORMATTER.format(Number.isFinite(parsed) ? parsed : 0);
}

function getPeriodDashboardRanges() {
  const todayUtcStart = getCurrentUtcDayStart();
  const todayUtcDate = new Date(todayUtcStart);
  const currentWeekStart = getCurrentWeekStartUtc(todayUtcStart);
  const previousWeekStart = currentWeekStart - 7 * DAY_IN_MS;
  const previousWeekEnd = currentWeekStart - DAY_IN_MS;
  const currentMonthStart = Date.UTC(todayUtcDate.getUTCFullYear(), todayUtcDate.getUTCMonth(), 1);
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

function calculatePeriodMetrics(recordsToMeasure, range) {
  let sales = 0;
  let received = 0;

  for (const record of recordsToMeasure) {
    const firstPaymentDate = parseDateValue(record.payment1Date);
    const isSaleInRange = isTimestampWithinInclusiveRange(firstPaymentDate, range.from, range.to);

    if (isSaleInRange) {
      const contractAmount = parseMoneyValue(record.contractTotals);
      if (contractAmount !== null) {
        sales += contractAmount;
      }
    }

    for (const [paymentFieldKey, paymentDateFieldKey] of PAYMENT_PAIRS) {
      const paymentDate = parseDateValue(record[paymentDateFieldKey]);
      if (!isTimestampWithinInclusiveRange(paymentDate, range.from, range.to)) {
        continue;
      }

      const paymentAmount = parseMoneyValue(record[paymentFieldKey]);
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

function calculateOverallDebt(recordsToMeasure) {
  let debt = 0;

  for (const record of recordsToMeasure) {
    const futureAmount = computeFuturePaymentsAmount(record);
    if (futureAmount !== null && futureAmount > ZERO_TOLERANCE) {
      debt += futureAmount;
    }
  }

  return debt;
}

function sumFieldValues(recordsToSum, fieldKey) {
  let hasAtLeastOneValue = false;
  let sum = 0;

  for (const record of recordsToSum) {
    let value;
    if (fieldKey === "futurePayments") {
      value = computeFuturePaymentsAmount(record);
    } else if (fieldKey === "totalPayments") {
      value = computeTotalPaymentsAmount(record, currentPaymentsDateRange);
    } else if (PAYMENT_FIELD_SET.has(fieldKey)) {
      const paymentDateFieldKey = PAYMENT_DATE_FIELD_BY_AMOUNT_FIELD.get(fieldKey);
      if (!shouldShowPaymentInDateRange(record, paymentDateFieldKey, currentPaymentsDateRange)) {
        value = null;
      } else {
        value = parseMoneyValue(record[fieldKey]);
      }
    } else {
      value = parseMoneyValue(record[fieldKey]);
    }

    if (value === null) {
      continue;
    }

    hasAtLeastOneValue = true;
    sum += value;
  }

  return hasAtLeastOneValue ? sum : null;
}

function getSortedRecords(recordsToSort) {
  if (!activeSortKey) {
    return recordsToSort.slice();
  }

  const field = FIELDS.find((item) => item.key === activeSortKey);
  if (!field) {
    return recordsToSort.slice();
  }

  const multiplier = activeSortDirection === SORT_DIRECTION_ASC ? 1 : -1;

  return recordsToSort
    .map((record, index) => ({ record, index }))
    .sort((left, right) => {
      const result = compareRecordsByField(left.record, right.record, field);
      if (result !== 0) {
        return result * multiplier;
      }

      return left.index - right.index;
    })
    .map((item) => item.record);
}

function compareRecordsByField(leftRecord, rightRecord, field) {
  if (field.key === "futurePayments") {
    return compareNullableNumbers(computeFuturePaymentsAmount(leftRecord), computeFuturePaymentsAmount(rightRecord));
  }

  if (field.key === "totalPayments") {
    return compareNullableNumbers(
      computeTotalPaymentsAmount(leftRecord, currentPaymentsDateRange),
      computeTotalPaymentsAmount(rightRecord, currentPaymentsDateRange),
    );
  }

  if (field.type === "checkbox") {
    const leftValue = isAfterResultEnabled(leftRecord[field.key]) ? 1 : 0;
    const rightValue = isAfterResultEnabled(rightRecord[field.key]) ? 1 : 0;
    return leftValue - rightValue;
  }

  if (field.type === "date") {
    return compareNullableNumbers(parseDateValue(leftRecord[field.key]), parseDateValue(rightRecord[field.key]));
  }

  if (MONEY_SORT_FIELDS.has(field.key)) {
    const leftValue = parseMoneyValue(leftRecord[field.key]);
    const rightValue = parseMoneyValue(rightRecord[field.key]);
    const numericResult = compareNullableNumbers(leftValue, rightValue);
    if (numericResult !== 0 || (leftValue !== null && rightValue !== null)) {
      return numericResult;
    }
  }

  return compareTextValues(leftRecord[field.key], rightRecord[field.key]);
}

function compareNullableNumbers(leftValue, rightValue) {
  if (leftValue === null && rightValue === null) {
    return 0;
  }

  if (leftValue === null) {
    return 1;
  }

  if (rightValue === null) {
    return -1;
  }

  return leftValue - rightValue;
}

function compareTextValues(leftValue, rightValue) {
  const leftText = (leftValue || "").toString().trim();
  const rightText = (rightValue || "").toString().trim();

  if (!leftText && !rightText) {
    return 0;
  }

  if (!leftText) {
    return 1;
  }

  if (!rightText) {
    return -1;
  }

  return TEXT_COLLATOR.compare(leftText, rightText);
}

function matchesStatusFilter(record, statusFilter, overdueRangeFilter = OVERDUE_RANGE_FILTER_ALL) {
  const statusFlags = getRecordStatusFlags(record);

  if (statusFilter === STATUS_FILTER_ALL) {
    return true;
  }

  if (statusFilter === STATUS_FILTER_WRITTEN_OFF) {
    return statusFlags.isWrittenOff;
  }

  if (statusFilter === STATUS_FILTER_FULLY_PAID) {
    return statusFlags.isFullyPaid;
  }

  if (statusFilter === STATUS_FILTER_AFTER_RESULT) {
    return statusFlags.isAfterResult;
  }

  if (statusFilter === STATUS_FILTER_OVERDUE) {
    if (!statusFlags.isOverdue) {
      return false;
    }

    if (!overdueRangeFilter) {
      return true;
    }

    return statusFlags.overdueRange === overdueRangeFilter;
  }

  return true;
}

function matchesClosedByFilter(record, selectedClosedBy) {
  if (!selectedClosedBy) {
    return true;
  }

  return normalizeClientName(record.closedBy) === selectedClosedBy;
}

function getDateRangeBounds(fromInput, toInput) {
  const fromValue = parseDateValue(fromInput?.value || "");
  const toValue = parseDateValue(toInput?.value || "");

  if (fromValue !== null && toValue !== null && fromValue > toValue) {
    return {
      from: toValue,
      to: fromValue,
    };
  }

  return {
    from: fromValue,
    to: toValue,
  };
}

function hasDateRangeValues(dateRange) {
  if (!dateRange) {
    return false;
  }

  return dateRange.from !== null || dateRange.to !== null;
}

function isDateWithinRange(dateValue, fromDate, toDate) {
  if (dateValue === null) {
    return false;
  }

  if (fromDate !== null && dateValue < fromDate) {
    return false;
  }

  if (toDate !== null && dateValue > toDate) {
    return false;
  }

  return true;
}

function shouldShowPaymentInDateRange(record, paymentDateFieldKey, dateRange = currentPaymentsDateRange) {
  if (!hasDateRangeValues(dateRange)) {
    return true;
  }

  const paymentDate = parseDateValue(record?.[paymentDateFieldKey]);
  return isDateWithinRange(paymentDate, dateRange.from, dateRange.to);
}

function matchesNewClientDateRange(record, fromDate, toDate) {
  if (fromDate === null && toDate === null) {
    return true;
  }

  const firstPaymentDate = parseDateValue(record.payment1Date);
  return isDateWithinRange(firstPaymentDate, fromDate, toDate);
}

function matchesAnyPaymentDateRange(record, fromDate, toDate) {
  if (fromDate === null && toDate === null) {
    return true;
  }

  for (const paymentDateField of PAYMENT_DATE_FIELDS) {
    const paymentDate = parseDateValue(record[paymentDateField]);
    if (isDateWithinRange(paymentDate, fromDate, toDate)) {
      return true;
    }
  }

  return false;
}

function matchesWrittenOffDateRange(record, fromDate, toDate) {
  if (fromDate === null && toDate === null) {
    return true;
  }

  if (!isRecordWrittenOff(record)) {
    return false;
  }

  const writtenOffDate = parseDateValue(record.dateWhenWrittenOff);
  return isDateWithinRange(writtenOffDate, fromDate, toDate);
}

function matchesFullyPaidDateRange(record, fromDate, toDate) {
  if (fromDate === null && toDate === null) {
    return true;
  }

  const statusFlags = getRecordStatusFlags(record);
  if (!statusFlags.isFullyPaid) {
    return false;
  }

  const fullyPaidDate = parseDateValue(record.dateWhenFullyPaid);
  return isDateWithinRange(fullyPaidDate, fromDate, toDate);
}

function refreshClosedByFilterOptions() {
  if (!closedByFilterSelect) {
    return;
  }

  const previousValue = closedByFilterSelect.value || "";
  const uniqueClosedByMap = new Map();

  for (const record of records) {
    const rawValue = (record.closedBy || "").toString().trim();
    if (!rawValue) {
      continue;
    }

    const normalizedValue = normalizeClientName(rawValue);
    if (!normalizedValue || uniqueClosedByMap.has(normalizedValue)) {
      continue;
    }

    uniqueClosedByMap.set(normalizedValue, rawValue);
  }

  const options = [...uniqueClosedByMap.entries()].sort((a, b) => TEXT_COLLATOR.compare(a[1], b[1]));
  const fragment = document.createDocumentFragment();

  const allOption = document.createElement("option");
  allOption.value = "";
  allOption.textContent = "All";
  fragment.append(allOption);

  for (const [normalizedValue, displayValue] of options) {
    const option = document.createElement("option");
    option.value = displayValue;
    option.dataset.normalized = normalizedValue;
    option.textContent = displayValue;
    fragment.append(option);
  }

  closedByFilterSelect.replaceChildren(fragment);

  if (previousValue) {
    const hasPreviousValue = [...closedByFilterSelect.options].some((option) => option.value === previousValue);
    closedByFilterSelect.value = hasPreviousValue ? previousValue : "";
  }
}

function getRecordStatusFlags(record) {
  const futureAmount = computeFuturePaymentsAmount(record);
  const isAfterResult = isAfterResultEnabled(record.afterResult);
  const isWrittenOff = isRecordWrittenOff(record);
  const isFullyPaid = !isWrittenOff && futureAmount !== null && Math.abs(futureAmount) <= ZERO_TOLERANCE;
  const hasOpenBalance = !isWrittenOff && futureAmount !== null && futureAmount > ZERO_TOLERANCE;
  const lastPaymentTimestamp = getLatestPaymentDateTimestamp(record);
  const overdueDays =
    !isAfterResult && hasOpenBalance && lastPaymentTimestamp !== null
      ? getDaysSinceDate(lastPaymentTimestamp)
      : null;
  const overdueRange = getOverdueRangeLabel(overdueDays);
  const isOverdue = Boolean(overdueRange);

  return {
    isAfterResult,
    isWrittenOff,
    isFullyPaid,
    isOverdue,
    overdueRange,
  };
}

function getRowToneClass(statusFlags) {
  if (statusFlags.isWrittenOff) {
    return "row-tone-written-off";
  }

  if (statusFlags.isOverdue) {
    return "row-tone-overdue";
  }

  if (statusFlags.isAfterResult) {
    return "row-tone-after-result";
  }

  if (statusFlags.isFullyPaid) {
    return "row-tone-fully-paid";
  }

  return "row-tone-active";
}

function buildStatusChipList(statusFlags) {
  const container = document.createElement("div");
  container.className = "status-chip-list";

  const chips = getStatusChipConfig(statusFlags);

  for (const chip of chips) {
    const chipElement = document.createElement("span");
    chipElement.className = `status-chip ${chip.className}`;
    chipElement.textContent = chip.label;
    container.append(chipElement);
  }

  return container;
}

function getStatusChipConfig(statusFlags) {
  if (statusFlags.isWrittenOff) {
    return [{ label: "Written Off", className: "status-chip--written-off" }];
  }

  const chips = [];

  if (statusFlags.isFullyPaid) {
    chips.push({ label: "Fully Paid", className: "status-chip--fully-paid" });
  }

  if (statusFlags.isOverdue) {
    const overdueLabel = statusFlags.overdueRange ? `Overdue ${statusFlags.overdueRange}` : "Overdue";
    chips.push({ label: overdueLabel, className: "status-chip--overdue" });
  }

  if (statusFlags.isAfterResult) {
    chips.push({ label: "After Result", className: "status-chip--after-result" });
  }

  if (!chips.length) {
    chips.push({ label: "Active", className: "status-chip--active" });
  }

  return chips;
}

function getDaysSinceDate(timestamp) {
  if (timestamp === null || !Number.isFinite(timestamp)) {
    return null;
  }

  const targetDate = new Date(timestamp);
  const targetDayStartUtc = Date.UTC(
    targetDate.getUTCFullYear(),
    targetDate.getUTCMonth(),
    targetDate.getUTCDate(),
  );
  const now = new Date();
  const todayDayStartUtc = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  return Math.floor((todayDayStartUtc - targetDayStartUtc) / DAY_IN_MS);
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

function formatFieldValue(field, value, record) {
  const isPaymentsRangeActive = hasDateRangeValues(currentPaymentsDateRange);

  if (field.key === "futurePayments") {
    const computedValue = computeFuturePayments(record);
    return computedValue || "-";
  }

  if (field.key === "totalPayments") {
    return computeTotalPayments(record, currentPaymentsDateRange);
  }

  if (isPaymentsRangeActive && PAYMENT_FIELD_SET.has(field.key)) {
    const paymentDateFieldKey = PAYMENT_DATE_FIELD_BY_AMOUNT_FIELD.get(field.key);
    if (!shouldShowPaymentInDateRange(record, paymentDateFieldKey, currentPaymentsDateRange)) {
      return "";
    }
  }

  if (isPaymentsRangeActive && PAYMENT_DATE_FIELD_SET.has(field.key)) {
    if (!shouldShowPaymentInDateRange(record, field.key, currentPaymentsDateRange)) {
      return "";
    }
  }

  if (field.type === "checkbox") {
    return isAfterResultEnabled(value) ? "Yes" : "-";
  }

  if (!value) {
    return "-";
  }

  if (field.type === "select") {
    const option = (field.options || []).find((item) => item.value === value);
    if (option) {
      return option.label;
    }
  }

  if (field.type === "date") {
    return formatDateValueUs(value) || "-";
  }

  return value;
}

function exportVisibleTableToXls() {
  const exportData = getVisibleTableExportData();
  if (!exportData) {
    return;
  }

  const tableHtml = buildExportTableHtml(exportData);

  const workbookHtml = [
    "<html>",
    "<head><meta charset=\"UTF-8\"></head>",
    "<body>",
    tableHtml,
    "</body>",
    "</html>",
  ].join("");

  const fileName = `credit-booster-client-payments-${formatTimestampForFileName(new Date())}.xls`;
  const blob = new Blob(["\ufeff", workbookHtml], {
    type: "application/vnd.ms-excel;charset=utf-8;",
  });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = fileName;
  document.body.append(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function exportVisibleTableToPdf() {
  const exportData = getVisibleTableExportData();
  if (!exportData) {
    return;
  }

  const tableHtml = buildExportTableHtml(exportData);
  const printWindow = window.open("", "_blank", "width=1280,height=900");
  if (!printWindow) {
    return;
  }

  const exportTimestamp = new Date().toLocaleString("en-US");
  const printHtml = [
    "<!doctype html>",
    "<html lang=\"en\">",
    "<head>",
    "<meta charset=\"UTF-8\" />",
    "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />",
    "<title>Credit Booster Client Payments Export</title>",
    "<style>",
    "body{margin:20px;font-family:Arial,Helvetica,sans-serif;color:#0f172a;}",
    "h1{margin:0 0 6px;font-size:22px;font-weight:700;}",
    ".meta{margin:0 0 14px;font-size:12px;color:#475569;}",
    "table{width:100%;border-collapse:collapse;font-size:11px;}",
    "th,td{border:1px solid #cbd5e1;padding:6px 8px;vertical-align:top;white-space:nowrap;}",
    "th{background:#f1f5f9;text-align:left;font-weight:700;}",
    "tr:last-child td{font-weight:700;background:#f8fafc;}",
    "@page{size:landscape;margin:12mm;}",
    "</style>",
    "</head>",
    "<body>",
    "<h1>Credit Booster Client Payments</h1>",
    `<p class=\"meta\">Exported: ${escapeHtmlForExport(exportTimestamp)}</p>`,
    tableHtml,
    "</body>",
    "</html>",
  ].join("");

  printWindow.document.open();
  printWindow.document.write(printHtml);
  printWindow.document.close();

  const triggerPrint = () => {
    try {
      printWindow.focus();
      printWindow.print();
    } catch {
      // Ignore print failures; user can print manually from the opened tab.
    }
  };

  if (typeof printWindow.addEventListener === "function") {
    printWindow.addEventListener("load", triggerPrint, { once: true });
  }

  // Fallback for browsers where `load` does not fire reliably after document.write.
  setTimeout(triggerPrint, 300);
}

function getVisibleTableExportData() {
  const headerCells = [...(tableHead?.querySelectorAll("th") || [])];
  if (!headerCells.length) {
    return null;
  }

  const headers = headerCells.map((headerCell) => {
    const label = headerCell.querySelector(".th-sort-label")?.textContent || headerCell.textContent || "";
    return normalizeTextForExport(label);
  });

  const bodyRows = [...(tableBody?.querySelectorAll("tr") || [])].filter((row) =>
    isExportableRow(row, headers.length),
  );
  const footerRows = [...(tableFoot?.querySelectorAll("tr") || [])].filter((row) =>
    isExportableRow(row, headers.length),
  );

  return {
    headers,
    rows: bodyRows.map((row) => extractRowCellsForExport(row, headers.length)),
    totalsRows: footerRows.map((row) => extractRowCellsForExport(row, headers.length)),
  };
}

function buildExportTableHtml(exportData) {
  const { headers, rows, totalsRows } = exportData;
  return [
    "<table border=\"1\">",
    `<thead><tr>${headers.map((header) => `<th>${escapeHtmlForExport(header)}</th>`).join("")}</tr></thead>`,
    "<tbody>",
    ...rows.map((cells) => `<tr>${cells.map((cell) => `<td>${escapeHtmlForExport(cell)}</td>`).join("")}</tr>`),
    ...totalsRows.map(
      (cells) => `<tr>${cells.map((cell) => `<td>${escapeHtmlForExport(cell)}</td>`).join("")}</tr>`,
    ),
    "</tbody>",
    "</table>",
  ].join("");
}

function isExportableRow(row, expectedCellCount) {
  if (!row) {
    return false;
  }

  if (row.querySelector("td.empty-row, td.error-row")) {
    return false;
  }

  return row.children.length === expectedCellCount;
}

function extractRowCellsForExport(row, expectedCellCount) {
  const cells = [...row.children].slice(0, expectedCellCount);
  return cells.map((cell) => extractCellTextForExport(cell));
}

function extractCellTextForExport(cell) {
  if (!cell) {
    return "";
  }

  const clientButton = cell.querySelector(".client-name-button");
  if (clientButton) {
    const name = normalizeTextForExport(clientButton.textContent);
    const chips = [...cell.querySelectorAll(".status-chip")]
      .map((chip) => normalizeTextForExport(chip.textContent))
      .filter(Boolean);
    const hasWarning = Boolean(cell.querySelector(".warning-badge"));
    const parts = [name];
    if (chips.length) {
      parts.push(`[${chips.join(", ")}]`);
    }
    if (hasWarning) {
      parts.push("!");
    }
    return parts.join(" ").trim();
  }

  return normalizeTextForExport(cell.textContent);
}

function normalizeTextForExport(value) {
  return (value || "")
    .toString()
    .replace(/\s+/g, " ")
    .trim();
}

function escapeHtmlForExport(value) {
  return (value || "")
    .toString()
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatTimestampForFileName(date) {
  const year = String(date.getFullYear());
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  return `${year}-${month}-${day}_${hours}-${minutes}`;
}

function loadRecords() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) {
      return [];
    }

    const parsed = JSON.parse(raw);
    return normalizeRecordsArray(parsed);
  } catch {
    return [];
  }
}

function normalizeRecordsArray(rawRecords) {
  if (!Array.isArray(rawRecords)) {
    return [];
  }

  return rawRecords
    .map((record, index) => normalizeLoadedRecord(record, index))
    .filter((record) => record.clientName || record.companyName);
}

function normalizeLoadedRecord(rawRecord, index) {
  const source = rawRecord && typeof rawRecord === "object" ? rawRecord : {};
  const normalizedRecord = {
    ...source,
    id: (source.id ?? "").toString().trim() || generateId(),
    createdAt: normalizeCreatedAt(source.createdAt),
  };

  for (const field of FIELDS) {
    if (field.computed) {
      continue;
    }

    if (field.type === "checkbox") {
      const isEnabled =
        field.key === "writtenOff" ? isWrittenOffEnabled(source[field.key]) : isAfterResultEnabled(source[field.key]);
      normalizedRecord[field.key] = isEnabled ? "Yes" : "";
      continue;
    }

    if (field.type === "date") {
      const normalizedDate = normalizeDateForStorage(source[field.key] ?? "");
      normalizedRecord[field.key] = normalizedDate || "";
      continue;
    }

    normalizedRecord[field.key] = (source[field.key] ?? "").toString().trim();
  }

  normalizedRecord.dateWhenFullyPaid = normalizeDateForStorage(source.dateWhenFullyPaid ?? "") || "";
  applyDerivedRecordState(normalizedRecord, source);
  return normalizedRecord;
}

function normalizeCreatedAt(rawValue) {
  const value = (rawValue ?? "").toString().trim();
  if (!value) {
    return new Date().toISOString();
  }

  const timestamp = Date.parse(value);
  if (Number.isNaN(timestamp)) {
    return new Date().toISOString();
  }

  return new Date(timestamp).toISOString();
}

function setApplicationBusyState(isBusy) {
  if (!pageShell) {
    return;
  }

  pageShell.setAttribute("aria-busy", String(Boolean(isBusy)));
}

function removeDeprecatedFieldsFromRecords() {
  const deprecatedKeys = ["badDebtBalances", "afterResultsBalances"];
  let didChange = false;

  for (const record of records) {
    for (const key of deprecatedKeys) {
      if (Object.prototype.hasOwnProperty.call(record, key)) {
        delete record[key];
        didChange = true;
      }
    }
  }

  if (didChange) {
    persistRecords();
  }
}

function persistRecords(options = {}) {
  const { skipRemote = false, immediateRemote = false } = options;
  localStorage.setItem(STORAGE_KEY, JSON.stringify(records));

  if (skipRemote || !isRemoteSyncEnabled || !hasCompletedInitialRemoteSync) {
    return;
  }

  if (immediateRemote) {
    void syncRecordsToRemote();
    return;
  }

  scheduleRemoteSync();
}

function scheduleRemoteSync(delay = REMOTE_SYNC_DEBOUNCE_MS) {
  if (!isRemoteSyncEnabled || !hasCompletedInitialRemoteSync) {
    return;
  }

  if (remoteSyncTimeoutId !== null) {
    clearTimeout(remoteSyncTimeoutId);
  }

  remoteSyncTimeoutId = window.setTimeout(() => {
    remoteSyncTimeoutId = null;
    void syncRecordsToRemote();
  }, delay);
}

async function syncRecordsToRemote(options = {}) {
  const { force = false } = options;
  if (!isRemoteSyncEnabled) {
    return false;
  }

  if (!force && !hasCompletedInitialRemoteSync) {
    return false;
  }

  if (isRemoteSyncInFlight) {
    hasPendingRemoteSync = true;
    return false;
  }

  if (remoteSyncTimeoutId !== null) {
    clearTimeout(remoteSyncTimeoutId);
    remoteSyncTimeoutId = null;
  }

  isRemoteSyncInFlight = true;
  hasPendingRemoteSync = false;

  try {
    const response = await fetch(REMOTE_RECORDS_ENDPOINT, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: JSON.stringify({ records }),
    });

    if (response.status === 401) {
      redirectToLoginPage();
      return false;
    }

    if (response.status === 404) {
      isRemoteSyncEnabled = false;
      return false;
    }

    if (!response.ok) {
      throw new Error(`Server responded with status ${response.status}`);
    }

    return true;
  } catch (error) {
    hasPendingRemoteSync = true;
    scheduleRemoteSync(REMOTE_SYNC_RETRY_MS);
    console.warn("Unable to sync records to remote storage. Changes stay in localStorage.", error);
    return false;
  } finally {
    isRemoteSyncInFlight = false;

    if (hasPendingRemoteSync && isRemoteSyncEnabled && remoteSyncTimeoutId === null) {
      scheduleRemoteSync();
    }
  }
}

async function hydrateRecordsFromRemote() {
  if (!isRemoteSyncEnabled) {
    hasCompletedInitialRemoteSync = true;
    return;
  }

  setApplicationBusyState(true);

  try {
    const response = await fetch(REMOTE_RECORDS_ENDPOINT, {
      headers: {
        Accept: "application/json",
      },
    });

    if (response.status === 401) {
      redirectToLoginPage();
      return;
    }

    if (response.status === 404) {
      isRemoteSyncEnabled = false;
      return;
    }

    if (!response.ok) {
      throw new Error(`Server responded with status ${response.status}`);
    }

    const payload = await response.json();
    const remoteRecords = normalizeRecordsArray(payload?.records);
    if (remoteRecords.length) {
      records = remoteRecords;
      persistRecords({ skipRemote: true });
      removeDeprecatedFieldsFromRecords();
      applyAfterResultFlags();
      recalculateFuturePaymentsForAllRecords();
      refreshClosedByFilterOptions();
      renderTable();
      return;
    }

    if (records.length) {
      hasCompletedInitialRemoteSync = true;
      await syncRecordsToRemote({ force: true });
    }
  } catch (error) {
    console.warn("Remote storage is unavailable. Continuing with localStorage.", error);
  } finally {
    hasCompletedInitialRemoteSync = true;
    setApplicationBusyState(false);
  }
}

function applyCsvImportOnce() {
  const isAlreadyImported = localStorage.getItem(CSV_IMPORT_MARKER_KEY) === "done";
  const hasUsableStoredRecords = hasUsableStoredRecordsInLocalStorage();

  // Recover gracefully if local storage was cleared/corrupted after initial import.
  if (isAlreadyImported && hasUsableStoredRecords) {
    return;
  }

  const seedData = Array.isArray(window.SEED_CLIENT_DATA) ? window.SEED_CLIENT_DATA : [];
  if (!seedData.length) {
    return;
  }

  const importedAt = new Date().toISOString();
  const importedRecords = seedData
    .map((item, index) => {
      const record = {
        id: `csv-${CSV_IMPORT_VERSION}-${index + 1}`,
        createdAt: importedAt,
      };

      for (const field of FIELDS) {
        record[field.key] = (item[field.key] ?? "").toString().trim();
      }

      applyDerivedRecordState(record);
      return record;
    })
    .filter((record) => record.clientName || record.companyName);

  localStorage.setItem(STORAGE_KEY, JSON.stringify(importedRecords));
  localStorage.setItem(CSV_IMPORT_MARKER_KEY, "done");
}

function hasUsableStoredRecordsInLocalStorage() {
  const raw = localStorage.getItem(STORAGE_KEY);
  if (!raw) {
    return false;
  }

  try {
    const parsed = JSON.parse(raw);
    const normalized = normalizeRecordsArray(parsed);
    return normalized.length > 0;
  } catch {
    return false;
  }
}

function applyAfterResultFlags() {
  let didChange = false;

  for (const record of records) {
    const normalizedName = normalizeClientName(record.clientName);
    if (!normalizedName) {
      continue;
    }

    if (AFTER_RESULT_CLIENT_NAMES.has(normalizedName) && !isAfterResultEnabled(record.afterResult)) {
      record.afterResult = "Yes";
      didChange = true;
    }
  }

  if (didChange) {
    persistRecords();
  }
}

function recalculateFuturePaymentsForAllRecords() {
  let didChange = false;

  for (const record of records) {
    const previousFuturePayments = (record.futurePayments || "").toString();
    const previousFullyPaidDate = (record.dateWhenFullyPaid || "").toString();
    applyDerivedRecordState(record);
    const nextFuturePayments = (record.futurePayments || "").toString();
    const nextFullyPaidDate = (record.dateWhenFullyPaid || "").toString();
    if (previousFuturePayments !== nextFuturePayments || previousFullyPaidDate !== nextFullyPaidDate) {
      didChange = true;
    }
  }

  if (didChange) {
    persistRecords();
  }
}

function bindFuturePaymentsPreview() {
  if (!form) {
    return;
  }

  const watchKeys = ["contractTotals", ...PAYMENT_FIELDS];

  for (const key of watchKeys) {
    const control = form.querySelector(`[name=\"${key}\"]`);
    if (control) {
      control.addEventListener("input", updateFuturePaymentsPreview);
    }
  }

  updateFuturePaymentsPreview();
}

function bindWrittenOffAutomation(targetForm) {
  if (!targetForm) {
    return;
  }

  const writtenOffControl = targetForm.querySelector('[name="writtenOff"]');
  const dateWhenWrittenOffControl = targetForm.querySelector('[name="dateWhenWrittenOff"]');
  if (!(writtenOffControl instanceof HTMLInputElement) || !(dateWhenWrittenOffControl instanceof HTMLInputElement)) {
    return;
  }

  if (writtenOffControl.dataset.writtenOffAutomationBound === "1") {
    return;
  }

  writtenOffControl.dataset.writtenOffAutomationBound = "1";
  writtenOffControl.addEventListener("change", () => {
    if (writtenOffControl.checked) {
      dateWhenWrittenOffControl.value = getTodayDateUs();
    } else {
      const clientName = (targetForm.querySelector('[name="clientName"]')?.value || "").toString().trim();
      if (!isWrittenOffByList(clientName)) {
        dateWhenWrittenOffControl.value = "";
      }
    }

    updateFuturePaymentsPreviewForForm(targetForm);
  });
}

function updateFuturePaymentsPreview() {
  if (!form) {
    return;
  }

  updateFuturePaymentsPreviewForForm(form);
}

function bindEditFuturePaymentsPreview() {
  if (!editClientForm) {
    return;
  }

  const watchKeys = ["contractTotals", ...PAYMENT_FIELDS];

  for (const key of watchKeys) {
    const control = editClientForm.querySelector(`[name="${key}"]`);
    if (control) {
      control.addEventListener("input", updateEditFuturePaymentsPreview);
    }
  }

  updateEditFuturePaymentsPreview();
}

function updateEditFuturePaymentsPreview() {
  if (!editClientForm) {
    return;
  }

  updateFuturePaymentsPreviewForForm(editClientForm);
}

function updateFuturePaymentsPreviewForForm(targetForm) {
  const futureControl = targetForm.querySelector('[name="futurePayments"]');
  if (!futureControl) {
    return;
  }

  const previewRecord = {};

  previewRecord.clientName = (targetForm.querySelector('[name="clientName"]')?.value || "").toString().trim();
  previewRecord.writtenOff = targetForm.querySelector('[name="writtenOff"]')?.checked ? "Yes" : "";
  previewRecord.contractTotals = (targetForm.querySelector('[name="contractTotals"]')?.value || "").toString().trim();
  for (const key of PAYMENT_FIELDS) {
    previewRecord[key] = (targetForm.querySelector(`[name="${key}"]`)?.value || "").toString().trim();
  }

  futureControl.value = computeFuturePayments(previewRecord);
}

function computeFuturePayments(record) {
  const future = computeFuturePaymentsAmount(record);

  if (future === null) {
    return "";
  }

  return MONEY_FORMATTER.format(future);
}

function applyDerivedRecordState(record, previousRecord = null) {
  if (!record) {
    return;
  }

  record.futurePayments = computeFuturePayments(record);
  record.dateWhenFullyPaid = computeDateWhenFullyPaid(record, previousRecord);
}

function computeDateWhenFullyPaid(record, previousRecord = null) {
  if (!record) {
    return "";
  }

  if (isRecordWrittenOff(record)) {
    return "";
  }

  const contractTotal = parseMoneyValue(record.contractTotals);
  if (contractTotal === null) {
    return "";
  }

  let runningPaymentsTotal = 0;
  let didCloseContract = false;
  let closureTimestamp = null;

  for (const [paymentFieldKey, paymentDateFieldKey] of PAYMENT_PAIRS) {
    const paymentValue = parseMoneyValue(record[paymentFieldKey]) ?? 0;
    runningPaymentsTotal += paymentValue;

    if (runningPaymentsTotal >= contractTotal - ZERO_TOLERANCE) {
      didCloseContract = true;
      closureTimestamp = parseDateValue(record[paymentDateFieldKey]);
      break;
    }
  }

  if (!didCloseContract) {
    return "";
  }

  if (closureTimestamp !== null) {
    return formatDateTimestampUs(closureTimestamp);
  }

  const previousStoredDate = normalizeDateForStorage(previousRecord?.dateWhenFullyPaid || "");
  if (previousStoredDate) {
    return previousStoredDate;
  }

  const fallbackLatestPaymentDate = getLatestPaymentDateTimestamp(record);
  if (fallbackLatestPaymentDate !== null) {
    return formatDateTimestampUs(fallbackLatestPaymentDate);
  }

  return getTodayDateUs();
}

function getLatestPaymentDateTimestamp(record) {
  let latestTimestamp = null;
  for (const [, paymentDateFieldKey] of PAYMENT_PAIRS) {
    const paymentDate = parseDateValue(record?.[paymentDateFieldKey]);
    if (paymentDate === null) {
      continue;
    }

    if (latestTimestamp === null || paymentDate > latestTimestamp) {
      latestTimestamp = paymentDate;
    }
  }

  return latestTimestamp;
}

function computeTotalPayments(record, dateRange = null) {
  const totalPayments = computeTotalPaymentsAmount(record, dateRange);
  return MONEY_FORMATTER.format(totalPayments ?? 0);
}

function computeTotalPaymentsAmount(record, dateRange = null) {
  if (!record) {
    return 0;
  }

  const effectiveDateRange = dateRange || { from: null, to: null };
  return PAYMENT_FIELDS.reduce((sum, paymentKey) => {
    const paymentDateFieldKey = PAYMENT_DATE_FIELD_BY_AMOUNT_FIELD.get(paymentKey);
    if (!shouldShowPaymentInDateRange(record, paymentDateFieldKey, effectiveDateRange)) {
      return sum;
    }

    const value = parseMoneyValue(record[paymentKey]);
    return sum + (value ?? 0);
  }, 0);
}

function computeFuturePaymentsAmount(record) {
  if (isRecordWrittenOff(record)) {
    return 0;
  }

  const contractTotal = parseMoneyValue(record?.contractTotals);

  if (contractTotal === null) {
    return null;
  }

  const paymentValues = PAYMENT_FIELDS.map((key) => parseMoneyValue(record?.[key]));
  const paidTotal = paymentValues.reduce((sum, value) => sum + (value ?? 0), 0);
  return contractTotal - paidTotal;
}

function parseMoneyValue(rawValue) {
  const value = (rawValue ?? "").toString().trim();
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

function getTodayDateUs() {
  const today = new Date();
  const month = String(today.getMonth() + 1).padStart(2, "0");
  const day = String(today.getDate()).padStart(2, "0");
  const year = String(today.getFullYear());
  return `${month}/${day}/${year}`;
}

function parseDateValue(rawValue) {
  const value = (rawValue ?? "").toString().trim();
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

  const timestamp = Date.parse(value);
  return Number.isNaN(timestamp) ? null : timestamp;
}

function normalizeDateForStorage(rawValue) {
  const value = (rawValue ?? "").toString().trim();
  if (!value) {
    return "";
  }

  const timestamp = parseDateValue(value);
  if (timestamp === null) {
    return null;
  }

  return formatDateTimestampUs(timestamp);
}

function formatDateValueUs(rawValue) {
  const value = (rawValue ?? "").toString().trim();
  if (!value) {
    return "";
  }

  const timestamp = parseDateValue(value);
  if (timestamp === null) {
    return value;
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

function formatDateTimestampIso(timestamp) {
  const date = new Date(timestamp);
  const year = String(date.getUTCFullYear());
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function initializeDateInputFeatures(control) {
  if (!(control instanceof HTMLInputElement) || control.dataset.dateMaskBound === "1") {
    return;
  }

  control.dataset.dateMaskBound = "1";

  const applyMask = () => {
    const digits = control.value.replace(/\D/g, "").slice(0, 8);
    if (!digits) {
      control.value = "";
      return;
    }

    if (digits.length <= 2) {
      control.value = digits;
      return;
    }

    if (digits.length <= 4) {
      control.value = `${digits.slice(0, 2)}/${digits.slice(2)}`;
      return;
    }

    control.value = `${digits.slice(0, 2)}/${digits.slice(2, 4)}/${digits.slice(4)}`;
  };

  control.addEventListener("input", applyMask);
  control.addEventListener("blur", () => {
    if (!control.value) {
      return;
    }

    const normalized = normalizeDateForStorage(control.value);
    if (normalized !== null) {
      control.value = normalized;
    }
  });

  enhanceDateInputWithPicker(control);
}

function enhanceDateInputWithPicker(control) {
  if (!(control instanceof HTMLInputElement) || control.dataset.datePickerBound === "1") {
    return;
  }

  const parent = control.parentElement;
  if (!parent) {
    return;
  }

  if (parent.classList.contains("date-input-shell")) {
    control.dataset.datePickerBound = "1";
    return;
  }

  const shell = document.createElement("div");
  shell.className = "date-input-shell";

  const trigger = document.createElement("button");
  trigger.type = "button";
  trigger.className = "date-picker-trigger";
  trigger.setAttribute("aria-label", "Open calendar");
  trigger.innerHTML =
    '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M7 2a1 1 0 0 1 1 1v1h8V3a1 1 0 1 1 2 0v1h1a3 3 0 0 1 3 3v12a3 3 0 0 1-3 3H5a3 3 0 0 1-3-3V7a3 3 0 0 1 3-3h1V3a1 1 0 0 1 1-1Zm12 8H5v9a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-9ZM6 6a1 1 0 0 0-1 1v1h14V7a1 1 0 0 0-1-1H6Z"/></svg>';

  const nativePicker = document.createElement("input");
  nativePicker.type = "date";
  nativePicker.className = "date-picker-proxy";
  nativePicker.tabIndex = -1;
  nativePicker.setAttribute("aria-hidden", "true");

  parent.replaceChild(shell, control);
  shell.append(control, trigger, nativePicker);
  control.dataset.datePickerBound = "1";

  trigger.addEventListener("click", () => {
    const timestamp = parseDateValue(control.value);
    nativePicker.value = timestamp === null ? "" : formatDateTimestampIso(timestamp);

    if (typeof nativePicker.showPicker === "function") {
      try {
        nativePicker.showPicker();
        return;
      } catch {
        // Browser blocked programmatic showPicker; fallback to focus.
      }
    }

    nativePicker.focus();
  });

  nativePicker.addEventListener("change", () => {
    if (!nativePicker.value) {
      return;
    }

    control.value = formatDateValueUs(nativePicker.value);
    control.dispatchEvent(new Event("input", { bubbles: true }));
  });
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

function normalizeClientName(value) {
  return (value || "")
    .toString()
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

function isAfterResultEnabled(value) {
  const normalized = (value || "").toString().trim().toLowerCase();
  return normalized === "yes" || normalized === "true" || normalized === "1" || normalized === "on";
}

function isWrittenOffEnabled(value) {
  const normalized = (value || "").toString().trim().toLowerCase();
  return normalized === "yes" || normalized === "true" || normalized === "1" || normalized === "on";
}

function isRecordWrittenOff(record) {
  if (!record) {
    return false;
  }

  return isWrittenOffEnabled(record.writtenOff) || isWrittenOffByList(record.clientName);
}

function isWrittenOffByList(clientName) {
  const normalizedName = normalizeClientName(clientName);
  return normalizedName ? WRITTEN_OFF_CLIENT_NAMES.has(normalizedName) : false;
}

function showMessage(message, tone) {
  formMessage.textContent = message;
  formMessage.className = `form-message ${tone}`;
}

function showEditMessage(message, tone) {
  if (!editFormMessage) {
    return;
  }

  editFormMessage.textContent = message;
  editFormMessage.className = `form-message ${tone}`;
}

function clearMessage() {
  formMessage.textContent = "";
  formMessage.className = "form-message";
}

function clearEditMessage() {
  if (!editFormMessage) {
    return;
  }

  editFormMessage.textContent = "";
  editFormMessage.className = "form-message";
}

function focusField(fieldKey) {
  const field = form.querySelector(`[name=\"${fieldKey}\"]`);
  if (field) {
    field.focus();
  }
}

function focusEditField(fieldKey) {
  if (!editClientForm) {
    return;
  }

  const field = editClientForm.querySelector(`[name="${fieldKey}"]`);
  if (field) {
    field.focus();
  }
}

function setClientFormVisibility(isVisible) {
  clientFormSection.hidden = !isVisible;
  toggleClientFormButton.setAttribute("aria-expanded", String(isVisible));
  toggleClientFormButton.textContent = isVisible ? "Hide Form" : "Add New Client";
  tablePanel?.classList.toggle("is-form-open", isVisible);
  requestAnimationFrame(syncCollapsedOverviewTableWrapHeight);

  if (isVisible) {
    collapseCreatePaymentsSection();
  }
}

function collapseCreatePaymentsSection() {
  if (!form) {
    return;
  }

  const extraPaymentsContainer = form.querySelector(".payments-extra");
  const toggleButton = form.querySelector(".payment-more-toggle");
  if (!extraPaymentsContainer || !toggleButton) {
    return;
  }

  extraPaymentsContainer.hidden = true;
  toggleButton.textContent = "Show Payment 2-7";
  toggleButton.setAttribute("aria-expanded", "false");
}

function scrollPageToTop() {
  const shouldReduceMotion = window.matchMedia?.("(prefers-reduced-motion: reduce)")?.matches;
  window.scrollTo({
    top: 0,
    behavior: shouldReduceMotion ? "auto" : "smooth",
  });
}

function initializeAccountMenu() {
  if (!accountMenu || !accountMenuToggleButton || !accountMenuPanel) {
    return;
  }

  accountMenuToggleButton.addEventListener("click", (event) => {
    event.stopPropagation();
    const isOpen = accountMenu.classList.contains("is-open");
    setAccountMenuOpen(!isOpen);
  });

  accountLoginActionButton?.addEventListener("click", () => {
    setAccountMenuOpen(false);
    const nextPath = `${window.location.pathname || "/"}${window.location.search || ""}`;
    window.location.href = `${AUTH_LOGIN_PATH}?next=${encodeURIComponent(nextPath)}`;
  });

  accountLogoutActionButton?.addEventListener("click", () => {
    setAccountMenuOpen(false);
    signOutCurrentUser();
  });

  document.addEventListener("click", (event) => {
    if (!accountMenu.contains(event.target)) {
      setAccountMenuOpen(false);
    }
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      setAccountMenuOpen(false);
    }
  });
}

function initializeAuthGate() {}

function initializeAuthSession() {
  currentAuthUser = "";
  syncAuthUi();
  void hydrateAuthSessionFromServer();
}

function setAccountMenuOpen(isOpen) {
  if (!accountMenu || !accountMenuToggleButton || !accountMenuPanel) {
    return;
  }

  accountMenu.classList.toggle("is-open", isOpen);
  accountMenuPanel.hidden = !isOpen;
  accountMenuToggleButton.setAttribute("aria-expanded", String(isOpen));
  accountMenuToggleButton.setAttribute("aria-label", isOpen ? "Close account menu" : "Open account menu");
}

function signOutCurrentUser() {
  window.location.href = AUTH_LOGOUT_PATH;
}

function syncAuthUi() {
  const isSignedIn = Boolean(currentAuthUser);

  if (accountMenuUser) {
    accountMenuUser.textContent = isSignedIn ? `User: ${currentAuthUser}` : "User: -";
  }

  if (accountLoginActionButton) {
    accountLoginActionButton.hidden = isSignedIn;
  }

  if (accountLogoutActionButton) {
    accountLogoutActionButton.hidden = !isSignedIn;
  }
}

async function hydrateAuthSessionFromServer() {
  try {
    const response = await fetch(AUTH_SESSION_ENDPOINT, {
      headers: {
        Accept: "application/json",
      },
    });

    if (response.status === 401) {
      redirectToLoginPage();
      return;
    }

    if (!response.ok) {
      return;
    }

    const payload = await response.json().catch(() => null);
    const username = (payload?.user?.username || "").toString().trim();
    currentAuthUser = username || "";
    syncAuthUi();
  } catch {
    // Keep optimistic menu state.
  }
}

function redirectToLoginPage() {
  const nextPath = `${window.location.pathname || "/"}${window.location.search || ""}`;
  window.location.href = `${AUTH_LOGIN_PATH}?next=${encodeURIComponent(nextPath)}`;
}

function syncFiltersStickyOffset() {
  if (!pageHeaderElement) {
    return;
  }

  const rootStyles = getComputedStyle(document.documentElement);
  const space2 = Number.parseFloat(rootStyles.getPropertyValue("--space-2")) || 8;
  const headerHeight = Math.ceil(pageHeaderElement.getBoundingClientRect().height);
  const offset = space2 + headerHeight + space2;
  document.documentElement.style.setProperty("--filters-sticky-top", `${offset}px`);
  requestAnimationFrame(syncCollapsedOverviewTableWrapHeight);
}

function initializeFiltersPanelToggle() {
  if (!dashboardGrid || !filtersPanel || !toggleFiltersPanelButton) {
    return;
  }

  const isCollapsed = localStorage.getItem(FILTERS_PANEL_COLLAPSED_KEY) === "1";
  setFiltersPanelCollapsed(isCollapsed, false);

  toggleFiltersPanelButton.addEventListener("click", (event) => {
    event.stopPropagation();
    const nextCollapsedState = !dashboardGrid.classList.contains("is-filters-collapsed");
    setFiltersPanelCollapsed(nextCollapsedState, true);
  });

  filtersPanel.addEventListener("click", (event) => {
    if (!dashboardGrid.classList.contains("is-filters-collapsed")) {
      return;
    }

    if (event.target.closest("#toggle-filters-panel")) {
      return;
    }

    setFiltersPanelCollapsed(false, true);
  });

  filtersPanel.addEventListener("keydown", (event) => {
    if (!dashboardGrid.classList.contains("is-filters-collapsed")) {
      return;
    }

    if (event.key !== "Enter" && event.key !== " ") {
      return;
    }

    event.preventDefault();
    setFiltersPanelCollapsed(false, true);
  });
}

function setFiltersPanelCollapsed(isCollapsed, persistState) {
  if (!dashboardGrid || !toggleFiltersPanelButton || !filtersPanel) {
    return;
  }

  dashboardGrid.classList.toggle("is-filters-collapsed", isCollapsed);
  toggleFiltersPanelButton.setAttribute("aria-expanded", String(!isCollapsed));
  toggleFiltersPanelButton.setAttribute(
    "aria-label",
    isCollapsed ? "Expand filters panel" : "Collapse filters panel",
  );

  const icon = toggleFiltersPanelButton.querySelector(".filters-toggle-icon");
  if (icon) {
    icon.textContent = isCollapsed ? "›" : "‹";
  }

  if (isCollapsed) {
    filtersPanel.setAttribute("role", "button");
    filtersPanel.setAttribute("tabindex", "0");
    filtersPanel.setAttribute("aria-expanded", "false");
    filtersPanel.setAttribute("aria-label", "Expand filters panel");
  } else {
    filtersPanel.removeAttribute("role");
    filtersPanel.removeAttribute("tabindex");
    filtersPanel.removeAttribute("aria-expanded");
    filtersPanel.setAttribute("aria-label", "Filtering options");
  }

  if (persistState) {
    localStorage.setItem(FILTERS_PANEL_COLLAPSED_KEY, isCollapsed ? "1" : "0");
  }

  syncFiltersStickyOffset();
  requestAnimationFrame(syncCollapsedOverviewTableWrapHeight);
}

function initializeOverviewPanelToggle() {
  if (!overviewPanel || !toggleOverviewPanelButton || !overviewContent) {
    return;
  }

  setOverviewPanelCollapsed(false);

  toggleOverviewPanelButton.addEventListener("click", (event) => {
    if (event.target.closest(".overview-segmented")) {
      return;
    }

    const isCollapsed = overviewPanel.classList.contains("is-collapsed");
    setOverviewPanelCollapsed(!isCollapsed);
  });

  toggleOverviewPanelButton.addEventListener("keydown", (event) => {
    if (event.target.closest(".overview-segmented")) {
      return;
    }

    if (event.key !== "Enter" && event.key !== " ") {
      return;
    }

    event.preventDefault();
    const isCollapsed = overviewPanel.classList.contains("is-collapsed");
    setOverviewPanelCollapsed(!isCollapsed);
  });
}

function setOverviewPanelCollapsed(isCollapsed) {
  if (!overviewPanel || !toggleOverviewPanelButton || !overviewContent) {
    return;
  }

  overviewPanel.classList.toggle("is-collapsed", isCollapsed);
  overviewContent.hidden = isCollapsed;
  if (overviewCollapsedSummary) {
    overviewCollapsedSummary.hidden = !isCollapsed;
    overviewCollapsedSummary.setAttribute("aria-hidden", String(!isCollapsed));
  }
  toggleOverviewPanelButton.setAttribute("aria-expanded", String(!isCollapsed));
  toggleOverviewPanelButton.setAttribute(
    "aria-label",
    isCollapsed ? "Expand overview" : "Collapse overview",
  );
  requestAnimationFrame(syncCollapsedOverviewTableWrapHeight);
}

function syncCollapsedOverviewTableWrapHeight() {
  if (!tableWrap) {
    return;
  }

  const isOverviewCollapsed = Boolean(overviewPanel?.classList.contains("is-collapsed"));
  if (!isOverviewCollapsed) {
    tableWrap.style.removeProperty("max-height");
    return;
  }

  const rect = tableWrap.getBoundingClientRect();
  const rootStyles = getComputedStyle(document.documentElement);
  const bottomGap = Number.parseFloat(rootStyles.getPropertyValue("--space-3")) || 12;
  const availableHeight = Math.floor(window.innerHeight - rect.top - bottomGap);
  const minHeight = 280;
  tableWrap.style.maxHeight = `${Math.max(minHeight, availableHeight)}px`;
}

function trapModalFocus(event) {
  if (event.key !== "Tab" || !editModalDialog || editModal?.hidden) {
    return;
  }

  const focusableSelectors = [
    "button:not([disabled])",
    "input:not([disabled])",
    "select:not([disabled])",
    "textarea:not([disabled])",
    "[href]",
    "[tabindex]:not([tabindex='-1'])",
  ];

  const focusableElements = [...editModalDialog.querySelectorAll(focusableSelectors.join(","))].filter(
    (element) => !element.hasAttribute("hidden"),
  );

  if (!focusableElements.length) {
    event.preventDefault();
    return;
  }

  const first = focusableElements[0];
  const last = focusableElements[focusableElements.length - 1];

  if (event.shiftKey && document.activeElement === first) {
    event.preventDefault();
    last.focus();
    return;
  }

  if (!event.shiftKey && document.activeElement === last) {
    event.preventDefault();
    first.focus();
  }
}

function generateId() {
  if (window.crypto?.randomUUID) {
    return crypto.randomUUID();
  }

  return `${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;
}
