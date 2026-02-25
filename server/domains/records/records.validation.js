"use strict";

function createRecordsValidation(dependencies = {}) {
  const {
    recordsPutMaxCount,
    recordsPutMaxRecordKeys,
    recordsPutMaxRecordChars,
    recordsPutMaxTotalChars,
    recordsPutFieldMaxLength = {},
    recordsPutDefaultFieldMaxLength = 4000,
    recordsAllowedFieldsSet = new Set(),
    recordCheckboxFieldSet = new Set(),
    recordsDateValidationFieldSet = new Set(),
    recordsPatchMaxOperations,
    patchOperationUpsert,
    patchOperationDelete,
    sanitizeTextValue,
    toCheckboxValue,
    normalizeDateForStorage,
    paymentFieldNames = ["payment1", "payment2", "payment3", "payment4", "payment5", "payment6", "payment7"],
    paymentDateFieldNames = [
      "payment1Date",
      "payment2Date",
      "payment3Date",
      "payment4Date",
      "payment5Date",
      "payment6Date",
      "payment7Date",
    ],
    contractTotalsField = "contractTotals",
    totalPaymentsField = "totalPayments",
    futurePaymentsField = "futurePayments",
    dateWhenFullyPaidField = "dateWhenFullyPaid",
    dateWhenWrittenOffField = "dateWhenWrittenOff",
    recordsMoneyMaxAbsoluteCents = 10_000_000_000,
  } = dependencies;

  function buildInvalidRecordsPayloadResult(message, code = "invalid_records_payload", httpStatus = 400) {
    return {
      ok: false,
      message,
      code,
      httpStatus,
    };
  }

  function normalizeRecordFieldValue(rawValue, options = {}) {
    const { allowBoolean = false } = options;

    if (rawValue === null || rawValue === undefined) {
      return "";
    }

    if (typeof rawValue === "string") {
      return rawValue.trim();
    }

    if (typeof rawValue === "number" && Number.isFinite(rawValue)) {
      return String(rawValue);
    }

    if (allowBoolean && typeof rawValue === "boolean") {
      return rawValue ? "Yes" : "";
    }

    return null;
  }

  function parseMoneyToCents(rawValue) {
    const value = sanitizeTextValue(rawValue, 120);
    if (!value) {
      return {
        ok: true,
        empty: true,
        cents: null,
      };
    }

    const normalized = value
      .replace(/[−–—]/g, "-")
      .replace(/\(([^)]+)\)/g, "-$1")
      .replace(/[^0-9.-]/g, "");
    if (!normalized || normalized === "-" || normalized === "." || normalized === "-.") {
      return {
        ok: false,
      };
    }

    if (!/^-?\d+(?:\.\d{1,2})?$/.test(normalized)) {
      return {
        ok: false,
      };
    }

    const parsed = Number(normalized);
    if (!Number.isFinite(parsed)) {
      return {
        ok: false,
      };
    }

    const cents = Math.round(parsed * 100);
    if (!Number.isSafeInteger(cents)) {
      return {
        ok: false,
      };
    }

    if (Math.abs(cents) > recordsMoneyMaxAbsoluteCents) {
      return {
        ok: false,
        tooLarge: true,
      };
    }

    return {
      ok: true,
      empty: false,
      cents,
    };
  }

  function formatMoneyFromCents(cents) {
    if (!Number.isFinite(cents)) {
      return "";
    }

    const roundedCents = Math.round(cents);
    const sign = roundedCents < 0 ? "-" : "";
    const absoluteCents = Math.abs(roundedCents);
    const dollars = Math.floor(absoluteCents / 100)
      .toString()
      .replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    const centsPart = String(absoluteCents % 100).padStart(2, "0");
    return `${sign}$${dollars}.${centsPart}`;
  }

  function normalizeRecordPaymentFields(normalizedRecord, recordIndex) {
    const parsedByField = new Map();
    const fieldsToParse = new Set([
      contractTotalsField,
      totalPaymentsField,
      futurePaymentsField,
      ...paymentFieldNames,
    ]);

    for (const fieldName of fieldsToParse) {
      if (!Object.prototype.hasOwnProperty.call(normalizedRecord, fieldName)) {
        continue;
      }

      const parsed = parseMoneyToCents(normalizedRecord[fieldName]);
      if (!parsed.ok) {
        return buildInvalidRecordsPayloadResult(
          parsed.tooLarge
            ? `Record at index ${recordIndex} exceeds allowed amount range in "${fieldName}".`
            : `Record at index ${recordIndex} has invalid amount in "${fieldName}".`,
          parsed.tooLarge ? "records_payload_amount_too_large" : "records_payload_invalid_amount",
        );
      }

      if (
        parsed.cents !== null &&
        parsed.cents < 0 &&
        fieldName !== futurePaymentsField
      ) {
        return buildInvalidRecordsPayloadResult(
          `Record at index ${recordIndex} has negative amount in "${fieldName}".`,
          "records_payload_negative_amount",
        );
      }

      parsedByField.set(fieldName, parsed.cents);
    }

    let derivedTotalPaymentsCents = 0;
    let hasAnyPaymentAmount = false;
    for (const fieldName of paymentFieldNames) {
      const cents = parsedByField.get(fieldName);
      if (cents === null || cents === undefined) {
        continue;
      }
      hasAnyPaymentAmount = true;
      derivedTotalPaymentsCents += cents;
    }

    if (hasAnyPaymentAmount) {
      normalizedRecord[totalPaymentsField] = formatMoneyFromCents(derivedTotalPaymentsCents);
      parsedByField.set(totalPaymentsField, derivedTotalPaymentsCents);
    }

    const contractTotalsCents = parsedByField.get(contractTotalsField);
    if (Number.isFinite(contractTotalsCents)) {
      const effectiveTotalPaymentsCents = Number.isFinite(parsedByField.get(totalPaymentsField))
        ? parsedByField.get(totalPaymentsField)
        : 0;
      const derivedFuturePaymentsCents = contractTotalsCents - effectiveTotalPaymentsCents;
      normalizedRecord[futurePaymentsField] = formatMoneyFromCents(derivedFuturePaymentsCents);

      if (derivedFuturePaymentsCents > 0) {
        normalizedRecord[dateWhenFullyPaidField] = "";
      }
    }

    if (
      normalizedRecord.writtenOff === "Yes" &&
      normalizedRecord.afterResult === "Yes"
    ) {
      normalizedRecord.afterResult = "";
    }

    if (
      normalizedRecord.writtenOff === "Yes" &&
      !sanitizeTextValue(normalizedRecord[dateWhenWrittenOffField], 40)
    ) {
      const today = new Date();
      const month = String(today.getMonth() + 1).padStart(2, "0");
      const day = String(today.getDate()).padStart(2, "0");
      const year = String(today.getFullYear());
      normalizedRecord[dateWhenWrittenOffField] = `${month}/${day}/${year}`;
    }

    if (sanitizeTextValue(normalizedRecord[dateWhenFullyPaidField], 40)) {
      const latestKnownPaymentDate = paymentDateFieldNames
        .map((fieldName) => sanitizeTextValue(normalizedRecord[fieldName], 40))
        .find(Boolean);
      if (!latestKnownPaymentDate && Number.isFinite(contractTotalsCents) && contractTotalsCents > 0) {
        return buildInvalidRecordsPayloadResult(
          `Record at index ${recordIndex} cannot include "${dateWhenFullyPaidField}" without payment dates.`,
          "records_payload_invalid_fully_paid_date",
        );
      }
    }

    return {
      ok: true,
    };
  }

  function isValidCheckboxInput(rawValue) {
    if (rawValue === null || rawValue === undefined || rawValue === "" || rawValue === false || rawValue === 0) {
      return true;
    }

    if (rawValue === true || rawValue === 1) {
      return true;
    }

    if (typeof rawValue === "boolean") {
      return true;
    }

    if (typeof rawValue === "number") {
      return rawValue === 0 || rawValue === 1;
    }

    if (typeof rawValue === "string") {
      const normalized = rawValue.trim().toLowerCase();
      return (
        normalized === "" ||
        normalized === "yes" ||
        normalized === "true" ||
        normalized === "1" ||
        normalized === "no" ||
        normalized === "false" ||
        normalized === "0"
      );
    }

    return false;
  }

  function validateRecordsPayload(value) {
    if (!Array.isArray(value)) {
      return buildInvalidRecordsPayloadResult("Payload must include `records` as an array.");
    }

    if (value.length > recordsPutMaxCount) {
      return buildInvalidRecordsPayloadResult(
        `Records payload is too large. Maximum allowed records: ${recordsPutMaxCount}.`,
        "records_payload_too_many_items",
        413,
      );
    }

    let totalChars = 0;
    const normalizedRecords = [];
    const seenRecordIds = new Set();

    for (let recordIndex = 0; recordIndex < value.length; recordIndex += 1) {
      const record = value[recordIndex];
      if (!record || typeof record !== "object" || Array.isArray(record)) {
        return buildInvalidRecordsPayloadResult(
          `Record at index ${recordIndex} must be an object.`,
          "records_payload_invalid_record",
        );
      }

      const entries = Object.entries(record);
      if (entries.length > recordsPutMaxRecordKeys) {
        return buildInvalidRecordsPayloadResult(
          `Record at index ${recordIndex} contains too many fields.`,
          "records_payload_record_too_wide",
          413,
        );
      }

      const normalizedRecord = {};
      let recordChars = 0;

      for (const [fieldName, rawFieldValue] of entries) {
        if (!recordsAllowedFieldsSet.has(fieldName)) {
          return buildInvalidRecordsPayloadResult(
            `Record at index ${recordIndex} contains unsupported field "${fieldName}".`,
            "records_payload_unknown_field",
          );
        }

        let normalizedValue = "";
        if (recordCheckboxFieldSet.has(fieldName)) {
          if (!isValidCheckboxInput(rawFieldValue)) {
            return buildInvalidRecordsPayloadResult(
              `Record at index ${recordIndex} has invalid checkbox value for "${fieldName}".`,
              "records_payload_invalid_checkbox",
            );
          }
          normalizedValue = toCheckboxValue(rawFieldValue);
        } else {
          normalizedValue = normalizeRecordFieldValue(rawFieldValue);
          if (normalizedValue === null) {
            return buildInvalidRecordsPayloadResult(
              `Record at index ${recordIndex} has invalid type for "${fieldName}".`,
              "records_payload_invalid_field_type",
            );
          }
        }

        const fieldLimit =
          Object.prototype.hasOwnProperty.call(recordsPutFieldMaxLength, fieldName)
            ? recordsPutFieldMaxLength[fieldName]
            : recordsPutDefaultFieldMaxLength;
        if (normalizedValue.length > fieldLimit) {
          return buildInvalidRecordsPayloadResult(
            `Record at index ${recordIndex} exceeds allowed length for "${fieldName}".`,
            "records_payload_field_too_long",
            413,
          );
        }

        if (fieldName === "createdAt" && normalizedValue) {
          const createdAtTimestamp = Date.parse(normalizedValue);
          if (!Number.isFinite(createdAtTimestamp)) {
            return buildInvalidRecordsPayloadResult(
              `Record at index ${recordIndex} has invalid createdAt value.`,
              "records_payload_invalid_created_at",
            );
          }
          normalizedValue = new Date(createdAtTimestamp).toISOString();
        }

        if (recordsDateValidationFieldSet.has(fieldName) && normalizedValue) {
          const normalizedDate = normalizeDateForStorage(normalizedValue);
          if (normalizedDate === null) {
            return buildInvalidRecordsPayloadResult(
              `Record at index ${recordIndex} has invalid date in "${fieldName}". Use MM/DD/YYYY.`,
              "records_payload_invalid_date",
            );
          }
          normalizedValue = normalizedDate;
        }

        recordChars += normalizedValue.length + fieldName.length;
        if (recordChars > recordsPutMaxRecordChars) {
          return buildInvalidRecordsPayloadResult(
            `Record at index ${recordIndex} is too large.`,
            "records_payload_record_too_large",
            413,
          );
        }

        totalChars += normalizedValue.length + fieldName.length;
        if (totalChars > recordsPutMaxTotalChars) {
          return buildInvalidRecordsPayloadResult(
            "Records payload is too large.",
            "records_payload_too_large",
            413,
          );
        }

        normalizedRecord[fieldName] = normalizedValue;
      }

      const normalizedRecordId = sanitizeTextValue(normalizedRecord.id, 180);
      if (normalizedRecordId) {
        if (seenRecordIds.has(normalizedRecordId)) {
          return buildInvalidRecordsPayloadResult(
            `Record at index ${recordIndex} has duplicate id "${normalizedRecordId}".`,
            "records_payload_duplicate_id",
          );
        }
        seenRecordIds.add(normalizedRecordId);
        normalizedRecord.id = normalizedRecordId;
      }

      const paymentNormalizationResult = normalizeRecordPaymentFields(normalizedRecord, recordIndex);
      if (!paymentNormalizationResult.ok) {
        return paymentNormalizationResult;
      }

      normalizedRecords.push(normalizedRecord);
    }

    return {
      ok: true,
      records: normalizedRecords,
    };
  }

  function buildInvalidRecordsPatchPayloadResult(message, code = "invalid_records_patch_payload", httpStatus = 400) {
    return {
      ok: false,
      message,
      code,
      httpStatus,
    };
  }

  function normalizeRecordsPatchOperationType(rawValue) {
    const normalized = sanitizeTextValue(rawValue, 40).toLowerCase();
    if (normalized === patchOperationUpsert || normalized === patchOperationDelete) {
      return normalized;
    }
    return "";
  }

  function validateRecordsPatchPayload(payload) {
    if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
      return buildInvalidRecordsPatchPayloadResult("Payload must be an object.");
    }

    const operations = payload.operations;
    if (!Array.isArray(operations)) {
      return buildInvalidRecordsPatchPayloadResult("Payload must include `operations` as an array.");
    }

    if (operations.length > recordsPatchMaxOperations) {
      return buildInvalidRecordsPatchPayloadResult(
        `Patch payload is too large. Maximum allowed operations: ${recordsPatchMaxOperations}.`,
        "records_patch_too_many_operations",
        413,
      );
    }

    const normalizedOperations = [];
    const seenOperationIds = new Set();

    for (let operationIndex = 0; operationIndex < operations.length; operationIndex += 1) {
      const operation = operations[operationIndex];
      if (!operation || typeof operation !== "object" || Array.isArray(operation)) {
        return buildInvalidRecordsPatchPayloadResult(
          `Operation at index ${operationIndex} must be an object.`,
          "records_patch_invalid_operation",
        );
      }

      const operationType = normalizeRecordsPatchOperationType(operation.type || operation.op);
      if (!operationType) {
        return buildInvalidRecordsPatchPayloadResult(
          `Operation at index ${operationIndex} has invalid type. Allowed values: upsert, delete.`,
          "records_patch_invalid_operation_type",
        );
      }

      const operationId = sanitizeTextValue(operation.id, 180);
      if (!operationId) {
        return buildInvalidRecordsPatchPayloadResult(
          `Operation at index ${operationIndex} must include \`id\`.`,
          "records_patch_missing_id",
        );
      }

      if (seenOperationIds.has(operationId)) {
        return buildInvalidRecordsPatchPayloadResult(
          `Operation at index ${operationIndex} repeats id "${operationId}" in the same request.`,
          "records_patch_duplicate_operation_id",
        );
      }
      seenOperationIds.add(operationId);

      if (operationType === patchOperationDelete) {
        normalizedOperations.push({
          type: patchOperationDelete,
          id: operationId,
        });
        continue;
      }

      const rawRecord = operation.record;
      if (!rawRecord || typeof rawRecord !== "object" || Array.isArray(rawRecord)) {
        return buildInvalidRecordsPatchPayloadResult(
          `Operation at index ${operationIndex} must include \`record\` object for upsert.`,
          "records_patch_invalid_record",
        );
      }

      const recordValidation = validateRecordsPayload([rawRecord]);
      if (!recordValidation.ok) {
        return buildInvalidRecordsPatchPayloadResult(
          `Operation at index ${operationIndex}: ${recordValidation.message}`,
          recordValidation.code || "records_patch_invalid_record",
          recordValidation.httpStatus || 400,
        );
      }

      const normalizedRecord = recordValidation.records[0] || {};
      const recordId = sanitizeTextValue(normalizedRecord.id, 180);
      if (recordId && recordId !== operationId) {
        return buildInvalidRecordsPatchPayloadResult(
          `Operation at index ${operationIndex} has mismatched record id.`,
          "records_patch_id_mismatch",
        );
      }

      normalizedRecord.id = operationId;
      normalizedOperations.push({
        type: patchOperationUpsert,
        id: operationId,
        record: normalizedRecord,
      });
    }

    return {
      ok: true,
      operations: normalizedOperations,
    };
  }

  function normalizeExpectedUpdatedAtFromRequest(body = {}) {
    const hasExpectedUpdatedAt = Object.prototype.hasOwnProperty.call(body || {}, "expectedUpdatedAt");
    if (!hasExpectedUpdatedAt) {
      return {
        ok: false,
        status: 428,
        error: "Payload must include `expectedUpdatedAt` from GET /api/records.",
        code: "records_precondition_required",
      };
    }

    const rawExpectedUpdatedAt = body?.expectedUpdatedAt;
    if (!(rawExpectedUpdatedAt === null || rawExpectedUpdatedAt === "" || typeof rawExpectedUpdatedAt === "string")) {
      return {
        ok: false,
        status: 400,
        error: "`expectedUpdatedAt` must be an ISO datetime string or null.",
        code: "invalid_expected_updated_at",
      };
    }

    if (typeof rawExpectedUpdatedAt === "string" && rawExpectedUpdatedAt.trim()) {
      const normalizedExpectedUpdatedAt = sanitizeTextValue(rawExpectedUpdatedAt, 120);
      const expectedTimestamp = Date.parse(normalizedExpectedUpdatedAt);
      if (!normalizedExpectedUpdatedAt || Number.isNaN(expectedTimestamp)) {
        return {
          ok: false,
          status: 400,
          error: "`expectedUpdatedAt` must be an ISO datetime string or null.",
          code: "invalid_expected_updated_at",
        };
      }

      return {
        ok: true,
        expectedUpdatedAt: new Date(expectedTimestamp).toISOString(),
      };
    }

    return {
      ok: true,
      expectedUpdatedAt: null,
    };
  }

  return {
    validateRecordsPayload,
    validateRecordsPatchPayload,
    normalizeExpectedUpdatedAtFromRequest,
  };
}

module.exports = {
  createRecordsValidation,
};
