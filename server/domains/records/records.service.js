"use strict";

function createRecordsService(dependencies = {}) {
  const {
    simulateSlowRecords,
    simulateSlowRecordsDelayMs,
    delayMs,
    hasDatabase,
    getStoredRecordsForApiRecordsRoute,
    readV2Enabled,
    scheduleDualReadCompareForLegacyRecords,
    filterClientRecordsForWebAuthUser,
    sanitizeTextValue,
    saveStoredRecords,
    saveStoredRecordsPatch,
    logWarn,
  } = dependencies;

  async function getRecordsForApi({ webAuthProfile, webAuthUser }) {
    if (simulateSlowRecords) {
      await delayMs(simulateSlowRecordsDelayMs);
      return {
        status: 200,
        body: {
          records: [],
          updatedAt: new Date().toISOString(),
        },
      };
    }

    if (!hasDatabase()) {
      return {
        status: 503,
        body: {
          error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
        },
      };
    }

    const state = await getStoredRecordsForApiRecordsRoute();
    if (!readV2Enabled && state.source === "legacy") {
      scheduleDualReadCompareForLegacyRecords(state.records, {
        source: "GET /api/records",
        requestedBy: webAuthUser,
      });
    }

    if (readV2Enabled && state.fallbackFromV2) {
      logWarn(
        `[records] READ_V2 served legacy fallback for user=${sanitizeTextValue(webAuthUser, 160) || "unknown"}`,
      );
    }

    const filteredRecords = filterClientRecordsForWebAuthUser(state.records, webAuthProfile);
    return {
      status: 200,
      body: {
        records: filteredRecords,
        updatedAt: state.updatedAt,
      },
    };
  }

  async function saveRecordsForApi({ records, expectedUpdatedAt }) {
    if (simulateSlowRecords) {
      await delayMs(simulateSlowRecordsDelayMs);
      return {
        status: 200,
        body: {
          ok: true,
          updatedAt: new Date().toISOString(),
        },
      };
    }

    if (!hasDatabase()) {
      return {
        status: 503,
        body: {
          error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
        },
      };
    }

    const updatedAt = await saveStoredRecords(records, {
      expectedUpdatedAt,
    });

    return {
      status: 200,
      body: {
        ok: true,
        updatedAt,
      },
    };
  }

  async function patchRecordsForApi({ operations, expectedUpdatedAt }) {
    if (simulateSlowRecords) {
      await delayMs(simulateSlowRecordsDelayMs);
      return {
        status: 200,
        body: {
          ok: true,
          updatedAt: new Date().toISOString(),
          appliedOperations: Array.isArray(operations) ? operations.length : 0,
        },
      };
    }

    if (!hasDatabase()) {
      return {
        status: 503,
        body: {
          error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
        },
      };
    }

    const result = await saveStoredRecordsPatch(operations, {
      expectedUpdatedAt,
    });

    return {
      status: 200,
      body: {
        ok: true,
        updatedAt: result.updatedAt,
        appliedOperations: Array.isArray(operations) ? operations.length : 0,
      },
    };
  }

  return {
    getRecordsForApi,
    saveRecordsForApi,
    patchRecordsForApi,
  };
}

module.exports = {
  createRecordsService,
};
