"use strict";

function createAssistantRepo(dependencies = {}) {
  const {
    db,
    ensureDatabaseReady,
    tables,
    constants,
    helpers,
    metrics,
    performanceObservability,
    logger,
  } = dependencies;

  const pool = db?.pool || null;
  const query = typeof db?.query === "function" ? db.query : null;
  const runInTransaction = typeof db?.tx === "function" ? db.tx : null;
  const createClientQuery = typeof db?.createClientQuery === "function" ? db.createClientQuery : null;

  const ASSISTANT_SESSION_SCOPE_TABLE = tables?.assistantSessionScopeTable;
  const ASSISTANT_REVIEW_TABLE = tables?.assistantReviewTable;

  const {
    ASSISTANT_SESSION_SCOPE_MAX_SESSIONS_PER_USER,
    ASSISTANT_SESSION_SCOPE_MAX_ENTRIES,
    ASSISTANT_SESSION_SCOPE_MAX_TOTAL_BYTES,
    ASSISTANT_SESSION_SCOPE_TTL_MS,
    ASSISTANT_REVIEW_RETENTION_SWEEP_ENABLED,
    ASSISTANT_REVIEW_RETENTION_SWEEP_BATCH_LIMIT,
    ASSISTANT_REVIEW_RETENTION_DAYS,
    ASSISTANT_MAX_MESSAGE_LENGTH,
    ASSISTANT_OWNER_LEARNING_CANDIDATE_LIMIT,
    ASSISTANT_OWNER_LEARNING_MIN_CONTEXT_SCORE,
    ASSISTANT_OWNER_LEARNING_MAX_PROMPT_EXAMPLES,
    ASSISTANT_OWNER_LEARNING_DIRECT_MATCH_MIN_SCORE,
    ASSISTANT_REVIEW_MAX_TEXT_LENGTH,
    ASSISTANT_REVIEW_MAX_COMMENT_LENGTH,
    ASSISTANT_REVIEW_PII_MODE,
  } = constants || {};

  const {
    parseAssistantSessionScopeStoreCount,
    parseAssistantSessionScopeStoreBytes,
    resolveAssistantSessionScopeIdentity,
    normalizeAssistantScopePayload,
    normalizeAssistantClientMessageSeq,
    buildAssistantScopeStoragePayload,
    buildAssistantScopeClearTombstonePayload,
    sanitizeTextValue,
    normalizeAssistantComparableText,
    tokenizeAssistantText,
    scoreAssistantOwnerLearningCandidate,
    normalizeAssistantReplyForDisplay,
    normalizeAssistantReviewClientMentions,
    sanitizeAssistantReviewTextForStorage,
    normalizeAssistantChatMode,
    normalizeAssistantReviewLimit,
    normalizeAssistantReviewOffset,
    mapAssistantReviewRow,
    createHttpError,
    resolveOptionalBoolean,
  } = helpers || {};

  const {
    recordAssistantSessionScopeMetricHit,
    recordAssistantSessionScopeMetricMiss,
    recordAssistantSessionScopeMetricError,
    recordAssistantSessionScopeMetricEvictions,
    recordAssistantSessionScopeMetricSize,
    recordAssistantSessionScopeMetricBytes,
  } = metrics || {};

  const log = logger || console;

  if (typeof ensureDatabaseReady !== "function") {
    throw new Error("createAssistantRepo requires ensureDatabaseReady()");
  }

  function resolveQueryableQuery(queryable = pool) {
    if (typeof queryable === "function") {
      return queryable;
    }
    if (queryable && typeof queryable.query === "function") {
      if (createClientQuery) {
        return createClientQuery(queryable);
      }
      return queryable.query.bind(queryable);
    }
    if (query) {
      return query;
    }
    return null;
  }

  async function getAssistantSessionScopeStoreStats(queryable = pool) {
    const executeQuery = resolveQueryableQuery(queryable);
    if (!executeQuery) {
      return {
        size: 0,
        totalBytes: 0,
      };
    }

    const result = await executeQuery(`
      SELECT COUNT(*)::BIGINT AS count, COALESCE(SUM(scope_bytes), 0)::BIGINT AS total_bytes
      FROM ${ASSISTANT_SESSION_SCOPE_TABLE}
    `);
    return {
      size: parseAssistantSessionScopeStoreCount(result.rows[0]?.count),
      totalBytes: parseAssistantSessionScopeStoreBytes(result.rows[0]?.total_bytes),
    };
  }

  async function pruneAssistantSessionScopeStoreForUser(identity, queryable = pool) {
    const executeQuery = resolveQueryableQuery(queryable);
    if (!executeQuery) {
      return {
        evictions: 0,
      };
    }

    const perUserOverflowResult = await executeQuery(
      `
        WITH ranked AS (
          SELECT
            cache_key,
            ROW_NUMBER() OVER (ORDER BY updated_at DESC, cache_key DESC) AS rn
          FROM ${ASSISTANT_SESSION_SCOPE_TABLE}
          WHERE tenant_key = $1
            AND user_key = $2
        ),
        overflow AS (
          SELECT cache_key
          FROM ranked
          WHERE rn > $3
        )
        DELETE FROM ${ASSISTANT_SESSION_SCOPE_TABLE}
        WHERE cache_key IN (SELECT cache_key FROM overflow)
      `,
      [identity.tenantKey, identity.userKey, ASSISTANT_SESSION_SCOPE_MAX_SESSIONS_PER_USER],
    );

    return {
      evictions: parseAssistantSessionScopeStoreCount(perUserOverflowResult?.rowCount),
    };
  }

  async function pruneAssistantSessionScopeStore(queryable = pool) {
    const executeQuery = resolveQueryableQuery(queryable);
    if (!executeQuery) {
      return {
        evictions: 0,
        size: 0,
        totalBytes: 0,
      };
    }

    let evictions = 0;

    const expiredResult = await executeQuery(`DELETE FROM ${ASSISTANT_SESSION_SCOPE_TABLE} WHERE expires_at <= NOW()`);
    evictions += parseAssistantSessionScopeStoreCount(expiredResult?.rowCount);

    let stats = await getAssistantSessionScopeStoreStats(executeQuery);
    if (stats.size > ASSISTANT_SESSION_SCOPE_MAX_ENTRIES || stats.totalBytes > ASSISTANT_SESSION_SCOPE_MAX_TOTAL_BYTES) {
      const overflowResult = await executeQuery(
        `
          WITH ranked AS (
            SELECT
              cache_key,
              ROW_NUMBER() OVER (ORDER BY updated_at DESC, cache_key DESC) AS rn,
              SUM(scope_bytes) OVER (ORDER BY updated_at DESC, cache_key DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS bytes_kept
            FROM ${ASSISTANT_SESSION_SCOPE_TABLE}
          ),
          overflow AS (
            SELECT cache_key
            FROM ranked
            WHERE rn > $1
               OR bytes_kept > $2
          )
          DELETE FROM ${ASSISTANT_SESSION_SCOPE_TABLE}
          WHERE cache_key IN (SELECT cache_key FROM overflow)
        `,
        [ASSISTANT_SESSION_SCOPE_MAX_ENTRIES, ASSISTANT_SESSION_SCOPE_MAX_TOTAL_BYTES],
      );
      const overflowDeleted = parseAssistantSessionScopeStoreCount(overflowResult?.rowCount);
      evictions += overflowDeleted;
      if (overflowDeleted > 0) {
        stats = await getAssistantSessionScopeStoreStats(executeQuery);
      }
    }

    return {
      evictions,
      size: stats.size,
      totalBytes: stats.totalBytes,
    };
  }

  async function getAssistantSessionScope(rawTenantKey, rawUsername, rawSessionId) {
    if (!pool || !query) {
      recordAssistantSessionScopeMetricMiss?.(performanceObservability);
      return null;
    }

    const identity = resolveAssistantSessionScopeIdentity(rawTenantKey, rawUsername, rawSessionId);
    try {
      await ensureDatabaseReady();
      const result = await query(
        `
          SELECT scope
          FROM ${ASSISTANT_SESSION_SCOPE_TABLE}
          WHERE cache_key = $1
            AND expires_at > NOW()
          LIMIT 1
        `,
        [identity.cacheKey],
      );

      if (!result.rows.length) {
        const expiredDeleteResult = await query(
          `
            DELETE FROM ${ASSISTANT_SESSION_SCOPE_TABLE}
            WHERE cache_key = $1
              AND expires_at <= NOW()
          `,
          [identity.cacheKey],
        );
        const expiredDeleted = parseAssistantSessionScopeStoreCount(expiredDeleteResult?.rowCount);
        if (expiredDeleted > 0) {
          recordAssistantSessionScopeMetricEvictions?.(performanceObservability, expiredDeleted);
        }
        recordAssistantSessionScopeMetricMiss?.(performanceObservability);
        return null;
      }

      const normalizedScope = normalizeAssistantScopePayload(result.rows[0]?.scope);
      if (!normalizedScope) {
        const invalidDeleteResult = await query(
          `
            DELETE FROM ${ASSISTANT_SESSION_SCOPE_TABLE}
            WHERE cache_key = $1
          `,
          [identity.cacheKey],
        );
        const invalidDeleted = parseAssistantSessionScopeStoreCount(invalidDeleteResult?.rowCount);
        if (invalidDeleted > 0) {
          recordAssistantSessionScopeMetricEvictions?.(performanceObservability, invalidDeleted);
        }
        recordAssistantSessionScopeMetricMiss?.(performanceObservability);
        return null;
      }

      if (normalizedScope.scopeEstablished !== true) {
        recordAssistantSessionScopeMetricMiss?.(performanceObservability);
        return null;
      }

      recordAssistantSessionScopeMetricHit?.(performanceObservability);
      return normalizedScope;
    } catch (error) {
      recordAssistantSessionScopeMetricError?.(performanceObservability, error);
      log.warn?.(`[assistant][scope-store] get failed: ${sanitizeTextValue(error?.message, 320) || "unknown error"}`);
      return null;
    }
  }

  async function upsertAssistantSessionScope(rawTenantKey, rawUsername, rawSessionId, rawScope, options = {}) {
    const scopeStoragePayload = buildAssistantScopeStoragePayload(rawScope);
    if (!scopeStoragePayload || !pool || !runInTransaction) {
      return {
        applied: false,
        stale: false,
        clientMessageSeq: normalizeAssistantClientMessageSeq(options?.clientMessageSeq),
      };
    }

    const { scope, scopeJson, scopeBytes, truncated } = scopeStoragePayload;
    const identity = resolveAssistantSessionScopeIdentity(rawTenantKey, rawUsername, rawSessionId);
    const clientMessageSeq = normalizeAssistantClientMessageSeq(options?.clientMessageSeq);
    const expiresAt = new Date(Date.now() + ASSISTANT_SESSION_SCOPE_TTL_MS).toISOString();

    try {
      await ensureDatabaseReady();
      const result = await runInTransaction(async ({ query: txQuery }) => {
        const upsertResult =
          clientMessageSeq > 0
            ? await txQuery(
                `
                  INSERT INTO ${ASSISTANT_SESSION_SCOPE_TABLE} (
                    cache_key,
                    tenant_key,
                    user_key,
                    session_key,
                    scope,
                    last_seq,
                    scope_bytes,
                    updated_at,
                    expires_at
                  )
                  VALUES ($1, $2, $3, $4, $5::jsonb, $6::bigint, $7, NOW(), $8::timestamptz)
                  ON CONFLICT (cache_key) DO UPDATE
                  SET tenant_key = EXCLUDED.tenant_key,
                      user_key = EXCLUDED.user_key,
                      session_key = EXCLUDED.session_key,
                      scope = EXCLUDED.scope,
                      last_seq = EXCLUDED.last_seq,
                      scope_bytes = EXCLUDED.scope_bytes,
                      updated_at = NOW(),
                      expires_at = EXCLUDED.expires_at
                  WHERE ${ASSISTANT_SESSION_SCOPE_TABLE}.last_seq < EXCLUDED.last_seq
                `,
                [
                  identity.cacheKey,
                  identity.tenantKey,
                  identity.userKey,
                  identity.sessionKey,
                  scopeJson,
                  clientMessageSeq,
                  scopeBytes,
                  expiresAt,
                ],
              )
            : await txQuery(
                `
                  INSERT INTO ${ASSISTANT_SESSION_SCOPE_TABLE} (
                    cache_key,
                    tenant_key,
                    user_key,
                    session_key,
                    scope,
                    last_seq,
                    scope_bytes,
                    updated_at,
                    expires_at
                  )
                  VALUES ($1, $2, $3, $4, $5::jsonb, 0, $6, NOW(), $7::timestamptz)
                  ON CONFLICT (cache_key) DO UPDATE
                  SET tenant_key = EXCLUDED.tenant_key,
                      user_key = EXCLUDED.user_key,
                      session_key = EXCLUDED.session_key,
                      scope = EXCLUDED.scope,
                      last_seq = EXCLUDED.last_seq,
                      scope_bytes = EXCLUDED.scope_bytes,
                      updated_at = NOW(),
                      expires_at = EXCLUDED.expires_at
                  WHERE ${ASSISTANT_SESSION_SCOPE_TABLE}.last_seq <= 0
                `,
                [identity.cacheKey, identity.tenantKey, identity.userKey, identity.sessionKey, scopeJson, scopeBytes, expiresAt],
              );

        const applied = parseAssistantSessionScopeStoreCount(upsertResult?.rowCount) > 0;
        if (!applied) {
          return {
            applied: false,
            stale: clientMessageSeq > 0,
            clientMessageSeq,
          };
        }

        const perUserMaintenance = await pruneAssistantSessionScopeStoreForUser(identity, txQuery);
        const maintenance = await pruneAssistantSessionScopeStore(txQuery);
        const totalEvictions = perUserMaintenance.evictions + maintenance.evictions;
        return {
          applied: true,
          stale: false,
          clientMessageSeq,
          maintenance,
          totalEvictions,
        };
      });

      if (!result.applied) {
        return result;
      }

      recordAssistantSessionScopeMetricSize?.(performanceObservability, result.maintenance.size);
      recordAssistantSessionScopeMetricBytes?.(performanceObservability, result.maintenance.totalBytes);
      if (result.totalEvictions > 0) {
        recordAssistantSessionScopeMetricEvictions?.(performanceObservability, result.totalEvictions);
      }
      if (truncated) {
        log.warn?.(
          `[assistant][scope-store] scope payload truncated to ${scope.clientComparables.length} clients for user=${identity.userKey} session=${identity.sessionKey}`,
        );
      }
      return {
        applied: true,
        stale: false,
        clientMessageSeq,
      };
    } catch (error) {
      recordAssistantSessionScopeMetricError?.(performanceObservability, error);
      log.warn?.(`[assistant][scope-store] upsert failed: ${sanitizeTextValue(error?.message, 320) || "unknown error"}`);
      return {
        applied: false,
        stale: false,
        clientMessageSeq,
      };
    }
  }

  async function clearAssistantSessionScope(rawTenantKey, rawUsername, rawSessionId, options = {}) {
    const clientMessageSeq = normalizeAssistantClientMessageSeq(options?.clientMessageSeq);
    if (!pool || !query) {
      recordAssistantSessionScopeMetricSize?.(performanceObservability, 0);
      recordAssistantSessionScopeMetricBytes?.(performanceObservability, 0);
      return {
        applied: false,
        stale: false,
        clientMessageSeq,
      };
    }

    const identity = resolveAssistantSessionScopeIdentity(rawTenantKey, rawUsername, rawSessionId);
    try {
      await ensureDatabaseReady();
      let applied = false;

      if (clientMessageSeq > 0) {
        const tombstonePayload = buildAssistantScopeClearTombstonePayload();
        if (!tombstonePayload) {
          return {
            applied: false,
            stale: false,
            clientMessageSeq,
          };
        }

        const expiresAt = new Date(Date.now() + ASSISTANT_SESSION_SCOPE_TTL_MS).toISOString();
        const clearResult = await query(
          `
            INSERT INTO ${ASSISTANT_SESSION_SCOPE_TABLE} (
              cache_key,
              tenant_key,
              user_key,
              session_key,
              scope,
              last_seq,
              scope_bytes,
              updated_at,
              expires_at
            )
            VALUES ($1, $2, $3, $4, $5::jsonb, $6::bigint, $7, NOW(), $8::timestamptz)
            ON CONFLICT (cache_key) DO UPDATE
            SET tenant_key = EXCLUDED.tenant_key,
                user_key = EXCLUDED.user_key,
                session_key = EXCLUDED.session_key,
                scope = EXCLUDED.scope,
                last_seq = EXCLUDED.last_seq,
                scope_bytes = EXCLUDED.scope_bytes,
                updated_at = NOW(),
                expires_at = EXCLUDED.expires_at
            WHERE ${ASSISTANT_SESSION_SCOPE_TABLE}.last_seq < EXCLUDED.last_seq
          `,
          [
            identity.cacheKey,
            identity.tenantKey,
            identity.userKey,
            identity.sessionKey,
            tombstonePayload.scopeJson,
            clientMessageSeq,
            tombstonePayload.scopeBytes,
            expiresAt,
          ],
        );
        applied = parseAssistantSessionScopeStoreCount(clearResult?.rowCount) > 0;
      } else {
        const clearResult = await query(
          `
            DELETE FROM ${ASSISTANT_SESSION_SCOPE_TABLE}
            WHERE cache_key = $1
          `,
          [identity.cacheKey],
        );
        applied = parseAssistantSessionScopeStoreCount(clearResult?.rowCount) > 0;
      }

      const maintenance = await pruneAssistantSessionScopeStore(query);
      recordAssistantSessionScopeMetricSize?.(performanceObservability, maintenance.size);
      recordAssistantSessionScopeMetricBytes?.(performanceObservability, maintenance.totalBytes);
      if (maintenance.evictions > 0) {
        recordAssistantSessionScopeMetricEvictions?.(performanceObservability, maintenance.evictions);
      }
      return {
        applied,
        stale: clientMessageSeq > 0 && !applied,
        clientMessageSeq,
      };
    } catch (error) {
      recordAssistantSessionScopeMetricError?.(performanceObservability, error);
      log.warn?.(`[assistant][scope-store] clear failed: ${sanitizeTextValue(error?.message, 320) || "unknown error"}`);
      return {
        applied: false,
        stale: false,
        clientMessageSeq,
      };
    }
  }

  async function findAssistantOwnerLearningForMessage(message, options = {}) {
    await ensureDatabaseReady();

    const normalizedMessage = sanitizeTextValue(message, ASSISTANT_MAX_MESSAGE_LENGTH);
    const messageComparable = normalizeAssistantComparableText(normalizedMessage, ASSISTANT_MAX_MESSAGE_LENGTH);
    const messageTokens = tokenizeAssistantText(messageComparable);
    if (!messageComparable || !messageTokens.length) {
      return {
        promptExamples: [],
        directMatch: null,
      };
    }

    const parsedCandidateLimit = Number.parseInt(options?.candidateLimit, 10);
    const candidateLimit =
      Number.isFinite(parsedCandidateLimit) && parsedCandidateLimit > 0
        ? Math.min(Math.max(parsedCandidateLimit, 10), 600)
        : ASSISTANT_OWNER_LEARNING_CANDIDATE_LIMIT;

    const result = await query(
      `
        SELECT
          id,
          question,
          corrected_reply,
          correction_note,
          corrected_at
        FROM ${ASSISTANT_REVIEW_TABLE}
        WHERE corrected_at IS NOT NULL
          AND COALESCE(NULLIF(TRIM(corrected_reply), ''), '') <> ''
        ORDER BY corrected_at DESC, id DESC
        LIMIT $1
      `,
      [candidateLimit],
    );

    const ranked = [];
    for (const row of result.rows) {
      const idValue = Number.parseInt(row?.id, 10);
      const question = sanitizeTextValue(row?.question, ASSISTANT_MAX_MESSAGE_LENGTH);
      const ownerAnswer = normalizeAssistantReplyForDisplay(sanitizeTextValue(row?.corrected_reply, ASSISTANT_REVIEW_MAX_TEXT_LENGTH));
      const correctionNote = sanitizeTextValue(row?.correction_note, ASSISTANT_REVIEW_MAX_COMMENT_LENGTH);
      const correctedAt = row?.corrected_at ? new Date(row.corrected_at).toISOString() : null;
      if (!question || !ownerAnswer) {
        continue;
      }

      const scoreProfile = scoreAssistantOwnerLearningCandidate(messageComparable, messageTokens, question, ownerAnswer);
      if (scoreProfile.score < ASSISTANT_OWNER_LEARNING_MIN_CONTEXT_SCORE) {
        continue;
      }

      ranked.push({
        id: Number.isFinite(idValue) ? idValue : 0,
        question,
        ownerAnswer,
        correctionNote,
        correctedAt,
        score: scoreProfile.score,
        isDirectMatch: scoreProfile.isDirectMatch,
      });
    }

    ranked.sort((left, right) => {
      if (right.score !== left.score) {
        return right.score - left.score;
      }

      const rightCorrectedAt = right.correctedAt ? Date.parse(right.correctedAt) : 0;
      const leftCorrectedAt = left.correctedAt ? Date.parse(left.correctedAt) : 0;
      if (Number.isFinite(rightCorrectedAt) && Number.isFinite(leftCorrectedAt) && rightCorrectedAt !== leftCorrectedAt) {
        return rightCorrectedAt - leftCorrectedAt;
      }

      return right.id - left.id;
    });

    const promptExamples = ranked.slice(0, ASSISTANT_OWNER_LEARNING_MAX_PROMPT_EXAMPLES).map((item) => ({
      question: item.question,
      ownerAnswer: item.ownerAnswer,
      correctionNote: item.correctionNote,
      correctedAt: item.correctedAt,
    }));
    const directMatchCandidate =
      ranked.find((item) => item.isDirectMatch && item.score >= ASSISTANT_OWNER_LEARNING_DIRECT_MATCH_MIN_SCORE) || null;

    return {
      promptExamples,
      directMatch: directMatchCandidate
        ? {
            question: directMatchCandidate.question,
            ownerAnswer: directMatchCandidate.ownerAnswer,
            correctionNote: directMatchCandidate.correctionNote,
            correctedAt: directMatchCandidate.correctedAt,
          }
        : null,
    };
  }

  function buildAssistantReviewRetentionCutoffIso(nowMs = Date.now()) {
    const retentionWindowMs = ASSISTANT_REVIEW_RETENTION_DAYS * 24 * 60 * 60 * 1000;
    return new Date(nowMs - retentionWindowMs).toISOString();
  }

  async function runAssistantReviewRetentionSweep() {
    if (!ASSISTANT_REVIEW_RETENTION_SWEEP_ENABLED || !pool) {
      return;
    }

    await ensureDatabaseReady();
    const deleteResult = await query(
      `
        WITH candidates AS (
          SELECT id
          FROM ${ASSISTANT_REVIEW_TABLE}
          WHERE asked_at < $1::timestamptz
          ORDER BY asked_at ASC, id ASC
          LIMIT $2
        )
        DELETE FROM ${ASSISTANT_REVIEW_TABLE}
        WHERE id IN (SELECT id FROM candidates)
      `,
      [buildAssistantReviewRetentionCutoffIso(), ASSISTANT_REVIEW_RETENTION_SWEEP_BATCH_LIMIT],
    );
    const deletedCount = Number.isFinite(deleteResult?.rowCount) ? deleteResult.rowCount : 0;
    return {
      deletedCount,
    };
  }

  async function logAssistantReviewQuestion(entry) {
    await ensureDatabaseReady();

    const reviewClientMentions = normalizeAssistantReviewClientMentions(entry?.clientMentions);
    const question = sanitizeAssistantReviewTextForStorage(entry?.question, {
      maxLength: ASSISTANT_MAX_MESSAGE_LENGTH,
      piiMode: ASSISTANT_REVIEW_PII_MODE,
      clientMentions: reviewClientMentions,
    });
    if (!question) {
      return null;
    }

    const assistantReply = sanitizeAssistantReviewTextForStorage(entry?.assistantReply, {
      maxLength: ASSISTANT_REVIEW_MAX_TEXT_LENGTH,
      piiMode: ASSISTANT_REVIEW_PII_MODE,
      clientMentions: reviewClientMentions,
    });
    const askedByUsername = sanitizeTextValue(entry?.askedByUsername, 200);
    const askedByDisplayName = sanitizeTextValue(entry?.askedByDisplayName, 220);
    const mode = normalizeAssistantChatMode(entry?.mode);
    const provider = sanitizeTextValue(entry?.provider, 40) || "rules";
    const recordsUsedValue = Number.parseInt(entry?.recordsUsed, 10);
    const recordsUsed = Number.isFinite(recordsUsedValue) && recordsUsedValue >= 0 ? recordsUsedValue : 0;

    const result = await query(
      `
        INSERT INTO ${ASSISTANT_REVIEW_TABLE}
          (asked_by_username, asked_by_display_name, mode, question, assistant_reply, provider, records_used)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING
          id,
          asked_at,
          asked_by_username,
          asked_by_display_name,
          mode,
          question,
          assistant_reply,
          provider,
          records_used,
          corrected_reply,
          correction_note,
          corrected_by,
          corrected_at
      `,
      [askedByUsername, askedByDisplayName, mode, question, assistantReply, provider, recordsUsed],
    );

    return result.rows[0] ? mapAssistantReviewRow(result.rows[0]) : null;
  }

  async function listAssistantReviewQuestions(options = {}) {
    await ensureDatabaseReady();

    const limit = normalizeAssistantReviewLimit(options.limit);
    const offset = normalizeAssistantReviewOffset(options.offset);

    const [countResult, listResult] = await Promise.all([
      query(
        `
          SELECT COUNT(*)::BIGINT AS total
          FROM ${ASSISTANT_REVIEW_TABLE}
          WHERE corrected_at IS NULL
        `,
      ),
      query(
        `
          SELECT
            id,
            asked_at,
            asked_by_username,
            asked_by_display_name,
            mode,
            question,
            assistant_reply,
            provider,
            records_used,
            corrected_reply,
            correction_note,
            corrected_by,
            corrected_at
          FROM ${ASSISTANT_REVIEW_TABLE}
          WHERE corrected_at IS NULL
          ORDER BY asked_at DESC, id DESC
          LIMIT $1
          OFFSET $2
        `,
        [limit, offset],
      ),
    ]);

    const total = Number.parseInt(countResult.rows[0]?.total, 10);
    const items = listResult.rows.map(mapAssistantReviewRow).filter((item) => item.id > 0);

    return {
      total: Number.isFinite(total) && total >= 0 ? total : 0,
      limit,
      offset,
      items,
    };
  }

  async function saveAssistantReviewCorrection(reviewId, payload, correctedBy) {
    await ensureDatabaseReady();

    const normalizedId = Number.parseInt(sanitizeTextValue(reviewId, 30), 10);
    if (!Number.isFinite(normalizedId) || normalizedId <= 0) {
      throw createHttpError("Invalid review id.", 400);
    }

    const correctedReply = sanitizeTextValue(payload?.correctedReply, ASSISTANT_REVIEW_MAX_TEXT_LENGTH);
    const correctionNote = sanitizeTextValue(payload?.correctionNote, ASSISTANT_REVIEW_MAX_COMMENT_LENGTH);
    const markCorrect = resolveOptionalBoolean(payload?.markCorrect) === true;
    const normalizedCorrectedBy = sanitizeTextValue(correctedBy, 220) || "owner";
    const hasCorrectionPayload = Boolean(correctedReply || correctionNote);
    const shouldMarkCompleted = markCorrect || hasCorrectionPayload;
    if (!shouldMarkCompleted) {
      throw createHttpError("Provide a corrected answer, a correction note, or mark as correct.", 400);
    }

    const result = await query(
      `
        UPDATE ${ASSISTANT_REVIEW_TABLE}
        SET corrected_reply = CASE WHEN $6 THEN COALESCE(NULLIF($2, ''), assistant_reply) ELSE $2 END,
            correction_note = $3,
            corrected_by = CASE WHEN $5 THEN $4 ELSE '' END,
            corrected_at = CASE WHEN $5 THEN NOW() ELSE NULL END
        WHERE id = $1
        RETURNING
          id,
          asked_at,
          asked_by_username,
          asked_by_display_name,
          mode,
          question,
          assistant_reply,
          provider,
          records_used,
          corrected_reply,
          correction_note,
          corrected_by,
          corrected_at
      `,
      [normalizedId, correctedReply, correctionNote, normalizedCorrectedBy, shouldMarkCompleted, markCorrect],
    );

    if (!result.rows.length) {
      throw createHttpError("Assistant review item not found.", 404);
    }

    return mapAssistantReviewRow(result.rows[0]);
  }

  return {
    getAssistantSessionScope,
    upsertAssistantSessionScope,
    clearAssistantSessionScope,
    findAssistantOwnerLearningForMessage,
    runAssistantReviewRetentionSweep,
    logAssistantReviewQuestion,
    listAssistantReviewQuestions,
    saveAssistantReviewCorrection,
  };
}

module.exports = {
  createAssistantRepo,
};
