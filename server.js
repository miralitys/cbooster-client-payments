const path = require("path");
const crypto = require("crypto");
const express = require("express");
const { Pool } = require("pg");

const PORT = Number.parseInt(process.env.PORT || "10000", 10);
const DATABASE_URL = (process.env.DATABASE_URL || "").trim();
const BASIC_AUTH_USER = (process.env.BASIC_AUTH_USER || "").trim();
const BASIC_AUTH_PASSWORD = (process.env.BASIC_AUTH_PASSWORD || "").trim();
const BASIC_AUTH_REALM = "CB Payment";
const IS_BASIC_AUTH_ENABLED = Boolean(BASIC_AUTH_USER && BASIC_AUTH_PASSWORD);
const STATE_ROW_ID = 1;
const DEFAULT_TABLE_NAME = "client_records_state";
const TABLE_NAME = resolveTableName(process.env.DB_TABLE_NAME, DEFAULT_TABLE_NAME);

const app = express();
app.use(express.json({ limit: "10mb" }));

app.use((req, res, next) => {
  if (!IS_BASIC_AUTH_ENABLED) {
    next();
    return;
  }

  if (req.path === "/api/health") {
    next();
    return;
  }

  const credentials = extractBasicAuthCredentials(req.headers.authorization);
  const isAuthorized =
    credentials !== null &&
    safeEqual(credentials.user, BASIC_AUTH_USER) &&
    safeEqual(credentials.password, BASIC_AUTH_PASSWORD);

  if (isAuthorized) {
    next();
    return;
  }

  requestAuth(res);
});

const staticRoot = __dirname;
app.use(express.static(staticRoot));

const pool = DATABASE_URL
  ? new Pool({
      connectionString: DATABASE_URL,
      ssl: shouldUseSsl() ? { rejectUnauthorized: false } : false,
    })
  : null;

let dbReadyPromise = null;

function resolveTableName(rawTableName, fallbackTableName) {
  const normalized = (rawTableName || fallbackTableName || "").trim();
  if (!normalized) {
    throw new Error("DB table name cannot be empty.");
  }

  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(normalized)) {
    throw new Error(`Unsafe DB table name: "${normalized}"`);
  }

  return normalized;
}

function shouldUseSsl() {
  const mode = (process.env.PGSSLMODE || "").toLowerCase();
  return mode !== "disable";
}

function requestAuth(res) {
  res.set("WWW-Authenticate", `Basic realm="${BASIC_AUTH_REALM}", charset="UTF-8"`);
  res.status(401).send("Authentication required");
}

function extractBasicAuthCredentials(rawAuthorization) {
  if (typeof rawAuthorization !== "string") {
    return null;
  }

  const [scheme, token] = rawAuthorization.split(" ");
  if (scheme !== "Basic" || !token) {
    return null;
  }

  try {
    const decoded = Buffer.from(token, "base64").toString("utf8");
    const separatorIndex = decoded.indexOf(":");
    if (separatorIndex < 0) {
      return null;
    }

    return {
      user: decoded.slice(0, separatorIndex),
      password: decoded.slice(separatorIndex + 1),
    };
  } catch {
    return null;
  }
}

function safeEqual(leftValue, rightValue) {
  const left = Buffer.from(leftValue, "utf8");
  const right = Buffer.from(rightValue, "utf8");

  if (left.length !== right.length) {
    return false;
  }

  return crypto.timingSafeEqual(left, right);
}

async function ensureDatabaseReady() {
  if (!pool) {
    throw new Error("DATABASE_URL is not configured.");
  }

  if (!dbReadyPromise) {
    dbReadyPromise = (async () => {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS ${TABLE_NAME} (
          id BIGINT PRIMARY KEY,
          records JSONB NOT NULL DEFAULT '[]'::jsonb,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `);

      await pool.query(
        `
          INSERT INTO ${TABLE_NAME} (id, records)
          VALUES ($1, '[]'::jsonb)
          ON CONFLICT (id) DO NOTHING
        `,
        [STATE_ROW_ID],
      );
    })().catch((error) => {
      dbReadyPromise = null;
      throw error;
    });
  }

  return dbReadyPromise;
}

async function getStoredRecords() {
  await ensureDatabaseReady();
  const result = await pool.query(`SELECT records, updated_at FROM ${TABLE_NAME} WHERE id = $1`, [STATE_ROW_ID]);

  if (!result.rows.length) {
    return { records: [], updatedAt: null };
  }

  const row = result.rows[0];
  return {
    records: Array.isArray(row.records) ? row.records : [],
    updatedAt: row.updated_at || null,
  };
}

async function saveStoredRecords(records) {
  await ensureDatabaseReady();
  const result = await pool.query(
    `
      INSERT INTO ${TABLE_NAME} (id, records, updated_at)
      VALUES ($1, $2::jsonb, NOW())
      ON CONFLICT (id)
      DO UPDATE SET records = EXCLUDED.records, updated_at = NOW()
      RETURNING updated_at
    `,
    [STATE_ROW_ID, JSON.stringify(records)],
  );

  return result.rows[0]?.updated_at || null;
}

function isValidRecordsPayload(value) {
  return Array.isArray(value);
}

app.get("/api/health", (_req, res) => {
  if (!pool) {
    res.status(503).json({
      ok: false,
      error: "DATABASE_URL is not configured",
    });
    return;
  }

  res.json({
    ok: true,
  });
});

app.get("/api/records", async (_req, res) => {
  if (!pool) {
    res.status(503).json({
      error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
    });
    return;
  }

  try {
    const state = await getStoredRecords();
    res.json({
      records: state.records,
      updatedAt: state.updatedAt,
    });
  } catch (error) {
    console.error("GET /api/records failed:", error);
    res.status(500).json({
      error: "Failed to load records",
    });
  }
});

app.put("/api/records", async (req, res) => {
  if (!pool) {
    res.status(503).json({
      error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
    });
    return;
  }

  const nextRecords = req.body?.records;
  if (!isValidRecordsPayload(nextRecords)) {
    res.status(400).json({
      error: "Payload must include `records` as an array.",
    });
    return;
  }

  try {
    const updatedAt = await saveStoredRecords(nextRecords);
    res.json({
      ok: true,
      updatedAt,
    });
  } catch (error) {
    console.error("PUT /api/records failed:", error);
    res.status(500).json({
      error: "Failed to save records",
    });
  }
});

app.use("/api", (_req, res) => {
  res.status(404).json({
    error: "API route not found",
  });
});

app.get("*", (_req, res) => {
  res.sendFile(path.join(staticRoot, "index.html"));
});

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
  if (!pool) {
    console.warn("DATABASE_URL is missing. API routes will return 503 until configured.");
  }
  if (!IS_BASIC_AUTH_ENABLED) {
    console.warn("Basic auth is disabled. Set BASIC_AUTH_USER and BASIC_AUTH_PASSWORD to enable it.");
  }
});
