#!/usr/bin/env node

const { spawnSync } = require("node:child_process");

const AUDIT_COMMAND = ["audit", "--json"];

function parseAuditOutput(stdoutText) {
  const raw = String(stdoutText || "").trim();
  if (!raw) {
    return null;
  }

  const firstBrace = raw.indexOf("{");
  if (firstBrace < 0) {
    return null;
  }

  try {
    return JSON.parse(raw.slice(firstBrace));
  } catch {
    return null;
  }
}

const result = spawnSync("npm", AUDIT_COMMAND, {
  encoding: "utf8",
  stdio: ["ignore", "pipe", "pipe"],
});

const report = parseAuditOutput(result.stdout);
if (!report || typeof report !== "object") {
  console.error("[backend-audit] Failed to parse npm audit JSON output (fail-closed).");
  if (result.stderr) {
    console.error(result.stderr.trim());
  }
  process.exit(1);
}

const vulnerabilities = report.metadata?.vulnerabilities || {};
const info = Number(vulnerabilities.info || 0);
const low = Number(vulnerabilities.low || 0);
const moderate = Number(vulnerabilities.moderate || 0);
const high = Number(vulnerabilities.high || 0);
const critical = Number(vulnerabilities.critical || 0);

console.log(
  `[backend-audit] vulnerabilities: info=${info} low=${low} moderate=${moderate} high=${high} critical=${critical}`,
);

if (critical > 0) {
  console.error("[backend-audit] Gate failed: critical vulnerabilities are present.");
  process.exit(1);
}

if (high > 0) {
  console.error("[backend-audit] Gate failed: high vulnerabilities are present.");
  process.exit(1);
}

console.log("[backend-audit] Gate passed: no high or critical vulnerabilities.");
