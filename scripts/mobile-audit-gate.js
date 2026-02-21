#!/usr/bin/env node

const { existsSync } = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const repoRoot = path.resolve(__dirname, "..");
const mobilePackageJson = path.join(repoRoot, "mobile-app", "package.json");

if (!existsSync(mobilePackageJson)) {
  console.log("[mobile-audit] Skipped: mobile-app/package.json not found.");
  process.exit(0);
}

const result = spawnSync("npm", ["--prefix", "mobile-app", "audit", "--omit=dev", "--json"], {
  cwd: repoRoot,
  encoding: "utf8",
  stdio: ["ignore", "pipe", "pipe"],
});

function parseAuditOutput(stdoutText) {
  const raw = (stdoutText || "").trim();
  if (!raw) {
    return null;
  }

  const firstBrace = raw.indexOf("{");
  if (firstBrace < 0) {
    return null;
  }

  const candidate = raw.slice(firstBrace);
  try {
    return JSON.parse(candidate);
  } catch {
    return null;
  }
}

const report = parseAuditOutput(result.stdout);

if (!report || typeof report !== "object") {
  console.error("[mobile-audit] Failed to parse npm audit JSON output.");
  if (result.stderr) {
    console.error(result.stderr.trim());
  }
  process.exit(result.status || 1);
}

const summary = report.metadata?.vulnerabilities || {};
const info = Number(summary.info || 0);
const low = Number(summary.low || 0);
const moderate = Number(summary.moderate || 0);
const high = Number(summary.high || 0);
const critical = Number(summary.critical || 0);

console.log(
  `[mobile-audit] vulnerabilities: info=${info} low=${low} moderate=${moderate} high=${high} critical=${critical}`,
);

if (critical > 0) {
  console.error("[mobile-audit] Gate failed: critical vulnerabilities are present.");
  process.exit(1);
}

if (high > 0) {
  console.warn(
    "[mobile-audit] Gate passed (temporary): high vulnerabilities are allowed, critical vulnerabilities are not present.",
  );
  process.exit(0);
}

console.log("[mobile-audit] Gate passed: no critical vulnerabilities.");
