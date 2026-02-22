#!/usr/bin/env node

const crypto = require("crypto");

const BASE32_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

function printUsageAndExit(message = "") {
  if (message) {
    process.stderr.write(`${message}\n\n`);
  }

  process.stderr.write("Usage:\n");
  process.stderr.write("  node scripts/generate-web-auth-totp.js --username owner\n");
  process.stderr.write('  node scripts/generate-web-auth-totp.js --username owner --issuer "Credit Booster"\n');
  process.stderr.write("\nOptions:\n");
  process.stderr.write("  --username   required username/login label\n");
  process.stderr.write("  --issuer     issuer name shown in Authenticator app (default: Credit Booster)\n");
  process.stderr.write("  --bytes      random secret size in bytes, 10..64 (default: 20)\n");
  process.stderr.write("  --period     TOTP step in seconds, 15..120 (default: 30)\n");
  process.stderr.write("  --compact    output only JSON payload\n");
  process.exit(1);
}

function sanitizeText(value, maxLength = 200) {
  return (value || "")
    .toString()
    .normalize("NFKC")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, maxLength);
}

function parseArgs(argv) {
  const options = {
    username: "",
    issuer: sanitizeText(process.env.WEB_AUTH_TOTP_ISSUER, 120) || "Credit Booster",
    bytes: 20,
    period: 30,
    compact: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--username") {
      options.username = sanitizeText(argv[index + 1], 180).toLowerCase();
      index += 1;
      continue;
    }
    if (arg === "--issuer") {
      options.issuer = sanitizeText(argv[index + 1], 120) || options.issuer;
      index += 1;
      continue;
    }
    if (arg === "--bytes") {
      const bytes = Number.parseInt(argv[index + 1], 10);
      if (!Number.isFinite(bytes) || bytes < 10 || bytes > 64) {
        printUsageAndExit("Invalid --bytes. Use integer between 10 and 64.");
      }
      options.bytes = bytes;
      index += 1;
      continue;
    }
    if (arg === "--period") {
      const period = Number.parseInt(argv[index + 1], 10);
      if (!Number.isFinite(period) || period < 15 || period > 120) {
        printUsageAndExit("Invalid --period. Use integer between 15 and 120.");
      }
      options.period = period;
      index += 1;
      continue;
    }
    if (arg === "--compact") {
      options.compact = true;
      continue;
    }
    printUsageAndExit(`Unknown argument: ${arg}`);
  }

  if (!options.username) {
    printUsageAndExit("--username is required.");
  }

  return options;
}

function encodeBase32(rawBuffer) {
  const buffer = Buffer.isBuffer(rawBuffer) ? rawBuffer : Buffer.from(rawBuffer || "");
  if (!buffer.length) {
    return "";
  }

  let bits = 0;
  let value = 0;
  let output = "";

  for (const byte of buffer) {
    value = (value << 8) | byte;
    bits += 8;

    while (bits >= 5) {
      const index = (value >>> (bits - 5)) & 31;
      output += BASE32_ALPHABET[index];
      bits -= 5;
    }
  }

  if (bits > 0) {
    const index = (value << (5 - bits)) & 31;
    output += BASE32_ALPHABET[index];
  }

  return output;
}

function buildTotpSetupUri({ username, issuer, secret, period }) {
  const label = `${issuer}:${username}`;
  const query = new URLSearchParams({
    secret,
    issuer,
    algorithm: "SHA1",
    digits: "6",
    period: String(period),
  });
  return `otpauth://totp/${encodeURIComponent(label)}?${query.toString()}`;
}

(function main() {
  try {
    const options = parseArgs(process.argv.slice(2));
    const secret = encodeBase32(crypto.randomBytes(options.bytes));
    const otpauthUri = buildTotpSetupUri({
      username: options.username,
      issuer: options.issuer,
      secret,
      period: options.period,
    });

    const payload = {
      username: options.username,
      totpSecret: secret,
      totpEnabled: true,
      otpauthUri,
      issuer: options.issuer,
      period: options.period,
    };

    if (options.compact) {
      process.stdout.write(`${JSON.stringify(payload)}\n`);
      return;
    }

    process.stdout.write(`Username: ${payload.username}\n`);
    process.stdout.write(`Issuer: ${payload.issuer}\n`);
    process.stdout.write(`TOTP Secret (Base32): ${payload.totpSecret}\n`);
    process.stdout.write(`otpauth URI: ${payload.otpauthUri}\n\n`);
    process.stdout.write("WEB_AUTH_USERS_JSON snippet:\n");
    process.stdout.write(
      `${JSON.stringify(
        {
          username: payload.username,
          totpSecret: payload.totpSecret,
          totpEnabled: true,
        },
        null,
        2,
      )}\n`,
    );
  } catch (error) {
    process.stderr.write(`Error: ${error?.message || "Unknown error"}\n`);
    process.exit(1);
  }
})();
