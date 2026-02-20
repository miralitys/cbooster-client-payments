#!/usr/bin/env node

const fs = require("fs");
const bcrypt = require("bcryptjs");

const BCRYPT_HASH_REGEX = /^\$2[aby]\$\d{2}\$[./A-Za-z0-9]{53}$/;

function printUsageAndExit(message = "") {
  if (message) {
    console.error(message);
    console.error("");
  }
  console.error("Usage:");
  console.error("  node scripts/hash-web-auth-users-json.js --file /path/to/users.json");
  console.error("  cat users.json | node scripts/hash-web-auth-users-json.js --stdin");
  console.error("  WEB_AUTH_USERS_JSON='[...]' node scripts/hash-web-auth-users-json.js --env");
  process.exit(1);
}

function parseArgs(argv) {
  const options = {
    source: "",
    filePath: "",
    cost: 12,
    compact: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--file") {
      options.source = "file";
      options.filePath = argv[index + 1] || "";
      index += 1;
      continue;
    }

    if (arg === "--stdin") {
      options.source = "stdin";
      continue;
    }

    if (arg === "--env") {
      options.source = "env";
      continue;
    }

    if (arg === "--compact") {
      options.compact = true;
      continue;
    }

    if (arg === "--cost") {
      const rawCost = Number.parseInt(argv[index + 1] || "", 10);
      if (!Number.isFinite(rawCost) || rawCost < 10 || rawCost > 15) {
        printUsageAndExit("Invalid --cost. Use integer between 10 and 15.");
      }
      options.cost = rawCost;
      index += 1;
      continue;
    }

    printUsageAndExit(`Unknown argument: ${arg}`);
  }

  if (!options.source) {
    printUsageAndExit("Choose one source: --file, --stdin, or --env");
  }

  if (options.source === "file" && !options.filePath) {
    printUsageAndExit("--file requires a path.");
  }

  return options;
}

function readInput(options) {
  if (options.source === "file") {
    return fs.readFileSync(options.filePath, "utf8");
  }

  if (options.source === "env") {
    return (process.env.WEB_AUTH_USERS_JSON || "").toString();
  }

  return fs.readFileSync(0, "utf8");
}

function toStringSafe(value) {
  return (value || "").toString().trim();
}

function stripOuterQuotes(value) {
  const text = toStringSafe(value);
  if (text.length < 2) {
    return text;
  }

  const first = text[0];
  const last = text[text.length - 1];
  if ((first === "\"" && last === "\"") || (first === "'" && last === "'")) {
    return text.slice(1, -1).trim();
  }

  return text;
}

function stripUsersEnvPrefix(value) {
  let text = toStringSafe(value);
  text = text.replace(/^export\s+WEB_AUTH_USERS_JSON\s*=\s*/i, "");
  text = text.replace(/^WEB_AUTH_USERS_JSON\s*=\s*/i, "");
  text = text.replace(/;$/, "").trim();
  return text;
}

function tryParseUsersArray(candidate) {
  if (!candidate) {
    return null;
  }

  try {
    const parsed = JSON.parse(candidate);
    if (Array.isArray(parsed)) {
      return parsed;
    }
    if (typeof parsed === "string") {
      const parsedTwice = JSON.parse(parsed);
      if (Array.isArray(parsedTwice)) {
        return parsedTwice;
      }
    }
  } catch {
    return null;
  }

  return null;
}

function parseUsersInput(rawInput) {
  const base = toStringSafe(rawInput);
  const withoutEnvPrefix = stripUsersEnvPrefix(base);
  const withoutQuotes = stripOuterQuotes(withoutEnvPrefix);
  const unescapedQuotes = withoutQuotes.replace(/\\"/g, "\"");
  const candidates = [base, withoutEnvPrefix, withoutQuotes, unescapedQuotes];

  for (const candidate of candidates) {
    const parsed = tryParseUsersArray(candidate);
    if (parsed) {
      return parsed;
    }
  }

  throw new Error(
    "Input is not valid users JSON array. Paste either pure JSON array or WEB_AUTH_USERS_JSON=... value.",
  );
}

function migrateUsers(users, cost) {
  let hashedCount = 0;
  let alreadyHashedCount = 0;
  let emptyPasswordCount = 0;

  const migrated = users.map((user, index) => {
    if (!user || typeof user !== "object" || Array.isArray(user)) {
      throw new Error(`User at index ${index} is not an object.`);
    }

    const nextUser = { ...user };
    const rawPasswordHash = toStringSafe(nextUser.passwordHash || nextUser.password_hash);
    const rawPassword = toStringSafe(nextUser.password);

    if (rawPasswordHash) {
      if (!BCRYPT_HASH_REGEX.test(rawPasswordHash)) {
        throw new Error(`Invalid bcrypt passwordHash for user at index ${index}.`);
      }
      nextUser.passwordHash = rawPasswordHash;
      delete nextUser.password_hash;
      delete nextUser.password;
      alreadyHashedCount += 1;
      return nextUser;
    }

    if (!rawPassword) {
      delete nextUser.password;
      delete nextUser.password_hash;
      emptyPasswordCount += 1;
      return nextUser;
    }

    nextUser.passwordHash = bcrypt.hashSync(rawPassword, cost);
    delete nextUser.password;
    delete nextUser.password_hash;
    hashedCount += 1;
    return nextUser;
  });

  return {
    migrated,
    stats: {
      total: users.length,
      hashedCount,
      alreadyHashedCount,
      emptyPasswordCount,
    },
  };
}

(function main() {
  try {
    const options = parseArgs(process.argv.slice(2));
    const rawInput = readInput(options).trim();
    if (!rawInput) {
      throw new Error("Input is empty.");
    }

    const parsed = parseUsersInput(rawInput);

    const result = migrateUsers(parsed, options.cost);
    const output = options.compact
      ? JSON.stringify(result.migrated)
      : JSON.stringify(result.migrated, null, 2);

    process.stdout.write(`${output}\n`);
    process.stderr.write(
      `Done: total=${result.stats.total}, hashed=${result.stats.hashedCount}, alreadyHashed=${result.stats.alreadyHashedCount}, emptyPassword=${result.stats.emptyPasswordCount}\n`,
    );
  } catch (error) {
    process.stderr.write(`Error: ${error.message || "Unknown error"}\n`);
    process.exit(1);
  }
})();
