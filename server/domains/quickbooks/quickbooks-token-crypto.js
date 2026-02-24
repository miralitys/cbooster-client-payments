"use strict";

const crypto = require("crypto");

const ENCRYPTION_PREFIX = "enc:v1";
const AES_ALGORITHM = "aes-256-gcm";
const GCM_IV_BYTES = 12;
const GCM_TAG_BYTES = 16;

function sanitizeKeyId(rawValue) {
  const value = (rawValue || "").toString().trim();
  if (!value) {
    return "default";
  }
  return value.replace(/[^A-Za-z0-9_.-]/g, "").slice(0, 64) || "default";
}

function normalizeBase64Padding(rawValue) {
  const value = (rawValue || "").toString();
  const remainder = value.length % 4;
  if (remainder === 0) {
    return value;
  }
  return `${value}${"=".repeat(4 - remainder)}`;
}

function encodeBase64Url(bufferValue) {
  return Buffer.from(bufferValue)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function decodeBase64Url(rawValue, label = "value") {
  const normalized = normalizeBase64Padding((rawValue || "").toString().replace(/-/g, "+").replace(/_/g, "/"));
  if (!/^[A-Za-z0-9+/=]+$/.test(normalized)) {
    throw new Error(`QuickBooks refresh token payload is invalid (${label}).`);
  }
  return Buffer.from(normalized, "base64");
}

function tryDecodeBase64Key(rawValue) {
  const value = (rawValue || "").toString().trim();
  if (!value) {
    return null;
  }
  if (!/^[A-Za-z0-9+/=]+$/.test(value)) {
    return null;
  }
  const normalized = normalizeBase64Padding(value);
  const decoded = Buffer.from(normalized, "base64");
  return decoded.length === 32 ? decoded : null;
}

function resolveEncryptionKey(rawValue) {
  const value = (rawValue || "").toString().trim();
  if (!value) {
    return null;
  }

  if (/^[A-Fa-f0-9]{64}$/.test(value)) {
    return Buffer.from(value, "hex");
  }

  const decodedBase64 = tryDecodeBase64Key(value);
  if (decodedBase64) {
    return decodedBase64;
  }

  const utf8Key = Buffer.from(value, "utf8");
  if (utf8Key.length === 32) {
    return utf8Key;
  }

  throw new Error(
    "Invalid QUICKBOOKS_REFRESH_TOKEN_ENCRYPTION_KEY. Provide exactly 32 bytes (hex-64, base64-32bytes, or plain 32-char key).",
  );
}

function isEncryptedQuickBooksRefreshToken(rawValue) {
  const value = (rawValue || "").toString().trim();
  return value.startsWith(`${ENCRYPTION_PREFIX}:`);
}

function createQuickBooksRefreshTokenCrypto(options = {}) {
  const keyBuffer = resolveEncryptionKey(options.encryptionKey);
  const keyId = sanitizeKeyId(options.encryptionKeyId);
  const randomBytes =
    typeof options.randomBytes === "function"
      ? options.randomBytes
      : (size) => crypto.randomBytes(size);

  function encrypt(rawTokenValue) {
    const tokenValue = (rawTokenValue || "").toString().trim();
    if (!tokenValue) {
      return "";
    }
    if (!keyBuffer) {
      return tokenValue;
    }

    const iv = randomBytes(GCM_IV_BYTES);
    if (!Buffer.isBuffer(iv) || iv.length !== GCM_IV_BYTES) {
      throw new Error("QuickBooks token crypto random source returned invalid IV.");
    }

    const cipher = crypto.createCipheriv(AES_ALGORITHM, keyBuffer, iv);
    const encrypted = Buffer.concat([cipher.update(tokenValue, "utf8"), cipher.final()]);
    const authTag = cipher.getAuthTag();

    return `${ENCRYPTION_PREFIX}:${keyId}:${encodeBase64Url(iv)}:${encodeBase64Url(authTag)}:${encodeBase64Url(encrypted)}`;
  }

  function decrypt(rawStoredValue) {
    const storedValue = (rawStoredValue || "").toString().trim();
    if (!storedValue) {
      return "";
    }
    if (!isEncryptedQuickBooksRefreshToken(storedValue)) {
      return storedValue;
    }
    if (!keyBuffer) {
      const error = new Error("QuickBooks refresh token is encrypted but encryption key is not configured.");
      error.code = "quickbooks_refresh_token_crypto_key_missing";
      throw error;
    }

    const parts = storedValue.split(":");
    if (parts.length !== 6 || parts[0] !== "enc" || parts[1] !== "v1") {
      const error = new Error("QuickBooks refresh token payload format is invalid.");
      error.code = "quickbooks_refresh_token_crypto_payload_invalid";
      throw error;
    }

    const iv = decodeBase64Url(parts[3], "iv");
    const authTag = decodeBase64Url(parts[4], "auth_tag");
    const encrypted = decodeBase64Url(parts[5], "ciphertext");

    if (iv.length !== GCM_IV_BYTES || authTag.length !== GCM_TAG_BYTES || encrypted.length < 1) {
      const error = new Error("QuickBooks refresh token payload content is invalid.");
      error.code = "quickbooks_refresh_token_crypto_payload_invalid";
      throw error;
    }

    try {
      const decipher = crypto.createDecipheriv(AES_ALGORITHM, keyBuffer, iv);
      decipher.setAuthTag(authTag);
      const decrypted = Buffer.concat([decipher.update(encrypted), decipher.final()]);
      return decrypted.toString("utf8").trim();
    } catch {
      const error = new Error("QuickBooks refresh token decryption failed.");
      error.code = "quickbooks_refresh_token_crypto_decrypt_failed";
      throw error;
    }
  }

  return {
    encrypt,
    decrypt,
    isConfigured: () => Boolean(keyBuffer),
    isEncrypted: isEncryptedQuickBooksRefreshToken,
    keyId,
  };
}

module.exports = {
  createQuickBooksRefreshTokenCrypto,
  isEncryptedQuickBooksRefreshToken,
};

