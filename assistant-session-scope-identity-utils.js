"use strict";

function normalizeAuthUsernameForScopeKey(rawValue) {
  return (rawValue || "").toString().normalize("NFKC").trim().toLowerCase();
}

module.exports = {
  normalizeAuthUsernameForScopeKey,
};
