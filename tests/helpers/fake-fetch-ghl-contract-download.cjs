"use strict";

const mode = String(process.env.TEST_FAKE_GHL_FETCH_MODE || "").trim().toLowerCase();
if (!mode) {
  return;
}

const PDF_BYTES = Buffer.from("%PDF-1.7\n1 0 obj\n<< /Type /Catalog >>\nendobj\n", "utf8");
const REDIRECT_LOCATION_PRIVATE = "http://169.254.169.254/latest/meta-data/iam/security-credentials/";
const REDIRECT_LOCATION_PUBLIC = "https://services.leadconnectorhq.com/contacts/redirect-contact/documents/redirect-doc";

function buildPdfResponse() {
  return new Response(PDF_BYTES, {
    status: 200,
    headers: {
      "content-type": "application/pdf",
      "content-length": String(PDF_BYTES.length),
      "content-disposition": 'attachment; filename="contract.pdf"',
    },
  });
}

function toUrlString(input) {
  if (typeof input === "string") {
    return input;
  }
  if (input instanceof URL) {
    return input.toString();
  }
  if (input && typeof input.url === "string") {
    return input.url;
  }
  if (input && typeof input.toString === "function") {
    const value = String(input.toString());
    if (value && value !== "[object Object]") {
      return value;
    }
  }
  return "";
}

global.fetch = async (input, init = {}) => {
  const requestUrl = toUrlString(input);
  const redirectMode = String(init?.redirect || "follow").toLowerCase();

  if (mode === "redirect-private") {
    if (redirectMode === "manual") {
      return new Response("", {
        status: 302,
        headers: {
          location: REDIRECT_LOCATION_PRIVATE,
        },
      });
    }
    return buildPdfResponse();
  }

  if (mode === "redirect-public") {
    if (requestUrl && requestUrl.startsWith(REDIRECT_LOCATION_PUBLIC)) {
      return buildPdfResponse();
    }
    if (redirectMode === "manual") {
      return new Response("", {
        status: 302,
        headers: {
          location: REDIRECT_LOCATION_PUBLIC,
        },
      });
    }
    return buildPdfResponse();
  }

  if (mode === "pdf") {
    return buildPdfResponse();
  }

  return new Response("not found", {
    status: 404,
    headers: {
      "content-type": "text/plain",
    },
  });
};
