import { apiRequest } from "@/shared/api/fetcher";
import type { GhlClientContractsPayload } from "@/shared/types/ghlDocuments";

interface DownloadGhlClientContractResult {
  blob: Blob;
  fileName: string;
}

export async function getGhlClientContracts(limit = 25): Promise<GhlClientContractsPayload> {
  const normalizedLimit = Number.isFinite(limit) ? Math.max(1, Math.min(200, Math.trunc(limit))) : 25;
  const query = new URLSearchParams({
    limit: String(normalizedLimit),
  });
  return apiRequest<GhlClientContractsPayload>(`/api/ghl/client-contracts?${query.toString()}`);
}

export async function downloadGhlClientContract(clientName: string, contactId = ""): Promise<DownloadGhlClientContractResult> {
  const normalizedClientName = (clientName || "").toString().trim();
  if (!normalizedClientName) {
    throw new Error("Client name is required for contract download.");
  }

  const query = new URLSearchParams({
    clientName: normalizedClientName,
  });
  const normalizedContactId = (contactId || "").toString().trim();
  if (normalizedContactId) {
    query.set("contactId", normalizedContactId);
  }

  const response = await fetch(`/api/ghl/client-contracts/download?${query.toString()}`, {
    credentials: "include",
  });

  if (!response.ok) {
    const rawBody = await response.text().catch(() => "");
    let errorMessage = `Failed to download contract for "${normalizedClientName}".`;
    if (rawBody) {
      try {
        const parsed = JSON.parse(rawBody) as { error?: string };
        if (parsed && typeof parsed.error === "string" && parsed.error.trim()) {
          errorMessage = parsed.error.trim();
        } else {
          errorMessage = rawBody.trim() || errorMessage;
        }
      } catch {
        errorMessage = rawBody.trim() || errorMessage;
      }
    }
    throw new Error(errorMessage);
  }

  const blob = await response.blob();
  if (blob.size <= 0) {
    throw new Error(`Downloaded contract for "${normalizedClientName}" is empty.`);
  }

  const fileName = parseFileNameFromContentDisposition(response.headers.get("content-disposition")) || "contract.pdf";
  return {
    blob,
    fileName,
  };
}

function parseFileNameFromContentDisposition(contentDisposition: string | null): string {
  const value = (contentDisposition || "").toString().trim();
  if (!value) {
    return "";
  }

  const utf8Match = value.match(/filename\*=UTF-8''([^;]+)/i);
  if (utf8Match && utf8Match[1]) {
    try {
      const decoded = decodeURIComponent(utf8Match[1].trim());
      if (decoded) {
        return decoded;
      }
    } catch {
      // no-op
    }
  }

  const plainMatch = value.match(/filename="?([^";]+)"?/i);
  if (plainMatch && plainMatch[1]) {
    return plainMatch[1].trim();
  }

  return "";
}
