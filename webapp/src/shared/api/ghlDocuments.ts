import { apiRequest } from "@/shared/api/fetcher";
import type { GhlClientContractsPayload } from "@/shared/types/ghlDocuments";

interface DownloadGhlClientContractResult {
  blob: Blob;
  fileName: string;
  mode: "direct" | "text-fallback" | "diagnostic-fallback";
}

interface GetGhlClientContractsOptions {
  clientName?: string;
}

export async function getGhlClientContracts(limit = 25, options: GetGhlClientContractsOptions = {}): Promise<GhlClientContractsPayload> {
  const normalizedLimit = Number.isFinite(limit) ? Math.max(1, Math.min(200, Math.trunc(limit))) : 25;
  const query = new URLSearchParams({
    limit: String(normalizedLimit),
  });
  const normalizedClientName = (options.clientName || "").toString().trim();
  if (normalizedClientName) {
    query.set("clientName", normalizedClientName);
  }
  return apiRequest<GhlClientContractsPayload>(`/api/ghl/client-contracts?${query.toString()}`, {
    timeoutMs: 60_000,
  });
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

  const timeoutMs = 45_000;
  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => {
    controller.abort();
  }, timeoutMs);

  let response: Response;
  try {
    response = await fetch(`/api/ghl/client-contracts/download?${query.toString()}`, {
      credentials: "include",
      signal: controller.signal,
    });
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") {
      throw new Error(`Download request timed out after ${Math.round(timeoutMs / 1000)} seconds for "${normalizedClientName}".`);
    }
    const message = error instanceof Error ? error.message : "Unknown network error";
    throw new Error(`Failed to download contract for "${normalizedClientName}": ${message}`);
  } finally {
    window.clearTimeout(timeoutId);
  }

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
  const modeHeader = (response.headers.get("x-contract-download-mode") || "").toString().trim().toLowerCase();
  const mode: "direct" | "text-fallback" | "diagnostic-fallback" =
    modeHeader === "text-fallback" ? "text-fallback" : modeHeader === "diagnostic-fallback" ? "diagnostic-fallback" : "direct";
  return {
    blob,
    fileName,
    mode,
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
