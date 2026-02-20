import { apiRequest } from "@/shared/api/fetcher";
import type { GhlClientBasicNotePayload } from "@/shared/types/ghlNotes";

export async function getGhlClientBasicNote(
  clientName: string,
  options: { signal?: AbortSignal; writtenOff?: boolean; refresh?: boolean } = {},
): Promise<GhlClientBasicNotePayload> {
  const normalizedClientName = (clientName || "").toString().trim();
  const shouldRefresh = options.refresh !== false;

  if (shouldRefresh) {
    return apiRequest<GhlClientBasicNotePayload>("/api/ghl/client-basic-note/refresh", {
      method: "POST",
      signal: options.signal,
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        clientName: normalizedClientName,
        writtenOff: options.writtenOff === true,
      }),
    });
  }

  const query = new URLSearchParams({
    clientName: normalizedClientName,
  });
  if (options.writtenOff === true) {
    query.set("writtenOff", "1");
  }
  return apiRequest<GhlClientBasicNotePayload>(`/api/ghl/client-basic-note?${query.toString()}`, {
    signal: options.signal,
  });
}
