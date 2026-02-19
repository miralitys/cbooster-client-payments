import { apiRequest } from "@/shared/api/fetcher";
import type { GhlClientBasicNotePayload } from "@/shared/types/ghlNotes";

export async function getGhlClientBasicNote(
  clientName: string,
  options: { signal?: AbortSignal; writtenOff?: boolean } = {},
): Promise<GhlClientBasicNotePayload> {
  const normalizedClientName = (clientName || "").toString().trim();
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
