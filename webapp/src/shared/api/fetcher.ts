export class ApiError extends Error {
  public readonly status: number;
  public readonly code: string;

  public constructor(message: string, status = 0, code = "api_error") {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
  }
}

interface RequestOptions extends RequestInit {
  allowUnauthorized?: boolean;
  timeoutMs?: number;
}

const DEFAULT_TIMEOUT_MS = 25_000;

export async function apiRequest<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const {
    allowUnauthorized = false,
    timeoutMs = DEFAULT_TIMEOUT_MS,
    headers,
    signal: externalSignal,
    ...rest
  } = options;
  let response: Response;
  const abortController = new AbortController();
  let didTimeout = false;
  const timeoutId = globalThis.setTimeout(() => {
    didTimeout = true;
    abortController.abort("timeout");
  }, timeoutMs);
  const detachExternalAbort = bindExternalAbortSignal(externalSignal, abortController);

  try {
    response = await fetch(path, {
      credentials: "same-origin",
      signal: abortController.signal,
      headers: {
        Accept: "application/json",
        ...headers,
      },
      ...rest,
    });
  } catch (error) {
    if (isTimeoutAbort(error, externalSignal, didTimeout)) {
      const timeoutSeconds = Math.max(1, Math.round(timeoutMs / 1000));
      throw new ApiError(
        `Request timed out after ${timeoutSeconds} seconds. Please try again.`,
        0,
        "timeout",
      );
    }

    if (isAbortError(error, abortController.signal, didTimeout)) {
      throw new ApiError("Request was cancelled.", 0, "aborted");
    }

    throw new ApiError("Network error. Check your internet connection.", 0, "network_error");
  } finally {
    globalThis.clearTimeout(timeoutId);
    detachExternalAbort();
  }

  if (response.status === 401 && !allowUnauthorized) {
    redirectToLogin();
    throw new ApiError("Session expired. Redirecting to login.", 401, "unauthorized");
  }

  const payload = await response
    .json()
    .catch(() => ({ error: `Request failed with status ${response.status}` }));

  if (!response.ok) {
    const errorText =
      typeof payload?.error === "string"
        ? payload.error
        : typeof payload?.details === "string"
          ? payload.details
          : `Request failed with status ${response.status}`;

    throw new ApiError(errorText, response.status, "http_error");
  }

  return payload as T;
}

function redirectToLogin(): void {
  const nextPath = `${window.location.pathname}${window.location.search}`;
  window.location.assign(`/login?next=${encodeURIComponent(nextPath)}`);
}

function bindExternalAbortSignal(
  externalSignal: AbortSignal | null | undefined,
  controller: AbortController,
): () => void {
  if (!externalSignal) {
    return () => {};
  }

  if (externalSignal.aborted) {
    controller.abort(externalSignal.reason);
    return () => {};
  }

  const onAbort = () => {
    controller.abort(externalSignal.reason);
  };
  externalSignal.addEventListener("abort", onAbort, { once: true });
  return () => {
    externalSignal.removeEventListener("abort", onAbort);
  };
}

function isTimeoutAbort(
  error: unknown,
  externalSignal: AbortSignal | null | undefined,
  didTimeout: boolean,
): boolean {
  if (didTimeout) {
    return true;
  }

  if (abortReasonMatchesTimeout(error)) {
    return true;
  }

  if (externalSignal?.aborted) {
    return false;
  }

  if (error instanceof DOMException && error.name === "AbortError") {
    return true;
  }

  return false;
}

function isAbortError(
  error: unknown,
  internalSignal: AbortSignal,
  didTimeout: boolean,
): boolean {
  if (didTimeout) {
    return false;
  }

  if (error instanceof DOMException && error.name === "AbortError") {
    return true;
  }

  if ((error as { name?: unknown })?.name === "AbortError") {
    return true;
  }

  return internalSignal.aborted && !abortReasonMatchesTimeout(error);
}

function abortReasonMatchesTimeout(error: unknown): boolean {
  if (typeof error === "string") {
    return error.toLowerCase() === "timeout";
  }

  return (error as { reason?: unknown })?.reason === "timeout";
}
