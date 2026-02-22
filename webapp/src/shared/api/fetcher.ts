export class ApiError extends Error {
  public readonly status: number;
  public readonly code: string;
  public readonly payload: unknown;

  public constructor(message: string, status = 0, code = "api_error", payload: unknown = null) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
    this.payload = payload;
  }
}

interface RequestOptions extends RequestInit {
  allowUnauthorized?: boolean;
  timeoutMs?: number;
}

const DEFAULT_TIMEOUT_MS = 25_000;
const WEB_CSRF_COOKIE_NAME = "cbooster_auth_csrf";
const WEB_CSRF_HEADER_NAME = "X-CSRF-Token";

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
    const requestMethod = String(rest.method || "GET").toUpperCase();
    const csrfToken = shouldAttachCsrfToken(requestMethod) ? readWebCsrfTokenFromCookie() : "";

    response = await fetch(path, {
      credentials: "same-origin",
      signal: abortController.signal,
      headers: {
        Accept: "application/json",
        ...(csrfToken ? { [WEB_CSRF_HEADER_NAME]: csrfToken } : {}),
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
    const errorCode =
      typeof payload?.code === "string" && payload.code.trim() ? payload.code.trim() : "http_error";

    throw new ApiError(errorText, response.status, errorCode, payload);
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

function shouldAttachCsrfToken(method: string): boolean {
  return method !== "GET" && method !== "HEAD" && method !== "OPTIONS";
}

function readWebCsrfTokenFromCookie(): string {
  if (typeof document === "undefined") {
    return "";
  }

  const rawCookie = String(document.cookie || "");
  if (!rawCookie) {
    return "";
  }

  const chunks = rawCookie.split(";");
  for (const chunk of chunks) {
    const [rawKey, ...rawValueParts] = chunk.split("=");
    const key = (rawKey || "").trim();
    if (key !== WEB_CSRF_COOKIE_NAME) {
      continue;
    }

    const rawValue = rawValueParts.join("=").trim();
    if (!rawValue) {
      return "";
    }

    try {
      return decodeURIComponent(rawValue);
    } catch {
      return rawValue;
    }
  }

  return "";
}
