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
}

export async function apiRequest<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const { allowUnauthorized = false, headers, ...rest } = options;
  let response: Response;

  try {
    response = await fetch(path, {
      credentials: "same-origin",
      headers: {
        Accept: "application/json",
        ...headers,
      },
      ...rest,
    });
  } catch {
    throw new ApiError("Network error. Check your internet connection.", 0, "network_error");
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
