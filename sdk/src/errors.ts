/**
 * Error thrown when the sandbox service returns a failed response.
 */
export class SandboxServiceError extends Error {
  /**
   * Creates a sandbox service error with HTTP status and optional API error code.
   */
  constructor(
    message: string,
    readonly status: number,
    readonly code?: number,
  ) {
    super(message);
    this.name = "SandboxServiceError";
  }
}

/**
 * Error thrown when the sandbox had to be restarted while the request was in flight,
 * because its guest crashed.
 *
 * The sandbox is running again by the time this is thrown, and its files are intact,
 * so retrying the request once is the right response. What did not survive is
 * everything that was in memory:
 *
 * - processes started by earlier executions are gone, and nothing restarts them;
 * - completed executions are no longer readable, so `getExecution` returns a
 *   not-found error even for a command that succeeded before the restart;
 * - a client-supplied `execId` is no longer idempotent — re-posting one that ran
 *   before the restart runs the command again rather than replaying its result.
 *
 * This is deliberately not retried automatically: a silent retry would hide the loss,
 * which is the one thing this error exists to prevent.
 */
export class SandboxRestartedError extends SandboxServiceError {
  constructor(message: string, code?: number) {
    super(message, 409, code);
    this.name = "SandboxRestartedError";
  }
}

/**
 * Error thrown when an invalid stream event is encountered, such as when a truncated
 * JSON record is encountered. This might indicate a transient connectivity issue with
 * the stream.
 */
export class InvalidStreamEventError extends Error {
  readonly line: string;

  constructor(line: string, cause?: unknown) {
    super(`Invalid stream event encountered`, {
      cause,
    });

    this.name = "InvalidStreamEventError";
    this.line = line;
  }
}

/** Set on the response of a request refused because the sandbox was restarted. */
const SANDBOX_RESTARTED_HEADER = "x-sandbox-restarted";

/** Body field carrying the same signal, for hops that rewrite headers. */
const SANDBOX_RESTARTED_REASON = "sandbox_restarted";

/**
 * Normalizes a sandbox service error response into a typed error instance.
 */
export function createErrorFromResponse(
  status: number,
  data: unknown,
  headers?: unknown,
): SandboxServiceError {
  let message =
    typeof data === "string" && data.length > 0
      ? data
      : `Sandbox service request failed with status ${status}`;
  let code: number | undefined;

  if (typeof data === "object" && data !== null && "error" in data) {
    const payload = data as { error: string; code?: number };
    message = payload.error;
    code = payload.code;
  }

  if (isSandboxRestarted(status, data, headers)) {
    return new SandboxRestartedError(message, code);
  }

  return new SandboxServiceError(message, status, code);
}

/**
 * The header is checked first because it is the one channel that survives every hop
 * unchanged; the body field is the fallback, since error bodies are reshaped as they
 * pass from runner to API.
 */
function isSandboxRestarted(status: number, data: unknown, headers: unknown): boolean {
  if (status !== 409) return false;
  if (headerValue(headers, SANDBOX_RESTARTED_HEADER) === "1") return true;

  return (
    typeof data === "object" &&
    data !== null &&
    "reason" in data &&
    (data as { reason?: unknown }).reason === SANDBOX_RESTARTED_REASON
  );
}

function headerValue(headers: unknown, name: string): string | undefined {
  if (typeof headers !== "object" || headers === null) return undefined;

  for (const [key, value] of Object.entries(headers as Record<string, unknown>)) {
    if (key.toLowerCase() !== name) continue;
    if (typeof value === "string") return value;
    if (Array.isArray(value) && typeof value[0] === "string") return value[0];
    return undefined;
  }

  return undefined;
}
