/**
 * Error thrown when the sandbox service returns a failed response.
 */
export class SandboxServiceError extends Error {
  /**
   * Creates a sandbox service error with HTTP status and optional API error code.
   * Sandbox/exec endpoints use numeric codes; job endpoints use string codes
   * (e.g. `"image_pull_failed"`) — this accepts either.
   */
  constructor(
    message: string,
    readonly status: number,
    readonly code?: number | string,
  ) {
    super(message);
    this.name = "SandboxServiceError";
  }
}

/**
 * Thrown by `getJobFile` when the requested job output file doesn't exist (HTTP 404). A
 * distinct subclass so callers can special-case "no such output file" without string-matching
 * the server's error message.
 */
export class JobFileNotFoundError extends SandboxServiceError {
  constructor(message: string, code?: number | string) {
    super(message, 404, code);
    this.name = "JobFileNotFoundError";
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

/**
 * Normalizes a sandbox service error response into a typed error instance.
 */
export function createErrorFromResponse(status: number, data: unknown): SandboxServiceError {
  if (typeof data === "object" && data !== null && "error" in data) {
    const payload = data as { error: string; code?: number | string };
    return new SandboxServiceError(payload.error, status, payload.code);
  }

  const message =
    typeof data === "string" && data.length > 0
      ? data
      : `Sandbox service request failed with status ${status}`;

  return new SandboxServiceError(message, status);
}
