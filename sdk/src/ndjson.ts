import type { Readable } from "node:stream";
import { InvalidStreamEventError } from "./errors";
import type {
  ExecEvent,
  ExecStartedEvent,
  ExecStdoutEvent,
  ExecStderrEvent,
  ExecExitEvent,
  ExecErrorEvent,
  JobErrorEvent,
  ExecPullingEvent,
} from "./types";

type JsonObject = Record<string, unknown>;

const KNOWN_EVENT_TYPES = new Set(["started", "stdout", "stderr", "exit", "error", "pulling"]);

// Note: this only asserts the common envelope (valid seq + a *known* type string), not a
// specific ExecEvent member — narrowing to the full union here would make TS think `json`
// is already one specific variant once `json.type === "error"` is checked below, hiding the
// other error-shape's fields (`code`/`data` vs `error`) from the type-specific guards.
function isExecEvent(json: JsonObject): json is JsonObject & { seq: number; type: string } {
  return (
    typeof json.seq === "number" &&
    typeof json.type === "string" &&
    KNOWN_EVENT_TYPES.has(json.type)
  );
}

/** Has a valid `seq` + `type` envelope, regardless of whether `type` is one we recognize. */
function isWellFormedEnvelope(json: JsonObject): json is { type: string; seq: number } {
  return typeof json.seq === "number" && typeof json.type === "string";
}

function isStartedEvent(json: JsonObject): json is ExecStartedEvent {
  return (
    isExecEvent(json) &&
    json.type === "started" &&
    (json.exec_id === undefined || typeof json.exec_id === "string")
  );
}

function isStdoutEvent(json: JsonObject): json is ExecStdoutEvent {
  return isExecEvent(json) && json.type === "stdout" && typeof json.data === "string";
}

function isStderrEvent(json: JsonObject): json is ExecStderrEvent {
  return isExecEvent(json) && json.type === "stderr" && typeof json.data === "string";
}

function isExitEvent(json: JsonObject): json is ExecExitEvent {
  return (
    isExecEvent(json) &&
    json.type === "exit" &&
    typeof json.exit_code === "number" &&
    typeof json.success === "boolean" &&
    typeof json.execution_time_ms === "number" &&
    typeof json.timed_out === "boolean" &&
    typeof json.killed === "boolean" &&
    typeof json.seq === "number"
  );
}

/** Exec stream's error shape: a human-readable message. */
function isExecErrorEvent(json: JsonObject): json is ExecErrorEvent {
  return isExecEvent(json) && json.type === "error" && typeof json.error === "string";
}

/** Jobs stream's error shape: a string code plus a message (contract v1). */
function isJobErrorEvent(json: JsonObject): json is JobErrorEvent {
  return (
    isExecEvent(json) &&
    json.type === "error" &&
    typeof json.code === "string" &&
    typeof json.data === "string"
  );
}

function isPullingEvent(json: JsonObject): json is ExecPullingEvent {
  return isExecEvent(json) && json.type === "pulling" && typeof json.data === "string";
}

/** Yields parsed exec events from an NDJSON stream, one per line. */
export async function* readNdjsonStream(stream: Readable): AsyncGenerator<ExecEvent> {
  let pending = "";
  const decoder = new TextDecoder("utf-8");

  for await (const chunk of stream) {
    pending += decodeChunk(decoder, chunk, { stream: true });

    let newlineIndex = pending.indexOf("\n");
    while (newlineIndex !== -1) {
      const line = pending.slice(0, newlineIndex).trim();
      pending = pending.slice(newlineIndex + 1);
      if (line.length > 0) {
        yield parseExecEvent(line);
      }
      newlineIndex = pending.indexOf("\n");
    }
  }

  pending += decoder.decode();

  const last = pending.trim();
  if (last.length > 0) {
    yield parseExecEvent(last);
  }
}

function decodeChunk(decoder: TextDecoder, chunk: unknown, options?: TextDecodeOptions): string {
  if (typeof chunk === "string") return decoder.decode(Buffer.from(chunk, "utf-8"), options);
  if (chunk instanceof ArrayBuffer) return decoder.decode(chunk, options);
  if (ArrayBuffer.isView(chunk)) return decoder.decode(chunk as ArrayBufferView, options);

  return decoder.decode(Buffer.from(String(chunk), "utf-8"), options);
}

/**
 * Parses a single NDJSON line into a typed exec/job event. Returns an error event when the
 * payload doesn't even have a valid `seq`+`type` envelope (or a known type with a malformed
 * shape), but returns a no-op "unknown" event — never an error — for a well-formed envelope
 * whose `type` this SDK version simply doesn't recognize yet. That keeps a future server-side
 * event type from killing an in-flight stream.
 */
export function parseExecEvent(line: string): ExecEvent {
  try {
    const json = JSON.parse(line) as JsonObject;

    if (isStartedEvent(json)) return json;
    if (isStdoutEvent(json)) return json;
    if (isStderrEvent(json)) return json;
    if (isExitEvent(json)) return json;
    if (isExecErrorEvent(json)) return json;
    if (isJobErrorEvent(json)) return json;
    if (isPullingEvent(json)) return json;
    if (isWellFormedEnvelope(json) && !KNOWN_EVENT_TYPES.has(json.type)) {
      return { type: "unknown", seq: json.seq };
    }

    return { type: "error", error: `Invalid exec event payload: ${line}` };
  } catch (error) {
    throw new InvalidStreamEventError(line, error instanceof Error ? error : undefined);
  }
}
