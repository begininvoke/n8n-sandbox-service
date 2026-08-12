import { JobFileNotFoundError, SandboxServiceError } from "./errors";
import { ExecStreamConsumer } from "./exec-stream-consumer";
import { delay, isTransientError, MAX_RESUME_RETRIES, RESUME_DELAY_MS } from "./exec";
import type { HttpClient } from "./http";
import type {
  ExecResult,
  FileContent,
  JobRecord,
  JobResult,
  JobSpec,
  JobWireResponse,
  StartJobOptions,
} from "./types";

export async function createJob(http: HttpClient, opts: JobSpec): Promise<JobRecord> {
  const response = await http.requestJson<JobWireResponse>("POST", "/jobs", {
    data: {
      image: opts.image,
      cmd: opts.cmd,
      env: opts.env,
      timeout_ms: opts.timeoutMs,
    },
  });
  return mapJobRecord(response);
}

export async function stageJobFile(
  http: HttpClient,
  id: string,
  path: string,
  data: FileContent,
): Promise<void> {
  await http.requestVoid("PUT", `/jobs/${id}/files`, {
    params: { path },
    headers: { "Content-Type": "application/octet-stream" },
    data: asBuffer(data),
  });
}

/**
 * Stages a job input file by having the runner download `url` server-side, saving the
 * caller a download-then-reupload round trip for inputs that are already reachable by URL
 * (e.g. a binary referenced by a workflow item).
 */
export async function stageJobFileFromURL(
  http: HttpClient,
  id: string,
  path: string,
  url: string,
): Promise<void> {
  await http.requestVoid("POST", `/jobs/${id}/files/fetch`, {
    data: { path, url },
  });
}

/**
 * Starts a job and streams its output. The job id pre-exists (created via `createJob`), so
 * unlike `exec`'s two-phase loop there's no client-side idempotency key to retry the initial
 * POST with: a single start attempt is made, and any resume after that goes through
 * `followJobEvents` — the same loop `resumeJobEvents` exposes standalone.
 */
export async function startJob(
  http: HttpClient,
  id: string,
  opts: StartJobOptions = {},
): Promise<JobResult> {
  const consumer = new ExecStreamConsumer(opts.onStdout, opts.onStderr);

  try {
    const { stream } = await http.requestStream("POST", `/jobs/${id}/start`, {
      isSafeToRetry: false,
      signal: opts.abortSignal,
    });
    await consumer.consume(stream);
  } catch (error) {
    if (!isTransientError(error)) throw error;
  }

  await followJobEvents(http, id, consumer, undefined, opts.abortSignal);
  return toJobResult(consumer.result());
}

/**
 * Resumes watching a job's event stream from `afterSeq` (omit for the full history) until a
 * terminal event, retrying transient disconnects the same way `startJob` does after its
 * initial POST. Useful for reattaching to an already-running or already-finished job.
 */
export async function resumeJobEvents(
  http: HttpClient,
  id: string,
  afterSeq?: number,
  opts: StartJobOptions = {},
): Promise<JobResult> {
  const consumer = new ExecStreamConsumer(opts.onStdout, opts.onStderr);
  await followJobEvents(http, id, consumer, afterSeq, opts.abortSignal);
  return toJobResult(consumer.result());
}

export async function getJob(http: HttpClient, id: string): Promise<JobRecord> {
  const response = await http.requestJson<JobWireResponse>("GET", `/jobs/${id}`);
  return mapJobRecord(response);
}

/** Reads a job's output file. Throws `JobFileNotFoundError` (not a bare `SandboxServiceError`) on 404. */
export async function getJobFile(http: HttpClient, id: string, path: string): Promise<Buffer> {
  try {
    return await http.requestBuffer("GET", `/jobs/${id}/files/content`, { params: { path } });
  } catch (error) {
    if (error instanceof SandboxServiceError && error.status === 404) {
      throw new JobFileNotFoundError(error.message, error.code);
    }
    throw error;
  }
}

export async function deleteJob(http: HttpClient, id: string): Promise<void> {
  await http.requestVoid("DELETE", `/jobs/${id}`);
}

/**
 * Loops GET /jobs/{id}/events?after=&follow=true until a terminal event, retrying transient
 * errors up to `MAX_RESUME_RETRIES`. `initialAfterSeq` seeds the first request only; once the
 * consumer has seen any event, its own `lastSeq` takes over. Never sends `after` when there's
 * nothing to resume from (negative/undefined) — the contract has no meaning for `after=-1`.
 */
async function followJobEvents(
  http: HttpClient,
  id: string,
  consumer: ExecStreamConsumer,
  initialAfterSeq: number | undefined,
  signal?: AbortSignal,
): Promise<void> {
  let retries = 0;

  while (!consumer.isDone) {
    try {
      const after = consumer.lastSeq >= 0 ? consumer.lastSeq : initialAfterSeq;
      const params: Record<string, string> = { follow: "true" };
      if (after !== undefined && after >= 0) params.after = String(after);

      const { stream } = await http.requestStream("GET", `/jobs/${id}/events`, {
        params,
        signal,
      });
      await consumer.consume(stream);
      if (consumer.isDone) break;
      if (++retries > MAX_RESUME_RETRIES) break;
      await delay(RESUME_DELAY_MS);
    } catch (error) {
      if (!isTransientError(error)) throw error;
      if (++retries > MAX_RESUME_RETRIES) throw error;
      await delay(RESUME_DELAY_MS);
    }
  }
}

function toJobResult(result: ExecResult): JobResult {
  return {
    exitCode: result.exitCode,
    stdout: result.stdout,
    stderr: result.stderr,
    executionTimeMs: result.executionTimeMs,
    timedOut: result.timedOut,
    killed: result.killed,
    success: result.success,
  };
}

function mapJobRecord(wire: JobWireResponse): JobRecord {
  return {
    id: wire.id,
    status: wire.status,
    image: wire.image,
    exitCode: wire.exit_code,
    timedOut: wire.timed_out,
    error: wire.error,
    createdAt: wire.created_at,
    startedAt: wire.started_at,
    finishedAt: wire.finished_at,
  };
}

function asBuffer(content: FileContent): Buffer {
  return typeof content === "string" ? Buffer.from(content, "utf-8") : Buffer.from(content);
}
