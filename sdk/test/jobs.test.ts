import { Readable } from "node:stream";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SandboxClient } from "../src/client.js";
import { JobFileNotFoundError, SandboxServiceError } from "../src/errors.js";
import type { HttpClient } from "../src/http.js";
import { getJobFile, resumeJobEvents, startJob } from "../src/jobs.js";
import { parseExecEvent } from "../src/ndjson.js";
import { startTestServer, type TestServer } from "./helpers.js";

type MockHttp = {
  requestJson: ReturnType<typeof vi.fn>;
  requestVoid: ReturnType<typeof vi.fn>;
  requestBuffer: ReturnType<typeof vi.fn>;
  requestStream: ReturnType<typeof vi.fn>;
};

/** Replaces a client's private HttpClient with a plain mock (mirrors client.test.ts's pattern). */
function withMockHttp(client: SandboxClient): MockHttp {
  const http: MockHttp = {
    requestJson: vi.fn(),
    requestVoid: vi.fn(),
    requestBuffer: vi.fn(),
    requestStream: vi.fn(),
  };
  (client as unknown as { http: MockHttp }).http = http;
  return http;
}

/** Builds a mock HttpClient whose successive `requestStream` calls resolve to the given NDJSON lines. */
function ndjsonHttp(streams: string[][]): HttpClient {
  const requestStream = vi.fn();
  for (const lines of streams) {
    requestStream.mockResolvedValueOnce({
      stream: Readable.from([Buffer.from(lines.join("\n") + "\n")]),
      status: 200,
    });
  }
  return { requestStream, requestVoid: vi.fn() } as unknown as HttpClient;
}

describe("parseExecEvent — jobs contract shapes", () => {
  it("recognizes a pulling event", () => {
    expect(parseExecEvent('{"seq":0,"type":"pulling","data":"alpine:3"}')).toEqual({
      seq: 0,
      type: "pulling",
      data: "alpine:3",
    });
  });

  it("recognizes a started event without exec_id (jobs never send one)", () => {
    expect(parseExecEvent('{"seq":1,"type":"started"}')).toEqual({
      seq: 1,
      type: "started",
    });
  });

  it("still tolerates a started event that includes exec_id (exec's shape)", () => {
    expect(parseExecEvent('{"seq":1,"type":"started","exec_id":"sess-1"}')).toEqual({
      seq: 1,
      type: "started",
      exec_id: "sess-1",
    });
  });

  it("recognizes the jobs error shape (code + data), distinct from exec's error shape", () => {
    expect(
      parseExecEvent(
        '{"seq":2,"type":"error","code":"image_pull_failed","data":"pull access denied"}',
      ),
    ).toEqual({
      seq: 2,
      type: "error",
      code: "image_pull_failed",
      data: "pull access denied",
    });
  });

  it("still recognizes exec's plain-message error shape", () => {
    expect(parseExecEvent('{"seq":2,"type":"error","error":"command not found"}')).toEqual({
      seq: 2,
      type: "error",
      error: "command not found",
    });
  });

  it("ignores a well-formed event of an unrecognized type instead of erroring", () => {
    expect(parseExecEvent('{"seq":3,"type":"checkpoint","data":"unused"}')).toEqual({
      seq: 3,
      type: "unknown",
    });
  });

  it("still errors on a malformed envelope (no seq/type)", () => {
    expect(parseExecEvent('{"type":"checkpoint","foo":"bar"}')).toEqual({
      type: "error",
      error: 'Invalid exec event payload: {"type":"checkpoint","foo":"bar"}',
    });
  });
});

describe("SandboxClient jobs CRUD", () => {
  let client: SandboxClient;

  beforeEach(() => {
    client = new SandboxClient({ baseUrl: "http://localhost:8080", apiKey: "test-key" });
  });

  it("createJob sends POST /jobs and maps the wire response", async () => {
    const http = withMockHttp(client);
    http.requestJson.mockResolvedValue({
      id: "job-1",
      status: "staging",
      image: "alpine:3",
      exit_code: null,
      timed_out: false,
      error: null,
      created_at: 1000,
      started_at: null,
      finished_at: null,
    });

    const result = await client.createJob({
      image: "alpine:3",
      cmd: ["sh", "-c", "echo hi"],
      timeoutMs: 60_000,
    });

    expect(http.requestJson).toHaveBeenCalledWith("POST", "/jobs", {
      data: { image: "alpine:3", cmd: ["sh", "-c", "echo hi"], env: undefined, timeout_ms: 60_000 },
    });
    expect(result).toEqual({
      id: "job-1",
      status: "staging",
      image: "alpine:3",
      exitCode: null,
      timedOut: false,
      error: null,
      createdAt: 1000,
      startedAt: null,
      finishedAt: null,
    });
  });

  it("stageJobFile sends PUT /jobs/{id}/files as octet-stream", async () => {
    const http = withMockHttp(client);
    http.requestVoid.mockResolvedValue(undefined);

    await client.stageJobFile("job-1", "input.json", '{"hello":"world"}');

    expect(http.requestVoid).toHaveBeenCalledWith("PUT", "/jobs/job-1/files", {
      params: { path: "input.json" },
      headers: { "Content-Type": "application/octet-stream" },
      data: Buffer.from('{"hello":"world"}'),
    });
  });

  it("stageJobFileFromURL sends POST /jobs/{id}/files/fetch with path and url", async () => {
    const http = withMockHttp(client);
    http.requestVoid.mockResolvedValue(undefined);

    await client.stageJobFileFromURL("job-1", "cat.jpg", "https://cataas.com/cat/abc");

    expect(http.requestVoid).toHaveBeenCalledWith("POST", "/jobs/job-1/files/fetch", {
      data: { path: "cat.jpg", url: "https://cataas.com/cat/abc" },
    });
  });

  it("getJob sends GET /jobs/{id} and maps nullable fields", async () => {
    const http = withMockHttp(client);
    http.requestJson.mockResolvedValue({
      id: "job-1",
      status: "exited",
      image: "alpine:3",
      exit_code: 0,
      timed_out: false,
      error: null,
      created_at: 1000,
      started_at: 1001,
      finished_at: 1002,
    });

    const result = await client.getJob("job-1");

    expect(http.requestJson).toHaveBeenCalledWith("GET", "/jobs/job-1");
    expect(result).toEqual({
      id: "job-1",
      status: "exited",
      image: "alpine:3",
      exitCode: 0,
      timedOut: false,
      error: null,
      createdAt: 1000,
      startedAt: 1001,
      finishedAt: 1002,
    });
  });

  it("getJobFile sends GET /jobs/{id}/files/content", async () => {
    const http = withMockHttp(client);
    http.requestBuffer.mockResolvedValue(Buffer.from("output"));

    const result = await client.getJobFile("job-1", "output.json");

    expect(http.requestBuffer).toHaveBeenCalledWith("GET", "/jobs/job-1/files/content", {
      params: { path: "output.json" },
    });
    expect(result.toString()).toBe("output");
  });

  it("getJobFile throws JobFileNotFoundError on a 404", async () => {
    const http = withMockHttp(client);
    http.requestBuffer.mockRejectedValue(
      new SandboxServiceError("output file not found", 404, "not_found"),
    );

    const err = await client.getJobFile("job-1", "missing.json").catch((e) => e);

    expect(err).toBeInstanceOf(JobFileNotFoundError);
    expect(err.message).toBe("output file not found");
    expect(err.status).toBe(404);
    expect(err.code).toBe("not_found");
  });

  it("getJobFile rethrows non-404 errors unchanged", async () => {
    const http = withMockHttp(client);
    const original = new SandboxServiceError("internal error", 500, "internal");
    http.requestBuffer.mockRejectedValue(original);

    const err = await client.getJobFile("job-1", "output.json").catch((e) => e);

    expect(err).toBe(original);
    expect(err).not.toBeInstanceOf(JobFileNotFoundError);
  });

  it("deleteJob sends DELETE /jobs/{id}", async () => {
    const http = withMockHttp(client);
    http.requestVoid.mockResolvedValue(undefined);

    await client.deleteJob("job-1");

    expect(http.requestVoid).toHaveBeenCalledWith("DELETE", "/jobs/job-1");
  });
});

describe("getJobFile (free function)", () => {
  it("throws JobFileNotFoundError on a 404", async () => {
    const http = {
      requestBuffer: vi
        .fn()
        .mockRejectedValue(new SandboxServiceError("not found", 404, "not_found")),
    } as unknown as HttpClient;

    const err = await getJobFile(http, "job-1", "missing.json").catch((e) => e);
    expect(err).toBeInstanceOf(JobFileNotFoundError);
  });
});

describe("startJob resume behavior", () => {
  it("ignores pulling/started (no exec_id) and aggregates stdout to the exit result", async () => {
    const http = ndjsonHttp([
      [
        '{"seq":0,"type":"pulling","data":"alpine:3"}',
        '{"seq":1,"type":"started"}',
        '{"seq":2,"type":"stdout","data":"hi\\n"}',
        '{"seq":3,"type":"exit","exit_code":0,"success":true,"execution_time_ms":10,"timed_out":false,"killed":false}',
      ],
    ]);

    const result = await startJob(http, "job-1");

    expect(result).toEqual({
      exitCode: 0,
      stdout: "hi\n",
      stderr: "",
      executionTimeMs: 10,
      timedOut: false,
      killed: false,
      success: true,
    });
    expect(http.requestStream).toHaveBeenCalledWith("POST", "/jobs/job-1/start", {
      isSafeToRetry: false,
      signal: undefined,
    });
  });

  it("ignores an unrecognized well-formed event instead of treating it as terminal", async () => {
    const http = ndjsonHttp([
      [
        '{"seq":0,"type":"pulling","data":"alpine:3"}',
        '{"seq":1,"type":"checkpoint","data":"unused"}',
        '{"seq":2,"type":"stdout","data":"hi"}',
        '{"seq":3,"type":"exit","exit_code":0,"success":true,"execution_time_ms":10,"timed_out":false,"killed":false}',
      ],
    ]);

    const result = await startJob(http, "job-1");

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toBe("hi");
  });

  it("resumes via GET /jobs/{id}/events after a non-terminal stream end", async () => {
    const http = ndjsonHttp([
      ['{"seq":0,"type":"pulling","data":"alpine:3"}', '{"seq":1,"type":"stdout","data":"part1"}'],
      [
        '{"seq":2,"type":"stdout","data":"part2"}',
        '{"seq":3,"type":"exit","exit_code":0,"success":true,"execution_time_ms":20,"timed_out":false,"killed":false}',
      ],
    ]);

    const result = await startJob(http, "job-1");

    expect(result.stdout).toBe("part1part2");
    expect(http.requestStream).toHaveBeenCalledTimes(2);
    const secondCall = (http.requestStream as ReturnType<typeof vi.fn>).mock.calls[1];
    expect(secondCall[0]).toBe("GET");
    expect(secondCall[1]).toBe("/jobs/job-1/events");
    expect(secondCall[2]).toEqual(
      expect.objectContaining({ params: { after: "1", follow: "true" } }),
    );
  });

  it("omits `after` on the resume GET when nothing has been consumed yet", async () => {
    const requestStream = vi
      .fn()
      .mockRejectedValueOnce(new SandboxServiceError("network error", 0))
      .mockResolvedValueOnce({
        stream: Readable.from([
          Buffer.from(
            '{"seq":0,"type":"exit","exit_code":0,"success":true,"execution_time_ms":5,"timed_out":false,"killed":false}\n',
          ),
        ]),
        status: 200,
      });
    const http = { requestStream, requestVoid: vi.fn() } as unknown as HttpClient;

    const result = await startJob(http, "job-1");

    expect(result.exitCode).toBe(0);
    const resumeCall = requestStream.mock.calls[1];
    expect(resumeCall[0]).toBe("GET");
    expect(resumeCall[2].params).toEqual({ follow: "true" });
    expect(resumeCall[2].params).not.toHaveProperty("after");
  });

  it("throws SandboxServiceError carrying the jobs error code + message on a terminal error event", async () => {
    const http = ndjsonHttp([
      [
        '{"seq":0,"type":"pulling","data":"ghcr.io/n8n-io/does-not-exist:404"}',
        '{"seq":1,"type":"error","code":"image_pull_failed","data":"pull access denied"}',
      ],
    ]);

    const err = await startJob(http, "job-1").catch((e) => e);
    expect(err).toBeInstanceOf(SandboxServiceError);
    expect(err.message).toBe("pull access denied");
    expect(err.code).toBe("image_pull_failed");
  });

  it("retries the events resume on a transient error", async () => {
    const requestStream = vi
      .fn()
      .mockResolvedValueOnce({
        stream: Readable.from([Buffer.from('{"seq":0,"type":"pulling","data":"alpine:3"}\n')]),
        status: 200,
      })
      .mockRejectedValueOnce(new SandboxServiceError("network error", 0))
      .mockResolvedValueOnce({
        stream: Readable.from([
          Buffer.from(
            '{"seq":1,"type":"exit","exit_code":0,"success":true,"execution_time_ms":5,"timed_out":false,"killed":false}\n',
          ),
        ]),
        status: 200,
      });
    const http = { requestStream, requestVoid: vi.fn() } as unknown as HttpClient;

    const result = await startJob(http, "job-1");

    expect(result.exitCode).toBe(0);
    expect(requestStream).toHaveBeenCalledTimes(3);
  });
});

describe("resumeJobEvents", () => {
  it("sends after=<seq> and follow=true, consuming to a terminal exit", async () => {
    const requestStream = vi.fn().mockResolvedValueOnce({
      stream: Readable.from([
        Buffer.from(
          '{"seq":6,"type":"stdout","data":"more\\n"}\n' +
            '{"seq":7,"type":"exit","exit_code":0,"success":true,"execution_time_ms":30,"timed_out":false,"killed":false}\n',
        ),
      ]),
      status: 200,
    });
    const http = { requestStream, requestVoid: vi.fn() } as unknown as HttpClient;

    const result = await resumeJobEvents(http, "job-1", 5);

    expect(result.stdout).toBe("more\n");
    expect(requestStream).toHaveBeenCalledWith("GET", "/jobs/job-1/events", {
      params: { after: "5", follow: "true" },
      signal: undefined,
    });
  });

  it("omits `after` when afterSeq is undefined", async () => {
    const requestStream = vi.fn().mockResolvedValueOnce({
      stream: Readable.from([
        Buffer.from(
          '{"seq":0,"type":"exit","exit_code":0,"success":true,"execution_time_ms":1,"timed_out":false,"killed":false}\n',
        ),
      ]),
      status: 200,
    });
    const http = { requestStream, requestVoid: vi.fn() } as unknown as HttpClient;

    await resumeJobEvents(http, "job-1");

    expect(requestStream).toHaveBeenCalledWith("GET", "/jobs/job-1/events", {
      params: { follow: "true" },
      signal: undefined,
    });
  });
});

describe("startJob streaming (real HTTP)", () => {
  let server: TestServer | undefined;

  afterEach(async () => {
    await server?.close();
    server = undefined;
  });

  it("emits pulling -> started -> stdout -> exit and returns the aggregated JobResult", async () => {
    server = await startTestServer((req, res) => {
      if (req.method === "POST" && req.url === "/jobs/job-1/start") {
        res.writeHead(200, { "Content-Type": "application/x-ndjson" });
        res.write('{"seq":0,"type":"pulling","data":"alpine:3"}\n');
        res.write('{"seq":1,"type":"started"}\n');
        res.write('{"seq":2,"type":"stdout","data":"hi\\n"}\n');
        res.end(
          '{"seq":3,"type":"exit","exit_code":0,"success":true,"execution_time_ms":15,"timed_out":false,"killed":false}\n',
        );
        return;
      }
      res.writeHead(404);
      res.end();
    });

    const client = new SandboxClient({ baseUrl: server.baseUrl });
    const stdoutChunks: string[] = [];

    const result = await client.startJob("job-1", { onStdout: (data) => stdoutChunks.push(data) });

    expect(result).toEqual({
      exitCode: 0,
      stdout: "hi\n",
      stderr: "",
      executionTimeMs: 15,
      timedOut: false,
      killed: false,
      success: true,
    });
    expect(stdoutChunks).toEqual(["hi\n"]);
  });
});
