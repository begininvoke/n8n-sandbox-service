import { test, expect } from '@playwright/test';
import { apiRequest, createJob, startJobAndCollect } from './helpers';

// Jobs API end-to-end proof: POST /jobs -> PUT files -> POST start (NDJSON to
// terminal) -> GET files/content -> DELETE, plus pull-failure and egress
// checks. See .superpowers/sdd/JOBS-POC-PLAN/task-6-brief.md.
test.describe('Jobs API', () => {
  test('alpine: input.json -> output.json round trip', async ({ request }) => {
    test.setTimeout(120_000);

    const id = await createJob(request, {
      image: 'alpine:3',
      cmd: ['sh', '-c', 'tr a-z A-Z < /n8n/input.json > /n8n/output.json'],
      timeout_ms: 60_000,
    });

    await apiRequest(request, 'PUT', `/jobs/${id}/files?path=input.json`, {
      data: '[{"hello":"world"}]',
    });

    const { exit } = await startJobAndCollect(request, id);
    expect(exit.type).toBe('exit');
    expect(exit.exit_code).toBe(0);

    const out = await apiRequest(request, 'GET', `/jobs/${id}/files/content?path=output.json`);
    expect(out.status).toBe(200);
    expect(out.body).toBe('[{"HELLO":"WORLD"}]');

    const del = await apiRequest(request, 'DELETE', `/jobs/${id}`);
    expect(del.status).toBe(204);
  });

  test('stdout streams and non-zero exit is reported', async ({ request }) => {
    test.setTimeout(120_000);

    const id = await createJob(request, {
      image: 'alpine:3',
      cmd: ['sh', '-c', 'echo hi; exit 3'],
      timeout_ms: 60_000,
    });

    const { events, exit } = await startJobAndCollect(request, id);
    expect(events.some((e) => e.type === 'stdout' && String(e.data ?? '').includes('hi'))).toBe(
      true,
    );
    expect(exit.type).toBe('exit');
    expect(exit.exit_code).toBe(3);

    await apiRequest(request, 'DELETE', `/jobs/${id}`);
  });

  test('image pull failure yields error event', async ({ request }) => {
    test.setTimeout(120_000);

    const id = await createJob(request, {
      image: 'ghcr.io/n8n-io/does-not-exist:404',
      timeout_ms: 60_000,
    });

    const { exit } = await startJobAndCollect(request, id);
    // Addendum correction: the terminal error event's machine-readable code
    // lives in `code`, not `data` (`data` carries the human-readable message).
    expect(exit.type).toBe('error');
    expect(exit.code).toBe('image_pull_failed');

    await apiRequest(request, 'DELETE', `/jobs/${id}`);
  });

  test('egress to private ranges is blocked', async ({ request }) => {
    test.setTimeout(120_000);

    // curlimages/curl's ENTRYPOINT is `curl`, so `cmd` is passed straight
    // through as curl's own args -- the job's exit_code is curl's exit code.
    const privateId = await createJob(request, {
      image: 'curlimages/curl:latest',
      cmd: ['--connect-timeout', '3', '-s', '-o', '/dev/null', 'http://169.254.169.254/'],
      timeout_ms: 30_000,
    });
    try {
      const { exit } = await startJobAndCollect(request, privateId);
      expect(exit.type).toBe('exit');
      expect(exit.exit_code).not.toBe(0);
    } finally {
      await apiRequest(request, 'DELETE', `/jobs/${privateId}`);
    }

    const publicId = await createJob(request, {
      image: 'curlimages/curl:latest',
      cmd: ['--connect-timeout', '10', '-fsS', '-o', '/dev/null', 'https://example.com/'],
      timeout_ms: 30_000,
    });
    try {
      const { exit } = await startJobAndCollect(request, publicId);
      expect(exit.type).toBe('exit');
      expect(exit.exit_code).toBe(0);
    } finally {
      await apiRequest(request, 'DELETE', `/jobs/${publicId}`);
    }
  });
});
