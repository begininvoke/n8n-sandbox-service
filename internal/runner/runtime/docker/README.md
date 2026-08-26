# Docker/Sysbox Runner Runtime

This runtime starts each sandbox as a Docker container managed by the runner's
inner Docker daemon. In production it is expected to run in a Sysbox-backed
runner container so Docker-in-Docker can run without giving ordinary workload
containers direct access to the host Docker daemon.

## Technology

- Uses the Docker CLI against `SANDBOX_RUNNER_DOCKER_HOST`.
- Starts sandbox containers from `SANDBOX_RUNNER_DOCKER_SANDBOX_IMAGE`.
- Connects containers to the runner bridge network.
- Proxies API traffic to the sandbox daemon on port `8081`.

## Supported Features

- Pulls the sandbox image in the background and retries with backoff until it is
  available.
- Reports readiness only after the sandbox image is present and Docker is
  reachable.
- Reports capacity from the current managed container count.
- Applies default memory, CPU, PID, and optional disk quota limits on create.
- Drops every Linux capability, then restores only `CAP_CHOWN`,
  `CAP_DAC_OVERRIDE`, `CAP_FOWNER`, `CAP_SETGID`, and `CAP_SETUID`. This keeps
  passwordless `sudo` usable for common package installation while preventing
  network administration, audit-log writes, mounts, tracing, device creation,
  raw sockets, and other privileged operations. Successful `sudo` commands may
  emit an audit warning because `CAP_AUDIT_WRITE` is intentionally excluded.
  Sysbox isolation and privileged local DinD apply to the runner container, so
  every sandbox container gets the same policy. Never create a sandbox container
  with `--privileged`: Docker then ignores `--cap-drop` and the allowlist has no
  effect.
- Applies Docker-specific network isolation rules through `netrules`.
- Waits for daemon `/healthz` and a tiny `/executions` round trip before
  returning a sandbox as ready.
- Wakes stopped containers on proxy access, reapplies network rules, and waits
  for the daemon before proxying.
- Uses singleflight so concurrent wake requests for the same sandbox only run
  one wake operation.
- Best-effort reconciles and removes stale managed containers on startup and
  shutdown.
- Detects guests that died on their own and reports the restart to the client.

## Crash recovery

Sandbox containers run with `--restart unless-stopped`, so Docker brings a died
container back on its own — with its files, and without telling anyone. Two things
are wrong with that on its own: the container can come back on a different IP, which
its network policy does not know about, and the client is never told that everything
the sandbox held in memory is gone.

`crash.go` closes both. The runner subscribes to `docker events` filtered to `die`
on its own containers, and reads every death it did not ask for as a crash. Deaths it
did ask for are recorded before the stop or remove that causes them, and matched
against the event — exit codes are never consulted, because a guest that exits `0` on
its own has still lost what it was running, and a `docker stop` of a healthy sandbox
produces the non-zero exit of a SIGTERM'd daemon. The recorded stops expire, so one
that never produced a death cannot go on excusing a later crash of the same
container.

A sandbox marked that way is reported as not running by `DaemonURL` until the wake
path has re-admitted it: reapplied its network policy for the address it came back
on, and waited for its daemon. Reporting it that way is what forces a container that
already looks healthy through that path, and what turns the restart into
`WakeResult{Recovered}` — and from there into `409 sandbox_restarted` for the request
that found it, exactly as on the Firecracker runner.

The event stream is the weak point, because losing it is silent: containers keep
working while crashes stop being reported. The watcher therefore reconnects for the
life of the runner, and a death missed while it was down means a restart served
without its `409`.
