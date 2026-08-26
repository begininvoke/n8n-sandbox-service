package docker

import (
	"context"
	"log/slog"
	"time"
)

// watchGuestDeaths reports sandbox containers that died without the runner asking,
// which is what a Firecracker runner gets for free from the process it supervises.
// Docker owns the container's lifecycle instead, so the deaths have to be subscribed
// to, and the runner learns of them from the same stream whether the guest killed
// itself, hit its memory limit, or was killed on the host.
//
// It runs for the life of the runner and reconnects, because losing this stream is
// the one failure that is silent: containers keep working, and crashes stop being
// reported. A death that happens while it is down is not replayed and stays
// unreported — the container usually comes back on the address it had, so the sandbox
// goes on serving as if nothing was lost. Nothing recovers that; the reconnects are
// what keep the window small.
func (m *Runtime) watchGuestDeaths(ctx context.Context) {
	for ctx.Err() == nil {
		err := m.docker.watchContainerDeaths(ctx, m.handleContainerDeath)
		if ctx.Err() != nil {
			return
		}
		slog.Warn("docker event stream ended, reconnecting", "retry_in", m.watchBackoff, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(m.watchBackoff):
		}
	}
}

// handleContainerDeath records a died container as a crash unless the runner is the
// one that stopped it. Exit codes are deliberately not consulted: a guest that exits
// 0 on its own has still lost everything it was running, and `docker stop` of a
// healthy sandbox produces the non-zero exit of a SIGTERM'd daemon.
//
// Nothing is torn down here. The container carries `--restart unless-stopped`, so
// Docker is already restarting it — a restart that is invisible to a client, and to
// the network rules pinned to the IP the container had before. Marking the sandbox is
// what makes both visible: DaemonURL reports it not running until the wake path has
// reapplied its policy, and that wake reports the restart to the client.
func (m *Runtime) handleContainerDeath(containerID, sandboxID string) {
	if m.takeExpectedStop(containerID) {
		return
	}
	if sandboxID == "" {
		// The event carries no sandbox label, so there is nothing to mark and no
		// request that could be told. Managed containers always have one.
		slog.Warn("managed container died without a sandbox label", "container_id", containerID)
		return
	}

	m.mu.Lock()
	m.restarted[sandboxID] = struct{}{}
	m.mu.Unlock()

	m.metrics.ObserveGuestDeath()
	slog.Warn("docker guest died", "sandbox_id", sandboxID, "container_id", containerID)
}

// expectedStopTTL bounds how long a recorded stop can excuse a death. Docker emits
// the event within milliseconds of the stop it is asked for, so anything older is a
// stop that never produced one — a stop that failed, or a container that was already
// exited when it was removed — and a mark kept past that would eventually excuse a
// real crash of a container whose ID it no longer refers to.
const expectedStopTTL = 2 * time.Minute

// expectStop records that the runner is about to stop or remove a container, so the
// die event that follows is not read as a crash. Keyed by container rather than
// sandbox because that is what every caller has, and what the event names.
func (m *Runtime) expectStop(containerID string) {
	if containerID == "" {
		return
	}
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	// Swept here because it is the only place the map grows, and it holds at most the
	// stops of the last two minutes.
	for id, at := range m.expectedStops {
		if now.Sub(at) > expectedStopTTL {
			delete(m.expectedStops, id)
		}
	}
	m.expectedStops[containerID] = now
}

// takeExpectedStop consumes one expected stop for a container. Consuming rather than
// reading is what keeps a stopped-then-restarted sandbox honest: the next death of
// the same container is a crash again.
func (m *Runtime) takeExpectedStop(containerID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	at, ok := m.expectedStops[containerID]
	if !ok {
		return false
	}
	delete(m.expectedStops, containerID)
	return time.Since(at) <= expectedStopTTL
}

// wasRestarted reports whether a sandbox's container died and Docker brought it
// back. It reads without consuming: the mark is cleared only once the runner has
// re-admitted the sandbox, so a wake that fails leaves the next request to try
// again rather than proxying into a container with stale network rules.
func (m *Runtime) wasRestarted(sandboxID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.restarted[sandboxID]
	return ok
}

// clearRestarted drops a sandbox's restart mark, which is what spends the one report
// a client gets for it. Also called when the sandbox is deleted: a crash nobody came
// back to ask about is not worth remembering.
func (m *Runtime) clearRestarted(sandboxID string) {
	m.mu.Lock()
	delete(m.restarted, sandboxID)
	m.mu.Unlock()
}
