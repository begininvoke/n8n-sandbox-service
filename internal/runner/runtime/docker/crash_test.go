package docker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/n8n-io/sandbox-service/internal/metrics"
	"github.com/n8n-io/sandbox-service/internal/runner/config"
	runnerruntime "github.com/n8n-io/sandbox-service/internal/runner/runtime"
)

const (
	crashSandboxID   = "sandbox-id-123456"
	crashContainerID = "container-1"
)

// crashBackend is a Docker whose container states the test drives, which is what
// crash handling reads: a container that died and came back looks running again, on
// an address it did not have before.
type crashBackend struct {
	mu       sync.Mutex
	states   []containerState // consumed in order; the last one repeats
	ip       string
	stops    int
	watch    func(ctx context.Context, onDie func(containerID, sandboxID string)) error
	inspects int
}

func runningState() containerState {
	return containerState{Status: containerStatusRunning, Running: true}
}

func (f *crashBackend) inspectContainer(context.Context, string) (*containerInspect, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inspects++
	state := f.states[0]
	if len(f.states) > 1 {
		f.states = f.states[1:]
	}
	inspect := &containerInspect{ID: crashContainerID, State: state}
	inspect.NetworkSettings.Networks = map[string]struct {
		IPAddress string `json:"IPAddress"`
	}{runnerBridgeNetwork: {IPAddress: f.ip}}
	return inspect, nil
}

func (f *crashBackend) findContainerByLabels(context.Context, ...string) ([]string, error) {
	return []string{crashContainerID}, nil
}

func (f *crashBackend) containerIP(context.Context, string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ip, nil
}

func (f *crashBackend) stopContainer(context.Context, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stops++
	return nil
}

func (f *crashBackend) watchContainerDeaths(ctx context.Context, onDie func(string, string)) error {
	if f.watch == nil {
		return errors.New("unexpected watchContainerDeaths")
	}
	return f.watch(ctx, onDie)
}

func (f *crashBackend) startContainer(context.Context, string) error { return nil }
func (f *crashBackend) ping(context.Context) error                   { return nil }
func (f *crashBackend) removeContainer(context.Context, string) error {
	return nil
}
func (f *crashBackend) createContainer(context.Context, string, string, string, *ResourceLimits, bool) (string, error) {
	return "", errors.New("unexpected createContainer")
}
func (f *crashBackend) inspectNetwork(context.Context, string) (*networkInspect, error) {
	return nil, errors.New("unexpected inspectNetwork")
}
func (f *crashBackend) listContainersByLabel(context.Context, string, string) ([]string, error) {
	return nil, errors.New("unexpected listContainersByLabel")
}
func (f *crashBackend) pullImage(context.Context, string) error { return nil }
func (f *crashBackend) run(context.Context, ...string) (string, error) {
	return "", errors.New("unexpected run")
}

// newCrashRuntime returns a runtime over backend with network policy and daemon
// readiness stubbed, recording the addresses the policy was applied for. The
// addresses are the point: after a restart the rules have to follow the container.
func newCrashRuntime(t *testing.T, backend *crashBackend) (*Runtime, *[]string) {
	t.Helper()
	m := newRuntime(&config.Config{}, Config{}, backend)
	policyIPs := &[]string{}
	m.applyPolicy = func(_, _, sourceIP, _ string, _ int) error {
		*policyIPs = append(*policyIPs, sourceIP)
		return nil
	}
	m.teardownRules = func(string) error { return nil }
	m.waitForDaemon = func(context.Context, string) error { return nil }
	return m, policyIPs
}

func TestARestartedContainerIsNotServedUntilTheRunnerHasReAdmittedIt(t *testing.T) {
	backend := &crashBackend{states: []containerState{runningState()}, ip: "172.18.0.2"}
	m, policyIPs := newCrashRuntime(t, backend)
	rec := metrics.NewRunnerRecorder(true)
	m.SetMetricsRecorder(rec)

	// The container is running throughout: Docker restarted it before any request
	// arrived, which is what makes the death invisible without the event.
	m.handleContainerDeath(crashContainerID, crashSandboxID)
	backend.ip = "172.18.0.7"

	if _, err := m.DaemonURL(context.Background(), crashSandboxID); !errors.Is(err, ErrSandboxNotRunning) {
		t.Fatalf("DaemonURL() error = %v, want %v; a restarted sandbox must not be proxied to", err, ErrSandboxNotRunning)
	}

	wake, err := m.EnsureSandboxRunning(context.Background(), crashSandboxID)
	if err != nil {
		t.Fatalf("EnsureSandboxRunning() failed: %v", err)
	}
	if !wake.Recovered {
		t.Error("wake did not report the restart, so the request that drove it would be proxied to a sandbox that lost its state")
	}
	if want := []string{"172.18.0.7"}; len(*policyIPs) != 1 || (*policyIPs)[0] != want[0] {
		t.Errorf("network policy applied for %v, want %v: the rules have to follow the container's new address", *policyIPs, want)
	}

	// Re-admitted: from here the sandbox is an ordinary one again, and the restart is
	// reported once rather than to every later request.
	url, err := m.DaemonURL(context.Background(), crashSandboxID)
	if err != nil {
		t.Fatalf("DaemonURL() after recovery failed: %v", err)
	}
	if want := "http://172.18.0.7:8081"; url != want {
		t.Errorf("DaemonURL() = %q, want %q", url, want)
	}
	again, err := m.EnsureSandboxRunning(context.Background(), crashSandboxID)
	if err != nil {
		t.Fatalf("second EnsureSandboxRunning() failed: %v", err)
	}
	if again.Recovered {
		t.Error("the restart was reported twice")
	}

	if got := rec.GuestDeathCount(); got != 1 {
		t.Errorf("guest death metric = %v, want 1", got)
	}
	if got := rec.RecoveryCount(true); got != 1 {
		t.Errorf("recovery metric = %v, want 1", got)
	}
}

func TestAWakeThatCannotRepairARestartedSandboxLeavesItForTheNextRequest(t *testing.T) {
	backend := &crashBackend{states: []containerState{runningState()}, ip: "172.18.0.2"}
	m, _ := newCrashRuntime(t, backend)
	rec := metrics.NewRunnerRecorder(true)
	m.SetMetricsRecorder(rec)
	m.applyPolicy = func(string, string, string, string, int) error {
		return errors.New("iptables failed")
	}

	m.handleContainerDeath(crashContainerID, crashSandboxID)

	wake, err := m.EnsureSandboxRunning(context.Background(), crashSandboxID)
	if err == nil {
		t.Fatal("expected the wake to fail")
	}
	if !wake.Recovered {
		t.Error("a failed recovery did not report itself as one, so it would be metered as an ordinary wake")
	}
	if got := rec.RecoveryCount(false); got != 1 {
		t.Errorf("failed recovery metric = %v, want 1", got)
	}
	if _, err := m.DaemonURL(context.Background(), crashSandboxID); !errors.Is(err, ErrSandboxNotRunning) {
		t.Errorf("DaemonURL() error = %v, want %v; the sandbox is still unrepaired", err, ErrSandboxNotRunning)
	}
}

func TestAWakeWaitsOutDockersRestartRatherThanFailingTheRequest(t *testing.T) {
	backend := &crashBackend{
		states: []containerState{
			{Status: containerStatusRestarting, Restarting: true},
			{Status: containerStatusRestarting, Restarting: true},
			runningState(),
		},
		ip: "172.18.0.2",
	}
	m, policyIPs := newCrashRuntime(t, backend)

	m.handleContainerDeath(crashContainerID, crashSandboxID)

	wake, err := m.EnsureSandboxRunning(context.Background(), crashSandboxID)
	if err != nil {
		t.Fatalf("EnsureSandboxRunning() failed: %v", err)
	}
	if !wake.Recovered {
		t.Error("wake did not report the restart")
	}
	if len(*policyIPs) != 1 {
		t.Errorf("network policy applied %d times, want 1", len(*policyIPs))
	}
}

func TestStopsAndDeletesTheRunnerAsksForAreNotCrashes(t *testing.T) {
	tests := map[string]func(*Runtime) error{
		"stop": func(m *Runtime) error {
			return m.StopSandboxContainer(context.Background(), crashSandboxID)
		},
		"delete": func(m *Runtime) error {
			return m.DeleteSandbox(context.Background(), crashSandboxID)
		},
		"wake failure cleanup": func(m *Runtime) error {
			m.cleanupWakeFailure(crashContainerID)
			return nil
		},
	}
	for name, stop := range tests {
		t.Run(name, func(t *testing.T) {
			backend := &crashBackend{states: []containerState{runningState()}, ip: "172.18.0.2"}
			m, _ := newCrashRuntime(t, backend)
			rec := metrics.NewRunnerRecorder(true)
			m.SetMetricsRecorder(rec)

			if err := stop(m); err != nil {
				t.Fatalf("stop failed: %v", err)
			}
			// Docker reports the death of a container the runner stopped exactly as it
			// reports a crash; only the runner knows which it asked for.
			m.handleContainerDeath(crashContainerID, crashSandboxID)

			if m.wasRestarted(crashSandboxID) {
				t.Error("a deliberate stop was read as a crash, which would refuse the next request with a 409")
			}
			if got := rec.GuestDeathCount(); got != 0 {
				t.Errorf("guest death metric = %v, want 0", got)
			}
		})
	}
}

func TestAStopThatNeverDiedDoesNotMaskALaterCrash(t *testing.T) {
	backend := &crashBackend{states: []containerState{runningState()}, ip: "172.18.0.2"}
	m, _ := newCrashRuntime(t, backend)

	// A stop whose death never arrives — the stop failed, or the container was already
	// exited when it was removed — leaves a mark behind, and Docker reuses nothing but
	// the runner would keep it forever. Aged past its TTL it stops excusing deaths.
	m.expectStop(crashContainerID)
	m.mu.Lock()
	m.expectedStops[crashContainerID] = time.Now().Add(-2 * expectedStopTTL)
	m.mu.Unlock()

	m.handleContainerDeath(crashContainerID, crashSandboxID)
	if !m.wasRestarted(crashSandboxID) {
		t.Error("a stop from another lifetime swallowed a real crash")
	}
	m.mu.Lock()
	_, kept := m.expectedStops[crashContainerID]
	m.mu.Unlock()
	if kept {
		t.Error("the expired stop was kept, so the map grows for every stop that never died")
	}
}

func TestTheDeathWatcherReconnectsUntilTheRunnerStops(t *testing.T) {
	connects := make(chan struct{}, 8)
	backend := &crashBackend{
		states: []containerState{runningState()},
		watch: func(_ context.Context, onDie func(string, string)) error {
			select {
			case connects <- struct{}{}:
			default:
			}
			onDie(crashContainerID, crashSandboxID)
			return errors.New("stream broke")
		},
	}
	m, _ := newCrashRuntime(t, backend)
	m.watchBackoff = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		m.watchGuestDeaths(ctx)
		close(done)
	}()

	// Two connects: losing the event stream is silent, so the runner has to come back
	// to it rather than stop reporting crashes for the rest of its life.
	for i := range 2 {
		select {
		case <-connects:
		case <-time.After(5 * time.Second):
			t.Fatalf("watcher did not connect %d time(s)", i+1)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher did not stop with its context")
	}
	if !m.wasRestarted(crashSandboxID) {
		t.Error("a death read from the stream was not recorded against its sandbox")
	}
}

var _ dockerBackend = (*crashBackend)(nil)
var _ runnerruntime.Runtime = (*Runtime)(nil)
