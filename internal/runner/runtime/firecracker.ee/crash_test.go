package firecracker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/n8n-io/sandbox-service/internal/metrics"
	runnerruntime "github.com/n8n-io/sandbox-service/internal/runner/runtime"
)

// guestExitStub records the exit callback the runtime registers for each microVM
// it starts, so a test can report that incarnation's process as gone. Firing a
// specific one is the point: it is how "an old incarnation died" is told apart
// from "the current one died" without a real Firecracker process.
type guestExitStub struct {
	mu    sync.Mutex
	exits []func(error)
}

func (s *guestExitStub) install(rt *Runtime, proc process) {
	rt.deps.start = func(_ context.Context, onExit func(error), _ string, _ ...string) (process, error) {
		s.mu.Lock()
		s.exits = append(s.exits, onExit)
		s.mu.Unlock()
		return proc, nil
	}
}

// fire reports the exit of the nth microVM this runtime started, counting from 0.
func (s *guestExitStub) fire(t *testing.T, n int) {
	t.Helper()
	s.at(t, n)(errors.New("signal: killed"))
}

// fireAsync reports the exit from a goroutine of its own, the way the real process
// watcher does. Needed when the exit is injected from inside a lifecycle
// operation, since handling it waits for that operation's transition claim.
func (s *guestExitStub) fireAsync(t *testing.T, n int) {
	t.Helper()
	onExit := s.at(t, n)
	go onExit(errors.New("signal: killed"))
}

func (s *guestExitStub) at(t *testing.T, n int) func(error) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if n >= len(s.exits) {
		t.Fatalf("microVM %d was never started (%d starts recorded)", n, len(s.exits))
	}
	return s.exits[n]
}

// waitForGuestDeaths blocks until the death handler has counted want deaths. The
// handler runs on the watcher goroutine, so it is not ordered against the caller.
func waitForGuestDeaths(t *testing.T, rec *metrics.RunnerRecorder, want float64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := rec.GuestDeathCount()
		if got >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("guest death metric = %v, want %v", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// countingProcess and countingProxy are the concurrency-safe counterparts of
// fakeProcess and fakeProxy, for the one test that drives two teardowns of the
// same microVM at once. blockFirstStop holds the first Stop until released, which
// is what parks a guest-death teardown mid-flight.
type countingProcess struct {
	kills atomic.Int32
}

func (p *countingProcess) Kill() error {
	p.kills.Add(1)
	return nil
}

type countingProxy struct {
	stops          atomic.Int32
	blockFirstStop chan struct{}
	firstStop      chan struct{}
}

func (p *countingProxy) Stop() error {
	if p.stops.Add(1) == 1 && p.blockFirstStop != nil {
		close(p.firstStop)
		<-p.blockFirstStop
	}
	return nil
}

// Shutdown will not wait for a transition claim it finds held, so it can tear down
// the same microVM a guest-death handler is already tearing down. Both must not
// stop the proxy or kill the process twice, and -race must stay quiet on the
// handles they share.
func TestShutdownRacingGuestDeathTearsDownTheMicroVMOnce(t *testing.T) {
	rt := testRuntimeT(t, 1)
	stubCreateDeps(rt)

	proc := &countingProcess{}
	proxy := &countingProxy{blockFirstStop: make(chan struct{}), firstStop: make(chan struct{})}
	exits := &guestExitStub{}
	exits.install(rt, proc)
	rt.deps.newProxy = func(context.Context, string, string, string) (daemonProxy, error) { return proxy, nil }

	const sandboxID = "sandbox-id-123456"
	if _, err := rt.CreateSandbox(context.Background(), sandboxID, nil); err != nil {
		t.Fatalf("CreateSandbox() failed: %v", err)
	}

	// The guest dies, and its handler is parked inside its own teardown holding the
	// sandbox's claim.
	exits.fireAsync(t, 0)
	select {
	case <-proxy.firstStop:
	case <-time.After(5 * time.Second):
		t.Fatal("guest death handler never reached its teardown")
	}

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		rt.Shutdown(context.Background())
	}()

	// Shutdown has to finish rather than block on the held claim, and the parked
	// teardown then has to finish against a state Shutdown has already deleted.
	close(proxy.blockFirstStop)
	select {
	case <-shutdownDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown blocked on the guest death handler's claim")
	}

	if got := proxy.stops.Load(); got != 1 {
		t.Errorf("daemon proxy stopped %d times, want 1", got)
	}
	if got := proc.kills.Load(); got != 1 {
		t.Errorf("microVM process killed %d times, want 1", got)
	}
	if got := capacityOf(t, rt); got.Used != 0 {
		t.Errorf("capacity used = %d, want the slot back after shutdown", got.Used)
	}
}

// Shutdown decides whether to kill a sandbox's microVM by reading handles that an
// activation has not published yet, and it hands the slot back without waiting for
// the activation's claim. So an activation still running afterwards must not go on
// to finish a guest nothing tracks: jailer's children survive the runner.
func TestShutdownDuringActivationDoesNotLeaveTheMicroVMRunning(t *testing.T) {
	rt := testRuntimeT(t, 1)
	stubCreateDeps(rt)

	proc := &countingProcess{}
	rt.deps.start = func(context.Context, func(error), string, ...string) (process, error) {
		return proc, nil
	}
	proxy := &countingProxy{}
	rt.deps.newProxy = func(context.Context, string, string, string) (daemonProxy, error) { return proxy, nil }

	// Parks the create before it starts the microVM, so shutdown runs while both
	// handles are still nil — the case where shutdown finds nothing to tear down.
	cloneStarted := make(chan struct{})
	allowClone := make(chan struct{})
	rt.deps.cloneGoldenSnapshot = func(context.Context, string, string, string) error {
		close(cloneStarted)
		<-allowClone
		return nil
	}

	const sandboxID = "sandbox-id-123456"
	createErr := make(chan error, 1)
	go func() {
		_, err := rt.CreateSandbox(context.Background(), sandboxID, nil)
		createErr <- err
	}()

	select {
	case <-cloneStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("CreateSandbox never reached the golden snapshot clone")
	}
	rt.Shutdown(context.Background())
	close(allowClone)

	var err error
	select {
	case err = <-createErr:
	case <-time.After(10 * time.Second):
		t.Fatal("CreateSandbox never returned after shutdown")
	}
	if !errors.Is(err, errActivationAbandoned) {
		t.Fatalf("CreateSandbox() error = %v, want errActivationAbandoned", err)
	}
	if got := proc.kills.Load(); got != 1 {
		t.Errorf("microVM process killed %d times, want 1 — a guest nothing tracks was left running", got)
	}
	if got := capacityOf(t, rt); got.Used != 0 {
		t.Errorf("capacity used = %d, want 0 after shutdown", got.Used)
	}
	if _, err := rt.GetSandboxInfo(context.Background(), sandboxID); !errors.Is(err, runnerruntime.ErrSandboxNotFound) {
		t.Errorf("GetSandboxInfo() error = %v, want ErrSandboxNotFound", err)
	}
}

func capacityOf(t *testing.T, rt *Runtime) runnerruntime.Capacity {
	t.Helper()
	capacity, err := rt.Capacity(context.Background())
	if err != nil {
		t.Fatalf("Capacity() failed: %v", err)
	}
	return capacity
}

func TestGuestDeathFreesSlotAndPinsSandboxToColdBoot(t *testing.T) {
	rt := testRuntimeT(t, 1)
	stubCreateDeps(rt)
	rec := metrics.NewRunnerRecorder(true)
	rt.SetMetricsRecorder(rec)

	proc := &fakeProcess{}
	proxy := &fakeProxy{}
	exits := &guestExitStub{}
	exits.install(rt, proc)
	rt.deps.newProxy = func(context.Context, string, string, string) (daemonProxy, error) { return proxy, nil }

	const sandboxID = "sandbox-id-123456"
	if _, err := rt.CreateSandbox(context.Background(), sandboxID, nil); err != nil {
		t.Fatalf("CreateSandbox() failed: %v", err)
	}
	if got := capacityOf(t, rt); got.Used != 1 {
		t.Fatalf("capacity used before crash = %d, want 1", got.Used)
	}

	exits.fire(t, 0)

	if got := capacityOf(t, rt); got.Used != 0 || got.Stopped != 1 {
		t.Fatalf("capacity after crash = %+v, want used 0 and stopped 1", got)
	}
	if !proc.killed {
		t.Fatal("crashed sandbox left its process group unkilled")
	}
	if !proxy.stopped {
		t.Fatal("crashed sandbox left its daemon proxy running")
	}
	if got := rec.GuestDeathCount(); got != 1 {
		t.Fatalf("guest death metric = %v, want 1", got)
	}

	// The crash has to reach a client as "not running", which is what sends the next
	// request down the wake path where recovery happens.
	if _, err := rt.DaemonURL(context.Background(), sandboxID); !errors.Is(err, runnerruntime.ErrSandboxNotRunning) {
		t.Fatalf("DaemonURL() error = %v, want ErrSandboxNotRunning", err)
	}

	// A crashed sandbox is already torn down, so the idle sweeper's stop must
	// succeed instead of failing against a dead API socket on every pass.
	rt.deps.pauseVM = func(context.Context, string) error {
		t.Fatal("StopSandbox paused a crashed sandbox")
		return nil
	}
	if err := rt.StopSandbox(context.Background(), sandboxID); err != nil {
		t.Fatalf("StopSandbox() after crash failed: %v", err)
	}
	if err := rt.DeleteSandbox(context.Background(), sandboxID); err != nil {
		t.Fatalf("DeleteSandbox() after crash failed: %v", err)
	}
}

// A guest that dies mid-wake is the case the generation counter cannot see: the
// wake's rollback bumps the generation before the exit is handled, so the death
// looks like one the runner asked for.
func TestGuestDeathDuringWakePinsSandboxToColdBoot(t *testing.T) {
	rt := testRuntimeT(t, 1)
	stubCreateDeps(rt)
	rec := metrics.NewRunnerRecorder(true)
	rt.SetMetricsRecorder(rec)

	exits := &guestExitStub{}
	exits.install(rt, &fakeProcess{})

	const sandboxID = "sandbox-id-123456"
	if _, err := rt.CreateSandbox(context.Background(), sandboxID, nil); err != nil {
		t.Fatalf("CreateSandbox() failed: %v", err)
	}
	if err := rt.StopSandbox(context.Background(), sandboxID); err != nil {
		t.Fatalf("StopSandbox() failed: %v", err)
	}

	// The restored guest dies while the wake is still in flight, which is why the
	// wake then fails: the probe only gives up once the guest is gone.
	rt.deps.probeDaemon = func(context.Context, string) error {
		exits.fireAsync(t, 1)
		waitForGuestDeaths(t, rec, 1)
		return errors.New("connection refused")
	}
	if _, err := rt.EnsureSandboxRunning(context.Background(), sandboxID); err == nil {
		t.Fatal("EnsureSandboxRunning() succeeded, want a failure with the guest gone")
	}

	// The guest ran against the rootfs after that snapshot was restored, so waking
	// again has to boot the rootfs rather than restore the same snapshot a second
	// time.
	rt.deps.probeDaemon = func(context.Context, string) error { return nil }
	recoversByColdBoot(t, rt, sandboxID)
}

// Recovery replays the sidecar the sandbox was created from, never the one the
// runner is configured with now. An operator who rebuilds or swaps the golden
// snapshot under a running runner would otherwise cold boot the sandboxes that
// predate it with another build's memory, kernel and init= — against a rootfs that
// does not match any of it.
func TestRecoveryReplaysTheBootParametersTheSandboxWasCreatedFrom(t *testing.T) {
	rt := testRuntimeT(t, 1)
	stubCreateDeps(rt)

	created := testBootParams(rt.config)
	created.MemSizeMiB = 512
	rt.deps.loadBootParams = func(string) (*bootParams, error) {
		params := created
		return &params, nil
	}

	exits := &guestExitStub{}
	exits.install(rt, &fakeProcess{})

	const sandboxID = "sandbox-id-123456"
	if _, err := rt.CreateSandbox(context.Background(), sandboxID, nil); err != nil {
		t.Fatalf("CreateSandbox() failed: %v", err)
	}
	exits.fire(t, 0)

	// The golden snapshot is rebuilt with a different flavor while the sandbox is
	// crashed, which is exactly when recovery goes looking for boot parameters.
	rebuilt := testBootParams(rt.config)
	rebuilt.MemSizeMiB = 4096
	rebuilt.BootArgs = strings.Replace(created.BootArgs, "init=/sandbox-daemon", "init=/other-daemon", 1)
	rt.deps.loadBootParams = func(string) (*bootParams, error) {
		params := rebuilt
		return &params, nil
	}

	var booted *bootParams
	rt.deps.coldBoot = func(_ context.Context, _ string, params *bootParams) error {
		booted = params
		return nil
	}
	if _, err := rt.EnsureSandboxRunning(context.Background(), sandboxID); err != nil {
		t.Fatalf("EnsureSandboxRunning() failed: %v", err)
	}
	if booted == nil {
		t.Fatal("recovery never cold booted the sandbox")
	}
	if booted.MemSizeMiB != created.MemSizeMiB {
		t.Errorf("cold boot used mem_size_mib %d, want the %d the sandbox was created with", booted.MemSizeMiB, created.MemSizeMiB)
	}
	if booted.BootArgs != created.BootArgs {
		t.Errorf("cold boot used boot_args %q, want %q", booted.BootArgs, created.BootArgs)
	}
}

// waitForColdBoot blocks until a cold boot has started, which is the point at which
// every caller of the burst is either waiting on it or about to be.
func waitForColdBoot(t *testing.T, coldBoots *atomic.Int32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for coldBoots.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no cold boot started within 5s")
		}
		time.Sleep(time.Millisecond)
	}
}

// A crash strands every request in flight, and they all arrive at the wake path at
// once. One recovery serves them, and each of them has to be told about it: a waiter
// that got a plain success would be proxied into a sandbox that had lost its state
// while the request next to it got the 409.
func TestConcurrentRequestsAfterACrashShareOneRecoveryAndAllLearnOfIt(t *testing.T) {
	rt := testRuntimeT(t, 1)
	stubCreateDeps(rt)

	exits := &guestExitStub{}
	exits.install(rt, &fakeProcess{})

	const sandboxID = "sandbox-id-123456"
	if _, err := rt.CreateSandbox(context.Background(), sandboxID, nil); err != nil {
		t.Fatalf("CreateSandbox() failed: %v", err)
	}
	exits.fire(t, 0)

	// Hold the cold boot until every caller is waiting, so they coalesce rather than
	// arriving one after another and each starting its own wake.
	const callers = 8
	var coldBoots atomic.Int32
	release := make(chan struct{})
	rt.deps.coldBoot = func(context.Context, string, *bootParams) error {
		coldBoots.Add(1)
		<-release
		return nil
	}

	var wg sync.WaitGroup
	results := make([]runnerruntime.WakeResult, callers)
	errs := make([]error, callers)
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = rt.EnsureSandboxRunning(context.Background(), sandboxID)
		}()
	}
	waitForColdBoot(t, &coldBoots)
	close(release)
	wg.Wait()

	for i := range callers {
		if errs[i] != nil {
			t.Fatalf("caller %d failed: %v", i, errs[i])
		}
		if !results[i].Recovered {
			t.Errorf("caller %d was not told its sandbox had been recovered", i)
		}
	}
	if got := coldBoots.Load(); got != 1 {
		t.Errorf("cold boots = %d, want exactly 1 for the whole burst", got)
	}
}

// A sandbox recovered once is still pinned: a cold boot leaves it with no snapshot
// describing its rootfs, no more than the crash did. So a second crash recovers the
// same way, and each recovery is reported to the request that drove it.
func TestRepeatedCrashesEachRecoverAndEachReportThemselves(t *testing.T) {
	rt := testRuntimeT(t, 1)
	stubCreateDeps(rt)
	rec := metrics.NewRunnerRecorder(true)
	rt.SetMetricsRecorder(rec)

	exits := &guestExitStub{}
	exits.install(rt, &fakeProcess{})

	const sandboxID = "sandbox-id-123456"
	if _, err := rt.CreateSandbox(context.Background(), sandboxID, nil); err != nil {
		t.Fatalf("CreateSandbox() failed: %v", err)
	}
	for generation := 0; generation < 3; generation++ {
		exits.fire(t, generation)
		recoversByColdBoot(t, rt, sandboxID)
		if got := capacityOf(t, rt); got.Used != 1 {
			t.Fatalf("capacity after recovery %d = %+v, want the sandbox back on a slot", generation, got)
		}
	}
	if got := rec.GuestDeathCount(); got != 3 {
		t.Errorf("guest death metric = %v, want one per crash", got)
	}
	if got := rec.RecoveryCount(true); got != 3 {
		t.Errorf("recovery metric = %v, want one per recovery", got)
	}
	if got := rec.RecoveryCount(false); got != 0 {
		t.Errorf("failed recovery metric = %v, want 0", got)
	}
}

// recoversByColdBoot asserts the next wake brings the sandbox back by booting its
// own rootfs, and never touches the snapshot the guest has written past.
func recoversByColdBoot(t *testing.T, rt *Runtime, sandboxID string) {
	t.Helper()
	restores, coldBoots := 0, 0
	rt.deps.loadSnapshot = func(context.Context, string, Config) error {
		restores++
		return nil
	}
	rt.deps.coldBoot = func(_ context.Context, _ string, params *bootParams) error {
		coldBoots++
		if params == nil {
			t.Error("cold boot ran without the boot parameters of the sandbox's snapshot")
		}
		return nil
	}
	wake, err := rt.EnsureSandboxRunning(context.Background(), sandboxID)
	if err != nil {
		t.Fatalf("EnsureSandboxRunning() failed: %v", err)
	}
	if !wake.Recovered {
		t.Error("recovery did not report itself, so the request that drove it would be proxied to a sandbox that had lost its state")
	}
	if restores != 0 {
		t.Errorf("wake restored the stale snapshot %d times, want a cold boot instead", restores)
	}
	if coldBoots != 1 {
		t.Errorf("cold boots = %d, want exactly 1", coldBoots)
	}
	if _, err := rt.DaemonURL(context.Background(), sandboxID); err != nil {
		t.Errorf("DaemonURL() after recovery failed: %v", err)
	}
}

// Same hazard without a death: the wake failed for a reason of its own and killed
// a guest that had been running since the restore.
func TestWakeFailureAfterRestorePinsSandboxToColdBoot(t *testing.T) {
	rt := testRuntimeT(t, 1)
	stubCreateDeps(rt)
	rec := metrics.NewRunnerRecorder(true)
	rt.SetMetricsRecorder(rec)

	const sandboxID = "sandbox-id-123456"
	if _, err := rt.CreateSandbox(context.Background(), sandboxID, nil); err != nil {
		t.Fatalf("CreateSandbox() failed: %v", err)
	}
	if err := rt.StopSandbox(context.Background(), sandboxID); err != nil {
		t.Fatalf("StopSandbox() failed: %v", err)
	}

	rt.deps.probeDaemon = func(context.Context, string) error { return errors.New("connection refused") }
	if _, err := rt.EnsureSandboxRunning(context.Background(), sandboxID); err == nil {
		t.Fatal("EnsureSandboxRunning() succeeded, want the probe failure")
	}
	if got := rec.GuestDeathCount(); got != 0 {
		t.Fatalf("guest death metric = %v, want 0 for a guest the runner killed itself", got)
	}

	rt.deps.probeDaemon = func(context.Context, string) error { return nil }
	recoversByColdBoot(t, rt, sandboxID)
}

// The ambiguous case, and the reason the pin is set before the restore is asked for
// rather than after it reports success: one request both loads the snapshot and
// resumes the guest, so a load that fails may still have let the guest write to the
// rootfs. Restoring the same snapshot again would corrupt it with nothing able to
// tell.
func TestWakeFailureAtRestorePinsSandboxToColdBoot(t *testing.T) {
	rt := testRuntimeT(t, 1)
	stubCreateDeps(rt)

	const sandboxID = "sandbox-id-123456"
	if _, err := rt.CreateSandbox(context.Background(), sandboxID, nil); err != nil {
		t.Fatalf("CreateSandbox() failed: %v", err)
	}
	if err := rt.StopSandbox(context.Background(), sandboxID); err != nil {
		t.Fatalf("StopSandbox() failed: %v", err)
	}

	rt.deps.loadSnapshot = func(context.Context, string, Config) error {
		return errors.New("context deadline exceeded awaiting response headers")
	}
	if _, err := rt.EnsureSandboxRunning(context.Background(), sandboxID); err == nil {
		t.Fatal("EnsureSandboxRunning() succeeded, want the load failure")
	}

	recoversByColdBoot(t, rt, sandboxID)
}

// The other side of that invariant: a wake that never got as far as restoring left
// the snapshot matching the rootfs, so it has to stay wakeable.
func TestWakeFailureBeforeRestoreLeavesSandboxWakeable(t *testing.T) {
	rt := testRuntimeT(t, 1)
	stubCreateDeps(rt)

	const sandboxID = "sandbox-id-123456"
	if _, err := rt.CreateSandbox(context.Background(), sandboxID, nil); err != nil {
		t.Fatalf("CreateSandbox() failed: %v", err)
	}
	if err := rt.StopSandbox(context.Background(), sandboxID); err != nil {
		t.Fatalf("StopSandbox() failed: %v", err)
	}

	rt.deps.start = func(context.Context, func(error), string, ...string) (process, error) {
		return nil, errors.New("jailer refused to start")
	}
	if _, err := rt.EnsureSandboxRunning(context.Background(), sandboxID); err == nil {
		t.Fatal("EnsureSandboxRunning() succeeded, want the jailer failure")
	}

	rt.deps.start = func(context.Context, func(error), string, ...string) (process, error) {
		return &fakeProcess{}, nil
	}
	if _, err := rt.EnsureSandboxRunning(context.Background(), sandboxID); err != nil {
		t.Fatalf("EnsureSandboxRunning() after a failure before the restore: %v", err)
	}
}

func TestIntentionalStopIsNotReportedAsGuestDeath(t *testing.T) {
	rt := testRuntimeT(t, 1)
	stubCreateDeps(rt)
	rec := metrics.NewRunnerRecorder(true)
	rt.SetMetricsRecorder(rec)

	exits := &guestExitStub{}
	exits.install(rt, &fakeProcess{})

	const sandboxID = "sandbox-id-123456"
	if _, err := rt.CreateSandbox(context.Background(), sandboxID, nil); err != nil {
		t.Fatalf("CreateSandbox() failed: %v", err)
	}
	if err := rt.StopSandbox(context.Background(), sandboxID); err != nil {
		t.Fatalf("StopSandbox() failed: %v", err)
	}

	// The stop killed the microVM, so its exit callback fires afterwards. Taking
	// that for a crash would pin every idle-stopped sandbox to cold boot.
	exits.fire(t, 0)

	if got := rec.GuestDeathCount(); got != 0 {
		t.Fatalf("guest death metric = %v, want 0 after an intentional stop", got)
	}
	if _, err := rt.EnsureSandboxRunning(context.Background(), sandboxID); err != nil {
		t.Fatalf("EnsureSandboxRunning() after stop failed: %v", err)
	}
}

func TestStaleGuestExitDoesNotCrashTheCurrentMicroVM(t *testing.T) {
	rt := testRuntimeT(t, 1)
	stubCreateDeps(rt)
	rec := metrics.NewRunnerRecorder(true)
	rt.SetMetricsRecorder(rec)

	exits := &guestExitStub{}
	exits.install(rt, &fakeProcess{})

	const sandboxID = "sandbox-id-123456"
	if _, err := rt.CreateSandbox(context.Background(), sandboxID, nil); err != nil {
		t.Fatalf("CreateSandbox() failed: %v", err)
	}
	if err := rt.StopSandbox(context.Background(), sandboxID); err != nil {
		t.Fatalf("StopSandbox() failed: %v", err)
	}
	if _, err := rt.EnsureSandboxRunning(context.Background(), sandboxID); err != nil {
		t.Fatalf("EnsureSandboxRunning() failed: %v", err)
	}

	// The first incarnation's exit can arrive at any time, including after the
	// sandbox is back up on a new one.
	exits.fire(t, 0)

	if got := capacityOf(t, rt); got.Used != 1 {
		t.Fatalf("capacity after stale exit = %+v, want the woken sandbox still on its slot", got)
	}
	if _, err := rt.DaemonURL(context.Background(), sandboxID); err != nil {
		t.Fatalf("DaemonURL() after stale exit failed: %v", err)
	}
	if got := rec.GuestDeathCount(); got != 0 {
		t.Fatalf("guest death metric = %v, want 0 for a stale exit", got)
	}

	// The incarnation that is actually running still gets detected.
	exits.fire(t, 1)

	if got := rec.GuestDeathCount(); got != 1 {
		t.Fatalf("guest death metric = %v, want 1 after the current microVM died", got)
	}
	recoversByColdBoot(t, rt, sandboxID)
}
