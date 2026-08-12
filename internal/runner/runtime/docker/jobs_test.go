package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/n8n-io/sandbox-service/internal/daemon"
	"github.com/n8n-io/sandbox-service/internal/runner/config"
)

// fakeJobBackend records the job verbs the run goroutine calls, in order.
type fakeJobBackend struct {
	mu     sync.Mutex
	events []string

	containerID string
	exitCode    int
	stdout      string
	stderr      string

	pullErr      error
	createErr    error
	copyToErr    error
	startErr     error
	copyFromOut  []byte
	copyFromErr  error
	removedID    string
	copiedFrom   string
	copiedToArgs [3]string

	// holdStreams makes startAttached return pipes that stay open until
	// killContainer closes them, standing in for a container that never exits.
	holdStreams bool
	streamWs    []io.Closer

	// pullEntered is closed when pullImage starts; pullGate blocks it until closed.
	pullEntered chan struct{}
	pullGate    chan struct{}
}

func (f *fakeJobBackend) record(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, name)
}

func (f *fakeJobBackend) recorded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.events...)
}

func (f *fakeJobBackend) ping(context.Context) error { return errors.New("unexpected ping") }

func (f *fakeJobBackend) createContainer(context.Context, string, string, string, *ResourceLimits, bool) (string, error) {
	return "", errors.New("unexpected createContainer")
}

func (f *fakeJobBackend) startContainer(context.Context, string) error {
	return errors.New("unexpected startContainer")
}

func (f *fakeJobBackend) stopContainer(context.Context, string) error {
	return errors.New("unexpected stopContainer")
}

func (f *fakeJobBackend) removeContainer(_ context.Context, containerID string) error {
	f.record("removeContainer")
	f.mu.Lock()
	f.removedID = containerID
	f.mu.Unlock()
	return nil
}

func (f *fakeJobBackend) containerIP(context.Context, string) (string, error) {
	f.record("containerIP")
	return "172.18.0.5", nil
}

func (f *fakeJobBackend) inspectContainer(context.Context, string) (*containerInspect, error) {
	f.record("inspect")
	return &containerInspect{
		ID:    f.containerID,
		State: containerState{Status: containerStatusExited, ExitCode: f.exitCode},
	}, nil
}

func (f *fakeJobBackend) inspectNetwork(context.Context, string) (*networkInspect, error) {
	return nil, errors.New("unexpected inspectNetwork")
}

func (f *fakeJobBackend) listContainersByLabel(context.Context, string, string) ([]string, error) {
	return nil, errors.New("unexpected listContainersByLabel")
}

func (f *fakeJobBackend) findContainerByLabels(context.Context, ...string) ([]string, error) {
	return nil, errors.New("unexpected findContainerByLabels")
}

func (f *fakeJobBackend) pullImage(context.Context, string) error {
	f.record("pull")
	// Lets a test act (e.g. delete the job) while the pull is still in flight.
	if f.pullEntered != nil {
		close(f.pullEntered)
	}
	if f.pullGate != nil {
		<-f.pullGate
	}
	return f.pullErr
}

func (f *fakeJobBackend) run(context.Context, ...string) (string, error) {
	return "", errors.New("unexpected run")
}

func (f *fakeJobBackend) createJobContainer(_ context.Context, _, _, _ string, _ []string, _ map[string]string, _ *ResourceLimits, _ bool) (string, error) {
	f.record("createJob")
	if f.createErr != nil {
		return "", f.createErr
	}
	return f.containerID, nil
}

func (f *fakeJobBackend) copyToContainer(_ context.Context, localPath, containerID, destPath string) error {
	f.record("cp")
	f.mu.Lock()
	f.copiedToArgs = [3]string{localPath, containerID, destPath}
	f.mu.Unlock()
	return f.copyToErr
}

func (f *fakeJobBackend) copyFromContainer(_ context.Context, _, srcPath string) ([]byte, error) {
	f.record("cpFrom")
	f.mu.Lock()
	f.copiedFrom = srcPath
	f.mu.Unlock()
	return f.copyFromOut, f.copyFromErr
}

func (f *fakeJobBackend) startAttached(context.Context, string) (io.ReadCloser, io.ReadCloser, func() error, error) {
	f.record("startAttached")
	if f.startErr != nil {
		return nil, nil, nil, f.startErr
	}
	if f.holdStreams {
		outR, outW := io.Pipe()
		errR, errW := io.Pipe()
		f.mu.Lock()
		f.streamWs = []io.Closer{outW, errW}
		f.mu.Unlock()
		return outR, errR, func() error { return nil }, nil
	}
	stdout := io.NopCloser(strings.NewReader(f.stdout))
	stderr := io.NopCloser(strings.NewReader(f.stderr))
	return stdout, stderr, func() error { return nil }, nil
}

func (f *fakeJobBackend) killContainer(context.Context, string) error {
	f.record("kill")
	f.mu.Lock()
	writers := f.streamWs
	f.streamWs = nil
	f.mu.Unlock()
	for _, w := range writers {
		_ = w.Close()
	}
	return nil
}

func newJobRuntime(t *testing.T, backend dockerBackend) *Runtime {
	t.Helper()
	m := newRuntime(&config.Config{DataDir: t.TempDir()}, Config{}, backend)
	t.Cleanup(m.jobExecs.Close)
	m.applyPolicy = func(string, string, string, int) error { return nil }
	m.teardownRules = func(string) error { return nil }
	return m
}

// waitForJobStatus polls until the run goroutine reaches a terminal state.
func waitForJobStatus(t *testing.T, m *Runtime, id string) *JobRecord {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rec, err := m.GetJob(id)
		if err != nil {
			t.Fatalf("GetJob() failed: %v", err)
		}
		if rec.FinishedAt != nil {
			return rec
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %s never finished", id)
	return nil
}

// waitFor polls cond until it holds, for assertions on background goroutines
// that leave no record behind to poll via GetJob.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// recordTeardowns captures the container IDs whose network rules were removed.
func recordTeardowns(m *Runtime) chan string {
	torn := make(chan string, 8)
	m.teardownRules = func(containerID string) error {
		torn <- containerID
		return nil
	}
	return torn
}

func terminalEvents(events []daemon.Response) []daemon.Response {
	var terminal []daemon.Response
	for _, e := range events {
		if e.Type == daemon.ResponseTypeExit || e.Type == daemon.ResponseTypeError {
			terminal = append(terminal, e)
		}
	}
	return terminal
}

func decodeEvents(t *testing.T, ex *daemon.Execution) []daemon.Response {
	t.Helper()
	raw, ok := ex.Snapshot(nil)
	if !ok {
		t.Fatal("event history was trimmed")
	}
	events := make([]daemon.Response, 0, len(raw))
	for _, line := range raw {
		var resp daemon.Response
		if err := json.Unmarshal(line, &resp); err != nil {
			t.Fatalf("decode event: %v", err)
		}
		events = append(events, resp)
	}
	return events
}

const testJobID = "job-abcdef123456"

func TestJobHappyPathLifecycle(t *testing.T) {
	backend := &fakeJobBackend{containerID: "container-job-1", stdout: "hello\nworld\n", stderr: "warn\n"}
	m := newJobRuntime(t, backend)

	rec, err := m.CreateJob(testJobID, JobSpec{Image: "alpine:3.20"})
	if err != nil {
		t.Fatalf("CreateJob() failed: %v", err)
	}
	if rec.Status != jobStatusStaging || rec.CreatedAt == 0 {
		t.Fatalf("CreateJob() record = %+v, want staging with created_at", rec)
	}

	if err := m.StageJobFile(testJobID, "src/main.py", strings.NewReader("print(1)\n"), 1024); err != nil {
		t.Fatalf("StageJobFile() failed: %v", err)
	}
	staged := filepath.Join(m.jobStagingDir(testJobID), "src", "main.py")
	if data, err := os.ReadFile(staged); err != nil || string(data) != "print(1)\n" {
		t.Fatalf("staged file = %q, err = %v", data, err)
	}

	ex, err := m.StartJob(testJobID)
	if err != nil {
		t.Fatalf("StartJob() failed: %v", err)
	}
	if _, err := m.StartJob(testJobID); !errors.Is(err, ErrJobNotStaging) {
		t.Fatalf("second StartJob() error = %v, want ErrJobNotStaging", err)
	}
	if err := m.StageJobFile(testJobID, "late.txt", strings.NewReader("x"), 1024); !errors.Is(err, ErrJobNotStaging) {
		t.Fatalf("StageJobFile() after start error = %v, want ErrJobNotStaging", err)
	}

	final := waitForJobStatus(t, m, testJobID)

	wantEvents := []string{"pull", "createJob", "cp", "startAttached", "containerIP", "inspect"}
	if got := backend.recorded(); !reflect.DeepEqual(got, wantEvents) {
		t.Fatalf("docker calls = %v, want %v", got, wantEvents)
	}
	if got := backend.copiedToArgs; got[0] != m.jobStagingDir(testJobID)+"/." || got[2] != jobWorkDir {
		t.Fatalf("copyToContainer args = %v, want staging dir with trailing /. into %s", got, jobWorkDir)
	}

	if final.Status != jobStatusExited || final.ExitCode == nil || *final.ExitCode != 0 || final.TimedOut {
		t.Fatalf("final record = %+v, want exited with exit code 0", final)
	}
	if final.StartedAt == nil || final.FinishedAt == nil || final.Error != nil {
		t.Fatalf("final record = %+v, want started/finished timestamps and no error", final)
	}

	events := decodeEvents(t, ex)
	if len(events) == 0 {
		t.Fatal("no events recorded")
	}
	if events[0].Type != daemon.ResponseTypePulling || events[0].Data != "alpine:3.20" {
		t.Fatalf("first event = %+v, want pulling alpine:3.20", events[0])
	}
	if events[1].Type != daemon.ResponseTypeStarted {
		t.Fatalf("second event = %+v, want started", events[1])
	}
	last := events[len(events)-1]
	if last.Type != daemon.ResponseTypeExit || last.ExitCode != 0 || last.Success == nil || !*last.Success {
		t.Fatalf("last event = %+v, want successful exit", last)
	}
	if last.TimedOut == nil || *last.TimedOut || last.Killed == nil || *last.Killed {
		t.Fatalf("last event = %+v, want timed_out/killed false", last)
	}

	var stdout, stderr strings.Builder
	for _, e := range events {
		switch e.Type {
		case daemon.ResponseTypeStdout:
			stdout.WriteString(e.Data)
		case daemon.ResponseTypeStderr:
			stderr.WriteString(e.Data)
		case daemon.ResponseTypeError:
			t.Fatalf("unexpected error event: %+v", e)
		}
	}
	if stdout.String() != "hello\nworld\n" || stderr.String() != "warn\n" {
		t.Fatalf("streams = %q / %q, want hello\\nworld\\n / warn\\n", stdout.String(), stderr.String())
	}

	// Outputs are only readable once the container exited.
	backend.copyFromOut = tarWithFile(t, "out.json", `{"ok":true}`)
	data, err := m.JobOutputFile(context.Background(), testJobID, "out.json")
	if err != nil {
		t.Fatalf("JobOutputFile() failed: %v", err)
	}
	if string(data) != `{"ok":true}` {
		t.Fatalf("JobOutputFile() = %q, want the untarred file bytes", data)
	}
	if backend.copiedFrom != jobWorkDir+"/out.json" {
		t.Fatalf("copyFromContainer src = %q, want %s/out.json", backend.copiedFrom, jobWorkDir)
	}

	if err := m.DeleteJob(context.Background(), testJobID); err != nil {
		t.Fatalf("DeleteJob() failed: %v", err)
	}
	if backend.removedID != "container-job-1" {
		t.Fatalf("removed container = %q, want container-job-1", backend.removedID)
	}
	if _, err := os.Stat(m.jobStagingDir(testJobID)); !os.IsNotExist(err) {
		t.Fatalf("staging dir still present after delete: %v", err)
	}
	if _, err := m.GetJob(testJobID); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("GetJob() after delete error = %v, want ErrJobNotFound", err)
	}
	if _, err := m.JobEvents(testJobID); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("JobEvents() after delete error = %v, want ErrJobNotFound", err)
	}
	if err := m.DeleteJob(context.Background(), testJobID); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("second DeleteJob() error = %v, want ErrJobNotFound", err)
	}
}

func TestJobImagePullFailureIsTerminal(t *testing.T) {
	backend := &fakeJobBackend{containerID: "container-job-2", pullErr: errors.New("manifest unknown")}
	m := newJobRuntime(t, backend)

	if _, err := m.CreateJob(testJobID, JobSpec{Image: "ghost:1"}); err != nil {
		t.Fatalf("CreateJob() failed: %v", err)
	}
	ex, err := m.StartJob(testJobID)
	if err != nil {
		t.Fatalf("StartJob() failed: %v", err)
	}

	final := waitForJobStatus(t, m, testJobID)
	if final.Status != jobStatusError || final.Error == nil || *final.Error != "manifest unknown" {
		t.Fatalf("final record = %+v, want error status carrying the pull message", final)
	}
	if got := backend.recorded(); !reflect.DeepEqual(got, []string{"pull"}) {
		t.Fatalf("docker calls = %v, want only pull", got)
	}

	// Wire contract: machine code in `code`, human message in `data`.
	events := decodeEvents(t, ex)
	last := events[len(events)-1]
	if last.Type != daemon.ResponseTypeError || last.Code != jobErrorCodeImagePullFailed || last.Data != "manifest unknown" {
		t.Fatalf("last event = %+v, want error event with %s code", last, jobErrorCodeImagePullFailed)
	}
	if last.Error != "" {
		t.Fatalf("last event = %+v, want the exec-stream error field unset", last)
	}
	for _, e := range events {
		if e.Type == daemon.ResponseTypeExit {
			t.Fatalf("unexpected exit event alongside error: %+v", e)
		}
	}

	// A job that failed before its container existed still cleans up.
	if err := m.DeleteJob(context.Background(), testJobID); err != nil {
		t.Fatalf("DeleteJob() failed: %v", err)
	}
}

// A delete that lands while the image is still pulling reads an empty container
// ID, so the run goroutine must clean up the container it goes on to create.
// Otherwise it is orphaned: no record for the reaper, no managed label for boot
// reconciliation, and its name prefix blocks later jobs.
func TestDeleteJobDuringImagePullRemovesTheRacedContainer(t *testing.T) {
	backend := &fakeJobBackend{
		containerID: "container-job-raced",
		pullEntered: make(chan struct{}),
		pullGate:    make(chan struct{}),
	}
	m := newJobRuntime(t, backend)
	torn := recordTeardowns(m)

	if _, err := m.CreateJob(testJobID, JobSpec{Image: "alpine:3.20"}); err != nil {
		t.Fatalf("CreateJob() failed: %v", err)
	}
	ex, err := m.StartJob(testJobID)
	if err != nil {
		t.Fatalf("StartJob() failed: %v", err)
	}

	<-backend.pullEntered
	if err := m.DeleteJob(context.Background(), testJobID); err != nil {
		t.Fatalf("DeleteJob() during pull failed: %v", err)
	}
	close(backend.pullGate)

	waitFor(t, "the raced container to be removed", func() bool {
		backend.mu.Lock()
		defer backend.mu.Unlock()
		return backend.removedID == "container-job-raced"
	})
	select {
	case got := <-torn:
		if got != "container-job-raced" {
			t.Fatalf("teardownRules containerID = %q, want container-job-raced", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("network rules were never torn down for the raced container")
	}

	wantCalls := []string{"pull", "createJob", "removeContainer"}
	if got := backend.recorded(); !reflect.DeepEqual(got, wantCalls) {
		t.Fatalf("docker calls = %v, want %v", got, wantCalls)
	}

	// The record is gone, so the run goroutine must not report a lifecycle it no
	// longer owns.
	if got := terminalEvents(decodeEvents(t, ex)); len(got) != 0 {
		t.Fatalf("terminal events = %+v, want none after delete", got)
	}
	if _, err := m.GetJob(testJobID); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("GetJob() error = %v, want ErrJobNotFound", err)
	}
}

// An errored job never reaches `exited`, so its outputs are unfetchable: any
// container already created is removed instead of waiting for the reaper.
func TestJobInternalFailureIsTerminalAndCleansUpContainer(t *testing.T) {
	const containerID = "container-job-internal"
	tests := []struct {
		name          string
		createErr     error
		copyToErr     error
		startErr      error
		wantCalls     []string
		wantTeardown  bool
		wantErrorText string
	}{
		{
			name:          "create fails",
			createErr:     errors.New("cgroup setup refused"),
			wantCalls:     []string{"pull", "createJob"},
			wantErrorText: "cgroup setup refused",
		},
		{
			name:          "staging copy fails",
			copyToErr:     errors.New("cp: permission denied"),
			wantCalls:     []string{"pull", "createJob", "cp", "removeContainer"},
			wantTeardown:  true,
			wantErrorText: "cp: permission denied",
		},
		{
			name:          "attach fails",
			startErr:      errors.New("attach refused"),
			wantCalls:     []string{"pull", "createJob", "cp", "startAttached", "removeContainer"},
			wantTeardown:  true,
			wantErrorText: "attach refused",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend := &fakeJobBackend{
				containerID: containerID,
				createErr:   tc.createErr,
				copyToErr:   tc.copyToErr,
				startErr:    tc.startErr,
			}
			m := newJobRuntime(t, backend)
			torn := recordTeardowns(m)

			if _, err := m.CreateJob(testJobID, JobSpec{Image: "alpine:3.20"}); err != nil {
				t.Fatalf("CreateJob() failed: %v", err)
			}
			ex, err := m.StartJob(testJobID)
			if err != nil {
				t.Fatalf("StartJob() failed: %v", err)
			}

			final := waitForJobStatus(t, m, testJobID)
			if final.Status != jobStatusError || final.Error == nil || *final.Error != tc.wantErrorText {
				t.Fatalf("final record = %+v, want error status carrying %q", final, tc.wantErrorText)
			}
			if final.ExitCode != nil || final.TimedOut {
				t.Fatalf("final record = %+v, want no exit code and timed_out false", final)
			}

			terminal := terminalEvents(decodeEvents(t, ex))
			if len(terminal) != 1 {
				t.Fatalf("terminal events = %+v, want exactly one", terminal)
			}
			last := terminal[0]
			if last.Type != daemon.ResponseTypeError || last.Code != jobErrorCodeInternal || last.Data != tc.wantErrorText {
				t.Fatalf("terminal event = %+v, want %s code with %q", last, jobErrorCodeInternal, tc.wantErrorText)
			}

			if got := backend.recorded(); !reflect.DeepEqual(got, tc.wantCalls) {
				t.Fatalf("docker calls = %v, want %v", got, tc.wantCalls)
			}
			select {
			case got := <-torn:
				if !tc.wantTeardown {
					t.Fatalf("network rules torn down for %q, want no teardown", got)
				}
				if got != containerID {
					t.Fatalf("teardownRules containerID = %q, want %q", got, containerID)
				}
			default:
				if tc.wantTeardown {
					t.Fatal("network rules were never torn down")
				}
			}
		})
	}
}

func TestJobTimeoutKillsContainerAndReportsTimedOut(t *testing.T) {
	backend := &fakeJobBackend{containerID: "container-job-timeout", holdStreams: true, exitCode: 137}
	m := newJobRuntime(t, backend)

	if _, err := m.CreateJob(testJobID, JobSpec{Image: "alpine:3.20", TimeoutMs: 1}); err != nil {
		t.Fatalf("CreateJob() failed: %v", err)
	}
	ex, err := m.StartJob(testJobID)
	if err != nil {
		t.Fatalf("StartJob() failed: %v", err)
	}

	final := waitForJobStatus(t, m, testJobID)
	if final.Status != jobStatusExited || !final.TimedOut || final.ExitCode == nil || *final.ExitCode != 137 {
		t.Fatalf("final record = %+v, want exited/timed out with exit code 137", final)
	}

	events := decodeEvents(t, ex)
	last := events[len(events)-1]
	if last.Type != daemon.ResponseTypeExit || last.ExitCode != 137 {
		t.Fatalf("last event = %+v, want exit 137", last)
	}
	if last.Success == nil || *last.Success {
		t.Fatalf("last event = %+v, want success false", last)
	}
	if last.TimedOut == nil || !*last.TimedOut || last.Killed == nil || !*last.Killed {
		t.Fatalf("last event = %+v, want timed_out/killed true", last)
	}

	// The container is killed, never the attached CLI process, which would orphan it.
	var sawKill bool
	for _, e := range backend.recorded() {
		if e == "kill" {
			sawKill = true
		}
	}
	if !sawKill {
		t.Fatalf("docker calls = %v, want a kill", backend.recorded())
	}
}

func TestJobOutputFileRequiresFinishedJob(t *testing.T) {
	backend := &fakeJobBackend{containerID: "container-job-3"}
	m := newJobRuntime(t, backend)

	if _, err := m.CreateJob(testJobID, JobSpec{Image: "alpine:3.20"}); err != nil {
		t.Fatalf("CreateJob() failed: %v", err)
	}
	if _, err := m.JobOutputFile(context.Background(), testJobID, "out.json"); !errors.Is(err, ErrJobNotFinished) {
		t.Fatalf("JobOutputFile() on staging job error = %v, want ErrJobNotFinished", err)
	}
	if _, err := m.JobOutputFile(context.Background(), testJobID, "../etc/passwd"); err == nil {
		t.Fatal("JobOutputFile() accepted a traversing path")
	}
	if _, err := m.JobOutputFile(context.Background(), "nope-nope-nope", "out.json"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("JobOutputFile() unknown job error = %v, want ErrJobNotFound", err)
	}
}

func TestJobOutputFileMapsDockerNotFound(t *testing.T) {
	backend := &fakeJobBackend{
		containerID: "container-job-4",
		copyFromErr: errors.New(`docker cp: Error: No such container:path: container-job-4:/n8n/missing.json`),
	}
	m := newJobRuntime(t, backend)

	if _, err := m.CreateJob(testJobID, JobSpec{Image: "alpine:3.20"}); err != nil {
		t.Fatalf("CreateJob() failed: %v", err)
	}
	if _, err := m.StartJob(testJobID); err != nil {
		t.Fatalf("StartJob() failed: %v", err)
	}
	waitForJobStatus(t, m, testJobID)

	if _, err := m.JobOutputFile(context.Background(), testJobID, "missing.json"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("JobOutputFile() error = %v, want ErrJobNotFound", err)
	}
}

func TestStageJobFileRejectsOversizedBody(t *testing.T) {
	m := newJobRuntime(t, &fakeJobBackend{containerID: "container-job-5"})
	if _, err := m.CreateJob(testJobID, JobSpec{Image: "alpine:3.20"}); err != nil {
		t.Fatalf("CreateJob() failed: %v", err)
	}

	err := m.StageJobFile(testJobID, "big.bin", strings.NewReader("0123456789"), 4)
	if err == nil {
		t.Fatal("StageJobFile() accepted a body over the cap")
	}
	if _, statErr := os.Stat(filepath.Join(m.jobStagingDir(testJobID), "big.bin")); !os.IsNotExist(statErr) {
		t.Fatalf("partial file left behind: %v", statErr)
	}
}

func TestCreateJobValidation(t *testing.T) {
	m := newJobRuntime(t, &fakeJobBackend{containerID: "container-job-6"})

	tests := []struct {
		name string
		id   string
		spec JobSpec
	}{
		{name: "empty image", id: testJobID, spec: JobSpec{}},
		{name: "flag-like image", id: testJobID, spec: JobSpec{Image: "--privileged"}},
		{name: "image with space", id: testJobID, spec: JobSpec{Image: "alpine 3.20"}},
		{name: "timeout too large", id: testJobID, spec: JobSpec{Image: "alpine", TimeoutMs: jobMaxTimeoutMs + 1}},
		{name: "negative timeout", id: testJobID, spec: JobSpec{Image: "alpine", TimeoutMs: -1}},
		{name: "short id", id: "abc", spec: JobSpec{Image: "alpine"}},
		{name: "traversing id", id: "../../etc/passwd", spec: JobSpec{Image: "alpine"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := m.CreateJob(tc.id, tc.spec); err == nil {
				t.Fatalf("CreateJob(%q, %+v) succeeded, want error", tc.id, tc.spec)
			}
		})
	}

	if _, err := m.CreateJob(testJobID, JobSpec{Image: "alpine"}); err != nil {
		t.Fatalf("CreateJob() failed: %v", err)
	}
	if _, err := m.CreateJob(testJobID, JobSpec{Image: "alpine"}); err == nil {
		t.Fatal("CreateJob() with a duplicate id succeeded, want error")
	}
}

func TestNormalizeJobSpecDefaultsTimeout(t *testing.T) {
	spec := JobSpec{Image: "alpine"}
	if err := normalizeJobSpec(&spec); err != nil {
		t.Fatalf("normalizeJobSpec() failed: %v", err)
	}
	if spec.TimeoutMs != jobDefaultTimeoutMs {
		t.Fatalf("TimeoutMs = %d, want %d", spec.TimeoutMs, jobDefaultTimeoutMs)
	}
}

func TestValidateJobPath(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{name: "ok", path: "a/b/c.txt"},
		{name: "ok single segment", path: "out.json"},
		{name: "ok dotted name", path: "src/.hidden.env"},
		{name: "empty", path: "", wantErr: true},
		{name: "parent", path: "../x", wantErr: true},
		{name: "absolute", path: "/abs", wantErr: true},
		{name: "nested parent", path: "a/../../x", wantErr: true},
		{name: "weird chars", path: "weird$chars", wantErr: true},
		{name: "space", path: "a b.txt", wantErr: true},
		{name: "newline", path: "a\nb", wantErr: true},
		{name: "backslash", path: `a\..\b`, wantErr: true},
		{name: "tilde", path: "~/.ssh/id_rsa", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateJobPath(tc.path)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateJobPath(%q) error = %v, wantErr = %v", tc.path, err, tc.wantErr)
			}
		})
	}
}

func TestFirstTarFileIgnoresNonRegularEntries(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Name: "dir/", Typeflag: tar.TypeDir, Mode: 0o755}); err != nil {
		t.Fatalf("write dir header: %v", err)
	}
	body := "payload"
	if err := tw.WriteHeader(&tar.Header{Name: "dir/f.txt", Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(body))}); err != nil {
		t.Fatalf("write file header: %v", err)
	}
	if _, err := tw.Write([]byte(body)); err != nil {
		t.Fatalf("write body: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}

	data, err := firstTarFile(buf.Bytes())
	if err != nil {
		t.Fatalf("firstTarFile() failed: %v", err)
	}
	if string(data) != body {
		t.Fatalf("firstTarFile() = %q, want %q", data, body)
	}

	if _, err := firstTarFile(nil); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("firstTarFile(nil) error = %v, want ErrJobNotFound", err)
	}
}

func TestReapJobsSweepsOnlyExpiredFinishedJobs(t *testing.T) {
	backend := &fakeJobBackend{containerID: "container-job-7"}
	m := newJobRuntime(t, backend)

	const freshID = "job-fresh-000001"
	if _, err := m.CreateJob(testJobID, JobSpec{Image: "alpine"}); err != nil {
		t.Fatalf("CreateJob() failed: %v", err)
	}
	if _, err := m.CreateJob(freshID, JobSpec{Image: "alpine"}); err != nil {
		t.Fatalf("CreateJob() failed: %v", err)
	}

	expired, err := m.lookupJob(testJobID)
	if err != nil {
		t.Fatalf("lookupJob() failed: %v", err)
	}
	expired.finish(jobStatusExited, new(int), false, nil)
	expired.mu.Lock()
	expired.finishedAt = time.Now().Add(-2 * jobRetention).Unix()
	expired.mu.Unlock()

	m.reapJobs(context.Background())

	if _, err := m.GetJob(testJobID); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("expired job survived reaping: %v", err)
	}
	if _, err := m.GetJob(freshID); err != nil {
		t.Fatalf("unfinished job was reaped: %v", err)
	}
}

func tarWithFile(t *testing.T, name, body string) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(body))}); err != nil {
		t.Fatalf("write tar header: %v", err)
	}
	if _, err := tw.Write([]byte(body)); err != nil {
		t.Fatalf("write tar body: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}
	return buf.Bytes()
}
