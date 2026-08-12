package docker

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/n8n-io/sandbox-service/internal/daemon"
)

// Job lifecycle states, as reported on the wire.
const (
	jobStatusStaging = "staging"
	jobStatusRunning = "running"
	jobStatusExited  = "exited"
	jobStatusError   = "error"
)

// Machine-readable codes carried in the Data field of a terminal error event.
const (
	jobErrorCodeImagePullFailed = "image_pull_failed"
	jobErrorCodeInternal        = "internal"
)

const (
	jobDefaultTimeoutMs int64 = 300_000
	jobMaxTimeoutMs     int64 = 900_000

	// jobWorkDir is where staged input files land inside the job container and
	// where output files are read back from.
	jobWorkDir = "/n8n"

	// jobIDMinLen keeps the derived container name non-empty after slicing.
	jobIDMinLen = 12
	jobIDMaxLen = 64

	jobRetention    = time.Hour
	jobReapInterval = time.Minute

	// jobNetPolicyPort keeps the ingress chain well-formed. Jobs run arbitrary
	// images with no daemon, so nothing is listening on it.
	jobNetPolicyPort = 1
)

// ErrJobNotFound is returned when a job ID (or a requested output file) does not exist.
var ErrJobNotFound = errors.New("job not found")

// ErrJobNotStaging is returned when an operation requires a job that has not started yet.
var ErrJobNotStaging = errors.New("job is not in staging state")

// ErrJobNotFinished is returned when outputs are requested before the job container exited.
var ErrJobNotFinished = errors.New("job has not finished")

// JobSpec describes a one-shot job container requested by a caller.
type JobSpec struct {
	Image     string            `json:"image"`
	Cmd       []string          `json:"cmd,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
	TimeoutMs int64             `json:"timeout_ms,omitempty"`
}

// JobRecord is the observable state of a job.
type JobRecord struct {
	ID         string  `json:"id"`
	Status     string  `json:"status"` // staging|running|exited|error
	Image      string  `json:"image"`
	ExitCode   *int    `json:"exit_code"`
	TimedOut   bool    `json:"timed_out"`
	Error      *string `json:"error"`
	CreatedAt  int64   `json:"created_at"`
	StartedAt  *int64  `json:"started_at"`
	FinishedAt *int64  `json:"finished_at"`
}

// job is the in-memory registry entry backing a JobRecord.
type job struct {
	mu          sync.Mutex
	id          string
	spec        JobSpec
	status      string
	stagingDir  string
	containerID string
	exitCode    *int
	timedOut    bool
	errMsg      *string
	// deleted is set by DeleteJob so the run goroutine can tell that the record
	// it is working on is gone.
	deleted bool

	createdAt  int64
	startedAt  int64 // 0 = unset
	finishedAt int64 // 0 = unset
}

// Anchoring on an alphanumeric first character also keeps a job ID or image
// reference from being read as a flag by the docker CLI.
var (
	jobIDRe    = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]*$`)
	jobImageRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._/:@-]*$`)
	jobPathRe  = regexp.MustCompile(`^[A-Za-z0-9._/-]+$`)
)

// CreateJob registers a job and prepares its staging directory. The job starts
// in the staging state and runs nothing until StartJob is called.
func (m *Runtime) CreateJob(id string, spec JobSpec) (*JobRecord, error) {
	if err := validateJobID(id); err != nil {
		return nil, err
	}
	if err := normalizeJobSpec(&spec); err != nil {
		return nil, err
	}

	stagingDir := m.jobStagingDir(id)
	if err := os.MkdirAll(stagingDir, 0o777); err != nil {
		return nil, fmt.Errorf("create job staging dir: %w", err)
	}
	// MkdirAll applies the process umask; /n8n inherits this dir's mode via
	// docker cp, so it has to be widened explicitly for unknown container users.
	if err := os.Chmod(stagingDir, 0o777); err != nil {
		return nil, fmt.Errorf("chmod job staging dir: %w", err)
	}

	j := &job{
		id:         id,
		spec:       spec,
		status:     jobStatusStaging,
		stagingDir: stagingDir,
		createdAt:  time.Now().Unix(),
	}

	m.jobsMu.Lock()
	if _, exists := m.jobs[id]; exists {
		m.jobsMu.Unlock()
		return nil, fmt.Errorf("job %s already exists", id)
	}
	m.jobs[id] = j
	m.jobsMu.Unlock()

	return j.record(), nil
}

// StageJobFile writes a caller-supplied input file into the job's staging
// directory. It fails once the job has started.
func (m *Runtime) StageJobFile(id, relPath string, r io.Reader, maxBytes int64) error {
	j, err := m.lookupJob(id)
	if err != nil {
		return err
	}
	if err := validateJobPath(relPath); err != nil {
		return err
	}

	j.mu.Lock()
	staging := j.status == jobStatusStaging
	stagingDir := j.stagingDir
	j.mu.Unlock()
	if !staging {
		return ErrJobNotStaging
	}

	dest := filepath.Join(stagingDir, relPath)
	if err := os.MkdirAll(filepath.Dir(dest), 0o777); err != nil {
		return fmt.Errorf("create job file directory: %w", err)
	}

	f, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		return fmt.Errorf("create job file: %w", err)
	}
	// Read one byte past the cap so an oversized body is detected rather than
	// silently truncated.
	written, copyErr := io.Copy(f, io.LimitReader(r, maxBytes+1))
	closeErr := f.Close()

	switch {
	case copyErr != nil:
		_ = os.Remove(dest)
		return fmt.Errorf("write job file: %w", copyErr)
	case closeErr != nil:
		_ = os.Remove(dest)
		return fmt.Errorf("write job file: %w", closeErr)
	case written > maxBytes:
		_ = os.Remove(dest)
		return fmt.Errorf("job file %q exceeds %d bytes", relPath, maxBytes)
	}
	return nil
}

// StartJob transitions a staging job to running and starts its container in the
// background. It returns the event log the caller can stream.
func (m *Runtime) StartJob(id string) (*daemon.Execution, error) {
	j, err := m.lookupJob(id)
	if err != nil {
		return nil, err
	}

	j.mu.Lock()
	if j.status != jobStatusStaging {
		j.mu.Unlock()
		return nil, ErrJobNotStaging
	}
	j.status = jobStatusRunning
	j.startedAt = time.Now().Unix()
	j.mu.Unlock()

	ex := m.jobExecs.NewExternal(id)
	go m.runJob(j, ex)
	return ex, nil
}

// JobEvents returns the event log of a started job.
func (m *Runtime) JobEvents(id string) (*daemon.Execution, error) {
	if _, err := m.lookupJob(id); err != nil {
		return nil, err
	}
	// A job that was created but never started has no execution yet.
	ex := m.jobExecs.Get(id)
	if ex == nil {
		return nil, ErrJobNotFound
	}
	return ex, nil
}

// GetJob returns the current state of a job.
func (m *Runtime) GetJob(id string) (*JobRecord, error) {
	j, err := m.lookupJob(id)
	if err != nil {
		return nil, err
	}
	return j.record(), nil
}

// JobOutputFile reads a file from the finished job container's work dir and
// returns its raw bytes (the tar envelope docker cp emits is unwrapped here).
func (m *Runtime) JobOutputFile(ctx context.Context, id, relPath string) ([]byte, error) {
	j, err := m.lookupJob(id)
	if err != nil {
		return nil, err
	}
	if err := validateJobPath(relPath); err != nil {
		return nil, err
	}

	j.mu.Lock()
	status, containerID := j.status, j.containerID
	j.mu.Unlock()
	if status != jobStatusExited || containerID == "" {
		return nil, ErrJobNotFinished
	}

	tarBytes, err := m.docker.copyFromContainer(ctx, containerID, jobWorkDir+"/"+relPath)
	if err != nil {
		if isDockerNotFound(err) {
			return nil, fmt.Errorf("%w: output %q", ErrJobNotFound, relPath)
		}
		return nil, fmt.Errorf("read job output: %w", err)
	}

	data, err := firstTarFile(tarBytes)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, relPath)
	}
	return data, nil
}

// DeleteJob removes the job container, its network rules, its staged files and
// its event log. Cleanup is best effort: every step runs even if an earlier one
// fails, and the first error is returned.
func (m *Runtime) DeleteJob(ctx context.Context, id string) error {
	m.jobsMu.Lock()
	j, ok := m.jobs[id]
	if !ok {
		m.jobsMu.Unlock()
		return ErrJobNotFound
	}
	delete(m.jobs, id)
	m.jobsMu.Unlock()

	// Marking the job deleted under the same lock that publishes the container ID
	// is what makes the handoff safe: either the ID is already visible here and
	// this call removes the container, or runJob sees the flag right after
	// creating it and cleans up itself. Otherwise a delete during a cold-image
	// pull would orphan the container with no record and no managed label, so
	// neither the reaper nor boot reconciliation would ever collect it.
	j.mu.Lock()
	j.deleted = true
	containerID, stagingDir := j.containerID, j.stagingDir
	j.mu.Unlock()

	var firstErr error
	if containerID != "" {
		firstErr = m.removeJobContainer(ctx, id, containerID)
	}
	if err := os.RemoveAll(stagingDir); err != nil {
		slog.Warn("remove job staging dir", "job_id", id, "err", err)
		if firstErr == nil {
			firstErr = err
		}
	}
	m.jobExecs.Delete(id)

	return firstErr
}

// removeJobContainer removes a job container and then its network rules. Order
// matters: rules must outlive the container so it cannot run unconfined during
// teardown. Both failures are logged; only the removal error is returned.
func (m *Runtime) removeJobContainer(ctx context.Context, jobID, containerID string) error {
	var err error
	if removeErr := m.docker.removeContainer(ctx, containerID); removeErr != nil {
		slog.Warn("remove job container", "job_id", jobID, "container_id", containerID, "err", removeErr)
		err = removeErr
	}
	if teardownErr := m.teardownRules(containerID); teardownErr != nil {
		slog.Warn("teardown job network rules", "job_id", jobID, "container_id", containerID, "err", teardownErr)
	}
	return err
}

// runJob drives one job container to completion and pushes its lifecycle onto
// the event log. It runs on a detached context: the job outlives the HTTP
// request that started it, so one caller disconnecting must not kill it.
func (m *Runtime) runJob(j *job, ex *daemon.Execution) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Job error events carry the machine code in `code` and the message in
	// `data`; `error` is the exec-stream shape and stays unset here.
	fail := func(code, msg string) {
		// An errored job never reaches the `exited` state, so its outputs are
		// unfetchable: drop the container now rather than parking it for the reaper.
		if containerID := j.currentContainerID(); containerID != "" {
			_ = m.removeJobContainer(ctx, j.id, containerID)
		}
		ex.Append(daemon.Response{Type: daemon.ResponseTypeError, Code: code, Data: msg})
		j.finish(jobStatusError, nil, false, &msg)
	}

	defer func() {
		if r := recover(); r != nil {
			slog.Error("job run panic", "job_id", j.id, "panic", r)
			// Only synthesize a terminal event if none was emitted yet: exactly
			// one of exit/error ends the stream.
			if !j.isFinished() {
				fail(jobErrorCodeInternal, fmt.Sprintf("internal error: %v", r))
			}
		}
	}()

	spec := j.snapshotSpec()

	ex.Append(daemon.Response{Type: daemon.ResponseTypePulling, Data: spec.Image})
	if err := m.docker.pullImage(ctx, spec.Image); err != nil {
		fail(jobErrorCodeImagePullFailed, err.Error())
		return
	}

	containerID, err := m.docker.createJobContainer(ctx, j.id, "job-"+j.id[:jobIDMinLen], spec.Image,
		spec.Cmd, spec.Env, m.defaultLimits(), m.config.EnableCgroups)
	if err != nil {
		fail(jobErrorCodeInternal, err.Error())
		return
	}
	if !j.setContainerIDIfLive(containerID) {
		// DeleteJob got here first and saw no container ID, so it could not remove
		// this one: ownership of the cleanup falls to us. The job record is already
		// gone, so no further events are emitted.
		slog.Info("removing job container created after delete", "job_id", j.id, "container_id", containerID)
		_ = m.removeJobContainer(ctx, j.id, containerID)
		return
	}

	// The trailing "/." copies the staged contents into /n8n rather than nesting
	// them under /n8n/<jobID> when the image already has a /n8n.
	if err := m.docker.copyToContainer(ctx, j.stagingDir+"/.", containerID, jobWorkDir); err != nil {
		fail(jobErrorCodeInternal, err.Error())
		return
	}

	stdout, stderr, wait, err := m.docker.startAttached(ctx, containerID)
	if err != nil {
		fail(jobErrorCodeInternal, err.Error())
		return
	}
	ex.Append(daemon.Response{Type: daemon.ResponseTypeStarted, ExecID: j.id})
	start := time.Now()

	// Network policy is applied after start because the container has no IP
	// before it: the same brief unfiltered window sandbox creation accepts.
	if ip, err := m.docker.containerIP(ctx, containerID); err == nil {
		if err := m.applyPolicy(containerID, ip, m.gatewayIP, jobNetPolicyPort); err != nil {
			slog.Warn("apply job network rules", "job_id", j.id, "container_id", containerID, "err", err)
		}
	} else {
		slog.Warn("inspect job container ip", "job_id", j.id, "container_id", containerID, "err", err)
	}

	// Kill the container, not the CLI process: killing `docker start -a` would
	// leave the container running.
	timer := time.AfterFunc(time.Duration(spec.TimeoutMs)*time.Millisecond, func() {
		j.markTimedOut()
		if err := m.docker.killContainer(context.Background(), containerID); err != nil {
			slog.Warn("kill timed out job container", "job_id", j.id, "container_id", containerID, "err", err)
		}
	})
	defer timer.Stop()

	var wg sync.WaitGroup
	wg.Add(2)
	go pumpToExecution(&wg, ex, stdout, daemon.ResponseTypeStdout)
	go pumpToExecution(&wg, ex, stderr, daemon.ResponseTypeStderr)
	wg.Wait()
	// Both streams closed, so the container is gone: stop the timer before
	// reading timedOut so a job that finished on its own near the deadline is
	// not reported as killed.
	timer.Stop()

	// The CLI's own exit status is unreliable across failure modes (kill, attach
	// errors); the container's recorded exit code is the truth.
	if err := wait(); err != nil {
		slog.Debug("job attach exited with error", "job_id", j.id, "err", err)
	}
	exitCode := 0
	if inspect, err := m.docker.inspectContainer(ctx, containerID); err == nil {
		exitCode = inspect.State.ExitCode
	} else {
		slog.Warn("inspect finished job container", "job_id", j.id, "container_id", containerID, "err", err)
	}

	timedOut := j.wasTimedOut()
	success := exitCode == 0 && !timedOut
	ex.Append(daemon.Response{
		Type:            daemon.ResponseTypeExit,
		ExitCode:        exitCode,
		Success:         &success,
		ExecutionTimeMs: time.Since(start).Milliseconds(),
		TimedOut:        &timedOut,
		Killed:          &timedOut,
	})
	j.finish(jobStatusExited, &exitCode, timedOut, nil)
}

// pumpToExecution forwards one container stream into the event log, one event
// per line, matching how daemon exec chunks output.
func pumpToExecution(wg *sync.WaitGroup, ex *daemon.Execution, r io.Reader, t daemon.ResponseType) {
	defer wg.Done()
	if r == nil {
		return
	}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		ex.Append(daemon.Response{Type: t, Data: sc.Text() + "\n"})
	}
}

// jobReaperLoop sweeps finished jobs (containers, staged files, event logs)
// once their retention window elapses.
func (m *Runtime) jobReaperLoop() {
	ticker := time.NewTicker(jobReapInterval)
	defer ticker.Stop()
	for range ticker.C {
		m.reapJobs(context.Background())
	}
}

func (m *Runtime) reapJobs(ctx context.Context) {
	cutoff := time.Now().Add(-jobRetention).Unix()

	m.jobsMu.Lock()
	var expired []string
	for id, j := range m.jobs {
		j.mu.Lock()
		if j.finishedAt > 0 && j.finishedAt < cutoff {
			expired = append(expired, id)
		}
		j.mu.Unlock()
	}
	m.jobsMu.Unlock()

	for _, id := range expired {
		if err := m.DeleteJob(ctx, id); err != nil && !errors.Is(err, ErrJobNotFound) {
			slog.Warn("reap job", "job_id", id, "err", err)
		}
	}
}

func (m *Runtime) jobStagingDir(id string) string {
	return filepath.Join(m.runnerConfig.DataDir, "jobs", id)
}

func (m *Runtime) lookupJob(id string) (*job, error) {
	m.jobsMu.Lock()
	defer m.jobsMu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return nil, ErrJobNotFound
	}
	return j, nil
}

func (j *job) record() *JobRecord {
	j.mu.Lock()
	defer j.mu.Unlock()

	rec := &JobRecord{
		ID:        j.id,
		Status:    j.status,
		Image:     j.spec.Image,
		TimedOut:  j.timedOut,
		CreatedAt: j.createdAt,
	}
	if j.exitCode != nil {
		code := *j.exitCode
		rec.ExitCode = &code
	}
	if j.errMsg != nil {
		msg := *j.errMsg
		rec.Error = &msg
	}
	if j.startedAt > 0 {
		started := j.startedAt
		rec.StartedAt = &started
	}
	if j.finishedAt > 0 {
		finished := j.finishedAt
		rec.FinishedAt = &finished
	}
	return rec
}

func (j *job) snapshotSpec() JobSpec {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.spec
}

// setContainerIDIfLive publishes the container ID unless the job was deleted
// meanwhile, reporting whether the caller still owns the run. Both this and
// DeleteJob's read happen under j.mu, so exactly one of them ends up
// responsible for removing the container.
func (j *job) setContainerIDIfLive(containerID string) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.deleted {
		return false
	}
	j.containerID = containerID
	return true
}

func (j *job) currentContainerID() string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.containerID
}

func (j *job) markTimedOut() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.timedOut = true
}

func (j *job) isFinished() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.finishedAt > 0
}

func (j *job) wasTimedOut() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.timedOut
}

// finish records the terminal state. It is a no-op if the job already finished,
// so exactly one terminal event maps to one terminal state.
func (j *job) finish(status string, exitCode *int, timedOut bool, errMsg *string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.finishedAt > 0 {
		return
	}
	j.status = status
	j.exitCode = exitCode
	j.errMsg = errMsg
	if timedOut {
		j.timedOut = true
	}
	j.finishedAt = time.Now().Unix()
}

func validateJobID(id string) error {
	if len(id) < jobIDMinLen || len(id) > jobIDMaxLen || !jobIDRe.MatchString(id) {
		return fmt.Errorf("invalid job id")
	}
	return nil
}

// normalizeJobSpec validates the caller's spec in place and fills in defaults.
func normalizeJobSpec(spec *JobSpec) error {
	if spec.Image == "" || !jobImageRe.MatchString(spec.Image) {
		return fmt.Errorf("invalid image")
	}
	switch {
	case spec.TimeoutMs == 0:
		spec.TimeoutMs = jobDefaultTimeoutMs
	case spec.TimeoutMs < 0 || spec.TimeoutMs > jobMaxTimeoutMs:
		return fmt.Errorf("timeout_ms must be between 1 and %d", jobMaxTimeoutMs)
	}
	return nil
}

// validateJobPath guards the host-privileged boundary: relPath is joined onto a
// staging dir on the runner host and onto the container work dir.
func validateJobPath(p string) error {
	if p == "" || !jobPathRe.MatchString(p) || strings.HasPrefix(p, "/") {
		return errors.New("invalid path")
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == ".." {
			return errors.New("invalid path")
		}
	}
	return nil
}

// firstTarFile extracts the first regular file from a docker cp tar stream.
func firstTarFile(tarBytes []byte) ([]byte, error) {
	tr := tar.NewReader(bytes.NewReader(tarBytes))
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read job output archive: %w", err)
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			return nil, fmt.Errorf("read job output archive: %w", err)
		}
		return data, nil
	}
	return nil, fmt.Errorf("%w: no regular file in output", ErrJobNotFound)
}
