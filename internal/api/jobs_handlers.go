package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/n8n-io/sandbox-service/internal/api/config"
	"github.com/n8n-io/sandbox-service/internal/api/registry"
	"github.com/n8n-io/sandbox-service/internal/api/store"
)

const (
	maxJobRequestBytes        = 1 << 20 // 1 MiB
	defaultJobTimeoutMS int64 = 300000
	maxJobTimeoutMS     int64 = 900000
)

// jobError is the frozen jobs-API error shape: string codes
// (invalid_request, no_capacity, not_found, internal), unlike the sandbox
// endpoints' integer codes (see APIError in errors.go). Kept separate on
// purpose rather than reusing writeError.
type jobError struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

func writeJobError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(jobError{Error: msg, Code: code}); err != nil {
		slog.Warn("write job error response", "err", err)
	}
}

// JobSpec is the create-job request payload. It is also embedded (alongside
// the API-generated id) in the body relayed to the runner.
type JobSpec struct {
	Image     string            `json:"image"`
	Cmd       []string          `json:"cmd,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
	TimeoutMS int64             `json:"timeout_ms,omitempty"`
}

// jobRoundRobinPicker is implemented by registries that support round-robin
// placement (the in-memory registry). Registries that don't (e.g. the
// Postgres-backed one) fall back to PickLowestUsed.
type jobRoundRobinPicker interface {
	PickRoundRobin() (*registry.Runner, error)
}

func pickJobRunner(reg registry.RunnerRegistry) (*registry.Runner, error) {
	if rr, ok := reg.(jobRoundRobinPicker); ok {
		return rr.PickRoundRobin()
	}
	return reg.PickLowestUsed()
}

func decodeJobSpec(w http.ResponseWriter, r *http.Request) (*JobSpec, bool) {
	var spec JobSpec
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxJobRequestBytes))
	if err := decoder.Decode(&spec); err != nil && !errors.Is(err, io.EOF) {
		writeJobError(w, http.StatusBadRequest, "invalid_request", "invalid request body")
		return nil, false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeJobError(w, http.StatusBadRequest, "invalid_request", "invalid request body")
		return nil, false
	}

	if strings.TrimSpace(spec.Image) == "" {
		writeJobError(w, http.StatusBadRequest, "invalid_request", "image is required")
		return nil, false
	}
	if spec.TimeoutMS == 0 {
		spec.TimeoutMS = defaultJobTimeoutMS
	}
	if spec.TimeoutMS <= 0 || spec.TimeoutMS > maxJobTimeoutMS {
		writeJobError(w, http.StatusBadRequest, "invalid_request", "timeout_ms must be in (0, 900000]")
		return nil, false
	}
	return &spec, true
}

// relayRunnerResponse writes the runner's status and body back to the caller
// unchanged (used both for the non-201 relay and the successful-create relay).
func relayRunnerResponse(w http.ResponseWriter, resp *http.Response, body []byte) {
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	} else {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(body)
}

// compensatingDeleteJob best-effort deletes a job on the runner after the API
// failed to persist its routing row, mirroring handleCreateSandbox's
// compensating delete on store failure.
func compensatingDeleteJob(runnerBase, apiKey, jobID string) {
	req, err := http.NewRequest(http.MethodDelete, runnerBase+"/jobs/"+jobID, nil)
	if err != nil {
		return
	}
	req.Header.Set("X-Api-Key", apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		slog.Error("compensating job delete failed", "job_id", jobID, "error", err)
		return
	}
	_ = resp.Body.Close()
}

// handleCreateJob places a job on a runner and pins job_id -> runner in the
// store. It calls the runner directly (not via the reverse proxy) because the
// API-generated job id must be injected into the body sent to the runner.
func handleCreateJob(s store.SandboxStore, reg registry.RunnerRegistry, cfg *config.APIConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		spec, ok := decodeJobSpec(w, r)
		if !ok {
			return
		}

		run, err := pickJobRunner(reg)
		if err != nil {
			if errors.Is(err, registry.ErrNoRunners) {
				writeJobError(w, http.StatusServiceUnavailable, "no_capacity", "no runner capacity available")
			} else {
				slog.Error("create job failed: pick runner", "error", err)
				writeJobError(w, http.StatusInternalServerError, "internal", err.Error())
			}
			return
		}

		jobID := generateUUID()
		body, err := json.Marshal(struct {
			ID string `json:"id"`
			JobSpec
		}{ID: jobID, JobSpec: *spec})
		if err != nil {
			writeJobError(w, http.StatusInternalServerError, "internal", "failed to encode job request")
			return
		}

		runnerBase := strings.TrimRight(run.HTTPBaseURL, "/")
		req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, runnerBase+"/jobs", bytes.NewReader(body))
		if err != nil {
			writeJobError(w, http.StatusInternalServerError, "internal", "failed to build runner request")
			return
		}
		req.Header.Set("X-Api-Key", cfg.RunnerAPIKey)
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			slog.Error("create job failed: runner unreachable", "job_id", jobID, "runner_id", run.ID, "error", err)
			writeJobError(w, http.StatusServiceUnavailable, "no_capacity", "runner unavailable")
			return
		}
		defer resp.Body.Close()

		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			writeJobError(w, http.StatusInternalServerError, "internal", "failed to read runner response")
			return
		}

		if resp.StatusCode != http.StatusCreated {
			relayRunnerResponse(w, resp, respBody)
			return
		}

		routing := &store.JobRoutingRecord{
			ID:             jobID,
			RunnerHTTPBase: runnerBase,
			CreatedAt:      time.Now().Unix(),
		}
		if err := s.CreateJobRouting(routing); err != nil {
			slog.Error("create job failed: store routing", "job_id", jobID, "runner_id", run.ID, "error", err)
			compensatingDeleteJob(runnerBase, cfg.RunnerAPIKey, jobID)
			writeJobError(w, http.StatusInternalServerError, "internal", "failed to store job routing")
			return
		}

		relayRunnerResponse(w, resp, respBody)
	}
}

// jobProxyHandler proxies every /jobs/{id}/* request to the runner pinned for
// that job. It is a thin variant of sandboxProxyHandler: no idle-window
// check, no UpdateLastActive/UpdateStatus, and (intentionally) no body
// limiter — the runner returns the contract's 413 for oversized files, and an
// API-side MaxBytesReader would turn that into a 400 instead.
func jobProxyHandler(s store.SandboxStore, cfg *config.APIConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !isValidUUID(id) {
			writeJobError(w, http.StatusBadRequest, "invalid_request", "invalid job id")
			return
		}

		rec, err := s.GetJobRouting(id)
		if err != nil {
			writeJobError(w, http.StatusInternalServerError, "internal", err.Error())
			return
		}
		if rec == nil {
			writeJobError(w, http.StatusNotFound, "not_found", "job not found")
			return
		}

		u, err := url.Parse(rec.RunnerHTTPBase)
		if err != nil {
			writeJobError(w, http.StatusInternalServerError, "internal", "invalid runner routing")
			return
		}
		proxy := newRunnerReverseProxy(u, cfg.RunnerAPIKey, nil)
		// newRunnerReverseProxy's default ErrorHandler writes the sandbox
		// int-code error shape; jobs need the frozen string-code shape, so
		// override it here rather than touching the shared helper.
		proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				writeJobError(w, http.StatusBadRequest, "invalid_request", "failed to read request body: "+maxBytesErr.Error())
				return
			}
			if strings.Contains(err.Error(), "request body too large") {
				writeJobError(w, http.StatusBadRequest, "invalid_request", "failed to read request body: http: request body too large")
				return
			}
			writeJobError(w, http.StatusServiceUnavailable, "runner_unavailable", "runner unavailable")
		}
		proxy.ServeHTTP(w, r)

		if r.Method == http.MethodDelete {
			_ = s.DeleteJobRouting(id) // best-effort row cleanup
		}
	}
}
