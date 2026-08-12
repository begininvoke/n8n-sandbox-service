package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/n8n-io/sandbox-service/internal/daemon"
	"github.com/n8n-io/sandbox-service/internal/runner/config"
	"github.com/n8n-io/sandbox-service/internal/runner/runtime/docker"
)

// JobManager is the subset of *docker.Runtime's job lifecycle the runner HTTP
// layer depends on. The firecracker.ee runtime doesn't implement it; job
// routes answer 501 when the router is built with a nil JobManager.
type JobManager interface {
	CreateJob(id string, spec docker.JobSpec) (*docker.JobRecord, error)
	StageJobFile(id, relPath string, r io.Reader, maxBytes int64) error
	StageJobFileFromURL(ctx context.Context, id, relPath, rawURL string, maxBytes int64) error
	StartJob(id string) (*daemon.Execution, error)
	JobEvents(id string) (*daemon.Execution, error)
	GetJob(id string) (*docker.JobRecord, error)
	JobOutputFile(ctx context.Context, id, relPath string) ([]byte, error)
	DeleteJob(ctx context.Context, id string) error
}

// maxJobCreateBodyBytes bounds the create-job request body (id + JobSpec),
// mirroring the API gateway's own limit.
const maxJobCreateBodyBytes = 1 << 20 // 1 MiB

// maxJobFetchBodyBytes bounds the fetch-by-URL request body: it only ever
// carries a path and a URL, so it stays far below the file-upload cap.
const maxJobFetchBodyBytes = 1 << 16 // 64 KiB

// jobIDRe/jobPathRe mirror the (unexported) validation the job manager itself
// enforces in internal/runner/runtime/docker/jobs.go; duplicated here so
// handlers can reject bad input with the frozen jobs-API error shape before
// ever calling into the manager.
var (
	jobIDRe    = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]*$`)
	jobImageRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._/:@-]*$`)
	jobPathRe  = regexp.MustCompile(`^[A-Za-z0-9._/-]+$`)
)

const (
	jobIDMinLen     = 12
	jobIDMaxLen     = 64
	jobMaxTimeoutMs = 900_000
)

func isValidJobID(id string) bool {
	return len(id) >= jobIDMinLen && len(id) <= jobIDMaxLen && jobIDRe.MatchString(id)
}

// isValidFetchURL mirrors (for early rejection, before ever calling into the
// job manager) the check docker.validateFetchURL performs: the URL must
// parse and use http/https.
func isValidFetchURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	return (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}

func isValidJobPath(p string) bool {
	if p == "" || !jobPathRe.MatchString(p) || strings.HasPrefix(p, "/") {
		return false
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == ".." {
			return false
		}
	}
	return true
}

// validateJobSpec applies the same image/timeout bounds the manager enforces,
// so CreateJob is only ever called with input it will accept.
func validateJobSpec(spec docker.JobSpec) error {
	if spec.Image == "" || !jobImageRe.MatchString(spec.Image) {
		return errors.New("invalid image")
	}
	if spec.TimeoutMs < 0 || spec.TimeoutMs > jobMaxTimeoutMs {
		return fmt.Errorf("timeout_ms must be between 1 and %d", jobMaxTimeoutMs)
	}
	return nil
}

// jobErrorBody is the frozen jobs-API error shape: string codes, unlike the
// sandbox endpoints' writeError (no code field). Kept separate on purpose.
type jobErrorBody struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

func writeJobError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(jobErrorBody{Error: msg, Code: code}); err != nil {
		slog.Warn("write job error response", "err", err)
	}
}

func writeJobsUnsupported(w http.ResponseWriter) {
	writeJobError(w, http.StatusNotImplemented, "unsupported", "jobs not supported by this runtime")
}

// writeJobManagerError maps the job manager's sentinel errors onto the
// contract's status/code pairs; anything else is a genuine internal error
// since input is validated before the manager is ever called.
func writeJobManagerError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, docker.ErrJobNotFound):
		writeJobError(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, docker.ErrJobNotStaging):
		writeJobError(w, http.StatusConflict, "job_not_staging", err.Error())
	case errors.Is(err, docker.ErrJobNotFinished):
		writeJobError(w, http.StatusConflict, "job_not_finished", err.Error())
	default:
		writeJobError(w, http.StatusInternalServerError, "internal", err.Error())
	}
}

// requireJobs gates a job route behind a non-nil JobManager, answering 501 for
// every job route when the underlying runtime doesn't support jobs.
func requireJobs(jobs JobManager, h http.HandlerFunc) http.HandlerFunc {
	if jobs == nil {
		return func(w http.ResponseWriter, r *http.Request) {
			writeJobsUnsupported(w)
		}
	}
	return h
}

// createJobRequest is the create-job request payload: the API-generated id
// injected top-level next to the JobSpec fields (see internal/api/jobs_handlers.go).
type createJobRequest struct {
	ID string `json:"id"`
	docker.JobSpec
}

// CreateJob handles POST /jobs.
func CreateJob(jobs JobManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req createJobRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxJobCreateBodyBytes)).Decode(&req); err != nil {
			writeJobError(w, http.StatusBadRequest, "invalid_request", "invalid request body")
			return
		}
		if !isValidJobID(req.ID) {
			writeJobError(w, http.StatusBadRequest, "invalid_request", "invalid job id")
			return
		}
		if err := validateJobSpec(req.JobSpec); err != nil {
			writeJobError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}

		rec, err := jobs.CreateJob(req.ID, req.JobSpec)
		if err != nil {
			writeJobManagerError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, rec)
	}
}

// GetJob handles GET /jobs/{id}.
func GetJob(jobs JobManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !isValidJobID(id) {
			writeJobError(w, http.StatusBadRequest, "invalid_request", "invalid job id")
			return
		}
		rec, err := jobs.GetJob(id)
		if err != nil {
			writeJobManagerError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, rec)
	}
}

// StageJobFile handles PUT /jobs/{id}/files?path=.
func StageJobFile(jobs JobManager, cfg *config.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !isValidJobID(id) {
			writeJobError(w, http.StatusBadRequest, "invalid_request", "invalid job id")
			return
		}
		path := r.URL.Query().Get("path")
		if !isValidJobPath(path) {
			writeJobError(w, http.StatusBadRequest, "invalid_request", "invalid path")
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, cfg.MaxFileBytes)
		if err := jobs.StageJobFile(id, path, r.Body, cfg.MaxFileBytes); err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				writeJobError(w, http.StatusRequestEntityTooLarge, "payload_too_large", "file exceeds maximum allowed size")
				return
			}
			writeJobManagerError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// stageJobFileFromURLRequest is the POST /jobs/{id}/files/fetch request body.
type stageJobFileFromURLRequest struct {
	Path string `json:"path"`
	URL  string `json:"url"`
}

// StageJobFileFromURL handles POST /jobs/{id}/files/fetch: the runner
// downloads url server-side into the job's staging directory, saving the
// caller a download-then-reupload round trip.
func StageJobFileFromURL(jobs JobManager, cfg *config.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !isValidJobID(id) {
			writeJobError(w, http.StatusBadRequest, "invalid_request", "invalid job id")
			return
		}

		var req stageJobFileFromURLRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxJobFetchBodyBytes)).Decode(&req); err != nil {
			writeJobError(w, http.StatusBadRequest, "invalid_request", "invalid request body")
			return
		}
		if !isValidJobPath(req.Path) {
			writeJobError(w, http.StatusBadRequest, "invalid_request", "invalid path")
			return
		}
		if !isValidFetchURL(req.URL) {
			writeJobError(w, http.StatusBadRequest, "invalid_request", "invalid url")
			return
		}

		err := jobs.StageJobFileFromURL(r.Context(), id, req.Path, req.URL, cfg.MaxFileBytes)
		if err != nil {
			writeJobFetchError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// writeJobFetchError maps StageJobFileFromURL's error results onto the
// contract's status/code pairs. Unlike writeJobManagerError, a fetch failure
// (DNS, connect, non-2xx, timeout, or an SSRF-blocked destination) is an
// expected outcome of downloading caller-supplied input, not an internal
// error, so it gets its own 502 fetch_failed mapping instead of falling
// through to the 500 default.
func writeJobFetchError(w http.ResponseWriter, err error) {
	var fetchErr *docker.JobFetchError
	switch {
	case errors.Is(err, docker.ErrJobFetchTooLarge):
		writeJobError(w, http.StatusRequestEntityTooLarge, "payload_too_large", "downloaded file exceeds maximum allowed size")
	case errors.As(err, &fetchErr):
		writeJobError(w, http.StatusBadGateway, "fetch_failed", fetchErr.Error())
	default:
		writeJobManagerError(w, err)
	}
}

// StartJob handles POST /jobs/{id}/start. It streams the job's event log as
// NDJSON from the beginning; the run itself continues in the background even
// if this connection drops (see docker.Runtime.runJob).
func StartJob(jobs JobManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !isValidJobID(id) {
			writeJobError(w, http.StatusBadRequest, "invalid_request", "invalid job id")
			return
		}

		ex, err := jobs.StartJob(id)
		if err != nil {
			writeJobManagerError(w, err)
			return
		}

		writeNdjsonHeader(w)
		ex.Follow(r.Context(), nil, ndjsonWriter(w))
	}
}

// JobEvents handles GET /jobs/{id}/events?after=&follow=.
func JobEvents(jobs JobManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !isValidJobID(id) {
			writeJobError(w, http.StatusBadRequest, "invalid_request", "invalid job id")
			return
		}
		after, err := parseAfterParam(r.URL.Query().Get("after"))
		if err != nil {
			writeJobError(w, http.StatusBadRequest, "invalid_request", "invalid after parameter")
			return
		}

		ex, err := jobs.JobEvents(id)
		if err != nil {
			writeJobManagerError(w, err)
			return
		}

		if !ex.HasHistory(after) {
			writeJobError(w, http.StatusGone, "gone", "requested event history is no longer retained")
			return
		}

		follow := r.URL.Query().Get("follow") == "true"

		// Best-effort preflight before committing the 200/NDJSON response, same
		// trade-off as the daemon's own /executions/{id} handler: history can
		// still be trimmed between the check above and Snapshot/Follow below.
		writeNdjsonHeader(w)
		write := ndjsonWriter(w)

		if follow {
			ex.Follow(r.Context(), after, write)
			return
		}

		events, ok := ex.Snapshot(after)
		if !ok {
			return
		}
		for _, data := range events {
			write(data)
		}
	}
}

// JobOutputFile handles GET /jobs/{id}/files/content?path=.
func JobOutputFile(jobs JobManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !isValidJobID(id) {
			writeJobError(w, http.StatusBadRequest, "invalid_request", "invalid job id")
			return
		}
		path := r.URL.Query().Get("path")
		if !isValidJobPath(path) {
			writeJobError(w, http.StatusBadRequest, "invalid_request", "invalid path")
			return
		}

		data, err := jobs.JobOutputFile(r.Context(), id, path)
		if err != nil {
			writeJobManagerError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}
}

// DeleteJob handles DELETE /jobs/{id}.
func DeleteJob(jobs JobManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !isValidJobID(id) {
			writeJobError(w, http.StatusBadRequest, "invalid_request", "invalid job id")
			return
		}
		if err := jobs.DeleteJob(r.Context(), id); err != nil {
			writeJobManagerError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// --- NDJSON helpers, copied from internal/daemon/daemon.go (unexported there). ---

func writeNdjsonHeader(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

func ndjsonWriter(w http.ResponseWriter) func([]byte) {
	flusher, _ := w.(http.Flusher)
	return func(data []byte) {
		if _, err := w.Write(data); err != nil {
			slog.Warn("write job event", "err", err)
			return
		}
		if _, err := w.Write([]byte{'\n'}); err != nil {
			slog.Warn("write job event newline", "err", err)
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
	}
}

// parseAfterParam parses the "after" query parameter. Returns nil when the
// parameter is absent (meaning "all events"), or a pointer to the parsed value.
func parseAfterParam(v string) (*uint64, error) {
	if v == "" {
		return nil, nil
	}
	after, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return nil, err
	}
	return &after, nil
}
