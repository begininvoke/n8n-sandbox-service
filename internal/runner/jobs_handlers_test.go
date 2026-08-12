package runner

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/n8n-io/sandbox-service/internal/daemon"
	"github.com/n8n-io/sandbox-service/internal/metrics"
	"github.com/n8n-io/sandbox-service/internal/runner/config"
	"github.com/n8n-io/sandbox-service/internal/runner/runtime/docker"
)

const testJobID = "job-1234567890"

// fakeJobManager is a scriptable stand-in for *docker.Runtime's job methods.
type fakeJobManager struct {
	createRec *docker.JobRecord
	createErr error

	stageErr error

	startEx  *daemon.Execution
	startErr error

	eventsEx  *daemon.Execution
	eventsErr error

	getRec *docker.JobRecord
	getErr error

	outputData []byte
	outputErr  error

	deleteErr error
}

func (f *fakeJobManager) CreateJob(id string, spec docker.JobSpec) (*docker.JobRecord, error) {
	return f.createRec, f.createErr
}

func (f *fakeJobManager) StageJobFile(id, relPath string, r io.Reader, maxBytes int64) error {
	if f.stageErr != nil {
		return f.stageErr
	}
	_, err := io.Copy(io.Discard, io.LimitReader(r, maxBytes+1))
	return err
}

func (f *fakeJobManager) StartJob(id string) (*daemon.Execution, error) {
	return f.startEx, f.startErr
}

func (f *fakeJobManager) JobEvents(id string) (*daemon.Execution, error) {
	return f.eventsEx, f.eventsErr
}

func (f *fakeJobManager) GetJob(id string) (*docker.JobRecord, error) {
	return f.getRec, f.getErr
}

func (f *fakeJobManager) JobOutputFile(ctx context.Context, id, relPath string) ([]byte, error) {
	return f.outputData, f.outputErr
}

func (f *fakeJobManager) DeleteJob(ctx context.Context, id string) error {
	return f.deleteErr
}

func jobsTestConfig() *config.Config {
	return &config.Config{
		APIKeys:      map[string]struct{}{"testkey": {}},
		MaxFileBytes: 1024,
	}
}

func jobsTestRouter(jobs JobManager, cfg *config.Config) http.Handler {
	return NewRouter(&fakeRuntime{}, jobs, cfg, metrics.NewRunnerRecorder(false))
}

func doJobRequest(t *testing.T, router http.Handler, method, path string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, body)
	req.Header.Set("X-Api-Key", "testkey")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func decodeJobError(t *testing.T, rec *httptest.ResponseRecorder) jobErrorBody {
	t.Helper()
	var body jobErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode job error body: %v (body: %s)", err, rec.Body.String())
	}
	return body
}

func TestJobRoutesReturn501WhenUnsupported(t *testing.T) {
	router := jobsTestRouter(nil, jobsTestConfig())

	for _, tc := range []struct {
		method, path string
	}{
		{http.MethodPost, "/jobs"},
		{http.MethodGet, "/jobs/" + testJobID},
		{http.MethodPut, "/jobs/" + testJobID + "/files?path=out.txt"},
		{http.MethodPost, "/jobs/" + testJobID + "/start"},
		{http.MethodGet, "/jobs/" + testJobID + "/events"},
		{http.MethodGet, "/jobs/" + testJobID + "/files/content?path=out.txt"},
		{http.MethodDelete, "/jobs/" + testJobID},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			rec := doJobRequest(t, router, tc.method, tc.path, nil)
			if rec.Code != http.StatusNotImplemented {
				t.Fatalf("status = %d, want %d (body: %s)", rec.Code, http.StatusNotImplemented, rec.Body.String())
			}
			if got := decodeJobError(t, rec).Code; got != "unsupported" {
				t.Errorf("code = %q, want %q", got, "unsupported")
			}
		})
	}
}

func TestCreateJob(t *testing.T) {
	t.Run("201 happy path", func(t *testing.T) {
		fake := &fakeJobManager{createRec: &docker.JobRecord{ID: testJobID, Status: "staging", Image: "alpine"}}
		router := jobsTestRouter(fake, jobsTestConfig())

		body := strings.NewReader(`{"id":"` + testJobID + `","image":"alpine"}`)
		rec := doJobRequest(t, router, http.MethodPost, "/jobs", body)

		if rec.Code != http.StatusCreated {
			t.Fatalf("status = %d, want %d (body: %s)", rec.Code, http.StatusCreated, rec.Body.String())
		}
		var got docker.JobRecord
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if got.ID != testJobID || got.Status != "staging" {
			t.Errorf("got %+v", got)
		}
	})

	t.Run("400 invalid job id", func(t *testing.T) {
		router := jobsTestRouter(&fakeJobManager{}, jobsTestConfig())
		body := strings.NewReader(`{"id":"short","image":"alpine"}`)
		rec := doJobRequest(t, router, http.MethodPost, "/jobs", body)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d (body: %s)", rec.Code, http.StatusBadRequest, rec.Body.String())
		}
		if got := decodeJobError(t, rec).Code; got != "invalid_request" {
			t.Errorf("code = %q, want %q", got, "invalid_request")
		}
	})

	t.Run("400 invalid image", func(t *testing.T) {
		router := jobsTestRouter(&fakeJobManager{}, jobsTestConfig())
		body := strings.NewReader(`{"id":"` + testJobID + `"}`)
		rec := doJobRequest(t, router, http.MethodPost, "/jobs", body)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d (body: %s)", rec.Code, http.StatusBadRequest, rec.Body.String())
		}
		if got := decodeJobError(t, rec).Code; got != "invalid_request" {
			t.Errorf("code = %q, want %q", got, "invalid_request")
		}
	})

	t.Run("500 internal on manager failure", func(t *testing.T) {
		fake := &fakeJobManager{createErr: errors.New("boom")}
		router := jobsTestRouter(fake, jobsTestConfig())
		body := strings.NewReader(`{"id":"` + testJobID + `","image":"alpine"}`)
		rec := doJobRequest(t, router, http.MethodPost, "/jobs", body)

		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d (body: %s)", rec.Code, http.StatusInternalServerError, rec.Body.String())
		}
		if got := decodeJobError(t, rec).Code; got != "internal" {
			t.Errorf("code = %q, want %q", got, "internal")
		}
	})
}

func TestGetJob(t *testing.T) {
	t.Run("200 happy path", func(t *testing.T) {
		fake := &fakeJobManager{getRec: &docker.JobRecord{ID: testJobID, Status: "exited"}}
		router := jobsTestRouter(fake, jobsTestConfig())

		rec := doJobRequest(t, router, http.MethodGet, "/jobs/"+testJobID, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
		}
	})

	t.Run("404 not found", func(t *testing.T) {
		fake := &fakeJobManager{getErr: docker.ErrJobNotFound}
		router := jobsTestRouter(fake, jobsTestConfig())

		rec := doJobRequest(t, router, http.MethodGet, "/jobs/"+testJobID, nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
		}
		if got := decodeJobError(t, rec).Code; got != "not_found" {
			t.Errorf("code = %q, want %q", got, "not_found")
		}
	})

	t.Run("400 invalid job id", func(t *testing.T) {
		router := jobsTestRouter(&fakeJobManager{}, jobsTestConfig())
		rec := doJobRequest(t, router, http.MethodGet, "/jobs/short", nil)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
		}
	})
}

func TestStageJobFile(t *testing.T) {
	t.Run("204 happy path", func(t *testing.T) {
		fake := &fakeJobManager{}
		router := jobsTestRouter(fake, jobsTestConfig())

		rec := doJobRequest(t, router, http.MethodPut, "/jobs/"+testJobID+"/files?path=input.txt", strings.NewReader("hello"))
		if rec.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want %d (body: %s)", rec.Code, http.StatusNoContent, rec.Body.String())
		}
	})

	t.Run("400 invalid path", func(t *testing.T) {
		router := jobsTestRouter(&fakeJobManager{}, jobsTestConfig())
		rec := doJobRequest(t, router, http.MethodPut, "/jobs/"+testJobID+"/files?path=../etc/passwd", strings.NewReader("x"))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
		}
		if got := decodeJobError(t, rec).Code; got != "invalid_request" {
			t.Errorf("code = %q, want %q", got, "invalid_request")
		}
	})

	t.Run("409 job not staging", func(t *testing.T) {
		fake := &fakeJobManager{stageErr: docker.ErrJobNotStaging}
		router := jobsTestRouter(fake, jobsTestConfig())
		rec := doJobRequest(t, router, http.MethodPut, "/jobs/"+testJobID+"/files?path=input.txt", strings.NewReader("hello"))
		if rec.Code != http.StatusConflict {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusConflict)
		}
		if got := decodeJobError(t, rec).Code; got != "job_not_staging" {
			t.Errorf("code = %q, want %q", got, "job_not_staging")
		}
	})

	t.Run("413 payload too large", func(t *testing.T) {
		fake := &fakeJobManager{}
		cfg := jobsTestConfig()
		cfg.MaxFileBytes = 4
		router := jobsTestRouter(fake, cfg)

		rec := doJobRequest(t, router, http.MethodPut, "/jobs/"+testJobID+"/files?path=input.txt", strings.NewReader("way too much data"))
		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want %d (body: %s)", rec.Code, http.StatusRequestEntityTooLarge, rec.Body.String())
		}
		if got := decodeJobError(t, rec).Code; got != "payload_too_large" {
			t.Errorf("code = %q, want %q", got, "payload_too_large")
		}
	})
}

func TestJobOutputFile(t *testing.T) {
	t.Run("200 happy path", func(t *testing.T) {
		fake := &fakeJobManager{outputData: []byte("result")}
		router := jobsTestRouter(fake, jobsTestConfig())

		rec := doJobRequest(t, router, http.MethodGet, "/jobs/"+testJobID+"/files/content?path=out.txt", nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
		}
		if rec.Body.String() != "result" {
			t.Errorf("body = %q, want %q", rec.Body.String(), "result")
		}
		if ct := rec.Header().Get("Content-Type"); ct != "application/octet-stream" {
			t.Errorf("content-type = %q, want application/octet-stream", ct)
		}
	})

	t.Run("409 job not finished", func(t *testing.T) {
		fake := &fakeJobManager{outputErr: docker.ErrJobNotFinished}
		router := jobsTestRouter(fake, jobsTestConfig())

		rec := doJobRequest(t, router, http.MethodGet, "/jobs/"+testJobID+"/files/content?path=out.txt", nil)
		if rec.Code != http.StatusConflict {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusConflict)
		}
		if got := decodeJobError(t, rec).Code; got != "job_not_finished" {
			t.Errorf("code = %q, want %q", got, "job_not_finished")
		}
	})

	t.Run("404 not found", func(t *testing.T) {
		fake := &fakeJobManager{outputErr: docker.ErrJobNotFound}
		router := jobsTestRouter(fake, jobsTestConfig())

		rec := doJobRequest(t, router, http.MethodGet, "/jobs/"+testJobID+"/files/content?path=out.txt", nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
		}
	})
}

func TestDeleteJob(t *testing.T) {
	t.Run("204 happy path", func(t *testing.T) {
		router := jobsTestRouter(&fakeJobManager{}, jobsTestConfig())
		rec := doJobRequest(t, router, http.MethodDelete, "/jobs/"+testJobID, nil)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
		}
	})

	t.Run("404 not found", func(t *testing.T) {
		fake := &fakeJobManager{deleteErr: docker.ErrJobNotFound}
		router := jobsTestRouter(fake, jobsTestConfig())
		rec := doJobRequest(t, router, http.MethodDelete, "/jobs/"+testJobID, nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
		}
	})
}

func TestStartJobStreamsNdjsonToTerminalEvent(t *testing.T) {
	em := daemon.NewExecManager()
	defer em.Close()
	ex := em.NewExternal(testJobID)
	ex.Append(daemon.Response{Type: daemon.ResponseTypePulling, Data: "alpine"})
	ex.Append(daemon.Response{Type: daemon.ResponseTypeStarted, ExecID: testJobID})
	success := true
	ex.Append(daemon.Response{Type: daemon.ResponseTypeExit, ExitCode: 0, Success: &success})

	fake := &fakeJobManager{startEx: ex}
	router := jobsTestRouter(fake, jobsTestConfig())

	rec := doJobRequest(t, router, http.MethodPost, "/jobs/"+testJobID+"/start", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body: %s)", rec.Code, http.StatusOK, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/x-ndjson" {
		t.Errorf("content-type = %q, want application/x-ndjson", ct)
	}
	lines := strings.Split(strings.TrimSpace(rec.Body.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d ndjson lines, want 3 (body: %s)", len(lines), rec.Body.String())
	}
	var last daemon.Response
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &last); err != nil {
		t.Fatalf("decode last event: %v", err)
	}
	if last.Type != daemon.ResponseTypeExit {
		t.Errorf("last event type = %q, want %q", last.Type, daemon.ResponseTypeExit)
	}
}

func TestStartJobErrorMapping(t *testing.T) {
	fake := &fakeJobManager{startErr: docker.ErrJobNotStaging}
	router := jobsTestRouter(fake, jobsTestConfig())
	rec := doJobRequest(t, router, http.MethodPost, "/jobs/"+testJobID+"/start", nil)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusConflict)
	}
	if got := decodeJobError(t, rec).Code; got != "job_not_staging" {
		t.Errorf("code = %q, want %q", got, "job_not_staging")
	}
}

func TestJobEventsSnapshotHappyPath(t *testing.T) {
	em := daemon.NewExecManager()
	defer em.Close()
	ex := em.NewExternal(testJobID)
	ex.Append(daemon.Response{Type: daemon.ResponseTypeStarted, ExecID: testJobID})

	fake := &fakeJobManager{eventsEx: ex}
	router := jobsTestRouter(fake, jobsTestConfig())

	rec := doJobRequest(t, router, http.MethodGet, "/jobs/"+testJobID+"/events", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body: %s)", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"type":"started"`) {
		t.Errorf("body missing started event: %s", rec.Body.String())
	}
}

func TestJobEventsReturns410WhenHistoryTrimmed(t *testing.T) {
	t.Setenv("SANDBOX_EXEC_MAX_EVENT_BYTES", "1")
	em := daemon.NewExecManager()
	defer em.Close()
	ex := em.NewExternal(testJobID)
	for i := 0; i < 5; i++ {
		ex.Append(daemon.Response{Type: daemon.ResponseTypeStdout, Data: "line\n"})
	}

	fake := &fakeJobManager{eventsEx: ex}
	router := jobsTestRouter(fake, jobsTestConfig())

	rec := doJobRequest(t, router, http.MethodGet, "/jobs/"+testJobID+"/events?after=0", nil)
	if rec.Code != http.StatusGone {
		t.Fatalf("status = %d, want %d (body: %s)", rec.Code, http.StatusGone, rec.Body.String())
	}
	if got := decodeJobError(t, rec).Code; got != "gone" {
		t.Errorf("code = %q, want %q", got, "gone")
	}
}

func TestJobEventsNotFound(t *testing.T) {
	fake := &fakeJobManager{eventsErr: docker.ErrJobNotFound}
	router := jobsTestRouter(fake, jobsTestConfig())
	rec := doJobRequest(t, router, http.MethodGet, "/jobs/"+testJobID+"/events", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}
