package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/n8n-io/sandbox-service/internal/api/config"
	"github.com/n8n-io/sandbox-service/internal/api/registry"
	"github.com/n8n-io/sandbox-service/internal/api/store"
	"github.com/n8n-io/sandbox-service/internal/metrics"
)

// newJobsTestGateway builds a gateway router and returns it alongside the
// registry, so tests can register fake runners before exercising placement.
func newJobsTestGateway(t *testing.T) (http.Handler, registry.RunnerRegistry) {
	t.Helper()
	s, err := store.New(":memory:")
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	reg := registry.New(45 * time.Second)
	router, err := NewGatewayRouter(s, &config.APIConfig{
		APIKeys:      map[string]struct{}{"public-key": {}},
		RunnerAPIKey: "runner-key",
		MaxFileBytes: 1024,
	}, reg, metrics.NewAPIRecorder(false))
	if err != nil {
		t.Fatalf("create gateway router: %v", err)
	}
	return router, reg
}

func doJobRequest(router http.Handler, method, path, body string) *httptest.ResponseRecorder {
	var reqBody strings.Reader
	if body != "" {
		reqBody = *strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, &reqBody)
	req.Header.Set("X-Api-Key", "public-key")
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	return rr
}

func TestCreateJobNoCapacityReturns503(t *testing.T) {
	router, _ := newJobsTestGateway(t)

	rr := doJobRequest(router, http.MethodPost, "/jobs", `{"image":"alpine:latest"}`)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected %d, got %d (body: %s)", http.StatusServiceUnavailable, rr.Code, rr.Body.String())
	}

	var got jobError
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if got.Code != "no_capacity" {
		t.Fatalf("expected code no_capacity, got %q", got.Code)
	}
}

func TestGetJobRoutingUnknownReturns404(t *testing.T) {
	router, _ := newJobsTestGateway(t)

	rr := doJobRequest(router, http.MethodGet, "/jobs/11111111-1111-4111-8111-111111111111", "")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected %d, got %d (body: %s)", http.StatusNotFound, rr.Code, rr.Body.String())
	}

	var got jobError
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if got.Code != "not_found" {
		t.Fatalf("expected code not_found, got %q", got.Code)
	}
}

func TestCreateJobInvalidRequestReturns400(t *testing.T) {
	router, reg := newJobsTestGateway(t)
	reg.Upsert("r1", "http://127.0.0.1:1", "", true, 10, 0, 0)

	rr := doJobRequest(router, http.MethodPost, "/jobs", `{"cmd":["echo","hi"]}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d (body: %s)", http.StatusBadRequest, rr.Code, rr.Body.String())
	}

	var got jobError
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if got.Code != "invalid_request" {
		t.Fatalf("expected code invalid_request, got %q", got.Code)
	}
}

// fakeRunnerServer stands in for the (separately implemented) runner's Jobs
// API: it accepts the create body with the API-injected id, then serves
// GET/DELETE for that id so the proxy path can be exercised end to end.
func fakeRunnerServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID    string `json:"id"`
			Image string `json:"image"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.ID == "" || req.Image == "" {
			http.Error(w, "missing id or image", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"id": req.ID, "status": "queued"})
	})
	mux.HandleFunc("GET /jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"id":%q,"status":"running"}`, r.PathValue("id"))))
	})
	mux.HandleFunc("DELETE /jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestCreateJobHappyPathAndProxy(t *testing.T) {
	router, reg := newJobsTestGateway(t)
	srv := fakeRunnerServer(t)
	reg.Upsert("r1", srv.URL, "", true, 10, 0, 0)

	createRR := doJobRequest(router, http.MethodPost, "/jobs", `{"image":"alpine:latest","cmd":["echo","hi"]}`)
	if createRR.Code != http.StatusCreated {
		t.Fatalf("expected %d, got %d (body: %s)", http.StatusCreated, createRR.Code, createRR.Body.String())
	}
	var created struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(createRR.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if !isValidUUID(created.ID) {
		t.Fatalf("expected a UUID job id, got %q", created.ID)
	}

	getRR := doJobRequest(router, http.MethodGet, "/jobs/"+created.ID, "")
	if getRR.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d (body: %s)", http.StatusOK, getRR.Code, getRR.Body.String())
	}
	if !strings.Contains(getRR.Body.String(), created.ID) {
		t.Fatalf("expected proxied body to contain job id, got %s", getRR.Body.String())
	}

	delRR := doJobRequest(router, http.MethodDelete, "/jobs/"+created.ID, "")
	if delRR.Code != http.StatusNoContent {
		t.Fatalf("expected %d, got %d (body: %s)", http.StatusNoContent, delRR.Code, delRR.Body.String())
	}

	// Row should be gone after DELETE, even though the fake runner doesn't
	// track state itself (best-effort cleanup happens on the API's routing row).
	afterDeleteRR := doJobRequest(router, http.MethodGet, "/jobs/"+created.ID, "")
	if afterDeleteRR.Code != http.StatusNotFound {
		t.Fatalf("expected %d after delete, got %d (body: %s)", http.StatusNotFound, afterDeleteRR.Code, afterDeleteRR.Body.String())
	}
}

func TestJobProxyRunnerUnavailableReturnsStringCode(t *testing.T) {
	s, err := store.New(":memory:")
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	jobID := generateUUID()
	// Nothing listens on this address, so the reverse proxy's dial fails and
	// its ErrorHandler runs.
	if err := s.CreateJobRouting(&store.JobRoutingRecord{
		ID:             jobID,
		RunnerHTTPBase: "http://127.0.0.1:1",
		CreatedAt:      time.Now().Unix(),
	}); err != nil {
		t.Fatalf("seed job routing: %v", err)
	}

	router, err := NewGatewayRouter(s, &config.APIConfig{
		APIKeys:      map[string]struct{}{"public-key": {}},
		RunnerAPIKey: "runner-key",
		MaxFileBytes: 1024,
	}, registry.New(45*time.Second), metrics.NewAPIRecorder(false))
	if err != nil {
		t.Fatalf("create gateway router: %v", err)
	}

	rr := doJobRequest(router, http.MethodGet, "/jobs/"+jobID, "")
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected %d, got %d (body: %s)", http.StatusServiceUnavailable, rr.Code, rr.Body.String())
	}

	// Unmarshaling Code as a string (not the sandbox endpoints' int) fails
	// the test if the shared proxy ErrorHandler's int-code shape leaks through.
	var got jobError
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("expected a string code field, got body %s: %v", rr.Body.String(), err)
	}
	if got.Code != "runner_unavailable" {
		t.Fatalf("expected code runner_unavailable, got %q (body: %s)", got.Code, rr.Body.String())
	}
}
