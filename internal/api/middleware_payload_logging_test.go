package api

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPayloadLoggingPreservesBody(t *testing.T) {
	large := strings.Repeat("x", maxLoggedPayloadBytes*3)
	for name, body := range map[string]string{
		"small json":      `{"image":"alpine:3"}`,
		"larger than cap": large,
		"binary":          "\x00\x01\x02payload",
		"empty":           "",
	} {
		t.Run(name, func(t *testing.T) {
			var got []byte
			h := PayloadLoggingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got, _ = io.ReadAll(r.Body)
			}))

			req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader([]byte(body)))
			h.ServeHTTP(httptest.NewRecorder(), req)

			if string(got) != body {
				t.Fatalf("handler received %d bytes, want %d; body altered by middleware", len(got), len(body))
			}
		})
	}
}

func TestIsLoggableText(t *testing.T) {
	if isLoggableText([]byte("\x00\x01")) {
		t.Fatal("NUL bytes should not be loggable")
	}
	if !isLoggableText([]byte("{\"a\":\"b\"}\n\tümlaut")) {
		t.Fatal("printable UTF-8 with whitespace should be loggable")
	}
}
