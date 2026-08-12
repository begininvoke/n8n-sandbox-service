package api

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"unicode/utf8"
)

// maxLoggedPayloadBytes caps how much of a request body is captured for logging;
// the full body still reaches the handler untouched.
const maxLoggedPayloadBytes = 2048

// PayloadLoggingMiddleware logs request bodies to ease client-side debugging.
// Bodies larger than maxLoggedPayloadBytes are logged truncated; binary content
// is summarized instead of dumped.
func PayloadLoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body == nil || r.ContentLength == 0 || r.URL.Path == "/healthz" || r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}

		head := make([]byte, maxLoggedPayloadBytes)
		n, err := io.ReadFull(r.Body, head)
		if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
			slog.Warn("request payload: read failed", "method", r.Method, "path", r.URL.Path, "error", err)
			next.ServeHTTP(w, r)
			return
		}
		head = head[:n]

		payload := "<binary>"
		if isLoggableText(head) {
			payload = string(head)
		}
		slog.Info("request payload",
			"method", r.Method,
			"path", r.URL.Path,
			"query", r.URL.RawQuery,
			"content_length", r.ContentLength,
			"truncated", int64(n) < r.ContentLength,
			"payload", payload,
		)

		r.Body = struct {
			io.Reader
			io.Closer
		}{io.MultiReader(bytes.NewReader(head), r.Body), r.Body}
		next.ServeHTTP(w, r)
	})
}

// isLoggableText reports whether the captured prefix is safe to log as a string.
// The prefix may end mid-rune on a truncated UTF-8 body, so trailing bytes of an
// incomplete rune are tolerated.
func isLoggableText(b []byte) bool {
	s := b
	for len(s) > 0 {
		r, size := utf8.DecodeRune(s)
		if r == utf8.RuneError && size == 1 {
			// Tolerate an incomplete rune at the very end of the truncated prefix.
			return len(s) < utf8.UTFMax && utf8.RuneStart(s[0]) && len(s) != len(b)
		}
		if r < 0x20 && !strings.ContainsRune("\t\n\r", r) {
			return false
		}
		s = s[size:]
	}
	return true
}
