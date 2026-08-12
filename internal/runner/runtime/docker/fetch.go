package docker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"syscall"
	"time"
)

// jobFetchMaxRedirects caps redirect hops for job input downloads. The
// default Go client re-dials per hop, so the DialContext guard below applies
// to every hop automatically; this only bounds how many hops are followed.
const jobFetchMaxRedirects = 5

// jobFetchTimeout bounds the whole download (DNS + connect + transfer), not
// just the dial.
const jobFetchTimeout = 60 * time.Second

// jobFetchBlockedRangesV4 mirrors netpolicy.PrivateRangesV4 (see
// internal/runner/runtime/netpolicy and its netrules consumer). Job input
// downloads run on the runner host, outside the per-container iptables
// egress rules, so the HTTP client itself has to refuse these destinations.
var jobFetchBlockedRangesV4 = mustParseCIDRs(
	"10.0.0.0/8",
	"172.16.0.0/12",
	"192.168.0.0/16",
	"169.254.0.0/16",
	"127.0.0.0/8",
	"100.64.0.0/10",
	"198.18.0.0/15",
	"240.0.0.0/4",
)

func mustParseCIDRs(cidrs ...string) []*net.IPNet {
	nets := make([]*net.IPNet, 0, len(cidrs))
	for _, c := range cidrs {
		_, n, err := net.ParseCIDR(c)
		if err != nil {
			panic(fmt.Sprintf("invalid CIDR %q: %v", c, err))
		}
		nets = append(nets, n)
	}
	return nets
}

// isBlockedFetchAddr reports whether ip must be refused as a job-fetch
// destination: any non-IPv4 address (IPv6 is disabled in this stack, so the
// simplest rule is to reject it outright), the unspecified address, or an
// address within a private/special-use IPv4 range.
func isBlockedFetchAddr(ip net.IP) bool {
	v4 := ip.To4()
	if v4 == nil {
		return true
	}
	if v4.Equal(net.IPv4zero) {
		return true
	}
	for _, n := range jobFetchBlockedRangesV4 {
		if n.Contains(v4) {
			return true
		}
	}
	return false
}

// blockPrivateDialAddrs is a net.Dialer.Control hook. Control runs once per
// candidate address, after the dialer has resolved the target to a literal
// IP and created the socket, but before connect() — checking here (rather
// than pre-resolving the URL's host once and checking that) is what closes
// the TOCTOU hole where a second DNS lookup could answer differently, and it
// also covers every hop of a redirect chain.
func blockPrivateDialAddrs(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid dial address %q: %w", address, err)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("dial address %q did not resolve to a literal IP", address)
	}
	if isBlockedFetchAddr(ip) {
		return fmt.Errorf("destination address %s is not allowed", ip)
	}
	return nil
}

// newJobFetchClient builds the hardened HTTP client used for job input
// downloads.
func newJobFetchClient() *http.Client {
	dialer := &net.Dialer{
		Timeout: 10 * time.Second,
		Control: blockPrivateDialAddrs,
	}
	return &http.Client{
		Transport: &http.Transport{DialContext: dialer.DialContext},
		Timeout:   jobFetchTimeout,
		CheckRedirect: func(_ *http.Request, via []*http.Request) error {
			if len(via) >= jobFetchMaxRedirects {
				return fmt.Errorf("stopped after %d redirects", jobFetchMaxRedirects)
			}
			return nil
		},
	}
}

// ErrJobFetchTooLarge is returned when a job-input download exceeds the
// runner's per-file cap, distinguishing it from a generic fetch failure.
var ErrJobFetchTooLarge = errors.New("downloaded file exceeds maximum allowed size")

// JobFetchError wraps a download failure (DNS, connect, non-2xx response,
// timeout, or an SSRF-blocked destination) with the caller-facing reason the
// runner HTTP layer maps onto the jobs API's 502 fetch_failed shape.
type JobFetchError struct {
	reason string
}

func (e *JobFetchError) Error() string { return e.reason }

func newJobFetchError(format string, args ...any) *JobFetchError {
	return &JobFetchError{reason: fmt.Sprintf(format, args...)}
}

// validateFetchURL checks that rawURL parses and uses http/https, mirroring
// (defense in depth) the check the runner HTTP handler already performs
// before ever calling into the manager.
func validateFetchURL(rawURL string) (*url.URL, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid url: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errors.New("url must be http or https")
	}
	return u, nil
}

// fetchURLToWriter downloads rawURL with client, writing at most maxBytes+1
// bytes into w so the caller can detect an oversized body rather than
// silently truncating it. It returns ErrJobFetchTooLarge or a *JobFetchError.
func fetchURLToWriter(ctx context.Context, client *http.Client, rawURL string, w io.Writer, maxBytes int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return newJobFetchError("invalid url: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return newJobFetchError("%v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return newJobFetchError("unexpected response status %d", resp.StatusCode)
	}

	written, err := io.Copy(w, io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return newJobFetchError("read response body: %v", err)
	}
	if written > maxBytes {
		return ErrJobFetchTooLarge
	}
	return nil
}
