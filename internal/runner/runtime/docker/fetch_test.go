package docker

import (
	"net"
	"testing"
)

func TestIsBlockedFetchAddr(t *testing.T) {
	tests := []struct {
		name    string
		ip      string
		blocked bool
	}{
		{name: "class A private", ip: "10.1.2.3", blocked: true},
		{name: "loopback", ip: "127.0.0.1", blocked: true},
		{name: "link-local", ip: "169.254.1.1", blocked: true},
		{name: "class B private", ip: "172.16.5.5", blocked: true},
		{name: "class C private", ip: "192.168.1.1", blocked: true},
		{name: "shared address space", ip: "100.64.0.1", blocked: true},
		{name: "benchmarking range", ip: "198.18.0.1", blocked: true},
		{name: "reserved/multicast", ip: "240.0.0.1", blocked: true},
		{name: "unspecified", ip: "0.0.0.0", blocked: true},
		{name: "IPv6", ip: "::1", blocked: true},
		{name: "cloudflare DNS", ip: "1.1.1.1", blocked: false},
		{name: "public host", ip: "93.184.216.34", blocked: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ip := net.ParseIP(tc.ip)
			if ip == nil {
				t.Fatalf("net.ParseIP(%q) returned nil", tc.ip)
			}
			if got := isBlockedFetchAddr(ip); got != tc.blocked {
				t.Errorf("isBlockedFetchAddr(%q) = %v, want %v", tc.ip, got, tc.blocked)
			}
		})
	}
}

func TestBlockPrivateDialAddrsRejectsPrivateAddress(t *testing.T) {
	if err := blockPrivateDialAddrs("tcp", "10.0.0.5:443", nil); err == nil {
		t.Fatal("blockPrivateDialAddrs() accepted a private address, want an error")
	}
}

func TestBlockPrivateDialAddrsAllowsPublicAddress(t *testing.T) {
	if err := blockPrivateDialAddrs("tcp", "1.1.1.1:443", nil); err != nil {
		t.Fatalf("blockPrivateDialAddrs() rejected a public address: %v", err)
	}
}

func TestValidateFetchURL(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{name: "https ok", raw: "https://example.com/cat.jpg"},
		{name: "http ok", raw: "http://example.com/cat.jpg"},
		{name: "missing scheme", raw: "example.com/cat.jpg", wantErr: true},
		{name: "ftp scheme", raw: "ftp://example.com/cat.jpg", wantErr: true},
		{name: "no host", raw: "https:///cat.jpg", wantErr: true},
		{name: "malformed", raw: "http://[::1", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := validateFetchURL(tc.raw)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateFetchURL(%q) error = %v, wantErr = %v", tc.raw, err, tc.wantErr)
			}
		})
	}
}
