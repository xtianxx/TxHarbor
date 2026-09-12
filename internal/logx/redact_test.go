package logx

import (
	"fmt"
	"strings"
	"testing"
)

func TestRedact(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    []string // substrings that must remain
		secrets []string // substrings that must be gone
	}{
		{
			name:    "postgres url password",
			in:      "connect postgres://txharbor:sup3rs3cret@db.internal:5432/txharbor?sslmode=disable failed",
			want:    []string{"postgres://txharbor:", "@db.internal:5432/txharbor", "sslmode=disable"},
			secrets: []string{"sup3rs3cret"},
		},
		{
			name:    "keyword DSN password",
			in:      "host=db.internal port=5432 user=txharbor password=sup3rs3cret dbname=txharbor",
			want:    []string{"host=db.internal", "port=5432", "user=txharbor", "dbname=txharbor"},
			secrets: []string{"sup3rs3cret"},
		},
		{
			name:    "quoted keyword DSN password",
			in:      "password='s3 cret' dbname=txharbor",
			want:    []string{"dbname=txharbor"},
			secrets: []string{"s3 cret"},
		},
		{
			name:    "url query token",
			in:      "http://rpc.local:8545/path?apikey=abc123&timeout=5s",
			want:    []string{"rpc.local:8545", "timeout=5s"},
			secrets: []string{"abc123"},
		},
		{
			name:    "logfmt token",
			in:      "level=info token=abc123 msg=started",
			want:    []string{"level=info", "msg=started"},
			secrets: []string{"abc123"},
		},
		{
			name:    "bearer header",
			in:      "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.payload.sig",
			want:    []string{"Authorization:", "Bearer "},
			secrets: []string{"eyJhbGciOiJIUzI1NiJ9.payload.sig"},
		},
		{
			name: "plain text untouched",
			in:   "listening on 127.0.0.1:8080, chain id 31337",
			want: []string{"127.0.0.1:8080", "31337"},
		},
		{
			name: "already redacted stays",
			in:   "dsn=postgres://u:[REDACTED]@h:5432/db",
			want: []string{"u:[REDACTED]@h:5432/db"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := Redact(tc.in)
			for _, w := range tc.want {
				if !strings.Contains(out, w) {
					t.Errorf("Redact(%q) = %q, lost %q", tc.in, out, w)
				}
			}
			for _, s := range tc.secrets {
				if strings.Contains(out, s) {
					t.Errorf("Redact(%q) = %q, leaked %q", tc.in, out, s)
				}
			}
		})
	}
}

func TestRedactErrorDetailPath(t *testing.T) {
	dsn := "postgres://txharbor:hunter2@127.0.0.1:5432/txharbor"
	err := fmt.Errorf("ping failed for %s: connection refused", dsn)
	if got := Redact(err.Error()); strings.Contains(got, "hunter2") || !strings.Contains(got, "[REDACTED]") {
		t.Fatalf("error detail not redacted: %s", got)
	}
}

func TestRedactStartupEchoPath(t *testing.T) {
	line := "config pg=postgres://txharbor:hunter2@127.0.0.1:5432/txharbor rpc=http://127.0.0.1:8545/?token=deadbeef chain_id=31337"
	got := Redact(line)
	for _, secret := range []string{"hunter2", "deadbeef"} {
		if strings.Contains(got, secret) {
			t.Fatalf("startup echo leaks %q: %s", secret, got)
		}
	}
	if !strings.Contains(got, "chain_id=31337") {
		t.Fatalf("startup echo lost diagnostics: %s", got)
	}
}
