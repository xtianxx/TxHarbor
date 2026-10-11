package main

import (
	"bytes"
	"strings"
	"testing"
)

const sampleDoc = `{
  "request_id": "wr-abc",
  "chain_id": 31337,
  "amount": "1000000000000000000000",
  "ok": true,
  "logs": [
    {"topics": ["0xtopic0", "0xtopic1"], "data": "0x01"}
  ],
  "recovery": {"state": "none", "execution": "not_started"}
}`

func runCLI(t *testing.T, stdin string, args ...string) (string, string, error) {
	t.Helper()
	var out, errBuf bytes.Buffer
	err := run(args, strings.NewReader(stdin), &out)
	return out.String(), errBuf.String(), err
}

func TestJSONGetScalarsAndPaths(t *testing.T) {
	cases := []struct {
		path string
		want string
	}{
		{"request_id", "wr-abc\n"},
		{"chain_id", "31337\n"},
		{"ok", "true\n"},
		{"recovery.state", "none\n"},
		{"logs[0].topics[1]", "0xtopic1\n"},
		{"logs[0].data", "0x01\n"},
	}
	for _, tc := range cases {
		got, _, err := runCLI(t, sampleDoc, "json", "get", tc.path)
		if err != nil {
			t.Fatalf("json get %s: %v", tc.path, err)
		}
		if got != tc.want {
			t.Fatalf("json get %s = %q, want %q", tc.path, got, tc.want)
		}
	}
}

func TestJSONGetCompositeAndCount(t *testing.T) {
	got, _, err := runCLI(t, sampleDoc, "json", "count", "logs")
	if err != nil || got != "1\n" {
		t.Fatalf("json count logs = %q err=%v, want 1", got, err)
	}
	got, _, err = runCLI(t, sampleDoc, "json", "get", "logs[0].topics")
	if err != nil {
		t.Fatalf("json get composite: %v", err)
	}
	if !strings.Contains(got, "0xtopic0") {
		t.Fatalf("composite output missing values: %q", got)
	}
}

func TestJSONRequireStrictDomains(t *testing.T) {
	if _, _, err := runCLI(t, sampleDoc, "json", "require", "chain_id", "31337"); err != nil {
		t.Fatalf("require chain_id: %v", err)
	}
	// JSON numbers compare as plain decimals only.
	if _, _, err := runCLI(t, sampleDoc, "json", "require", "chain_id", "0x7a69"); err == nil {
		t.Fatal("require chain_id=0x7a69 unexpectedly passed")
	}
	if _, _, err := runCLI(t, sampleDoc, "json", "require", "chain_id", "1"); err == nil {
		t.Fatal("require chain_id=1 unexpectedly passed")
	}
	// Strings compare exactly: identity/hash text is never numeric.
	if _, _, err := runCLI(t, sampleDoc, "json", "require", "request_id", "wr-abc"); err != nil {
		t.Fatalf("require request_id: %v", err)
	}
	if _, _, err := runCLI(t, sampleDoc, "json", "require", "request_id", "wr-abd"); err == nil {
		t.Fatal("require wrong request_id unexpectedly passed")
	}
	if _, _, err := runCLI(t, sampleDoc, "json", "require", "recovery", "none"); err == nil {
		t.Fatal("require on composite unexpectedly passed")
	}
}

func TestJSONMissingPathFails(t *testing.T) {
	if _, _, err := runCLI(t, sampleDoc, "json", "get", "nope"); err == nil {
		t.Fatal("missing key unexpectedly passed")
	}
	if _, _, err := runCLI(t, sampleDoc, "json", "get", "logs[7]"); err == nil {
		t.Fatal("out-of-range index unexpectedly passed")
	}
}

func TestUint256ComparisonsAndHex(t *testing.T) {
	big1 := "1000000000000000000000000"
	if _, _, err := runCLI(t, "", "uint256", "eq", big1, big1); err != nil {
		t.Fatalf("uint256 eq identical: %v", err)
	}
	if _, _, err := runCLI(t, "", "uint256", "ge", big1, "999999999999999999999999"); err != nil {
		t.Fatalf("uint256 ge: %v", err)
	}
	if _, _, err := runCLI(t, "", "uint256", "ge", "1", big1); err == nil {
		t.Fatal("uint256 ge smaller-first unexpectedly passed")
	}
	if _, _, err := runCLI(t, "", "uint256", "eq", "0x7a69", "31337"); err != nil {
		t.Fatalf("uint256 eq hex/decimal: %v", err)
	}
	got, _, err := runCLI(t, "", "uint256", "hex", "1000")
	if err != nil {
		t.Fatalf("uint256 hex: %v", err)
	}
	want := "0x00000000000000000000000000000000000000000000000000000000000003e8\n"
	if got != want {
		t.Fatalf("uint256 hex = %q, want %q", got, want)
	}
	if _, _, err := runCLI(t, "", "uint256", "hex", "-1"); err == nil {
		t.Fatal("negative value unexpectedly accepted")
	}
	if _, _, err := runCLI(t, "", "uint256", "hex", "not-a-number"); err == nil {
		t.Fatal("non-numeric value unexpectedly accepted")
	}
}

func TestSolcManifestLookupStrict(t *testing.T) {
	manifest := `{
	  "builds": [
	    {"path":"solc-linux-amd64-v0.8.30+commit.73712a01","version":"0.8.30","longVersion":"0.8.30+commit.73712a01","sha256":"0xf3e987dc6ecebd4bd350c48edcbc320b46cf9e3109bd3fc3d88f1acaf4c428f7"},
	    {"path":"solc-linux-amd64-nightly.2025.1.1+commit.aaaa","version":"0.8.31","longVersion":"0.8.31-nightly","sha256":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	  ],
	  "releases": {"0.8.30":"solc-linux-amd64-v0.8.30+commit.73712a01"}
	}`
	got, _, err := runCLI(t, manifest, "solc-manifest", "0.8.30")
	if err != nil {
		t.Fatalf("solc-manifest: %v", err)
	}
	want := "solc-linux-amd64-v0.8.30+commit.73712a01\t0.8.30+commit.73712a01\t0xf3e987dc6ecebd4bd350c48edcbc320b46cf9e3109bd3fc3d88f1acaf4c428f7\n"
	if got != want {
		t.Fatalf("solc-manifest = %q, want %q", got, want)
	}
	// A version present only as a build (no releases entry) is refused: the
	// lookup never falls back to matching builds by version alone.
	if _, _, err := runCLI(t, manifest, "solc-manifest", "0.8.31"); err == nil {
		t.Fatal("version without releases entry unexpectedly passed")
	}
	dup := `{"builds":[
	  {"path":"p","version":"0.8.30","longVersion":"0.8.30+x","sha256":"0xaa"},
	  {"path":"p","version":"0.8.30","longVersion":"0.8.30+x","sha256":"0xaa"}],
	  "releases":{"0.8.30":"p"}}`
	if _, _, err := runCLI(t, dup, "solc-manifest", "0.8.30"); err == nil {
		t.Fatal("duplicate build path unexpectedly passed")
	}
	trav := `{"builds":[{"path":"../evil","version":"0.8.30","longVersion":"0.8.30+x","sha256":"0xaa"}],"releases":{"0.8.30":"../evil"}}`
	if _, _, err := runCLI(t, trav, "solc-manifest", "0.8.30"); err == nil {
		t.Fatal("path traversal unexpectedly passed")
	}
	bad := `{"builds":[{"path":"p","version":"0.8.30","longVersion":"0.8.30+x","sha256":"0xzz"}],"releases":{"0.8.30":"p"}}`
	if _, _, err := runCLI(t, bad, "solc-manifest", "0.8.30"); err == nil {
		t.Fatal("malformed sha256 unexpectedly passed")
	}
	mism := `{"builds":[{"path":"p","version":"0.8.29","longVersion":"0.8.29+x","sha256":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}],"releases":{"0.8.30":"p"}}`
	if _, _, err := runCLI(t, mism, "solc-manifest", "0.8.30"); err == nil {
		t.Fatal("version mismatch unexpectedly passed")
	}
}

func TestJSONDecodeRejectsTrailingData(t *testing.T) {
	if _, _, err := runCLI(t, `{"a":1} {"b":2}`, "json", "get", "a"); err == nil {
		t.Fatal("trailing second object unexpectedly passed")
	}
	if _, _, err := runCLI(t, `{"a":1} garbage`, "json", "get", "a"); err == nil {
		t.Fatal("trailing garbage unexpectedly passed")
	}
	if _, _, err := runCLI(t, "{\"a\":1}\n", "json", "get", "a"); err != nil {
		t.Fatalf("trailing whitespace rejected: %v", err)
	}
}

func TestJSONArrayIndexStrictDecimal(t *testing.T) {
	for _, path := range []string{"logs[1x]", "logs[]", "logs[-1]", "logs[+1]"} {
		if _, _, err := runCLI(t, sampleDoc, "json", "get", path); err == nil {
			t.Fatalf("index %q unexpectedly passed", path)
		}
	}
}

func TestUint256BaseRules(t *testing.T) {
	// Leading zeros are decimal, not octal: 007 == 7.
	if _, _, err := runCLI(t, "", "uint256", "eq", "007", "7"); err != nil {
		t.Fatalf("decimal leading zeros: %v", err)
	}
	if _, _, err := runCLI(t, "", "uint256", "eq", "0x10", "16"); err != nil {
		t.Fatalf("hex vs decimal: %v", err)
	}
	for _, bad := range []string{"+1", "-1", "1_000", "1junk", "0x", " "} {
		if _, _, err := runCLI(t, "", "uint256", "eq", bad, "1"); err == nil {
			t.Fatalf("uint256 %q unexpectedly passed", bad)
		}
	}
}

func TestDispatchErrors(t *testing.T) {
	if _, _, err := runCLI(t, ""); err == nil {
		t.Fatal("empty args unexpectedly passed")
	}
	if _, _, err := runCLI(t, "", "bogus"); err == nil {
		t.Fatal("unknown command unexpectedly passed")
	}
}
