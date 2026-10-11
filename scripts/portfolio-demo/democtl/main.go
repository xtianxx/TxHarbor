// Command democtl is the TxHarbor portfolio demo's dependency-free helper.
//
// It exists so the demo shell scripts can assert on JSON responses, compare
// uint256 values and read the solc release manifest without adding jq, python
// or other runtime dependencies. Standard library only; this is demo tooling,
// not a product component.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"regexp"
	"strconv"
	"strings"
)

const usage = `usage:
  democtl json get [--pretty] <path>      read JSON from stdin; path like a.b[0].c
  democtl json count <path>               number of elements at path (array or object)
  democtl json require <path> <expected>  exit 0 iff the scalar at <path> equals <expected>;
                                          strings compare exactly, JSON numbers as plain decimals
  democtl uint256 eq <a> <b>              big-int equality (decimal or 0x-hex)
  democtl uint256 ge <a> <b>              big-int a >= b
  democtl uint256 hex <value>             print 0x + 64 hex digits (uint256)
  democtl solc-manifest <version>         read solc list.json from stdin;
                                          print path<TAB>longVersion<TAB>sha256
`

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "democtl:", err)
		os.Exit(1)
	}
}

func run(args []string, stdin io.Reader, stdout io.Writer) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	switch args[0] {
	case "json":
		return runJSON(args[1:], stdin, stdout)
	case "uint256":
		return runUint256(args[1:], stdout)
	case "solc-manifest":
		return runSolcManifest(args[1:], stdin, stdout)
	default:
		return fmt.Errorf("unknown command %q\n%s", args[0], usage)
	}
}

func runJSON(args []string, stdin io.Reader, stdout io.Writer) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	switch args[0] {
	case "get":
		rest := args[1:]
		pretty := false
		if len(rest) > 0 && rest[0] == "--pretty" {
			pretty = true
			rest = rest[1:]
		}
		if len(rest) != 1 {
			return errors.New(usage)
		}
		doc, err := decode(stdin)
		if err != nil {
			return err
		}
		v, err := lookup(doc, rest[0])
		if err != nil {
			return err
		}
		return printValue(stdout, v, pretty)
	case "count":
		if len(args) != 2 {
			return errors.New(usage)
		}
		doc, err := decode(stdin)
		if err != nil {
			return err
		}
		v, err := lookup(doc, args[1])
		if err != nil {
			return err
		}
		switch t := v.(type) {
		case []any:
			fmt.Fprintln(stdout, len(t))
		case map[string]any:
			fmt.Fprintln(stdout, len(t))
		default:
			return fmt.Errorf("count: %s is not an array or object", args[1])
		}
		return nil
	case "require":
		if len(args) != 3 {
			return errors.New(usage)
		}
		doc, err := decode(stdin)
		if err != nil {
			return err
		}
		v, err := lookup(doc, args[1])
		if err != nil {
			return err
		}
		if err := requireScalarEqual(args[1], args[2], v); err != nil {
			return err
		}
		return nil
	default:
		return errors.New(usage)
	}
}

func runUint256(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	switch args[0] {
	case "eq", "ge":
		if len(args) != 3 {
			return errors.New(usage)
		}
		a, err := parseUint256(args[1])
		if err != nil {
			return err
		}
		b, err := parseUint256(args[2])
		if err != nil {
			return err
		}
		cmp := a.Cmp(b)
		if args[0] == "eq" && cmp != 0 {
			return fmt.Errorf("uint256 eq: %s != %s", args[1], args[2])
		}
		if args[0] == "ge" && cmp < 0 {
			return fmt.Errorf("uint256 ge: %s < %s", args[1], args[2])
		}
		return nil
	case "hex":
		if len(args) != 2 {
			return errors.New(usage)
		}
		v, err := parseUint256(args[1])
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "0x%064x\n", v)
		return nil
	default:
		return errors.New(usage)
	}
}

type solcManifest struct {
	Builds   []solcBuild       `json:"builds"`
	Releases map[string]string `json:"releases"`
}

type solcBuild struct {
	Path        string `json:"path"`
	Version     string `json:"version"`
	LongVersion string `json:"longVersion"`
	SHA256      string `json:"sha256"`
}

var solcSHA256Re = regexp.MustCompile(`^0x[0-9a-f]{64}$`)

// runSolcManifest resolves a version through manifest.releases to its exact
// build path and prints path<TAB>longVersion<TAB>sha256. It refuses missing
// release entries, unsafe paths, duplicate builds, version mismatches and
// malformed hashes; it never falls back to matching a build by version alone.
func runSolcManifest(args []string, stdin io.Reader, stdout io.Writer) error {
	if len(args) != 1 {
		return errors.New(usage)
	}
	version := args[0]
	raw, err := io.ReadAll(stdin)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	var m solcManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return fmt.Errorf("decode manifest: %w", err)
	}
	path, ok := m.Releases[version]
	if !ok || path == "" {
		return fmt.Errorf("solc version %s: no releases entry", version)
	}
	if strings.ContainsAny(path, "/\\") || strings.Contains(path, "..") {
		return fmt.Errorf("solc version %s: unsafe release path %q", version, path)
	}
	var match *solcBuild
	for i := range m.Builds {
		b := &m.Builds[i]
		if b.Path != path {
			continue
		}
		if match != nil {
			return fmt.Errorf("solc version %s: duplicate builds for %q", version, path)
		}
		match = b
	}
	if match == nil {
		return fmt.Errorf("solc version %s: release path %q has no build", version, path)
	}
	if match.Version != version || !strings.HasPrefix(match.LongVersion, version) {
		return fmt.Errorf("solc version %s: build version/longVersion mismatch (%q, %q)",
			version, match.Version, match.LongVersion)
	}
	if !solcSHA256Re.MatchString(match.SHA256) {
		return fmt.Errorf("solc version %s: malformed sha256 %q", version, match.SHA256)
	}
	fmt.Fprintf(stdout, "%s\t%s\t%s\n", match.Path, match.LongVersion, match.SHA256)
	return nil
}

func decode(r io.Reader) (any, error) {
	dec := json.NewDecoder(r)
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("decode json: %w", err)
	}
	if _, err := dec.Token(); err != io.EOF {
		if err == nil {
			return nil, errors.New("decode json: trailing data after the first JSON value")
		}
		return nil, fmt.Errorf("decode json: trailing data: %w", err)
	}
	return v, nil
}

// lookup navigates a decoded JSON value by a dotted path. Segments may carry
// one array index: "logs[0].topics[0]" or "[0]".
func lookup(v any, path string) (any, error) {
	if path == "" {
		return v, nil
	}
	for _, seg := range strings.Split(path, ".") {
		name, idx, hasIdx, err := splitIndex(seg)
		if err != nil {
			return nil, err
		}
		if name != "" {
			m, ok := v.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("lookup %s: value is not an object", name)
			}
			nv, ok := m[name]
			if !ok {
				return nil, fmt.Errorf("lookup %s: key not found", name)
			}
			v = nv
		}
		if hasIdx {
			arr, ok := v.([]any)
			if !ok {
				return nil, fmt.Errorf("lookup [%d]: value is not an array", idx)
			}
			if idx < 0 || idx >= len(arr) {
				return nil, fmt.Errorf("lookup [%d]: out of range (len %d)", idx, len(arr))
			}
			v = arr[idx]
		}
	}
	return v, nil
}

func splitIndex(seg string) (name string, idx int, hasIdx bool, err error) {
	if strings.HasPrefix(seg, "[") {
		if !strings.HasSuffix(seg, "]") {
			return "", 0, false, fmt.Errorf("bad path segment %q", seg)
		}
		n, convErr := parseIndex(seg[1 : len(seg)-1])
		if convErr != nil {
			return "", 0, false, convErr
		}
		return "", n, true, nil
	}
	open := strings.IndexByte(seg, '[')
	if open < 0 {
		return seg, 0, false, nil
	}
	if !strings.HasSuffix(seg, "]") {
		return "", 0, false, fmt.Errorf("bad path segment %q", seg)
	}
	n, convErr := parseIndex(seg[open+1 : len(seg)-1])
	if convErr != nil {
		return "", 0, false, convErr
	}
	return seg[:open], n, true, nil
}

func parseIndex(s string) (int, error) {
	if !isDecimal(s) {
		return 0, fmt.Errorf("bad array index %q", s)
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("bad array index %q", s)
	}
	return n, nil
}

func printValue(w io.Writer, v any, pretty bool) error {
	if s, ok := scalarString(v); ok {
		fmt.Fprintln(w, s)
		return nil
	}
	var (
		b   []byte
		err error
	)
	if pretty {
		b, err = json.MarshalIndent(v, "", "  ")
	} else {
		b, err = json.Marshal(v)
	}
	if err != nil {
		return fmt.Errorf("marshal value: %w", err)
	}
	b = append(b, '\n')
	_, err = w.Write(b)
	return err
}

// scalarString returns the textual form of a JSON scalar.
func scalarString(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case json.Number:
		return t.String(), true
	case bool:
		if t {
			return "true", true
		}
		return "false", true
	case nil:
		return "null", true
	default:
		return "", false
	}
}

// requireScalarEqual compares the scalar at path with expected. JSON numbers
// compare as plain decimal integers; strings, booleans and null compare
// exactly, so identity/hash strings are never treated as numeric equivalents.
func requireScalarEqual(path, expected string, v any) error {
	switch t := v.(type) {
	case string:
		if t != expected {
			return fmt.Errorf("require: %s = %q, want %q", path, t, expected)
		}
		return nil
	case json.Number:
		got := t.String()
		if !isDecimal(got) || !isDecimal(expected) {
			return fmt.Errorf("require: %s = %s; expected a plain decimal, got %q", path, got, expected)
		}
		a, _ := new(big.Int).SetString(got, 10)
		b, _ := new(big.Int).SetString(expected, 10)
		if a.Cmp(b) != 0 {
			return fmt.Errorf("require: %s = %s, want %s", path, got, expected)
		}
		return nil
	case bool:
		want := "false"
		if t {
			want = "true"
		}
		if want != expected {
			return fmt.Errorf("require: %s = %s, want %s", path, want, expected)
		}
		return nil
	case nil:
		if expected != "null" {
			return fmt.Errorf("require: %s = null, want %q", path, expected)
		}
		return nil
	default:
		return fmt.Errorf("require: %s is not a scalar", path)
	}
}

func isDecimal(s string) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// parseUint256 accepts an explicit decimal (base 10) or 0x-prefixed hex value;
// leading zeros are decimal, never octal.
func parseUint256(s string) (*big.Int, error) {
	t := strings.TrimSpace(s)
	base := 10
	digits := t
	if strings.HasPrefix(t, "0x") || strings.HasPrefix(t, "0X") {
		base = 16
		digits = t[2:]
	}
	if digits == "" {
		return nil, fmt.Errorf("invalid uint256 %q", s)
	}
	for i := range len(digits) {
		c := digits[i]
		ok := c >= '0' && c <= '9'
		if base == 16 {
			ok = ok || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
		}
		if !ok {
			return nil, fmt.Errorf("invalid uint256 %q", s)
		}
	}
	v, ok := new(big.Int).SetString(digits, base)
	if !ok || v.Sign() < 0 || v.BitLen() > 256 {
		return nil, fmt.Errorf("invalid uint256 %q", s)
	}
	return v, nil
}
