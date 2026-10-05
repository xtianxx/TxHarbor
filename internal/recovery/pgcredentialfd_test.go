//go:build linux

package recovery

import (
	"os"
	"strconv"
	"testing"
)

func TestPGCredentialFileIsUnlinkedAndPrivate(t *testing.T) {
	f, err := createPGCredentialFile()
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("credential file mode/type = %v", info.Mode())
	}
	if _, err := os.Stat(f.Name()); !os.IsNotExist(err) {
		t.Fatalf("credential pathname remains reachable: %v", err)
	}
	if _, err := os.Readlink("/proc/self/fd/" + strconv.FormatUint(uint64(f.Fd()), 10)); err != nil {
		t.Fatalf("anonymous fd is unavailable: %v", err)
	}
}
