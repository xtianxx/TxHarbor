//go:build linux

package recovery

import (
	"fmt"
	"os"
)

// createPGCredentialFile creates a private regular file and unlinks its name
// before returning. Its only remaining reference is the open descriptor.
func createPGCredentialFile() (*os.File, error) {
	f, err := os.CreateTemp("", "txharbor-pgpass-*")
	if err != nil {
		return nil, fmt.Errorf("create private PostgreSQL credential file")
	}
	name := f.Name()
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		_ = os.Remove(name)
		return nil, fmt.Errorf("protect private PostgreSQL credential file")
	}
	if err := os.Remove(name); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("unlink private PostgreSQL credential file")
	}
	return f, nil
}
