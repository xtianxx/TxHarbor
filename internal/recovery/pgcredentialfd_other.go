//go:build !linux

package recovery

import (
	"fmt"
	"os"
)

func createPGCredentialFile() (*os.File, error) {
	return nil, fmt.Errorf("descriptor-backed PostgreSQL credentials are unsupported on this platform")
}
