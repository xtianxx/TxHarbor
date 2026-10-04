//go:build !linux

package recovery

import "os/exec"

// lockPGChildParentDeath is a no-op outside Linux. Other platforms retain the
// existing context/Wait behavior; this does not promise parent-death cleanup.
func lockPGChildParentDeath(_ *exec.Cmd) func() { return func() {} }
