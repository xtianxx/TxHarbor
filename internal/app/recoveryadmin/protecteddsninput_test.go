package recoveryadmin

import "testing"

func TestRecoveryProtectedDSNInputSupportsEnvironmentAndRejectsConflicts(t *testing.T) {
	env := map[string]string{recoveryTargetDSNEnv: "postgres://safe-source"}
	getenv := func(key string) (string, bool) { v, ok := env[key]; return v, ok }
	got, err := recoveryProtectedDSNInput("", false, getenv, recoveryTargetDSNEnv, "--target-dsn")
	if err != nil || got != env[recoveryTargetDSNEnv] {
		t.Fatalf("environment DSN = %q, %v", got, err)
	}
	if _, err := recoveryProtectedDSNInput("postgres://argv", true, getenv, recoveryTargetDSNEnv, "--target-dsn"); err == nil {
		t.Fatal("accepted conflicting credential sources")
	}
	got, err = recoveryProtectedDSNInput("postgres://legacy", true, nil, recoveryTargetDSNEnv, "--target-dsn")
	if err != nil || got != "postgres://legacy" {
		t.Fatalf("legacy DSN = %q, %v", got, err)
	}
}
