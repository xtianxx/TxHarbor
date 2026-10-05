package recoveryadmin

import (
	"fmt"
	"strings"
)

const (
	recoveryTargetDSNEnv = "TXHARBOR_RECOVERY_TARGET_DSN"
	recoveryBrokerDSNEnv = "TXHARBOR_RECOVERY_BROKER_DSN"
)

// recoveryProtectedDSNInput accepts a DSN through deployment environment
// configuration (the safe, non-argv route), retaining the legacy flag route.
// Supplying both is rejected to avoid ambiguous credential sources.
func recoveryProtectedDSNInput(flagValue string, flagSet bool, getenv func(string) (string, bool), envKey, flagName string) (string, error) {
	fromEnv, envSet := "", false
	if getenv != nil {
		fromEnv, envSet = getenv(envKey)
	}
	fromEnv = strings.TrimSpace(fromEnv)
	fromFlag := strings.TrimSpace(flagValue)
	if flagSet && envSet && fromEnv != "" {
		return "", fmt.Errorf("%s and %s are both supplied; choose one credential source", flagName, envKey)
	}
	if fromFlag != "" {
		return flagValue, nil
	}
	if envSet {
		return fromEnv, nil
	}
	return "", nil
}
