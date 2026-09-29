package recovery

import (
	"errors"
	"fmt"
	"strings"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// IsolatedInstanceBinding is the credential-free immutable target binding of
// an already-open recovery instance. Empty fields represent a legacy-null
// binding and are rejected rather than inferred.
type IsolatedInstanceBinding struct {
	TargetGuardKey        string
	TargetRoleFingerprint string
}

// IsolatedTarget is an opaque deployment-derived binding for the disposable
// verification database. Its DSN cannot be supplied by a caller label; only
// the constructor can populate it after comparing deployment configuration
// against the authoritative and currently open targets.
type IsolatedTarget struct {
	dsn               string
	targetGuardKey    string
	roleFingerprint   string
	targetFingerprint string
}

// BoundDSN is for the verification operation's database connection only.
// Core verification must consume this binding rather than a caller-provided
// DSN before claiming that its target is protected.
func (b IsolatedTarget) BoundDSN() string { return b.dsn }

// TargetGuardKey returns the credential-free endpoint identity.
func (b IsolatedTarget) TargetGuardKey() string { return b.targetGuardKey }

// RoleFingerprint returns the credential-free role identity.
func (b IsolatedTarget) RoleFingerprint() string { return b.roleFingerprint }

// TargetFingerprint returns the credential-free endpoint-and-role identity.
func (b IsolatedTarget) TargetFingerprint() string { return b.targetFingerprint }

// BindIsolatedTarget establishes the supported strict topology: the disposable
// target must use the same canonical host and port as the authoritative data
// database, but a different database name, and must not collide with any open
// instance target. Distinct hostnames are rejected because DNS aliases cannot
// be distinguished here. This does not prove that host aliases, proxies,
// failover routing, or external writers are absent; deployment must protect
// that topology and ensure the supplied open-instance inventory is complete.
func BindIsolatedTarget(authoritativeDSN, controlDSN, isolatedDSN string, open []IsolatedInstanceBinding) (IsolatedTarget, error) {
	authoritative, err := strictTarget(authoritativeDSN)
	if err != nil {
		return IsolatedTarget{}, fmt.Errorf("authoritative database target is not safely identifiable: %w", err)
	}
	control, err := strictTarget(controlDSN)
	if err != nil {
		return IsolatedTarget{}, fmt.Errorf("control-store target is not safely identifiable: %w", err)
	}
	isolated, err := strictTarget(isolatedDSN)
	if err != nil {
		return IsolatedTarget{}, fmt.Errorf("isolated database target is not safely identifiable: %w", err)
	}
	if !strings.EqualFold(authoritative.Host, isolated.Host) || authoritative.Port != isolated.Port {
		return IsolatedTarget{}, errors.New("isolated target host/port must exactly match the authoritative target; aliases and distinct hosts are unsupported")
	}
	if authoritative.Database == isolated.Database {
		return IsolatedTarget{}, errors.New("isolated target database must differ from the authoritative database")
	}
	if control.SameDatabase(isolated) {
		return IsolatedTarget{}, errors.New("isolated target must differ from the recovery control-store database")
	}
	key, err := controlstore.TargetGuardKey(isolated)
	if err != nil {
		return IsolatedTarget{}, fmt.Errorf("isolated target identity is invalid: %w", err)
	}
	for i, binding := range open {
		if binding.TargetGuardKey == "" || binding.TargetRoleFingerprint == "" {
			return IsolatedTarget{}, fmt.Errorf("open recovery instance %d has a legacy-null target binding; refusing", i)
		}
		if binding.TargetGuardKey == key {
			return IsolatedTarget{}, errors.New("isolated target collides with an open recovery instance target")
		}
	}
	fingerprint := isolated.DataTargetFingerprint()
	return IsolatedTarget{
		dsn: isolatedDSN, targetGuardKey: key,
		roleFingerprint:   fingerprint.RoleFingerprint,
		targetFingerprint: fingerprint.TargetFingerprint,
	}, nil
}

// AssertIsolatedTarget checks an optional operator assertion against BOTH the
// configured endpoint key and role fingerprint. The asserted DSN is never used
// as a connection target.
func AssertIsolatedTarget(binding IsolatedTarget, assertedDSN string) error {
	asserted, err := strictTarget(assertedDSN)
	if err != nil {
		return fmt.Errorf("asserted isolated target is not safely identifiable: %w", err)
	}
	key, err := controlstore.TargetGuardKey(asserted)
	if err != nil {
		return fmt.Errorf("asserted isolated target identity is invalid: %w", err)
	}
	fingerprint := asserted.DataTargetFingerprint()
	if key != binding.targetGuardKey || fingerprint.RoleFingerprint != binding.roleFingerprint {
		return errors.New("--target-dsn does not match the deployment-configured isolated target endpoint and role")
	}
	return nil
}

func strictTarget(dsn string) (controlstore.DSNTarget, error) {
	target, err := controlstore.ParseDSNTarget(dsn)
	if err != nil {
		return controlstore.DSNTarget{}, err
	}
	if strings.TrimSpace(target.Host) == "" || target.Host != strings.TrimSpace(target.Host) || strings.Contains(target.Host, ",") || target.Port == 0 || strings.TrimSpace(target.Database) == "" || strings.TrimSpace(target.Role) == "" {
		return controlstore.DSNTarget{}, errors.New("host, numeric port, database, and role must be unambiguous and non-empty")
	}
	return target, nil
}
