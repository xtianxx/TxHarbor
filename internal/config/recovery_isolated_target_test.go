package config

import (
	"strings"
	"testing"
)

func TestLoadRecoveryIsolatedTargetDSN(t *testing.T) {
	t.Run("optional when absent", func(t *testing.T) {
		cfg, err := Load(fakeEnv(baseEnv()))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Recovery.IsolatedTargetDSN != "" {
			t.Fatalf("isolated target defaulted to %q", cfg.Recovery.IsolatedTargetDSN)
		}
	})
	t.Run("configured DSN retained", func(t *testing.T) {
		env := baseEnv()
		want := "postgres://verify:secret@127.0.0.1:5432/verify?sslmode=disable"
		env[EnvRecoveryIsolatedTargetDSN] = want
		cfg, err := Load(fakeEnv(env))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Recovery.IsolatedTargetDSN != want {
			t.Fatalf("IsolatedTargetDSN = %q, want configured value", cfg.Recovery.IsolatedTargetDSN)
		}
	})
	t.Run("malformed DSN refused without echo", func(t *testing.T) {
		env := baseEnv()
		env[EnvRecoveryIsolatedTargetDSN] = "not-a-dsn password=should-not-leak"
		_, err := Load(fakeEnv(env))
		if err == nil {
			t.Fatal("malformed isolated DSN accepted")
		}
		if got := err.Error(); strings.Contains(got, "should-not-leak") {
			t.Fatalf("configuration error leaked DSN contents: %s", got)
		}
	})
}

func TestLoadRecoveryObserverDSN(t *testing.T) {
	t.Run("optional when absent", func(t *testing.T) {
		cfg, err := Load(fakeEnv(baseEnv()))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Recovery.ObserverDSN != "" {
			t.Fatalf("observer DSN defaulted to %q", cfg.Recovery.ObserverDSN)
		}
	})
	t.Run("configured DSN retained", func(t *testing.T) {
		env := baseEnv()
		want := "postgres://observer:secret@127.0.0.1:5432/verify?sslmode=disable"
		env[EnvRecoveryObserverDSN] = want
		cfg, err := Load(fakeEnv(env))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Recovery.ObserverDSN != want {
			t.Fatalf("ObserverDSN = %q, want configured value", cfg.Recovery.ObserverDSN)
		}
	})
	t.Run("malformed DSN refused without echo", func(t *testing.T) {
		env := baseEnv()
		env[EnvRecoveryObserverDSN] = "not-a-dsn password=should-not-leak"
		_, err := Load(fakeEnv(env))
		if err == nil {
			t.Fatal("malformed observer DSN accepted")
		}
		if strings.Contains(err.Error(), "should-not-leak") {
			t.Fatalf("configuration error leaked DSN contents: %s", err)
		}
	})
}
