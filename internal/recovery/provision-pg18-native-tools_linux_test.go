//go:build linux && drill

// provision-pg18-native-tools_linux_test.go proves the OG01 Phase 1 tool
// authority: the privately provisioned pinned 18.6 clients are sealed with
// carrier provenance and a private expected result, and tampered or forged
// tool values can never arm or start a supervised restore.
package recovery_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/recovery"
)

func mustProvisionDrillNativePGTools(t *testing.T) recovery.DrillNativePGTools {
	t.Helper()
	tools, err := recovery.DrillProvisionNativePGTools(t)
	if err != nil {
		t.Fatalf("provision sealed pinned PostgreSQL 18.6 tools: %v", err)
	}
	if !tools.Valid() {
		t.Fatal("provisioned native PostgreSQL tools are not sealed")
	}
	return tools
}

func mustOpenDrillOriginEndpoint(t *testing.T) recovery.DrillOriginEndpoint {
	t.Helper()
	endpoint, err := recovery.DrillOpenOriginEndpoint()
	if err != nil {
		t.Fatalf("open origin endpoint: %v", err)
	}
	if !endpoint.Valid() {
		t.Fatal("factory origin endpoint is not valid")
	}
	return endpoint
}

func drillProvisionedBinDir(t *testing.T, tools recovery.DrillNativePGTools) string {
	t.Helper()
	dumpPath, ok := tools.DumpPath()
	if !ok {
		t.Fatal("sealed tools did not expose the dump path diagnostic")
	}
	return filepath.Dir(dumpPath)
}

func TestDrillProvisionNativePGToolsCarrierSeal(t *testing.T) {
	tools := mustProvisionDrillNativePGTools(t)
	dumpPath, ok := tools.DumpPath()
	if !ok || !strings.HasSuffix(dumpPath, filepath.Join("bin", "pg_dump")) {
		t.Fatalf("dump path diagnostic is not the provisioned pg_dump: %q ok=%t", dumpPath, ok)
	}
	version, err := exec.Command(dumpPath, "--version").CombinedOutput()
	if err != nil || !strings.Contains(string(version), "(PostgreSQL) 18.6") {
		t.Fatalf("provisioned pg_dump is not the genuine 18.6 client: err=%v out=%q", err, strings.TrimSpace(string(version)))
	}
	dirInfo, err := os.Stat(filepath.Dir(filepath.Dir(dumpPath)))
	if err != nil {
		t.Fatalf("stat tool directory: %v", err)
	}
	if dirInfo.Mode().Perm() != 0o700 {
		t.Fatalf("tool directory is not private 0700: mode=%v", dirInfo.Mode().Perm())
	}

	// No JSON representation carries authority: marshaling is inert and a
	// forged document cannot seal itself into a usable tool set.
	serialized, err := json.Marshal(tools)
	if err != nil || string(serialized) != "{}" {
		t.Fatalf("sealed tools serialized authority material: err=%v doc=%q", err, serialized)
	}
	var forged recovery.DrillNativePGTools
	if err := json.Unmarshal([]byte(`{"sealed":{"dir":"/tmp/forged","restore":{"path":"/tmp/forged/pg_restore"}}}`), &forged); err != nil {
		t.Fatalf("forged tools document: %v", err)
	}
	if forged.Valid() {
		t.Fatal("JSON document constructed sealed native PostgreSQL tools")
	}
	endpoint := mustOpenDrillOriginEndpoint(t)
	opts := recovery.DrillPGRestoreArmOptions{
		TargetDSN:   "postgres://txharbor:txharbor@" + endpoint.Addr() + "/origin_arm_forged?sslmode=disable",
		OperationID: "provision-forged",
	}
	forgedHandle := recovery.DrillNewTargetProcessObservation()
	if err := recovery.DrillArmObservedPGRestore(forgedHandle, endpoint, forged, opts); err == nil {
		t.Fatal("forged tools were sealed into the authority")
	}

	// A wrong-basename script placed over the sealed dump is refused: no
	// caller path/hash/trust input can re-seal it.
	if err := os.WriteFile(dumpPath, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("replace sealed dump with a script: %v", err)
	}
	scriptHandle := recovery.DrillNewTargetProcessObservation()
	if err := recovery.DrillArmObservedPGRestore(scriptHandle, endpoint, tools, opts); err == nil {
		t.Fatal("a script replacing the sealed dump was accepted")
	}
	result, err := recovery.DrillRunObservedPGRestore(context.Background(), recovery.TargetProcessRunner{}, scriptHandle, nil, nil, nil)
	if err == nil || result.Started || result.Outcome != recovery.PGCommandNotStarted {
		t.Fatalf("a refused tool set started a run: %+v err=%v", result, err)
	}
}

func TestDrillArmRefusesReplacedELFAndPreStartTamper(t *testing.T) {
	tools := mustProvisionDrillNativePGTools(t)
	binDir := drillProvisionedBinDir(t, tools)
	dumpBytes, err := os.ReadFile(filepath.Join(binDir, "pg_dump"))
	if err != nil {
		t.Fatalf("read provisioned pg_dump: %v", err)
	}
	// A different (but genuine) ELF under the pg_restore name changes the file
	// identity and content digest, so the sealed set refuses to arm.
	if err := os.WriteFile(filepath.Join(binDir, "pg_restore"), dumpBytes, 0o700); err != nil {
		t.Fatalf("replace provisioned pg_restore with a different ELF: %v", err)
	}
	endpoint := mustOpenDrillOriginEndpoint(t)
	replacedHandle := recovery.DrillNewTargetProcessObservation()
	replacedOpts := recovery.DrillPGRestoreArmOptions{
		TargetDSN:   "postgres://txharbor:txharbor@" + endpoint.Addr() + "/origin_arm_replaced?sslmode=disable",
		OperationID: "provision-replaced",
	}
	if err := recovery.DrillArmObservedPGRestore(replacedHandle, endpoint, tools, replacedOpts); err == nil {
		t.Fatal("a replaced different ELF was accepted by the sealed tool authority")
	}

	// A successfully armed run must revalidate provenance immediately before
	// start: replacing the executable after arm refuses the start.
	fresh := mustProvisionDrillNativePGTools(t)
	freshBin := drillProvisionedBinDir(t, fresh)
	freshEndpoint := mustOpenDrillOriginEndpoint(t)
	handle := recovery.DrillNewTargetProcessObservation()
	freshOpts := recovery.DrillPGRestoreArmOptions{
		TargetDSN:   "postgres://txharbor:txharbor@" + freshEndpoint.Addr() + "/origin_arm_prestart?sslmode=disable",
		OperationID: "provision-prestart",
	}
	if err := recovery.DrillArmObservedPGRestore(handle, freshEndpoint, fresh, freshOpts); err != nil {
		t.Fatalf("arm with sealed tools failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(freshBin, "pg_restore"), []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("replace armed executable before start: %v", err)
	}
	result, err := recovery.DrillRunObservedPGRestore(context.Background(), recovery.TargetProcessRunner{}, handle, nil, nil, nil)
	if err == nil || result.Started || result.Outcome != recovery.PGCommandNotStarted {
		t.Fatalf("pre-start tamper was not refused: %+v err=%v", result, err)
	}
}
