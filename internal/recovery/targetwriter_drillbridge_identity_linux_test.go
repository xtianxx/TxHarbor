//go:build linux && drill

package recovery

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Compile-time pins for the frozen carrier-identity helper contract.
var (
	_ func(string, string) (drillCarrierStoreClass, error)       = drillCarrierStoreClassify
	_ func(drillCarrierStoreClass, string, string, string) error = drillVerifyCarrierIdentity
	_ func(string, string) error                                 = drillVerifyCarrierContainerImage
)

// TestDrillCarrierContentChainMatchesCommittedPayloads re-derives the pinned
// carrier identity chain from the committed evidence bytes: the OCI index
// digest that is the pinned image reference, the linux/amd64 platform manifest
// descriptor inside that index, and the image config digest it points at.
func TestDrillCarrierContentChainMatchesCommittedPayloads(t *testing.T) {
	payloadDir := filepath.Join("..", "..", "docs", "evidence", "016-carrier-image-identity", "payloads")
	readPayload := func(name string) []byte {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(payloadDir, name))
		if err != nil {
			t.Fatalf("read carrier evidence payload %s: %v", name, err)
		}
		return data
	}
	payloadDigest := func(data []byte) string {
		return fmt.Sprintf("sha256:%x", sha256.Sum256(data))
	}

	indexBytes := readPayload("index-manifest.json")
	platformBytes := readPayload("amd64-platform-manifest.json")
	configBytes := readPayload("image-config.json")

	if got := payloadDigest(indexBytes); got != drillCarrierIndexDigest {
		t.Errorf("index-manifest.json digest = %s, want %s", got, drillCarrierIndexDigest)
	}
	if want := "postgres@" + drillCarrierIndexDigest; drillCarrierImage != want {
		t.Errorf("drillCarrierImage = %q, want %q", drillCarrierImage, want)
	}
	if got := payloadDigest(platformBytes); got != drillCarrierPlatformManifestDigest {
		t.Errorf("amd64-platform-manifest.json digest = %s, want %s", got, drillCarrierPlatformManifestDigest)
	}
	if got := payloadDigest(configBytes); got != drillCarrierConfigDigest {
		t.Errorf("image-config.json digest = %s, want %s", got, drillCarrierConfigDigest)
	}

	type payloadPlatform struct {
		OS           string `json:"os"`
		Architecture string `json:"architecture"`
	}
	type payloadDescriptor struct {
		Digest   string           `json:"digest"`
		Size     int64            `json:"size"`
		Platform *payloadPlatform `json:"platform"`
	}

	var index struct {
		Manifests []payloadDescriptor `json:"manifests"`
	}
	if err := json.Unmarshal(indexBytes, &index); err != nil {
		t.Fatalf("index-manifest.json is not valid JSON: %v", err)
	}
	var amd64Entries []payloadDescriptor
	for _, manifest := range index.Manifests {
		if manifest.Platform != nil && manifest.Platform.OS == "linux" && manifest.Platform.Architecture == "amd64" {
			amd64Entries = append(amd64Entries, manifest)
		}
	}
	if len(amd64Entries) != 1 {
		t.Fatalf("index-manifest.json has %d linux/amd64 entries, want exactly 1", len(amd64Entries))
	}
	if got := amd64Entries[0].Digest; got != drillCarrierPlatformManifestDigest {
		t.Errorf("index linux/amd64 digest = %s, want %s", got, drillCarrierPlatformManifestDigest)
	}
	if got := amd64Entries[0].Size; got != int64(len(platformBytes)) {
		t.Errorf("index linux/amd64 size = %d, want %d (amd64-platform-manifest.json length)", got, len(platformBytes))
	}

	var platformManifest struct {
		Config payloadDescriptor `json:"config"`
	}
	if err := json.Unmarshal(platformBytes, &platformManifest); err != nil {
		t.Fatalf("amd64-platform-manifest.json is not valid JSON: %v", err)
	}
	if got := platformManifest.Config.Digest; got != drillCarrierConfigDigest {
		t.Errorf("platform manifest config digest = %s, want %s", got, drillCarrierConfigDigest)
	}
	if got := platformManifest.Config.Size; got != int64(len(configBytes)) {
		t.Errorf("platform manifest config size = %d, want %d (image-config.json length)", got, len(configBytes))
	}

	var imageConfig struct {
		Architecture string `json:"architecture"`
		OS           string `json:"os"`
	}
	if err := json.Unmarshal(configBytes, &imageConfig); err != nil {
		t.Fatalf("image-config.json is not valid JSON: %v", err)
	}
	if imageConfig.OS != "linux" || imageConfig.Architecture != "amd64" {
		t.Errorf("image config platform = %s/%s, want linux/amd64", imageConfig.OS, imageConfig.Architecture)
	}
}

// TestDrillCarrierStoreClassify pins the strict store classification contract:
// the driver status must be a JSON array of two-element string rows, the exact
// "driver-type" key with the exact versioned snapshotter value marks the
// containerd store, and only a structurally valid status without a
// "driver-type" row may fall back to a classic overlay2 store.
func TestDrillCarrierStoreClassify(t *testing.T) {
	inspectDir := filepath.Join("..", "..", "docs", "evidence", "016-carrier-image-identity", "inspect")
	readDriverStatus := func(name string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(inspectDir, name))
		if err != nil {
			t.Fatalf("read carrier evidence driver status %s: %v", name, err)
		}
		return string(data)
	}
	runnerDriverStatus := readDriverStatus("runner-driver-status.json")
	localDriverStatus := readDriverStatus("local-driver-status.json")

	cases := []struct {
		name         string
		driver       string
		driverStatus string
		want         drillCarrierStoreClass
		wantErr      bool
	}{
		{
			name:         "real runner driver status is the classic store",
			driver:       "overlay2",
			driverStatus: runnerDriverStatus,
			want:         drillCarrierStoreClassic,
		},
		{
			name:         "real local driver status is the containerd store",
			driver:       "overlayfs",
			driverStatus: localDriverStatus,
			want:         drillCarrierStoreContainerd,
		},
		{
			name:         "exact containerd marker outranks the classic overlay2 driver",
			driver:       "overlay2",
			driverStatus: `[["driver-type","io.containerd.snapshotter.v1"],["Backing Filesystem","extfs"]]`,
			want:         drillCarrierStoreContainerd,
		},
		{
			name:         "ordinary metadata without a driver-type row is classic overlay2",
			driver:       "overlay2",
			driverStatus: `[["Backing Filesystem","extfs"]]`,
			want:         drillCarrierStoreClassic,
		},
		{
			// Only the exact "driver-type" key takes part in marker
			// detection; a matching value under any other key never does.
			name:         "snapshotter value under a non driver-type key is not a marker",
			driver:       "overlay2",
			driverStatus: `[["other","io.containerd.snapshotter.v1"]]`,
			want:         drillCarrierStoreClassic,
		},
		{
			name:         "unknown containerd driver-type version with a vfs driver is rejected",
			driver:       "vfs",
			driverStatus: `[["driver-type","io.containerd.snapshotter.v10"]]`,
			wantErr:      true,
		},
		{
			name:         "unknown containerd driver-type version with an overlayfs driver is rejected",
			driver:       "overlayfs",
			driverStatus: `[["driver-type","io.containerd.snapshotter.v10"]]`,
			wantErr:      true,
		},
		{
			name:         "non driver-type key does not rescue an unknown driver",
			driver:       "vfs",
			driverStatus: `[["other","io.containerd.snapshotter.v1"]]`,
			wantErr:      true,
		},
		{
			name:         "wrapped containerd marker value is rejected",
			driver:       "overlay2",
			driverStatus: `[["driver-type","prefix-io.containerd.snapshotter.v1-suffix"]]`,
			wantErr:      true,
		},
		{
			name:         "empty containerd marker value is rejected",
			driver:       "overlay2",
			driverStatus: `[["driver-type",""]]`,
			wantErr:      true,
		},
		{
			name:         "bare snapshotter string is not a driver status",
			driver:       "overlay2",
			driverStatus: `io.containerd.snapshotter.v1`,
			wantErr:      true,
		},
		{
			name:         "truncated driver status is rejected",
			driver:       "overlay2",
			driverStatus: `[["driver-type",`,
			wantErr:      true,
		},
		{
			name:         "one element driver status row is rejected",
			driver:       "overlay2",
			driverStatus: `[["driver-type"]]`,
			wantErr:      true,
		},
		{
			name:         "three element driver status row is rejected",
			driver:       "overlay2",
			driverStatus: `[["driver-type","io.containerd.snapshotter.v1","extra"]]`,
			wantErr:      true,
		},
		{
			name:         "number token in a driver status row is rejected",
			driver:       "overlay2",
			driverStatus: `[["driver-type",123]]`,
			wantErr:      true,
		},
		{
			name:         "null token in a driver status row is rejected",
			driver:       "overlay2",
			driverStatus: `[["driver-type",null]]`,
			wantErr:      true,
		},
		{
			name:         "object row in a driver status is rejected",
			driver:       "overlay2",
			driverStatus: `[{"driver-type":"io.containerd.snapshotter.v1"}]`,
			wantErr:      true,
		},
		{
			name:         "null driver status is rejected",
			driver:       "overlay2",
			driverStatus: `null`,
			wantErr:      true,
		},
		{
			name:         "empty driver status string is rejected",
			driver:       "overlay2",
			driverStatus: ``,
			wantErr:      true,
		},
		{
			name:         "empty driver status array is rejected",
			driver:       "overlay2",
			driverStatus: `[]`,
			wantErr:      true,
		},
		{
			name:         "duplicate containerd markers are rejected",
			driver:       "overlayfs",
			driverStatus: `[["driver-type","io.containerd.snapshotter.v1"],["driver-type","io.containerd.snapshotter.v1"]]`,
			wantErr:      true,
		},
		{
			name:         "conflicting containerd marker versions are rejected",
			driver:       "overlayfs",
			driverStatus: `[["driver-type","io.containerd.snapshotter.v1"],["driver-type","io.containerd.snapshotter.v10"]]`,
			wantErr:      true,
		},
		{
			name:         "real classic driver status with an unknown driver is rejected",
			driver:       "vfs",
			driverStatus: runnerDriverStatus,
			wantErr:      true,
		},
		{
			name:    "missing store facts are rejected",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := drillCarrierStoreClassify(tc.driver, tc.driverStatus)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("drillCarrierStoreClassify(%q, %q) = %q, want error", tc.driver, tc.driverStatus, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("drillCarrierStoreClassify(%q, %q) unexpected error: %v", tc.driver, tc.driverStatus, err)
			}
			if got != tc.want {
				t.Fatalf("drillCarrierStoreClassify(%q, %q) = %q, want %q", tc.driver, tc.driverStatus, got, tc.want)
			}
		})
	}
}

// TestDrillCarrierIdentityStoreSemantics pins the store-dependent image
// identity, the fixed platform and the byte-exact repository digest
// provenance.
func TestDrillCarrierIdentityStoreSemantics(t *testing.T) {
	otherRepoDigest := "example.com/other@sha256:" + strings.Repeat("0", 64)
	pinnedRepoDigests := fmt.Sprintf("[%q]", drillCarrierImage)

	cases := []struct {
		name        string
		store       drillCarrierStoreClass
		imageID     string
		platform    string
		repoDigests string
		wantErr     bool
	}{
		{
			name:        "classic store accepts the config digest",
			store:       drillCarrierStoreClassic,
			imageID:     drillCarrierConfigDigest,
			platform:    "linux/amd64",
			repoDigests: pinnedRepoDigests,
		},
		{
			name:        "containerd store accepts the index digest",
			store:       drillCarrierStoreContainerd,
			imageID:     drillCarrierIndexDigest,
			platform:    "linux/amd64",
			repoDigests: pinnedRepoDigests,
		},
		{
			name:        "pinned reference may be one member among many",
			store:       drillCarrierStoreClassic,
			imageID:     drillCarrierConfigDigest,
			platform:    "linux/amd64",
			repoDigests: fmt.Sprintf("[%q,%q]", otherRepoDigest, drillCarrierImage),
		},
		{
			name:        "classic store rejects the index digest",
			store:       drillCarrierStoreClassic,
			imageID:     drillCarrierIndexDigest,
			platform:    "linux/amd64",
			repoDigests: pinnedRepoDigests,
			wantErr:     true,
		},
		{
			name:        "containerd store rejects the config digest",
			store:       drillCarrierStoreContainerd,
			imageID:     drillCarrierConfigDigest,
			platform:    "linux/amd64",
			repoDigests: pinnedRepoDigests,
			wantErr:     true,
		},
		{
			name:        "unknown store class is rejected",
			store:       drillCarrierStoreClass("unknown"),
			imageID:     drillCarrierIndexDigest,
			platform:    "linux/amd64",
			repoDigests: pinnedRepoDigests,
			wantErr:     true,
		},
		{
			name:        "wrong platform is rejected",
			store:       drillCarrierStoreClassic,
			imageID:     drillCarrierConfigDigest,
			platform:    "linux/arm64",
			repoDigests: pinnedRepoDigests,
			wantErr:     true,
		},
		{
			name:        "empty platform is rejected",
			store:       drillCarrierStoreClassic,
			imageID:     drillCarrierConfigDigest,
			platform:    "",
			repoDigests: pinnedRepoDigests,
			wantErr:     true,
		},
		{
			name:        "empty repo digests are rejected",
			store:       drillCarrierStoreClassic,
			imageID:     drillCarrierConfigDigest,
			platform:    "linux/amd64",
			repoDigests: "[]",
			wantErr:     true,
		},
		{
			name:        "null repo digests are rejected",
			store:       drillCarrierStoreClassic,
			imageID:     drillCarrierConfigDigest,
			platform:    "linux/amd64",
			repoDigests: "null",
			wantErr:     true,
		},
		{
			name:        "malformed repo digests are rejected",
			store:       drillCarrierStoreClassic,
			imageID:     drillCarrierConfigDigest,
			platform:    "linux/amd64",
			repoDigests: fmt.Sprintf("[%q", drillCarrierImage),
			wantErr:     true,
		},
		{
			name:        "platform manifest digest is not image provenance",
			store:       drillCarrierStoreClassic,
			imageID:     drillCarrierConfigDigest,
			platform:    "linux/amd64",
			repoDigests: fmt.Sprintf("[%q]", drillCarrierPlatformManifestDigest),
			wantErr:     true,
		},
		{
			name:        "platform manifest digest is rejected by the classic store",
			store:       drillCarrierStoreClassic,
			imageID:     drillCarrierPlatformManifestDigest,
			platform:    "linux/amd64",
			repoDigests: pinnedRepoDigests,
			wantErr:     true,
		},
		{
			name:        "platform manifest digest is rejected by the containerd store",
			store:       drillCarrierStoreContainerd,
			imageID:     drillCarrierPlatformManifestDigest,
			platform:    "linux/amd64",
			repoDigests: pinnedRepoDigests,
			wantErr:     true,
		},
		{
			name:        "truncated pinned reference is not a member",
			store:       drillCarrierStoreClassic,
			imageID:     drillCarrierConfigDigest,
			platform:    "linux/amd64",
			repoDigests: `["postgres@sha256:4ef4dbc9"]`,
			wantErr:     true,
		},
		{
			name:        "wrapped pinned reference is not a member",
			store:       drillCarrierStoreClassic,
			imageID:     drillCarrierConfigDigest,
			platform:    "linux/amd64",
			repoDigests: fmt.Sprintf("[%q]", "x"+drillCarrierImage+"-suffix"),
			wantErr:     true,
		},
		{
			name:        "whitespace padded member is not byte exact",
			store:       drillCarrierStoreClassic,
			imageID:     drillCarrierConfigDigest,
			platform:    "linux/amd64",
			repoDigests: fmt.Sprintf("[%q]", " "+drillCarrierImage),
			wantErr:     true,
		},
		{
			name:        "normalized repository reference is not a member",
			store:       drillCarrierStoreClassic,
			imageID:     drillCarrierConfigDigest,
			platform:    "linux/amd64",
			repoDigests: fmt.Sprintf("[%q]", "docker.io/library/postgres@"+drillCarrierIndexDigest),
			wantErr:     true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := drillVerifyCarrierIdentity(tc.store, tc.imageID, tc.platform, tc.repoDigests)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("drillVerifyCarrierIdentity(%q, %q, %q, %s) = nil, want error", tc.store, tc.imageID, tc.platform, tc.repoDigests)
				}
				return
			}
			if err != nil {
				t.Fatalf("drillVerifyCarrierIdentity(%q, %q, %q, %s) unexpected error: %v", tc.store, tc.imageID, tc.platform, tc.repoDigests, err)
			}
		})
	}
}

// TestDrillCarrierContainerImageConsistency pins the pristine-container image
// check: whitespace-tolerant equality of the verified identity, and refusal of
// any other digest from the same chain.
func TestDrillCarrierContainerImageConsistency(t *testing.T) {
	cases := []struct {
		name           string
		imageID        string
		containerImage string
		wantErr        bool
	}{
		{
			name:           "container image matches the verified identity",
			imageID:        drillCarrierConfigDigest,
			containerImage: drillCarrierConfigDigest,
		},
		{
			name:           "whitespace around the container image is tolerated",
			imageID:        drillCarrierConfigDigest,
			containerImage: " \n " + drillCarrierConfigDigest + "\t",
		},
		{
			name:           "whitespace around both sides is tolerated",
			imageID:        " " + drillCarrierConfigDigest + " ",
			containerImage: "\t" + drillCarrierConfigDigest + "\n",
		},
		{
			name:           "container reporting the index digest is rejected",
			imageID:        drillCarrierConfigDigest,
			containerImage: drillCarrierIndexDigest,
			wantErr:        true,
		},
		{
			name:           "container reporting the config digest for an index identity is rejected",
			imageID:        drillCarrierIndexDigest,
			containerImage: drillCarrierConfigDigest,
			wantErr:        true,
		},
		{
			name:           "empty container image is rejected",
			imageID:        drillCarrierConfigDigest,
			containerImage: "",
			wantErr:        true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := drillVerifyCarrierContainerImage(tc.imageID, tc.containerImage)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("drillVerifyCarrierContainerImage(%q, %q) = nil, want error", tc.imageID, tc.containerImage)
				}
				return
			}
			if err != nil {
				t.Fatalf("drillVerifyCarrierContainerImage(%q, %q) unexpected error: %v", tc.imageID, tc.containerImage, err)
			}
		})
	}
}
