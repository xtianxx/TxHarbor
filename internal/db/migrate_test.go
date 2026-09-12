package db

import (
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"
)

func TestEmbeddedMigrationsPresent(t *testing.T) {
	if Migrations == nil {
		t.Fatal("embedded migrations FS is nil")
	}
	names, err := fs.Glob(Migrations, "*.sql")
	if err != nil {
		t.Fatalf("glob embedded migrations: %v", err)
	}
	if len(names) == 0 {
		t.Fatal("no embedded migration files")
	}
	foundBaseline := false
	for _, name := range names {
		if name == "000001_baseline.sql" {
			foundBaseline = true
		}
	}
	if !foundBaseline {
		t.Fatalf("baseline migration missing, got %v", names)
	}
}

func TestEmbeddedBaselineIsTransactionalGooseFile(t *testing.T) {
	data, err := fs.ReadFile(Migrations, "000001_baseline.sql")
	if err != nil {
		t.Fatalf("read baseline: %v", err)
	}
	content := string(data)
	for _, want := range []string{"-- +goose Up", "-- +goose Down"} {
		if !strings.Contains(content, want) {
			t.Errorf("baseline missing %q", want)
		}
	}
	if strings.Contains(content, "NO TRANSACTION") {
		t.Error("baseline must not disable transactions")
	}
}

func TestMigrationFilesSortedByVersion(t *testing.T) {
	fsys := fstest.MapFS{
		"000010_ten.sql":  &fstest.MapFile{Data: []byte("-- +goose Up\n")},
		"000002_two.sql":  &fstest.MapFile{Data: []byte("-- +goose Up\n")},
		"000001_one.sql":  &fstest.MapFile{Data: []byte("-- +goose Up\n")},
		"000100_last.sql": &fstest.MapFile{Data: []byte("-- +goose Up\n")},
	}
	files, err := MigrationFiles(fsys)
	if err != nil {
		t.Fatalf("MigrationFiles() error = %v", err)
	}
	want := []int64{1, 2, 10, 100}
	if len(files) != len(want) {
		t.Fatalf("got %d files, want %d", len(files), len(want))
	}
	for i, f := range files {
		if f.Version != want[i] {
			t.Errorf("files[%d].Version = %d, want %d", i, f.Version, want[i])
		}
	}
}

func TestMigrationFilesRejectsBadInput(t *testing.T) {
	cases := []struct {
		name string
		fsys fstest.MapFS
	}{
		{
			name: "no files",
			fsys: fstest.MapFS{"README.md": &fstest.MapFile{Data: []byte("x")}},
		},
		{
			name: "non numeric version",
			fsys: fstest.MapFS{"abc_bad.sql": &fstest.MapFile{Data: []byte("x")}},
		},
		{
			name: "zero version",
			fsys: fstest.MapFS{"000000_zero.sql": &fstest.MapFile{Data: []byte("x")}},
		},
		{
			name: "duplicate version",
			fsys: fstest.MapFS{
				"1_first.sql":    &fstest.MapFile{Data: []byte("x")},
				"000001_dup.sql": &fstest.MapFile{Data: []byte("x")},
			},
		},
		{
			name: "missing name separator",
			fsys: fstest.MapFS{"000001.sql": &fstest.MapFile{Data: []byte("x")}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := MigrationFiles(tc.fsys); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
