package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ret"
)

func TestDumpForcePreservesCompletePriorCollectionAndCallsDumpAtOriginalPath(t *testing.T) {
	parent := t.TempDir()
	destination := filepath.Join(parent, "dump")
	nested := filepath.Join(destination, "nested")
	simulatedMount := filepath.Join(destination, "simulated-mount")
	external := t.TempDir()
	externalMarker := filepath.Join(external, "external")
	sibling := filepath.Join(parent, "sibling")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("mkdir nested prior collection: %v", err)
	}
	if err := os.Mkdir(simulatedMount, 0o755); err != nil {
		t.Fatalf("mkdir simulated mount entry: %v", err)
	}
	regular := filepath.Join(nested, "regular")
	if err := os.WriteFile(regular, []byte{0, 1, 2, 3, 255}, 0o640); err != nil {
		t.Fatalf("write regular file: %v", err)
	}
	hardlinkSupported := true
	if err := os.Link(regular, filepath.Join(nested, "hardlink")); err != nil {
		hardlinkSupported = false
	}
	if err := os.WriteFile(filepath.Join(simulatedMount, "mounted-data"), []byte("mounted"), 0o600); err != nil {
		t.Fatalf("write simulated mount data: %v", err)
	}
	if err := os.WriteFile(externalMarker, []byte("external"), 0o600); err != nil {
		t.Fatalf("write external marker: %v", err)
	}
	if err := os.Symlink(external, filepath.Join(destination, "external-link")); err != nil {
		t.Fatalf("create external symlink: %v", err)
	}
	if err := os.WriteFile(sibling, []byte("sibling"), 0o600); err != nil {
		t.Fatalf("write sibling: %v", err)
	}

	dumpCalled := false
	runtime := newTestCommandRuntime(commandOperations{
		openDatabase: successfulTestDatabaseOpen,
		dump: func(_ context.Context, _ graph.Database, config ret.DumpConfig) (ret.DumpResult, error) {
			dumpCalled = true
			if config.Directory != destination {
				t.Fatalf("dump directory = %q, want original path %q", config.Directory, destination)
			}
			if _, err := os.Lstat(config.Directory); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("original destination exists before Dump: %v", err)
			}
			if err := os.Mkdir(config.Directory, 0o755); err != nil {
				return ret.DumpResult{}, err
			}
			return ret.DumpResult{}, os.WriteFile(
				filepath.Join(config.Directory, "fresh"),
				[]byte("fresh"),
				0o600,
			)
		},
	})

	if err := runtime.run(context.Background(), []string{
		"dump", "-out", destination, "-force", "-graph", "asset",
	}); err != nil {
		t.Fatalf("dump force: %v", err)
	}
	if !dumpCalled {
		t.Fatal("ret.Dump was not called")
	}

	tombstone := requireSinglePreservedForceTombstone(t, parent)
	if output := runtime.stderr.(*bytes.Buffer).String(); !strings.Contains(output, tombstone) {
		t.Fatalf("force output %q does not report tombstone %q", output, tombstone)
	}
	requirePreservedPriorCollection(t, tombstone, external, hardlinkSupported)
	if contents, err := os.ReadFile(externalMarker); err != nil || string(contents) != "external" {
		t.Fatalf("external target changed: contents=%q err=%v", contents, err)
	}
	if contents, err := os.ReadFile(sibling); err != nil || string(contents) != "sibling" {
		t.Fatalf("sibling changed: contents=%q err=%v", contents, err)
	}
	if contents, err := os.ReadFile(filepath.Join(destination, "fresh")); err != nil || string(contents) != "fresh" {
		t.Fatalf("fresh dump output missing: contents=%q err=%v", contents, err)
	}
}

func TestDumpForceRejectsPostQuarantineReplacementBeforeExternalWork(t *testing.T) {
	cases := []struct {
		name    string
		create  func(parent *os.Root, original string) error
		require func(t *testing.T, destination string)
	}{
		{
			name: "regular file",
			create: func(parent *os.Root, original string) error {
				return parent.WriteFile(original, []byte("replacement-file"), 0o600)
			},
			require: func(t *testing.T, destination string) {
				t.Helper()
				if contents, err := os.ReadFile(destination); err != nil ||
					string(contents) != "replacement-file" {
					t.Fatalf("replacement file changed: contents=%q err=%v", contents, err)
				}
			},
		},
		{
			name: "directory",
			create: func(parent *os.Root, original string) error {
				if err := parent.Mkdir(original, 0o755); err != nil {
					return err
				}
				return parent.WriteFile(filepath.Join(original, "replacement"), []byte("replacement-dir"), 0o600)
			},
			require: func(t *testing.T, destination string) {
				t.Helper()
				if contents, err := os.ReadFile(filepath.Join(destination, "replacement")); err != nil ||
					string(contents) != "replacement-dir" {
					t.Fatalf("replacement directory changed: contents=%q err=%v", contents, err)
				}
			},
		},
		{
			name: "symlink",
			create: func(parent *os.Root, original string) error {
				return parent.Symlink("replacement-target", original)
			},
			require: func(t *testing.T, destination string) {
				t.Helper()
				if target, err := os.Readlink(destination); err != nil || target != "replacement-target" {
					t.Fatalf("replacement symlink changed: target=%q err=%v", target, err)
				}
			},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			parent := t.TempDir()
			destination := filepath.Join(parent, "dump")
			if err := os.Mkdir(destination, 0o755); err != nil {
				t.Fatalf("mkdir prior collection: %v", err)
			}
			if err := os.WriteFile(filepath.Join(destination, "prior"), []byte("prior"), 0o600); err != nil {
				t.Fatalf("write prior marker: %v", err)
			}

			databaseCalled := false
			dumpCalled := false
			runtime := newTestCommandRuntime(commandOperations{
				openDatabase: func(context.Context, databaseConfig) (graph.Database, string, error) {
					databaseCalled = true
					return nil, "", errors.New("database must not be opened")
				},
				dump: func(context.Context, graph.Database, ret.DumpConfig) (ret.DumpResult, error) {
					dumpCalled = true
					return ret.DumpResult{}, errors.New("dump must not be called")
				},
			})
			runtime.force.afterQuarantine = func(parent *os.Root, original, _ string) error {
				return test.create(parent, original)
			}

			err := runtime.run(context.Background(), []string{
				"dump",
				"-out", destination,
				"-force",
				"-graph", "asset",
				"-pprof-listen", "127.0.0.1:0",
			})
			if err == nil || !strings.Contains(err.Error(), "original destination") {
				t.Fatalf("dump error = %v, want original destination occupancy failure", err)
			}
			if databaseCalled {
				t.Fatal("database was opened after post-quarantine replacement")
			}
			if dumpCalled {
				t.Fatal("ret.Dump was called after post-quarantine replacement")
			}
			if output := runtime.stderr.(*bytes.Buffer).String(); strings.Contains(output, "pprof:") {
				t.Fatalf("pprof started after post-quarantine replacement: %q", output)
			}

			test.require(t, destination)
			tombstone := requireSinglePreservedForceTombstone(t, parent)
			if !strings.Contains(err.Error(), tombstone) {
				t.Fatalf("dump error %q does not report preserved prior collection %q", err, tombstone)
			}
			if contents, readErr := os.ReadFile(filepath.Join(tombstone, "prior")); readErr != nil ||
				string(contents) != "prior" {
				t.Fatalf("prior collection changed: contents=%q err=%v", contents, readErr)
			}
		})
	}
}

func TestReplaceDumpDestinationRestoresAfterTargetRootCloseFailure(t *testing.T) {
	testForceCloseFailureRestoration(t, "target", false)
}

func TestReplaceDumpDestinationRestoresAfterParentRootCloseFailure(t *testing.T) {
	testForceCloseFailureRestoration(t, "parent", false)
}

func TestReplaceDumpDestinationReportsTombstoneAfterParentDirectoryCloseFailure(t *testing.T) {
	testForceCloseFailureRestoration(t, "parent directory handle", true)
}

func testForceCloseFailureRestoration(t *testing.T, failingRole string, expectTombstone bool) {
	t.Helper()
	parent := t.TempDir()
	destination := filepath.Join(parent, "dump")
	if err := os.Mkdir(destination, 0o755); err != nil {
		t.Fatalf("mkdir destination: %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, "old"), []byte("old"), 0o600); err != nil {
		t.Fatalf("write destination marker: %v", err)
	}

	closeFailure := errors.New("injected close failure")
	dumpCalled := false
	runtime := newTestCommandRuntime(commandOperations{
		openDatabase: successfulTestDatabaseOpen,
		dump: func(context.Context, graph.Database, ret.DumpConfig) (ret.DumpResult, error) {
			dumpCalled = true
			return ret.DumpResult{}, nil
		},
	})
	runtime.force = forceReplaceOperations{
		closeRoot: func(role string, root *os.Root) error {
			closeErr := root.Close()
			if role == failingRole {
				return errors.Join(closeErr, closeFailure)
			}
			return closeErr
		},
		closeFile: func(role string, file *os.File) error {
			closeErr := file.Close()
			if role == failingRole {
				return errors.Join(closeErr, closeFailure)
			}
			return closeErr
		},
	}
	err := runtime.run(context.Background(), []string{
		"dump", "-out", destination, "-force", "-graph", "asset",
	})
	if !errors.Is(err, closeFailure) {
		t.Fatalf("replace error = %v, want injected close failure", err)
	}
	if dumpCalled {
		t.Fatal("ret.Dump was called after force close failure")
	}

	tombstones, globErr := filepath.Glob(filepath.Join(parent, ".ret-force-*.preserved"))
	if globErr != nil {
		t.Fatalf("glob tombstones: %v", globErr)
	}
	if expectTombstone {
		if len(tombstones) != 1 {
			t.Fatalf("tombstones = %v, want one preserved tombstone", tombstones)
		}
		if !strings.Contains(err.Error(), tombstones[0]) {
			t.Fatalf("close error %q does not report tombstone %q", err, tombstones[0])
		}
		if contents, readErr := os.ReadFile(filepath.Join(tombstones[0], "old")); readErr != nil ||
			string(contents) != "old" {
			t.Fatalf("preserved tombstone changed: contents=%q err=%v", contents, readErr)
		}
		if _, statErr := os.Lstat(destination); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("destination exists after unrecoverable close failure: %v", statErr)
		}
		return
	}

	if len(tombstones) != 0 {
		t.Fatalf("unexpected tombstones after restoration: %v", tombstones)
	}
	if contents, readErr := os.ReadFile(filepath.Join(destination, "old")); readErr != nil ||
		string(contents) != "old" {
		t.Fatalf("restored prior collection changed: contents=%q err=%v", contents, readErr)
	}
}

func requireSinglePreservedForceTombstone(t *testing.T, parent string) string {
	t.Helper()
	tombstones, err := filepath.Glob(filepath.Join(parent, ".ret-force-*.preserved"))
	if err != nil {
		t.Fatalf("glob preserved tombstones: %v", err)
	}
	if len(tombstones) != 1 {
		t.Fatalf("preserved tombstones = %v, want one", tombstones)
	}
	return tombstones[0]
}

func requirePreservedPriorCollection(
	t *testing.T,
	tombstone string,
	external string,
	hardlinkSupported bool,
) {
	t.Helper()
	regular := filepath.Join(tombstone, "nested", "regular")
	if contents, err := os.ReadFile(regular); err != nil ||
		string(contents) != string([]byte{0, 1, 2, 3, 255}) {
		t.Fatalf("regular file changed: contents=%v err=%v", contents, err)
	}
	if hardlinkSupported {
		regularInfo, err := os.Stat(regular)
		if err != nil {
			t.Fatalf("stat regular file: %v", err)
		}
		hardlinkInfo, err := os.Stat(filepath.Join(tombstone, "nested", "hardlink"))
		if err != nil {
			t.Fatalf("stat hardlink: %v", err)
		}
		if !os.SameFile(regularInfo, hardlinkInfo) {
			t.Fatal("hardlink identity was not preserved")
		}
	}
	if contents, err := os.ReadFile(filepath.Join(tombstone, "simulated-mount", "mounted-data")); err != nil ||
		string(contents) != "mounted" {
		t.Fatalf("simulated mount data changed: contents=%q err=%v", contents, err)
	}
	if target, err := os.Readlink(filepath.Join(tombstone, "external-link")); err != nil || target != external {
		t.Fatalf("external symlink changed: target=%q err=%v", target, err)
	}
}
