package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ret"
)

func TestCommandRuntimeHelpListsProductCommands(t *testing.T) {
	runtime := commandRuntime{
		stdout: &bytes.Buffer{},
		stderr: &bytes.Buffer{},
	}
	if err := runtime.run(context.Background(), []string{"help"}); err != nil {
		t.Fatalf("help: %v", err)
	}

	output := runtime.stdout.(*bytes.Buffer).String()
	for _, command := range []string{
		"dump",
		"load",
		"verify-collection",
		"verify-database",
		"pack",
		"unpack",
		"keygen",
		"bench",
	} {
		if !strings.Contains(output, command) {
			t.Fatalf("help output missing %s command", command)
		}
	}
	if strings.Contains(output, "\n  verify  ") {
		t.Fatalf("help output retained ambiguous verify command:\n%s", output)
	}
}

func TestProductCommandsValidateRequiredPathsBeforeExternalWork(t *testing.T) {
	cases := []struct {
		command string
		want    string
	}{
		{command: "dump", want: "output directory"},
		{command: "load", want: "load directory"},
		{command: "verify-collection", want: "collection directory"},
		{command: "verify-database", want: "database verification directory"},
		{command: "pack", want: "collection directory"},
		{command: "unpack", want: "archive path"},
		{command: "keygen", want: "private"},
	}
	for _, test := range cases {
		t.Run(test.command, func(t *testing.T) {
			runtime := newTestCommandRuntime(commandOperations{})
			err := runtime.run(context.Background(), []string{test.command})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("%s error = %v, want containing %q", test.command, err, test.want)
			}
		})
	}
}

func TestProductCommandsRejectRemovedFlagBleed(t *testing.T) {
	cases := [][]string{
		{"dump", "-archive-out", "dump.tar.enc"},
		{"dump", "-recipient", "public.key"},
		{"dump", "-workers", "2"},
		{"load", "-archive", "dump.tar.enc"},
		{"load", "-identity", "private.key"},
		{"load", "-workers", "2"},
		{"unpack", "-force"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			runtime := newTestCommandRuntime(commandOperations{})
			err := runtime.run(context.Background(), args)
			if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
				t.Fatalf("run(%v) error = %v, want undefined flag", args, err)
			}
		})
	}
}

func TestVerifyCollectionDoesNotOpenDatabase(t *testing.T) {
	var opened int
	runtime := newTestCommandRuntime(commandOperations{
		openDatabase: func(context.Context, databaseConfig) (graph.Database, string, error) {
			opened++
			return nil, "", errors.New("database must not open")
		},
		verifyCollection: func(_ context.Context, config ret.VerifyCollectionConfig) (ret.VerifyCollectionResult, error) {
			if config.Directory != "collection" {
				t.Fatalf("verify collection directory = %q", config.Directory)
			}
			return ret.VerifyCollectionResult{GraphCount: 2, NodeCount: 3, RelationshipCount: 4}, nil
		},
	})

	if err := runtime.run(context.Background(), []string{"verify-collection", "-in", "collection"}); err != nil {
		t.Fatalf("verify collection: %v", err)
	}
	if opened != 0 {
		t.Fatalf("database open calls = %d, want 0", opened)
	}
}

func TestVerifyDatabaseOpensDatabaseAndCallsFacade(t *testing.T) {
	var opened, verified int
	runtime := newTestCommandRuntime(commandOperations{
		openDatabase: func(context.Context, databaseConfig) (graph.Database, string, error) {
			opened++
			return nil, "pg", nil
		},
		verifyDatabase: func(_ context.Context, database graph.Database, config ret.VerifyDatabaseConfig) (ret.VerifyDatabaseResult, error) {
			verified++
			if database != nil {
				t.Fatal("test opener should supply nil database")
			}
			if config.Directory != "collection" || config.BatchSize != defaultEntityBatchSize {
				t.Fatalf("verify database config = %+v", config)
			}
			return ret.VerifyDatabaseResult{}, nil
		},
	})

	if err := runtime.run(context.Background(), []string{
		"verify-database",
		"-in", "collection",
		"-connection", "postgresql://example/database",
	}); err != nil {
		t.Fatalf("verify database: %v", err)
	}
	if opened != 1 || verified != 1 {
		t.Fatalf("open calls = %d, verify calls = %d; want 1 each", opened, verified)
	}
}

func TestLoadVerifyDatabaseIsASecondFacadeCall(t *testing.T) {
	var calls []string
	runtime := newTestCommandRuntime(commandOperations{
		openDatabase: successfulTestDatabaseOpen,
		load: func(_ context.Context, _ graph.Database, config ret.LoadConfig) (ret.LoadResult, error) {
			calls = append(calls, "load")
			if config.Directory != "collection" || config.BatchSize != 77 {
				t.Fatalf("load config = %+v", config)
			}
			return ret.LoadResult{}, nil
		},
		verifyDatabase: func(_ context.Context, _ graph.Database, config ret.VerifyDatabaseConfig) (ret.VerifyDatabaseResult, error) {
			calls = append(calls, "verify-database")
			if config.Directory != "collection" || config.BatchSize != 77 {
				t.Fatalf("verify database config = %+v", config)
			}
			return ret.VerifyDatabaseResult{}, nil
		},
	})

	if err := runtime.run(context.Background(), []string{
		"load",
		"-in", "collection",
		"-batch-size", "77",
		"-verify-database",
		"-connection", "postgresql://example/database",
	}); err != nil {
		t.Fatalf("load: %v", err)
	}
	if !reflect.DeepEqual(calls, []string{"load", "verify-database"}) {
		t.Fatalf("operation calls = %v", calls)
	}
}

func TestLoadFailureWarnsToClearBeforeRetryWithoutContinuing(t *testing.T) {
	var calls []string
	runtime := newTestCommandRuntime(commandOperations{
		openDatabase: successfulTestDatabaseOpen,
		load: func(context.Context, graph.Database, ret.LoadConfig) (ret.LoadResult, error) {
			calls = append(calls, "load")
			return ret.LoadResult{}, errors.New("write transaction failed")
		},
		verifyDatabase: func(context.Context, graph.Database, ret.VerifyDatabaseConfig) (ret.VerifyDatabaseResult, error) {
			calls = append(calls, "verify-database")
			return ret.VerifyDatabaseResult{}, nil
		},
	})

	err := runtime.run(context.Background(), []string{
		"load",
		"-in", "collection",
		"-verify-database",
		"-connection", "postgresql://example/database",
	})
	if err == nil || !strings.Contains(err.Error(), "clear") || !strings.Contains(err.Error(), "retry") {
		t.Fatalf("load error = %v, want clear-then-retry guidance", err)
	}
	if !reflect.DeepEqual(calls, []string{"load"}) {
		t.Fatalf("operation calls = %v, want only load", calls)
	}
}

func TestPackUnpackAndKeygenCallIndependentFacadeOperations(t *testing.T) {
	keyDir := t.TempDir()
	privatePath := filepath.Join(keyDir, "private.key")
	publicPath := filepath.Join(keyDir, "public.key")
	if err := ret.Keygen(ret.KeygenConfig{
		PrivateKeyPath: privatePath,
		PublicKeyPath:  publicPath,
	}); err != nil {
		t.Fatalf("generate test keys: %v", err)
	}

	var calls []string
	runtime := newTestCommandRuntime(commandOperations{
		pack: func(_ context.Context, config ret.PackConfig) error {
			calls = append(calls, "pack")
			if config.CollectionDirectory != "collection" || config.ArchivePath != "collection.tar.enc" {
				t.Fatalf("pack config = %+v", config)
			}
			return nil
		},
		unpack: func(_ context.Context, config ret.UnpackConfig) error {
			calls = append(calls, "unpack")
			if config.ArchivePath != "collection.tar.enc" || config.OutputDirectory != "restored" {
				t.Fatalf("unpack config = %+v", config)
			}
			return nil
		},
		keygen: func(config ret.KeygenConfig) error {
			calls = append(calls, "keygen")
			if config.PrivateKeyPath != "new-private.key" || config.PublicKeyPath != "new-public.key" {
				t.Fatalf("keygen config = %+v", config)
			}
			return nil
		},
	})

	if err := runtime.run(context.Background(), []string{
		"pack",
		"-in", "collection",
		"-archive", "collection.tar.enc",
		"-recipient", publicPath,
	}); err != nil {
		t.Fatalf("pack: %v", err)
	}
	if err := runtime.run(context.Background(), []string{
		"unpack",
		"-archive", "collection.tar.enc",
		"-out", "restored",
		"-identity", privatePath,
	}); err != nil {
		t.Fatalf("unpack: %v", err)
	}
	if err := runtime.run(context.Background(), []string{
		"keygen",
		"-private-key", "new-private.key",
		"-public-key", "new-public.key",
	}); err != nil {
		t.Fatalf("keygen: %v", err)
	}
	if !reflect.DeepEqual(calls, []string{"pack", "unpack", "keygen"}) {
		t.Fatalf("operation calls = %v", calls)
	}
}

func TestKeygenRejectsLegacyKeyFlagNames(t *testing.T) {
	runtime := newTestCommandRuntime(commandOperations{})
	err := runtime.run(context.Background(), []string{
		"keygen",
		"-private", "private.key",
		"-public", "public.key",
	})
	if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("keygen error = %v, want undefined legacy flag", err)
	}
}

func TestDumpPassesGraphOrderToFacade(t *testing.T) {
	runtime := newTestCommandRuntime(commandOperations{
		openDatabase: successfulTestDatabaseOpen,
		dump: func(_ context.Context, _ graph.Database, config ret.DumpConfig) (ret.DumpResult, error) {
			if !reflect.DeepEqual(config.Graphs, []string{"second", "first"}) {
				t.Fatalf("dump graphs = %v", config.Graphs)
			}
			return ret.DumpResult{}, nil
		},
	})

	if err := runtime.run(context.Background(), []string{
		"dump",
		"-out", filepath.Join(t.TempDir(), "dump"),
		"-graph", "second",
		"-graph", "first",
		"-connection", "postgresql://example/database",
	}); err != nil {
		t.Fatalf("dump: %v", err)
	}
}

func TestDumpForceResumeFailsBeforeDeletionOrDatabaseOpen(t *testing.T) {
	destination := t.TempDir()
	marker := filepath.Join(destination, "keep")
	if err := os.WriteFile(marker, []byte("keep"), 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	var opened int
	runtime := newTestCommandRuntime(commandOperations{
		openDatabase: func(context.Context, databaseConfig) (graph.Database, string, error) {
			opened++
			return nil, "", nil
		},
	})
	err := runtime.run(context.Background(), []string{
		"dump",
		"-out", destination,
		"-force",
		"-resume",
	})
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("dump error = %v, want force/resume conflict", err)
	}
	if opened != 0 {
		t.Fatalf("database open calls = %d, want 0", opened)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("force/resume removed marker: %v", err)
	}
}

func TestDumpForcePureValidationFailuresPreserveDestination(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "all graphs conflict",
			args: []string{"-graph", "asset", "-all-graphs"},
			want: "-all-graphs cannot be combined",
		},
		{
			name: "duplicate graph",
			args: []string{"-graph", "asset", "-graph", "asset"},
			want: "duplicate graph",
		},
		{
			name: "unsafe graph path",
			args: []string{"-graph", "../asset"},
			want: "safe path segment",
		},
		{
			name: "invalid pprof address",
			args: []string{"-graph", "asset", "-pprof-listen", "not-an-address"},
			want: "invalid pprof listen address",
		},
		{
			name: "non-loopback pprof address",
			args: []string{"-graph", "asset", "-pprof-listen", "0.0.0.0:6060"},
			want: "not loopback",
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			destination := filepath.Join(t.TempDir(), "dump")
			if err := os.Mkdir(destination, 0o755); err != nil {
				t.Fatalf("mkdir destination: %v", err)
			}
			marker := filepath.Join(destination, "keep")
			if err := os.WriteFile(marker, []byte("keep"), 0o600); err != nil {
				t.Fatalf("write marker: %v", err)
			}

			runtime := newTestCommandRuntime(commandOperations{})
			args := append([]string{"dump", "-out", destination, "-force"}, test.args...)
			err := runtime.run(context.Background(), args)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("dump error = %v, want containing %q", err, test.want)
			}
			if contents, err := os.ReadFile(marker); err != nil || string(contents) != "keep" {
				t.Fatalf("pure validation failure changed marker: contents=%q err=%v", contents, err)
			}
		})
	}
}

func TestDumpForceRejectsRepositoryBroadTargetBeforeDeletion(t *testing.T) {
	repositoryRoot, err := findRepositoryRoot()
	if err != nil {
		t.Fatalf("find repository root: %v", err)
	}
	marker := filepath.Join(repositoryRoot, "go.mod")

	runtime := newTestCommandRuntime(commandOperations{})
	err = runtime.run(context.Background(), []string{
		"dump",
		"-out", repositoryRoot,
		"-force",
	})
	if err == nil || !strings.Contains(err.Error(), "unsafe") {
		t.Fatalf("dump error = %v, want unsafe target", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("repository marker missing after rejection: %v", err)
	}
}

func TestDumpForceRejectsStaticIntermediateSymlinkWithoutDeletingTarget(t *testing.T) {
	physicalParent := t.TempDir()
	physicalDestination := filepath.Join(physicalParent, "dump")
	if err := os.Mkdir(physicalDestination, 0o755); err != nil {
		t.Fatalf("mkdir physical destination: %v", err)
	}
	marker := filepath.Join(physicalDestination, "keep")
	if err := os.WriteFile(marker, []byte("keep"), 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	aliasParent := t.TempDir()
	alias := filepath.Join(aliasParent, "alias")
	if err := os.Symlink(physicalParent, alias); err != nil {
		t.Fatalf("create intermediate symlink: %v", err)
	}
	runtime := newTestCommandRuntime(commandOperations{})
	err := runtime.run(context.Background(), []string{
		"dump",
		"-out", filepath.Join(alias, "dump"),
		"-force",
		"-graph", "asset",
	})
	if err == nil || !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("dump error = %v, want intermediate symlink rejection", err)
	}
	if contents, err := os.ReadFile(marker); err != nil || string(contents) != "keep" {
		t.Fatalf("symlink target changed: contents=%q err=%v", contents, err)
	}
}

func TestDumpForceParentSubstitutionAfterPinPreservesBothTrees(t *testing.T) {
	base := t.TempDir()
	parent := filepath.Join(base, "parent")
	movedParent := filepath.Join(base, "approved-parent")
	destination := filepath.Join(parent, "dump")
	if err := os.MkdirAll(destination, 0o755); err != nil {
		t.Fatalf("mkdir destination: %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, "approved"), []byte("approved"), 0o600); err != nil {
		t.Fatalf("write approved marker: %v", err)
	}

	runtime := newTestCommandRuntime(commandOperations{})
	runtime.force.afterParentPinned = func(parentPath string, _ *os.Root) error {
		if err := os.Rename(parentPath, movedParent); err != nil {
			return err
		}
		if err := os.Mkdir(parentPath, 0o755); err != nil {
			return err
		}
		if err := os.Mkdir(filepath.Join(parentPath, "dump"), 0o755); err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(parentPath, "dump", "replacement"), []byte("replacement"), 0o600)
	}
	err := runtime.run(context.Background(), []string{
		"dump", "-out", destination, "-force", "-graph", "asset",
	})
	if err == nil || !strings.Contains(err.Error(), "parent changed") {
		t.Fatalf("dump error = %v, want pinned parent substitution failure", err)
	}
	if _, err := os.Stat(filepath.Join(movedParent, "dump", "approved")); err != nil {
		t.Fatalf("approved tree was not preserved: %v", err)
	}
	if _, err := os.Stat(filepath.Join(parent, "dump", "replacement")); err != nil {
		t.Fatalf("replacement tree was not preserved: %v", err)
	}
}

func TestDumpForceTargetSubstitutionAtQuarantineBoundaryRestoresReplacement(t *testing.T) {
	parentPath := t.TempDir()
	destination := filepath.Join(parentPath, "dump")
	approvedMoved := filepath.Join(parentPath, "approved-moved")
	if err := os.Mkdir(destination, 0o755); err != nil {
		t.Fatalf("mkdir destination: %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, "approved"), []byte("approved"), 0o600); err != nil {
		t.Fatalf("write approved marker: %v", err)
	}

	runtime := newTestCommandRuntime(commandOperations{})
	runtime.force.beforeQuarantine = func(parent *os.Root, name string, _ *os.Root) error {
		if err := parent.Rename(name, "approved-moved"); err != nil {
			return err
		}
		if err := parent.Mkdir(name, 0o755); err != nil {
			return err
		}
		return parent.WriteFile(filepath.Join(name, "replacement"), []byte("replacement"), 0o600)
	}
	err := runtime.run(context.Background(), []string{
		"dump", "-out", destination, "-force", "-graph", "asset",
	})
	if err == nil || !strings.Contains(err.Error(), "substituted") {
		t.Fatalf("dump error = %v, want target substitution failure", err)
	}
	if _, err := os.Stat(filepath.Join(approvedMoved, "approved")); err != nil {
		t.Fatalf("approved tree was not preserved: %v", err)
	}
	if _, err := os.Stat(filepath.Join(destination, "replacement")); err != nil {
		t.Fatalf("replacement tree was not restored: %v", err)
	}
}

func TestDumpForcePostQuarantineSubstitutionRestoresReplacement(t *testing.T) {
	parentPath := t.TempDir()
	destination := filepath.Join(parentPath, "dump")
	if err := os.Mkdir(destination, 0o755); err != nil {
		t.Fatalf("mkdir destination: %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, "approved"), []byte("approved"), 0o600); err != nil {
		t.Fatalf("write approved marker: %v", err)
	}

	var approvedMoved string
	runtime := newTestCommandRuntime(commandOperations{})
	runtime.force.afterQuarantine = func(parent *os.Root, _, quarantine string) error {
		approvedMoved = quarantine + ".approved"
		if err := parent.Rename(quarantine, approvedMoved); err != nil {
			return err
		}
		if err := parent.Mkdir(quarantine, 0o755); err != nil {
			return err
		}
		return parent.WriteFile(filepath.Join(quarantine, "replacement"), []byte("replacement"), 0o600)
	}
	err := runtime.run(context.Background(), []string{
		"dump", "-out", destination, "-force", "-graph", "asset",
	})
	if err == nil || !strings.Contains(err.Error(), "substituted") {
		t.Fatalf("dump error = %v, want post-quarantine substitution failure", err)
	}
	if _, err := os.Stat(filepath.Join(parentPath, approvedMoved, "approved")); err != nil {
		t.Fatalf("approved tree was not preserved: %v", err)
	}
	if _, err := os.Stat(filepath.Join(destination, "replacement")); err != nil {
		t.Fatalf("replacement tree was not restored: %v", err)
	}
}

func TestDumpForceBlockedSubstitutionRestorePreservesEveryObject(t *testing.T) {
	parentPath := t.TempDir()
	destination := filepath.Join(parentPath, "dump")
	if err := os.Mkdir(destination, 0o755); err != nil {
		t.Fatalf("mkdir destination: %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, "approved"), []byte("approved"), 0o600); err != nil {
		t.Fatalf("write approved marker: %v", err)
	}

	runtime := newTestCommandRuntime(commandOperations{})
	runtime.force.beforeQuarantine = func(parent *os.Root, name string, _ *os.Root) error {
		if err := parent.Rename(name, "approved-moved"); err != nil {
			return err
		}
		if err := parent.Mkdir(name, 0o755); err != nil {
			return err
		}
		return parent.WriteFile(filepath.Join(name, "replacement"), []byte("replacement"), 0o600)
	}
	runtime.force.afterQuarantine = func(parent *os.Root, original, _ string) error {
		if err := parent.Mkdir(original, 0o755); err != nil {
			return err
		}
		return parent.WriteFile(filepath.Join(original, "blocker"), []byte("blocker"), 0o600)
	}
	err := runtime.run(context.Background(), []string{
		"dump", "-out", destination, "-force", "-graph", "asset",
	})
	if err == nil || !strings.Contains(err.Error(), "preserving both pathnames") {
		t.Fatalf("dump error = %v, want blocked restoration report", err)
	}
	if _, err := os.Stat(filepath.Join(parentPath, "approved-moved", "approved")); err != nil {
		t.Fatalf("approved tree was not preserved: %v", err)
	}
	if _, err := os.Stat(filepath.Join(destination, "blocker")); err != nil {
		t.Fatalf("blocking tree was not preserved: %v", err)
	}
	quarantines, err := filepath.Glob(filepath.Join(parentPath, ".ret-force-*.preserved"))
	if err != nil {
		t.Fatalf("glob quarantines: %v", err)
	}
	if len(quarantines) != 1 {
		t.Fatalf("quarantines = %v, want preserved replacement", quarantines)
	}
	if _, err := os.Stat(filepath.Join(quarantines[0], "replacement")); err != nil {
		t.Fatalf("quarantined replacement was not preserved: %v", err)
	}
}

func TestDumpForceRejectsExistingDestinationSymlink(t *testing.T) {
	parentPath := t.TempDir()
	physical := filepath.Join(parentPath, "physical")
	if err := os.Mkdir(physical, 0o755); err != nil {
		t.Fatalf("mkdir physical target: %v", err)
	}
	marker := filepath.Join(physical, "keep")
	if err := os.WriteFile(marker, []byte("keep"), 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	destination := filepath.Join(parentPath, "dump")
	if err := os.Symlink(physical, destination); err != nil {
		t.Fatalf("symlink destination: %v", err)
	}

	runtime := newTestCommandRuntime(commandOperations{})
	err := runtime.run(context.Background(), []string{
		"dump", "-out", destination, "-force", "-graph", "asset",
	})
	if err == nil || !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("dump error = %v, want destination symlink rejection", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("symlink target changed: %v", err)
	}
}

func TestDumpForceAbsentTargetCreatesNoTombstoneAndCallsDump(t *testing.T) {
	parent := t.TempDir()
	destination := filepath.Join(parent, "dump")
	var called bool
	runtime := newTestCommandRuntime(commandOperations{
		openDatabase: successfulTestDatabaseOpen,
		dump: func(_ context.Context, _ graph.Database, config ret.DumpConfig) (ret.DumpResult, error) {
			called = true
			if config.Directory != destination {
				t.Fatalf("dump directory = %q, want %q", config.Directory, destination)
			}
			return ret.DumpResult{}, nil
		},
	})
	if err := runtime.run(context.Background(), []string{
		"dump", "-out", destination, "-force", "-graph", "asset",
	}); err != nil {
		t.Fatalf("dump: %v", err)
	}
	if !called {
		t.Fatal("dump operation was not called")
	}
	tombstones, err := filepath.Glob(filepath.Join(parent, ".ret-force-*.preserved"))
	if err != nil {
		t.Fatalf("glob tombstones: %v", err)
	}
	if len(tombstones) != 0 {
		t.Fatalf("absent target created tombstones: %v", tombstones)
	}
}

func TestForcePlatformValidationRejectsUnsupportedName(t *testing.T) {
	err := validateForcePlatform("windows")
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("platform validation error = %v", err)
	}
}

func TestReplaceDumpDestinationRejectsRootHomeAndRepositoryAncestors(t *testing.T) {
	repositoryRoot, err := findRepositoryRoot()
	if err != nil {
		t.Fatalf("find repository root: %v", err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("user home: %v", err)
	}
	root := filepath.Clean(filepath.VolumeName(repositoryRoot) + string(os.PathSeparator))

	for _, target := range []string{
		root,
		home,
		repositoryRoot,
		filepath.Dir(repositoryRoot),
	} {
		t.Run(target, func(t *testing.T) {
			if _, err := replaceDumpDestination(target, forceReplaceOperations{}); err == nil || !strings.Contains(err.Error(), "unsafe") {
				t.Fatalf("replaceDumpDestination(%q) error = %v, want unsafe", target, err)
			}
		})
	}
}

func TestReplaceDumpDestinationReturnsCleanAbsoluteChild(t *testing.T) {
	target := filepath.Join(t.TempDir(), "parent", "..", "dump")
	replacement, err := replaceDumpDestination(target, forceReplaceOperations{})
	if err != nil {
		t.Fatalf("validate replace target: %v", err)
	}
	if !filepath.IsAbs(replacement.destination) || replacement.destination != filepath.Clean(target) {
		t.Fatalf("resolved target = %q, want clean absolute %q", replacement.destination, filepath.Clean(target))
	}
}

func TestDumpForceMovesAsideOnlyExactDestinationBeforeDump(t *testing.T) {
	parent := t.TempDir()
	destination := filepath.Join(parent, "replace")
	sibling := filepath.Join(parent, "keep")
	if err := os.Mkdir(destination, 0o755); err != nil {
		t.Fatalf("mkdir destination: %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, "old"), []byte("old"), 0o600); err != nil {
		t.Fatalf("write old file: %v", err)
	}
	if err := os.WriteFile(sibling, []byte("keep"), 0o600); err != nil {
		t.Fatalf("write sibling: %v", err)
	}

	runtime := newTestCommandRuntime(commandOperations{
		openDatabase: successfulTestDatabaseOpen,
		dump: func(_ context.Context, _ graph.Database, config ret.DumpConfig) (ret.DumpResult, error) {
			if _, err := os.Lstat(config.Directory); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("dump destination still exists before facade call: %v", err)
			}
			return ret.DumpResult{}, nil
		},
	})
	if err := runtime.run(context.Background(), []string{
		"dump",
		"-out", destination,
		"-force",
		"-connection", "postgresql://example/database",
	}); err != nil {
		t.Fatalf("dump: %v", err)
	}
	if contents, err := os.ReadFile(sibling); err != nil || string(contents) != "keep" {
		t.Fatalf("sibling changed: contents=%q err=%v", contents, err)
	}
	tombstones, err := filepath.Glob(filepath.Join(parent, ".ret-force-*.preserved"))
	if err != nil {
		t.Fatalf("glob tombstones: %v", err)
	}
	if len(tombstones) != 1 {
		t.Fatalf("tombstones = %v, want one preserved prior collection", tombstones)
	}
	if contents, err := os.ReadFile(filepath.Join(tombstones[0], "old")); err != nil || string(contents) != "old" {
		t.Fatalf("prior destination changed: contents=%q err=%v", contents, err)
	}
}

func TestProductCommandsReportDatabaseCloseErrors(t *testing.T) {
	closeFailure := errors.New("close failed")
	cases := []struct {
		name         string
		args         []string
		ops          func(error) commandOperations
		wantNoOutput bool
	}{
		{
			name: "dump",
			args: []string{"dump", "-out", filepath.Join(t.TempDir(), "dump"), "-graph", "asset"},
			ops: func(closeErr error) commandOperations {
				return commandOperations{
					dump: func(context.Context, graph.Database, ret.DumpConfig) (ret.DumpResult, error) {
						return ret.DumpResult{}, nil
					},
				}
			},
		},
		{
			name: "load",
			args: []string{"load", "-in", "collection"},
			ops: func(closeErr error) commandOperations {
				return commandOperations{
					load: func(context.Context, graph.Database, ret.LoadConfig) (ret.LoadResult, error) {
						return ret.LoadResult{}, nil
					},
				}
			},
		},
		{
			name: "verify database",
			args: []string{"verify-database", "-in", "collection"},
			ops: func(closeErr error) commandOperations {
				return commandOperations{
					verifyDatabase: func(context.Context, graph.Database, ret.VerifyDatabaseConfig) (ret.VerifyDatabaseResult, error) {
						return ret.VerifyDatabaseResult{}, nil
					},
				}
			},
		},
		{
			name:         "bench",
			args:         []string{"bench", "-graph", "asset"},
			wantNoOutput: true,
			ops: func(closeErr error) commandOperations {
				return commandOperations{}
			},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			database := &closingTestDatabase{closeErr: closeFailure}
			operations := test.ops(closeFailure)
			operations.openDatabase = func(context.Context, databaseConfig) (graph.Database, string, error) {
				return database, "pg", nil
			}
			runtime := newTestCommandRuntime(operations)
			err := runtime.run(context.Background(), test.args)
			if !errors.Is(err, closeFailure) {
				t.Fatalf("command error = %v, want close failure", err)
			}
			if test.wantNoOutput && runtime.stdout.(*bytes.Buffer).Len() != 0 {
				t.Fatalf("command emitted success report before close failure: %q", runtime.stdout)
			}
		})
	}
}

func TestProductCommandJoinsPrimaryAndDatabaseCloseErrors(t *testing.T) {
	primaryFailure := errors.New("dump failed")
	closeFailure := errors.New("close failed")
	database := &closingTestDatabase{closeErr: closeFailure}
	runtime := newTestCommandRuntime(commandOperations{
		openDatabase: func(context.Context, databaseConfig) (graph.Database, string, error) {
			return database, "pg", nil
		},
		dump: func(context.Context, graph.Database, ret.DumpConfig) (ret.DumpResult, error) {
			return ret.DumpResult{}, primaryFailure
		},
	})

	err := runtime.run(context.Background(), []string{
		"dump",
		"-out", filepath.Join(t.TempDir(), "dump"),
		"-graph", "asset",
	})
	if !errors.Is(err, primaryFailure) || !errors.Is(err, closeFailure) {
		t.Fatalf("command error = %v, want joined primary and close failures", err)
	}
}

func TestProductDatabaseCloseUsesNonCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	database := &closingTestDatabase{}
	runtime := newTestCommandRuntime(commandOperations{
		openDatabase: func(context.Context, databaseConfig) (graph.Database, string, error) {
			return database, "pg", nil
		},
		dump: func(context.Context, graph.Database, ret.DumpConfig) (ret.DumpResult, error) {
			cancel()
			return ret.DumpResult{}, context.Canceled
		},
	})

	err := runtime.run(ctx, []string{
		"dump",
		"-out", filepath.Join(t.TempDir(), "dump"),
		"-graph", "asset",
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("command error = %v, want context cancellation", err)
	}
	if len(database.closeContextErrors) != 1 {
		t.Fatalf("close contexts = %d, want 1", len(database.closeContextErrors))
	}
	if err := database.closeContextErrors[0]; err != nil {
		t.Fatalf("database close context was already canceled: %v", err)
	}
}

func TestBenchDatabaseCloseUsesNonCanceledContextAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	database := &closingTestDatabase{}
	runtime := newTestCommandRuntime(commandOperations{
		openDatabase: func(context.Context, databaseConfig) (graph.Database, string, error) {
			cancel()
			return database, "pg", nil
		},
	})

	err := runtime.run(ctx, []string{"bench", "-graph", "asset"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("bench error = %v, want context cancellation", err)
	}
	if len(database.closeContextErrors) != 1 {
		t.Fatalf("close contexts = %d, want 1", len(database.closeContextErrors))
	}
	if err := database.closeContextErrors[0]; err != nil {
		t.Fatalf("database close context was already canceled: %v", err)
	}
}

func newTestCommandRuntime(operations commandOperations) commandRuntime {
	return commandRuntime{
		stdout:     &bytes.Buffer{},
		stderr:     &bytes.Buffer{},
		operations: operations,
	}
}

func successfulTestDatabaseOpen(context.Context, databaseConfig) (graph.Database, string, error) {
	return nil, "pg", nil
}

type closingTestDatabase struct {
	graph.Database
	closeErr           error
	closeContextErrors []error
}

func (s *closingTestDatabase) Close(ctx context.Context) error {
	s.closeContextErrors = append(s.closeContextErrors, ctx.Err())
	return s.closeErr
}

func (s *closingTestDatabase) ReadTransaction(_ context.Context, delegate graph.TransactionDelegate, _ ...graph.TransactionOption) error {
	return delegate(emptyBenchTransaction{})
}
