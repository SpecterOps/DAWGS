package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommandRuntimeHelpAndValidation(t *testing.T) {
	runtime := commandRuntime{
		stdout: &bytes.Buffer{},
		stderr: &bytes.Buffer{},
	}
	if err := runtime.run(context.Background(), []string{"help"}); err != nil {
		t.Fatalf("help: %v", err)
	}
	helpOutput := runtime.stdout.(*bytes.Buffer).String()
	for _, command := range []string{"keygen", "dump", "unpack", "load", "verify", "bench"} {
		if !strings.Contains(helpOutput, command) {
			t.Fatalf("help output missing %s command", command)
		}
	}

	err := runtime.run(context.Background(), []string{"unknown"})
	if err == nil || !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("expected unknown command error, got %v", err)
	}

	err = runtime.run(context.Background(), []string{"dump", "-out", t.TempDir(), "-scrub", "full"})
	if err == nil || !strings.Contains(err.Error(), "-scrub full requires") {
		t.Fatalf("expected scrub salt validation error, got %v", err)
	}

	err = runtime.run(context.Background(), []string{"dump", "-out", t.TempDir(), "-shard-size", "1"})
	if err == nil || !strings.Contains(err.Error(), "flag provided but not defined: -shard-size") {
		t.Fatalf("expected shard-size flag rejection, got %v", err)
	}

	err = runtime.run(context.Background(), []string{"dump", "-out", t.TempDir(), "-parquet"})
	if err == nil || !strings.Contains(err.Error(), "database connection is required") {
		t.Fatalf("expected parquet flag to parse before database validation, got %v", err)
	}

	err = runtime.run(context.Background(), []string{"load"})
	if err == nil || !strings.Contains(err.Error(), "input directory or archive path is required") {
		t.Fatalf("expected load input validation error, got %v", err)
	}

	err = runtime.run(context.Background(), []string{"keygen"})
	if err == nil || !strings.Contains(err.Error(), "private key path is required") {
		t.Fatalf("expected keygen validation error, got %v", err)
	}

	err = runtime.run(context.Background(), []string{"unpack"})
	if err == nil || !strings.Contains(err.Error(), "archive path is required") {
		t.Fatalf("expected unpack validation error, got %v", err)
	}

	err = runtime.run(context.Background(), []string{"verify"})
	if err == nil || !strings.Contains(err.Error(), "input directory is required") {
		t.Fatalf("expected verify input validation error, got %v", err)
	}

	err = runtime.run(context.Background(), []string{"bench", "-workers", "0"})
	if err == nil || !strings.Contains(err.Error(), "worker counts must be > 0") {
		t.Fatalf("expected worker validation error, got %v", err)
	}
}

func TestPrepareLoadInputFromArchive(t *testing.T) {
	dir := t.TempDir()
	privatePath := filepath.Join(dir, "private.key")
	publicPath := filepath.Join(dir, "public.key")
	if err := generateArchiveKeyFiles(privatePath, publicPath); err != nil {
		t.Fatalf("generate archive keys: %v", err)
	}
	publicKey, err := loadArchivePublicKey(publicPath)
	if err != nil {
		t.Fatalf("load public key: %v", err)
	}
	dumpDir := writeArchiveFixture(t)
	archivePath := filepath.Join(dir, "dump.tar.pq")
	if err := writeEncryptedCollectionArchive(dumpDir, archivePath, publicKey); err != nil {
		t.Fatalf("write encrypted archive: %v", err)
	}

	cfg := loadOptions{
		ArchivePath:  archivePath,
		IdentityPath: privatePath,
		BatchSize:    defaultBatchSize,
	}
	cfg, cleanup, err := prepareLoadInput(cfg)
	if err != nil {
		t.Fatalf("prepare load input from archive: %v", err)
	}
	if cfg.InputDir == "" {
		t.Fatalf("expected temp input dir")
	}
	if _, err := readManifest(cfg.InputDir); err != nil {
		t.Fatalf("read prepared load manifest: %v", err)
	}
	inputDir := cfg.InputDir
	cleanup()
	if _, err := os.Stat(inputDir); !os.IsNotExist(err) {
		t.Fatalf("expected load temp dir cleanup, got %v", err)
	}
}

func TestPrepareLoadInputValidation(t *testing.T) {
	cfg := loadOptions{
		InputDir:     t.TempDir(),
		ArchivePath:  "archive.tar.pq",
		IdentityPath: "private.key",
		BatchSize:    defaultBatchSize,
	}
	if _, _, err := prepareLoadInput(cfg); err == nil || !strings.Contains(err.Error(), "either -in or -archive") {
		t.Fatalf("expected mutually exclusive input error, got %v", err)
	}

	cfg = loadOptions{
		ArchivePath: "archive.tar.pq",
		BatchSize:   defaultBatchSize,
	}
	if _, _, err := prepareLoadInput(cfg); err == nil || !strings.Contains(err.Error(), "requires -identity") {
		t.Fatalf("expected missing identity error, got %v", err)
	}
	cfg = loadOptions{
		IdentityPath: "private.key",
		BatchSize:    defaultBatchSize,
	}
	if _, _, err := prepareLoadInput(cfg); err == nil || !strings.Contains(err.Error(), "requires -archive") {
		t.Fatalf("expected identity without archive error, got %v", err)
	}
	cfg = loadOptions{
		ArchivePath:  "archive.tar.pq",
		IdentityPath: "private.key",
	}
	cfg.BatchSize = 0
	if _, _, err := prepareLoadInput(cfg); err == nil || !strings.Contains(err.Error(), "batch-size") {
		t.Fatalf("expected batch-size error, got %v", err)
	}
}

func TestDumpArchiveOutputPreflightBeforeDatabase(t *testing.T) {
	runtime := commandRuntime{
		stdout: &bytes.Buffer{},
		stderr: &bytes.Buffer{},
	}
	dir := t.TempDir()
	privatePath := filepath.Join(dir, "private.key")
	publicPath := filepath.Join(dir, "public.key")
	if err := generateArchiveKeyFiles(privatePath, publicPath); err != nil {
		t.Fatalf("generate archive keys: %v", err)
	}
	archivePath := filepath.Join(dir, "dump.tar.pq")
	if err := os.WriteFile(archivePath, []byte("exists"), 0o600); err != nil {
		t.Fatalf("write existing archive: %v", err)
	}

	err := runtime.run(context.Background(), []string{
		"dump",
		"-out", filepath.Join(dir, "dump"),
		"-archive-out", archivePath,
		"-recipient", publicPath,
	})
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected archive preflight error before database open, got %v", err)
	}
}
