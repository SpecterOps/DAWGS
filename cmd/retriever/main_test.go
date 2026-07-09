package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/specterops/dawgs/retriever"
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

	err = runtime.run(context.Background(), []string{"load"})
	if err == nil || !strings.Contains(err.Error(), "input directory or archive reader is required") {
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

func TestDumpArchiveOutputPreflightBeforeDatabase(t *testing.T) {
	runtime := commandRuntime{
		stdout: &bytes.Buffer{},
		stderr: &bytes.Buffer{},
	}
	dir := t.TempDir()
	privatePath := filepath.Join(dir, "private.key")
	publicPath := filepath.Join(dir, "public.key")
	if err := retriever.GenerateArchiveKeyFiles(privatePath, publicPath); err != nil {
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
