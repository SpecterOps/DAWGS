package main

import (
	"bytes"
	"context"
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

	err := runtime.run(context.Background(), []string{"unknown"})
	if err == nil || !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("expected unknown command error, got %v", err)
	}

	err = runtime.run(context.Background(), []string{"dump", "-out", t.TempDir(), "-scrub", "full"})
	if err == nil || !strings.Contains(err.Error(), "-scrub full requires") {
		t.Fatalf("expected scrub salt validation error, got %v", err)
	}

	err = runtime.run(context.Background(), []string{"load"})
	if err == nil || !strings.Contains(err.Error(), "input directory is required") {
		t.Fatalf("expected load input validation error, got %v", err)
	}

	err = runtime.run(context.Background(), []string{"bench", "-workers", "0"})
	if err == nil || !strings.Contains(err.Error(), "worker counts must be > 0") {
		t.Fatalf("expected worker validation error, got %v", err)
	}
}
