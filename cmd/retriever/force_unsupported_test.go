//go:build !linux && !darwin

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDumpForceUnsupportedPlatformFailsBeforeMutation(t *testing.T) {
	destination := filepath.Join(t.TempDir(), "dump")
	if err := os.Mkdir(destination, 0o755); err != nil {
		t.Fatalf("mkdir destination: %v", err)
	}
	marker := filepath.Join(destination, "keep")
	if err := os.WriteFile(marker, []byte("keep"), 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	runtime := newTestCommandRuntime(commandOperations{})
	err := runtime.run(context.Background(), []string{
		"dump", "-out", destination, "-force", "-graph", "asset",
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("dump error = %v, want unsupported-platform failure", err)
	}
	if contents, err := os.ReadFile(marker); err != nil || string(contents) != "keep" {
		t.Fatalf("unsupported force changed marker: contents=%q err=%v", contents, err)
	}
}
