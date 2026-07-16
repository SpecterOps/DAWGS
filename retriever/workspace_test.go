package retriever

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalCollectionWorkspacePublishesAtomically(t *testing.T) {
	root := filepath.Join(t.TempDir(), "collection")
	workspace := newLocalCollectionWorkspace(root, false)
	if err := workspace.Prepare(context.Background()); err != nil {
		t.Fatalf("prepare workspace: %v", err)
	}

	publishedPath, err := workspace.Publish(context.Background(), "metadata/result.json", []byte("result\n"))
	if err != nil {
		t.Fatalf("publish artifact: %v", err)
	}
	if publishedPath != filepath.Join(root, "metadata", "result.json") {
		t.Fatalf("published path = %q", publishedPath)
	}
	if payload, err := os.ReadFile(publishedPath); err != nil {
		t.Fatalf("read published artifact: %v", err)
	} else if string(payload) != "result\n" {
		t.Fatalf("published payload = %q", payload)
	}
	if _, err := os.Stat(publishedPath + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("staged artifact remains after publish: %v", err)
	}
}

func TestLocalCollectionWorkspaceRejectsUnsafePaths(t *testing.T) {
	workspace := newLocalCollectionWorkspace(t.TempDir(), false)
	for _, relativePath := range []string{
		"",
		"/absolute",
		"../parent",
		"graphs/../escaped",
		`graphs\windows`,
	} {
		t.Run(relativePath, func(t *testing.T) {
			if _, err := workspace.Stage(context.Background(), relativePath); err == nil {
				t.Fatalf("expected workspace path %q to be rejected", relativePath)
			}
		})
	}
}
