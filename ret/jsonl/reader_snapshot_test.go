package jsonl

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/specterops/dawgs/ret/entity"
)

func TestReadNodesVisitsOnlyVerifiedSnapshotAfterPathReplacement(t *testing.T) {
	root := t.TempDir()
	artifact := writeNodeArtifact(t, root, "nodes.jsonl", []entity.Node{{SourceID: "old-1"}, {SourceID: "old-2"}})
	replacement := filepath.Join(root, "replacement.jsonl")
	_ = writeNodeArtifact(t, root, "replacement.jsonl", []entity.Node{{SourceID: "new-1"}, {SourceID: "new-2"}})

	afterVerifiedSnapshotForTest = func() {
		if err := os.Rename(replacement, filepath.Join(root, artifact.Path)); err != nil {
			t.Fatalf("replace artifact: %v", err)
		}
	}
	defer func() { afterVerifiedSnapshotForTest = nil }()

	var visited []string
	if err := ReadNodes(root, artifact, func(node entity.Node) error {
		visited = append(visited, node.SourceID)
		return nil
	}); err != nil {
		t.Fatalf("read nodes: %v", err)
	}
	if got, want := visited, []string{"old-1", "old-2"}; !equalStrings(got, want) {
		t.Fatalf("visited nodes = %v, want verified snapshot %v", got, want)
	}
}

func TestReadRelationshipsHasNoPartialVisitsAfterPathReplacement(t *testing.T) {
	root := t.TempDir()
	artifact := writeRelationshipArtifact(t, root, "relationships.jsonl", []entity.Relationship{{StartID: "old-1", EndID: "old-2", Kind: "Old"}, {StartID: "old-2", EndID: "old-3", Kind: "Old"}})
	replacement := filepath.Join(root, "replacement.jsonl")
	if err := os.WriteFile(replacement, []byte("{\"start_id\":\"new-1\",\"end_id\":\"new-2\",\"kind\":\"New\"}\n{malformed}\n"), 0o600); err != nil {
		t.Fatalf("write replacement: %v", err)
	}

	afterVerifiedSnapshotForTest = func() {
		if err := os.Rename(replacement, filepath.Join(root, artifact.Path)); err != nil {
			t.Fatalf("replace artifact: %v", err)
		}
	}
	defer func() { afterVerifiedSnapshotForTest = nil }()

	var visited []string
	if err := ReadRelationships(root, artifact, func(relationship entity.Relationship) error {
		visited = append(visited, relationship.StartID)
		return nil
	}); err != nil {
		t.Fatalf("read relationships: %v", err)
	}
	if got, want := visited, []string{"old-1", "old-2"}; !equalStrings(got, want) {
		t.Fatalf("visited relationships = %v, want verified snapshot %v", got, want)
	}
}

func writeNodeArtifact(t *testing.T, root, path string, nodes []entity.Node) NodeArtifact {
	t.Helper()
	temporary := filepath.Join(root, path+".tmp")
	artifact, err := WriteNodes(temporary, path, Config{Enabled: true, Codec: CodecNone}, nodes)
	if err != nil {
		t.Fatalf("write nodes: %v", err)
	}
	if err := os.Rename(temporary, filepath.Join(root, path)); err != nil {
		t.Fatalf("install nodes: %v", err)
	}
	return artifact
}

func writeRelationshipArtifact(t *testing.T, root, path string, relationships []entity.Relationship) RelationshipArtifact {
	t.Helper()
	temporary := filepath.Join(root, path+".tmp")
	artifact, err := WriteRelationships(temporary, path, Config{Enabled: true, Codec: CodecNone}, relationships)
	if err != nil {
		t.Fatalf("write relationships: %v", err)
	}
	if err := os.Rename(temporary, filepath.Join(root, path)); err != nil {
		t.Fatalf("install relationships: %v", err)
	}
	return artifact
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}
