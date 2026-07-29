package jsonl_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/specterops/dawgs/ret/entity"
	"github.com/specterops/dawgs/ret/jsonl"
)

func TestRelationshipJSONLOmitsSourceID(t *testing.T) {
	artifact := writeRelationshipFixture(t, jsonl.Config{Enabled: true, Codec: jsonl.CodecNone})
	line := readOnlyLine(t, artifact.Path)
	if bytes.Contains(line, []byte("source_id")) {
		t.Fatalf("relationship JSONL unexpectedly contains source_id: %s", line)
	}
	if !bytes.Contains(line, []byte(`"start_id":"10"`)) {
		t.Fatalf("relationship JSONL = %s, want start_id", line)
	}
}

func TestNodeRoundTripPreservesKindOrderAndDuplicates(t *testing.T) {
	for _, codec := range []jsonl.Codec{jsonl.CodecNone, jsonl.CodecGzip, jsonl.CodecZstd} {
		t.Run(string(codec), func(t *testing.T) {
			want := entity.Node{SourceID: "1", Kinds: []string{"User", "Admin", "User"}, Properties: map[string]any{"name": "Ada"}}
			root := t.TempDir()
			artifact, err := jsonl.WriteNodes(filepath.Join(root, "node.tmp"), "nodes.jsonl", jsonl.Config{Enabled: true, Codec: codec}, []entity.Node{want})
			if err != nil {
				t.Fatalf("write nodes: %v", err)
			}
			if err := os.Rename(filepath.Join(root, "node.tmp"), filepath.Join(root, artifact.Path)); err != nil {
				t.Fatalf("install artifact: %v", err)
			}

			var got []entity.Node
			if err := jsonl.ReadNodes(root, artifact, func(node entity.Node) error {
				got = append(got, node)
				return nil
			}); err != nil {
				t.Fatalf("read nodes: %v", err)
			}
			if len(got) != 1 || !equalNode(got[0], want) {
				t.Fatalf("round trip nodes = %#v, want %#v", got, want)
			}
		})
	}
}

func TestWriteNodesRecordsStoredAndUncompressedBytes(t *testing.T) {
	root := t.TempDir()
	artifact, err := jsonl.WriteNodes(filepath.Join(root, "nodes.tmp"), "nested/nodes.jsonl", jsonl.Config{Enabled: true, Codec: jsonl.CodecNone}, []entity.Node{{SourceID: "1", Kinds: []string{"User"}}})
	if err != nil {
		t.Fatalf("write nodes: %v", err)
	}
	contents, err := os.ReadFile(filepath.Join(root, "nodes.tmp"))
	if err != nil {
		t.Fatalf("read temporary artifact: %v", err)
	}
	if got, want := artifact.SchemaVersion, jsonl.SchemaVersion; got != want {
		t.Fatalf("schema version = %q, want %q", got, want)
	}
	if got, want := artifact.Count, int64(1); got != want {
		t.Fatalf("count = %d, want %d", got, want)
	}
	if got, want := artifact.UncompressedBytes, int64(len(contents)); got != want {
		t.Fatalf("uncompressed bytes = %d, want %d", got, want)
	}
	if got, want := artifact.StoredBytes, int64(len(contents)); got != want {
		t.Fatalf("stored bytes = %d, want %d", got, want)
	}
	hash := sha256.Sum256(contents)
	if got, want := artifact.SHA256, hex.EncodeToString(hash[:]); got != want {
		t.Fatalf("SHA-256 = %q, want %q", got, want)
	}
	if !bytes.HasSuffix(contents, []byte("\n")) || bytes.Count(contents, []byte("\n")) != 1 {
		t.Fatalf("JSONL contents must have exactly one record newline: %q", contents)
	}
}

func TestWriteNodesRejectsUnsafeRelativePath(t *testing.T) {
	_, err := jsonl.WriteNodes(filepath.Join(t.TempDir(), "nodes.tmp"), "../nodes.jsonl", jsonl.Config{Enabled: true, Codec: jsonl.CodecNone}, nil)
	if err == nil {
		t.Fatal("expected unsafe path error")
	}
}

func TestConfigRejectsUnsupportedCodecAndInvalidLevel(t *testing.T) {
	for _, config := range []jsonl.Config{
		{Enabled: true, Codec: jsonl.Codec("zip")},
		{Enabled: true, Codec: jsonl.CodecGzip, Level: 99},
		{Enabled: true, Codec: jsonl.CodecZstd, Level: 99},
	} {
		if err := config.Validate(); err == nil {
			t.Fatalf("Validate(%+v) unexpectedly succeeded", config)
		}
	}
}

func writeRelationshipFixture(t *testing.T, config jsonl.Config) jsonl.RelationshipArtifact {
	t.Helper()
	root := t.TempDir()
	artifact, err := jsonl.WriteRelationships(filepath.Join(root, "relationships.tmp"), "relationships.jsonl", config, []entity.Relationship{{SourceID: "ignored", StartID: "10", EndID: "11", Kind: "MemberOf"}})
	if err != nil {
		t.Fatalf("write relationships: %v", err)
	}
	if err := os.Rename(filepath.Join(root, "relationships.tmp"), filepath.Join(root, artifact.Path)); err != nil {
		t.Fatalf("install relationships artifact: %v", err)
	}
	artifact.Path = filepath.Join(root, artifact.Path)
	return artifact
}

func readOnlyLine(t *testing.T, path string) []byte {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read artifact: %v", err)
	}
	return bytes.TrimSuffix(contents, []byte("\n"))
}

func equalNode(got, want entity.Node) bool {
	gotJSON, gotErr := json.Marshal(got)
	wantJSON, wantErr := json.Marshal(want)
	return gotErr == nil && wantErr == nil && bytes.Equal(gotJSON, wantJSON)
}
