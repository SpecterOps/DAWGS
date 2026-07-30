package jsonl_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
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

			got, err := jsonl.ReadNodes(root, artifact)
			if err != nil {
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

func TestWriteRejectsInvalidEntitiesBeforeOpeningTemporaryFile(t *testing.T) {
	for name, write := range map[string]func(string) error{
		"node": func(temporary string) error {
			_, err := jsonl.WriteNodes(temporary, "nodes.jsonl", jsonl.Config{Enabled: true, Codec: jsonl.CodecNone}, []entity.Node{{}})
			return err
		},
		"relationship": func(temporary string) error {
			_, err := jsonl.WriteRelationships(temporary, "relationships.jsonl", jsonl.Config{Enabled: true, Codec: jsonl.CodecNone}, []entity.Relationship{{}})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			temporary := filepath.Join(t.TempDir(), name+".tmp")

			if err := write(temporary); err == nil {
				t.Fatal("expected invalid entity error")
			}
			if _, err := os.Stat(temporary); !os.IsNotExist(err) {
				t.Fatalf("temporary file created before entity validation: %v", err)
			}
		})
	}
}

func TestWritePreflightErrorsWinOverInvalidEntities(t *testing.T) {
	writers := []struct {
		name  string
		write func(string, string, jsonl.Config) error
	}{
		{
			name: "nodes",
			write: func(temporary, relative string, config jsonl.Config) error {
				_, err := jsonl.WriteNodes(temporary, relative, config, []entity.Node{{}})
				return err
			},
		},
		{
			name: "relationships",
			write: func(temporary, relative string, config jsonl.Config) error {
				_, err := jsonl.WriteRelationships(temporary, relative, config, []entity.Relationship{{}})
				return err
			},
		},
	}
	preflights := []struct {
		name      string
		config    jsonl.Config
		temporary func(*testing.T, string) string
		relative  string
		wantError string
	}{
		{
			name:      "disabled",
			config:    jsonl.Config{Codec: jsonl.CodecNone},
			temporary: func(_ *testing.T, root string) string { return filepath.Join(root, "disabled.tmp") },
			relative:  "artifact.jsonl",
			wantError: "disabled",
		},
		{
			name:      "invalid config",
			config:    jsonl.Config{Enabled: true, Codec: jsonl.Codec("zip")},
			temporary: func(_ *testing.T, root string) string { return filepath.Join(root, "config.tmp") },
			relative:  "artifact.jsonl",
			wantError: "unsupported JSONL codec",
		},
		{
			name:      "relative temporary path",
			config:    jsonl.Config{Enabled: true, Codec: jsonl.CodecNone},
			temporary: relativeTemporaryPath,
			relative:  "artifact.jsonl",
			wantError: "must be absolute",
		},
		{
			name:      "unsafe final path",
			config:    jsonl.Config{Enabled: true, Codec: jsonl.CodecNone},
			temporary: func(_ *testing.T, root string) string { return filepath.Join(root, "path.tmp") },
			relative:  "../artifact.jsonl",
			wantError: "escapes collection",
		},
	}

	for _, writer := range writers {
		for _, preflight := range preflights {
			t.Run(writer.name+"/"+preflight.name, func(t *testing.T) {
				temporary := preflight.temporary(t, t.TempDir())

				err := writer.write(temporary, preflight.relative, preflight.config)

				if err == nil || !strings.Contains(err.Error(), preflight.wantError) {
					t.Fatalf("write error = %v, want error containing %q", err, preflight.wantError)
				}
				if _, err := os.Stat(temporary); !os.IsNotExist(err) {
					t.Fatalf("temporary file created during failed preflight: %v", err)
				}
			})
		}
	}
}

func TestWriteAndReadEmptyArtifactsAcrossCodecs(t *testing.T) {
	writers := []struct {
		name  string
		write func(*testing.T, string, string, jsonl.Config, bool) emptyArtifactFacts
	}{
		{name: "nodes", write: writeEmptyNodes},
		{name: "relationships", write: writeEmptyRelationships},
	}

	for _, writer := range writers {
		for _, codec := range []jsonl.Codec{jsonl.CodecNone, jsonl.CodecGzip, jsonl.CodecZstd} {
			for _, explicitEmpty := range []bool{false, true} {
				sliceName := "nil"
				if explicitEmpty {
					sliceName = "empty"
				}
				t.Run(writer.name+"/"+string(codec)+"/"+sliceName, func(t *testing.T) {
					root := t.TempDir()
					relative := "nested/" + writer.name + ".jsonl"
					facts := writer.write(t, root, relative, jsonl.Config{Enabled: true, Codec: codec}, explicitEmpty)
					stored, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relative)))
					if err != nil {
						t.Fatalf("read installed artifact: %v", err)
					}
					hash := sha256.Sum256(stored)

					if facts.path != relative {
						t.Fatalf("artifact path = %q, want clean path %q", facts.path, relative)
					}
					if facts.count != 0 || facts.uncompressedBytes != 0 || facts.readCount != 0 {
						t.Fatalf("empty artifact facts = count %d, uncompressed %d, read count %d; want all zero", facts.count, facts.uncompressedBytes, facts.readCount)
					}
					if facts.storedBytes != int64(len(stored)) {
						t.Fatalf("stored bytes = %d, want %d", facts.storedBytes, len(stored))
					}
					if want := hex.EncodeToString(hash[:]); facts.sha256 != want {
						t.Fatalf("SHA-256 = %q, want %q", facts.sha256, want)
					}
				})
			}
		}
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

type emptyArtifactFacts struct {
	path, sha256                          string
	count, uncompressedBytes, storedBytes int64
	readCount                             int
}

func relativeTemporaryPath(t *testing.T, root string) string {
	t.Helper()
	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	temporary, err := filepath.Rel(workingDirectory, filepath.Join(root, "relative.tmp"))
	if err != nil {
		t.Fatalf("make relative temporary path: %v", err)
	}
	return temporary
}

func writeEmptyNodes(t *testing.T, root, relative string, config jsonl.Config, explicitEmpty bool) emptyArtifactFacts {
	t.Helper()
	var nodes []entity.Node
	if explicitEmpty {
		nodes = []entity.Node{}
	}
	temporary := filepath.Join(root, "nodes.tmp")
	artifact, err := jsonl.WriteNodes(temporary, relative, config, nodes)
	if err != nil {
		t.Fatalf("write empty nodes: %v", err)
	}
	installArtifact(t, root, temporary, artifact.Path)
	readNodes, err := jsonl.ReadNodes(root, artifact)
	if err != nil {
		t.Fatalf("read empty nodes: %v", err)
	}
	return emptyArtifactFacts{
		path: artifact.Path, sha256: artifact.SHA256, count: artifact.Count,
		uncompressedBytes: artifact.UncompressedBytes, storedBytes: artifact.StoredBytes, readCount: len(readNodes),
	}
}

func writeEmptyRelationships(t *testing.T, root, relative string, config jsonl.Config, explicitEmpty bool) emptyArtifactFacts {
	t.Helper()
	var relationships []entity.Relationship
	if explicitEmpty {
		relationships = []entity.Relationship{}
	}
	temporary := filepath.Join(root, "relationships.tmp")
	artifact, err := jsonl.WriteRelationships(temporary, relative, config, relationships)
	if err != nil {
		t.Fatalf("write empty relationships: %v", err)
	}
	installArtifact(t, root, temporary, artifact.Path)
	readRelationships, err := jsonl.ReadRelationships(root, artifact)
	if err != nil {
		t.Fatalf("read empty relationships: %v", err)
	}
	return emptyArtifactFacts{
		path: artifact.Path, sha256: artifact.SHA256, count: artifact.Count,
		uncompressedBytes: artifact.UncompressedBytes, storedBytes: artifact.StoredBytes, readCount: len(readRelationships),
	}
}

func installArtifact(t *testing.T, root, temporary, relative string) {
	t.Helper()
	final := filepath.Join(root, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(final), 0o700); err != nil {
		t.Fatalf("create artifact directory: %v", err)
	}
	if err := os.Rename(temporary, final); err != nil {
		t.Fatalf("install artifact: %v", err)
	}
}
