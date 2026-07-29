package jsonl_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ret/entity"
	"github.com/specterops/dawgs/ret/jsonl"
)

func TestReadNodesNormalizesJSONNumbersRecursivelyForGraphProperties(t *testing.T) {
	root := t.TempDir()
	contents := []byte(`{"source_id":"1","properties":{"integer":9007199254740993,"fraction":1.25,"nested":[2,{"exponent":1e2}],"null":null,"bool":true,"string":"value"}}` + "\n")
	if err := os.WriteFile(filepath.Join(root, "nodes.jsonl"), contents, 0o600); err != nil {
		t.Fatalf("write nodes: %v", err)
	}
	var got entity.Node

	err := jsonl.ReadNodes(root, nodeArtifact("nodes.jsonl", contents), func(node entity.Node) error {
		got = node
		return nil
	})

	if err != nil {
		t.Fatalf("read nodes: %v", err)
	}
	if value, ok := got.Properties["integer"].(int64); !ok || value != 9007199254740993 {
		t.Fatalf("integer = %#v (%T), want exact int64", got.Properties["integer"], got.Properties["integer"])
	}
	if value, ok := got.Properties["fraction"].(float64); !ok || value != 1.25 {
		t.Fatalf("fraction = %#v (%T), want float64(1.25)", got.Properties["fraction"], got.Properties["fraction"])
	}
	nested, ok := got.Properties["nested"].([]any)
	if !ok || len(nested) != 2 {
		t.Fatalf("nested = %#v (%T), want two-element []any", got.Properties["nested"], got.Properties["nested"])
	}
	if value, ok := nested[0].(int64); !ok || value != 2 {
		t.Fatalf("nested[0] = %#v (%T), want int64(2)", nested[0], nested[0])
	}
	object, ok := nested[1].(map[string]any)
	if !ok {
		t.Fatalf("nested[1] = %#v (%T), want map[string]any", nested[1], nested[1])
	}
	if value, ok := object["exponent"].(float64); !ok || value != 100 {
		t.Fatalf("nested exponent = %#v (%T), want float64(100)", object["exponent"], object["exponent"])
	}
	if got.Properties["null"] != nil || got.Properties["bool"] != true || got.Properties["string"] != "value" {
		t.Fatalf("non-numeric properties changed: %#v", got.Properties)
	}
	mapped, err := graph.AsProperties(got.Properties).Get("integer").Int64()
	if err != nil || mapped != 9007199254740993 {
		t.Fatalf("graph mapper integer = %d, %v", mapped, err)
	}
}

func TestReadRelationshipsNormalizesJSONNumbersRecursively(t *testing.T) {
	root := t.TempDir()
	contents := []byte(`{"start_id":"1","end_id":"2","kind":"MemberOf","properties":{"integer":7,"nested":{"fraction":2.5}}}` + "\n")
	if err := os.WriteFile(filepath.Join(root, "relationships.jsonl"), contents, 0o600); err != nil {
		t.Fatalf("write relationships: %v", err)
	}
	var got entity.Relationship

	err := jsonl.ReadRelationships(root, relationshipArtifact("relationships.jsonl", contents), func(relationship entity.Relationship) error {
		got = relationship
		return nil
	})

	if err != nil {
		t.Fatalf("read relationships: %v", err)
	}
	if value, ok := got.Properties["integer"].(int64); !ok || value != 7 {
		t.Fatalf("integer = %#v (%T), want int64(7)", got.Properties["integer"], got.Properties["integer"])
	}
	nested, ok := got.Properties["nested"].(map[string]any)
	if !ok {
		t.Fatalf("nested = %#v (%T), want map[string]any", got.Properties["nested"], got.Properties["nested"])
	}
	if value, ok := nested["fraction"].(float64); !ok || value != 2.5 {
		t.Fatalf("nested fraction = %#v (%T), want float64(2.5)", nested["fraction"], nested["fraction"])
	}
}

func TestReadNodesRejectsUnsupportedJSONNumberDomainBeforeVisiting(t *testing.T) {
	for name, contents := range map[string][]byte{
		"integer above int64": []byte(`{"source_id":"1","properties":{"limit":9223372036854775808}}` + "\n"),
		"integer below int64": []byte(`{"source_id":"1","properties":{"limit":-9223372036854775809}}` + "\n"),
		"non-finite exponent": []byte(`{"source_id":"1","properties":{"limit":1e309}}` + "\n"),
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "nodes.jsonl"), contents, 0o600); err != nil {
				t.Fatalf("write nodes: %v", err)
			}
			visited := false

			err := jsonl.ReadNodes(root, nodeArtifact("nodes.jsonl", contents), func(entity.Node) error {
				visited = true
				return nil
			})

			if err == nil {
				t.Fatal("expected numeric domain error")
			}
			if visited {
				t.Fatal("visitor called for unsupported numeric property")
			}
			for _, fragment := range []string{"node record 1", "properties.limit"} {
				if !strings.Contains(err.Error(), fragment) {
					t.Fatalf("error %q does not contain %q", err, fragment)
				}
			}
		})
	}
}

func TestReadNodesRejectsInvalidArtifactsBeforeVisiting(t *testing.T) {
	root := t.TempDir()
	artifact, err := jsonl.WriteNodes(filepath.Join(root, "nodes.tmp"), "nodes.jsonl", jsonl.Config{Enabled: true, Codec: jsonl.CodecNone}, []entity.Node{{SourceID: "1"}})
	if err != nil {
		t.Fatalf("write nodes: %v", err)
	}
	path := filepath.Join(root, artifact.Path)
	if err := os.Rename(filepath.Join(root, "nodes.tmp"), path); err != nil {
		t.Fatalf("install artifact: %v", err)
	}

	tests := map[string]func(jsonl.NodeArtifact){
		"byte mutation": func(value jsonl.NodeArtifact) { mutateFile(t, path, []byte("{\"source_id\":\"2\"}\n")) },
		"truncation":    func(value jsonl.NodeArtifact) { mutateFile(t, path, []byte(`{"source_id":"1"`)) },
		"count mismatch": func(value jsonl.NodeArtifact) {
			value.Count++
			requireReadNodeErrorWithoutVisitor(t, root, value)
		},
		"stored size mismatch": func(value jsonl.NodeArtifact) {
			value.StoredBytes++
			requireReadNodeErrorWithoutVisitor(t, root, value)
		},
		"unsupported schema": func(value jsonl.NodeArtifact) {
			value.SchemaVersion = "wrong"
			requireReadNodeErrorWithoutVisitor(t, root, value)
		},
		"path traversal": func(value jsonl.NodeArtifact) {
			value.Path = "../nodes.jsonl"
			requireReadNodeErrorWithoutVisitor(t, root, value)
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := artifact
			if name == "byte mutation" || name == "truncation" {
				original, readErr := os.ReadFile(path)
				if readErr != nil {
					t.Fatalf("save artifact: %v", readErr)
				}
				defer func() {
					if writeErr := os.WriteFile(path, original, 0o600); writeErr != nil {
						t.Fatalf("restore artifact: %v", writeErr)
					}
				}()
			}
			mutate(value)
			if name == "byte mutation" || name == "truncation" {
				requireReadNodeErrorWithoutVisitor(t, root, value)
			}
		})
	}
}

func TestReadNodesRejectsGzipArtifactLabeledZstd(t *testing.T) {
	root := t.TempDir()
	artifact, err := jsonl.WriteNodes(filepath.Join(root, "nodes.tmp"), "nodes.jsonl", jsonl.Config{Enabled: true, Codec: jsonl.CodecGzip}, []entity.Node{{SourceID: "1"}})
	if err != nil {
		t.Fatalf("write gzip nodes: %v", err)
	}
	if err := os.Rename(filepath.Join(root, "nodes.tmp"), filepath.Join(root, artifact.Path)); err != nil {
		t.Fatalf("install artifact: %v", err)
	}
	artifact.Codec = string(jsonl.CodecZstd)
	requireReadNodeErrorWithoutVisitor(t, root, artifact)
}

func TestReadRelationshipsRejectsTrailingJSONAndInvalidEntity(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "relationships.jsonl")
	contents := []byte(`{"start_id":"1","end_id":"2","kind":"MemberOf"} {}` + "\n")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatalf("write relationships: %v", err)
	}
	artifact := relationshipArtifact("relationships.jsonl", contents)
	visited := false
	err := jsonl.ReadRelationships(root, artifact, func(entity.Relationship) error {
		visited = true
		return nil
	})
	if err == nil {
		t.Fatal("expected trailing JSON error")
	}
	if visited {
		t.Fatal("visitor called for malformed JSON")
	}

	contents = []byte(`{"start_id":"","end_id":"2","kind":"MemberOf"}` + "\n")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatalf("write invalid relationship: %v", err)
	}
	artifact = relationshipArtifact("relationships.jsonl", contents)
	visited = false
	err = jsonl.ReadRelationships(root, artifact, func(entity.Relationship) error {
		visited = true
		return nil
	})
	if err == nil {
		t.Fatal("expected invalid relationship error")
	}
	if visited {
		t.Fatal("visitor called for invalid entity")
	}
}

func TestReadNodesRejectsUncompressedSizeMismatch(t *testing.T) {
	root := t.TempDir()
	contents := []byte(`{"source_id":"1"}` + "\n")
	if err := os.WriteFile(filepath.Join(root, "nodes.jsonl"), contents, 0o600); err != nil {
		t.Fatalf("write nodes: %v", err)
	}
	artifact := nodeArtifact("nodes.jsonl", contents)
	artifact.UncompressedBytes++
	requireReadNodeErrorWithoutVisitor(t, root, artifact)
}

func TestReadNodesRejectsMalformedJSONAndOversizedPhysicalLine(t *testing.T) {
	for name, contents := range map[string][]byte{
		"malformed JSON": []byte("{\"source_id\":\n"),
		"oversized line": []byte(`{"source_id":"1","properties":{"description":"` + strings.Repeat("x", 10*1024*1024) + `"}}` + "\n"),
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "nodes.jsonl"), contents, 0o600); err != nil {
				t.Fatalf("write nodes: %v", err)
			}
			requireReadNodeErrorWithoutVisitor(t, root, nodeArtifact("nodes.jsonl", contents))
		})
	}
}

func requireReadNodeErrorWithoutVisitor(t *testing.T, root string, artifact jsonl.NodeArtifact) {
	t.Helper()
	visited := false
	err := jsonl.ReadNodes(root, artifact, func(entity.Node) error {
		visited = true
		return nil
	})
	if err == nil {
		t.Fatal("expected read error")
	}
	if visited {
		t.Fatalf("visitor called for invalid artifact: %v", err)
	}
}

func mutateFile(t *testing.T, path string, contents []byte) {
	t.Helper()
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatalf("mutate artifact: %v", err)
	}
}

func nodeArtifact(path string, contents []byte) jsonl.NodeArtifact {
	hash := sha256.Sum256(contents)
	return jsonl.NodeArtifact{SchemaVersion: jsonl.SchemaVersion, Path: path, Codec: string(jsonl.CodecNone), SHA256: hex.EncodeToString(hash[:]), Count: 1, UncompressedBytes: int64(len(contents)), StoredBytes: int64(len(contents))}
}

func relationshipArtifact(path string, contents []byte) jsonl.RelationshipArtifact {
	hash := sha256.Sum256(contents)
	return jsonl.RelationshipArtifact{SchemaVersion: jsonl.SchemaVersion, Path: path, Codec: string(jsonl.CodecNone), SHA256: hex.EncodeToString(hash[:]), Count: 1, UncompressedBytes: int64(len(contents)), StoredBytes: int64(len(contents))}
}
