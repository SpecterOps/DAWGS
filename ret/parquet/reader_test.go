package parquet

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	parquetgo "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/specterops/dawgs/ret/entity"
)

func TestVariantValuesAndRelationshipSourceIDSurviveRoundTrip(t *testing.T) {
	want := entity.Relationship{
		SourceID: "relationship-7",
		StartID:  "node-1",
		EndID:    "node-2",
		Kind:     "MemberOf",
		Properties: map[string]any{
			"null":    nil,
			"bool":    true,
			"integer": int64(42),
			"float":   3.5,
			"string":  "value",
			"list":    []any{"first", int64(2), false},
			"object": map[string]any{
				"nested": "yes",
			},
		},
	}
	root := t.TempDir()
	artifact, err := WriteRelationships(filepath.Join(root, "relationships.tmp"), "relationships.parquet", Config{Enabled: true}, []entity.Relationship{want})
	if err != nil {
		t.Fatalf("write relationships: %v", err)
	}
	if err := os.Rename(filepath.Join(root, "relationships.tmp"), filepath.Join(root, artifact.Path)); err != nil {
		t.Fatalf("install artifact: %v", err)
	}

	var got []entity.Relationship
	if err := ReadRelationships(root, artifact, func(relationship entity.Relationship) error {
		got = append(got, relationship)
		return nil
	}); err != nil {
		t.Fatalf("read relationships: %v", err)
	}
	if len(got) != 1 || !reflect.DeepEqual(got[0], want) {
		t.Fatalf("round trip relationships = %#v, want %#v", got, want)
	}
}

func TestReadNodesRejectsInvalidArtifactMetadataBeforeVisiting(t *testing.T) {
	root, artifact := installedNodeArtifact(t, []entity.Node{{SourceID: "1"}})
	tests := []struct {
		name   string
		mutate func(NodeArtifact) NodeArtifact
	}{
		{
			name: "schema version",
			mutate: func(value NodeArtifact) NodeArtifact {
				value.SchemaVersion = "wrong"
				return value
			},
		},
		{
			name: "stored size",
			mutate: func(value NodeArtifact) NodeArtifact {
				value.StoredBytes++
				return value
			},
		},
		{
			name: "stored SHA",
			mutate: func(value NodeArtifact) NodeArtifact {
				value.SHA256 = strings.Repeat("0", sha256.Size*2)
				return value
			},
		},
		{
			name: "row count",
			mutate: func(value NodeArtifact) NodeArtifact {
				value.Count++
				return value
			},
		},
		{
			name: "unsafe path",
			mutate: func(value NodeArtifact) NodeArtifact {
				value.Path = "../nodes.parquet"
				return value
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			visited := 0
			err := ReadNodes(root, test.mutate(artifact), func(entity.Node) error {
				visited++
				return nil
			})
			if err == nil {
				t.Fatal("ReadNodes unexpectedly succeeded")
			}
			if visited != 0 {
				t.Fatalf("visited %d nodes before rejecting artifact", visited)
			}
		})
	}
}

func TestReadNodesRejectsCorruptAndTruncatedFiles(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{
			name: "corrupt",
			mutate: func(contents []byte) []byte {
				contents[len(contents)/2] ^= 0xff
				return contents
			},
		},
		{
			name: "truncated",
			mutate: func(contents []byte) []byte {
				return contents[:len(contents)-1]
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root, artifact := installedNodeArtifact(t, []entity.Node{{SourceID: "1"}})
			path := filepath.Join(root, artifact.Path)
			contents, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read artifact: %v", err)
			}
			if err := os.WriteFile(path, test.mutate(contents), 0o600); err != nil {
				t.Fatalf("mutate artifact: %v", err)
			}

			visited := 0
			if err := ReadNodes(root, artifact, func(entity.Node) error {
				visited++
				return nil
			}); err == nil {
				t.Fatal("ReadNodes unexpectedly accepted damaged artifact")
			}
			if visited != 0 {
				t.Fatalf("visited %d nodes from damaged artifact", visited)
			}
		})
	}
}

func TestReadRelationshipsRequiresSourceIDBeforeAnyVisit(t *testing.T) {
	root := t.TempDir()
	contents := rawParquet(t, []RelationshipRow{
		{SourceID: "valid", StartID: "1", EndID: "2", Kind: "MemberOf", Properties: map[string]any{}},
		{StartID: "2", EndID: "3", Kind: "MemberOf", Properties: map[string]any{}},
	})
	path := "relationships.parquet"
	writeContents(t, filepath.Join(root, path), contents)
	hash := sha256.Sum256(contents)
	artifact := RelationshipArtifact{
		SchemaVersion: SchemaVersion,
		Path:          path,
		SHA256:        hex.EncodeToString(hash[:]),
		Count:         2,
		StoredBytes:   int64(len(contents)),
	}

	visited := 0
	err := ReadRelationships(root, artifact, func(entity.Relationship) error {
		visited++
		return nil
	})
	if err == nil {
		t.Fatal("ReadRelationships unexpectedly accepted empty source ID")
	}
	if visited != 0 {
		t.Fatalf("visited %d relationships before validating all source IDs", visited)
	}
}

func TestReadersValidateEveryEntityBeforeAnyVisit(t *testing.T) {
	tests := []struct {
		name string
		read func(func() error) error
	}{
		{
			name: "node",
			read: func(visit func() error) error {
				root := t.TempDir()
				contents := rawParquet(t, []NodeRow{
					{SourceID: "valid", Properties: map[string]any{}},
					{Properties: map[string]any{}},
				})
				path := "nodes.parquet"
				writeContents(t, filepath.Join(root, path), contents)
				hash := sha256.Sum256(contents)
				return ReadNodes(root, NodeArtifact{
					SchemaVersion: SchemaVersion,
					Path:          path,
					SHA256:        hex.EncodeToString(hash[:]),
					Count:         2,
					StoredBytes:   int64(len(contents)),
				}, func(entity.Node) error {
					return visit()
				})
			},
		},
		{
			name: "relationship",
			read: func(visit func() error) error {
				root := t.TempDir()
				contents := rawParquet(t, []RelationshipRow{
					{SourceID: "valid-1", StartID: "1", EndID: "2", Kind: "MemberOf", Properties: map[string]any{}},
					{SourceID: "valid-2", EndID: "3", Kind: "MemberOf", Properties: map[string]any{}},
				})
				path := "relationships.parquet"
				writeContents(t, filepath.Join(root, path), contents)
				hash := sha256.Sum256(contents)
				return ReadRelationships(root, RelationshipArtifact{
					SchemaVersion: SchemaVersion,
					Path:          path,
					SHA256:        hex.EncodeToString(hash[:]),
					Count:         2,
					StoredBytes:   int64(len(contents)),
				}, func(entity.Relationship) error {
					return visit()
				})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			visited := 0
			err := test.read(func() error {
				visited++
				return nil
			})
			if err == nil {
				t.Fatal("read unexpectedly accepted invalid entity")
			}
			if visited != 0 {
				t.Fatalf("visited %d entities before validating all rows", visited)
			}
		})
	}
}

func TestReadEmptyNodeArtifact(t *testing.T) {
	root, artifact := installedNodeArtifact(t, nil)
	visited := 0
	if err := ReadNodes(root, artifact, func(entity.Node) error {
		visited++
		return nil
	}); err != nil {
		t.Fatalf("read empty node artifact: %v", err)
	}
	if visited != 0 {
		t.Fatalf("visited %d nodes, want none", visited)
	}
}

func TestReadNodesVisitsVerifiedSnapshotAfterPathReplacement(t *testing.T) {
	root, artifact := installedNodeArtifact(t, []entity.Node{{SourceID: "old-1"}, {SourceID: "old-2"}})
	replacementTemporary := filepath.Join(root, "replacement.tmp")
	replacementArtifact, err := WriteNodes(replacementTemporary, "replacement.parquet", Config{Enabled: true}, []entity.Node{{SourceID: "new-1"}, {SourceID: "new-2"}})
	if err != nil {
		t.Fatalf("write replacement: %v", err)
	}
	replacement := filepath.Join(root, replacementArtifact.Path)
	if err := os.Rename(replacementTemporary, replacement); err != nil {
		t.Fatalf("install replacement: %v", err)
	}

	afterVerifiedSnapshotForTest = func() {
		if err := os.Rename(replacement, filepath.Join(root, artifact.Path)); err != nil {
			t.Fatalf("replace artifact path: %v", err)
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
	if got, want := visited, []string{"old-1", "old-2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("visited nodes = %v, want verified snapshot %v", got, want)
	}
}

func TestReadNodesRejectsSymlinksBeneathRootBeforeVisiting(t *testing.T) {
	outsideRoot, outsideArtifact := installedNodeArtifact(t, []entity.Node{{SourceID: "outside"}})
	outsidePath := filepath.Join(outsideRoot, outsideArtifact.Path)

	tests := []struct {
		name string
		link func(string) (NodeArtifact, error)
	}{
		{
			name: "final file",
			link: func(root string) (NodeArtifact, error) {
				artifact := outsideArtifact
				artifact.Path = "nodes.parquet"
				return artifact, os.Symlink(outsidePath, filepath.Join(root, artifact.Path))
			},
		},
		{
			name: "intermediate directory",
			link: func(root string) (NodeArtifact, error) {
				artifact := outsideArtifact
				artifact.Path = "linked/nodes.parquet"
				return artifact, os.Symlink(outsideRoot, filepath.Join(root, "linked"))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			artifact, err := test.link(root)
			if err != nil {
				t.Fatalf("create in-root symlink to outside artifact: %v", err)
			}

			visited := 0
			err = ReadNodes(root, artifact, func(entity.Node) error {
				visited++
				return nil
			})
			if err == nil {
				t.Fatal("ReadNodes unexpectedly followed in-root symlink")
			}
			if visited != 0 {
				t.Fatalf("visited %d nodes through in-root symlink", visited)
			}
		})
	}
}

func installedNodeArtifact(t *testing.T, nodes []entity.Node) (string, NodeArtifact) {
	t.Helper()
	root := t.TempDir()
	temporary := filepath.Join(root, "nodes.tmp")
	artifact, err := WriteNodes(temporary, "nodes.parquet", Config{Enabled: true}, nodes)
	if err != nil {
		t.Fatalf("write nodes: %v", err)
	}
	if err := os.Rename(temporary, filepath.Join(root, artifact.Path)); err != nil {
		t.Fatalf("install nodes: %v", err)
	}
	return root, artifact
}

func rawParquet[T any](t *testing.T, rows []T) []byte {
	t.Helper()
	var output bytes.Buffer
	writer := parquetgo.NewGenericWriter[T](&output, parquetgo.Compression(&zstd.Codec{}))
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("write raw Parquet rows: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close raw Parquet writer: %v", err)
	}
	return output.Bytes()
}

func writeContents(t *testing.T, path string, contents []byte) {
	t.Helper()
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatalf("write artifact: %v", err)
	}
}
