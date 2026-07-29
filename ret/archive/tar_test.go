package archive

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/ret/collection"
	"github.com/specterops/dawgs/ret/entity"
	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/metrics"
	"github.com/specterops/dawgs/ret/parquet"
	"github.com/stretchr/testify/require"
)

func TestCollectionTarIsDeterministicSortedAndHasFixedMetadata(t *testing.T) {
	// Break caught: allowing host directory iteration order or filesystem
	// metadata to alter the plaintext TAR bytes.
	root := writeArchiveTestCollection(t, true, true)
	first := createPlainTestTar(t, root)

	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		require.NoError(t, err)
		require.NoError(t, os.Chmod(path, 0o777))
		return os.Chtimes(
			path,
			time.Date(2038, time.January, 19, 3, 14, 7, 0, time.UTC),
			time.Date(2040, time.February, 20, 4, 15, 8, 0, time.UTC),
		)
	}))

	second := createPlainTestTar(t, root)
	require.Equal(t, first, second)

	reader := tar.NewReader(bytes.NewReader(first))
	var names []string
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		names = append(names, header.Name)
		require.Equal(t, int64(0o600), header.Mode)
		require.Zero(t, header.Uid)
		require.Zero(t, header.Gid)
		require.Empty(t, header.Uname)
		require.Empty(t, header.Gname)
		require.Equal(t, byte(tar.TypeReg), header.Typeflag)
		require.True(t, header.ModTime.Equal(time.Unix(0, 0).UTC()))
		require.True(t, header.AccessTime.IsZero())
		require.True(t, header.ChangeTime.IsZero())
		require.Empty(t, header.PAXRecords)
	}

	require.Equal(t, []string{
		"graphs/example/nodes/000001.jsonl",
		"graphs/example/nodes/000001.parquet",
		"graphs/example/relationships/000001.jsonl",
		"graphs/example/relationships/000001.parquet",
		"manifest.json",
	}, names)
	require.True(t, sort.StringsAreSorted(names))
}

func TestCollectionTarRejectsAnythingOutsideTheDeclaredFileSet(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, string)
		match  string
	}{
		{
			name: "undeclared regular file",
			mutate: func(t *testing.T, root string) {
				require.NoError(t, os.WriteFile(filepath.Join(root, "extra.txt"), []byte("extra"), 0o600))
			},
			match: "unexpected",
		},
		{
			name: "undeclared empty directory",
			mutate: func(t *testing.T, root string) {
				require.NoError(t, os.Mkdir(filepath.Join(root, "extra"), 0o700))
			},
			match: "unexpected",
		},
		{
			name: "undeclared symlink",
			mutate: func(t *testing.T, root string) {
				if err := os.Symlink(collection.ManifestName, filepath.Join(root, "extra")); err != nil {
					t.Skipf("symlinks unavailable: %v", err)
				}
			},
			match: "symlink",
		},
		{
			name: "declared symlink",
			mutate: func(t *testing.T, root string) {
				manifest := readArchiveTestManifest(t, root)
				path := manifest.Graphs[0].NodeShards[0].JSONL.Path
				require.NoError(t, os.Remove(filepath.Join(root, filepath.FromSlash(path))))
				if err := os.Symlink(
					filepath.Join(root, collection.ManifestName),
					filepath.Join(root, filepath.FromSlash(path)),
				); err != nil {
					t.Skipf("symlinks unavailable: %v", err)
				}
			},
			match: "symlink",
		},
		{
			name: "missing declared file",
			mutate: func(t *testing.T, root string) {
				manifest := readArchiveTestManifest(t, root)
				path := manifest.Graphs[0].NodeShards[0].JSONL.Path
				require.NoError(t, os.Remove(filepath.Join(root, filepath.FromSlash(path))))
			},
			match: "no such file",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := writeArchiveTestCollection(t, true, false)
			test.mutate(t, root)

			var destination bytes.Buffer
			paths, err := collectionTarPaths(context.Background(), root, nil)
			if err == nil {
				err = writeCollectionTar(context.Background(), &destination, root, paths, nil)
			}

			require.ErrorContains(t, err, test.match)
			require.Empty(t, destination.Bytes())
		})
	}
}

func TestCollectionTarFullyVerifiesBeforeWriting(t *testing.T) {
	// Break caught: packaging a dual-format collection after checking only its
	// JSONL artifacts or after beginning to emit TAR bytes.
	root := writeArchiveTestCollection(t, true, true)
	manifest := readArchiveTestManifest(t, root)
	parquetPath := manifest.Graphs[0].NodeShards[0].Parquet.Path
	require.NoError(t, os.WriteFile(
		filepath.Join(root, filepath.FromSlash(parquetPath)),
		[]byte("corrupt parquet"),
		0o600,
	))

	var destination bytes.Buffer
	paths, err := collectionTarPaths(context.Background(), root, nil)
	if err == nil {
		err = writeCollectionTar(context.Background(), &destination, root, paths, nil)
	}

	require.Error(t, err)
	require.Empty(t, destination.Bytes())
}

func TestCollectionTarRejectsInPlaceMutationAfterVerification(t *testing.T) {
	// Break caught: publishing bytes changed in place after full verification
	// while preserving the file identity, size, mode, and modification time.
	root := writeArchiveTestCollection(t, true, true)
	plan, err := collectionTarPaths(context.Background(), root, nil)
	require.NoError(t, err)
	require.NotEmpty(t, plan.files)
	planned := plan.files[0]
	require.NotEqual(t, collection.ManifestName, planned.path)
	absolute := filepath.Join(root, filepath.FromSlash(planned.path))
	payload := mustReadArchiveTestFile(t, absolute)
	require.NotEmpty(t, payload)
	payload[0] ^= 0x01
	require.NoError(t, os.WriteFile(absolute, payload, planned.info.Mode().Perm()))
	require.NoError(t, os.Chmod(absolute, planned.info.Mode().Perm()))
	require.NoError(t, os.Chtimes(absolute, planned.info.ModTime(), planned.info.ModTime()))

	var destination bytes.Buffer
	err = writeCollectionTar(context.Background(), &destination, root, plan, nil)

	require.ErrorContains(t, err, "SHA-256")
}

func TestCollectionTarRoundTripsLongValidPAXPath(t *testing.T) {
	// Break caught: forcing USTAR and rejecting a valid collection path longer
	// than its name/prefix fields can represent.
	graphName := strings.Repeat("g", 240)
	root := writeArchiveTestCollectionNamed(t, graphName, true, false)
	recipient, identity, err := GenerateKeyPair()
	require.NoError(t, err)
	archivePath := filepath.Join(t.TempDir(), "collection.ret")
	output := filepath.Join(t.TempDir(), "collection")

	require.NoError(t, Create(context.Background(), CreateConfig{
		CollectionDirectory: root,
		ArchivePath:         archivePath,
		Recipient:           recipient,
	}))
	require.NoError(t, Extract(context.Background(), ExtractConfig{
		ArchivePath:     archivePath,
		OutputDirectory: output,
		Identity:        identity,
	}))
	_, err = collection.Verify(context.Background(), output, nil)
	require.NoError(t, err)
}

func createPlainTestTar(t *testing.T, root string) []byte {
	t.Helper()
	paths, err := collectionTarPaths(context.Background(), root, nil)
	require.NoError(t, err)
	var destination bytes.Buffer
	require.NoError(t, writeCollectionTar(context.Background(), &destination, root, paths, nil))
	return append([]byte(nil), destination.Bytes()...)
}

func writeArchiveTestCollection(t *testing.T, withJSONL, withParquet bool) string {
	t.Helper()
	return writeArchiveTestCollectionNamed(t, "example", withJSONL, withParquet)
}

func writeArchiveTestCollectionNamed(
	t *testing.T,
	graphName string,
	withJSONL, withParquet bool,
) string {
	t.Helper()
	root := t.TempDir()
	nodes := []entity.Node{
		{SourceID: "node-1", Kinds: []string{"User", "Principal"}, Properties: map[string]any{"name": "Alice"}},
		{SourceID: "node-2", Kinds: []string{"Group"}, Properties: map[string]any{"name": "Admins"}},
	}
	relationships := []entity.Relationship{{
		SourceID:   "relationship-1",
		StartID:    "node-1",
		EndID:      "node-2",
		Kind:       "MEMBER_OF",
		Properties: map[string]any{"active": true},
	}}

	builder := metrics.NewBuilder()
	for _, node := range nodes {
		require.NoError(t, builder.ObserveNode(node))
	}
	for _, relationship := range relationships {
		require.NoError(t, builder.ObserveRelationship(relationship))
	}

	graph := collection.Graph{
		Name:               graphName,
		NodeCount:          int64(len(nodes)),
		RelationshipCount:  int64(len(relationships)),
		KindCatalog:        []string{"User", "Principal", "Group", "MEMBER_OF"},
		NodeShards:         []collection.NodeShard{{Index: 1, Count: int64(len(nodes)), LastSourceID: 2}},
		RelationshipShards: []collection.RelationshipShard{{Index: 1, Count: int64(len(relationships)), LastSourceID: 1}},
		Metrics:            builder.Finalize(),
	}
	outputs := collection.OutputConfig{}

	if withJSONL {
		config := jsonl.Config{Enabled: true, Codec: jsonl.CodecNone}
		outputs.JSONL = &collection.JSONLOutput{
			SchemaVersion: jsonl.SchemaVersion,
			Codec:         string(config.Codec),
			Level:         config.Level,
		}
		nodePath := collection.NodeJSONLPath(graph.Name, 1, config.Codec)
		nodeTemporary := filepath.Join(root, "nodes.jsonl.tmp")
		nodeArtifact, err := jsonl.WriteNodes(nodeTemporary, nodePath, config, nodes)
		require.NoError(t, err)
		installArchiveTestArtifact(t, root, nodeTemporary, nodePath)
		graph.NodeShards[0].JSONL = &nodeArtifact

		relationshipPath := collection.RelationshipJSONLPath(graph.Name, 1, config.Codec)
		relationshipTemporary := filepath.Join(root, "relationships.jsonl.tmp")
		relationshipArtifact, err := jsonl.WriteRelationships(
			relationshipTemporary,
			relationshipPath,
			config,
			relationships,
		)
		require.NoError(t, err)
		installArchiveTestArtifact(t, root, relationshipTemporary, relationshipPath)
		graph.RelationshipShards[0].JSONL = &relationshipArtifact
	}

	if withParquet {
		config := parquet.Config{Enabled: true}
		outputs.Parquet = &collection.ParquetOutput{SchemaVersion: parquet.SchemaVersion}
		nodePath := collection.NodeParquetPath(graph.Name, 1)
		nodeTemporary := filepath.Join(root, "nodes.parquet.tmp")
		nodeArtifact, err := parquet.WriteNodes(nodeTemporary, nodePath, config, nodes)
		require.NoError(t, err)
		installArchiveTestArtifact(t, root, nodeTemporary, nodePath)
		graph.NodeShards[0].Parquet = &nodeArtifact

		relationshipPath := collection.RelationshipParquetPath(graph.Name, 1)
		relationshipTemporary := filepath.Join(root, "relationships.parquet.tmp")
		relationshipArtifact, err := parquet.WriteRelationships(
			relationshipTemporary,
			relationshipPath,
			config,
			relationships,
		)
		require.NoError(t, err)
		installArchiveTestArtifact(t, root, relationshipTemporary, relationshipPath)
		graph.RelationshipShards[0].Parquet = &relationshipArtifact
	}

	require.NoError(t, collection.Write(root, collection.Manifest{
		Format:    collection.Format,
		CreatedAt: time.Date(2026, time.July, 28, 12, 0, 0, 0, time.UTC),
		Outputs:   outputs,
		Graphs:    []collection.Graph{graph},
	}))
	return root
}

func installArchiveTestArtifact(t *testing.T, root, temporary, relative string) {
	t.Helper()
	final := filepath.Join(root, filepath.FromSlash(relative))
	require.NoError(t, os.MkdirAll(filepath.Dir(final), 0o700))
	require.NoError(t, os.Rename(temporary, final))
}

func readArchiveTestManifest(t *testing.T, root string) collection.Manifest {
	t.Helper()
	manifest, err := collection.Read(root)
	require.NoError(t, err)
	return manifest
}
