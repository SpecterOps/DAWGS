package collection_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ret/collection"
	"github.com/specterops/dawgs/ret/entity"
	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/metrics"
	"github.com/specterops/dawgs/ret/observe"
	"github.com/specterops/dawgs/ret/parquet"
	"github.com/stretchr/testify/require"
)

func TestVerifySupportsJSONLParquetDualAndEmptyGraphs(t *testing.T) {
	nodes, relationships := verificationEntities()

	for _, testCase := range []struct {
		name    string
		jsonl   bool
		parquet bool
	}{
		{name: "JSONL only", jsonl: true},
		{name: "Parquet only", parquet: true},
		{name: "dual", jsonl: true, parquet: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			root := writeVerificationCollection(t, nodes, nodes, relationships, relationships, testCase.jsonl, testCase.parquet)

			got, err := collection.Verify(context.Background(), root, nil)

			require.NoError(t, err)
			require.Len(t, got.Graphs, 1)
			require.Equal(t, collection.GraphVerification{Name: "example", NodeCount: 2, RelationshipCount: 1}, got.Graphs[0])
		})
	}

	t.Run("empty graph", func(t *testing.T) {
		root := writeVerificationCollection(t, nil, nil, nil, nil, true, true)

		got, err := collection.Verify(context.Background(), root, nil)

		require.NoError(t, err)
		require.Equal(t, []collection.GraphVerification{{Name: "example"}}, got.Graphs)
	})
}

func TestVerifyJSONLForLoadIgnoresCorruptAndMissingParquet(t *testing.T) {
	nodes, relationships := verificationEntities()

	t.Run("corrupt", func(t *testing.T) {
		root := writeVerificationCollection(t, nodes, nodes, relationships, relationships, true, true)
		manifest := readManifest(t, root)
		require.NoError(t, os.WriteFile(filepath.Join(root, filepath.FromSlash(manifest.Graphs[0].NodeShards[0].Parquet.Path)), []byte("corrupt"), 0o600))

		_, err := collection.VerifyJSONLForLoad(context.Background(), root, nil)
		require.NoError(t, err)

		_, err = collection.Verify(context.Background(), root, nil)
		require.Error(t, err)
	})

	t.Run("missing", func(t *testing.T) {
		root := writeVerificationCollection(t, nodes, nodes, relationships, relationships, true, true)
		manifest := readManifest(t, root)
		for _, path := range parquetPaths(manifest) {
			require.NoError(t, os.Remove(filepath.Join(root, filepath.FromSlash(path))))
		}

		_, err := collection.VerifyJSONLForLoad(context.Background(), root, nil)
		require.NoError(t, err)
	})
}

func TestVerifyJSONLForLoadRejectsParquetOnly(t *testing.T) {
	nodes, relationships := verificationEntities()
	root := writeVerificationCollection(t, nil, nodes, nil, relationships, false, true)

	_, err := collection.VerifyJSONLForLoad(context.Background(), root, nil)

	require.ErrorContains(t, err, "JSONL")
}

func TestVerifyRejectsDuplicateNodesAndMissingRelationshipEndpoints(t *testing.T) {
	nodes, relationships := verificationEntities()

	t.Run("duplicate node source ID", func(t *testing.T) {
		root := writeVerificationCollection(t, nodes, nil, relationships, nil, true, false)
		manifest := readManifest(t, root)
		manifest.Graphs[0].NodeShards[0].JSONL = installJSONLNodes(t, root, "example", []entity.Node{
			nodes[0],
			{SourceID: nodes[0].SourceID, Kinds: nodes[1].Kinds, Properties: nodes[1].Properties},
		})
		writeManifest(t, root, manifest)

		_, err := collection.Verify(context.Background(), root, nil)

		require.ErrorContains(t, err, "duplicate")
		require.ErrorContains(t, err, nodes[0].SourceID)
	})

	t.Run("missing endpoint", func(t *testing.T) {
		root := writeVerificationCollection(t, nodes, nil, relationships, nil, true, false)
		manifest := readManifest(t, root)
		broken := relationships
		broken[0].EndID = "missing"
		manifest.Graphs[0].RelationshipShards[0].JSONL = installJSONLRelationships(t, root, "example", broken)
		writeManifest(t, root, manifest)

		_, err := collection.Verify(context.Background(), root, nil)

		require.ErrorContains(t, err, "missing endpoint")
	})
}

func TestVerifyRequiresUniqueParquetRelationshipSourceIDs(t *testing.T) {
	nodes, relationships := verificationEntities()
	relationships = append(relationships, entity.Relationship{
		SourceID: "relationship-2",
		StartID:  "node-2",
		EndID:    "node-1",
		Kind:     "OWNS",
	})
	root := writeVerificationCollection(t, nil, nodes, nil, relationships, false, true)
	manifest := readManifest(t, root)
	duplicate := append([]entity.Relationship(nil), relationships...)
	duplicate[1].SourceID = duplicate[0].SourceID
	manifest.Graphs[0].RelationshipShards[0].Parquet = installParquetRelationships(t, root, "example", duplicate)
	writeManifest(t, root, manifest)

	_, err := collection.Verify(context.Background(), root, nil)

	require.ErrorContains(t, err, "duplicate")
	require.ErrorContains(t, err, duplicate[0].SourceID)
}

func TestVerifyRejectsCatalogAndMetricsMismatch(t *testing.T) {
	nodes, relationships := verificationEntities()

	t.Run("first-seen catalog order", func(t *testing.T) {
		root := writeVerificationCollection(t, nodes, nil, relationships, nil, true, false)
		manifest := readManifest(t, root)
		manifest.Graphs[0].KindCatalog[0], manifest.Graphs[0].KindCatalog[1] =
			manifest.Graphs[0].KindCatalog[1], manifest.Graphs[0].KindCatalog[0]
		writeManifest(t, root, manifest)

		_, err := collection.Verify(context.Background(), root, nil)

		require.ErrorContains(t, err, "kind catalog")
	})

	t.Run("metrics", func(t *testing.T) {
		root := writeVerificationCollection(t, nodes, nil, relationships, nil, true, false)
		manifest := readManifest(t, root)
		delete(manifest.Graphs[0].Metrics.NodeKindSequences, metrics.OrderedKindsKey(nodes[0].Kinds))
		manifest.Graphs[0].Metrics.NodeKindSequences[metrics.OrderedKindsKey([]string{"Wrong"})] = 1
		writeManifest(t, root, manifest)

		_, err := collection.Verify(context.Background(), root, nil)

		require.ErrorContains(t, err, "metrics")
	})
}

func TestVerifyComparesDualRowsAndCanonicalProperties(t *testing.T) {
	nodes, relationships := verificationEntities()

	t.Run("equivalent numeric and container values", func(t *testing.T) {
		nodes[0].Properties = map[string]any{
			"large": int64(9_007_199_254_740_993),
			"whole": int64(42),
			"nested": map[string]any{
				"array": []any{int64(1), 2.5, map[string]any{"enabled": true}},
				"null":  nil,
			},
		}
		root := writeVerificationCollection(t, nodes, nodes, relationships, relationships, true, true)

		_, err := collection.Verify(context.Background(), root, nil)

		require.NoError(t, err)
	})

	t.Run("node value mismatch", func(t *testing.T) {
		root := writeVerificationCollection(t, nodes, nodes, relationships, relationships, true, true)
		manifest := readManifest(t, root)
		different := append([]entity.Node(nil), nodes...)
		different[0].Properties = map[string]any{"different": true}
		manifest.Graphs[0].NodeShards[0].Parquet = installParquetNodes(t, root, "example", different)
		writeManifest(t, root, manifest)

		_, err := collection.Verify(context.Background(), root, nil)

		require.ErrorContains(t, err, `graph "example"`)
		require.ErrorContains(t, err, "node shard 1")
		require.ErrorContains(t, err, "row 1")
	})

	t.Run("node order mismatch", func(t *testing.T) {
		root := writeVerificationCollection(t, nodes, nodes, relationships, relationships, true, true)
		manifest := readManifest(t, root)
		reversed := []entity.Node{nodes[1], nodes[0]}
		manifest.Graphs[0].NodeShards[0].Parquet = installParquetNodes(t, root, "example", reversed)
		writeManifest(t, root, manifest)

		_, err := collection.Verify(context.Background(), root, nil)

		require.ErrorContains(t, err, "row 1")
	})

	t.Run("relationship value mismatch", func(t *testing.T) {
		root := writeVerificationCollection(t, nodes, nodes, relationships, relationships, true, true)
		manifest := readManifest(t, root)
		different := append([]entity.Relationship(nil), relationships...)
		different[0].Properties = map[string]any{"different": true}
		manifest.Graphs[0].RelationshipShards[0].Parquet = installParquetRelationships(t, root, "example", different)
		writeManifest(t, root, manifest)

		_, err := collection.Verify(context.Background(), root, nil)

		require.ErrorContains(t, err, "relationship shard 1")
		require.ErrorContains(t, err, "row 1")
	})

	t.Run("non-JSON-compatible Parquet value", func(t *testing.T) {
		nodes[0].Properties = map[string]any{"binary": []byte{0x01, 0x02}}
		root := writeVerificationCollection(t, nodes, nodes, relationships, relationships, true, true)

		_, err := collection.Verify(context.Background(), root, nil)

		require.ErrorContains(t, err, "not JSON-compatible")
	})
}

func TestVerifyRejectsSymlinksBeforeReadingAnyArtifact(t *testing.T) {
	nodes, relationships := verificationEntities()
	root := writeVerificationCollection(t, nodes, nodes, relationships, relationships, true, true)
	manifest := readManifest(t, root)
	target := filepath.Join(root, "outside.parquet")
	require.NoError(t, os.WriteFile(target, []byte("outside"), 0o600))
	link := filepath.Join(root, filepath.FromSlash(manifest.Graphs[0].RelationshipShards[0].Parquet.Path))
	require.NoError(t, os.Remove(link))
	require.NoError(t, os.Symlink(target, link))
	var events []observe.Event

	_, err := collection.Verify(context.Background(), root, observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		events = append(events, event)
	}))

	require.ErrorContains(t, err, "symlink")
	require.Empty(t, events, "all artifact paths must be checked before any reader emits success")
}

func TestArtifactVerifiedIsEmittedOnlyAfterConcreteReaderSucceeds(t *testing.T) {
	nodes, relationships := verificationEntities()
	root := writeVerificationCollection(t, nodes, nil, relationships, nil, true, false)
	manifest := readManifest(t, root)
	manifest.Graphs[0].NodeShards[0].JSONL = installJSONLNodes(t, root, "example", []entity.Node{
		nodes[0],
		{SourceID: nodes[0].SourceID, Kinds: nodes[1].Kinds},
	})
	writeManifest(t, root, manifest)
	var events []observe.Event

	_, err := collection.Verify(context.Background(), root, observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		events = append(events, event)
	}))

	require.Error(t, err)
	require.Empty(t, events)
}

func TestVerifyEmitsArtifactsInConcreteReaderCompletionOrder(t *testing.T) {
	nodes, relationships := verificationEntities()
	root := writeVerificationCollection(t, nodes, nodes, relationships, relationships, true, true)
	var got []string

	_, err := collection.Verify(context.Background(), root, observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		if verified, ok := event.(observe.ArtifactVerified); ok {
			got = append(got, verified.EntityType+":"+verified.Format)
		}
	}))

	require.NoError(t, err)
	require.Equal(t, []string{
		"node:JSONL",
		"node:Parquet",
		"relationship:JSONL",
		"relationship:Parquet",
	}, got)
}

func TestReplayGraphVisitsAllNodesBeforeRelationshipsAndNeverTouchesParquet(t *testing.T) {
	nodes, relationships := verificationEntities()
	root := writeVerificationCollection(t, nodes, nodes, relationships, relationships, true, true)
	manifest := readManifest(t, root)
	for _, path := range parquetPaths(manifest) {
		require.NoError(t, os.Remove(filepath.Join(root, filepath.FromSlash(path))))
	}
	_, err := collection.VerifyJSONLForLoad(context.Background(), root, nil)
	require.NoError(t, err)
	var order []string

	err = collection.ReplayGraph(
		context.Background(),
		root,
		manifest.Graphs[0],
		func(node entity.Node) error {
			order = append(order, "node:"+node.SourceID)
			return nil
		},
		func(relationship entity.Relationship) error {
			order = append(order, "relationship:"+relationship.Kind)
			return nil
		},
	)

	require.NoError(t, err)
	require.Equal(t, []string{"node:node-1", "node:node-2", "relationship:MEMBER_OF"}, order)
}

func TestReplayGraphReturnsBackendSupportedNativeNumbers(t *testing.T) {
	nodes, relationships := verificationEntities()
	nodes[0].Properties = map[string]any{
		"integer":  int64(9_007_199_254_740_993),
		"fraction": 1.25,
		"nested":   []any{int64(2)},
	}
	root := writeVerificationCollection(t, nodes, nil, relationships, nil, true, false)
	manifest := readManifest(t, root)
	_, err := collection.VerifyJSONLForLoad(context.Background(), root, nil)
	require.NoError(t, err)

	err = collection.ReplayGraph(
		context.Background(),
		root,
		manifest.Graphs[0],
		func(node entity.Node) error {
			if node.SourceID != "node-1" {
				return nil
			}
			require.IsType(t, int64(0), node.Properties["integer"])
			require.Equal(t, int64(9_007_199_254_740_993), node.Properties["integer"])
			require.IsType(t, float64(0), node.Properties["fraction"])
			nested := node.Properties["nested"].([]any)
			require.IsType(t, int64(0), nested[0])
			mapped, mapErr := graph.AsProperties(node.Properties).Get("integer").Int64()
			require.NoError(t, mapErr)
			require.Equal(t, int64(9_007_199_254_740_993), mapped)
			return nil
		},
		nil,
	)

	require.NoError(t, err)
}

func verificationEntities() ([]entity.Node, []entity.Relationship) {
	return []entity.Node{
			{SourceID: "node-1", Kinds: []string{"User", "Principal"}, Properties: map[string]any{"name": "Alice"}},
			{SourceID: "node-2", Kinds: []string{"Group"}, Properties: map[string]any{"name": "Admins"}},
		}, []entity.Relationship{{
			SourceID:   "relationship-1",
			StartID:    "node-1",
			EndID:      "node-2",
			Kind:       "MEMBER_OF",
			Properties: map[string]any{"active": true},
		}}
}

func writeVerificationCollection(
	t *testing.T,
	jsonNodes, parquetNodes []entity.Node,
	jsonRelationships, parquetRelationships []entity.Relationship,
	withJSONL, withParquet bool,
) string {
	t.Helper()
	root := t.TempDir()
	graph := collection.Graph{Name: "example"}
	outputs := collection.OutputConfig{}
	if withJSONL {
		outputs.JSONL = &collection.JSONLOutput{SchemaVersion: jsonl.SchemaVersion, Codec: string(jsonl.CodecNone)}
	}
	if withParquet {
		outputs.Parquet = &collection.ParquetOutput{SchemaVersion: parquet.SchemaVersion}
	}

	canonicalNodes := parquetNodes
	canonicalRelationships := parquetRelationships
	if withJSONL {
		canonicalNodes = jsonNodes
		canonicalRelationships = jsonRelationships
	}
	graph.NodeCount = int64(len(canonicalNodes))
	graph.RelationshipCount = int64(len(canonicalRelationships))
	graph.KindCatalog = firstSeenCatalog(canonicalNodes, canonicalRelationships)
	graph.Metrics = buildMetrics(t, canonicalNodes, canonicalRelationships)

	if len(canonicalNodes) != 0 {
		shard := collection.NodeShard{Index: 1, Count: int64(len(canonicalNodes)), LastSourceID: 100}
		if withJSONL {
			shard.JSONL = installJSONLNodes(t, root, graph.Name, jsonNodes)
		}
		if withParquet {
			shard.Parquet = installParquetNodes(t, root, graph.Name, parquetNodes)
		}
		graph.NodeShards = []collection.NodeShard{shard}
	}
	if len(canonicalRelationships) != 0 {
		shard := collection.RelationshipShard{Index: 1, Count: int64(len(canonicalRelationships)), LastSourceID: 200}
		if withJSONL {
			shard.JSONL = installJSONLRelationships(t, root, graph.Name, jsonRelationships)
		}
		if withParquet {
			shard.Parquet = installParquetRelationships(t, root, graph.Name, parquetRelationships)
		}
		graph.RelationshipShards = []collection.RelationshipShard{shard}
	}

	writeManifest(t, root, collection.Manifest{
		Format:    collection.Format,
		CreatedAt: time.Date(2026, time.July, 28, 12, 0, 0, 0, time.UTC),
		Outputs:   outputs,
		Graphs:    []collection.Graph{graph},
	})
	return root
}

func installJSONLNodes(t *testing.T, root, graph string, nodes []entity.Node) *collection.JSONLArtifact {
	t.Helper()
	path := collection.NodeJSONLPath(graph, 1, jsonl.CodecNone)
	temporary := filepath.Join(root, "nodes.jsonl.tmp")
	file, err := os.Create(temporary)
	require.NoError(t, err)
	writer, err := jsonl.NewNodeWriter(file, jsonl.Config{Codec: jsonl.CodecNone})
	require.NoError(t, err)
	require.NoError(t, writer.Push(nodes))
	require.NoError(t, writer.Close())
	artifact, err := writer.Result()
	require.NoError(t, err)
	require.NoError(t, file.Close())
	installArtifact(t, root, temporary, path)
	return &collection.JSONLArtifact{Path: path, Artifact: artifact}
}

func installJSONLRelationships(t *testing.T, root, graph string, relationships []entity.Relationship) *collection.JSONLArtifact {
	t.Helper()
	path := collection.RelationshipJSONLPath(graph, 1, jsonl.CodecNone)
	temporary := filepath.Join(root, "relationships.jsonl.tmp")
	file, err := os.Create(temporary)
	require.NoError(t, err)
	writer, err := jsonl.NewRelationshipWriter(file, jsonl.Config{Codec: jsonl.CodecNone})
	require.NoError(t, err)
	require.NoError(t, writer.Push(relationships))
	require.NoError(t, writer.Close())
	artifact, err := writer.Result()
	require.NoError(t, err)
	require.NoError(t, file.Close())
	installArtifact(t, root, temporary, path)
	return &collection.JSONLArtifact{Path: path, Artifact: artifact}
}

func installParquetNodes(t *testing.T, root, graph string, nodes []entity.Node) *collection.ParquetArtifact {
	t.Helper()
	path := collection.NodeParquetPath(graph, 1)
	temporary := filepath.Join(root, "nodes.parquet.tmp")
	file, err := os.Create(temporary)
	require.NoError(t, err)
	writer, err := parquet.NewNodeWriter(file, parquet.Config{})
	require.NoError(t, err)
	require.NoError(t, writer.Push(nodes))
	require.NoError(t, writer.Close())
	artifact, err := writer.Result()
	require.NoError(t, err)
	require.NoError(t, file.Close())
	installArtifact(t, root, temporary, path)
	return &collection.ParquetArtifact{Path: path, Artifact: artifact}
}

func installParquetRelationships(t *testing.T, root, graph string, relationships []entity.Relationship) *collection.ParquetArtifact {
	t.Helper()
	path := collection.RelationshipParquetPath(graph, 1)
	temporary := filepath.Join(root, "relationships.parquet.tmp")
	file, err := os.Create(temporary)
	require.NoError(t, err)
	writer, err := parquet.NewRelationshipWriter(file, parquet.Config{})
	require.NoError(t, err)
	require.NoError(t, writer.Push(relationships))
	require.NoError(t, writer.Close())
	artifact, err := writer.Result()
	require.NoError(t, err)
	require.NoError(t, file.Close())
	installArtifact(t, root, temporary, path)
	return &collection.ParquetArtifact{Path: path, Artifact: artifact}
}

func installArtifact(t *testing.T, root, temporary, relative string) {
	t.Helper()
	final := filepath.Join(root, filepath.FromSlash(relative))
	require.NoError(t, os.MkdirAll(filepath.Dir(final), 0o700))
	require.NoError(t, os.Rename(temporary, final))
}

func writeManifest(t *testing.T, root string, manifest collection.Manifest) {
	t.Helper()
	require.NoError(t, collection.Write(root, manifest))
}

func readManifest(t *testing.T, root string) collection.Manifest {
	t.Helper()
	manifest, err := collection.Read(root)
	require.NoError(t, err)
	return manifest
}

func buildMetrics(t *testing.T, nodes []entity.Node, relationships []entity.Relationship) metrics.GraphMetrics {
	t.Helper()
	builder := metrics.NewBuilder()
	for _, node := range nodes {
		require.NoError(t, builder.ObserveNode(node))
	}
	for _, relationship := range relationships {
		require.NoError(t, builder.ObserveRelationship(relationship))
	}
	return builder.Finalize()
}

func firstSeenCatalog(nodes []entity.Node, relationships []entity.Relationship) []string {
	seen := map[string]struct{}{}
	var result []string
	add := func(kind string) {
		if _, found := seen[kind]; !found {
			seen[kind] = struct{}{}
			result = append(result, kind)
		}
	}
	for _, node := range nodes {
		for _, kind := range node.Kinds {
			add(kind)
		}
	}
	for _, relationship := range relationships {
		add(relationship.Kind)
	}
	return result
}

func parquetPaths(manifest collection.Manifest) []string {
	var result []string
	for _, graph := range manifest.Graphs {
		for _, shard := range graph.NodeShards {
			if shard.Parquet != nil {
				result = append(result, shard.Parquet.Path)
			}
		}
		for _, shard := range graph.RelationshipShards {
			if shard.Parquet != nil {
				result = append(result, shard.Parquet.Path)
			}
		}
	}
	return result
}

func TestVerifyJSONLForLoadErrorNamesMissingJSONLShard(t *testing.T) {
	nodes, _ := verificationEntities()
	root := writeVerificationCollection(t, nil, nodes, nil, nil, false, true)

	_, err := collection.VerifyJSONLForLoad(context.Background(), root, nil)

	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "node shard 1") && strings.Contains(err.Error(), "JSONL"))
}
