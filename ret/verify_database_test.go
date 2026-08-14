package ret

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ret/collection"
	"github.com/specterops/dawgs/ret/observe"
	"github.com/stretchr/testify/require"
)

func TestVerifyDatabaseDoesNotOpenArtifacts(t *testing.T) {
	// Break caught: using full collection verification and opening JSONL or
	// Parquet even though database verification consumes only manifest metadata.
	root, database := collectionAndMatchingDatabase(t, []string{"asset"}, map[string]*dumpTestGraph{
		"asset": {
			nodes:         dumpTestNodes(1, 2),
			relationships: dumpTestRelationships(10, 1, 2, "LINKED"),
		},
	})
	removeEveryArtifact(t, root)

	result, err := VerifyDatabase(context.Background(), database, VerifyDatabaseConfig{
		Directory: root,
		BatchSize: 2,
	})

	require.NoError(t, err)
	require.Equal(t, VerifyDatabaseResult{GraphCount: 1, NodeCount: 2, RelationshipCount: 1}, result)
}

func TestVerifyDatabaseReportsEveryGraphMismatch(t *testing.T) {
	// Break caught: returning after the first graph difference or comparing only
	// totals and missing catalog/metric mismatches.
	root, database := collectionAndMatchingDatabase(t, []string{"first", "second"}, map[string]*dumpTestGraph{
		"first": {
			nodes:         dumpTestNodes(1, 2),
			relationships: dumpTestRelationships(10, 1, 2, "LINKED"),
		},
		"second": {nodes: dumpTestNodesWithKinds([]string{"Original"}, 3)},
	})
	database.graphs["first"].relationships[0].Kind = graph.StringKind("CHANGED")
	database.graphs["second"].nodes[0].Kinds = graph.Kinds{graph.StringKind("Changed")}

	_, err := VerifyDatabase(context.Background(), database, validVerifyDatabaseConfig(root))

	require.ErrorIs(t, err, ErrMetricsMismatch)
	require.ErrorContains(t, err, `graph "first" kind catalog differs`)
	require.ErrorContains(t, err, "relationship kinds")
	require.ErrorContains(t, err, `graph "second" kind catalog differs`)
	require.ErrorContains(t, err, "node kind sequences")
}

func TestVerifyDatabaseRejectsReorderedKindsWithSameCatalog(t *testing.T) {
	// Break caught: treating the catalog as a set and omitting ordered node-kind
	// sequence metrics. Both variants first see A then B, but their nodes differ.
	root, database := collectionAndMatchingDatabase(t, []string{"asset"}, map[string]*dumpTestGraph{
		"asset": {
			nodes: []*graph.Node{
				graph.NewNode(1, graph.NewProperties(), graph.StringKind("A"), graph.StringKind("B"), graph.StringKind("A")),
			},
		},
	})
	database.graphs["asset"].nodes[0].Kinds = graph.Kinds{graph.StringKind("A"), graph.StringKind("A"), graph.StringKind("B")}

	_, err := VerifyDatabase(context.Background(), database, validVerifyDatabaseConfig(root))

	require.ErrorIs(t, err, ErrMetricsMismatch)
	require.NotContains(t, err.Error(), "kind catalog differs")
	require.ErrorContains(t, err, "node kind sequences")
}

func TestVerifyDatabasePreservesManifestGraphSourceAndLifecycleOrder(t *testing.T) {
	// Break caught: sorting graphs, interleaving phases, exceeding source batch
	// size, or emitting a lifecycle different from the dump-style scan.
	root, database := collectionAndMatchingDatabase(t, []string{"second", "first"}, map[string]*dumpTestGraph{
		"second": {
			nodes:         dumpTestNodes(1, 2, 3),
			relationships: dumpTestRelationships(10, 1, 2, "LINKED"),
		},
		"first": {nodes: dumpTestNodes(20)},
	})
	database.fetches = nil
	var events []observe.Event

	result, err := VerifyDatabase(context.Background(), database, VerifyDatabaseConfig{
		Directory: root,
		BatchSize: 2,
		Observer: observe.ObserverFunc(func(_ context.Context, event observe.Event) {
			events = append(events, event)
		}),
	})

	require.NoError(t, err)
	require.Equal(t, VerifyDatabaseResult{GraphCount: 2, NodeCount: 4, RelationshipCount: 1}, result)
	require.Equal(t, []string{
		"second:nodes", "second:nodes", "second:nodes", "second:relationships", "second:relationships",
		"first:nodes", "first:nodes", "first:relationships",
	}, database.fetches)
	require.Equal(t, []string{
		"operation_started:verify_database",
		"graph_started:second",
		"phase_started:second:nodes:0:3",
		"phase_progress:second:nodes:1:3",
		"phase_progress:second:nodes:2:3",
		"phase_progress:second:nodes:3:3",
		"phase_completed:second:nodes:3",
		"phase_started:second:relationships:0:1",
		"phase_progress:second:relationships:1:1",
		"phase_completed:second:relationships:1",
		"graph_completed:second:3:1",
		"graph_started:first",
		"phase_started:first:nodes:0:1",
		"phase_progress:first:nodes:1:1",
		"phase_completed:first:nodes:1",
		"phase_started:first:relationships:0:0",
		"phase_completed:first:relationships:0",
		"graph_completed:first:1:0",
		"operation_completed:verify_database:ok",
	}, verifyDatabaseEventNames(events))
}

func TestVerifyDatabaseObserverCancellationStopsBeforeLaterSourceQueriesOrEvents(t *testing.T) {
	// Break caught: continuing into another source query or completion event after
	// a synchronous progress observer cancels the operation.
	root, database := collectionAndMatchingDatabase(t, []string{"asset"}, map[string]*dumpTestGraph{
		"asset": {
			nodes:         dumpTestNodes(1, 2),
			relationships: dumpTestRelationships(10, 1, 2, "LINKED"),
		},
	})
	database.fetches = nil
	ctx, cancel := context.WithCancel(context.Background())
	var events []observe.Event

	result, err := VerifyDatabase(ctx, database, VerifyDatabaseConfig{
		Directory: root,
		BatchSize: 1,
		Observer: observe.ObserverFunc(func(_ context.Context, event observe.Event) {
			events = append(events, event)
			if value, ok := event.(observe.PhaseProgress); ok && value.Phase == "nodes" {
				cancel()
			}
		}),
	})

	require.Zero(t, result)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []string{"asset:nodes"}, database.fetches)
	require.Equal(t, []string{
		"operation_started:verify_database",
		"graph_started:asset",
		"phase_started:asset:nodes:0:2",
		"phase_progress:asset:nodes:1:2",
		"operation_completed:verify_database:error",
	}, verifyDatabaseEventNames(events))
	requireSingleCanceledTerminalEvent(t, events)
}

func collectionAndMatchingDatabase(
	t *testing.T,
	graphOrder []string,
	graphs map[string]*dumpTestGraph,
) (string, *dumpTestDatabase) {
	t.Helper()
	root := writeLoadCollection(t, graphOrder, graphs, true, true)
	return root, newDumpTestDatabase(graphs)
}

func validVerifyDatabaseConfig(root string) VerifyDatabaseConfig {
	return VerifyDatabaseConfig{Directory: root, BatchSize: 2}
}

func removeEveryArtifact(t *testing.T, root string) {
	t.Helper()
	manifest, err := collection.Read(root)
	require.NoError(t, err)
	for _, graphEntry := range manifest.Graphs {
		for _, shard := range graphEntry.NodeShards {
			if shard.JSONL != nil {
				require.NoError(t, os.Remove(filepath.Join(root, filepath.FromSlash(shard.JSONL.Path))))
			}
			if shard.Parquet != nil {
				require.NoError(t, os.Remove(filepath.Join(root, filepath.FromSlash(shard.Parquet.Path))))
			}
		}
		for _, shard := range graphEntry.RelationshipShards {
			if shard.JSONL != nil {
				require.NoError(t, os.Remove(filepath.Join(root, filepath.FromSlash(shard.JSONL.Path))))
			}
			if shard.Parquet != nil {
				require.NoError(t, os.Remove(filepath.Join(root, filepath.FromSlash(shard.Parquet.Path))))
			}
		}
	}
}

func verifyDatabaseEventNames(events []observe.Event) []string {
	names := make([]string, len(events))
	for index, event := range events {
		switch value := event.(type) {
		case observe.OperationStarted:
			names[index] = "operation_started:" + value.Operation
		case observe.OperationCompleted:
			status := "ok"
			if value.Err != nil {
				status = "error"
			}
			names[index] = "operation_completed:" + value.Operation + ":" + status
		case observe.GraphStarted:
			names[index] = "graph_started:" + value.Graph
		case observe.GraphCompleted:
			names[index] = fmt.Sprintf("graph_completed:%s:%d:%d", value.Graph, value.Nodes, value.Relationships)
		case observe.PhaseStarted:
			names[index] = fmt.Sprintf("phase_started:%s:%s:%d:%d", value.Graph, value.Phase, value.Completed, value.Total)
		case observe.PhaseProgress:
			names[index] = fmt.Sprintf("phase_progress:%s:%s:%d:%d", value.Graph, value.Phase, value.Completed, value.Total)
		case observe.PhaseCompleted:
			names[index] = fmt.Sprintf("phase_completed:%s:%s:%d", value.Graph, value.Phase, value.Completed)
		default:
			names[index] = fmt.Sprintf("unexpected:%T", event)
		}
	}
	return names
}
