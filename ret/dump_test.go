package ret

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	cypherModel "github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ret/checkpoint"
	"github.com/specterops/dawgs/ret/collection"
	"github.com/specterops/dawgs/ret/dawgs"
	"github.com/specterops/dawgs/ret/entity"
	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/observe"
	"github.com/specterops/dawgs/ret/parquet"
	"github.com/specterops/dawgs/ret/scrub"
	"github.com/stretchr/testify/require"
)

func TestDumpSplitsDatabaseBatchAcrossLogicalShards(t *testing.T) {
	// Break caught: treating each database batch as one shard instead of enforcing
	// exact logical shard boundaries within that batch.
	database := newDumpTestDatabase(map[string]*dumpTestGraph{
		"asset": {nodes: dumpTestNodes(1, 2, 3, 4, 5)},
	})
	config := validRootDumpConfig(t)
	config.EntityBatchSize = 5
	config.ShardSize = 2

	result, err := Dump(context.Background(), database, config)

	require.NoError(t, err)
	manifest := readDumpManifest(t, result.ManifestPath)
	require.Equal(t, []int64{2, 2, 1}, dumpNodeShardCounts(manifest.Graphs[0]))
	require.Equal(t, []uint64{2, 4, 5}, dumpNodeShardCursors(manifest.Graphs[0]))

	var sourceIDs []string
	for _, shard := range manifest.Graphs[0].NodeShards {
		nodes, err := readJSONLNodesForTest(config.Directory, *shard.JSONL)
		require.NoError(t, err)
		for _, node := range nodes {
			sourceIDs = append(sourceIDs, node.SourceID)
		}
	}
	require.Equal(t, []string{"1", "2", "3", "4", "5"}, sourceIDs)
}

func TestDumpWritesEmptyGraphWithoutShards(t *testing.T) {
	// Break caught: inventing an empty artifact or omitting a valid empty graph
	// from the final collection.
	database := newDumpTestDatabase(map[string]*dumpTestGraph{"asset": {}})
	config := validRootDumpConfig(t)

	result, err := Dump(context.Background(), database, config)

	require.NoError(t, err)
	manifest := readDumpManifest(t, result.ManifestPath)
	require.Len(t, manifest.Graphs, 1)
	require.Empty(t, manifest.Graphs[0].NodeShards)
	require.Empty(t, manifest.Graphs[0].RelationshipShards)
	require.Zero(t, manifest.Graphs[0].NodeCount)
	require.Zero(t, manifest.Graphs[0].RelationshipCount)
	require.Equal(t, 1, result.GraphCount)
	require.Zero(t, result.NodeCount)
	require.Zero(t, result.RelationshipCount)
}

func TestDumpWritesOnePartialShardForEveryEnabledOutputMode(t *testing.T) {
	// Break caught: coupling JSONL and Parquet publication, or dropping a final
	// partial logical shard when the phase ends below ShardSize.
	for _, test := range []struct {
		name        string
		jsonl       bool
		parquet     bool
		wantJSONL   bool
		wantParquet bool
	}{
		{name: "JSONL only", jsonl: true, wantJSONL: true},
		{name: "Parquet only", parquet: true, wantParquet: true},
		{name: "dual output", jsonl: true, parquet: true, wantJSONL: true, wantParquet: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := newDumpTestDatabase(map[string]*dumpTestGraph{
				"asset": {nodes: dumpTestNodes(7)},
			})
			config := validRootDumpConfig(t)
			config.ShardSize = 3
			if !test.jsonl {
				config.JSONL = nil
			}
			if !test.parquet {
				config.Parquet = nil
			}

			result, err := Dump(context.Background(), database, config)

			require.NoError(t, err)
			manifest := readDumpManifest(t, result.ManifestPath)
			require.Len(t, manifest.Graphs[0].NodeShards, 1)
			shard := manifest.Graphs[0].NodeShards[0]
			require.EqualValues(t, 1, shard.Count)
			require.Equal(t, test.wantJSONL, shard.JSONL != nil)
			require.Equal(t, test.wantParquet, shard.Parquet != nil)
		})
	}
}

func TestDumpPreservesCallerGraphAndEntityPhaseOrder(t *testing.T) {
	// Break caught: sorting graphs or interleaving relationship reads before all
	// nodes for a graph have been processed.
	database := newDumpTestDatabase(map[string]*dumpTestGraph{
		"second": {
			nodes:         dumpTestNodesWithKinds([]string{"User", "Admin", "User"}, 1, 2),
			relationships: dumpTestRelationships(10, 1, 2, "MEMBER_OF"),
		},
		"first": {nodes: dumpTestNodes(20)},
	})
	config := validRootDumpConfig(t)
	config.Graphs = []string{"second", "first"}
	config.EntityBatchSize = 10
	config.ShardSize = 10

	result, err := Dump(context.Background(), database, config)

	require.NoError(t, err)
	manifest := readDumpManifest(t, result.ManifestPath)
	require.Equal(t, []string{"second", "first"}, []string{manifest.Graphs[0].Name, manifest.Graphs[1].Name})
	require.Equal(t, []string{"second:nodes", "second:nodes", "second:relationships", "second:relationships", "first:nodes", "first:nodes", "first:relationships"}, database.fetches)
	require.Equal(t, []string{"User", "Admin", "MEMBER_OF"}, manifest.Graphs[0].KindCatalog)
	require.EqualValues(t, 2, manifest.Graphs[0].Metrics.NodeCount)
	require.EqualValues(t, 1, manifest.Graphs[0].Metrics.RelationshipCount)
	require.Equal(t, 2, result.GraphCount)
	require.EqualValues(t, 3, result.NodeCount)
	require.EqualValues(t, 1, result.RelationshipCount)
}

func TestDumpRejectsExistingDestinationWithoutTouchingIt(t *testing.T) {
	// Break caught: accepting a pre-existing destination and mixing a fresh dump
	// with files the caller already owns.
	root := t.TempDir()
	sentinel := filepath.Join(root, "sentinel")
	require.NoError(t, os.WriteFile(sentinel, []byte("keep"), 0o600))
	config := validRootDumpConfig(t)
	config.Directory = root

	_, err := Dump(context.Background(), newDumpTestDatabase(map[string]*dumpTestGraph{"asset": {}}), config)

	require.ErrorIs(t, err, ErrDestinationExists)
	require.FileExists(t, sentinel)
	require.NoFileExists(t, filepath.Join(root, checkpointFileNameForTest))
}

func TestDumpObserverEventsAreOrderedAndShardEventsFollowCheckpoint(t *testing.T) {
	// Break caught: emitting lifecycle events out of order, using a different
	// entity vocabulary from artifact events, or announcing a shard before its
	// checkpoint is durable.
	database := newDumpTestDatabase(map[string]*dumpTestGraph{
		"asset": {
			nodes:         dumpTestNodes(1, 2),
			relationships: dumpTestRelationships(10, 1, 2, "LINKED"),
		},
	})
	config := validRootDumpConfig(t)
	config.EntityBatchSize = 2
	config.ShardSize = 2
	var names []string
	config.Observer = observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		names = append(names, dumpTestEventName(event))
		shard, ok := event.(observe.ShardCommitted)
		if !ok {
			return
		}
		state, exists, err := (checkpoint.Store{Root: config.Directory}).Load()
		require.NoError(t, err)
		require.True(t, exists)
		if shard.EntityType == "node" {
			require.Len(t, state.Graphs[0].NodeShards, 1)
		} else {
			require.Len(t, state.Graphs[0].RelationshipShards, 1)
		}
	})

	_, err := Dump(context.Background(), database, config)

	require.NoError(t, err)
	require.Equal(t, []string{
		"operation_started",
		"graph_started:asset",
		"phase_started:nodes:0",
		"phase_progress:nodes:1",
		"phase_progress:nodes:2",
		"shard:node:1",
		"phase_completed:nodes",
		"phase_started:relationships:0",
		"phase_progress:relationships:1",
		"shard:relationship:1",
		"phase_completed:relationships",
		"graph_completed:asset",
		"operation_completed",
	}, names)
}

func TestDumpObserverTerminalEventContainsFailureAndIsLast(t *testing.T) {
	// Break caught: dropping the terminal cause from observation or emitting
	// success/graph events after a writer has failed.
	injected := errors.New("injected observer-order writer failure")
	originalWrite := writeJSONLNodes
	writeJSONLNodes = func(string, string, jsonl.Config, []entity.Node) (collection.JSONLArtifact, error) {
		return collection.JSONLArtifact{}, injected
	}
	t.Cleanup(func() { writeJSONLNodes = originalWrite })

	database := newDumpTestDatabase(map[string]*dumpTestGraph{"asset": {nodes: dumpTestNodes(1)}})
	config := validRootDumpConfig(t)
	var events []observe.Event
	config.Observer = observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		events = append(events, event)
	})

	_, err := Dump(context.Background(), database, config)

	require.ErrorIs(t, err, injected)
	require.NotEmpty(t, events)
	completed, ok := events[len(events)-1].(observe.OperationCompleted)
	require.True(t, ok)
	require.ErrorIs(t, completed.Err, injected)
	for _, event := range events[:len(events)-1] {
		_, graphCompleted := event.(observe.GraphCompleted)
		require.False(t, graphCompleted)
	}
}

func TestDumpObserverCancellationStopsLaterNonTerminalLifecycleEvents(t *testing.T) {
	// Break caught: synchronously observing cancellation but emitting the next
	// phase or graph lifecycle event before checking the context again.
	for _, test := range []struct {
		name      string
		cancelOn  func(observe.Event) bool
		wantPhase checkpoint.Phase
	}{
		{
			name: "after graph started",
			cancelOn: func(event observe.Event) bool {
				_, ok := event.(observe.GraphStarted)
				return ok
			},
			wantPhase: checkpoint.PhaseNodes,
		},
		{
			name: "between node and relationship phases",
			cancelOn: func(event observe.Event) bool {
				value, ok := event.(observe.PhaseCompleted)
				return ok && value.Phase == string(checkpoint.PhaseNodes)
			},
			wantPhase: checkpoint.PhaseRelationships,
		},
		{
			name: "before graph completed",
			cancelOn: func(event observe.Event) bool {
				value, ok := event.(observe.PhaseCompleted)
				return ok && value.Phase == string(checkpoint.PhaseRelationships)
			},
			wantPhase: checkpoint.PhaseComplete,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			database := newDumpTestDatabase(map[string]*dumpTestGraph{"asset": {}})
			config := validRootDumpConfig(t)
			var events []observe.Event
			cancelEvent := -1
			config.Observer = observe.ObserverFunc(func(_ context.Context, event observe.Event) {
				events = append(events, event)
				if cancelEvent < 0 && test.cancelOn(event) {
					cancelEvent = len(events) - 1
					cancel()
				}
			})

			_, err := Dump(ctx, database, config)

			require.ErrorIs(t, err, context.Canceled)
			require.GreaterOrEqual(t, cancelEvent, 0)
			require.Len(t, events[cancelEvent+1:], 1)
			completed, ok := events[len(events)-1].(observe.OperationCompleted)
			require.True(t, ok)
			require.ErrorIs(t, completed.Err, context.Canceled)
			state, exists, loadErr := (checkpoint.Store{Root: config.Directory}).Load()
			require.NoError(t, loadErr)
			require.True(t, exists)
			require.Equal(t, test.wantPhase, state.Graphs[0].Phase)
			require.NoFileExists(t, filepath.Join(config.Directory, collection.ManifestName))
		})
	}
}

func TestDumpCancellationDuringScanningDoesNotPublishOrCheckpointProgress(t *testing.T) {
	// Break caught: processing a batch returned concurrently with cancellation
	// and publishing artifacts or a later cursor before returning context.Canceled.
	ctx, cancel := context.WithCancel(context.Background())
	database := newDumpTestDatabase(map[string]*dumpTestGraph{"asset": {nodes: dumpTestNodes(1, 2)}})
	config := validRootDumpConfig(t)
	var initialCheckpoint []byte
	database.onNodeFetch = func(string) {
		initialCheckpoint = mustReadFile(t, filepath.Join(config.Directory, checkpoint.FileName))
		cancel()
	}
	var events []observe.Event
	config.Observer = observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		events = append(events, event)
	})

	_, err := Dump(ctx, database, config)

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, initialCheckpoint, mustReadFile(t, filepath.Join(config.Directory, checkpoint.FileName)))
	require.Len(t, regularFiles(t, config.Directory), 1)
	require.NoFileExists(t, filepath.Join(config.Directory, collection.ManifestName))
	completed, ok := events[len(events)-1].(observe.OperationCompleted)
	require.True(t, ok)
	require.ErrorIs(t, completed.Err, context.Canceled)
	for _, event := range events {
		_, committed := event.(observe.ShardCommitted)
		require.False(t, committed)
	}
}

func TestDumpCancellationWithEmptyTerminalBatchDoesNotAdvancePhaseCheckpoint(t *testing.T) {
	// Break caught: overlooking cancellation returned alongside the terminal
	// empty scan batch and checkpointing a phase transition afterward.
	ctx, cancel := context.WithCancel(context.Background())
	database := newDumpTestDatabase(map[string]*dumpTestGraph{"asset": {nodes: dumpTestNodes(1, 2)}})
	config := validRootDumpConfig(t)
	config.EntityBatchSize = 2
	config.ShardSize = 2
	fetches := 0
	var checkpointAtCancellation []byte
	database.onNodeFetch = func(string) {
		fetches++
		if fetches == 2 {
			checkpointAtCancellation = mustReadFile(t, filepath.Join(config.Directory, checkpoint.FileName))
			cancel()
		}
	}

	_, err := Dump(ctx, database, config)

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, checkpointAtCancellation, mustReadFile(t, filepath.Join(config.Directory, checkpoint.FileName)))
	state, exists, loadErr := (checkpoint.Store{Root: config.Directory}).Load()
	require.NoError(t, loadErr)
	require.True(t, exists)
	require.Equal(t, checkpoint.PhaseNodes, state.Graphs[0].Phase)
	require.Len(t, state.Graphs[0].NodeShards, 1)
	require.NoFileExists(t, filepath.Join(config.Directory, collection.ManifestName))
}

func TestDumpCancellationDuringInitialSnapshotDoesNotPublishCheckpoint(t *testing.T) {
	// Break caught: treating a successful snapshot as proof the context remains
	// active and publishing the initial checkpoint after cancellation.
	ctx, cancel := context.WithCancel(context.Background())
	database := newDumpTestDatabase(map[string]*dumpTestGraph{"asset": {}})
	database.onRelationshipCount = func(context.Context, string) {
		cancel()
	}
	config := validRootDumpConfig(t)

	_, err := Dump(ctx, database, config)

	require.ErrorIs(t, err, context.Canceled)
	require.NoFileExists(t, filepath.Join(config.Directory, checkpoint.FileName))
	require.NoFileExists(t, filepath.Join(config.Directory, collection.ManifestName))
}

func TestDumpCancellationDuringFinalSnapshotDoesNotPublishManifest(t *testing.T) {
	// Break caught: accepting the final recount result after cancellation and
	// replacing the resumable checkpoint with a manifest.
	ctx, cancel := context.WithCancel(context.Background())
	database := newDumpTestDatabase(map[string]*dumpTestGraph{"asset": {nodes: dumpTestNodes(1)}})
	counts := 0
	database.onRelationshipCount = func(context.Context, string) {
		counts++
		if counts == 2 {
			cancel()
		}
	}
	config := validRootDumpConfig(t)

	_, err := Dump(ctx, database, config)

	require.ErrorIs(t, err, context.Canceled)
	require.FileExists(t, filepath.Join(config.Directory, checkpoint.FileName))
	require.NoFileExists(t, filepath.Join(config.Directory, collection.ManifestName))
}

func TestDumpCancellationBeforeManifestPublicationDoesNotPublishManifest(t *testing.T) {
	// Break caught: removing the context gate immediately before manifest
	// publication after the final recount's post-snapshot gate has succeeded.
	ctx := newStagedCancelContext()
	database := newDumpTestDatabase(map[string]*dumpTestGraph{"asset": {}})
	snapshots := 0
	database.onRelationshipCount = func(context.Context, string) {
		snapshots++
		if snapshots == 2 {
			// The recount post-snapshot gate observes an active context; the
			// immediately following pre-publication gate observes cancellation.
			ctx.arm(2)
		}
	}
	config := validRootDumpConfig(t)

	_, err := Dump(ctx, database, config)

	require.ErrorIs(t, err, context.Canceled)
	require.FileExists(t, filepath.Join(config.Directory, checkpoint.FileName))
	require.NoFileExists(t, filepath.Join(config.Directory, collection.ManifestName))
}

func TestDumpCancellationFromFinalShardObserverDoesNotAdvancePhase(t *testing.T) {
	// Break caught: allowing synchronous observer cancellation after the final
	// partial shard commit to publish a later phase checkpoint or phase event.
	for _, test := range []struct {
		name      string
		cancelOn  string
		value     *dumpTestGraph
		wantPhase checkpoint.Phase
	}{
		{
			name:      "node shard",
			cancelOn:  "node",
			value:     &dumpTestGraph{nodes: dumpTestNodes(1)},
			wantPhase: checkpoint.PhaseNodes,
		},
		{
			name:     "relationship shard",
			cancelOn: "relationship",
			value: &dumpTestGraph{
				nodes:         dumpTestNodes(1, 2),
				relationships: dumpTestRelationships(10, 1, 2, "LINKED"),
			},
			wantPhase: checkpoint.PhaseRelationships,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			database := newDumpTestDatabase(map[string]*dumpTestGraph{"asset": test.value})
			config := validRootDumpConfig(t)
			config.EntityBatchSize = 2
			config.ShardSize = 3
			var events []observe.Event
			cancelEvent := -1
			config.Observer = observe.ObserverFunc(func(_ context.Context, event observe.Event) {
				events = append(events, event)
				if shard, ok := event.(observe.ShardCommitted); ok && shard.EntityType == test.cancelOn {
					cancelEvent = len(events) - 1
					cancel()
				}
			})

			_, err := Dump(ctx, database, config)

			require.ErrorIs(t, err, context.Canceled)
			require.GreaterOrEqual(t, cancelEvent, 0)
			state, exists, loadErr := (checkpoint.Store{Root: config.Directory}).Load()
			require.NoError(t, loadErr)
			require.True(t, exists)
			require.Equal(t, test.wantPhase, state.Graphs[0].Phase)
			require.IsType(t, observe.OperationCompleted{}, events[len(events)-1])
			require.Len(t, events[cancelEvent+1:], 1)
			require.NoFileExists(t, filepath.Join(config.Directory, collection.ManifestName))
		})
	}
}

func TestDumpScrubsConcreteArtifactsAndRecordsPerShardActions(t *testing.T) {
	// Break caught: bypassing root scrubbing, writing pre-scrub properties, or
	// dropping node/relationship action counts from logical shard metadata.
	database := newDumpTestDatabase(map[string]*dumpTestGraph{
		"asset": {
			nodes: []*graph.Node{
				graph.NewNode(1, graph.AsProperties(map[string]any{"password": "one"}), graph.StringKind("Entity")),
				graph.NewNode(2, graph.AsProperties(map[string]any{"password": "two"}), graph.StringKind("Entity")),
			},
			relationships: []*graph.Relationship{
				graph.NewRelationship(
					10,
					1,
					2,
					graph.AsProperties(map[string]any{"password": "edge-secret"}),
					graph.StringKind("LINKED"),
				),
			},
		},
	})
	config := validRootDumpConfig(t)
	config.Parquet = nil
	config.EntityBatchSize = 2
	config.ShardSize = 2

	result, err := Dump(context.Background(), database, config)

	require.NoError(t, err)
	manifest := readDumpManifest(t, result.ManifestPath)
	require.Equal(t, scrub.ActionCounts{Redact: 2}, manifest.Graphs[0].NodeShards[0].ScrubCounts)
	require.Equal(t, scrub.ActionCounts{Redact: 1}, manifest.Graphs[0].RelationshipShards[0].ScrubCounts)
	nodes, err := readJSONLNodesForTest(
		config.Directory,
		*manifest.Graphs[0].NodeShards[0].JSONL,
	)
	require.NoError(t, err)
	nodePasswords := make([]any, 0, len(nodes))
	for _, node := range nodes {
		nodePasswords = append(nodePasswords, node.Properties["password"])
	}
	require.Equal(t, []any{"[REDACTED]", "[REDACTED]"}, nodePasswords)
	relationships, err := readJSONLRelationshipsForTest(
		config.Directory,
		*manifest.Graphs[0].RelationshipShards[0].JSONL,
	)
	require.NoError(t, err)
	require.Len(t, relationships, 1)
	require.Equal(t, "[REDACTED]", relationships[0].Properties["password"])
}

func TestDumpWithoutScrubConfigPreservesPropertiesAndDisablesScrubMetadata(t *testing.T) {
	// Break caught: compiling or applying a zero-value scrub policy when the
	// library caller disables scrubbing with a nil configuration.
	database := newDumpTestDatabase(map[string]*dumpTestGraph{
		"asset": {
			nodes: []*graph.Node{
				graph.NewNode(1, graph.AsProperties(map[string]any{"password": "secret"}), graph.StringKind("Entity")),
			},
		},
	})
	config := validRootDumpConfig(t)
	config.Parquet = nil
	config.Scrub = nil

	result, err := Dump(context.Background(), database, config)

	require.NoError(t, err)
	manifest := readDumpManifest(t, result.ManifestPath)
	require.False(t, manifest.Scrub.Enabled)
	require.Empty(t, manifest.Scrub.RulesFingerprint)
	require.Empty(t, manifest.Scrub.SaltFingerprint)
	require.True(t, manifest.Graphs[0].NodeShards[0].ScrubCounts.IsZero())
	nodes, err := readJSONLNodesForTest(
		config.Directory,
		*manifest.Graphs[0].NodeShards[0].JSONL,
	)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, "secret", nodes[0].Properties["password"])
}

const checkpointFileNameForTest = ".ret-checkpoint.json"

func validRootDumpConfig(t *testing.T) DumpConfig {
	t.Helper()
	return DumpConfig{
		Directory:       filepath.Join(t.TempDir(), "collection"),
		Graphs:          []string{"asset"},
		EntityBatchSize: 2,
		ShardSize:       2,
		JSONL:           pointerTo(jsonl.Config{Codec: jsonl.CodecNone}),
		Parquet:         pointerTo(parquet.Config{}),
		Scrub:           pointerTo(scrub.DefaultConfig()),
	}
}

func readDumpManifest(t *testing.T, manifestPath string) collection.Manifest {
	t.Helper()
	require.Equal(t, collection.ManifestName, filepath.Base(manifestPath))
	manifest, err := collection.Read(filepath.Dir(manifestPath))
	require.NoError(t, err)
	return manifest
}

func dumpNodeShardCounts(graph collection.Graph) []int64 {
	counts := make([]int64, len(graph.NodeShards))
	for index, shard := range graph.NodeShards {
		counts[index] = shard.Count
	}
	return counts
}

func dumpNodeShardCursors(graph collection.Graph) []uint64 {
	cursors := make([]uint64, len(graph.NodeShards))
	for index, shard := range graph.NodeShards {
		cursors[index] = shard.LastSourceID
	}
	return cursors
}

func dumpTestEventName(event observe.Event) string {
	switch value := event.(type) {
	case observe.OperationStarted:
		return "operation_started"
	case observe.OperationCompleted:
		return "operation_completed"
	case observe.GraphStarted:
		return "graph_started:" + value.Graph
	case observe.GraphCompleted:
		return "graph_completed:" + value.Graph
	case observe.PhaseStarted:
		return fmt.Sprintf("phase_started:%s:%d", value.Phase, value.Completed)
	case observe.PhaseProgress:
		return fmt.Sprintf("phase_progress:%s:%d", value.Phase, value.Completed)
	case observe.PhaseCompleted:
		return "phase_completed:" + value.Phase
	case observe.ShardCommitted:
		return fmt.Sprintf("shard:%s:%d", value.EntityType, value.Index)
	default:
		return fmt.Sprintf("unexpected:%T", event)
	}
}

type dumpTestGraph struct {
	nodes         []*graph.Node
	relationships []*graph.Relationship
	snapshots     []dawgs.Snapshot
	countRound    int
}

type dumpTestDatabase struct {
	graph.Database

	graphs              map[string]*dumpTestGraph
	fetches             []string
	onNodeCount         func(context.Context, string)
	onRelationshipCount func(context.Context, string)
	onNodeFetch         func(string)
	onRelationshipFetch func(string)
}

func newDumpTestDatabase(graphs map[string]*dumpTestGraph) *dumpTestDatabase {
	for _, value := range graphs {
		if len(value.snapshots) == 0 {
			value.snapshots = []dawgs.Snapshot{{
				NodeCount:         int64(len(value.nodes)),
				RelationshipCount: int64(len(value.relationships)),
			}}
		}
	}
	return &dumpTestDatabase{graphs: graphs}
}

func (s *dumpTestDatabase) ReadTransaction(ctx context.Context, delegate graph.TransactionDelegate, _ ...graph.TransactionOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return delegate(&dumpTestTransaction{database: s, ctx: ctx})
}

type dumpTestTransaction struct {
	graph.Transaction
	database  *dumpTestDatabase
	ctx       context.Context
	graphName string
}

func (s *dumpTestTransaction) WithGraph(target graph.Graph) graph.Transaction {
	s.graphName = target.Name
	return s
}

func (s *dumpTestTransaction) Nodes() graph.NodeQuery {
	return &dumpTestNodeQuery{database: s.database, ctx: s.ctx, graphName: s.graphName}
}

func (s *dumpTestTransaction) Relationships() graph.RelationshipQuery {
	return &dumpTestRelationshipQuery{database: s.database, ctx: s.ctx, graphName: s.graphName}
}

type dumpTestNodeQuery struct {
	graph.NodeQuery
	database  *dumpTestDatabase
	ctx       context.Context
	graphName string
	afterID   graph.ID
	limit     int
}

func (s *dumpTestNodeQuery) OrderBy(...graph.Criteria) graph.NodeQuery { return s }
func (s *dumpTestNodeQuery) Filter(criteria graph.Criteria) graph.NodeQuery {
	s.afterID = dumpTestAfterID(criteria)
	return s
}
func (s *dumpTestNodeQuery) Limit(limit int) graph.NodeQuery {
	s.limit = limit
	return s
}
func (s *dumpTestNodeQuery) Count() (int64, error) {
	if s.database.onNodeCount != nil {
		s.database.onNodeCount(s.ctx, s.graphName)
	}
	value := s.database.graphs[s.graphName]
	return value.snapshots[min(value.countRound, len(value.snapshots)-1)].NodeCount, nil
}
func (s *dumpTestNodeQuery) Fetch(delegate func(graph.Cursor[*graph.Node]) error, _ ...graph.Criteria) error {
	s.database.fetches = append(s.database.fetches, s.graphName+":nodes")
	if s.database.onNodeFetch != nil {
		s.database.onNodeFetch(s.graphName)
	}
	return delegate(newDumpTestCursor(dumpTestNodesAfter(s.database.graphs[s.graphName].nodes, s.afterID, s.limit)))
}

type dumpTestRelationshipQuery struct {
	graph.RelationshipQuery
	database  *dumpTestDatabase
	ctx       context.Context
	graphName string
	afterID   graph.ID
	limit     int
}

func (s *dumpTestRelationshipQuery) OrderBy(...graph.Criteria) graph.RelationshipQuery { return s }
func (s *dumpTestRelationshipQuery) Filter(criteria graph.Criteria) graph.RelationshipQuery {
	s.afterID = dumpTestAfterID(criteria)
	return s
}
func (s *dumpTestRelationshipQuery) Limit(limit int) graph.RelationshipQuery {
	s.limit = limit
	return s
}
func (s *dumpTestRelationshipQuery) Count() (int64, error) {
	if s.database.onRelationshipCount != nil {
		s.database.onRelationshipCount(s.ctx, s.graphName)
	}
	value := s.database.graphs[s.graphName]
	round := min(value.countRound, len(value.snapshots)-1)
	value.countRound++
	return value.snapshots[round].RelationshipCount, nil
}
func (s *dumpTestRelationshipQuery) Fetch(delegate func(graph.Cursor[*graph.Relationship]) error) error {
	s.database.fetches = append(s.database.fetches, s.graphName+":relationships")
	if s.database.onRelationshipFetch != nil {
		s.database.onRelationshipFetch(s.graphName)
	}
	return delegate(newDumpTestCursor(dumpTestRelationshipsAfter(s.database.graphs[s.graphName].relationships, s.afterID, s.limit)))
}

type dumpTestCursor[T any] struct {
	values chan T
}

func newDumpTestCursor[T any](values []T) *dumpTestCursor[T] {
	channel := make(chan T, len(values))
	for _, value := range values {
		channel <- value
	}
	close(channel)
	return &dumpTestCursor[T]{values: channel}
}

func (s *dumpTestCursor[T]) Error() error { return nil }
func (s *dumpTestCursor[T]) Close()       {}
func (s *dumpTestCursor[T]) Chan() chan T { return s.values }

func dumpTestAfterID(criteria graph.Criteria) graph.ID {
	comparison, ok := criteria.(*cypherModel.Comparison)
	if !ok || len(comparison.Partials) != 1 {
		panic(fmt.Sprintf("unexpected cursor criteria %T", criteria))
	}
	parameter, ok := comparison.Partials[0].Right.(*cypherModel.Parameter)
	if !ok {
		panic(fmt.Sprintf("unexpected cursor parameter %T", comparison.Partials[0].Right))
	}
	return parameter.Value.(graph.ID)
}

func dumpTestNodesAfter(values []*graph.Node, after graph.ID, limit int) []*graph.Node {
	result := make([]*graph.Node, 0, limit)
	for _, value := range values {
		if value.ID > after && len(result) < limit {
			result = append(result, value)
		}
	}
	return result
}

func dumpTestRelationshipsAfter(values []*graph.Relationship, after graph.ID, limit int) []*graph.Relationship {
	result := make([]*graph.Relationship, 0, limit)
	for _, value := range values {
		if value.ID > after && len(result) < limit {
			result = append(result, value)
		}
	}
	return result
}

func dumpTestNodes(ids ...uint64) []*graph.Node {
	return dumpTestNodesWithKinds([]string{"Entity"}, ids...)
}

func dumpTestNodesWithKinds(kinds []string, ids ...uint64) []*graph.Node {
	graphKinds := make([]graph.Kind, len(kinds))
	for index, kind := range kinds {
		graphKinds[index] = graph.StringKind(kind)
	}
	nodes := make([]*graph.Node, len(ids))
	for index, id := range ids {
		nodes[index] = graph.NewNode(graph.ID(id), graph.AsProperties(map[string]any{"id": id}), graphKinds...)
	}
	return nodes
}

func dumpTestRelationships(id, startID, endID uint64, kind string) []*graph.Relationship {
	return []*graph.Relationship{
		graph.NewRelationship(graph.ID(id), graph.ID(startID), graph.ID(endID), graph.NewProperties(), graph.StringKind(kind)),
	}
}
