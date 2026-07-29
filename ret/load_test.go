package ret

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ret/collection"
	"github.com/specterops/dawgs/ret/entity"
	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/observe"
	"github.com/specterops/dawgs/ret/parquet"
	"github.com/specterops/dawgs/ret/scrub"
	"github.com/stretchr/testify/require"
)

func TestLoadCorruptJSONLFailsBeforeDatabaseMutation(t *testing.T) {
	// Break caught: validating and writing one graph at a time, which can leave
	// earlier graphs mutated before corruption in a later JSONL artifact is found.
	root := writeLoadCollection(t, []string{"first", "second"}, map[string]*dumpTestGraph{
		"first":  {nodes: dumpTestNodes(1)},
		"second": {nodes: dumpTestNodes(2)},
	}, true, false)
	manifest, err := collection.Read(root)
	require.NoError(t, err)
	lastArtifact := manifest.Graphs[1].NodeShards[0].JSONL
	require.NoError(t, os.WriteFile(filepath.Join(root, filepath.FromSlash(lastArtifact.Path)), []byte("corrupt"), 0o600))
	database := newLoadTestDatabase()

	_, err = Load(context.Background(), database, LoadConfig{Directory: root, BatchSize: 2})

	require.ErrorIs(t, err, ErrCollectionNotLoadable)
	require.Empty(t, database.schemas)
	require.Empty(t, database.durableMutations())
}

func TestLoadIgnoresCorruptParquet(t *testing.T) {
	// Break caught: using full collection verification for load and therefore
	// opening an optional Parquet twin that JSONL replay does not consume.
	root := writeLoadCollection(t, []string{"asset"}, map[string]*dumpTestGraph{
		"asset": {
			nodes:         dumpTestNodes(1, 2),
			relationships: dumpTestRelationships(10, 1, 2, "LINKED"),
		},
	}, true, true)
	corruptLoadParquetArtifacts(t, root)
	database := newLoadTestDatabase()

	result, err := Load(context.Background(), database, LoadConfig{Directory: root, BatchSize: 2})

	require.NoError(t, err)
	require.Equal(t, LoadResult{GraphCount: 1, NodeCount: 2, RelationshipCount: 1}, result)
	require.Len(t, database.graphs["asset"].nodes, 2)
	require.Len(t, database.graphs["asset"].relationships, 1)
}

func TestLoadRejectsParquetOnlyBeforeDatabaseMutation(t *testing.T) {
	// Break caught: silently treating Parquet as a supported load source or
	// preparing the target before discovering that JSONL is absent.
	root := writeLoadCollection(t, []string{"asset"}, map[string]*dumpTestGraph{
		"asset": {nodes: dumpTestNodes(1)},
	}, false, true)
	database := newLoadTestDatabase()

	_, err := Load(context.Background(), database, LoadConfig{Directory: root, BatchSize: 1})

	require.ErrorIs(t, err, ErrCollectionNotLoadable)
	require.Empty(t, database.schemas)
	require.Empty(t, database.durableMutations())
}

func TestLoadRequiresEveryTargetEmptyBeforeAssertingAnySchema(t *testing.T) {
	// Break caught: asserting schema immediately after checking each graph,
	// which mutates schema before a later nonempty graph rejects the load.
	root := writeLoadCollection(t, []string{"first", "second"}, map[string]*dumpTestGraph{
		"first":  {nodes: dumpTestNodes(1)},
		"second": {nodes: dumpTestNodes(2)},
	}, true, false)
	database := newLoadTestDatabase()
	database.graphs["second"] = &loadTestGraphState{
		nodes: []*graph.Node{graph.NewNode(900, graph.NewProperties(), graph.StringKind("Existing"))},
	}

	_, err := Load(context.Background(), database, LoadConfig{Directory: root, BatchSize: 2})

	require.ErrorIs(t, err, ErrNonEmptyTarget)
	require.Equal(t, []string{"empty:first", "empty:second"}, database.operations)
	require.Empty(t, database.schemas)
	require.Len(t, database.graphs["second"].nodes, 1)
}

func TestLoadSnapshotFailureIsNotClassifiedAsNonEmpty(t *testing.T) {
	// Break caught: wrapping an unavailable snapshot with ErrNonEmptyTarget even
	// though no nonzero target count was observed.
	root := writeLoadCollection(t, []string{"asset"}, map[string]*dumpTestGraph{
		"asset": {nodes: dumpTestNodes(1)},
	}, true, false)
	injected := errors.New("injected load snapshot failure")
	database := newLoadTestDatabase()
	database.readErr = injected

	_, err := Load(context.Background(), database, LoadConfig{Directory: root, BatchSize: 1})

	require.ErrorIs(t, err, injected)
	require.NotErrorIs(t, err, ErrNonEmptyTarget)
	require.Empty(t, database.schemas)
}

func TestLoadEmptinessCancellationIsNotClassifiedAsNonEmpty(t *testing.T) {
	// Break caught: telling callers cleanup is required when the emptiness query
	// was merely canceled and never proved that the graph had data.
	root := writeLoadCollection(t, []string{"asset"}, map[string]*dumpTestGraph{
		"asset": {nodes: dumpTestNodes(1)},
	}, true, false)
	database := newLoadTestDatabase()
	database.readErr = context.Canceled

	_, err := Load(context.Background(), database, LoadConfig{Directory: root, BatchSize: 1})

	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, ErrNonEmptyTarget)
	require.Empty(t, database.schemas)
}

func TestLoadPreservesManifestGraphPhaseBatchAndEntityOrder(t *testing.T) {
	// Break caught: sorting manifest graphs, interleaving entity phases, sending
	// oversized slices, or normalizing away kind order and shallow properties.
	nested := map[string]any{"shared": "node"}
	root := writeLoadCollection(t, []string{"second", "first"}, map[string]*dumpTestGraph{
		"second": {
			nodes: []*graph.Node{
				graph.NewNode(1, graph.AsProperties(map[string]any{"name": "Ada", "nested": nested}), graph.StringKind("User"), graph.StringKind("Admin"), graph.StringKind("User")),
				graph.NewNode(2, graph.AsProperties(map[string]any{"name": "Ops"}), graph.StringKind("Group")),
				graph.NewNode(3, graph.AsProperties(map[string]any{"name": "Grace"}), graph.StringKind("User")),
			},
			relationships: []*graph.Relationship{
				graph.NewRelationship(10, 1, 2, graph.AsProperties(map[string]any{"role": "owner"}), graph.StringKind("MEMBER_OF")),
				graph.NewRelationship(11, 3, 2, graph.AsProperties(map[string]any{"role": "reader"}), graph.StringKind("MEMBER_OF")),
			},
		},
		"first": {nodes: dumpTestNodes(20)},
	}, true, false)
	database := newLoadTestDatabase()
	var events []observe.Event
	observer := observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		switch event.(type) {
		case observe.OperationStarted, observe.OperationCompleted, observe.GraphStarted, observe.GraphCompleted,
			observe.PhaseStarted, observe.PhaseProgress, observe.PhaseCompleted:
			events = append(events, event)
		}
	})

	result, err := Load(context.Background(), database, LoadConfig{
		Directory: root,
		BatchSize: 2,
		Observer:  observer,
	})

	require.NoError(t, err)
	require.Equal(t, LoadResult{GraphCount: 2, NodeCount: 4, RelationshipCount: 2}, result)
	require.Equal(t, []string{
		"empty:second", "empty:first",
		"schema:second", "schema:first",
		"nodes:second:2", "nodes:second:1", "relationships:second:2",
		"nodes:first:1",
	}, database.operations)
	require.Equal(t, []int{2, 1, 2, 1}, database.committedBatchSizes)
	require.Equal(t, []int{2, 2, 2, 2}, database.requestedBatchSizes)

	second := database.graphs["second"]
	require.Len(t, second.nodes, 3)
	require.Equal(t, []string{"User", "Admin", "User"}, second.nodes[0].Kinds.Strings())
	require.Equal(t, "Ada", second.nodes[0].Properties.Map["name"])
	require.Equal(t, map[string]any{"shared": "node"}, second.nodes[0].Properties.Map["nested"])
	require.Len(t, second.relationships, 2)
	require.Equal(t, "MEMBER_OF", second.relationships[0].Kind.String())
	require.Equal(t, "owner", second.relationships[0].Properties.Map["role"])

	require.Equal(t, []string{
		"operation_started:load",
		"graph_started:second",
		"phase_started:second:nodes:3",
		"phase_progress:second:nodes:2:3",
		"phase_progress:second:nodes:3:3",
		"phase_completed:second:nodes:3",
		"phase_started:second:relationships:2",
		"phase_progress:second:relationships:2:2",
		"phase_completed:second:relationships:2",
		"graph_completed:second:3:2",
		"graph_started:first",
		"phase_started:first:nodes:1",
		"phase_progress:first:nodes:1:1",
		"phase_completed:first:nodes:1",
		"phase_started:first:relationships:0",
		"phase_completed:first:relationships:0",
		"graph_completed:first:1:0",
		"operation_completed:load:ok",
	}, loadEventNames(events))
	require.NoFileExists(t, filepath.Join(root, checkpointFileNameForTest))
}

func TestLoadDatabaseFailureReportsDurablePartialGraphAndRequiredRetryCleanup(t *testing.T) {
	// Break caught: reporting only the driver error, or implying an automatic
	// rollback across earlier committed batches when the graph is partial.
	root := writeLoadCollection(t, []string{"asset"}, map[string]*dumpTestGraph{
		"asset": {
			nodes:         dumpTestNodes(1, 2, 3),
			relationships: dumpTestRelationships(10, 1, 2, "LINKED"),
		},
	}, true, false)
	injected := errors.New("injected database batch failure")
	database := newLoadTestDatabase()
	database.failBatchAt = 2
	database.failBatchErr = injected
	var events []observe.Event

	_, err := Load(context.Background(), database, LoadConfig{
		Directory: root,
		BatchSize: 2,
		Observer: observe.ObserverFunc(func(_ context.Context, event observe.Event) {
			events = append(events, event)
		}),
	})

	require.ErrorIs(t, err, injected)
	require.ErrorContains(t, err, `graph "asset"`)
	require.ErrorContains(t, err, "nodes phase")
	require.ErrorContains(t, err, "partial graph must be cleared before retry")
	require.Len(t, database.graphs["asset"].nodes, 2)
	require.Empty(t, database.graphs["asset"].relationships)
	completed, ok := events[len(events)-1].(observe.OperationCompleted)
	require.True(t, ok)
	require.ErrorIs(t, completed.Err, injected)
}

func TestLoadMissingEndpointKeepsPriorRelationshipAndRollsBackFailingBatch(t *testing.T) {
	// Break caught: losing a valid preceding relationship in the failing batch
	// test, or committing the unresolved relationship despite delegate rollback.
	root := writeLoadCollection(t, []string{"asset"}, map[string]*dumpTestGraph{
		"asset": {
			nodes: []*graph.Node{
				graph.NewNode(1, graph.NewProperties(), graph.StringKind("Entity")),
				graph.NewNode(2, graph.NewProperties(), graph.StringKind("Entity")),
			},
			relationships: []*graph.Relationship{
				graph.NewRelationship(10, 1, 2, graph.NewProperties(), graph.StringKind("VALID")),
				graph.NewRelationship(11, 2, 1, graph.NewProperties(), graph.StringKind("ALSO_VALID")),
			},
		},
	}, true, false)
	replay := func(
		_ context.Context,
		_ string,
		_ collection.Graph,
		visitNode func(entity.Node) error,
		visitRelationship func(entity.Relationship) error,
	) error {
		for _, node := range []entity.Node{
			{SourceID: "1", Kinds: []string{"Entity"}},
			{SourceID: "2", Kinds: []string{"Entity"}},
		} {
			if err := visitNode(node); err != nil {
				return err
			}
		}
		for _, relationship := range []entity.Relationship{
			{SourceID: "10", StartID: "1", EndID: "2", Kind: "VALID"},
			{SourceID: "11", StartID: "2", EndID: "missing", Kind: "BROKEN"},
		} {
			if err := visitRelationship(relationship); err != nil {
				return err
			}
		}
		return nil
	}
	database := newLoadTestDatabase()

	_, err := loadWithReplay(
		context.Background(),
		database,
		LoadConfig{Directory: root, BatchSize: 1},
		replay,
	)

	require.ErrorContains(t, err, `unresolved endpoints "2" -> "missing"`)
	require.ErrorContains(t, err, `graph "asset"`)
	require.ErrorContains(t, err, "relationships phase")
	require.ErrorContains(t, err, "partial graph must be cleared before retry")
	require.Len(t, database.graphs["asset"].nodes, 2)
	require.Len(t, database.graphs["asset"].relationships, 1)
	require.Equal(t, "VALID", database.graphs["asset"].relationships[0].Kind.String())
}

func TestLoadDetectsReplayCountMismatchAfterSuccessfulBoundedWrites(t *testing.T) {
	// Break caught: trusting the preflight manifest totals without comparing
	// what the replay callback actually delivered at the load boundary.
	root := writeLoadCollection(t, []string{"asset"}, map[string]*dumpTestGraph{
		"asset": {nodes: dumpTestNodes(1, 2)},
	}, true, false)
	replay := func(
		_ context.Context,
		_ string,
		_ collection.Graph,
		visitNode func(entity.Node) error,
		_ func(entity.Relationship) error,
	) error {
		return visitNode(entity.Node{SourceID: "1", Kinds: []string{"Entity"}})
	}
	database := newLoadTestDatabase()

	_, err := loadWithReplay(
		context.Background(),
		database,
		LoadConfig{Directory: root, BatchSize: 2},
		replay,
	)

	require.ErrorContains(t, err, `graph "asset"`)
	require.ErrorContains(t, err, "nodes phase")
	require.ErrorContains(t, err, "count mismatch: got 1 want 2")
	require.ErrorContains(t, err, "partial graph must be cleared before retry")
	require.Len(t, database.graphs["asset"].nodes, 1)
}

func TestLoadCancellationOnFinalPreflightArtifactStopsBeforeTargetAccess(t *testing.T) {
	// Break caught: continuing from the synchronous final preflight artifact
	// event into target reads after its observer cancels the operation.
	root := writeLoadCollection(t, []string{"asset"}, map[string]*dumpTestGraph{
		"asset": {nodes: dumpTestNodes(1)},
	}, true, false)
	database := newLoadTestDatabase()
	ctx, cancel := context.WithCancel(context.Background())
	var events []observe.Event

	_, err := Load(ctx, database, LoadConfig{
		Directory: root,
		BatchSize: 1,
		Observer: observe.ObserverFunc(func(_ context.Context, event observe.Event) {
			events = append(events, event)
			if _, ok := event.(observe.ArtifactVerified); ok {
				cancel()
			}
		}),
	})

	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, ErrNonEmptyTarget)
	require.Zero(t, database.readCalls)
	require.Empty(t, database.schemas)
	requireSingleCanceledTerminalEvent(t, events)
}

func TestLoadCancellationAfterAllEmptinessChecksStopsBeforeSchema(t *testing.T) {
	// Break caught: crossing the global empty-target barrier into schema mutation
	// after the final synchronous emptiness read cancels the context.
	root := writeLoadCollection(t, []string{"asset"}, map[string]*dumpTestGraph{
		"asset": {nodes: dumpTestNodes(1)},
	}, true, false)
	database := newLoadTestDatabase()
	ctx, cancel := context.WithCancel(context.Background())
	database.afterRead = cancel
	var events []observe.Event

	_, err := Load(ctx, database, LoadConfig{
		Directory: root,
		BatchSize: 1,
		Observer: observe.ObserverFunc(func(_ context.Context, event observe.Event) {
			events = append(events, event)
		}),
	})

	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, ErrNonEmptyTarget)
	require.Empty(t, database.schemas)
	require.Empty(t, database.durableMutations())
	requireSingleCanceledTerminalEvent(t, events)
}

func TestLoadCancellationOnRelationshipPhaseStartStopsBeforeRelationshipMutation(t *testing.T) {
	// Break caught: relying on the database to notice cancellation instead of
	// gating the mutation immediately after synchronous PhaseStarted delivery.
	root := writeLoadCollection(t, []string{"asset"}, map[string]*dumpTestGraph{
		"asset": {
			nodes:         dumpTestNodes(1, 2),
			relationships: dumpTestRelationships(10, 1, 2, "LINKED"),
		},
	}, true, false)
	database := newLoadTestDatabase()
	database.ignoreContext = true
	ctx, cancel := context.WithCancel(context.Background())
	var events []observe.Event

	_, err := Load(ctx, database, LoadConfig{
		Directory: root,
		BatchSize: 1,
		Observer: observe.ObserverFunc(func(_ context.Context, event observe.Event) {
			events = append(events, event)
			if value, ok := event.(observe.PhaseStarted); ok && value.Phase == loadRelationshipsPhase {
				cancel()
			}
		}),
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, database.graphs["asset"].nodes, 2)
	require.Empty(t, database.graphs["asset"].relationships)
	require.Equal(t, []string{
		"operation_started:load",
		"graph_started:asset",
		"phase_started:asset:nodes:2",
		"phase_progress:asset:nodes:1:2",
		"phase_progress:asset:nodes:2:2",
		"phase_completed:asset:nodes:2",
		"phase_started:asset:relationships:1",
		"operation_completed:load:error",
	}, loadLifecycleEventNames(events))
	requireSingleCanceledTerminalEvent(t, events)
}

func TestLoadCancellationOnFinalPhaseProgressSuppressesSuccessEvents(t *testing.T) {
	// Break caught: returning success and emitting phase/graph completion after
	// the observer cancels on the final committed progress event.
	root := writeLoadCollection(t, []string{"asset"}, map[string]*dumpTestGraph{
		"asset": {
			nodes:         dumpTestNodes(1, 2),
			relationships: dumpTestRelationships(10, 1, 2, "LINKED"),
		},
	}, true, false)
	database := newLoadTestDatabase()
	ctx, cancel := context.WithCancel(context.Background())
	var events []observe.Event

	_, err := Load(ctx, database, LoadConfig{
		Directory: root,
		BatchSize: 2,
		Observer: observe.ObserverFunc(func(_ context.Context, event observe.Event) {
			events = append(events, event)
			if value, ok := event.(observe.PhaseProgress); ok &&
				value.Phase == loadRelationshipsPhase &&
				value.Completed == value.Total {
				cancel()
			}
		}),
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, database.graphs["asset"].relationships, 1)
	names := loadLifecycleEventNames(events)
	require.Equal(t, "phase_progress:asset:relationships:1:1", names[len(names)-2])
	require.Equal(t, "operation_completed:load:error", names[len(names)-1])
	requireSingleCanceledTerminalEvent(t, events)
}

func writeLoadCollection(
	t *testing.T,
	graphOrder []string,
	graphs map[string]*dumpTestGraph,
	jsonlEnabled bool,
	parquetEnabled bool,
) string {
	t.Helper()
	config := validRootDumpConfig(t)
	config.Graphs = append([]string(nil), graphOrder...)
	config.EntityBatchSize = 10
	config.ShardSize = 10
	config.JSONL = jsonl.Config{Enabled: jsonlEnabled, Codec: jsonl.CodecNone}
	config.Parquet = parquet.Config{Enabled: parquetEnabled}
	config.Scrub = scrub.Config{}
	_, err := Dump(context.Background(), newDumpTestDatabase(graphs), config)
	require.NoError(t, err)
	return config.Directory
}

func corruptLoadParquetArtifacts(t *testing.T, root string) {
	t.Helper()
	manifest, err := collection.Read(root)
	require.NoError(t, err)
	for _, graphEntry := range manifest.Graphs {
		for _, shard := range graphEntry.NodeShards {
			if shard.Parquet != nil {
				require.NoError(t, os.WriteFile(filepath.Join(root, filepath.FromSlash(shard.Parquet.Path)), []byte("corrupt"), 0o600))
			}
		}
		for _, shard := range graphEntry.RelationshipShards {
			if shard.Parquet != nil {
				require.NoError(t, os.WriteFile(filepath.Join(root, filepath.FromSlash(shard.Parquet.Path)), []byte("corrupt"), 0o600))
			}
		}
	}
}

func loadEventNames(events []observe.Event) []string {
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
			names[index] = fmt.Sprintf("phase_started:%s:%s:%d", value.Graph, value.Phase, value.Total)
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

func loadLifecycleEventNames(events []observe.Event) []string {
	var lifecycle []observe.Event
	for _, event := range events {
		switch event.(type) {
		case observe.OperationStarted, observe.OperationCompleted, observe.GraphStarted, observe.GraphCompleted,
			observe.PhaseStarted, observe.PhaseProgress, observe.PhaseCompleted:
			lifecycle = append(lifecycle, event)
		}
	}
	return loadEventNames(lifecycle)
}

func requireSingleCanceledTerminalEvent(t *testing.T, events []observe.Event) {
	t.Helper()
	var completions []observe.OperationCompleted
	for _, event := range events {
		if value, ok := event.(observe.OperationCompleted); ok {
			completions = append(completions, value)
		}
	}
	require.Len(t, completions, 1)
	require.ErrorIs(t, completions[0].Err, context.Canceled)
	require.IsType(t, observe.OperationCompleted{}, events[len(events)-1])
}

type loadTestDatabase struct {
	graph.Database

	graphs              map[string]*loadTestGraphState
	schemas             []graph.Schema
	operations          []string
	requestedBatchSizes []int
	committedBatchSizes []int
	nextID              graph.ID
	batchCalls          int
	failBatchAt         int
	failBatchErr        error
	failSchemaGraph     string
	failSchemaErr       error
	readErr             error
	readCalls           int
	afterRead           func()
	ignoreContext       bool
}

type loadTestGraphState struct {
	nodes         []*graph.Node
	relationships []*graph.Relationship
}

func newLoadTestDatabase() *loadTestDatabase {
	return &loadTestDatabase{
		graphs: make(map[string]*loadTestGraphState),
		nextID: 100,
	}
}

func (s *loadTestDatabase) graphState(name string) *loadTestGraphState {
	state := s.graphs[name]
	if state == nil {
		state = &loadTestGraphState{}
		s.graphs[name] = state
	}
	return state
}

func (s *loadTestDatabase) durableMutations() []string {
	var mutations []string
	for name, state := range s.graphs {
		if len(state.nodes) > 0 || len(state.relationships) > 0 {
			mutations = append(mutations, name)
		}
	}
	return mutations
}

func (s *loadTestDatabase) ReadTransaction(ctx context.Context, delegate graph.TransactionDelegate, _ ...graph.TransactionOption) error {
	s.readCalls++
	if s.readErr != nil {
		return s.readErr
	}
	if err := ctx.Err(); err != nil && !s.ignoreContext {
		return err
	}
	err := delegate(&loadTestTransaction{database: s})
	if s.afterRead != nil {
		s.afterRead()
	}
	return err
}

func (s *loadTestDatabase) AssertSchema(_ context.Context, schema graph.Schema) error {
	name := schema.DefaultGraph.Name
	s.operations = append(s.operations, "schema:"+name)
	if name == s.failSchemaGraph {
		return s.failSchemaErr
	}
	s.schemas = append(s.schemas, schema)
	return nil
}

func (s *loadTestDatabase) BatchOperation(ctx context.Context, delegate graph.BatchDelegate, options ...graph.BatchOption) error {
	if err := ctx.Err(); err != nil && !s.ignoreContext {
		return err
	}
	config := graph.BatchConfig{}
	for _, option := range options {
		option(&config)
	}
	s.requestedBatchSizes = append(s.requestedBatchSizes, config.BatchSize)
	s.batchCalls++
	batch := &loadTestBatch{database: s}
	if err := delegate(batch); err != nil {
		return err
	}
	if s.failBatchAt == s.batchCalls {
		return s.failBatchErr
	}

	state := s.graphState(batch.graphName)
	state.nodes = append(state.nodes, batch.nodes...)
	state.relationships = append(state.relationships, batch.relationships...)
	size := len(batch.nodes) + len(batch.relationships)
	s.committedBatchSizes = append(s.committedBatchSizes, size)
	entityType := "nodes"
	if len(batch.relationships) > 0 {
		entityType = "relationships"
	}
	s.operations = append(s.operations, fmt.Sprintf("%s:%s:%d", entityType, batch.graphName, size))
	return nil
}

type loadTestTransaction struct {
	graph.Transaction
	database  *loadTestDatabase
	graphName string
}

func (s *loadTestTransaction) WithGraph(target graph.Graph) graph.Transaction {
	s.graphName = target.Name
	s.database.operations = append(s.database.operations, "empty:"+target.Name)
	return s
}

func (s *loadTestTransaction) Nodes() graph.NodeQuery {
	return loadTestNodeQuery{count: int64(len(s.database.graphState(s.graphName).nodes))}
}

func (s *loadTestTransaction) Relationships() graph.RelationshipQuery {
	return loadTestRelationshipQuery{count: int64(len(s.database.graphState(s.graphName).relationships))}
}

type loadTestNodeQuery struct {
	graph.NodeQuery
	count int64
}

func (s loadTestNodeQuery) Count() (int64, error) { return s.count, nil }

type loadTestRelationshipQuery struct {
	graph.RelationshipQuery
	count int64
}

func (s loadTestRelationshipQuery) Count() (int64, error) { return s.count, nil }

type loadTestBatch struct {
	graph.Batch
	database      *loadTestDatabase
	graphName     string
	nodes         []*graph.Node
	relationships []*graph.Relationship
}

func (s *loadTestBatch) WithGraph(target graph.Graph) graph.Batch {
	s.graphName = target.Name
	return s
}

func (s *loadTestBatch) CreateNodes(nodes []*graph.Node) ([]graph.ID, error) {
	ids := make([]graph.ID, len(nodes))
	for index, node := range nodes {
		ids[index] = s.database.nextID
		s.database.nextID++
		s.nodes = append(s.nodes, graph.NewNode(ids[index], node.Properties, node.Kinds...))
	}
	return ids, nil
}

func (s *loadTestBatch) CreateRelationshipByIDs(startID, endID graph.ID, kind graph.Kind, properties *graph.Properties) error {
	s.relationships = append(s.relationships, graph.NewRelationship(0, startID, endID, properties, kind))
	return nil
}
