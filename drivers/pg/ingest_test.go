package pg

import (
	"context"
	"errors"
	"iter"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg/model"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestDriverIngestRunsLifecycleInOrderAndMeasuresDurations(t *testing.T) {
	var events []string
	driver, hooks := newDriverIngestLifecycleTestDriver(&events, testDriverIngestTarget())
	hooks.now = ingestClock(
		time.Unix(0, 0),
		time.Unix(10, 0),
		time.Unix(30, 0),
		time.Unix(40, 0),
		time.Unix(70, 0),
		time.Unix(80, 0),
		time.Unix(100, 0),
		time.Unix(120, 0),
	)
	hooks.processNodeBuckets = func(_ context.Context, engine *ingestEngine) error {
		events = append(events, "process nodes")
		engine.stats.Nodes.CommittedMutations = 2
		return nil
	}
	hooks.processEdgeBuckets = func(_ context.Context, engine *ingestEngine) error {
		events = append(events, "process edges")
		engine.stats.Edges.CommittedMutations = 3
		return nil
	}

	stats, err := driver.Ingest(context.Background(), testDriverIngestRequestedGraph("target"), IngestInput{}, IngestOptions{
		BucketCount:        4,
		TempDir:            "/caller/temp",
		ClusterAfterIngest: true,
	})

	require.NoError(t, err)
	require.Equal(t, []string{
		"validate",
		"resolve target",
		"create run /caller/temp",
		"create nodes spool",
		"spool nodes",
		"assert node kinds",
		"process nodes",
		"remove nodes spool",
		"create edges spool",
		"spool edges",
		"assert edge kinds",
		"process edges",
		"remove edges spool",
		"cluster",
		"remove run /caller/temp/private-run",
	}, events)
	require.Equal(t, int64(2), stats.Nodes.CommittedMutations)
	require.Equal(t, int64(3), stats.Edges.CommittedMutations)
	require.Equal(t, 20*time.Second, stats.Nodes.Duration)
	require.Equal(t, 30*time.Second, stats.Edges.Duration)
	require.Equal(t, 20*time.Second, stats.ClusterDuration)
	require.Equal(t, 120*time.Second, stats.TotalDuration)
}

func TestDriverIngestValidatesOptionsAndDependenciesBeforeSideEffects(t *testing.T) {
	for name, bucketCount := range map[string]int{
		"zero":             0,
		"not power of two": 3,
	} {
		t.Run(name, func(t *testing.T) {
			var events []string
			driver, _ := newDriverIngestLifecycleTestDriver(&events, testDriverIngestTarget())

			_, err := driver.Ingest(context.Background(), testDriverIngestRequestedGraph("target"), IngestInput{}, IngestOptions{
				BucketCount: bucketCount,
			})

			require.Error(t, err)
			require.Empty(t, events)
		})
	}

	t.Run("missing dependency", func(t *testing.T) {
		var events []string
		driver, hooks := newDriverIngestLifecycleTestDriver(&events, testDriverIngestTarget())
		hooks.removeRunDir = nil

		_, err := driver.Ingest(context.Background(), testDriverIngestRequestedGraph("target"), IngestInput{}, IngestOptions{
			BucketCount: 1,
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "remove run directory")
		require.Empty(t, events)
	})
}

func TestDriverIngestRejectsNilDriverWithoutSideEffects(t *testing.T) {
	var driver *Driver

	_, err := driver.Ingest(
		context.Background(),
		testDriverIngestRequestedGraph("target"),
		IngestInput{},
		IngestOptions{BucketCount: 1},
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "driver is nil")
}

func TestDriverIngestLifecycleValidatesProductionDependenciesWithoutIO(t *testing.T) {
	pool := &pgxpool.Pool{}
	tests := map[string]struct {
		driver *Driver
		want   string
	}{
		"nil pool": {
			driver: &Driver{SchemaManager: NewSchemaManager(pool, 0)},
			want:   "driver pool",
		},
		"nil schema manager": {
			driver: &Driver{pool: pool},
			want:   "schema manager is not configured",
		},
		"schema manager nil pool": {
			driver: &Driver{pool: pool, SchemaManager: NewSchemaManager(nil, 0)},
			want:   "schema manager pool",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			hooks, err := test.driver.ingestLifecycle()

			require.Error(t, err)
			require.Contains(t, err.Error(), test.want)
			require.Nil(t, hooks)
		})
	}
}

func TestDriverIngestLifecycleBuildsProductionHooksWithoutConnecting(t *testing.T) {
	pool := &pgxpool.Pool{}
	manager := NewSchemaManager(pool, 0)
	driver := &Driver{pool: pool, SchemaManager: manager}

	hooks, err := driver.ingestLifecycle()
	require.NoError(t, err)
	buckets, err := newIngestBucketSet(1)
	require.NoError(t, err)
	engine := hooks.newEngine(testDriverIngestTarget(), buckets)

	require.Same(t, pool, engine.db.(*pgxpool.Pool))
	require.Same(t, pool, engine.clusterDB.(*pgxpool.Pool))
	require.Same(t, manager, engine.kindMapper.(*SchemaManager))
}

func TestDriverIngestResolvesExactTargetAndRequiresObjectIDConstraint(t *testing.T) {
	requested := testDriverIngestRequestedGraph("requested")

	t.Run("passes exact target", func(t *testing.T) {
		var events []string
		driver, hooks := newDriverIngestLifecycleTestDriver(&events, testDriverIngestTarget())
		var actual graph.Graph
		resolve := hooks.resolveTarget
		hooks.resolveTarget = func(ctx context.Context, target graph.Graph) (model.Graph, error) {
			actual = target
			return resolve(ctx, target)
		}

		_, err := driver.Ingest(context.Background(), requested, IngestInput{}, IngestOptions{BucketCount: 1})

		require.NoError(t, err)
		require.Equal(t, requested, actual)
	})

	tests := map[string]model.Graph{
		"missing": {
			Name: "target",
			Partitions: model.GraphPartitions{
				Node: model.NewGraphPartition("node_42"),
				Edge: model.NewGraphPartition("edge_42"),
			},
		},
		"wrong type": func() model.Graph {
			target := testDriverIngestTarget()
			target.Partitions.Node.Constraints = map[string]graph.Constraint{"opaque": {
				Field: "objectid",
				Type:  graph.TextSearchIndex,
			}}
			return target
		}(),
		"ambiguous": func() model.Graph {
			target := testDriverIngestTarget()
			target.Partitions.Node.Constraints["another opaque key"] = graph.Constraint{
				Field: "objectid",
				Type:  graph.BTreeIndex,
			}
			return target
		}(),
	}

	for name, target := range tests {
		t.Run(name, func(t *testing.T) {
			var events []string
			driver, _ := newDriverIngestLifecycleTestDriver(&events, target)

			_, err := driver.Ingest(context.Background(), requested, IngestInput{}, IngestOptions{BucketCount: 1})

			require.Error(t, err)
			require.Contains(t, err.Error(), "objectid")
			require.Contains(t, err.Error(), "B-tree")
			require.NotContains(t, err.Error(), "opaque")
			require.Equal(t, []string{"validate", "resolve target"}, events)
		})
	}
}

func TestDriverIngestRejectsInvalidRequestedObjectIDConstraintBeforeResolver(t *testing.T) {
	tests := map[string]graph.Graph{
		"missing": {
			Name: "target",
			NodeConstraints: []graph.Constraint{{
				Field: "tenantid",
				Type:  graph.BTreeIndex,
			}},
		},
		"wrong type": {
			Name: "target",
			NodeConstraints: []graph.Constraint{{
				Name:  "generated_objectid_constraint",
				Field: "objectid",
				Type:  graph.TextSearchIndex,
			}},
		},
		"duplicate": {
			Name: "target",
			NodeConstraints: []graph.Constraint{
				{Name: "generated_objectid_constraint", Field: "objectid", Type: graph.BTreeIndex},
				{Name: "another_generated_constraint", Field: "objectid", Type: graph.BTreeIndex},
			},
		},
	}

	for name, requested := range tests {
		t.Run(name, func(t *testing.T) {
			var events []string
			driver, _ := newDriverIngestLifecycleTestDriver(&events, testDriverIngestTarget())

			_, err := driver.Ingest(context.Background(), requested, IngestInput{}, IngestOptions{BucketCount: 1})

			require.Error(t, err)
			require.Contains(t, err.Error(), "objectid")
			require.Contains(t, err.Error(), "B-tree")
			require.NotContains(t, err.Error(), "generated_objectid_constraint")
			require.Empty(t, events, "requested schema validation must run before the resolver hook")
		})
	}
}

func TestDriverIngestNodeFailurePreventsEdgeConsumptionAndJoinsCleanupErrors(t *testing.T) {
	primaryErr := errors.New("node phase failed")
	closeErr := errors.New("node close failed")
	removeErr := errors.New("run removal failed")
	var events []string
	driver, hooks := newDriverIngestLifecycleTestDriver(&events, testDriverIngestTarget())
	hooks.processNodeBuckets = func(_ context.Context, engine *ingestEngine) error {
		events = append(events, "process nodes")
		engine.stats.Nodes.CommittedMutations = 7
		return primaryErr
	}
	hooks.closeSpool = func(spool *ingestSpool) error {
		events = append(events, "close "+spool.phase.filename()+" spool")
		return closeErr
	}
	hooks.removeRunDir = func(runDir string) error {
		events = append(events, "remove run "+runDir)
		return removeErr
	}
	edgeRecordsRead := 0

	stats, err := driver.Ingest(context.Background(), testDriverIngestRequestedGraph("target"), IngestInput{
		Edges: observedIngestEdges(&edgeRecordsRead),
	}, IngestOptions{BucketCount: 1})

	require.ErrorIs(t, err, primaryErr)
	require.ErrorIs(t, err, closeErr)
	require.ErrorIs(t, err, removeErr)
	require.Zero(t, edgeRecordsRead)
	require.Equal(t, int64(7), stats.Nodes.CommittedMutations)
	require.Zero(t, stats.Edges.CommittedMutations)
	require.Equal(t, []string{
		"validate",
		"resolve target",
		"create run /caller/temp",
		"create nodes spool",
		"spool nodes",
		"assert node kinds",
		"process nodes",
		"close nodes spool",
		"remove run /caller/temp/private-run",
	}, events)
}

func TestDriverIngestEdgeFailurePreservesCommittedNodeAndEdgeStatsAndSkipsCluster(t *testing.T) {
	primaryErr := errors.New("edge phase failed")
	var events []string
	driver, hooks := newDriverIngestLifecycleTestDriver(&events, testDriverIngestTarget())
	hooks.processNodeBuckets = func(_ context.Context, engine *ingestEngine) error {
		events = append(events, "process nodes")
		engine.stats.Nodes.CommittedMutations = 5
		return nil
	}
	hooks.processEdgeBuckets = func(_ context.Context, engine *ingestEngine) error {
		events = append(events, "process edges")
		engine.stats.Edges.CommittedMutations = 2
		return primaryErr
	}

	stats, err := driver.Ingest(context.Background(), testDriverIngestRequestedGraph("target"), IngestInput{}, IngestOptions{
		BucketCount:        1,
		ClusterAfterIngest: true,
	})

	require.ErrorIs(t, err, primaryErr)
	require.Equal(t, int64(5), stats.Nodes.CommittedMutations)
	require.Equal(t, int64(2), stats.Edges.CommittedMutations)
	require.NotContains(t, events, "cluster")
	require.Contains(t, events, "remove nodes spool")
	require.Contains(t, events, "close edges spool")
	require.Equal(t, "remove run /caller/temp/private-run", events[len(events)-1])
}

func TestDriverIngestNodeSpoolDisposalFailureRetriesCloseAndPreventsEdgeConsumption(t *testing.T) {
	disposeErr := errors.New("node spool removal failed")
	var events []string
	driver, hooks := newDriverIngestLifecycleTestDriver(&events, testDriverIngestTarget())
	hooks.processNodeBuckets = func(_ context.Context, engine *ingestEngine) error {
		events = append(events, "process nodes")
		engine.stats.Nodes.CommittedMutations = 9
		return nil
	}
	hooks.disposeSpool = func(spool *ingestSpool) error {
		events = append(events, "remove "+spool.phase.filename()+" spool")
		if spool.phase == ingestPhaseNodes {
			return disposeErr
		}
		return nil
	}
	edgeRecordsRead := 0

	stats, err := driver.Ingest(context.Background(), testDriverIngestRequestedGraph("target"), IngestInput{
		Edges: observedIngestEdges(&edgeRecordsRead),
	}, IngestOptions{BucketCount: 1})

	require.ErrorIs(t, err, disposeErr)
	require.Zero(t, edgeRecordsRead)
	require.Equal(t, int64(9), stats.Nodes.CommittedMutations)
	require.Equal(t, []string{
		"remove nodes spool",
		"close nodes spool",
		"remove run /caller/temp/private-run",
	}, events[len(events)-3:])
}

func TestDriverIngestNilStreamsRunAsEmptyPhasesAndClusteringDefaultsOff(t *testing.T) {
	var events []string
	driver, _ := newDriverIngestLifecycleTestDriver(&events, testDriverIngestTarget())

	stats, err := driver.Ingest(context.Background(), testDriverIngestRequestedGraph("target"), IngestInput{}, IngestOptions{
		BucketCount: 1,
	})

	require.NoError(t, err)
	require.Zero(t, stats.Nodes.InputRecords)
	require.Zero(t, stats.Edges.InputRecords)
	require.Contains(t, events, "spool nodes")
	require.Contains(t, events, "spool edges")
	require.NotContains(t, events, "cluster")
}

func TestDriverIngestClusterFailureKeepsCommittedStatsAndCleansRun(t *testing.T) {
	clusterErr := errors.New("cluster failed")
	var events []string
	driver, hooks := newDriverIngestLifecycleTestDriver(&events, testDriverIngestTarget())
	hooks.processNodeBuckets = func(_ context.Context, engine *ingestEngine) error {
		events = append(events, "process nodes")
		engine.stats.Nodes.CommittedMutations = 4
		return nil
	}
	hooks.processEdgeBuckets = func(_ context.Context, engine *ingestEngine) error {
		events = append(events, "process edges")
		engine.stats.Edges.CommittedMutations = 6
		return nil
	}
	hooks.clusterTargetPartitions = func(_ context.Context, _ *ingestEngine) error {
		events = append(events, "cluster")
		return clusterErr
	}

	stats, err := driver.Ingest(context.Background(), testDriverIngestRequestedGraph("target"), IngestInput{}, IngestOptions{
		BucketCount:        1,
		ClusterAfterIngest: true,
	})

	require.ErrorIs(t, err, clusterErr)
	require.Equal(t, int64(4), stats.Nodes.CommittedMutations)
	require.Equal(t, int64(6), stats.Edges.CommittedMutations)
	require.Equal(t, []string{"cluster", "remove run /caller/temp/private-run"}, events[len(events)-2:])
}

func newDriverIngestLifecycleTestDriver(
	events *[]string,
	target model.Graph,
) (*Driver, *ingestLifecycleHooks) {
	constantNow := time.Unix(0, 0)
	hooks := &ingestLifecycleHooks{}
	hooks.now = func() time.Time { return constantNow }
	hooks.validated = func(IngestOptions) { *events = append(*events, "validate") }
	hooks.resolveTarget = func(_ context.Context, _ graph.Graph) (model.Graph, error) {
		*events = append(*events, "resolve target")
		return target, nil
	}
	hooks.newEngine = func(graphTarget model.Graph, buckets ingestBucketSet) *ingestEngine {
		return &ingestEngine{graphTarget: graphTarget, buckets: buckets}
	}
	hooks.createRunDir = func(parent string) (string, error) {
		if parent == "" {
			parent = "/caller/temp"
		}
		*events = append(*events, "create run "+parent)
		return parent + "/private-run", nil
	}
	hooks.createSpool = func(_ string, phase ingestPhase, _ uint64) (*ingestSpool, error) {
		*events = append(*events, "create "+phase.filename()+" spool")
		return &ingestSpool{phase: phase}, nil
	}
	hooks.disposeSpool = func(spool *ingestSpool) error {
		*events = append(*events, "remove "+spool.phase.filename()+" spool")
		return nil
	}
	hooks.closeSpool = func(spool *ingestSpool) error {
		*events = append(*events, "close "+spool.phase.filename()+" spool")
		return nil
	}
	hooks.removeRunDir = func(runDir string) error {
		*events = append(*events, "remove run "+runDir)
		return nil
	}
	hooks.spoolNodes = func(_ context.Context, _ *ingestEngine, nodes iter.Seq2[*IngestNode, error]) error {
		*events = append(*events, "spool nodes")
		if nodes != nil {
			for range nodes {
			}
		}
		return nil
	}
	hooks.assertNodeKinds = func(_ context.Context, _ *ingestEngine) error {
		*events = append(*events, "assert node kinds")
		return nil
	}
	hooks.processNodeBuckets = func(_ context.Context, _ *ingestEngine) error {
		*events = append(*events, "process nodes")
		return nil
	}
	hooks.spoolEdges = func(_ context.Context, _ *ingestEngine, edges iter.Seq2[*IngestEdge, error]) error {
		*events = append(*events, "spool edges")
		if edges != nil {
			for range edges {
			}
		}
		return nil
	}
	hooks.assertEdgeKinds = func(_ context.Context, _ *ingestEngine) error {
		*events = append(*events, "assert edge kinds")
		return nil
	}
	hooks.processEdgeBuckets = func(_ context.Context, _ *ingestEngine) error {
		*events = append(*events, "process edges")
		return nil
	}
	hooks.clusterTargetPartitions = func(_ context.Context, _ *ingestEngine) error {
		*events = append(*events, "cluster")
		return nil
	}

	return &Driver{ingestHooks: hooks}, hooks
}

func testDriverIngestTarget() model.Graph {
	target := model.Graph{
		ID:   42,
		Name: "target",
		Partitions: model.GraphPartitions{
			Node: model.NewGraphPartition("node_42"),
			Edge: model.NewGraphPartition("edge_42"),
		},
	}
	target.Partitions.Node.Constraints["opaque constraint key"] = graph.Constraint{
		Field: "objectid",
		Type:  graph.BTreeIndex,
	}
	target.Partitions.Node.Constraints["unrelated constraint key"] = graph.Constraint{
		Field: "tenantid",
		Type:  graph.BTreeIndex,
	}
	return target
}

func testDriverIngestRequestedGraph(name string) graph.Graph {
	return graph.Graph{
		Name: name,
		NodeConstraints: []graph.Constraint{
			{Field: "objectid", Type: graph.BTreeIndex},
			{Field: "tenantid", Type: graph.BTreeIndex},
		},
	}
}

func ingestClock(values ...time.Time) func() time.Time {
	index := 0
	return func() time.Time {
		value := values[index]
		index++
		return value
	}
}

func observedIngestEdges(recordsRead *int) iter.Seq2[*IngestEdge, error] {
	return func(yield func(*IngestEdge, error) bool) {
		*recordsRead++
		yield(&IngestEdge{
			StartObjectID: "start",
			EndObjectID:   "end",
			Kind:          graph.StringKind("K"),
		}, nil)
	}
}
