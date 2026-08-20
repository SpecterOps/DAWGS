package pg

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
	"os"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/drivers/pg/model"
	"github.com/specterops/dawgs/graph"
)

const ingestRollbackCleanupTimeout = 30 * time.Second

// IngestNode is a node mutation supplied to Driver.Ingest.
type IngestNode struct {
	ObjectID   string
	Kinds      graph.Kinds
	Properties *graph.Properties
}

// IngestEdge is an edge mutation supplied to Driver.Ingest.
type IngestEdge struct {
	StartObjectID string
	EndObjectID   string
	Kind          graph.Kind
	Properties    *graph.Properties
}

// IngestInput contains the node and edge streams for one ingest. Nil streams are empty.
type IngestInput struct {
	Nodes iter.Seq2[*IngestNode, error]
	Edges iter.Seq2[*IngestEdge, error]
}

// IngestOptions configures PostgreSQL's hash-filtered ingest path.
type IngestOptions struct {
	BucketCount        int
	TempDir            string
	ClusterAfterIngest bool
}

// IngestPhaseStats reports the work completed for one ingest phase.
type IngestPhaseStats struct {
	InputRecords       int64
	CoalescedRecords   int64
	PopulatedBuckets   int64
	IdentityRowsRead   int64
	HashMatches        int64
	StagedInserts      int64
	StagedUpdates      int64
	CommittedMutations int64
	SpoolBytes         int64
	Duration           time.Duration
}

// IngestStats reports completed work for a complete PostgreSQL ingest.
type IngestStats struct {
	Nodes           IngestPhaseStats
	Edges           IngestPhaseStats
	ClusterDuration time.Duration
	TotalDuration   time.Duration
}

type ingestDB interface {
	Begin(context.Context) (pgx.Tx, error)
}

func newIngestRollbackCleanupContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), ingestRollbackCleanupTimeout)
}

type ingestKindAsserter interface {
	AssertKinds(context.Context, graph.Kinds) ([]int16, error)
}

type ingestEngine struct {
	db          ingestDB
	clusterDB   ingestClusterDB
	kindMapper  ingestKindAsserter
	graphTarget model.Graph
	buckets     ingestBucketSet
	nodeSpool   *ingestSpool
	nodeKinds   map[string]struct{}
	nodeKindIDs map[string]int16
	edgeSpool   *ingestSpool
	edgeKinds   map[string]struct{}
	edgeKindIDs map[string]int16
	stats       IngestStats
}

// ingestLifecycleHooks contains the narrow process and resource boundaries used by Driver.Ingest.
// Production drivers construct these hooks from their pool and schema manager. Tests may install a
// per-driver instance so lifecycle ordering and elapsed time can be observed without global state.
type ingestLifecycleHooks struct {
	now                     func() time.Time
	validated               func(IngestOptions)
	resolveTarget           func(context.Context, graph.Graph) (model.Graph, error)
	newEngine               func(model.Graph, ingestBucketSet) *ingestEngine
	createRunDir            func(string) (string, error)
	createSpool             func(string, ingestPhase, uint64) (*ingestSpool, error)
	disposeSpool            func(*ingestSpool) error
	closeSpool              func(*ingestSpool) error
	removeRunDir            func(string) error
	spoolNodes              func(context.Context, *ingestEngine, iter.Seq2[*IngestNode, error]) error
	assertNodeKinds         func(context.Context, *ingestEngine) error
	processNodeBuckets      func(context.Context, *ingestEngine) error
	spoolEdges              func(context.Context, *ingestEngine, iter.Seq2[*IngestEdge, error]) error
	assertEdgeKinds         func(context.Context, *ingestEngine) error
	processEdgeBuckets      func(context.Context, *ingestEngine) error
	clusterTargetPartitions func(context.Context, *ingestEngine) error
}

func validateIngestOptions(options IngestOptions) error {
	if options.BucketCount < 1 {
		return fmt.Errorf("bucket count must be a power of two between 1 and 2^32")
	}

	_, err := newIngestBucketSet(uint64(options.BucketCount))

	return err
}

func (s *ingestLifecycleHooks) validate() error {
	if s == nil {
		return fmt.Errorf("PostgreSQL ingest driver dependencies are not configured")
	}

	required := []struct {
		name       string
		configured bool
	}{
		{name: "clock", configured: s.now != nil},
		{name: "target resolver", configured: s.resolveTarget != nil},
		{name: "engine constructor", configured: s.newEngine != nil},
		{name: "run directory creator", configured: s.createRunDir != nil},
		{name: "spool creator", configured: s.createSpool != nil},
		{name: "spool disposer", configured: s.disposeSpool != nil},
		{name: "spool closer", configured: s.closeSpool != nil},
		{name: "remove run directory", configured: s.removeRunDir != nil},
		{name: "node spooler", configured: s.spoolNodes != nil},
		{name: "node kind asserter", configured: s.assertNodeKinds != nil},
		{name: "node bucket processor", configured: s.processNodeBuckets != nil},
		{name: "edge spooler", configured: s.spoolEdges != nil},
		{name: "edge kind asserter", configured: s.assertEdgeKinds != nil},
		{name: "edge bucket processor", configured: s.processEdgeBuckets != nil},
		{name: "partition clusterer", configured: s.clusterTargetPartitions != nil},
	}
	for _, dependency := range required {
		if !dependency.configured {
			return fmt.Errorf("PostgreSQL ingest driver dependency %s is not configured", dependency.name)
		}
	}

	return nil
}

func (s *Driver) ingestLifecycle() (*ingestLifecycleHooks, error) {
	if s == nil {
		return nil, fmt.Errorf("PostgreSQL ingest driver is nil")
	}
	if s.ingestHooks != nil {
		if err := s.ingestHooks.validate(); err != nil {
			return nil, err
		}

		return s.ingestHooks, nil
	}
	if s.pool == nil {
		return nil, fmt.Errorf("PostgreSQL ingest driver pool is not configured")
	}
	if s.SchemaManager == nil {
		return nil, fmt.Errorf("PostgreSQL ingest schema manager is not configured")
	}
	if s.SchemaManager.pool == nil {
		return nil, fmt.Errorf("PostgreSQL ingest schema manager pool is not configured")
	}

	hooks := &ingestLifecycleHooks{
		now:           time.Now,
		resolveTarget: s.resolveIngestTarget,
		newEngine: func(graphTarget model.Graph, buckets ingestBucketSet) *ingestEngine {
			return &ingestEngine{
				db:          s.pool,
				clusterDB:   s.pool,
				kindMapper:  s.SchemaManager,
				graphTarget: graphTarget,
				buckets:     buckets,
			}
		},
		createRunDir: newIngestRunDir,
		createSpool:  newIngestSpool,
		disposeSpool: func(spool *ingestSpool) error {
			return spool.RemoveFiles()
		},
		closeSpool: func(spool *ingestSpool) error {
			return spool.Close()
		},
		removeRunDir: os.RemoveAll,
		spoolNodes: func(ctx context.Context, engine *ingestEngine, nodes iter.Seq2[*IngestNode, error]) error {
			return engine.spoolNodes(ctx, nodes)
		},
		assertNodeKinds: func(ctx context.Context, engine *ingestEngine) error {
			if engine.nodeSpool == nil {
				return fmt.Errorf("node ingest kind assertion requires a configured spool")
			}
			if engine.nodeSpool.PopulatedBucketCount() == 0 {
				return nil
			}

			return engine.assertNodeKinds(ctx)
		},
		processNodeBuckets: func(ctx context.Context, engine *ingestEngine) error {
			return engine.processNodeBuckets(ctx)
		},
		spoolEdges: func(ctx context.Context, engine *ingestEngine, edges iter.Seq2[*IngestEdge, error]) error {
			return engine.spoolEdges(ctx, edges)
		},
		assertEdgeKinds: func(ctx context.Context, engine *ingestEngine) error {
			if engine.edgeSpool == nil {
				return fmt.Errorf("edge ingest kind assertion requires a configured spool")
			}
			if engine.edgeSpool.PopulatedBucketCount() == 0 {
				return nil
			}

			return engine.assertEdgeKinds(ctx)
		},
		processEdgeBuckets: func(ctx context.Context, engine *ingestEngine) error {
			return engine.processEdgeBuckets(ctx)
		},
		clusterTargetPartitions: func(ctx context.Context, engine *ingestEngine) error {
			return engine.clusterTargetPartitions(ctx)
		},
	}
	if err := hooks.validate(); err != nil {
		return nil, err
	}

	return hooks, nil
}

func (s *Driver) resolveIngestTarget(ctx context.Context, target graph.Graph) (model.Graph, error) {
	var graphTarget model.Graph
	err := s.SchemaManager.WriteTransaction(ctx, func(tx graph.Transaction) error {
		targetedTx, typeOK := tx.WithGraph(target).(*transaction)
		if !typeOK {
			return fmt.Errorf("PostgreSQL ingest target resolver received transaction type %T", tx)
		}

		resolved, err := targetedTx.getTargetGraph()
		if err != nil {
			return err
		}
		graphTarget = resolved

		return nil
	})
	if err != nil {
		return model.Graph{}, fmt.Errorf("resolve PostgreSQL ingest target graph: %w", err)
	}

	return graphTarget, nil
}

func validateIngestObjectIDConstraint(constraints iter.Seq[graph.Constraint]) error {
	matching := 0
	valid := 0
	for constraint := range constraints {
		if constraint.Field != "objectid" {
			continue
		}
		matching++
		if constraint.Type == graph.BTreeIndex {
			valid++
		}
	}
	if matching != 1 || valid != 1 {
		return fmt.Errorf(`PostgreSQL ingest target requires exactly one unique B-tree node constraint with field "objectid"`)
	}

	return nil
}

// Ingest applies the supplied node mutations before consuming any edge mutations. Each populated
// bucket commits independently, so returned statistics retain work committed before a later error.
func (s *Driver) Ingest(
	ctx context.Context,
	target graph.Graph,
	input IngestInput,
	options IngestOptions,
) (stats IngestStats, resultErr error) {
	if err := validateIngestOptions(options); err != nil {
		return IngestStats{}, err
	}
	if err := validateIngestObjectIDConstraint(slices.Values(target.NodeConstraints)); err != nil {
		return IngestStats{}, err
	}
	hooks, err := s.ingestLifecycle()
	if err != nil {
		return IngestStats{}, err
	}
	if hooks.validated != nil {
		hooks.validated(options)
	}

	started := hooks.now()
	var (
		engine *ingestEngine
		runDir string
	)
	defer func() {
		var cleanupErr error
		if engine != nil && engine.nodeSpool != nil {
			cleanupErr = errors.Join(cleanupErr, wrapIngestCleanupError(
				hooks.closeSpool(engine.nodeSpool),
				"close node ingest spool",
			))
		}
		if engine != nil && engine.edgeSpool != nil {
			cleanupErr = errors.Join(cleanupErr, wrapIngestCleanupError(
				hooks.closeSpool(engine.edgeSpool),
				"close edge ingest spool",
			))
		}
		if runDir != "" {
			cleanupErr = errors.Join(cleanupErr, wrapIngestCleanupError(
				hooks.removeRunDir(runDir),
				"remove ingest run directory",
			))
		}
		if engine != nil {
			stats = engine.stats
		}
		stats.TotalDuration = hooks.now().Sub(started)
		resultErr = errors.Join(resultErr, cleanupErr)
	}()

	graphTarget, err := hooks.resolveTarget(ctx, target)
	if err != nil {
		return stats, err
	}
	if err := validateIngestObjectIDConstraint(maps.Values(graphTarget.Partitions.Node.Constraints)); err != nil {
		return stats, err
	}
	buckets, err := newIngestBucketSet(uint64(options.BucketCount))
	if err != nil {
		return stats, err
	}
	engine = hooks.newEngine(graphTarget, buckets)
	if engine == nil {
		return stats, fmt.Errorf("PostgreSQL ingest engine constructor returned nil")
	}

	runDir, err = hooks.createRunDir(options.TempDir)
	if err != nil {
		return stats, err
	}
	if runDir == "" {
		return stats, fmt.Errorf("create ingest run directory returned an empty path")
	}

	if err := runNodeIngestPhase(ctx, hooks, engine, runDir, uint64(options.BucketCount), input.Nodes); err != nil {
		return stats, err
	}
	if err := runEdgeIngestPhase(ctx, hooks, engine, runDir, uint64(options.BucketCount), input.Edges); err != nil {
		return stats, err
	}
	if options.ClusterAfterIngest {
		clusterStarted := hooks.now()
		clusterErr := hooks.clusterTargetPartitions(ctx, engine)
		engine.stats.ClusterDuration = hooks.now().Sub(clusterStarted)
		if clusterErr != nil {
			return stats, clusterErr
		}
	}

	return stats, nil
}

func runNodeIngestPhase(
	ctx context.Context,
	hooks *ingestLifecycleHooks,
	engine *ingestEngine,
	runDir string,
	bucketCount uint64,
	nodes iter.Seq2[*IngestNode, error],
) (resultErr error) {
	started := hooks.now()
	defer func() {
		engine.stats.Nodes.Duration = hooks.now().Sub(started)
	}()

	spool, err := hooks.createSpool(runDir, ingestPhaseNodes, bucketCount)
	if err != nil {
		return err
	}
	if spool == nil {
		return fmt.Errorf("create node ingest spool returned nil")
	}
	engine.nodeSpool = spool
	if err := hooks.spoolNodes(ctx, engine, nodes); err != nil {
		return err
	}
	if err := hooks.assertNodeKinds(ctx, engine); err != nil {
		return err
	}
	if err := hooks.processNodeBuckets(ctx, engine); err != nil {
		return err
	}

	disposeErr := hooks.disposeSpool(spool)
	if disposeErr != nil {
		return fmt.Errorf("remove node ingest spool files: %w", disposeErr)
	}
	engine.nodeSpool = nil

	return nil
}

func runEdgeIngestPhase(
	ctx context.Context,
	hooks *ingestLifecycleHooks,
	engine *ingestEngine,
	runDir string,
	bucketCount uint64,
	edges iter.Seq2[*IngestEdge, error],
) (resultErr error) {
	started := hooks.now()
	defer func() {
		engine.stats.Edges.Duration = hooks.now().Sub(started)
	}()

	spool, err := hooks.createSpool(runDir, ingestPhaseEdges, bucketCount)
	if err != nil {
		return err
	}
	if spool == nil {
		return fmt.Errorf("create edge ingest spool returned nil")
	}
	engine.edgeSpool = spool
	if err := hooks.spoolEdges(ctx, engine, edges); err != nil {
		return err
	}
	if err := hooks.assertEdgeKinds(ctx, engine); err != nil {
		return err
	}
	if err := hooks.processEdgeBuckets(ctx, engine); err != nil {
		return err
	}

	disposeErr := hooks.disposeSpool(spool)
	if disposeErr != nil {
		return fmt.Errorf("remove edge ingest spool files: %w", disposeErr)
	}
	engine.edgeSpool = nil

	return nil
}

func wrapIngestCleanupError(err error, operation string) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("%s: %w", operation, err)
}
