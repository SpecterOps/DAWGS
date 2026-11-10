package pg

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/drivers/pg/model"
	"github.com/specterops/dawgs/drivers/pg/query"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
)

// KindMapper translates graph kind names to and from PostgreSQL kind identifiers.
type KindMapper interface {
	// MapKindID resolves one PostgreSQL kind identifier to its graph kind.
	MapKindID(ctx context.Context, kindID int16) (graph.Kind, error)

	// MapKindIDs resolves PostgreSQL kind identifiers to their graph kinds.
	MapKindIDs(ctx context.Context, kindIDs []int16) (graph.Kinds, error)

	// MapKind resolves one graph kind to its PostgreSQL identifier.
	MapKind(ctx context.Context, kind graph.Kind) (int16, error)

	// MapKinds resolves graph kinds to their PostgreSQL identifiers.
	MapKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error)

	// AssertKinds creates missing kinds and returns PostgreSQL identifiers for every input kind.
	AssertKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error)
}

// StableSnapshotTraversalWorkspaceProvider optionally owns the readiness of
// session-local traversal workspaces. Providers must treat a closed or
// replaced physical connection as unready.
type StableSnapshotTraversalWorkspaceProvider interface {
	// EnsureStableSnapshotTraversalWorkspaces initializes workspaces for conn when required.
	EnsureStableSnapshotTraversalWorkspaces(ctx context.Context, conn *pgxpool.Conn) error
}

// LazyStableSnapshotTraversalWorkspaceProvider elects to initialize traversal
// workspaces only when a shortest-path query is actually issued. This is safe
// for V2 because readiness remains tied to the leased physical connection and
// schema generation.
type LazyStableSnapshotTraversalWorkspaceProvider interface {
	// DeferStableSnapshotTraversalWorkspaces reports whether workspaces may initialize on first use.
	DeferStableSnapshotTraversalWorkspaces() bool
}

// KindMapperFromGraphDatabase returns graphDB's PostgreSQL kind mapper when it exposes one.
func KindMapperFromGraphDatabase(graphDB graph.Database) (KindMapper, error) {
	if kindMapperProvider, supported := graphDB.(interface{ KindMapper() KindMapper }); supported {
		return kindMapperProvider.KindMapper(), nil
	}
	return nil, fmt.Errorf("unsupported graph database type: %T", graphDB)
}

// SchemaManager coordinates graph and kind metadata with the query caches that depend on that schema state.
type SchemaManager struct {
	// schemaDDL serializes base-schema upgrades after a connection has been leased.
	schemaDDL sync.Mutex

	// defaultGraph caches the first graph selected as the schema default.
	defaultGraph model.Graph

	// pool supplies PostgreSQL connections for schema operations.
	pool *pgxpool.Pool

	// parseCache retains immutable Cypher ASTs keyed by normalized query text.
	parseCache *cypherParseCache

	// compilationCache retains upstream compiler results and provenance.
	compilationCache *translationCache

	// compilationCacheProvider supplies the upstream compiler cache.
	compilationCacheProvider translationCacheProvider

	// translationCacheProvider selects the connection-local translation cache
	// for each physical PostgreSQL connection. A missing provider deliberately
	// bypasses retention for pools not constructed by this package.
	translationCacheProvider CypherTranslationCacheProvider

	// hasDefaultGraph distinguishes a cached default graph from the zero-value graph model.
	hasDefaultGraph bool

	// graphs indexes asserted database graph models by schema name.
	graphs map[string]model.Graph

	// kindCache owns immutable metadata independently of graph reconciliation.
	kindCache *kindCache

	// lock protects cached graph metadata.
	lock *sync.RWMutex

	// graphQueryMemoryLimit caps memory available to a graph query transaction.
	graphQueryMemoryLimit size.Size

	// traversalPolicyLock protects the versioned default-off production canary policy.
	traversalPolicyLock sync.RWMutex

	// traversalPolicy is copied on reads so callers cannot mutate live selection state.
	traversalPolicy TraversalPolicy
}

// NewSchemaManager creates an empty metadata manager with bounded parse and
// compilation caches and an optional connection-local translation provider.
func NewSchemaManager(pool *pgxpool.Pool, graphQueryMemoryLimit size.Size, providers ...CypherTranslationCacheProvider) *SchemaManager {
	var provider CypherTranslationCacheProvider
	if len(providers) > 0 {
		provider = providers[0]
	}
	return newSchemaManagerWithOptionsAndProvider(pool, graphQueryMemoryLimit, DefaultDriverOptions(), provider)
}

// NewSchemaManagerWithTranslationCache permits an application to disable the
// compilation cache (zero), or to choose a bounded driver-wide entry capacity.
// Negative values disable the cache.
func NewSchemaManagerWithTranslationCache(pool *pgxpool.Pool, graphQueryMemoryLimit size.Size, translationCacheEntries int) *SchemaManager {
	return NewSchemaManagerWithOptions(pool, graphQueryMemoryLimit, DriverOptions{
		TranslationCacheEntries: translationCacheEntries,
	})
}

// NewSchemaManagerWithOptions constructs the shared compilation service with
// a bounded translation cache.
func NewSchemaManagerWithOptions(pool *pgxpool.Pool, graphQueryMemoryLimit size.Size, options DriverOptions) *SchemaManager {
	return newSchemaManagerWithOptionsAndProvider(pool, graphQueryMemoryLimit, options, nil)
}

func newSchemaManagerWithOptionsAndProvider(pool *pgxpool.Pool, graphQueryMemoryLimit size.Size, options DriverOptions, provider CypherTranslationCacheProvider) *SchemaManager {
	normalizedOptions := normalizeDriverOptions(options)
	compilationCache := newTranslationCache(normalizedOptions.TranslationCacheEntries)

	return &SchemaManager{
		pool:                     pool,
		parseCache:               newCypherParseCache(defaultCypherParseCacheEntries),
		translationCacheProvider: provider,
		hasDefaultGraph:          false,
		graphs:                   map[string]model.Graph{},
		kindCache:                newKindCache(),
		lock:                     &sync.RWMutex{},
		graphQueryMemoryLimit:    graphQueryMemoryLimit,
		compilationCache:         compilationCache,
		compilationCacheProvider: sharedTranslationCacheProvider{
			cache: compilationCache,
		},
	}
}

// cypherTranslationCacheForConnection selects a cache for conn. A missing
// provider or a nil cache is an intentional uncached fallback.
func (s *SchemaManager) cypherTranslationCacheForConnection(conn *pgx.Conn) CypherTranslationCache {
	if s == nil || s.translationCacheProvider == nil {
		return nil
	}

	return s.translationCacheProvider.CacheForConnection(conn)
}

// WriteTransaction executes txDelegate in an explicit read-write PostgreSQL transaction.
func (s *SchemaManager) WriteTransaction(ctx context.Context, txDelegate graph.TransactionDelegate, options ...graph.TransactionOption) error {
	if cfg, err := renderConfig(batchWriteSize, readWriteTxOptions, options); err != nil {
		return err
	} else if conn, err := s.pool.Acquire(ctx); err != nil {
		return err
	} else {
		defer conn.Release()

		if tx, err := newTransactionWrapper(ctx, conn, s, cfg, true); err != nil {
			return err
		} else {
			defer tx.Close()

			if err := txDelegate(tx); err != nil {
				return err
			}

			return tx.Commit()
		}
	}
}

// GetKindIDsByKind returns an owned, independently mutable snapshot.
func (s *SchemaManager) GetKindIDsByKind() map[int16]graph.Kind {
	return maps.Clone(s.kindCache.current.Load().ids)
}

// invalidateKindTranslations also invalidates physical-connection translations.
func (s *SchemaManager) invalidateKindTranslations() {
	s.compilationCache.Invalidate()
	if provider, ok := s.translationCacheProvider.(interface{ advanceSchemaGeneration() uint64 }); ok {
		provider.advanceSchemaGeneration()
	}
}

// Fetch explicitly reloads committed metadata on a safe autocommit connection.
// Bootstrap bypasses automatic servicing, so pending requests cause only one fetch.
func (s *SchemaManager) Fetch(ctx context.Context) error {
	// A failed lease is still an outstanding request to resynchronize metadata.
	s.kindCache.invalidate()
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	wrapper := &transaction{ctx: ctx, conn: conn, schemaManager: s, queryExecMode: pgx.QueryExecModeSimpleProtocol}
	wrapper.kindMapper = &transactionKindMapper{tx: wrapper, snapshot: s.kindCache.current.Load()}
	if err := s.kindCache.lockRefresh(ctx); err != nil {
		return err
	}
	defer s.kindCache.unlockRefresh()
	s.kindCache.fence()
	for {
		installed, err := wrapper.reloadKinds(true)
		if err != nil {
			return err
		}
		if installed {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

func (s *SchemaManager) mapKinds(kinds graph.Kinds) ([]int16, graph.Kinds) {
	mapper := &transactionKindMapper{snapshot: s.kindCache.current.Load()}
	return mapper.mapKinds(kinds)
}

// MapKind resolves a name, looking up only missing names on one owned connection.
func (s *SchemaManager) MapKind(ctx context.Context, kind graph.Kind) (int16, error) {
	ids, err := s.MapKinds(ctx, graph.Kinds{kind})
	if err != nil {
		var missing *pgsql.MissingKindsError
		if errors.As(err, &missing) {
			return -1, fmt.Errorf("unable to map kind: %s", kind.String())
		}
		return -1, err
	}
	return ids[0], nil
}

func (s *SchemaManager) MapKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error) {
	if ids, missing := s.mapKinds(kinds); len(missing) == 0 {
		return ids, nil
	}
	var ids []int16
	err := s.ReadTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		ids, err = tx.(*transaction).kindMapper.MapKinds(ctx, kinds)
		return err
	})
	return ids, err
}

// ReadTransaction executes txDelegate on a leased read-only connection or transaction.
func (s *SchemaManager) ReadTransaction(ctx context.Context, txDelegate graph.TransactionDelegate, options ...graph.TransactionOption) error {
	if cfg, err := renderConfig(batchWriteSize, readOnlyTxOptions, options); err != nil {
		return err
	} else if conn, err := s.pool.Acquire(ctx); err != nil {
		return err
	} else {
		defer conn.Release()
		if stableSnapshotIsolation(cfg.Options.IsoLevel) && !cfg.skipStableSnapshotTraversalWorkspaces {
			workspaceProvider, hasWorkspaceProvider := s.translationCacheProvider.(StableSnapshotTraversalWorkspaceProvider)
			lazyProvider, deferWorkspace := s.translationCacheProvider.(LazyStableSnapshotTraversalWorkspaceProvider)
			if deferWorkspace && lazyProvider.DeferStableSnapshotTraversalWorkspaces() {
				// V2 initializes only when transaction.Query observes a shortest-path
				// operation. Ordinary repeatable-read work needs no temporary tables.
			} else if hasWorkspaceProvider {
				err = workspaceProvider.EnsureStableSnapshotTraversalWorkspaces(ctx, conn)
			} else {
				err = EnsureStableSnapshotTraversalWorkspaces(ctx, conn)
			}
			if err != nil {
				return err
			}
		}
		if cfg.initializeTraversalRuntimeAttestation {
			if _, err := conn.Exec(ctx, "select public.ensure_traversal_runtime_attestation_workspace_v1()"); err != nil {
				return fmt.Errorf("initialize traversal runtime attestation workspace: %w", err)
			}
		}
		allocateTransaction := cfg.Options.IsoLevel != ""
		wrapper, err := newTransactionWrapper(ctx, conn, s, cfg, allocateTransaction)
		if err != nil {
			return err
		}
		defer wrapper.Close()
		if err := txDelegate(wrapper); err != nil {
			return err
		}
		if allocateTransaction {
			return wrapper.Commit()
		}
		return nil
	}
}

// stableSnapshotIsolation reports whether isolation prevents concurrent writes from changing reads.
func stableSnapshotIsolation(isolation pgx.TxIsoLevel) bool {
	return isolation == pgx.RepeatableRead || isolation == pgx.Serializable
}

// EnsureStableSnapshotTraversalWorkspaces initializes the reusable
// session-local workspace required before stable-snapshot traversal queries.
// Drivers with connection-local lifecycle state may call this only when their
// tracked physical connection is not already ready.
func EnsureStableSnapshotTraversalWorkspaces(ctx context.Context, conn *pgxpool.Conn) error {
	const initializeSQL = `select
		public.ensure_shortest_dag_workspace(),
		public.ensure_bidirectional_shortest_path_workspace(),
		public.ensure_bidirectional_all_shortest_path_workspace(),
		set_config('dawgs.shortest_dag_workspace_ready', 'v2', false)`
	if _, err := conn.Exec(ctx, initializeSQL); err != nil {
		return fmt.Errorf("initialize stable-snapshot traversal workspaces: %w", err)
	}
	return nil
}

// MapKindID resolves an ID without database I/O, including during result decoding.
func (s *SchemaManager) MapKindID(ctx context.Context, id int16) (graph.Kind, error) {
	kinds, err := s.MapKindIDs(ctx, []int16{id})
	if err != nil {
		return nil, err
	}
	return kinds[0], nil
}

func (s *SchemaManager) MapKindIDs(ctx context.Context, ids []int16) (graph.Kinds, error) {
	snapshot := s.kindCache.current.Load()
	kinds := make(graph.Kinds, 0, len(ids))
	var missing []int16
	for _, id := range ids {
		kind, ok := snapshot.ids[id]
		if !ok {
			kind, ok = s.kindCache.current.Load().ids[id]
		}
		if ok {
			kinds = append(kinds, kind)
		} else {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		s.kindCache.invalidate()
		return nil, fmt.Errorf("unable to map kind ids: %v", missing)
	}
	return kinds, nil
}

// AssertKinds registers kinds on one leased connection and publishes only committed metadata.
func (s *SchemaManager) AssertKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error) {
	ids, missing := s.mapKinds(kinds)
	if len(missing) == 0 {
		return ids, nil
	}
	err := s.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		ids, err = tx.(*transaction).kindMapper.AssertKinds(ctx, kinds)
		return err
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

// SelectGraph resolves an existing graph without creating or reconciling schema.
func (s *SchemaManager) SelectGraph(tx graph.Transaction, name string) (model.Graph, error) {
	return query.On(tx).SelectGraphByName(name)
}

// setDefaultGraph caches the first successfully resolved default graph and ignores later attempts to replace it.
func (s *SchemaManager) setDefaultGraph(defaultGraph model.Graph, schema graph.Graph) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.hasDefaultGraph {
		// Another actor has already asserted or otherwise set a default graph
		return
	}

	s.graphs[schema.Name] = defaultGraph

	s.defaultGraph = defaultGraph
	s.hasDefaultGraph = true
}

// SetDefaultGraph validates an existing graph and records it as the immutable default target.
func (s *SchemaManager) SetDefaultGraph(ctx context.Context, schema graph.Graph) error {
	return s.ReadTransaction(ctx, func(tx graph.Transaction) error {
		// Validate the schema if the graph already exists in the database
		if graphModel, err := query.On(tx).SelectGraphByName(schema.Name); err != nil {
			return err
		} else {
			s.setDefaultGraph(graphModel, schema)
			return nil
		}
	})
}

// AssertDefaultGraph creates or validates schema and records it as the immutable default target.
func (s *SchemaManager) AssertDefaultGraph(ctx context.Context, schema graph.Graph) error {
	return s.WriteTransaction(ctx, func(tx graph.Transaction) error {
		if graphModel, err := s.AssertGraph(tx, schema); err != nil {
			return err
		} else {
			s.setDefaultGraph(graphModel, schema)
		}

		return nil
	})
}

// DefaultGraph returns the cached default graph and whether one has been selected.
func (s *SchemaManager) DefaultGraph() (model.Graph, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.defaultGraph, s.hasDefaultGraph
}

// assertGraph creates or validates schema while the caller holds the metadata write lock.
func (s *SchemaManager) assertGraph(tx graph.Transaction, schema graph.Graph) (model.Graph, error) {
	var assertedGraph model.Graph

	// Validate the schema if the graph already exists in the database
	if definition, err := query.On(tx).SelectGraphByName(schema.Name); err != nil {
		// ErrNoRows is ignored as it signifies that this graph must be created
		if !errors.Is(err, pgx.ErrNoRows) {
			return model.Graph{}, err
		}

		if newDefinition, err := query.On(tx).CreateGraph(schema); err != nil {
			return model.Graph{}, err
		} else {
			assertedGraph = newDefinition
		}
	} else if assertedDefinition, err := query.On(tx).AssertGraph(schema, definition); err != nil {
		return model.Graph{}, err
	} else {
		// Graph existed and may have been updated
		assertedGraph = assertedDefinition
	}

	// Cache the graph definition and return it
	s.graphs[schema.Name] = assertedGraph
	return assertedGraph, nil
}

// AssertGraph explicitly reconciles schema, even when the graph was previously cached.
func (s *SchemaManager) AssertGraph(tx graph.Transaction, schema graph.Graph) (model.Graph, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.assertGraph(tx, schema)
}

// assertSchema creates schema storage and defines every node and relationship kind required by its graphs.
func (s *SchemaManager) assertSchema(tx graph.Transaction, schema graph.Schema) error {
	s.schemaDDL.Lock()
	defer s.schemaDDL.Unlock()
	if err := query.On(tx).CreateSchema(); err != nil {
		return err
	}

	// Schema assertion may follow an external reset. Validate its private catalog
	// view after creating storage rather than trusting stale positive entries.
	owner := tx.(*transaction)
	epoch := s.kindCache.current.Load().epoch
	started := time.Now()
	kinds, err := query.On(metadataTransaction{owner}).SelectKinds()
	owner.recordKindWork("kind_refresh", started)
	if err != nil {
		return err
	}
	ids := make(map[int16]graph.Kind, len(kinds))
	for kind, id := range kinds {
		ids[id] = kind
	}
	owner.kindMapper.snapshot = &kindSnapshot{kinds: kinds, ids: ids, epoch: epoch, loaded: true}
	owner.kindMapper.privateCatalog = true

	for _, graphSchema := range schema.Graphs {
		if _, err := tx.(*transaction).kindMapper.AssertKinds(tx.(*transaction).ctx, append(append(graph.Kinds{}, graphSchema.Nodes...), graphSchema.Edges...)); err != nil {
			return err
		}
	}

	return nil
}

// AssertSchema creates base storage and defines kinds required by every graph in schema.
func (s *SchemaManager) AssertSchema(ctx context.Context, schema graph.Schema) error {
	if err := s.WriteTransaction(ctx, func(tx graph.Transaction) error {
		return s.assertSchema(tx, schema)
	}, OptionSetQueryExecMode(pgx.QueryExecModeSimpleProtocol), func(config *graph.TransactionConfig) {
		config.DriverConfig.(*Config).skipKindRefresh = true
	}); err != nil {
		return err
	}

	return s.Fetch(ctx)
}
