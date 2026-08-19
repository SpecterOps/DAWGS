package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg/model"
	"github.com/specterops/dawgs/drivers/pg/query"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
)

// KindMapper groups state that must remain consistent while processing kind mapper.
type KindMapper interface {
	// MapKindID identifies the map kind id.
	MapKindID(ctx context.Context, kindID int16) (graph.Kind, error)
	// MapKindIDs supplies the map kind i ds input to the KindMapper contract.
	MapKindIDs(ctx context.Context, kindIDs []int16) (graph.Kinds, error)
	// MapKind supplies the map kind input to the KindMapper contract.
	MapKind(ctx context.Context, kind graph.Kind) (int16, error)
	// MapKinds supplies the map kinds input to the KindMapper contract.
	MapKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error)
	// AssertKinds supplies the assert kinds input to the KindMapper contract.
	AssertKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error)
}

// KindMapperFromGraphDatabase coordinates PostgreSQL driver behavior for kind mapper from graph database.
func KindMapperFromGraphDatabase(graphDB graph.Database) (KindMapper, error) {
	if kindMapperProvider, supported := graphDB.(interface{ KindMapper() KindMapper }); supported {
		return kindMapperProvider.KindMapper(), nil
	}
	return nil, fmt.Errorf("unsupported graph database type: %T", graphDB)
}

// SchemaManager coordinates graph and kind metadata with the query caches that depend on that schema state.
type SchemaManager struct {
	// defaultGraph caches the first graph selected as the schema default.
	defaultGraph model.Graph

	// pool supplies PostgreSQL connections for schema operations.
	pool *pgxpool.Pool

	// parseCache retains immutable Cypher ASTs keyed by normalized query text.
	parseCache *cypherParseCache

	// translationCache retains parameter-rebindable SQL translations by graph and parameter shape.
	translationCache *cypherTranslationCache

	// translationCacheProvider selects the translation cache for each physical
	// PostgreSQL connection. V1 installs a provider for translationCache;
	// absent or nil selections safely bypass retention.
	translationCacheProvider CypherTranslationCacheProvider

	// hasDefaultGraph distinguishes a cached default graph from the zero-value graph model.
	hasDefaultGraph bool

	// graphs indexes asserted database graph models by schema name.
	graphs map[string]model.Graph

	// kindsByID maps graph kind names to their PostgreSQL int2 identifiers.
	kindsByID map[graph.Kind]int16

	// kindIDsByKind maps PostgreSQL int2 identifiers back to graph kind names.
	kindIDsByKind map[int16]graph.Kind

	// lock protects cached graph and kind metadata from concurrent access.
	lock *sync.RWMutex

	// graphQueryMemoryLimit caps memory available to a graph query transaction.
	graphQueryMemoryLimit size.Size

	// traversalPolicyLock protects the versioned default-off production canary policy.
	traversalPolicyLock sync.RWMutex

	// traversalPolicy is copied on reads so callers cannot mutate live selection state.
	traversalPolicy TraversalPolicy
}

// NewSchemaManager creates an empty metadata manager with bounded parse and translation caches for pool.
func NewSchemaManager(pool *pgxpool.Pool, graphQueryMemoryLimit size.Size) *SchemaManager {
	translationCache := newCypherTranslationCache(defaultCypherTranslationCacheEntries)
	return &SchemaManager{
		pool:             pool,
		parseCache:       newCypherParseCache(defaultCypherParseCacheEntries),
		translationCache: translationCache,
		translationCacheProvider: sharedCypherTranslationCacheProvider{
			cache: translationCache,
		},
		hasDefaultGraph:       false,
		graphs:                map[string]model.Graph{},
		kindsByID:             map[graph.Kind]int16{},
		kindIDsByKind:         map[int16]graph.Kind{},
		lock:                  &sync.RWMutex{},
		graphQueryMemoryLimit: graphQueryMemoryLimit,
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

// WriteTransaction coordinates PostgreSQL driver behavior for write transaction.
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

// fetch replaces both in-memory kind indexes with the kinds visible through tx.
func (s *SchemaManager) fetch(tx graph.Transaction) error {
	if kinds, err := query.On(tx).SelectKinds(); err != nil {
		return err
	} else {
		s.kindsByID = kinds

		for kind, kindID := range s.kindsByID {
			s.kindIDsByKind[kindID] = kind
		}
	}

	return nil
}

// GetKindIDsByKind coordinates PostgreSQL driver behavior for get kind i ds by kind.
func (s *SchemaManager) GetKindIDsByKind() map[int16]graph.Kind {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.kindIDsByKind
}

// Fetch refreshes both in-memory kind indexes from a read transaction against the current schema.
func (s *SchemaManager) Fetch(ctx context.Context) error {
	return s.ReadTransaction(ctx, func(tx graph.Transaction) error {
		return s.fetch(tx)
	}, OptionSetQueryExecMode(pgx.QueryExecModeSimpleProtocol))
}

// defineKinds inserts any missing kinds and records their database IDs in both
// in-memory indexes.
func (s *SchemaManager) defineKinds(tx graph.Transaction, kinds graph.Kinds) error {
	for _, kind := range kinds {
		if kindID, err := query.On(tx).InsertOrGetKind(kind); err != nil {
			return err
		} else {
			s.kindsByID[kind] = kindID
			s.kindIDsByKind[kindID] = kind
		}
	}

	return nil
}

// mapKinds partitions semantic kinds into cached database IDs and unresolved kinds without refreshing the cache.
func (s *SchemaManager) mapKinds(kinds graph.Kinds) ([]int16, graph.Kinds) {
	var (
		missingKinds = make(graph.Kinds, 0, len(kinds))
		ids          = make([]int16, 0, len(kinds))
	)

	for _, kind := range kinds {
		if id, hasID := s.kindsByID[kind]; hasID {
			ids = append(ids, id)
		} else {
			missingKinds = append(missingKinds, kind)
		}
	}

	return ids, missingKinds
}

// MapKind coordinates PostgreSQL driver behavior for map kind.
func (s *SchemaManager) MapKind(ctx context.Context, kind graph.Kind) (int16, error) {
	s.lock.RLock()

	if id, hasID := s.kindsByID[kind]; hasID {
		s.lock.RUnlock()
		return id, nil
	}

	s.lock.RUnlock()
	s.lock.Lock()
	defer s.lock.Unlock()

	if err := s.Fetch(ctx); err != nil {
		return -1, err
	}

	if id, hasID := s.kindsByID[kind]; hasID {
		return id, nil
	} else {
		return -1, fmt.Errorf("unable to map kind: %s", kind.String())
	}
}

// MapKinds coordinates PostgreSQL driver behavior for map kinds.
func (s *SchemaManager) MapKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error) {
	s.lock.RLock()

	if mappedKinds, missingKinds := s.mapKinds(kinds); len(missingKinds) == 0 {
		s.lock.RUnlock()
		return mappedKinds, nil
	}

	s.lock.RUnlock()
	s.lock.Lock()
	defer s.lock.Unlock()

	if err := s.Fetch(ctx); err != nil {
		return nil, err
	}

	if mappedKinds, missingKinds := s.mapKinds(kinds); len(missingKinds) == 0 {
		return mappedKinds, nil
	} else {
		return nil, fmt.Errorf("unable to map kinds: %s", strings.Join(missingKinds.Strings(), ", "))
	}
}

// ReadTransaction coordinates PostgreSQL driver behavior for read transaction.
func (s *SchemaManager) ReadTransaction(ctx context.Context, txDelegate graph.TransactionDelegate, options ...graph.TransactionOption) error {
	if cfg, err := renderConfig(batchWriteSize, readOnlyTxOptions, options); err != nil {
		return err
	} else if conn, err := s.pool.Acquire(ctx); err != nil {
		return err
	} else {
		defer conn.Release()
		if stableSnapshotIsolation(cfg.Options.IsoLevel) && !cfg.skipStableSnapshotTraversalWorkspaces {
			if err := initializeStableSnapshotTraversalWorkspaces(ctx, conn); err != nil {
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

// stableSnapshotIsolation coordinates PostgreSQL driver behavior for stable snapshot isolation.
func stableSnapshotIsolation(isolation pgx.TxIsoLevel) bool {
	return isolation == pgx.RepeatableRead || isolation == pgx.Serializable
}

// initializeStableSnapshotTraversalWorkspaces coordinates PostgreSQL driver behavior for initialize stable snapshot traversal workspaces.
func initializeStableSnapshotTraversalWorkspaces(ctx context.Context, conn *pgxpool.Conn) error {
	const initializeSQL = `select
		public.ensure_shortest_dag_workspace(),
		public.ensure_bidirectional_shortest_path_workspace(),
		public.ensure_bidirectional_all_shortest_path_workspace()`
	if _, err := conn.Exec(ctx, initializeSQL); err != nil {
		return fmt.Errorf("initialize stable-snapshot traversal workspaces: %w", err)
	}
	return nil
}

// mapKindIDs partitions database kind IDs into cached semantic kinds and unresolved IDs without refreshing the cache.
func (s *SchemaManager) mapKindIDs(kindIDs []int16) (graph.Kinds, []int16) {
	var (
		missingIDs = make([]int16, 0, len(kindIDs))
		kinds      = make(graph.Kinds, 0, len(kindIDs))
	)

	for _, kindID := range kindIDs {
		if kind, hasKind := s.kindIDsByKind[kindID]; hasKind {
			kinds = append(kinds, kind)
		} else {
			missingIDs = append(missingIDs, kindID)
		}
	}

	return kinds, missingIDs
}

// MapKindID coordinates PostgreSQL driver behavior for map kind id.
func (s *SchemaManager) MapKindID(ctx context.Context, kindID int16) (graph.Kind, error) {
	if kindIDs, err := s.MapKindIDs(ctx, []int16{kindID}); err != nil {
		return nil, err
	} else {
		return kindIDs[0], nil
	}
}

// MapKindIDs coordinates PostgreSQL driver behavior for map kind i ds.
func (s *SchemaManager) MapKindIDs(ctx context.Context, kindIDs []int16) (graph.Kinds, error) {
	s.lock.RLock()

	if kinds, missingKinds := s.mapKindIDs(kindIDs); len(missingKinds) == 0 {
		s.lock.RUnlock()
		return kinds, nil
	}

	s.lock.RUnlock()
	s.lock.Lock()
	defer s.lock.Unlock()

	if err := s.Fetch(ctx); err != nil {
		return nil, err
	}

	if kinds, missingKinds := s.mapKindIDs(kindIDs); len(missingKinds) == 0 {
		return kinds, nil
	} else {
		return nil, fmt.Errorf("unable to map kind ids: %v", missingKinds)
	}
}

// assertKinds defines any missing kinds while holding the write lock and returns IDs from the refreshed in-memory mapping.
func (s *SchemaManager) assertKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error) {
	// Acquire a write-lock and release on-exit
	s.lock.Lock()
	defer s.lock.Unlock()

	// We have to re-acquire the missing kinds since there's a potential for another writer to acquire the write-lock
	// in between release of the read-lock and acquisition of the write-lock for this operation
	if _, missingKinds := s.mapKinds(kinds); len(missingKinds) > 0 {
		if err := s.WriteTransaction(ctx, func(tx graph.Transaction) error {
			return s.defineKinds(tx, missingKinds)
		}, OptionSetQueryExecMode(pgx.QueryExecModeSimpleProtocol)); err != nil {
			return nil, err
		}
	}

	// Lookup the kinds again from memory as they should now be up to date
	kindIDs, _ := s.mapKinds(kinds)
	return kindIDs, nil
}

// AssertKinds coordinates PostgreSQL driver behavior for assert kinds.
func (s *SchemaManager) AssertKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error) {
	// Acquire a read-lock first to fast-pass validate if we're missing any kind definitions
	s.lock.RLock()

	if kindIDs, missingKinds := s.mapKinds(kinds); len(missingKinds) == 0 {
		// All kinds are defined. Release the read-lock here before returning
		s.lock.RUnlock()
		return kindIDs, nil
	}

	// Release the read-lock here so that we can acquire a write-lock
	s.lock.RUnlock()
	return s.assertKinds(ctx, kinds)
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

// SetDefaultGraph coordinates PostgreSQL driver behavior for set default graph.
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

// AssertDefaultGraph coordinates PostgreSQL driver behavior for assert default graph.
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

// DefaultGraph coordinates PostgreSQL driver behavior for default graph.
func (s *SchemaManager) DefaultGraph() (model.Graph, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.defaultGraph, s.hasDefaultGraph
}

// assertGraph coordinates PostgreSQL driver behavior for assert graph.
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

// AssertGraph coordinates PostgreSQL driver behavior for assert graph.
func (s *SchemaManager) AssertGraph(tx graph.Transaction, schema graph.Graph) (model.Graph, error) {
	// Acquire a read-lock first to fast-pass validate if we're missing the graph definitions
	s.lock.RLock()

	if graphInstance, isDefined := s.graphs[schema.Name]; isDefined {
		// The graph is defined. Release the read-lock here before returning
		s.lock.RUnlock()
		return graphInstance, nil
	}

	// Release the read-lock here so that we can acquire a write-lock next
	s.lock.RUnlock()

	s.lock.Lock()
	defer s.lock.Unlock()

	if graphInstance, isDefined := s.graphs[schema.Name]; isDefined {
		// The graph was defined by a different actor between the read unlock and the write lock, return it
		return graphInstance, nil
	}

	return s.assertGraph(tx, schema)
}

// assertSchema creates schema storage and defines every node and relationship kind required by its graphs.
func (s *SchemaManager) assertSchema(tx graph.Transaction, schema graph.Schema) error {
	if err := query.On(tx).CreateSchema(); err != nil {
		return err
	}

	if err := s.fetch(tx); err != nil {
		return err
	}

	for _, graphSchema := range schema.Graphs {
		if _, missingKinds := s.mapKinds(graphSchema.Nodes); len(missingKinds) > 0 {
			if err := s.defineKinds(tx, missingKinds); err != nil {
				return err
			}
		}

		if _, missingKinds := s.mapKinds(graphSchema.Edges); len(missingKinds) > 0 {
			if err := s.defineKinds(tx, missingKinds); err != nil {
				return err
			}
		}
	}

	return nil
}

// AssertSchema coordinates PostgreSQL driver behavior for assert schema.
func (s *SchemaManager) AssertSchema(ctx context.Context, schema graph.Schema) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.WriteTransaction(ctx, func(tx graph.Transaction) error {
		return s.assertSchema(tx, schema)
	}, OptionSetQueryExecMode(pgx.QueryExecModeSimpleProtocol))
}
