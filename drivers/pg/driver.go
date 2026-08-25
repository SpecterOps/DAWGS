package pg

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
)

var (
	// batchWriteSize is the process-wide flush threshold used by new batch operations.
	batchWriteSize = defaultBatchWriteSize

	// readOnlyTxOptions configures transactions that must not mutate PostgreSQL state.
	readOnlyTxOptions = pgx.TxOptions{
		AccessMode: pgx.ReadOnly,
	}

	// readWriteTxOptions configures transactions that may mutate PostgreSQL state.
	readWriteTxOptions = pgx.TxOptions{
		AccessMode: pgx.ReadWrite,
	}
)

// Config configures PostgreSQL transaction execution for one graph operation.
type Config struct {
	// Options controls PostgreSQL transaction isolation and access mode.
	Options pgx.TxOptions

	// QueryExecMode selects pgx's query execution protocol.
	QueryExecMode pgx.QueryExecMode

	// QueryResultFormats selects the PostgreSQL wire format for returned columns.
	QueryResultFormats pgx.QueryResultFormats

	// BatchWriteSize is the number of mutations accumulated before a batch flushes.
	BatchWriteSize int

	// initializeTraversalRuntimeAttestation prepares session-local receipt state before BEGIN.
	initializeTraversalRuntimeAttestation bool

	// skipStableSnapshotTraversalWorkspaces prevents ordinary-expansion tool
	// studies from paying unrelated SP/ASP temporary-workspace setup.
	skipStableSnapshotTraversalWorkspaces bool
}

// OptionSetQueryExecMode classifies option set query exec mode for downstream policy decisions.
func OptionSetQueryExecMode(queryExecMode pgx.QueryExecMode) graph.TransactionOption {
	return func(config *graph.TransactionConfig) {
		if pgCfg, typeOK := config.DriverConfig.(*Config); typeOK {
			pgCfg.QueryExecMode = queryExecMode
		}
	}
}

// OptionSetTransactionIsolation requests an explicit PostgreSQL transaction at
// the supplied isolation level. B traversal candidates are selected only for
// REPEATABLE READ or SERIALIZABLE transactions. The driver prepares the
// production shortest-path and all-shortest-path temporary workspaces on the
// acquired session before beginning either stable-snapshot transaction and
// uses PostgreSQL READ WRITE access so those session-local tables can reset.
func OptionSetTransactionIsolation(isolation pgx.TxIsoLevel) graph.TransactionOption {
	return func(config *graph.TransactionConfig) {
		if pgCfg, typeOK := config.DriverConfig.(*Config); typeOK {
			pgCfg.Options.IsoLevel = isolation
			if stableSnapshotIsolation(isolation) {
				pgCfg.Options.AccessMode = pgx.ReadWrite
			}
		}
	}
}

// OptionInitializeTraversalRuntimeAttestation prepares the acquired PostgreSQL
// session before an explicit read-only transaction begins. Callers that arm
// traversal runtime receipts inside a graph transaction need this option
// because PostgreSQL forbids creating the temporary workspace after BEGIN READ
// ONLY. GraphBench normally pins and prepares its session before the timed
// transaction instead.
func OptionInitializeTraversalRuntimeAttestation() graph.TransactionOption {
	return func(config *graph.TransactionConfig) {
		if pgCfg, typeOK := config.DriverConfig.(*Config); typeOK {
			pgCfg.initializeTraversalRuntimeAttestation = true
		}
	}
}

// OptionSkipStableSnapshotTraversalWorkspacesForTool keeps a Repeatable Read
// ordinary-expansion measurement free of unrelated shortest-path workspace
// setup. It is intentionally tool-scoped and does not alter production policy.
func OptionSkipStableSnapshotTraversalWorkspacesForTool() graph.TransactionOption {
	return func(config *graph.TransactionConfig) {
		if pgCfg, typeOK := config.DriverConfig.(*Config); typeOK {
			pgCfg.skipStableSnapshotTraversalWorkspaces = true
		}
	}
}

// Driver implements the graph database contract on a PostgreSQL connection pool.
type Driver struct {
	// pool supplies connections for graph transactions and maintenance operations.
	pool *pgxpool.Pool

	// runtime owns the connection-local cache provider installed by the pool
	// constructor. It is nil only for an externally constructed pool.
	runtime *poolRuntime

	// SchemaManager owns asserted graph metadata, kind mappings, and query caches.
	*SchemaManager
}

// NewDriver creates a PostgreSQL graph driver for pool. Pool constructors are
// responsible for installing all required lifecycle hooks; this constructor
// uses the associated connection-local runtime when present.
func NewDriver(graphQueryMemoryLimit size.Size, pool *pgxpool.Pool) *Driver {
	runtime := poolRuntimeFor(pool)
	var provider CypherTranslationCacheProvider
	if runtime != nil {
		provider = runtime.provider
	}
	schemaManager := NewSchemaManager(pool, graphQueryMemoryLimit, provider)

	return &Driver{
		pool:          pool,
		runtime:       runtime,
		SchemaManager: schemaManager,
	}
}

// SetDefaultGraph validates and selects graphSchema as the driver's default graph.
func (s *Driver) SetDefaultGraph(ctx context.Context, graphSchema graph.Graph) error {
	return s.SchemaManager.SetDefaultGraph(ctx, graphSchema)
}

// KindMapper returns the driver's graph-kind to PostgreSQL-ID mapper.
func (s *Driver) KindMapper() KindMapper {
	return s.SchemaManager
}

// SetBatchWriteSize changes the process-wide mutation count used for new batch flushes.
func (s *Driver) SetBatchWriteSize(size int) {
	batchWriteSize = size
}

// SetWriteFlushSize is a no-op because PostgreSQL batches do not rotate transactions by size.
func (s *Driver) SetWriteFlushSize(size int) {
	// THis is a no-op function since PostgreSQL does not require transaction rotation like Neo4j does
}

// BatchOperation runs batchDelegate in a write batch using the supplied batch options.
func (s *Driver) BatchOperation(ctx context.Context, batchDelegate graph.BatchDelegate, options ...graph.BatchOption) error {
	batchConfig := &graph.BatchConfig{
		BatchSize: batchWriteSize,
	}

	for _, opt := range options {
		opt(batchConfig)
	}

	if cfg, err := renderConfig(batchConfig.BatchSize, readWriteTxOptions, nil); err != nil {
		return err
	} else if conn, err := s.pool.Acquire(ctx); err != nil {
		return err
	} else {
		defer conn.Release()

		if batch, err := newBatch(ctx, conn, s.SchemaManager, cfg); err != nil {
			return err
		} else {
			defer batch.Close()

			if err := batchDelegate(batch); err != nil {
				return err
			}

			return batch.Commit()
		}
	}
}

// Close stops the driver's query caches before releasing its PostgreSQL pool.
func (s *Driver) Close(ctx context.Context) error {
	if s.SchemaManager != nil {
		s.SchemaManager.parseCache.Close()
	}
	if s.runtime != nil {
		s.runtime.close()
	} else if s.pool != nil {
		s.pool.Close()
	}
	return nil
}

// TranslationCacheStats returns query-text-free counters for this driver's
// bounded Cypher-to-SQL translation cache.
func (s *Driver) TranslationCacheStats() Stats {
	if s == nil || s.runtime == nil || s.runtime.provider == nil {
		return Stats{}
	}
	return s.runtime.provider.stats()
}

// ParseCacheStats returns query-text-free counters for this driver's bounded Cypher parse cache.
func (s *Driver) ParseCacheStats() ParseCacheStats {
	if s == nil || s.SchemaManager == nil {
		return ParseCacheStats{}
	}
	return s.SchemaManager.parseCache.Stats()
}

// renderConfig applies transaction options to PostgreSQL defaults and rejects
// a driver configuration of the wrong concrete type.
func renderConfig(batchWriteSize int, pgxOptions pgx.TxOptions, userOptions []graph.TransactionOption) (*Config, error) {
	graphCfg := graph.TransactionConfig{
		DriverConfig: &Config{
			Options:            pgxOptions,
			QueryExecMode:      pgx.QueryExecModeCacheStatement,
			QueryResultFormats: pgx.QueryResultFormats{pgx.BinaryFormatCode},
			BatchWriteSize:     batchWriteSize,
		},
	}

	for _, option := range userOptions {
		option(&graphCfg)
	}

	if graphCfg.DriverConfig != nil {
		if pgCfg, typeOK := graphCfg.DriverConfig.(*Config); !typeOK {
			return nil, fmt.Errorf("invalid driver config type %T", graphCfg.DriverConfig)
		} else {
			return pgCfg, nil
		}
	}

	return nil, fmt.Errorf("driver config is nil")
}

// FetchSchema is not implemented because PostgreSQL schema discovery is owned by SchemaManager.
func (s *Driver) FetchSchema(ctx context.Context) (graph.Schema, error) {
	// TODO: This is not required for existing functionality as the SchemaManager type handles most of this negotiation
	//		 however, in the future this function would make it easier to make schema management generic and should be
	//		 implemented.
	return graph.Schema{}, fmt.Errorf("not implemented")
}

// AssertSchema creates or validates the requested schema and resets pooled type metadata afterward.
func (s *Driver) AssertSchema(ctx context.Context, schema graph.Schema) error {
	// Resetting the pool must be done on every schema assertion as composite types may have changed OIDs
	defer s.pool.Reset()

	// Assert that the base graph schema exists and has a matching schema definition
	if err := s.SchemaManager.AssertSchema(ctx, schema); err != nil {
		return err
	}

	if schema.DefaultGraph.Name != "" {
		// There's a default graph defined. Assert that it exists and has a matching schema
		if err := s.SchemaManager.AssertDefaultGraph(ctx, schema.DefaultGraph); err != nil {
			return err
		}
	}
	if s.runtime != nil && s.runtime.provider != nil {
		s.runtime.provider.advanceSchemaGeneration()
	}

	return nil
}

// Run executes raw SQL in a write transaction and returns its terminal result error.
func (s *Driver) Run(ctx context.Context, query string, parameters map[string]any) error {
	return s.WriteTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Raw(query, parameters)
		defer result.Close()

		return result.Error()
	})
}

// FetchKinds returns the current in-memory graph-kind mapping.
func (s *Driver) FetchKinds(_ context.Context) (graph.Kinds, error) {
	var kinds graph.Kinds
	for _, kind := range s.SchemaManager.GetKindIDsByKind() {
		kinds = append(kinds, kind)
	}

	return kinds, nil
}

// RefreshKinds discards and reloads the driver's in-memory kind mapping.
func (s *Driver) RefreshKinds(ctx context.Context) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Wipe this map to be rebuilt in the fetch call below
	s.SchemaManager.kindIDsByKind = map[int16]graph.Kind{}
	if err := s.SchemaManager.Fetch(ctx); err != nil {
		return err
	}
	if s.runtime != nil && s.runtime.provider != nil {
		s.runtime.provider.advanceSchemaGeneration()
	}
	return nil
}

// WarmStatements prepares selected SQL on currently idle physical
// connections without executing it.
func (s *Driver) WarmStatements(ctx context.Context, statements ...string) error {
	if s == nil || s.runtime == nil {
		return nil
	}
	return s.runtime.warmStatements(ctx, statements...)
}

// SetStatementWarmupPolicy installs the warm set for current and future
// physical connections.
func (s *Driver) SetStatementWarmupPolicy(ctx context.Context, statements ...string) error {
	if s == nil || s.runtime == nil {
		return nil
	}
	return s.runtime.setStatementWarmupPolicy(ctx, statements...)
}

// OptimizeStorage runs PostgreSQL storage maintenance on a leased pool connection.
func (s *Driver) OptimizeStorage(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection for VACUUM: %w", err)
	}
	defer conn.Release()

	return optimizeStorage(ctx, conn)
}

// WipeGraph truncates the partitioned node and edge tables, removing every node and edge across all graphs in a single
// statement that bypasses the per-row edge cascade trigger. The optional retain delegate runs within the same
// transaction after the truncate, allowing callers to atomically recreate any data that must survive the wipe. If retain
// returns an error the transaction is rolled back and the graph is left untouched.
func (s *Driver) WipeGraph(ctx context.Context, retain graph.TransactionDelegate) error {
	return s.WriteTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Raw("truncate table node, edge;", nil)

		// Close before issuing further statements: a pgx transaction shares a single connection and cannot run the
		// retain delegate's queries while these rows remain open.
		result.Close()

		if err := result.Error(); err != nil {
			return fmt.Errorf("truncating graph tables: %w", err)
		}

		if retain != nil {
			return retain(tx)
		}

		return nil
	})
}

// resolveKindIDs maps kinds to their integer IDs, refreshing the schema cache once on a miss. It returns the resolved
// IDs alongside any kinds that remain undefined after the refresh, so callers can decide whether an unresolved kind is
// a tolerable no-op (include predicates) or must fail closed (exclude predicates).
func (s *Driver) resolveKindIDs(ctx context.Context, kinds graph.Kinds) ([]int16, graph.Kinds, error) {
	if len(kinds) == 0 {
		return nil, nil, nil
	}

	s.lock.RLock()
	if kindIDs, missingKinds := s.mapKinds(kinds); len(missingKinds) == 0 {
		s.lock.RUnlock()
		return kindIDs, nil, nil
	}
	s.lock.RUnlock()

	s.lock.Lock()
	defer s.lock.Unlock()

	if err := s.Fetch(ctx); err != nil {
		return nil, nil, err
	}

	kindIDs, missingKinds := s.mapKinds(kinds)
	return kindIDs, missingKinds, nil
}

// DeleteNodesByKinds performs a server-side, set-based delete of nodes using the kind_ids GIN index instead of
// streaming node IDs through the application. A node is deleted when its kind_ids overlap includeAny (or, when
// includeAny is empty, for every node) and do not overlap excludeAny. Deleting nodes fires the delete_node_edges
// trigger, cascading the attached edge deletes.
//
// includeAny is mapped to kind IDs tolerantly: include kinds that are not defined in the database map to no IDs and
// therefore match no nodes, so a request that targets only undefined kinds is a safe no-op rather than an accidental
// full delete. excludeAny is mapped fail-closed: if any exclude kind is undefined the delete is refused, because
// silently dropping an exclusion would widen the delete and could remove protected nodes (e.g. an unresolved
// MigrationData would turn a guarded wipe into an unguarded delete from node).
func (s *Driver) DeleteNodesByKinds(ctx context.Context, includeAny graph.Kinds, excludeAny graph.Kinds) error {
	includeIDs, _, err := s.resolveKindIDs(ctx, includeAny)
	if err != nil {
		return err
	}

	excludeIDs, excludeMissing, err := s.resolveKindIDs(ctx, excludeAny)
	if err != nil {
		return err
	}
	if len(excludeMissing) > 0 {
		return fmt.Errorf("cannot exclude undefined kinds from node delete: %v", excludeMissing)
	}

	statement, arguments := buildNodeDeleteStatement(len(includeAny) > 0, includeIDs, excludeIDs)

	return s.execDelete(ctx, "node", statement, arguments...)
}

// buildNodeDeleteStatement renders the node delete statement and its positional arguments for the given resolved kind
// IDs. The include predicate is emitted whenever an include filter was requested (includeRequested), even if includeIDs
// is empty, so that targeting only undefined kinds matches no nodes. The exclude predicate is emitted only when
// excludeIDs is non-empty, so an unresolved exclusion can never widen the delete into an unguarded wipe.
func buildNodeDeleteStatement(includeRequested bool, includeIDs []int16, excludeIDs []int16) (string, []any) {
	var (
		predicates []string
		arguments  []any
	)

	if includeRequested {
		arguments = append(arguments, includeIDs)
		predicates = append(predicates, fmt.Sprintf("kind_ids operator (pg_catalog.&&) $%d::int2[]", len(arguments)))
	}

	if len(excludeIDs) > 0 {
		arguments = append(arguments, excludeIDs)
		predicates = append(predicates, fmt.Sprintf("not (kind_ids operator (pg_catalog.&&) $%d::int2[])", len(arguments)))
	}

	statement := "delete from node"
	if len(predicates) > 0 {
		statement += " where " + strings.Join(predicates, " and ")
	}

	return statement, arguments
}

// DeleteRelationshipsByKinds performs a server-side, set-based delete of relationships whose kind_id matches any of
// the given kinds, using the edge_kind_id_id_start_id_end_id_index covering index instead of streaming relationship
// IDs through the application.
//
// kinds are mapped to kind IDs tolerantly: kinds that are not defined in the database map to no IDs. An empty kinds
// argument, or one that maps entirely to undefined kinds, deletes nothing rather than every relationship.
func (s *Driver) DeleteRelationshipsByKinds(ctx context.Context, kinds graph.Kinds) error {
	if len(kinds) == 0 {
		return nil
	}

	kindIDs, _, err := s.resolveKindIDs(ctx, kinds)
	if err != nil {
		return err
	}

	const statement = "delete from edge where kind_id = any($1::int2[])"

	return s.execDelete(ctx, "relationship", statement, kindIDs)
}

// execDelete acquires a pooled connection and runs a delete statement, wrapping acquisition and execution errors. label
// names the delete for the acquire error message; statement and arguments are passed through unchanged so each caller
// preserves its own SQL, positional arguments, and statement error wrapping.
func (s *Driver) execDelete(ctx context.Context, label, statement string, arguments ...any) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection for %s delete: %w", label, err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, statement, arguments...); err != nil {
		return fmt.Errorf("%s: %w", statement, err)
	}

	return nil
}
