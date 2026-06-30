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
	batchWriteSize    = defaultBatchWriteSize
	readOnlyTxOptions = pgx.TxOptions{
		AccessMode: pgx.ReadOnly,
	}

	readWriteTxOptions = pgx.TxOptions{
		AccessMode: pgx.ReadWrite,
	}
)

type Config struct {
	Options            pgx.TxOptions
	QueryExecMode      pgx.QueryExecMode
	QueryResultFormats pgx.QueryResultFormats
	BatchWriteSize     int
}

func OptionSetQueryExecMode(queryExecMode pgx.QueryExecMode) graph.TransactionOption {
	return func(config *graph.TransactionConfig) {
		if pgCfg, typeOK := config.DriverConfig.(*Config); typeOK {
			pgCfg.QueryExecMode = queryExecMode
		}
	}
}

type Driver struct {
	pool *pgxpool.Pool
	*SchemaManager
}

func NewDriver(graphQueryMemoryLimit size.Size, pool *pgxpool.Pool) *Driver {
	return &Driver{
		pool:          pool,
		SchemaManager: NewSchemaManager(pool, graphQueryMemoryLimit),
	}
}

func (s *Driver) SetDefaultGraph(ctx context.Context, graphSchema graph.Graph) error {
	return s.SchemaManager.SetDefaultGraph(ctx, graphSchema)
}

func (s *Driver) KindMapper() KindMapper {
	return s.SchemaManager
}

func (s *Driver) SetBatchWriteSize(size int) {
	batchWriteSize = size
}

func (s *Driver) SetWriteFlushSize(size int) {
	// THis is a no-op function since PostgreSQL does not require transaction rotation like Neo4j does
}

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

func (s *Driver) Close(ctx context.Context) error {
	s.pool.Close()
	return nil
}

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

func (s *Driver) FetchSchema(ctx context.Context) (graph.Schema, error) {
	// TODO: This is not required for existing functionality as the SchemaManager type handles most of this negotiation
	//		 however, in the future this function would make it easier to make schema management generic and should be
	//		 implemented.
	return graph.Schema{}, fmt.Errorf("not implemented")
}

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

	return nil
}

func (s *Driver) Run(ctx context.Context, query string, parameters map[string]any) error {
	return s.WriteTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Raw(query, parameters)
		defer result.Close()

		return result.Error()
	})
}

func (s *Driver) FetchKinds(_ context.Context) (graph.Kinds, error) {
	var kinds graph.Kinds
	for _, kind := range s.SchemaManager.GetKindIDsByKind() {
		kinds = append(kinds, kind)
	}

	return kinds, nil
}

func (s *Driver) RefreshKinds(ctx context.Context) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Wipe this map to be rebuilt in the fetch call below
	s.SchemaManager.kindIDsByKind = map[int16]graph.Kind{}
	return s.SchemaManager.Fetch(ctx)
}

func (s *Driver) OptimizeStorage(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection for VACUUM: %w", err)
	}
	defer conn.Release()

	return optimizeStorage(ctx, conn)
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

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection for node delete: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, statement, arguments...); err != nil {
		return fmt.Errorf("%s: %w", statement, err)
	}

	return nil
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

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection for relationship delete: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, statement, kindIDs); err != nil {
		return fmt.Errorf("%s: %w", statement, err)
	}

	return nil
}
