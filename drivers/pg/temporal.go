package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/graph"
)

const (
	createTemporalNodeTable = `create temp table node on commit drop as
		select n.id, n.graph_id, n.kind_ids, n.properties, n.created_at
		from node n where n.graph_id = $1 and n.created_at <= $2
		union all
		select d.node_id as id, d.graph_id, d.kind_ids, d.properties, d.created_at
		from node_deletion_log d where d.graph_id = $1 and d.deleted_at > $2 and d.created_at <= $2`

	createTemporalEdgeTable = `create temp table edge on commit drop as
		select e.id, e.graph_id, e.start_id, e.end_id, e.kind_id, e.properties, e.created_at
		from edge e where e.graph_id = $1 and e.created_at <= $2
		union all
		select d.edge_id as id, d.graph_id, d.start_id, d.end_id, d.kind_id, d.properties, d.created_at
		from edge_deletion_log d where d.graph_id = $1 and d.deleted_at > $2 and d.created_at <= $2`
)

// AsOfReadTransaction opens a read transaction that presents the graph as it existed at the given time.
// It creates temporary tables that shadow the real node and edge tables, populated with the historical
// state reconstructed from current data and deletion logs. All queries within the delegate transparently
// operate against the historical snapshot.
func (s *Driver) AsOfReadTransaction(ctx context.Context, asOfTime time.Time, txDelegate graph.TransactionDelegate, options ...graph.TransactionOption) error {
	defaultGraph, hasDefaultGraph := s.SchemaManager.DefaultGraph()
	if !hasDefaultGraph {
		return fmt.Errorf("as-of read transaction requires a default graph to be set")
	}

	cfg, err := renderConfig(batchWriteSize, readWriteTxOptions, options)
	if err != nil {
		return err
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	// We need a real PG transaction for ON COMMIT DROP temp tables.
	// ReadWrite is required because CREATE TEMP TABLE ... AS involves writing to the temp table.
	pgxTx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadWrite})
	if err != nil {
		return err
	}
	defer pgxTx.Rollback(ctx)

	// Create temp tables that shadow the permanent node/edge tables with historical data
	if _, err := pgxTx.Exec(ctx, createTemporalNodeTable, defaultGraph.ID, asOfTime); err != nil {
		return fmt.Errorf("failed to create temporal node table: %w", err)
	}

	if _, err := pgxTx.Exec(ctx, createTemporalEdgeTable, defaultGraph.ID, asOfTime); err != nil {
		return fmt.Errorf("failed to create temporal edge table: %w", err)
	}

	// Wrap in a transaction struct so the delegate can use the standard query interface
	tx := &transaction{
		schemaManager:      s.SchemaManager,
		queryExecMode:      cfg.QueryExecMode,
		queryResultsFormat: cfg.QueryResultFormats,
		ctx:                ctx,
		conn:               conn,
		tx:                 pgxTx,
		targetSchemaSet:    false,
	}

	if err := txDelegate(tx); err != nil {
		return err
	}

	return pgxTx.Commit(ctx)
}
