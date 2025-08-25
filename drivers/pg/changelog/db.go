package changelog

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
)

type db struct {
	Conn       DBConn
	kindMapper pg.KindMapper
}

// DBConn contains the methods we actually use from pgxpool. putting these here makes testing easier
type DBConn interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

func newLogDB(pgxPool DBConn, kindMapper pg.KindMapper) (db, error) {
	return db{
		Conn:       pgxPool,
		kindMapper: kindMapper,
	}, nil
}

var updateNodesLastSeenSQL = `
		UPDATE node n
		SET properties = jsonb_set(
			COALESCE(n.properties, '{}'::jsonb),
			'{lastseen}',
			to_jsonb($1::timestamptz)
		)
		WHERE n.properties->>'objectid' = ANY($2::text[]);
	`

// flushNodeChanges updates the nodes table with lastseen information for each node in `changes`.
// this is necessary to maintain reconciliation behavior, in which nodes expire out of the graph when lastseen exceeds a configurable threshold.
func (s *db) flushNodeChanges(ctx context.Context, changes []NodeChange) (int64, error) {
	// Early exit check for empty buffer flushes
	if len(changes) == 0 {
		return 0, nil
	}

	// defensive:
	// - ingest codepath always sets the lastseen property. if property is not present on change, log warning and set lastseen to time.now()
	// seems like unnecessary overhead to extract the property from each change. just read once from first change and re-use.
	props := changes[0].Properties
	if props == nil {
		return 0, fmt.Errorf("property bag for node %s is nil", changes[0].NodeID)
	}

	var lastseen time.Time
	lastseen, err := props.Get("lastseen").Time()
	if err != nil {
		slog.Warn("lastseen property", slog.Any("error", err))
		lastseen = time.Now()
	}

	// Collect all node IDs as strings for the WHERE filter
	ids := make([]string, 0, len(changes))
	for _, c := range changes {
		ids = append(ids, c.NodeID)
	}

	ct, err := s.Conn.Exec(ctx, updateNodesLastSeenSQL, lastseen, ids)
	if err != nil {
		return 0, fmt.Errorf("updating nodes lastseen: %w", err)
	}

	return ct.RowsAffected(), nil
}

var updateEdgesLastSeenSQL = `
		UPDATE edge e
		SET properties = jsonb_set(
			COALESCE(e.properties, '{}'::jsonb),
			'{lastseen}',
			to_jsonb($1::timestamptz)
		)
		WHERE (e.start_id, e.end_id, e.kind_id) IN (
			SELECT src.id, tgt.id, pair.kind_id
			FROM unnest($2::text[], $3::text[], $4::smallint[]) 
				AS pair(src_objid, tgt_objid, kind_id)
			JOIN node src ON src.properties->>'objectid' = pair.src_objid
			JOIN node tgt ON tgt.properties->>'objectid' = pair.tgt_objid
		);
		`

// flushEdgeChanges updates the edges table with lastseen information for each edge in `changes`.
// this is necessary to maintain reconciliation behavior, in which edges expire out of the graph when lastseen exceeds a configurable threshold.
func (s *db) flushEdgeChanges(ctx context.Context, changes []EdgeChange) (int64, error) {
	// Early exit check for empty buffer flushes
	if len(changes) == 0 {
		return 0, nil
	}

	// defensive:
	// - ingest codepath always sets the lastseen property. if property is not present on change, log warning and set lastseen to time.now()
	// seems like unnecessary overhead to extract the property from each change. just read once from first change and re-use.
	props := changes[0].Properties
	if props == nil {
		return 0, fmt.Errorf("property bag for edge %s-[:%s]-%s is nil",
			changes[0].SourceNodeID,
			changes[0].Kind,
			changes[0].TargetNodeID)
	}

	var lastseen time.Time
	lastseen, err := props.Get("lastseen").Time()
	if err != nil {
		slog.Warn("lastseen property", slog.Any("error", err))
		lastseen = time.Now()
	}

	// Collect all edge IDs and kinds as strings for the WHERE filter
	startIDs := make([]string, 0, len(changes))
	endIDs := make([]string, 0, len(changes))
	kinds := make(graph.Kinds, 0, len(changes))
	for _, c := range changes {
		startIDs = append(startIDs, c.SourceNodeID)
		endIDs = append(endIDs, c.TargetNodeID)
		kinds = append(kinds, c.Kind)
	}

	kindIDs, err := s.kindMapper.MapKinds(ctx, kinds)
	if err != nil {
		return 0, fmt.Errorf("mapping kinds: %w", err)
	}

	ct, err := s.Conn.Exec(ctx, updateEdgesLastSeenSQL, lastseen, startIDs, endIDs, kindIDs)
	if err != nil {
		return 0, fmt.Errorf("updating edges lastseen: %w", err)
	}

	return ct.RowsAffected(), nil
}
