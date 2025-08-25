package changelog

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
)

// db is the persistence layer for the changelog.
//
// It provides methods to issue bulk UPDATEs against the node and edge
// tables. The db type is intentionally thin: it abstracts only the minimal
// database operations (currently Exec) so that it can be unit tested with
// pgxmock while still working against a real pgxpool in production.
//
// In practice, db is responsible for writing "lastseen" timestamps during
// flushes, which allows stale nodes and edges to expire out of the graph
// according to reconciliation policies.
type db struct {
	conn       Execer
	kindMapper pg.KindMapper
}

// Execer contains the methods we actually use from pgxpool. putting these here makes testing easier
type Execer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

func newLogDB(conn Execer, kindMapper pg.KindMapper) db {
	return db{
		conn:       conn,
		kindMapper: kindMapper,
	}
}

func extractLastSeen(props *graph.Properties, entityID string) time.Time {
	now := time.Now()

	if props == nil {
		slog.Warn("property bag is nil", "id", entityID)
		return now
	}

	if lastseen, err := props.Get("lastseen").Time(); err != nil {
		slog.Warn("lastseen property parse error", "id", entityID, "error", err)
		return now
	} else {
		return lastseen
	}
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
	lastseen := extractLastSeen(changes[0].Properties, changes[0].NodeID)

	// Collect all node IDs as strings for the WHERE filter
	ids := make([]string, len(changes))
	for i, c := range changes {
		ids[i] = c.NodeID
	}

	ct, err := s.conn.Exec(ctx, updateNodesLastSeenSQL, lastseen, ids)
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
	lastseen := extractLastSeen(changes[0].Properties,
		fmt.Sprintf("%s-[:%s]-%s",
			changes[0].SourceNodeID,
			changes[0].Kind,
			changes[0].TargetNodeID))

	// Collect all edge IDs and kinds as strings for the WHERE filter
	startIDs := make([]string, len(changes))
	endIDs := make([]string, len(changes))
	kinds := make(graph.Kinds, len(changes))
	for i, change := range changes {
		startIDs[i] = change.SourceNodeID
		endIDs[i] = change.TargetNodeID
		kinds[i] = change.Kind
	}

	kindIDs, err := s.kindMapper.MapKinds(ctx, kinds)
	if err != nil {
		return 0, fmt.Errorf("mapping kinds: %w", err)
	}

	ct, err := s.conn.Exec(ctx, updateEdgesLastSeenSQL, lastseen, startIDs, endIDs, kindIDs)
	if err != nil {
		return 0, fmt.Errorf("updating edges lastseen: %w", err)
	}

	return ct.RowsAffected(), nil
}
