package changelog

import (
	"context"
	"fmt"
	"log/slog"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type db struct {
	Conn DBConn
}

// DBConn contains the methods we actually use from pgxpool. putting these here makes testing easier
type DBConn interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

func newLogDB(ctx context.Context, pgxPool DBConn) (db, error) {
	return db{
		Conn: pgxPool,
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

// flushNodeChanges updates the nodes table with lastseen information for each node in `changes`
func (s *db) flushNodeChanges(ctx context.Context, changes []*NodeChange) (int64, error) {
	slog.Info("flushnodechanges")
	// Early exit check for empty buffer flushes
	if len(changes) == 0 {
		return 0, nil
	}

	// ingest sets the lastseen property.
	// seems like unnecessary overhead to extract the property from each change. just read once from first change and re-use
	t, err := changes[0].Properties.Get("lastseen").Time()
	if err != nil {
		return 0, fmt.Errorf("reading lastseen property: %w", err)
	}

	// Collect all node IDs as strings for the WHERE filter
	ids := make([]string, 0, len(changes))
	for _, c := range changes {
		ids = append(ids, c.NodeID)
	}

	ct, err := s.Conn.Exec(ctx, updateNodesLastSeenSQL, t, ids)
	if err != nil {
		return 0, fmt.Errorf("updating nodes lastseen: %w", err)
	}

	return ct.RowsAffected(), nil
}
