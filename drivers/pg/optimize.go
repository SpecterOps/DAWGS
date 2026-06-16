package pg

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// deadTupleThreshold is the minimum fraction of dead tuples a partitioned
// parent must accumulate across its partitions before OptimizeStorage will
// vacuum it.
const deadTupleThreshold = 0.1

// Sum n_dead_tup and n_live_tup across every leaf partition of the parent;
const optimizeStorageStatsQuery = `
	SELECT
		COALESCE(SUM(stat.n_dead_tup), 0),
		COALESCE(SUM(stat.n_live_tup), 0)
	FROM pg_partition_tree($1::regclass) tree
	LEFT JOIN pg_stat_user_tables stat ON stat.relid = tree.relid
	WHERE tree.isleaf
`

type optimizeStorageConn interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, arguments ...any) pgx.Row
}

func optimizeStorage(ctx context.Context, conn optimizeStorageConn) error {
	var targets []string
	for _, table := range []string{"node", "edge"} {
		var dead, live int64
		if err := conn.QueryRow(ctx, optimizeStorageStatsQuery, table).Scan(&dead, &live); err != nil {
			return fmt.Errorf("query dead tuple stats for %s: %w", table, err)
		}

		total := dead + live
		var deadTupleRatio float64
		if total > 0 {
			deadTupleRatio = float64(dead) / float64(total)
		}

		slog.InfoContext(ctx, "Queried PostgreSQL table storage statistics",
			slog.String("table", table),
			slog.Int64("dead_tuples", dead),
			slog.Int64("live_tuples", live),
			slog.Int64("total_tuples", total),
			slog.Float64("dead_tuple_ratio", deadTupleRatio),
			slog.Float64("dead_tuple_threshold", deadTupleThreshold),
		)

		if total == 0 {
			continue
		}
		if deadTupleRatio >= deadTupleThreshold {
			targets = append(targets, table)
		}
	}

	if len(targets) == 0 {
		return nil
	}

	// Targeting the partitioned parents cascades to every partition.
	stmt := "VACUUM (ANALYZE) " + strings.Join(targets, ", ")
	slog.InfoContext(ctx, "Executing PostgreSQL storage optimization",
		slog.Any("targets", targets),
		slog.String("statement", stmt),
	)

	if _, err := conn.Exec(ctx, stmt, pgx.QueryExecModeSimpleProtocol); err != nil {
		return fmt.Errorf("%s: %w", stmt, err)
	}

	return nil
}
