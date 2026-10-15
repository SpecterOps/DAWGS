package pg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// sqlSelectInvalidReindexArtifacts lists INVALID btree indexes on node and edge partitions that were left behind by an
// interrupted REINDEX CONCURRENTLY. PostgreSQL names the replacement index <name>_ccnew[N] and, once swapped, the
// original <name>_ccold[N]; either may remain INVALID if the rebuild fails or is cancelled.
const sqlSelectInvalidReindexArtifacts = `select n.nspname, i.relname, pg_relation_size(i.oid)
from pg_inherits inh
         join pg_index x on x.indrelid = inh.inhrelid
         join pg_class i on i.oid = x.indexrelid
         join pg_namespace n on n.oid = i.relnamespace
         join pg_am a on a.oid = i.relam
where inh.inhparent in ('node'::regclass, 'edge'::regclass)
  and a.amname = 'btree'
  and not x.indisvalid
  and i.relname ~ '_cc(new|old)[0-9]*$'
order by n.nspname, i.relname`

type optimizeStorageConn interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type optimizeStoragePhase func(ctx context.Context, conn optimizeStorageConn) error

// optimizeStoragePhases run in order. Each phase continues past individual failures; their errors are joined into the
// result. Cancellation stops the pass before the next phase or index.
var optimizeStoragePhases = []optimizeStoragePhase{
	dropInvalidReindexArtifacts,
	vacuumGraphTables,
}

type graphIndex struct {
	schema    string
	name      string
	sizeBytes int64
}

func (s graphIndex) identifier() string {
	return pgx.Identifier{s.schema, s.name}.Sanitize()
}

func optimizeStorage(ctx context.Context, conn optimizeStorageConn) error {
	var errs []error

	for _, phase := range optimizeStoragePhases {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}

		if err := phase(ctx, conn); err != nil {
			errs = append(errs, err)

			if ctx.Err() != nil {
				break
			}
		}
	}

	if err := ctx.Err(); err != nil && !errors.Is(errors.Join(errs...), err) {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

func listGraphIndexes(ctx context.Context, conn optimizeStorageConn, sql string) ([]graphIndex, error) {
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexes []graphIndex
	for rows.Next() {
		var index graphIndex
		if err := rows.Scan(&index.schema, &index.name, &index.sizeBytes); err != nil {
			return nil, err
		}

		indexes = append(indexes, index)
	}

	return indexes, rows.Err()
}

// execIndexStatements runs the statement produced by stmtFor for each index in order. Failures are logged and
// collected; cancellation stops further statements.
func execIndexStatements(ctx context.Context, conn optimizeStorageConn, operation string, indexes []graphIndex, stmtFor func(graphIndex) string) error {
	var errs []error

	for _, index := range indexes {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}

		var (
			stmt    = stmtFor(index)
			started = time.Now()
			attrs   = []any{
				slog.String("operation", operation),
				slog.String("index", index.identifier()),
				slog.Int64("size_bytes", index.sizeBytes),
			}
		)

		if _, err := conn.Exec(ctx, stmt, pgx.QueryExecModeSimpleProtocol); err != nil {
			slog.WarnContext(ctx, "PostgreSQL index maintenance statement failed",
				append(attrs, slog.Duration("elapsed", time.Since(started)), slog.String("error", err.Error()))...,
			)
			errs = append(errs, fmt.Errorf("%s: %w", stmt, err))
			continue
		}

		slog.InfoContext(ctx, "PostgreSQL index maintenance statement complete",
			append(attrs, slog.Duration("elapsed", time.Since(started)))...,
		)
	}

	return errors.Join(errs...)
}

func dropInvalidReindexArtifacts(ctx context.Context, conn optimizeStorageConn) error {
	indexes, err := listGraphIndexes(ctx, conn, sqlSelectInvalidReindexArtifacts)
	if err != nil {
		return fmt.Errorf("list invalid reindex artifacts: %w", err)
	}

	slog.InfoContext(ctx, "Dropping invalid PostgreSQL reindex artifacts", slog.Int("count", len(indexes)))

	return execIndexStatements(ctx, conn, "drop", indexes, func(index graphIndex) string {
		return "DROP INDEX CONCURRENTLY IF EXISTS " + index.identifier()
	})
}

func vacuumGraphTables(ctx context.Context, conn optimizeStorageConn) error {
	targets := []string{"node", "edge"}

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
