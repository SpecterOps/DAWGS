package pg

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type optimizeStorageConn interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

func optimizeStorage(ctx context.Context, conn optimizeStorageConn) error {
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
