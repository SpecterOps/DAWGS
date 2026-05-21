package pg

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/graph"
)

const (
	DriverName = "pg"

	// defaultBatchWriteSize is currently set to 2k. This is meant to strike a balance between the cost of thousands
	// of round-trips against the cost of locking tables for too long.
	defaultBatchWriteSize = 2_000
)

func AfterPooledConnectionEstablished(ctx context.Context, conn *pgx.Conn) error {
	for _, dataType := range pgsql.CompositeTypes {
		if definition, err := conn.LoadType(ctx, dataType.String()); err != nil {
			if !StateObjectDoesNotExist.ErrorMatches(err) {
				return fmt.Errorf("failed to match composite type %s to database: %w", dataType, err)
			}
		} else {
			conn.TypeMap().RegisterType(definition)
		}
	}

	return nil
}

func AfterPooledConnectionRelease(conn *pgx.Conn) bool {
	for _, dataType := range pgsql.CompositeTypes {
		if _, hasType := conn.TypeMap().TypeForName(dataType.String()); !hasType {
			// This connection should be destroyed since it does not contain information regarding the schema's
			// composite types
			slog.Warn(fmt.Sprintf("Unable to find expected data type: %s. This database connection will not be pooled.", dataType))
			return false
		}
	}

	return true
}

func init() {
	dawgs.Register(DriverName, func(ctx context.Context, cfg dawgs.Config) (graph.Database, error) {
		return NewDriver(cfg.GraphQueryMemoryLimit, cfg.Pool), nil
	})
}
