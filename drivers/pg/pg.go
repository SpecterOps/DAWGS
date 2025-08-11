package pg

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/bloodhound/src/config"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/graph"
)

const (
	DriverName = "pg"

	// defaultBatchWriteSize is currently set to 2k. This is meant to strike a balance between the cost of thousands
	// of round-trips against the cost of locking tables for too long.
	defaultBatchWriteSize = 2_000

	poolInitConnectionTimeout = time.Second * 10
)

func afterPooledConnectionEstablished(ctx context.Context, conn *pgx.Conn) error {
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

func afterPooledConnectionRelease(conn *pgx.Conn) bool {
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

func NewPool(cfg config.DatabaseConfiguration) (*pgxpool.Pool, error) {

	poolCtx, done := context.WithTimeout(context.Background(), poolInitConnectionTimeout)
	defer done()

	poolCfg, err := pgxpool.ParseConfig(cfg.PostgreSQLConnectionString())
	if err != nil {
		return nil, err
	}

	// TODO: Min and Max connections for the pool should be configurable
	poolCfg.MinConns = 5
	poolCfg.MaxConns = 50

	// Bind functions to the AfterConnect and AfterRelease hooks to ensure that composite type registration occurs.
	// Without composite type registration, the pgx connection type will not be able to marshal PG OIDs to their
	// respective Golang structs.
	poolCfg.AfterConnect = afterPooledConnectionEstablished
	poolCfg.AfterRelease = afterPooledConnectionRelease

	poolCfg.BeforeConnect = func(ctx context.Context, poolCfg *pgx.ConnConfig) error {
		slog.Info(fmt.Sprint("RDS credentional beforeConnect(), creating new IAM credentials"))
		refreshConnectionString := cfg.PostgreSQLConnectionString()
		newPoolCfg, err := pgxpool.ParseConfig(refreshConnectionString)
		if err != nil {
			return err
		}
		poolCfg.Password = newPoolCfg.ConnConfig.Password
		return nil
	}

	pool, err := pgxpool.NewWithConfig(poolCtx, poolCfg)
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func init() {
	dawgs.Register(DriverName, func(ctx context.Context, cfg dawgs.Config) (graph.Database, error) {
		return NewDriver(cfg.Pool), nil
	})
}
