// pool is reference for pool initialization and is used for tests and internal tools
package pool

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
)

const (
	poolInitConnectionTimeout = time.Second * 10
)

func NewPool(cfg DatabaseConfiguration) (*pgxpool.Pool, error) {
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
	poolCfg.AfterConnect = pg.AfterPooledConnectionEstablished
	poolCfg.AfterRelease = pg.AfterPooledConnectionRelease

	pool, err := pgxpool.NewWithConfig(poolCtx, poolCfg)
	if err != nil {
		return nil, err
	}

	return pool, nil
}
