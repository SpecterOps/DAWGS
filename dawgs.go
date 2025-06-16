package dawgs

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
)

var (
	ErrDriverMissing = errors.New("driver missing")
)

type DriverConstructor func(ctx context.Context, cfg Config) (graph.Database, error)

var availableDrivers = map[string]DriverConstructor{}

func Register(driverName string, constructor DriverConstructor) {
	availableDrivers[driverName] = constructor
}

type Config struct {
	GraphQueryMemoryLimit size.Size
	ConnectionString      string
	Pool                  *pgxpool.Pool
}

func Open(ctx context.Context, driverName string, config Config) (graph.Database, error) {
	if driverConstructor, hasDriver := availableDrivers[driverName]; !hasDriver {
		return nil, ErrDriverMissing
	} else {
		return driverConstructor(ctx, config)
	}
}
