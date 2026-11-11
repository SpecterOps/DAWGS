package dawgs

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
)

var (
	// ErrDriverMissing indicates that Open has no constructor registered for the requested driver.
	ErrDriverMissing = errors.New("driver missing")
)

// DriverConstructor opens a database using the context and driver-specific configuration.
type DriverConstructor func(ctx context.Context, cfg Config) (graph.Database, error)

// availableDrivers holds constructors registered during driver initialization; registration is not synchronized.
var availableDrivers = map[string]DriverConstructor{}

// Register installs a constructor under driverName, replacing any previous registration.
// Call it during initialization before concurrent database opens.
func Register(driverName string, constructor DriverConstructor) {
	availableDrivers[driverName] = constructor
}

// Config supplies connection and memory settings to the selected driver constructor.
type Config struct {
	// GraphQueryMemoryLimit bounds in-memory query results for drivers that enforce it.
	GraphQueryMemoryLimit size.Size
	// ConnectionString supplies the backend URI when the driver creates its connection pool.
	ConnectionString string
	// Pool supplies an existing PostgreSQL pool when supported by the selected driver.
	Pool *pgxpool.Pool
}

// Open invokes the registered driver constructor and returns ErrDriverMissing for an unknown name.
func Open(ctx context.Context, driverName string, config Config) (graph.Database, error) {
	if driverConstructor, hasDriver := availableDrivers[driverName]; !hasDriver {
		return nil, ErrDriverMissing
	} else {
		return driverConstructor(ctx, config)
	}
}
