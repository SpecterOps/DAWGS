package dawgs

import (
	"context"
	"errors"

	"github.com/specterops/dawgs/database"
	"github.com/specterops/dawgs/database/v1compat"
	"github.com/specterops/dawgs/util/size"
)

var (
	ErrDriverMissing = errors.New("driver missing")
)

// DriverConstructor describes a function that takes a context and a dawgs configuration struct and returns either
// a valid `database.Instance` reference or the associated error that prevented instantiation.
type DriverConstructor func(ctx context.Context, cfg Config) (database.Instance, error)

var availableDrivers = map[string]DriverConstructor{}

// Register registers a dawgs driver under the given driverName
func Register(driverName string, constructor DriverConstructor) {
	availableDrivers[driverName] = constructor
}

// Config is the basic configuration struct for a dawgs connection
type Config struct {
	GraphQueryMemoryLimit size.Size
	ConnectionString      string

	// DriverConfig holds driver-specific configuration data that will be passed to the driver constructor. The type
	// and structure depend on the specific driver.
	DriverConfig any
}

// Open creates a new dawgs graph database instance. This function expects the driver name, often imported to ensure
// that registration logic occurs.
func Open(ctx context.Context, driverName string, config Config) (database.Instance, error) {
	if driverConstructor, hasDriver := availableDrivers[driverName]; !hasDriver {
		return nil, ErrDriverMissing
	} else {
		return driverConstructor(ctx, config)
	}
}

// OpenV1 creates a new dawgs graph database instance but with a dawgs version 1 compatible interface.
func OpenV1(ctx context.Context, driverName string, config Config) (v1compat.Database, error) {
	if driver, err := Open(ctx, driverName, config); err != nil {
		return nil, err
	} else {
		return v1compat.V1Wrapper(driver), nil
	}
}
