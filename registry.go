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

type DriverConstructor func(ctx context.Context, cfg Config) (database.Instance, error)

var availableDrivers = map[string]DriverConstructor{}

func Register(driverName string, constructor DriverConstructor) {
	availableDrivers[driverName] = constructor
}

type Config struct {
	GraphQueryMemoryLimit size.Size
	ConnectionString      string

	// DriverConfig holds driver-specific configuration data that will be passed to the driver constructor. The type
	// and structure depend on the specific driver.
	DriverConfig any
}

func Open(ctx context.Context, driverName string, config Config) (database.Instance, error) {
	if driverConstructor, hasDriver := availableDrivers[driverName]; !hasDriver {
		return nil, ErrDriverMissing
	} else {
		return driverConstructor(ctx, config)
	}
}

func OpenV1(ctx context.Context, driverName string, config Config) (v1compat.Database, error) {
	if driver, err := Open(ctx, driverName, config); err != nil {
		return nil, err
	} else {
		return v1compat.V1Wrapper(driver), nil
	}
}
