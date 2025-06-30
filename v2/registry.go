package v2

import (
	"context"
	"errors"

	"github.com/specterops/dawgs/util/size"
)

var (
	ErrDriverMissing = errors.New("driver missing")
)

type DriverConstructor func(ctx context.Context, cfg Config) (Database, error)

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

func Open(ctx context.Context, driverName string, config Config) (Database, error) {
	if driverConstructor, hasDriver := availableDrivers[driverName]; !hasDriver {
		return nil, ErrDriverMissing
	} else {
		return driverConstructor(ctx, config)
	}
}
