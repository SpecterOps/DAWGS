package v2

import (
	"context"

	"github.com/specterops/dawgs/cypher/models/cypher"
)

type Option int

const (
	SessionOptionReadOnly  Option = 0
	SessionOptionReadWrite Option = 1
)

var (
	NoParameters map[string]any = nil
)

type Driver interface {
	First(ctx context.Context, query *cypher.RegularQuery, parameters map[string]any) Result
	Execute(ctx context.Context, query *cypher.RegularQuery, parameters map[string]any) Result
}

type DriverLogic func(ctx context.Context, driver Driver) error

type Database interface {
	Session(ctx context.Context, driverLogic DriverLogic, options ...Option) error
	Transaction(ctx context.Context, driverLogic DriverLogic, options ...Option) error
	Close() error
}

type Result interface {
	HasNext(ctx context.Context) bool
	Scan(scanTargets ...any) error
	Error() error
	Close(ctx context.Context) error
}
