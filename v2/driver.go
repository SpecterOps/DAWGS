package v2

import (
	"context"

	"github.com/specterops/dawgs/graph"

	"github.com/specterops/dawgs/cypher/models/cypher"
)

type Option int

const (
	OptionReadOnly  Option = 0
	OptionReadWrite Option = 1
)

var (
	NoParameters map[string]any = nil
)

type Driver interface {
	WithGraph(target Graph) Driver

	CypherQuery(ctx context.Context, query *cypher.RegularQuery, parameters map[string]any) Result
	CreateNode(ctx context.Context, node *graph.Node) error
}

type DriverLogic func(ctx context.Context, driver Driver) error

type Database interface {
	AssertSchema(ctx context.Context, schema Schema) error
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

type errorResult struct {
	err error
}

func (s errorResult) HasNext(ctx context.Context) bool {
	return false
}

func (s errorResult) Scan(scanTargets ...any) error {
	return s.err
}

func (s errorResult) Error() error {
	return s.err
}

func (s errorResult) Close(ctx context.Context) error {
	return nil
}

func NewErrorResult(err error) Result {
	return errorResult{
		err: err,
	}
}
