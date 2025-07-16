package database

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

type Result interface {
	HasNext(ctx context.Context) bool
	Scan(scanTargets ...any) error
	Error() error
	Close(ctx context.Context) error
}

type Driver interface {
	WithGraph(target Graph) Driver

	CreateNode(ctx context.Context, node *graph.Node) error

	Exec(ctx context.Context, query *cypher.RegularQuery, parameters map[string]any) Result
	Explain(ctx context.Context, query *cypher.RegularQuery, parameters map[string]any) Result
	Profile(ctx context.Context, query *cypher.RegularQuery, parameters map[string]any) Result
}

type QueryLogic func(ctx context.Context, driver Driver) error

type Instance interface {
	AssertSchema(ctx context.Context, schema Schema) error
	Session(ctx context.Context, driverLogic QueryLogic, options ...Option) error
	Transaction(ctx context.Context, driverLogic QueryLogic, options ...Option) error
	Close(ctx context.Context) error

	// FetchKinds retrieves the complete list of kinds available to the database.
	FetchKinds(ctx context.Context) (graph.Kinds, error)
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
