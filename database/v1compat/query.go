package v1compat

import (
	"context"
	"fmt"

	"github.com/specterops/dawgs/database"
	"github.com/specterops/dawgs/graph"
)

type Result interface {
	Next() bool
	Keys() []string
	Values() []any
	Mapper() graph.ValueMapper

	// Scan takes a list of target any and attempts to map the next row from the result to the targets. This function
	// is semantically equivalent to calling graph.ScanNextResult(...)
	//
	// This is Deprecated. Call the graph.ScanNextResult(...) function.
	Scan(targets ...any) error
	Error() error
	Close()
}

func ScanNextResult(result Result, targets ...any) error {
	var (
		nextTargetIdx = 0
		mapper        = result.Mapper()
	)

	for _, nextValue := range result.Values() {
		if !mapper.Map(nextValue, targets[nextTargetIdx]) {
			return fmt.Errorf("unable to marshal next value %T into target %T", nextValue, targets[nextTargetIdx])
		}

		nextTargetIdx++
	}

	return nil
}

type resultWrapper struct {
	result database.Result
	ctx    context.Context
	mapper graph.ValueMapper
}

func (s resultWrapper) Keys() []string {
	return s.result.Keys()
}

func (s resultWrapper) Next() bool {
	return s.result.HasNext(s.ctx)
}

func (s resultWrapper) Values() []any {
	return s.result.Values()
}

func (s resultWrapper) Mapper() graph.ValueMapper {
	return s.mapper
}

func (s resultWrapper) Scan(targets ...any) error {
	return s.result.Scan(targets...)
}

func (s resultWrapper) Error() error {
	return s.result.Error()
}

func (s resultWrapper) Close() {
	s.result.Close(s.ctx)
}

func wrapResult(ctx context.Context, result database.Result, mapper graph.ValueMapper) Result {
	return &resultWrapper{
		ctx:    ctx,
		result: result,
		mapper: mapper,
	}
}

type ErrorResult struct {
	err error
}

func (s ErrorResult) Keys() []string {
	return nil
}

func (s ErrorResult) Values() []any {
	return nil
}

func (s ErrorResult) Next() bool {
	return false
}

func (s ErrorResult) Mapper() graph.ValueMapper {
	return graph.ValueMapper{}
}

func (s ErrorResult) Scan(targets ...any) error {
	return s.err
}

func (s ErrorResult) Error() error {
	return s.err
}

func (s ErrorResult) Close() {
}

func NewErrorResult(err error) Result {
	return ErrorResult{
		err: err,
	}
}

// CriteriaProvider is a function delegate that returns criteria.
type CriteriaProvider func() Criteria

// NodeQuery is an interface that covers all supported node query combinations. The contract supports a fluent
// interface to make query specifications more succinct.
type NodeQuery interface {
	// Filter applies the given criteria to this query.
	Filter(criteria Criteria) NodeQuery

	// Filterf applies the given criteria provider function to this query.
	Filterf(criteriaDelegate CriteriaProvider) NodeQuery

	// Query completes the query and hands the raw result to the given delegate for unmarshalling
	Query(delegate func(results Result) error, finalCriteria ...any) error

	// Delete deletes any candidate nodes that match the query criteria
	Delete() error

	// Update updates all candidate nodes with the given properties
	Update(properties *graph.Properties) error

	// OrderBy sets the OrderBy clause of the NodeQuery.
	OrderBy(criteria ...Criteria) NodeQuery

	// Offset sets an offset for the result set of the query. Using this function will enforce order on the result set.
	Offset(skip int) NodeQuery

	// Limit sets a maximum number of results to collect from the database.
	Limit(skip int) NodeQuery

	// Count completes the query and returns a tuple containing the count of results that were addressed by the
	// database and any error encountered during execution.
	Count() (int64, error)

	// First completes the query and returns the result and any error encountered during execution.
	First() (*graph.Node, error)

	// Fetch completes the query and captures a cursor for iterating the result set. This cursor is passed to the given
	// delegate. Errors from the delegate are returned upwards as the error result of this call.
	Fetch(delegate func(cursor Cursor[*graph.Node]) error, finalCriteria ...Criteria) error

	// FetchIDs completes the query and captures a cursor for iterating the result set. This cursor is passed to the given
	// delegate. Errors from the delegate are returned upwards as the error result of this call.
	FetchIDs(delegate func(cursor Cursor[graph.ID]) error) error

	// FetchKinds returns the ID and Kinds of matched nodes and omits property fetching
	FetchKinds(func(cursor Cursor[KindsResult]) error) error
}

// RelationshipQuery is an interface that covers all supported relationship query combinations. The contract supports a
// fluent interface to make query specifications more succinct.
type RelationshipQuery interface {
	// Filter applies the given criteria to this query.
	Filter(criteria Criteria) RelationshipQuery

	// Filterf applies the given criteria provider function to this query.
	Filterf(criteriaDelegate CriteriaProvider) RelationshipQuery

	// Update replaces the properties of all candidate relationships that matches the query criteria with the
	// given properties
	Update(properties *graph.Properties) error

	// Delete deletes any candidate relationships that match the query criteria
	Delete() error

	// OrderBy sets the OrderBy clause of the RelationshipQuery.
	OrderBy(criteria ...Criteria) RelationshipQuery

	// Offset sets an offset for the result set of the query. Using this function will enforce order on the result set.
	Offset(skip int) RelationshipQuery

	// Limit sets a maximum number of results to collect from the database.
	Limit(skip int) RelationshipQuery

	// Count completes the query and returns a tuple containing the count of results that were addressed by the
	// database and any error encountered during execution.
	Count() (int64, error)

	// First completes the query and returns the result and any error encountered during execution.
	First() (*graph.Relationship, error)

	// Query completes the query and hands the raw result to the given delegate for unmarshalling
	Query(delegate func(results Result) error, finalCriteria ...any) error

	// Fetch completes the query and captures a cursor for iterating the result set. This cursor is passed to the given
	// delegate. Errors from the delegate are returned upwards as the error result of this call.
	Fetch(delegate func(cursor Cursor[*graph.Relationship]) error) error

	// FetchDirection completes the query and captures a cursor for iterating through the relationship related nodes
	// for the given path direction
	FetchDirection(direction graph.Direction, delegate func(cursor Cursor[DirectionalResult]) error) error

	// FetchIDs completes the query and captures a cursor for iterating the result set. This cursor is passed to the given
	// delegate. Errors from the delegate are returned upwards as the error result of this call.
	FetchIDs(delegate func(cursor Cursor[graph.ID]) error) error

	//
	FetchTriples(delegate func(cursor Cursor[RelationshipTripleResult]) error) error

	//
	FetchAllShortestPaths(delegate func(cursor Cursor[graph.Path]) error) error

	// FetchKinds returns the ID, Kind, Start ID and End ID of matched relationships and omits property fetching
	FetchKinds(delegate func(cursor Cursor[RelationshipKindsResult]) error) error
}
