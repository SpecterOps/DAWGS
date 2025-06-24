package v2

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/cypher/format"
	"github.com/specterops/dawgs/graph"
	v2 "github.com/specterops/dawgs/v2"
)

type firstResult struct {
	values []any
	mapper graph.ValueMapper
	err    error
}

func (s *firstResult) HasNext(ctx context.Context) bool {
	return s.err != nil && s.values != nil
}

func (s *firstResult) Scan(scanTargets ...any) error {
	if s.err != nil {
		return s.err
	}

	if len(scanTargets) != len(s.values) {
		return fmt.Errorf("expected to scan %d values but received %d to map to", len(s.values), len(scanTargets))
	}

	for idx, nextTarget := range scanTargets {
		nextValue := s.values[idx]

		if !s.mapper.TryMap(nextValue, nextTarget) {
			return fmt.Errorf("unable to scan type %T into target type %T", nextValue, nextTarget)
		}
	}

	// Exhaust the result by assigning nil to s.values
	s.values = nil
	return nil
}

func (s *firstResult) Error() error {
	return s.err
}

func (s *firstResult) Close(ctx context.Context) error {
	return nil
}

func newFirstResult(ctx context.Context, result neo4j.ResultWithContext, err error) v2.Result {
	if record, err := result.Single(ctx); err != nil {
		return &firstResult{
			err: err,
		}
	} else {
		return &firstResult{
			values: record.Values,
			mapper: resultValueMapper,
			err:    err,
		}
	}
}

type sessionResult struct {
	result     neo4j.ResultWithContext
	nextRecord *neo4j.Record
	mapper     graph.ValueMapper
	err        error
}

var (
	resultValueMapper = newValueMapper()
)

func newResult(result neo4j.ResultWithContext, err error) v2.Result {
	return &sessionResult{
		result: result,
		mapper: resultValueMapper,
		err:    err,
	}
}

func newErrorResult(err error) v2.Result {
	return newResult(nil, err)
}

func (s *sessionResult) HasNext(ctx context.Context) bool {
	if s.err != nil {
		return false
	}

	hasNext := s.result.NextRecord(ctx, &s.nextRecord)

	if !hasNext {
		s.err = s.result.Err()
	}

	return hasNext
}

func (s *sessionResult) Scan(scanTargets ...any) error {
	if s.err != nil {
		return s.err
	}

	if len(scanTargets) != len(s.nextRecord.Values) {
		return fmt.Errorf("expected to scan %d values but received %d to map to", len(s.nextRecord.Values), len(scanTargets))
	}

	for idx, nextTarget := range scanTargets {
		nextValue := s.nextRecord.Values[idx]

		if !s.mapper.TryMap(nextValue, nextTarget) {
			return fmt.Errorf("unable to scan type %T into target type %T", nextValue, nextTarget)
		}
	}

	return nil
}

func (s *sessionResult) Close(ctx context.Context) error {
	if s.err != nil {
		return s.err
	}

	_, err := s.result.Consume(ctx)
	return err
}

func (s *sessionResult) Error() error {
	return s.err
}

type neo4jDriver interface {
	Run(ctx context.Context, cypher string, params map[string]any) (neo4j.ResultWithContext, error)
}

type sessionDriver struct {
	session neo4j.SessionWithContext
}

func (s *sessionDriver) Run(ctx context.Context, cypher string, params map[string]any) (neo4j.ResultWithContext, error) {
	return s.session.Run(ctx, cypher, params)
}

type dawgsDriver struct {
	internalDriver neo4jDriver
}

func newInternalDriver(internalDriver neo4jDriver) *dawgsDriver {
	return &dawgsDriver{
		internalDriver: internalDriver,
	}
}

func (s *dawgsDriver) First(ctx context.Context, query *cypher.RegularQuery, parameters map[string]any) v2.Result {
	if cypherQuery, err := format.RegularQuery(query, false); err != nil {
		return newErrorResult(err)
	} else {
		internalResult, err := s.internalDriver.Run(ctx, cypherQuery, parameters)
		return newFirstResult(ctx, internalResult, err)
	}
}

func (s *dawgsDriver) Execute(ctx context.Context, query *cypher.RegularQuery, parameters map[string]any) v2.Result {
	if cypherQuery, err := format.RegularQuery(query, false); err != nil {
		return newErrorResult(err)
	} else {
		internalResult, err := s.internalDriver.Run(ctx, cypherQuery, parameters)
		return newResult(internalResult, err)
	}
}
