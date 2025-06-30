package neo4j

import (
	"context"
	"errors"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/cypher/format"
	"github.com/specterops/dawgs/database"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

var (
	resultValueMapper = newValueMapper()
)

type sessionResult struct {
	result     neo4j.ResultWithContext
	nextRecord *neo4j.Record
	mapper     graph.ValueMapper
	err        error
}

func (s *sessionResult) Keys() []string {
	if s.nextRecord == nil {
		return nil
	}

	return s.nextRecord.Keys
}

func (s *sessionResult) Values() []any {
	if s.nextRecord == nil {
		return nil
	}

	return s.nextRecord.Values
}

func newResult(result neo4j.ResultWithContext, err error) database.Result {
	return &sessionResult{
		result: result,
		mapper: resultValueMapper,
		err:    err,
	}
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

		if !s.mapper.Map(nextValue, nextTarget) {
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

func (s *dawgsDriver) Mapper() graph.ValueMapper {
	return resultValueMapper
}

func (s *dawgsDriver) InsertNode(ctx context.Context, node *graph.Node) error {
	return errors.New("unsupported")
}

func (s *dawgsDriver) InsertRelationship(ctx context.Context, startMatchProperty, endMatchProperty string, startMatchValue, endMatchValue any, kind graph.Kind, properties *graph.Properties) error {
	return errors.New("unsupported")
}

func (s *dawgsDriver) UpsertNode(ctx context.Context, node *graph.Node) error {
	return errors.New("unsupported")
}

func (s *dawgsDriver) UpsertRelationship(ctx context.Context, startMatchProperty, endMatchProperty string, startMatchValue, endMatchValue any, kind graph.Kind, properties *graph.Properties) error {
	return errors.New("unsupported")
}

func (s *dawgsDriver) CreateNode(ctx context.Context, node *graph.Node) (graph.ID, error) {
	if builtQuery, err := query.New().Create(
		query.Node().NodePattern(node.Kinds, node.Properties.MapOrEmpty()),
	).Return(
		query.Node().ID(),
	).Build(); err != nil {
		return 0, err
	} else {
		var (
			newEntityID graph.ID
			result      = s.Exec(ctx, builtQuery.Query, builtQuery.Parameters)
		)

		defer result.Close(ctx)

		if !result.HasNext(ctx) {
			return 0, graph.ErrNoResultsFound
		}

		if err := result.Scan(&newEntityID); err != nil {
			return 0, err
		}

		return newEntityID, result.Error()
	}
}

func (s *dawgsDriver) CreateRelationship(ctx context.Context, relationship *graph.Relationship) (graph.ID, error) {
	if builtQuery, err := query.New().Where(
		query.And(
			query.Start().ID().Equals(relationship.StartID),
			query.End().ID().Equals(relationship.EndID),
		),
	).Create(
		query.Relationship().RelationshipPattern(relationship.Kind, relationship.Properties.MapOrEmpty(), graph.DirectionOutbound),
	).Return(
		query.Relationship().ID(),
	).Build(); err != nil {
		return 0, err
	} else {
		var (
			newEntityID graph.ID
			result      = s.Exec(ctx, builtQuery.Query, builtQuery.Parameters)
		)

		defer result.Close(ctx)

		if !result.HasNext(ctx) {
			return 0, graph.ErrNoResultsFound
		}

		if err := result.Scan(&newEntityID); err != nil {
			return 0, err
		}

		return newEntityID, result.Error()
	}
}

func newInternalDriver(internalDriver neo4jDriver) *dawgsDriver {
	return &dawgsDriver{
		internalDriver: internalDriver,
	}
}

func (s *dawgsDriver) WithGraph(target database.Graph) database.Driver {
	// NOOP for now
	return s
}

func (s *dawgsDriver) exec(ctx context.Context, cypherQuery string, parameters map[string]any) database.Result {
	internalResult, err := s.internalDriver.Run(ctx, cypherQuery, parameters)
	return newResult(internalResult, err)
}

func (s *dawgsDriver) Exec(ctx context.Context, query *cypher.RegularQuery, parameters map[string]any) database.Result {
	if cypherQuery, err := format.RegularQuery(query, false); err != nil {
		return database.NewErrorResult(err)
	} else {
		return s.exec(ctx, cypherQuery, parameters)
	}
}

func (s *dawgsDriver) Explain(ctx context.Context, query *cypher.RegularQuery, parameters map[string]any) database.Result {
	if cypherQuery, err := format.RegularQuery(query, false); err != nil {
		return database.NewErrorResult(err)
	} else {
		return s.exec(ctx, "explain "+cypherQuery, parameters)
	}
}

func (s *dawgsDriver) Profile(ctx context.Context, query *cypher.RegularQuery, parameters map[string]any) database.Result {
	if cypherQuery, err := format.RegularQuery(query, false); err != nil {
		return database.NewErrorResult(err)
	} else {
		return s.exec(ctx, "profile "+cypherQuery, parameters)
	}
}
