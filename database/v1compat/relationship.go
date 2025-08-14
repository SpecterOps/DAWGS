package v1compat

import (
	"context"
	"fmt"

	"github.com/specterops/dawgs/database"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

type relationshipQuery struct {
	ctx     context.Context
	driver  database.Driver
	builder query.QueryBuilder
}

func newRelationshipQuery(ctx context.Context, driver database.Driver) RelationshipQuery {
	return &relationshipQuery{
		ctx:     ctx,
		driver:  driver,
		builder: query.New(),
	}
}

func (s relationshipQuery) exec() (Result, error) {
	if builtQuery, err := s.builder.Build(); err != nil {
		return nil, err
	} else {
		result := s.driver.Exec(s.ctx, builtQuery.Query, builtQuery.Parameters)
		return wrapResult(s.ctx, result, s.driver.Mapper()), nil
	}
}

func (s relationshipQuery) Filter(criteria Criteria) RelationshipQuery {
	s.builder.Where(criteria)
	return s
}

func (s relationshipQuery) Filterf(criteriaDelegate CriteriaProvider) RelationshipQuery {
	s.builder.Where(criteriaDelegate())
	return s
}

func (s relationshipQuery) Update(properties *graph.Properties) error {
	s.builder.Update(query.Relationship().SetProperties(properties.MapOrEmpty()))

	if result, err := s.exec(); err != nil {
		return err
	} else {
		result.Close()
		return nil
	}
}

func (s relationshipQuery) Delete() error {
	s.builder.Delete(query.Relationship())

	if result, err := s.exec(); err != nil {
		return err
	} else {
		result.Close()
		return nil
	}
}

func (s relationshipQuery) OrderBy(criteria ...Criteria) RelationshipQuery {
	s.builder.OrderBy(criteria...)
	return s
}

func (s relationshipQuery) Offset(skip int) RelationshipQuery {
	s.builder.Skip(skip)
	return s
}

func (s relationshipQuery) Limit(limit int) RelationshipQuery {
	s.builder.Limit(limit)
	return s
}

func (s relationshipQuery) Count() (int64, error) {
	s.builder.Return(query.Relationship().Count())

	if result, err := s.exec(); err != nil {
		return 0, err
	} else {
		defer result.Close()

		if result.Next() {
			var count int64

			if err := result.Scan(&count); err != nil {
				return 0, err
			}

			return count, result.Error()
		}

		if result.Error() != nil {
			return 0, result.Error()
		}

		return 0, ErrNoResultsFound
	}
}

func (s relationshipQuery) First() (*graph.Relationship, error) {
	s.builder.Return(query.Relationship()).Limit(1)

	if result, err := s.exec(); err != nil {
		return nil, err
	} else {
		defer result.Close()

		if result.Next() {
			var relationship graph.Relationship

			if err := result.Scan(&relationship); err != nil {
				return nil, err
			}

			return &relationship, result.Error()
		}

		if result.Error() != nil {
			return nil, result.Error()
		}

		return nil, ErrNoResultsFound
	}
}

func (s relationshipQuery) Query(delegate func(results Result) error, finalCriteria ...any) error {
	s.builder.Return(finalCriteria...)

	if result, err := s.exec(); err != nil {
		return err
	} else {
		defer result.Close()
		return delegate(result)
	}
}

func (s relationshipQuery) Fetch(delegate func(cursor Cursor[*graph.Relationship]) error) error {
	s.builder.Return(query.Relationship())

	if builtQuery, err := s.builder.Build(); err != nil {
		return err
	} else {
		resultIter := NewResultIterator(s.ctx, s.driver.Exec(s.ctx, builtQuery.Query, builtQuery.Parameters), func(result database.Result) (*graph.Relationship, error) {
			var (
				relationship graph.Relationship
				err          = result.Scan(&relationship)
			)

			return &relationship, err
		})

		defer resultIter.Close()
		return delegate(resultIter)
	}
}

func (s relationshipQuery) FetchDirection(direction graph.Direction, delegate func(cursor Cursor[DirectionalResult]) error) error {
	switch direction {
	case DirectionOutbound:
		s.builder.Return(query.Relationship(), query.Start())
	case DirectionInbound:
		s.builder.Return(query.Relationship(), query.End())
	default:
		return fmt.Errorf("unsupported direction: %v", direction)
	}

	if builtQuery, err := s.builder.Build(); err != nil {
		return err
	} else {
		resultIter := NewResultIterator(s.ctx, s.driver.Exec(s.ctx, builtQuery.Query, builtQuery.Parameters), func(result database.Result) (DirectionalResult, error) {
			var (
				relationship graph.Relationship
				node         graph.Node
			)

			if err := result.Scan(&relationship, &node); err != nil {
				return DirectionalResult{}, err
			}

			return DirectionalResult{
				Direction:    direction,
				Relationship: &relationship,
				Node:         &node,
			}, nil
		})

		defer resultIter.Close()
		return delegate(resultIter)
	}
}

func (s relationshipQuery) FetchIDs(delegate func(cursor Cursor[graph.ID]) error) error {
	s.builder.Return(query.Relationship().ID())

	if builtQuery, err := s.builder.Build(); err != nil {
		return err
	} else {
		resultIter := NewResultIterator(s.ctx, s.driver.Exec(s.ctx, builtQuery.Query, builtQuery.Parameters), func(result database.Result) (graph.ID, error) {
			var nodeID graph.ID
			return nodeID, result.Scan(&nodeID)
		})

		defer resultIter.Close()
		return delegate(resultIter)
	}
}

func (s relationshipQuery) FetchTriples(delegate func(cursor Cursor[RelationshipTripleResult]) error) error {
	s.builder.Return(query.Start().ID(), query.End().ID(), query.Relationship().ID())

	if builtQuery, err := s.builder.Build(); err != nil {
		return err
	} else {
		resultIter := NewResultIterator(s.ctx, s.driver.Exec(s.ctx, builtQuery.Query, builtQuery.Parameters), func(result database.Result) (RelationshipTripleResult, error) {
			var (
				startID graph.ID
				endID   graph.ID
				edgeID  graph.ID
				err     = result.Scan(&startID, &endID, &edgeID)
			)

			return RelationshipTripleResult{
				StartID: startID,
				EndID:   endID,
				ID:      edgeID,
			}, err
		})

		defer resultIter.Close()
		return delegate(resultIter)
	}
}

func (s relationshipQuery) FetchAllShortestPaths(delegate func(cursor Cursor[graph.Path]) error) error {
	s.builder.WithAllShortestPaths().Return(query.Path())

	if builtQuery, err := s.builder.Build(); err != nil {
		return err
	} else {
		resultIter := NewResultIterator(s.ctx, s.driver.Exec(s.ctx, builtQuery.Query, builtQuery.Parameters), func(result database.Result) (graph.Path, error) {
			var (
				path graph.Path
				err  = result.Scan(&path)
			)

			return path, err
		})

		defer resultIter.Close()
		return delegate(resultIter)
	}
}

func (s relationshipQuery) FetchKinds(delegate func(cursor Cursor[RelationshipKindsResult]) error) error {
	s.builder.Return(query.Start().ID(), query.End().ID(), query.Relationship().ID(), query.Relationship().Kind())

	if builtQuery, err := s.builder.Build(); err != nil {
		return err
	} else {
		resultIter := NewResultIterator(s.ctx, s.driver.Exec(s.ctx, builtQuery.Query, builtQuery.Parameters), func(result database.Result) (RelationshipKindsResult, error) {
			var (
				startID  graph.ID
				endID    graph.ID
				edgeID   graph.ID
				edgeKind graph.Kind
				err      = result.Scan(&startID, &endID, &edgeID, &edgeKind)
			)

			return RelationshipKindsResult{
				RelationshipTripleResult: RelationshipTripleResult{
					StartID: startID,
					EndID:   endID,
					ID:      endID,
				},
				Kind: edgeKind,
			}, err
		})

		defer resultIter.Close()
		return delegate(resultIter)
	}
}
