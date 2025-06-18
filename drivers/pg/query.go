package pg

import (
	"context"

	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

type liveQuery struct {
	ctx          context.Context
	tx           graph.Transaction
	kindMapper   KindMapper
	queryBuilder *query.Builder
}

func newLiveQuery(ctx context.Context, tx graph.Transaction, kindMapper KindMapper) liveQuery {
	return liveQuery{
		ctx:          ctx,
		tx:           tx,
		kindMapper:   kindMapper,
		queryBuilder: query.NewBuilder(nil),
	}
}

func (s *liveQuery) runRegularQuery(allShortestPaths bool) graph.Result {
	if regularQuery, err := s.queryBuilder.Build(allShortestPaths); err != nil {
		return graph.NewErrorResult(err)
	} else if translation, err := translate.FromCypher(s.ctx, regularQuery, s.kindMapper, false); err != nil {
		return graph.NewErrorResult(err)
	} else {
		return s.tx.Raw(translation.Statement, translation.Parameters)
	}
}

func (s *liveQuery) Query(delegate func(results graph.Result) error, finalCriteria ...graph.Criteria) error {
	s.queryBuilder.Apply(finalCriteria...)

	if result := s.runRegularQuery(false); result.Error() != nil {
		return result.Error()
	} else {
		defer result.Close()
		return delegate(result)
	}
}

func (s *liveQuery) QueryAllShortestPaths(delegate func(results graph.Result) error, finalCriteria ...graph.Criteria) error {
	s.queryBuilder.Apply(finalCriteria...)

	if result := s.runRegularQuery(true); result.Error() != nil {
		return result.Error()
	} else {
		defer result.Close()
		return delegate(result)
	}
}

func (s *liveQuery) exec(finalCriteria ...graph.Criteria) error {
	return s.Query(func(results graph.Result) error {
		return results.Error()
	}, finalCriteria...)
}
