package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

// liveQuery executes a mutable builder against its leased transaction and graph resolver.
type liveQuery struct {
	// ctx bounds builder preparation, execution, and cursor delivery.
	ctx context.Context

	// tx owns the connection and optional explicit PostgreSQL transaction.
	tx *transaction

	// graphIDResolver resolves the currently selected graph before compilation.
	graphIDResolver func() (int32, error)

	// queryBuilder accumulates criteria before each execution is prepared.
	queryBuilder *query.Builder

	// budgetedCompact records whether the current builder execution owns compact-search lifecycle rules.
	budgetedCompact bool
}

func newLiveQuery(ctx context.Context, tx *transaction, graphIDResolver func() (int32, error)) liveQuery {
	return liveQuery{
		ctx:             ctx,
		tx:              tx,
		graphIDResolver: graphIDResolver,
		queryBuilder:    query.NewBuilder(nil),
	}
}

// runRegularQuery executes one prepared builder query with snapshot ownership when compact search qualifies.
func (s *liveQuery) runRegularQuery(allShortestPaths bool) (result graph.Result) {
	s.budgetedCompact = false
	if s.tx.compactSearch != nil {
		return graph.NewErrorResult(fmt.Errorf("close the active compact-search result before executing another query"))
	}

	if regularQuery, err := s.queryBuilder.Build(allShortestPaths); err != nil {
		return graph.NewErrorResult(err)
	} else if prepared, err := prepareRegularQuery(regularQuery); err != nil {
		return graph.NewErrorResult(err)
	} else {
		compactSearch := false
		if allShortestPaths && OptimizedTranslationEnabled() {
			if translationQuery, err := prepared.translationQuery(); err != nil {
				return graph.NewErrorResult(err)
			} else {
				compactSearch = translate.UsesBudgetedAllShortestPaths(translationQuery)
			}

			if s.tx.tx != nil && !stableSnapshotIsolation(s.tx.isolation) {
				compactSearch = false
			}
		}

		if compactSearch {
			if scope, err := s.tx.beginCompactSearchQuery(); err != nil {
				return graph.NewErrorResult(err)
			} else if scope != nil {
				defer func() { result = scope.wrapResult(result) }()
			}
			s.budgetedCompact = true
		}

		if graphID, err := s.graphIDResolver(); err != nil {
			return graph.NewErrorResult(err)
		} else if err := s.tx.beginTraversalBudgetForQuery(prepared.query, compactSearch); err != nil {
			return graph.NewErrorResult(err)
		} else if err := s.tx.serviceKinds(); err != nil {
			return graph.NewErrorResult(err)
		} else if compiled, err := s.tx.schemaManager.compileWithMapperAndCompactSearch(s.ctx, prepared.source, prepared.parameters, graphID, &prepared.identity, prepared.translationQuery, s.tx.kindMapper, compactSearch); err != nil {
			return graph.NewErrorResult(err)
		} else {
			s.tx.invalidateTopologyRouteDecisions()
			return s.tx.raw(commentRegularQuery(prepared.commentSource, compiled.statement), compiled.bindings)
		}
	}
}

// Query applies final builder criteria and releases the delegated result before returning.
func (s *liveQuery) Query(delegate func(results graph.Result) error, finalCriteria ...graph.Criteria) error {
	s.queryBuilder.Apply(finalCriteria...)

	if result := s.runRegularQuery(false); result.Error() != nil {
		return result.Error()
	} else {
		defer result.Close()

		return delegate(result)
	}
}

// QueryAllShortestPaths drains final compact-search errors after the delegate releases its result.
func (s *liveQuery) QueryAllShortestPaths(delegate func(results graph.Result) error, finalCriteria ...graph.Criteria) error {
	s.queryBuilder.Apply(finalCriteria...)

	if result := s.runRegularQuery(true); result.Error() != nil {
		return result.Error()
	} else {
		defer result.Close()

		delegateError := delegate(result)
		if s.budgetedCompact {
			result.Close()
			return errors.Join(delegateError, result.Error())
		}

		return delegateError
	}
}

func (s *liveQuery) exec(finalCriteria ...graph.Criteria) error {
	return s.Query(func(results graph.Result) error {
		return results.Error()
	}, finalCriteria...)
}
