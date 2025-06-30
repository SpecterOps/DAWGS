package v2

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg/v2/model"
	"github.com/specterops/dawgs/graph"
	v2 "github.com/specterops/dawgs/v2"
)

type queryResult struct {
	rows      pgx.Rows
	mapper    graph.ValueMapper
	values    []any
	err       error
	takeFirst bool
}

func newQueryResult(mapper graph.ValueMapper, rows pgx.Rows, takeFirst bool) v2.Result {
	return &queryResult{
		mapper:    mapper,
		rows:      rows,
		takeFirst: takeFirst,
	}
}

func (s *queryResult) HasNext(ctx context.Context) bool {
	if s.err != nil {
		return false
	}

	if s.rows.Next() {
		if values, err := s.rows.Values(); err != nil {
			s.err = err
		} else {
			s.values = values

			if s.takeFirst {
				s.err = s.Close(ctx)
			}

			return true
		}
	}

	return false
}

func (s *queryResult) Scan(scanTargets ...any) error {
	if s.err != nil {
		return s.err
	}

	if s.values == nil {
		return fmt.Errorf("no results to scan to; call HasNext()")
	}

	if len(scanTargets) != len(s.values) {
		return fmt.Errorf("expected to scan %d values but received %d to map to", len(s.values), len(scanTargets))
	}

	for idx, nextTarget := range scanTargets {
		nextValue := s.values[idx]

		if !s.mapper.Map(nextValue, nextTarget) {
			return fmt.Errorf("unable to scan type %T into target type %T", nextValue, nextTarget)
		}
	}

	return nil
}

func (s *queryResult) Error() error {
	return s.err
}

func (s *queryResult) Close(ctx context.Context) error {
	s.rows.Close()
	return nil
}

type internalDriver interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type dawgsDriver struct {
	internalConn       internalDriver
	queryExecMode      pgx.QueryExecMode
	queryResultFormats pgx.QueryResultFormats
	schemaManager      *SchemaManager
	targetGraph        *v2.Graph
}

func newInternalDriver(internalConn internalDriver, schemaManager *SchemaManager) v2.Driver {
	return &dawgsDriver{
		internalConn:       internalConn,
		queryExecMode:      pgx.QueryExecModeCacheStatement,
		queryResultFormats: pgx.QueryResultFormats{pgx.BinaryFormatCode},
		schemaManager:      schemaManager,
	}
}

func (s *dawgsDriver) getTargetGraph(ctx context.Context) (model.Graph, error) {
	var targetGraph v2.Graph

	if s.targetGraph != nil {
		targetGraph = *s.targetGraph
	} else {
		if defaultGraph, hasDefaultGraph := s.schemaManager.DefaultGraph(); !hasDefaultGraph {
			return model.Graph{}, fmt.Errorf("no graph target set for operation")
		} else {
			targetGraph = defaultGraph
		}
	}

	return s.schemaManager.AssertGraph(ctx, targetGraph)
}

func (s *dawgsDriver) queryArgs(parameters map[string]any) []any {
	queryArgs := []any{s.queryExecMode, s.queryResultFormats}

	if parameters != nil && len(parameters) > 0 {
		queryArgs = append(queryArgs, pgx.NamedArgs(parameters))
	}

	return queryArgs
}

func (s *dawgsDriver) executeSQL(ctx context.Context, sqlQuery string, parameters map[string]any, takeFirst bool) v2.Result {
	if internalResult, err := s.internalConn.Query(ctx, sqlQuery, s.queryArgs(parameters)...); err != nil {
		return v2.NewErrorResult(err)
	} else {
		return newQueryResult(newValueMapper(ctx, s.schemaManager), internalResult, takeFirst)
	}
}

func (s *dawgsDriver) executeCypher(ctx context.Context, query *cypher.RegularQuery, parameters map[string]any, takeFirst bool) v2.Result {
	if translated, err := translate.Translate(ctx, query, s.schemaManager, parameters); err != nil {
		return v2.NewErrorResult(err)
	} else if sqlQuery, err := translate.Translated(translated); err != nil {
		return v2.NewErrorResult(err)
	} else {
		return s.executeSQL(ctx, sqlQuery, parameters, takeFirst)
	}
}

func (s *dawgsDriver) First(ctx context.Context, query *cypher.RegularQuery, parameters map[string]any) v2.Result {
	return s.executeCypher(ctx, query, parameters, true)
}

func (s *dawgsDriver) CypherQuery(ctx context.Context, query *cypher.RegularQuery, parameters map[string]any) v2.Result {
	return s.executeCypher(ctx, query, parameters, false)
}

func (s *dawgsDriver) WithGraph(targetGraph v2.Graph) v2.Driver {
	s.targetGraph = &targetGraph
	return s
}

func (s *dawgsDriver) CreateNode(ctx context.Context, node *graph.Node) error {
	if targetGraph, err := s.getTargetGraph(ctx); err != nil {
		return err
	} else if kindIDSlice, err := s.schemaManager.AssertKinds(ctx, node.Kinds); err != nil {
		return err
	} else if propertiesJSONB, err := pgsql.PropertiesToJSONB(node.Properties); err != nil {
		return err
	} else {
		result := s.executeSQL(ctx, createNodeStatement, map[string]any{
			"graph_id":   targetGraph.ID,
			"kind_ids":   kindIDSlice,
			"properties": propertiesJSONB,
		}, false)

		defer result.Close(ctx)
		return result.Error()
	}
}
