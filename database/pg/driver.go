package pg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/database"
	"github.com/specterops/dawgs/database/pg/model"
	"github.com/specterops/dawgs/database/v1compat"
	"github.com/specterops/dawgs/graph"
)

type queryResult struct {
	rows   pgx.Rows
	mapper graph.ValueMapper
	keys   []string
	values []any
}

func newQueryResult(mapper graph.ValueMapper, rows pgx.Rows) database.Result {
	return &queryResult{
		mapper: mapper,
		rows:   rows,
	}
}

func (s *queryResult) HasNext(ctx context.Context) bool {
	if s.rows.Next() {
		fields := s.rows.FieldDescriptions()
		s.keys = make([]string, len(fields))

		for idx, desc := range fields {
			s.keys[idx] = desc.Name
		}

		if values, err := s.rows.Values(); err != nil {
			return false
		} else {
			s.values = values
			return true
		}
	}

	return false
}

func (s *queryResult) Keys() []string {
	return s.keys
}

func (s *queryResult) Values() []any {
	return s.values
}

func (s *queryResult) Scan(scanTargets ...any) error {
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
	return s.rows.Err()
}

func (s *queryResult) Close(ctx context.Context) error {
	s.rows.Close()
	return nil
}

type internalDriver interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type translatedQuery struct {
	SQL        string
	Parameters map[string]any
}

type dawgsDriver struct {
	internalConn       internalDriver
	queryExecMode      pgx.QueryExecMode
	queryResultFormats pgx.QueryResultFormats
	schemaManager      *SchemaManager
	targetGraph        *database.Graph
}

func (s *dawgsDriver) Mapper() graph.ValueMapper {
	return newValueMapper(context.TODO(), s.schemaManager)
}

func newInternalDriver(internalConn internalDriver, schemaManager *SchemaManager) v1compat.BackwardCompatibleDriver {
	return &dawgsDriver{
		internalConn:       internalConn,
		queryExecMode:      pgx.QueryExecModeCacheStatement,
		queryResultFormats: pgx.QueryResultFormats{pgx.BinaryFormatCode},
		schemaManager:      schemaManager,
	}
}

func (s *dawgsDriver) getTargetGraph(ctx context.Context) (model.Graph, error) {
	var targetGraph database.Graph

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

	if len(parameters) > 0 {
		queryArgs = append(queryArgs, pgx.NamedArgs(parameters))
	}

	return queryArgs
}

func (s *dawgsDriver) executeTranslated(ctx context.Context, query translatedQuery) database.Result {
	if internalResult, err := s.internalConn.Query(ctx, query.SQL, s.queryArgs(query.Parameters)...); err != nil {
		return database.NewErrorResult(err)
	} else {
		return newQueryResult(newValueMapper(ctx, s.schemaManager), internalResult)
	}
}

func (s *dawgsDriver) translateCypherToPGSQL(ctx context.Context, query *cypher.RegularQuery, parameters map[string]any) (translatedQuery, error) {
	if translated, err := translate.Translate(ctx, query, s.schemaManager, parameters); err != nil {
		return translatedQuery{}, err
	} else if sqlQuery, err := translate.Translated(translated); err != nil {
		return translatedQuery{}, err
	} else {
		return translatedQuery{
			SQL:        sqlQuery,
			Parameters: parameters,
		}, nil
	}
}

func (s *dawgsDriver) Exec(ctx context.Context, query *cypher.RegularQuery, parameters map[string]any) database.Result {
	if translated, err := s.translateCypherToPGSQL(ctx, query, parameters); err != nil {
		return database.NewErrorResult(err)
	} else {
		return s.executeTranslated(ctx, translated)
	}
}

func (s *dawgsDriver) Explain(ctx context.Context, query *cypher.RegularQuery, parameters map[string]any) database.Result {
	if translated, err := s.translateCypherToPGSQL(ctx, query, parameters); err != nil {
		return database.NewErrorResult(err)
	} else {
		translated.SQL = "explain " + translated.SQL
		return s.executeTranslated(ctx, translated)
	}
}

func (s *dawgsDriver) Profile(ctx context.Context, query *cypher.RegularQuery, parameters map[string]any) database.Result {
	if translated, err := s.translateCypherToPGSQL(ctx, query, parameters); err != nil {
		return database.NewErrorResult(err)
	} else {
		translated.SQL = "explain (verbose, analyze, costs, buffers, format json) " + translated.SQL
		return s.executeTranslated(ctx, translated)
	}
}

func (s *dawgsDriver) WithGraph(targetGraph database.Graph) database.Driver {
	s.targetGraph = &targetGraph
	return s
}

func (s *dawgsDriver) CreateRelationship(ctx context.Context, relationship *graph.Relationship) (graph.ID, error) {
	if targetGraph, err := s.getTargetGraph(ctx); err != nil {
		return 0, err
	} else if kindIDSlice, err := s.schemaManager.AssertKinds(ctx, graph.Kinds{relationship.Kind}); err != nil {
		return 0, err
	} else if propertiesJSONB, err := pgsql.PropertiesToJSONB(relationship.Properties); err != nil {
		return 0, err
	} else {
		var (
			newEdgeID graph.ID
			result    = s.executeTranslated(ctx, translatedQuery{
				SQL: createEdgeStatement,
				Parameters: map[string]any{
					"graph_id":   targetGraph.ID,
					"start_id":   relationship.StartID,
					"end_id":     relationship.EndID,
					"kind_id":    kindIDSlice[0],
					"properties": propertiesJSONB,
				},
			})
		)

		defer result.Close(ctx)

		if !result.HasNext(ctx) {
			return 0, graph.ErrNoResultsFound
		}

		if err := result.Scan(&newEdgeID); err != nil {
			return 0, err
		}

		return newEdgeID, result.Error()
	}
}

func (s *dawgsDriver) InsertNode(ctx context.Context, node *graph.Node) error {
	if targetGraph, err := s.getTargetGraph(ctx); err != nil {
		return err
	} else if kindIDSlice, err := s.schemaManager.AssertKinds(ctx, node.Kinds); err != nil {
		return err
	} else if propertiesJSONB, err := pgsql.PropertiesToJSONB(node.Properties); err != nil {
		return err
	} else {
		result := s.executeTranslated(ctx, translatedQuery{
			SQL: formatNodeInsertSQL(targetGraph.ID),
			Parameters: map[string]any{
				"graph_id":   targetGraph.ID,
				"kind_ids":   kindIDSlice,
				"properties": propertiesJSONB,
			},
		})

		defer result.Close(ctx)
		return result.Error()
	}
}

func (s *dawgsDriver) InsertRelationship(ctx context.Context, startMatchProperty, endMatchProperty string, startMatchValue, endMatchValue any, kind graph.Kind, properties *graph.Properties) error {
	if targetGraph, err := s.getTargetGraph(ctx); err != nil {
		return err
	} else if kindID, err := s.schemaManager.AssertKind(ctx, kind); err != nil {
		return err
	} else if propertiesJSONB, err := pgsql.PropertiesToJSONB(properties); err != nil {
		return err
	} else {
		result := s.executeTranslated(ctx, translatedQuery{
			SQL: formatEdgeInsertSQL(targetGraph.ID, startMatchProperty, endMatchProperty),
			Parameters: map[string]any{
				"start_match_value": startMatchValue,
				"end_match_value":   endMatchValue,
				"graph_id":          targetGraph.ID,
				"kind_id":           kindID,
				"properties":        propertiesJSONB,
			},
		})

		defer result.Close(ctx)
		return result.Error()
	}
}

func (s *dawgsDriver) UpsertNode(ctx context.Context, node *graph.Node) error {
	if targetGraph, err := s.getTargetGraph(ctx); err != nil {
		return err
	} else if kindIDSlice, err := s.schemaManager.AssertKinds(ctx, node.Kinds); err != nil {
		return err
	} else if propertiesJSONB, err := pgsql.PropertiesToJSONB(node.Properties); err != nil {
		return err
	} else {
		result := s.executeTranslated(ctx, translatedQuery{
			SQL: formatNodeUpsertSQL(targetGraph.ID),
			Parameters: map[string]any{
				"graph_id":   targetGraph.ID,
				"kind_ids":   kindIDSlice,
				"properties": propertiesJSONB,
			},
		})

		defer result.Close(ctx)
		return result.Error()
	}
}

func (s *dawgsDriver) UpsertRelationship(ctx context.Context, startMatchProperty, endMatchProperty string, startMatchValue, endMatchValue any, kind graph.Kind, properties *graph.Properties) error {
	if targetGraph, err := s.getTargetGraph(ctx); err != nil {
		return err
	} else if kindID, err := s.schemaManager.AssertKind(ctx, kind); err != nil {
		return err
	} else if propertiesJSONB, err := pgsql.PropertiesToJSONB(properties); err != nil {
		return err
	} else {
		result := s.executeTranslated(ctx, translatedQuery{
			SQL: formatEdgeUpsertSQL(targetGraph.ID, startMatchProperty, endMatchProperty),
			Parameters: map[string]any{
				"start_match_value": startMatchValue,
				"end_match_value":   endMatchValue,
				"graph_id":          targetGraph.ID,
				"kind_id":           kindID,
				"properties":        propertiesJSONB,
			},
		})

		defer result.Close(ctx)
		return result.Error()
	}
}

func (s *dawgsDriver) CreateNode(ctx context.Context, node *graph.Node) (graph.ID, error) {
	if targetGraph, err := s.getTargetGraph(ctx); err != nil {
		return 0, err
	} else if kindIDSlice, err := s.schemaManager.AssertKinds(ctx, node.Kinds); err != nil {
		return 0, err
	} else if propertiesJSONB, err := pgsql.PropertiesToJSONB(node.Properties); err != nil {
		return 0, err
	} else {
		var (
			newNodeID graph.ID
			result    = s.executeTranslated(ctx, translatedQuery{
				SQL: createNodeStatement,
				Parameters: map[string]any{
					"graph_id":   targetGraph.ID,
					"kind_ids":   kindIDSlice,
					"properties": propertiesJSONB,
				},
			})
		)

		defer result.Close(ctx)

		if !result.HasNext(ctx) {
			return 0, graph.ErrNoResultsFound
		}

		if err := result.Scan(&newNodeID); err != nil {
			return 0, err
		}

		return newNodeID, result.Error()
	}
}
