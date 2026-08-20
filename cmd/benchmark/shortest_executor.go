package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/drivers/pg/model"
	"github.com/specterops/dawgs/graph"
)

// shortestExecutorBenchmarkDatabase forces one repository-qualified executor
// only at the benchmark boundary. Production driver routing remains governed
// by the signed traversal-policy manifest.
type shortestExecutorBenchmarkDatabase struct {
	graph.Database
	mapper        pg.KindMapper
	graph         model.Graph
	executor      optimize.ShortestPathExecutor
	planCacheMode string
	jitEnabled    bool
}

func newShortestExecutorBenchmarkDatabase(database graph.Database, mapper pg.KindMapper, target model.Graph, executor optimize.ShortestPathExecutor, planCacheMode string, jitEnabled bool) (graph.Database, error) {
	if !benchmarkShortestPathExecutor(executor) {
		return nil, fmt.Errorf("unsupported benchmark shortest-path executor %q", executor)
	}
	if !benchmarkPlanCacheMode(planCacheMode) {
		return nil, fmt.Errorf("unsupported PostgreSQL plan cache mode %q", planCacheMode)
	}
	return &shortestExecutorBenchmarkDatabase{Database: database, mapper: mapper, graph: target, executor: executor, planCacheMode: planCacheMode, jitEnabled: jitEnabled}, nil
}

func benchmarkPlanCacheMode(mode string) bool {
	return mode == "auto" || mode == "force_custom_plan" || mode == "force_generic_plan"
}

func benchmarkShortestPathExecutor(executor optimize.ShortestPathExecutor) bool {
	switch executor {
	case optimize.ShortestPathExecutorB2SmallerCurrentLevelDistance,
		optimize.ShortestPathExecutorB2SmallerCurrentLevelWitness,
		optimize.ShortestPathExecutorASPB2SmallerCurrentLevelDAG,
		optimize.ShortestPathExecutorI1CanonicalPredecessorWitness,
		optimize.ShortestPathExecutorASPI1DAG,
		optimize.ShortestPathExecutorI2GuardedDistanceV2:
		return true
	default:
		return false
	}
}

func (s *shortestExecutorBenchmarkDatabase) ReadTransaction(ctx context.Context, delegate graph.TransactionDelegate, options ...graph.TransactionOption) error {
	options = append(options, pg.OptionSetTransactionIsolation(pgx.RepeatableRead))
	return s.Database.ReadTransaction(ctx, func(tx graph.Transaction) error {
		settings := []string{"set local plan_cache_mode = " + s.planCacheMode}
		if s.jitEnabled {
			settings = append(settings, "set local jit = on")
		} else {
			settings = append(settings, "set local jit = off")
		}
		for _, sql := range settings {
			setting := tx.Raw(sql, nil)
			setting.Close()
			if err := setting.Error(); err != nil {
				return fmt.Errorf("apply PostgreSQL shortest-path benchmark setting: %w", err)
			}
		}
		return delegate(&shortestExecutorBenchmarkTransaction{Transaction: tx, ctx: ctx, mapper: s.mapper, graphID: s.graph.ID, executor: s.executor})
	}, options...)
}

func (s *shortestExecutorBenchmarkDatabase) KindMapper() pg.KindMapper {
	return s.mapper
}

func (s *shortestExecutorBenchmarkDatabase) DefaultGraph() (model.Graph, bool) {
	return s.graph, true
}

type shortestExecutorBenchmarkTransaction struct {
	graph.Transaction
	ctx      context.Context
	mapper   pg.KindMapper
	graphID  int32
	executor optimize.ShortestPathExecutor
}

func (s *shortestExecutorBenchmarkTransaction) Query(cypherQuery string, parameters map[string]any) graph.Result {
	if !strings.Contains(strings.ToLower(cypherQuery), "shortestpath") {
		return s.Transaction.Query(cypherQuery, parameters)
	}
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), cypherQuery)
	if err != nil {
		return graph.NewErrorResult(err)
	}
	translation, err := translate.TranslateForTool(s.ctx, regularQuery, s.mapper, parameters, s.graphID, translate.ToolOptions{ForceShortestPathExecutor: s.executor})
	if err != nil {
		return graph.NewErrorResult(err)
	}
	sqlQuery, err := translate.Translated(translation)
	if err != nil {
		return graph.NewErrorResult(err)
	}
	return s.Transaction.Raw(sqlQuery, translation.Parameters)
}
