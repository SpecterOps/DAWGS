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
	pgv2 "github.com/specterops/dawgs/drivers/pg/v2"
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
		if err := applyPostgreSQLShortestPathBenchmarkSettings(tx, s.planCacheMode, s.jitEnabled); err != nil {
			return err
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

// traversalPolicyBenchmarkDatabase changes only the transaction boundary used
// by a policy-path benchmark. Query translation remains in the real V2 driver,
// which therefore performs the manifest selection, SQL-anchor validation, and
// normal connection-local cache lookup.
type traversalPolicyBenchmarkDatabase struct {
	graph.Database
	planCacheMode string
	jitEnabled    bool
}

func newTraversalPolicyBenchmarkDatabase(database graph.Database, planCacheMode string, jitEnabled bool) (graph.Database, error) {
	if !benchmarkPlanCacheMode(planCacheMode) {
		return nil, fmt.Errorf("unsupported PostgreSQL plan cache mode %q", planCacheMode)
	}
	return &traversalPolicyBenchmarkDatabase{Database: database, planCacheMode: planCacheMode, jitEnabled: jitEnabled}, nil
}

func (s *traversalPolicyBenchmarkDatabase) ReadTransaction(ctx context.Context, delegate graph.TransactionDelegate, options ...graph.TransactionOption) error {
	options = append(options, pg.OptionSetTransactionIsolation(pgx.RepeatableRead))
	return s.Database.ReadTransaction(ctx, func(tx graph.Transaction) error {
		if err := applyPostgreSQLShortestPathBenchmarkSettings(tx, s.planCacheMode, s.jitEnabled); err != nil {
			return err
		}
		return delegate(tx)
	}, options...)
}

// TranslationCacheStats preserves the V2 telemetry surface through the policy
// boundary wrapper so policy-path reports include actual cache activity.
func (s *traversalPolicyBenchmarkDatabase) TranslationCacheStats() pgv2.Stats {
	if provider, ok := s.Database.(interface{ TranslationCacheStats() pgv2.Stats }); ok {
		return provider.TranslationCacheStats()
	}
	return pgv2.Stats{}
}

func applyPostgreSQLShortestPathBenchmarkSettings(tx graph.Transaction, planCacheMode string, jitEnabled bool) error {
	settings := []string{"set local plan_cache_mode = " + planCacheMode}
	if jitEnabled {
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
	return nil
}
