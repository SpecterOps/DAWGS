// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//go:build manual_integration

package integration

import (
	"context"
	"fmt"
	"maps"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/stretchr/testify/require"
)

const liveAggregateTraversalCypher = `
MATCH (u:User)
WHERE u.hasspn = true
  AND u.enabled = true
  AND NOT u.objectid ENDS WITH '-502'
  AND NOT COALESCE(u.gmsa, false) = true
  AND NOT COALESCE(u.msa, false) = true
MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
WITH DISTINCT u, COUNT(c) AS adminCount
RETURN u
ORDER BY adminCount DESC
LIMIT 100
`

// TestPostgreSQLAggregateTraversalCountBindingSemantics runs the shared F5/F6
// cases against both compiler modes, including exact counts and mutation state.
func TestPostgreSQLAggregateTraversalCountBindingSemantics(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "aggregate_traversal_count_shapes")
}

// TestPostgreSQLExpansionBoundTerminalSemantics checks exact endpoint identities,
// paths, and mutation effects with each optimizer mode.
func TestPostgreSQLExpansionBoundTerminalSemantics(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "bound_expansion_endpoint_shapes")
}

func TestPostgreSQLNodeRepresentationSemantics(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "node_representation_shapes")
}

func TestPostgreSQLLeadingMultipartSemantics(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "leading_multipart_shapes")
}

func TestPostgreSQLCorrelatedSuffixSemantics(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "correlated_suffix_shapes")
}

func TestPostgreSQLCarriedNodeIntegerIDs(t *testing.T) {
	var corpus cypherTemplateFile
	for _, file := range loadCypherTemplateFiles(t) {
		if strings.HasSuffix(file.path, "/node_representation_shapes.json") {
			corpus = file
			break
		}
	}
	require.NotEmpty(t, corpus.Families)
	nodeKinds, edgeKinds := cypherTemplateKinds([]cypherTemplateFile{corpus})
	session := Open(t, Options{RequireDriver: pg.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true, CleanupMode: CleanupGraph, ExtraNodeKinds: nodeKinds, ExtraEdgeKinds: edgeKinds})
	pgDriver := session.DB.(*pg.Driver)
	defaultGraph, ok := pgDriver.DefaultGraph()
	require.True(t, ok)
	for _, mode := range []translate.OptimizerMode{translate.OptimizerEnabled, translate.OptimizerDisabled} {
		for _, hops := range []string{"", "*1..2"} {
			t.Run(string(mode)+"/"+hops, func(t *testing.T) {
				require.NoError(t, session.WithRollbackFixture(t, corpus.Families[0].Fixture, false, func(tx graph.Transaction, ids opengraph.IDMap) error {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), `MATCH (a) WHERE a.name = 'source' WITH a MATCH (a)-[:F1Edge`+hops+`]->(b) RETURN id(a), id(b)`)
					require.NoError(t, err)
					translation, err := translate.TranslateWithOptions(session.Ctx, parsed, pgDriver.KindMapper(), nil, defaultGraph.ID, translate.Options{OptimizerMode: mode})
					require.NoError(t, err)
					formatted, err := translate.Translated(translation)
					require.NoError(t, err)
					maps.Copy(translation.Parameters, formatted.Parameters)
					result := tx.Raw(formatted.Statement, translation.Parameters)
					defer result.Close()
					var got [][2]int64
					for result.Next() {
						values := result.Values()
						require.Len(t, values, 2)
						require.IsType(t, int64(0), values[0])
						require.IsType(t, int64(0), values[1])
						got = append(got, [2]int64{values[0].(int64), values[1].(int64)})
					}
					require.NoError(t, result.Error())
					want := [][2]int64{{ids["source"].Int64(), ids["target"].Int64()}}
					if hops != "" {
						want = append(want, [2]int64{ids["source"].Int64(), ids["far"].Int64()})
					}
					require.ElementsMatch(t, want, got)
					return nil
				}))
			})
		}
	}
}

// runPostgreSQLTemplateCompilerModes executes a shared template corpus using
// explicit compiler options; the raw query API does not honor the driver switch.
func runPostgreSQLTemplateCompilerModes(t *testing.T, corpusName string) {
	t.Helper()
	var corpus cypherTemplateFile
	for _, file := range loadCypherTemplateFiles(t) {
		if strings.HasSuffix(file.path, "/"+corpusName+".json") {
			corpus = file
			break
		}
	}
	if len(corpus.Families) == 0 {
		t.Fatalf("template corpus %q is missing", corpusName)
	}
	nodeKinds, edgeKinds := cypherTemplateKinds([]cypherTemplateFile{corpus})
	session := Open(t, Options{
		RequireDriver:        pg.DriverName,
		SkipIfNoConnection:   true,
		SkipIfDriverMismatch: true,
		CleanupMode:          CleanupGraph,
		ExtraNodeKinds:       nodeKinds,
		ExtraEdgeKinds:       edgeKinds,
	})
	ClearGraph(t, session.DB, session.Ctx)
	pgDriver := session.DB.(*pg.Driver)
	defaultGraph, ok := pgDriver.DefaultGraph()
	if !ok {
		t.Fatal("default graph is missing")
	}

	for _, mode := range []translate.OptimizerMode{translate.OptimizerEnabled, translate.OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, family := range corpus.Families {
				t.Run(family.Name, func(t *testing.T) {
					for _, variant := range family.Variants {
						t.Run(variant.Name, func(t *testing.T) {
							query := renderCypherTemplate(t, family.Template, variant.Vars)
							parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
							if err != nil {
								t.Fatal(err)
							}
							if err := session.WithRollbackFixture(t, family.Fixture, false, func(tx graph.Transaction, idMap opengraph.IDMap) error {
								params := resolveFixtureParams(t, mergeParams(family.Params, variant.Params), mergeStringMap(family.NodeParams, variant.NodeParams), mergeStringListMap(family.NodeListParams, variant.NodeListParams), idMap)
								translation, err := translate.TranslateWithOptions(session.Ctx, parsed, pgDriver.KindMapper(), params, defaultGraph.ID, translate.Options{OptimizerMode: mode})
								if err != nil {
									t.Fatal(err)
								}
								result := executeTemplateTranslation(t, tx, translation)
								parseAssertion(t, variant.Assert).checkResult(t, result, newAssertionContext(idMap))
								result.Close()
								return runStateAssertions(t, tx, idMap, variant.PostAssertions)
							}); err != nil {
								t.Fatal(err)
							}
						})
					}
				})
			}
		})
	}
}

// executeTemplateTranslation preserves statement boundaries for explicit
// optimizer-mode checks. Fixtures run in an explicit rollback transaction.
func executeTemplateTranslation(t *testing.T, tx graph.Transaction, translation translate.Result) graph.Result {
	t.Helper()
	statements, err := translate.TranslatedStatements(translation, "__dawgs_template_")
	if err != nil {
		t.Fatal(err)
	}
	for index, statement := range statements {
		bindings := make(map[string]any, len(translation.Parameters)+len(statement.Parameters))
		maps.Copy(bindings, translation.Parameters)
		maps.Copy(bindings, statement.Parameters)
		result := tx.Raw(statement.Statement, bindings)
		if index == len(statements)-1 {
			return result
		}
		for result.Next() {
		}
		result.Close()
		if err := result.Error(); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("translation contains no statements")
	return nil
}

type livePGKindMapper struct {
	pool *pgxpool.Pool
}

func (s livePGKindMapper) MapKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error) {
	ids := make([]int16, 0, len(kinds))

	for _, kind := range kinds {
		id, err := liveKindID(ctx, s.pool, kind.String())
		if err != nil {
			return nil, err
		}

		ids = append(ids, id)
	}

	return ids, nil
}

func (s livePGKindMapper) AssertKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error) {
	return s.MapKinds(ctx, kinds)
}

func TestPostgreSQLLiveAggregateTraversalCountPlanShape(t *testing.T) {
	connStr := os.Getenv("CONNECTION_STRING")
	if connStr == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}

	driver, err := DriverFromConnectionString(connStr)
	if err != nil {
		t.Fatalf("failed to detect driver: %v", err)
	}
	if driver != pg.DriverName {
		t.Skipf("CONNECTION_STRING is not a PostgreSQL connection string")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	poolCfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		t.Fatalf("failed to parse PG connection string: %v", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		t.Fatalf("failed to connect to PostgreSQL: %v", err)
	}
	defer pool.Close()

	liveStats, ok := liveAggregateTraversalStats(ctx, t, pool)
	if !ok {
		return
	}
	if liveStats.candidateUsers == 0 || liveStats.computers < 1000 || liveStats.adminEdges < 10000 {
		t.Skipf(
			"connected PostgreSQL database does not look like the live aggregate traversal dataset: %+v",
			liveStats,
		)
	}

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), liveAggregateTraversalCypher)
	if err != nil {
		t.Fatalf("failed to parse live aggregate traversal query: %v", err)
	}

	translation, err := translate.Translate(ctx, regularQuery, livePGKindMapper{pool: pool}, nil, translate.DefaultGraphID)
	if err != nil {
		t.Fatalf("failed to translate live aggregate traversal query: %v", err)
	}
	requireLoweringDecision(t, translation.Optimization.PlannedLowerings, optimize.LoweringAggregateTraversalCount)
	requireLoweringDecision(t, translation.Optimization.Lowerings, optimize.LoweringAggregateTraversalCount)

	sqlQuery, err := translate.Translated(translation)
	if err != nil {
		t.Fatalf("failed to render live aggregate traversal SQL: %v", err)
	}
	maps.Copy(translation.Parameters, sqlQuery.Parameters)
	normalizedSQL := strings.Join(strings.Fields(strings.ToLower(sqlQuery.Statement)), " ")

	for _, expected := range []string{
		"with recursive candidate_sources(root_id)",
		"traversal(root_id, next_id, depth, path)",
		"terminal_nodes(id) as materialized",
		"terminal_hits(root_id)",
		"ranked(root_id, admincount)",
		"join edge e on e.start_id = candidate_sources.root_id",
		"e.start_id = traversal.next_id",
		"group by terminal_hits.root_id",
		"from ranked join node source_node on source_node.id = ranked.root_id",
	} {
		if !strings.Contains(normalizedSQL, expected) {
			t.Fatalf("expected translated SQL to contain %q, got:\n%s", expected, sqlQuery.Statement)
		}
	}
	if strings.Contains(normalizedSQL, "group by (") {
		t.Fatalf("expected aggregate traversal SQL to avoid grouping by composites, got:\n%s", sqlQuery.Statement)
	}

	plan := explainAggregateTraversalPlan(ctx, t, pool, sqlQuery.Statement, translation.Parameters)
	for _, expected := range []string{
		"CTE traversal",
		"Recursive Union",
		"start_id = source_node",
		"start_id = traversal",
		"Group Key: traversal.root_id",
		"Hash Cond: (traversal.next_id = terminal_nodes.id)",
	} {
		if !strings.Contains(plan, expected) {
			t.Fatalf("expected PostgreSQL plan to contain %q, got:\n%s", expected, plan)
		}
	}
	for _, unexpected := range []string{
		"end_id = source_node",
		"end_id = traversal",
		"Group Key: (",
	} {
		if strings.Contains(plan, unexpected) {
			t.Fatalf("expected PostgreSQL plan to avoid %q, got:\n%s", unexpected, plan)
		}
	}

	var (
		limitMatch                 = regexp.MustCompile(`(?m)->\s+Limit\b`).FindStringIndex(plan)
		sourceMaterializationIndex = strings.LastIndex(plan, "Index Scan using node_")
	)

	if limitMatch == nil || sourceMaterializationIndex < 0 || sourceMaterializationIndex < limitMatch[0] {
		t.Fatalf("expected source node materialization after top-N limiting, got:\n%s", plan)
	}
}

type liveAggregateStats struct {
	candidateUsers int64
	computers      int64
	adminEdges     int64
}

func liveAggregateTraversalStats(ctx context.Context, t *testing.T, pool *pgxpool.Pool) (liveAggregateStats, bool) {
	t.Helper()

	userKindID, err := liveKindID(ctx, pool, "User")
	if err != nil {
		t.Skipf("connected PostgreSQL database has no User kind: %v", err)
		return liveAggregateStats{}, false
	}
	computerKindID, err := liveKindID(ctx, pool, "Computer")
	if err != nil {
		t.Skipf("connected PostgreSQL database has no Computer kind: %v", err)
		return liveAggregateStats{}, false
	}
	memberOfKindID, err := liveKindID(ctx, pool, "MemberOf")
	if err != nil {
		t.Skipf("connected PostgreSQL database has no MemberOf kind: %v", err)
		return liveAggregateStats{}, false
	}
	adminToKindID, err := liveKindID(ctx, pool, "AdminTo")
	if err != nil {
		t.Skipf("connected PostgreSQL database has no AdminTo kind: %v", err)
		return liveAggregateStats{}, false
	}

	var stats liveAggregateStats
	if err := pool.QueryRow(ctx, `
		select
			(
				select count(*)
				from node n
				where n.kind_ids operator (pg_catalog.@>) array[$1::int2]
				  and (n.properties -> 'hasspn') = to_jsonb(true)
				  and (n.properties -> 'enabled') = to_jsonb(true)
				  and coalesce(n.properties ->> 'objectid', '') not like '%-502'
				  and not coalesce((n.properties ->> 'gmsa')::bool, false)
				  and not coalesce((n.properties ->> 'msa')::bool, false)
			),
			(
				select count(*)
				from node n
				where n.kind_ids operator (pg_catalog.@>) array[$2::int2]
			),
			(
				select count(*)
				from edge e
				where e.kind_id = any(array[$3::int2, $4::int2])
			)
	`, userKindID, computerKindID, memberOfKindID, adminToKindID).Scan(
		&stats.candidateUsers,
		&stats.computers,
		&stats.adminEdges,
	); err != nil {
		t.Fatalf("failed to inspect live aggregate traversal dataset: %v", err)
	}

	return stats, true
}

func liveKindID(ctx context.Context, pool *pgxpool.Pool, name string) (int16, error) {
	var id int16
	if err := pool.QueryRow(ctx, `select id from kind where name = $1`, name).Scan(&id); err != nil {
		return 0, fmt.Errorf("map kind %q: %w", name, err)
	}

	return id, nil
}

func explainAggregateTraversalPlan(ctx context.Context, t *testing.T, pool *pgxpool.Pool, sqlQuery string, params map[string]any) string {
	t.Helper()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin PostgreSQL explain transaction: %v", err)
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	if _, err := tx.Exec(ctx, `set local statement_timeout = '30s'`); err != nil {
		t.Fatalf("failed to set PostgreSQL statement timeout: %v", err)
	}

	args := []any{}
	if len(params) > 0 {
		args = append(args, pgx.NamedArgs(params))
	}

	rows, err := tx.Query(ctx, "explain (analyze, buffers, timing off, summary off) "+sqlQuery, args...)
	if err != nil {
		t.Fatalf("failed to explain live aggregate traversal query: %v", err)
	}
	defer rows.Close()

	var planLines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("failed to scan live aggregate traversal plan line: %v", err)
		}
		planLines = append(planLines, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("failed while reading live aggregate traversal plan: %v", err)
	}

	return strings.Join(planLines, "\n")
}

func requireLoweringDecision(t *testing.T, lowerings []optimize.LoweringDecision, name string) {
	t.Helper()

	for _, lowering := range lowerings {
		if lowering.Name == name {
			return
		}
	}

	t.Fatalf("expected lowering %s in %v", name, lowerings)
}
