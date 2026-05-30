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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg"
	pgmodel "github.com/specterops/dawgs/drivers/pg/model"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
)

var indexedNodeKind = graph.StringKind("IndexedNode")

// indexedPostgresDB carries the live PostgreSQL graph and generated partition
// metadata needed to translate Cypher and assert index usage against EXPLAIN.
type indexedPostgresDB struct {
	db           graph.Database
	driver       *pg.Driver
	ctx          context.Context
	nodeTable    string
	nodeIndexes  map[string]string
	propertyKind graph.Kind
}

// TestPostgreSQLPropertyIndexPlans verifies that currently supported CySQL
// property predicates remain matchable to explicit PostgreSQL node property
// indexes. The positive cases assert btree and trigram GIN index usage, while
// the negative controls mutate the translated SQL into non-matching expression
// shapes to prove the assertion catches index misses.
func TestPostgreSQLPropertyIndexPlans(t *testing.T) {
	t.Run("btree equality", func(t *testing.T) {
		indexedDB := setupIndexedPostgresDB(t, "integration_property_btree_index_test", []graph.Index{
			{Field: "objectid", Type: graph.BTreeIndex},
			{Field: "name", Type: graph.BTreeIndex},
		})
		loadPropertyIndexFixture(t, indexedDB)
		analyzeIndexedNodePartition(t, indexedDB)

		for _, testCase := range []struct {
			name       string
			cypher     string
			params     map[string]any
			indexField string
		}{
			{
				name:       "untyped objectid parameter",
				cypher:     `MATCH (n) WHERE n.objectid = $objectid RETURN n`,
				params:     map[string]any{"objectid": "S-1-5-21-index-target"},
				indexField: "objectid",
			},
			{
				name:       "typed objectid parameter",
				cypher:     `MATCH (n:IndexedNode) WHERE n.objectid = $objectid RETURN n`,
				params:     map[string]any{"objectid": "S-1-5-21-index-target"},
				indexField: "objectid",
			},
			{
				name:       "inline name property map",
				cypher:     `MATCH (n:IndexedNode {name: $name}) RETURN n`,
				params:     map[string]any{"name": "indexed-name"},
				indexField: "name",
			},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				assertTranslatedPlanUsesIndex(t, indexedDB, testCase.cypher, testCase.params, indexedDB.nodeIndexes[testCase.indexField])
			})
		}

		t.Run("negative control misses wrapped objectid expression", func(t *testing.T) {
			assertMutatedTranslatedPlanMissesIndex(
				t,
				indexedDB,
				`MATCH (n) WHERE n.objectid = $objectid RETURN n`,
				map[string]any{"objectid": "S-1-5-21-index-target"},
				indexedDB.nodeIndexes["objectid"],
				func(sqlQuery string) (string, bool) {
					return replaceSQLExpressionOnce(
						sqlQuery,
						"n0.properties ->> 'objectid'",
						"coalesce((n0.properties ->> 'objectid'), '')::text",
					)
				},
			)
		})
	})

	t.Run("gin trigram text search", func(t *testing.T) {
		indexedDB := setupIndexedPostgresDB(t, "integration_property_gin_index_test", []graph.Index{
			{Field: "name", Type: graph.TextSearchIndex},
		})
		loadPropertyIndexFixture(t, indexedDB)
		analyzeIndexedNodePartition(t, indexedDB)

		for _, testCase := range []struct {
			name   string
			cypher string
		}{
			{
				name:   "literal starts with",
				cypher: `MATCH (n) WHERE n.name STARTS WITH 'prefix' RETURN n`,
			},
			{
				name:   "literal ends with",
				cypher: `MATCH (n) WHERE n.name ENDS WITH 'suffix' RETURN n`,
			},
			{
				name:   "literal contains",
				cypher: `MATCH (n) WHERE n.name CONTAINS 'needle' RETURN n`,
			},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				assertTranslatedPlanUsesIndex(t, indexedDB, testCase.cypher, nil, indexedDB.nodeIndexes["name"])
			})
		}

		t.Run("negative control misses wrapped name expression", func(t *testing.T) {
			assertMutatedTranslatedPlanMissesIndex(
				t,
				indexedDB,
				`MATCH (n) WHERE n.name CONTAINS 'needle' RETURN n`,
				nil,
				indexedDB.nodeIndexes["name"],
				func(sqlQuery string) (string, bool) {
					return replaceSQLExpressionOnce(
						sqlQuery,
						"n0.properties ->> 'name'",
						"coalesce((n0.properties ->> 'name'), '')::text",
					)
				},
			)
		})
	})
}

// setupIndexedPostgresDB opens a PostgreSQL-backed DAWGS database, asserts a
// graph with the supplied node property indexes, and returns the generated
// partition/index names used by plan assertions.
func setupIndexedPostgresDB(t *testing.T, graphName string, nodeIndexes []graph.Index) indexedPostgresDB {
	t.Helper()

	connStr := os.Getenv("CONNECTION_STRING")
	if connStr == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}

	driverName, err := driverFromConnStr(connStr)
	if err != nil {
		t.Fatalf("failed to detect driver: %v", err)
	}
	if driverName != pg.DriverName {
		t.Skip("CONNECTION_STRING is not a PostgreSQL connection string")
	}

	ctx := context.Background()
	poolCfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		t.Fatalf("failed to parse PG connection string: %v", err)
	}
	pool, err := pg.NewPool(poolCfg)
	if err != nil {
		t.Fatalf("failed to create PG pool: %v", err)
	}

	db, err := dawgs.Open(ctx, pg.DriverName, dawgs.Config{
		GraphQueryMemoryLimit: size.Gibibyte,
		ConnectionString:      connStr,
		Pool:                  pool,
	})
	if err != nil {
		pool.Close()
		t.Fatalf("failed to open PostgreSQL database: %v", err)
	}

	pgDriver, ok := db.(*pg.Driver)
	if !ok {
		_ = db.Close(ctx)
		t.Fatalf("expected *pg.Driver, got %T", db)
	}

	graphSchema := graph.Graph{
		Name:        graphName,
		Nodes:       graph.Kinds{indexedNodeKind},
		NodeIndexes: nodeIndexes,
	}
	schema := graph.Schema{
		Graphs:       []graph.Graph{graphSchema},
		DefaultGraph: graphSchema,
	}
	if err := db.AssertSchema(ctx, schema); err != nil {
		_ = db.Close(ctx)
		t.Fatalf("failed to assert indexed PostgreSQL schema: %v", err)
	}

	defaultGraph, ok := pgDriver.DefaultGraph()
	if !ok {
		_ = db.Close(ctx)
		t.Fatal("PostgreSQL default graph is not set")
	}

	indexNames := make(map[string]string, len(nodeIndexes))
	for _, index := range nodeIndexes {
		indexNames[index.Field] = pgmodel.IndexName(defaultGraph.Partitions.Node.Name, index)
	}

	indexedDB := indexedPostgresDB{
		db:           db,
		driver:       pgDriver,
		ctx:          ctx,
		nodeTable:    defaultGraph.Partitions.Node.Name,
		nodeIndexes:  indexNames,
		propertyKind: indexedNodeKind,
	}

	clearIndexedGraph(t, indexedDB)
	t.Cleanup(func() {
		clearIndexedGraph(t, indexedDB)
		_ = db.Close(ctx)
	})

	return indexedDB
}

// clearIndexedGraph removes fixture nodes from the indexed graph before and
// after each test graph run.
func clearIndexedGraph(t *testing.T, indexedDB indexedPostgresDB) {
	t.Helper()

	if err := indexedDB.db.WriteTransaction(indexedDB.ctx, func(tx graph.Transaction) error {
		return tx.Nodes().Delete()
	}); err != nil {
		t.Fatalf("failed to clear indexed graph: %v", err)
	}
}

// loadPropertyIndexFixture creates selective node data for both exact string
// equality and literal trigram predicate plan checks.
func loadPropertyIndexFixture(t *testing.T, indexedDB indexedPostgresDB) {
	t.Helper()

	if err := indexedDB.db.WriteTransaction(indexedDB.ctx, func(tx graph.Transaction) error {
		if _, err := tx.CreateNode(graph.AsProperties(map[string]any{
			"objectid": "S-1-5-21-index-target",
			"name":     "indexed-name",
		}), indexedDB.propertyKind); err != nil {
			return err
		}

		if _, err := tx.CreateNode(graph.AsProperties(map[string]any{
			"objectid": "S-1-5-21-trigram-target",
			"name":     "prefix-needle-suffix",
		}), indexedDB.propertyKind); err != nil {
			return err
		}

		for idx := 0; idx < 256; idx++ {
			if _, err := tx.CreateNode(graph.AsProperties(map[string]any{
				"objectid": fmt.Sprintf("S-1-5-21-filler-%03d", idx),
				"name":     fmt.Sprintf("filler-%03d-value", idx),
			}), indexedDB.propertyKind); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		t.Fatalf("failed to load property index fixture: %v", err)
	}
}

// analyzeIndexedNodePartition refreshes PostgreSQL statistics for the synthetic
// fixture partition before EXPLAIN plans are captured.
func analyzeIndexedNodePartition(t *testing.T, indexedDB indexedPostgresDB) {
	t.Helper()

	if err := indexedDB.db.WriteTransaction(indexedDB.ctx, func(tx graph.Transaction) error {
		result := tx.Raw("analyze "+indexedDB.nodeTable, nil)
		defer result.Close()
		return result.Error()
	}); err != nil {
		t.Fatalf("failed to analyze indexed node partition: %v", err)
	}
}

// assertTranslatedPlanUsesIndex translates the Cypher query through the normal
// CySQL path, explains the rendered SQL, and fails unless the expected index is
// present in the JSON plan.
func assertTranslatedPlanUsesIndex(t *testing.T, indexedDB indexedPostgresDB, cypherQuery string, params map[string]any, expectedIndex string) {
	t.Helper()

	sqlQuery, sqlParams := translateIndexedCypher(t, indexedDB, cypherQuery, params)
	plan := explainWithSeqScanDisabled(t, indexedDB, sqlQuery, sqlParams)
	if !planContainsIndex(plan, expectedIndex) {
		t.Fatalf("expected PostgreSQL plan for %q to use index %q, got:\n%s", cypherQuery, expectedIndex, formatPlanForFailure(plan))
	}
}

// assertMutatedTranslatedPlanMissesIndex translates a covered Cypher query,
// applies a deliberate SQL mutation that should break expression-index matching,
// and fails if PostgreSQL still reports the expected index in the plan.
func assertMutatedTranslatedPlanMissesIndex(
	t *testing.T,
	indexedDB indexedPostgresDB,
	cypherQuery string,
	params map[string]any,
	expectedIndex string,
	mutate func(string) (string, bool),
) {
	t.Helper()

	sqlQuery, sqlParams := translateIndexedCypher(t, indexedDB, cypherQuery, params)
	mutatedSQLQuery, mutated := mutate(sqlQuery)
	if !mutated {
		t.Fatalf("negative control did not mutate translated SQL:\n%s", sqlQuery)
	}

	plan := explainWithSeqScanDisabled(t, indexedDB, mutatedSQLQuery, sqlParams)
	if planContainsIndex(plan, expectedIndex) {
		t.Fatalf("expected mutated PostgreSQL plan for %q to miss index %q, got:\n%s", cypherQuery, expectedIndex, formatPlanForFailure(plan))
	}
}

// replaceSQLExpressionOnce performs the narrow SQL rewrite used by negative
// controls and reports whether the expected source expression was found.
func replaceSQLExpressionOnce(sqlQuery, oldExpression, newExpression string) (string, bool) {
	if !strings.Contains(sqlQuery, oldExpression) {
		return sqlQuery, false
	}

	return strings.Replace(sqlQuery, oldExpression, newExpression, 1), true
}

// translateIndexedCypher parses and translates Cypher using the same PgSQL
// translator path as production query execution.
func translateIndexedCypher(t *testing.T, indexedDB indexedPostgresDB, cypherQuery string, params map[string]any) (string, map[string]any) {
	t.Helper()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), cypherQuery)
	if err != nil {
		t.Fatalf("failed to parse Cypher query: %v", err)
	}

	defaultGraph, ok := indexedDB.driver.DefaultGraph()
	if !ok {
		t.Fatal("PostgreSQL default graph is not set")
	}

	translation, err := translate.Translate(indexedDB.ctx, regularQuery, indexedDB.driver.KindMapper(), params, defaultGraph.ID)
	if err != nil {
		t.Fatalf("failed to translate Cypher query: %v", err)
	}

	sqlQuery, err := translate.Translated(translation)
	if err != nil {
		t.Fatalf("failed to render translated SQL: %v", err)
	}

	return sqlQuery, translation.Parameters
}

// explainWithSeqScanDisabled captures a JSON EXPLAIN plan with sequential scans
// disabled so the test verifies expression-index matchability rather than the
// cost model's choice for a tiny fixture.
func explainWithSeqScanDisabled(t *testing.T, indexedDB indexedPostgresDB, sqlQuery string, params map[string]any) any {
	t.Helper()

	var plan any
	if err := indexedDB.db.WriteTransaction(indexedDB.ctx, func(tx graph.Transaction) error {
		setResult := tx.Raw("set local enable_seqscan = off", nil)
		setResult.Close()
		if err := setResult.Error(); err != nil {
			return fmt.Errorf("disable sequential scan: %w", err)
		}

		result := tx.Raw("explain (format json, costs off) "+sqlQuery, params)
		defer result.Close()

		if !result.Next() {
			if err := result.Error(); err != nil {
				return err
			}
			return errors.New("PostgreSQL EXPLAIN returned no rows")
		}

		values := result.Values()
		if len(values) == 0 {
			return errors.New("PostgreSQL EXPLAIN returned an empty row")
		}

		parsedPlan, err := normalizeExplainPlan(values[0])
		if err != nil {
			return err
		}
		plan = parsedPlan

		return result.Error()
	}); err != nil {
		t.Fatalf("failed to explain translated SQL: %v", err)
	}

	return plan
}

// normalizeExplainPlan converts the JSON plan value returned by pgx/DAWGS into
// ordinary Go slices and maps for recursive inspection.
func normalizeExplainPlan(value any) (any, error) {
	switch typedValue := value.(type) {
	case []any:
		return typedValue, nil
	case map[string]any:
		return typedValue, nil
	case string:
		var decoded any
		if err := json.Unmarshal([]byte(typedValue), &decoded); err != nil {
			return nil, fmt.Errorf("decode PostgreSQL JSON plan: %w", err)
		}
		return decoded, nil
	case []byte:
		var decoded any
		if err := json.Unmarshal(typedValue, &decoded); err != nil {
			return nil, fmt.Errorf("decode PostgreSQL JSON plan: %w", err)
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("unexpected PostgreSQL JSON plan value %T", value)
	}
}

// planContainsIndex recursively walks a PostgreSQL JSON plan looking for an
// Index Name field equal to expectedIndex.
func planContainsIndex(plan any, expectedIndex string) bool {
	switch typedPlan := plan.(type) {
	case []any:
		for _, item := range typedPlan {
			if planContainsIndex(item, expectedIndex) {
				return true
			}
		}

	case map[string]any:
		if indexName, hasIndexName := typedPlan["Index Name"].(string); hasIndexName && indexName == expectedIndex {
			return true
		}

		for _, value := range typedPlan {
			if planContainsIndex(value, expectedIndex) {
				return true
			}
		}
	}

	return false
}

// formatPlanForFailure renders a PostgreSQL JSON plan for assertion failures.
func formatPlanForFailure(plan any) string {
	encoded, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return fmt.Sprintf("%#v", plan)
	}

	return string(encoded)
}
