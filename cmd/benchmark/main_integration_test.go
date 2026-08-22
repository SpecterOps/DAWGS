//go:build manual_integration

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

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/databaseguard"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
	"github.com/stretchr/testify/require"
)

func postgresBenchmarkIntegrationConnection(t *testing.T) string {
	t.Helper()
	connection := os.Getenv("CONNECTION_STRING")
	if connection == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}
	normalized := strings.ToLower(connection)
	if !strings.HasPrefix(normalized, "postgres://") && !strings.HasPrefix(normalized, "postgresql://") {
		t.Skip("CONNECTION_STRING is not a PostgreSQL connection string")
	}
	if err := databaseguard.ValidateEnvironment(connection); err != nil {
		t.Fatalf("integration database safety check failed: %v", err)
	}
	return connection
}

// TestPostgresV2BenchmarkMode proves the benchmark's explicit v2 path opens
// a live database, loads a graph fixture, and measures a Cypher scenario.
func TestPostgresV2BenchmarkMode(t *testing.T) {
	ctx := context.Background()
	database, err := openBenchmarkDatabase(ctx, pg.DriverName, postgresBenchmarkIntegrationConnection(t), size.Gibibyte)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, database.Close(context.Background()))
	})

	nodeKind := graph.StringKind("BenchmarkV2IntegrationNode")
	schema := graph.Schema{
		Graphs: []graph.Graph{{
			Name:  "benchmark_v2_integration",
			Nodes: graph.Kinds{nodeKind},
		}},
		DefaultGraph: graph.Graph{Name: "benchmark_v2_integration"},
	}
	require.NoError(t, database.AssertSchema(ctx, schema))
	t.Cleanup(func() {
		_ = database.WriteTransaction(context.Background(), func(tx graph.Transaction) error {
			return tx.Nodes().Delete()
		})
	})
	require.NoError(t, database.WriteTransaction(ctx, func(tx graph.Transaction) error {
		if _, err := tx.CreateNode(graph.NewProperties(), nodeKind); err != nil {
			return err
		}
		_, err := tx.CreateNode(graph.NewProperties(), nodeKind)
		return err
	}))

	scenario := expectScenarioRows(cypherScenario("v2", "live", "nodes", "MATCH (n:BenchmarkV2IntegrationNode) RETURN n"), 2)
	result, err := runScenario(ctx, database, scenario, 2, RunOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(2), result.RowCount)

	statsProvider, ok := database.(*pg.Driver)
	require.True(t, ok)
	stats := statsProvider.TranslationCacheStats()
	require.NotEmpty(t, stats.Connections)
	require.GreaterOrEqual(t, stats.Aggregate.Misses, uint64(1))
}

// TestPostgresV2BenchmarkPolicyPath proves a benchmark reaches the real V2
// manifest gate rather than a tool-only forced translation. The candidate SQL
// anchor is rendered before policy installation, then validated by the driver
// when the parameterized scenario executes at repeatable read.
func TestPostgresV2BenchmarkPolicyPath(t *testing.T) {
	ctx := context.Background()
	database, err := openBenchmarkDatabase(ctx, pg.DriverName, postgresBenchmarkIntegrationConnection(t), size.Gibibyte)
	require.NoError(t, err)
	driver, ok := database.(*pg.Driver)
	require.True(t, ok)
	t.Cleanup(func() {
		require.NoError(t, driver.SetTraversalPolicy(pg.TraversalPolicy{}))
		require.NoError(t, database.Close(context.Background()))
	})

	nodeKind := graph.StringKind("BenchmarkV2PolicyNode")
	edgeKind := graph.StringKind("BenchmarkV2PolicyEdge")
	schema := graph.Schema{
		Graphs: []graph.Graph{{
			Name:  "benchmark_v2_policy_integration",
			Nodes: graph.Kinds{nodeKind},
			Edges: graph.Kinds{edgeKind},
		}},
		DefaultGraph: graph.Graph{Name: "benchmark_v2_policy_integration"},
	}
	require.NoError(t, database.AssertSchema(ctx, schema))
	t.Cleanup(func() {
		_ = database.WriteTransaction(context.Background(), func(tx graph.Transaction) error {
			return tx.Nodes().Delete()
		})
	})

	var startID, endID graph.ID
	require.NoError(t, database.WriteTransaction(ctx, func(tx graph.Transaction) error {
		start, err := tx.CreateNode(graph.NewProperties(), nodeKind)
		if err != nil {
			return err
		}
		end, err := tx.CreateNode(graph.NewProperties(), nodeKind)
		if err != nil {
			return err
		}
		startID, endID = start.ID, end.ID
		_, err = tx.CreateRelationshipByIDs(startID, endID, edgeKind, graph.NewProperties())
		return err
	}))

	const cypher = "MATCH p = allShortestPaths((s)-[:BenchmarkV2PolicyEdge*1..4]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p"
	parameters := map[string]any{"start_id": startID, "end_id": endID}
	scenario := expectScenarioRows(cypherScenarioWithParameters("Shortest Paths", "policy", "candidate", cypher, parameters), 1)
	defaultGraph, found := driver.DefaultGraph()
	require.True(t, found)
	productionOptions := translate.ProductionOptions{
		ShortestPathExecutor: optimize.ShortestPathExecutorASPI1DAG,
		ShortestPathCaps: &translate.ProductionShortestPathCaps{
			StateLimit: 1000, PredecessorLimit: 1000, EnumerationLimit: 1000, OutputBytesLimit: 1 << 20,
		},
		AuthorizedBucket: &translate.ProductionTraversalBucket{
			Direction: "outbound", ObservationMode: "all_paths", MinimumDepth: 1, MaximumDepth: 4, RelationshipKindCount: 1,
		},
		SelectorVersion: "benchmark-policy-path-v1",
	}
	parsed, err := frontend.ParseCypher(frontend.NewContext(), cypher)
	require.NoError(t, err)
	translation, err := translate.TranslateWithProductionOptions(ctx, parsed, driver.KindMapper(), parameters, defaultGraph.ID, productionOptions)
	require.NoError(t, err)
	sqlQuery, err := translate.Translated(translation)
	require.NoError(t, err)
	sqlDigest := sha256.Sum256([]byte(sqlQuery))

	queryDigest := pg.TraversalPolicyQuerySHA256(cypher)
	preflightManifest := benchmarkTraversalPromotionManifest{
		Candidate:       string(optimize.ShortestPathExecutorASPI1DAG),
		SelectorVersion: "benchmark-policy-path-v1",
		Caps: map[string]int64{
			"state_limit": 1000, "predecessor_limit": 1000, "enumeration_limit": 1000, "output_bytes_limit": 1 << 20,
		},
		Buckets: []benchmarkPolicyBucket{{
			QuerySHA256: []string{queryDigest}, Direction: "outbound", ObservationMode: "all_paths",
			MinimumDepth: 1, MaximumDepth: 4, RelationshipKindCount: 1,
		}},
	}
	preflight, err := renderTraversalPolicyPreflight(ctx, driver.KindMapper(), defaultGraph, scenario, preflightManifest)
	require.NoError(t, err)
	require.Equal(t, queryDigest, preflight.QuerySHA256)
	require.Equal(t, hex.EncodeToString(sqlDigest[:]), preflight.SQLSHA256)
	require.Equal(t, string(optimize.ShortestPathExecutorASPI1DAG), preflight.Candidate)

	evidence := map[string]map[string]string{}
	for _, role := range []string{"aa", "confirmation", "performance", "resource", "reference_closure", "operational"} {
		evidence[role] = map[string]string{"path": role + ".json", "sha256": strings.Repeat("0", sha256.Size*2)}
	}
	manifest, err := json.Marshal(map[string]any{
		"version": 2, "candidate": string(optimize.ShortestPathExecutorASPI1DAG), "selector_version": "benchmark-policy-path-v1",
		"source_commit": "benchmark-integration", "source_sha256": strings.Repeat("0", sha256.Size*2),
		"binary_sha256": hex.EncodeToString(sqlDigest[:]), "corpus_sha256": strings.Repeat("0", sha256.Size*2),
		"operational_candidate_sql_sha256": hex.EncodeToString(sqlDigest[:]),
		"execution_boundary":               "guarded_dual_arm", "fallback_executor": string(optimize.ShortestPathExecutorASPA1DAG),
		"caps": map[string]int64{"state_limit": 1000, "predecessor_limit": 1000, "enumeration_limit": 1000, "output_bytes_limit": 1 << 20},
		"buckets": []map[string]any{{
			"name": "benchmark-policy-path", "query_sha256": []string{queryDigest}, "qualification_split": []string{"training", "holdout"},
			"direction": "outbound", "observation_mode": "all_paths", "minimum_depth": 1, "maximum_depth": 4,
			"relationship_kind_count": 1, "untyped_relationship": false,
		}},
		"evidence": evidence,
	})
	require.NoError(t, err)
	manifestPath := t.TempDir() + "/manifest.json"
	require.NoError(t, os.WriteFile(manifestPath, manifest, 0o600))
	policy, err := loadBenchmarkTraversalPolicy(manifestPath, 1)
	require.NoError(t, err)
	require.NoError(t, driver.SetTraversalPolicy(policy))

	wrapped, err := newTraversalPolicyBenchmarkDatabase(database, "auto", true)
	require.NoError(t, err)
	result, err := runScenario(ctx, wrapped, scenario, 2, RunOptions{WarmupIterations: 1})
	require.NoError(t, err)
	require.Equal(t, int64(1), result.RowCount)

	stats := driver.TranslationCacheStats()
	require.GreaterOrEqual(t, stats.Aggregate.Misses, uint64(1))
}
