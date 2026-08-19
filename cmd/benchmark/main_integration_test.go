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
	"os"
	"strings"
	"testing"

	"github.com/specterops/dawgs/databaseguard"
	pgv2 "github.com/specterops/dawgs/drivers/pg/v2"
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
	database, err := openBenchmarkDatabase(ctx, pgV2BenchmarkDriver, postgresBenchmarkIntegrationConnection(t), size.Gibibyte)
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

	statsProvider, ok := database.(*pgv2.Driver)
	require.True(t, ok)
	stats := statsProvider.TranslationCacheStats()
	require.NotEmpty(t, stats.Connections)
	require.GreaterOrEqual(t, stats.Aggregate.Misses, uint64(1))
}
