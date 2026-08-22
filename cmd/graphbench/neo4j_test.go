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
	"net/url"
	"testing"

	neo4jcore "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

// TestParseNeo4jPlanDriverConfig verifies parse neo4j plan driver config behavior.
func TestParseNeo4jPlanDriverConfig(t *testing.T) {
	cfg, err := parseNeo4jPlanDriverConfig("neo4j://neo4j:secret@example.com:7687/neo4jdb?x=1")

	require.NoError(t, err)
	require.Equal(t, "neo4j://example.com:7687?x=1", cfg.Target)
	require.Equal(t, "neo4j", cfg.Username)
	require.Equal(t, "secret", cfg.Password)
	require.Equal(t, "neo4jdb", cfg.DatabaseName)
}

// TestNeo4jDatabaseNameRejectsNestedPath verifies neo4j database name rejects nested path behavior.
func TestNeo4jDatabaseNameRejectsNestedPath(t *testing.T) {
	for _, connStr := range []string{
		"neo4j://neo4j:secret@example.com:7687/a/b",
		"neo4j://neo4j:secret@example.com:7687/a%2Fb",
	} {
		parsed, err := url.Parse(connStr)
		require.NoError(t, err)

		_, err = neo4jDatabaseName(parsed)
		require.ErrorContains(t, err, "single database name")
	}
}

// TestNeo4jOperatorsAnnotatesOperators verifies neo4j operators annotates operators behavior.
func TestNeo4jOperatorsAnnotatesOperators(t *testing.T) {
	operators := neo4jOperators(Neo4jPlanNode{
		Operator: "ProduceResults@neo4j@neo4j",
		Children: []Neo4jPlanNode{{
			Operator: "AllNodesScan@neo4j",
		}},
	})

	require.Equal(t, []string{"ProduceResults@neo4j", "AllNodesScan@neo4j"}, operators)
}

// TestNeo4jPlanCaptureStatementProfilesReadsAndExplainsWrites verifies neo4j plan capture statement profiles reads and explains writes behavior.
func TestNeo4jPlanCaptureStatementProfilesReadsAndExplainsWrites(t *testing.T) {
	require.Equal(t, "PROFILE MATCH (n) RETURN n", neo4jPlanCaptureStatement(" MATCH (n) RETURN n; ", false))
	require.Equal(t, "EXPLAIN CREATE (n)", neo4jPlanCaptureStatement("CREATE (n);", true))
}

// TestConvertNeo4jPlanPreservesEndpointChildOrder verifies convert neo4j plan preserves endpoint child order behavior.
func TestConvertNeo4jPlanPreservesEndpointChildOrder(t *testing.T) {
	plan := stubNeo4jPlan{
		operator:  "CartesianProduct@neo4j@neo4j",
		arguments: map[string]any{"EstimatedRows": 2.5, "Loops": int64(3)},
		children: []neo4jcore.Plan{
			stubNeo4jPlan{
				operator:    "NodeIndexSeek",
				identifiers: []string{"start"},
			},
			stubNeo4jPlan{
				operator:    "NodeIndexSeek",
				identifiers: []string{"end"},
			},
		},
	}

	converted := convertNeo4jPlan(plan)

	require.Equal(t, "CartesianProduct@neo4j", converted.Operator)
	require.Equal(t, 2.5, *converted.EstimatedRows)
	require.Equal(t, int64(3), *converted.Loops)
	require.Equal(t, []string{"start"}, converted.Children[0].Identifiers)
	require.Equal(t, []string{"end"}, converted.Children[1].Identifiers)
}

// TestConvertNeo4jProfiledPlanCapturesMetricsMetadataAndOpaqueShortestPath verifies convert neo4j profiled plan captures metrics metadata and opaque shortest path behavior.
func TestConvertNeo4jProfiledPlanCapturesMetricsMetadataAndOpaqueShortestPath(t *testing.T) {
	profile := stubNeo4jProfiledPlan{
		operator: "ProduceResults@neo4j",
		arguments: map[string]any{
			"EstimatedRows":   1.5,
			"planner":         "COST",
			"planner-impl":    "IDP",
			"planner-version": "4.4",
			"runtime":         "INTERPRETED",
			"runtime-impl":    "INTERPRETED",
			"runtime-version": "4.4",
			"version":         "CYPHER 4.4",
		},
		dbHits:            11,
		records:           7,
		pageCacheHits:     13,
		pageCacheMisses:   2,
		pageCacheHitRatio: 0.86,
		timeNS:            101,
		children: []neo4jcore.ProfiledPlan{
			stubNeo4jProfiledPlan{
				operator: "ShortestPath@neo4j@neo4j",
				dbHits:   1,
				records:  1,
			},
			stubNeo4jProfiledPlan{
				operator:    "NodeIndexSeek",
				identifiers: []string{"end"},
				dbHits:      3,
				records:     1,
			},
		},
	}
	metadata := neo4jProfileMetadata(profile.Arguments(), "Neo4j/4.4.44", true)

	converted := convertNeo4jProfiledPlan(profile, metadata.internalTraversalOpaque())
	converted.ProfileMetadata = &metadata

	require.Equal(t, "ProduceResults@neo4j", converted.Operator)
	require.Equal(t, 1.5, *converted.EstimatedRows)
	require.Equal(t, int64(7), *converted.ActualRows)
	require.Equal(t, int64(11), *converted.DBHits)
	require.Equal(t, int64(13), *converted.PageCacheHits)
	require.Equal(t, int64(2), *converted.PageCacheMisses)
	require.Equal(t, 0.86, *converted.PageCacheHitRatio)
	require.Equal(t, int64(101), *converted.TimeNS)
	require.Equal(t, "PROFILE", converted.ProfileMetadata.CaptureMode)
	require.True(t, converted.ProfileMetadata.Profiled)
	require.Equal(t, "4.4", converted.ProfileMetadata.PlannerVersion)
	require.Equal(t, "4.4", converted.ProfileMetadata.RuntimeVersion)
	require.Equal(t, "ShortestPath@neo4j", converted.Children[0].Operator)
	require.Equal(t, "opaque", converted.Children[0].InternalTraversalWork)
	require.Empty(t, converted.Children[1].InternalTraversalWork)
	require.Equal(t, []string{"end"}, converted.Children[1].Identifiers)
}

// stubNeo4jPlan groups state that must remain consistent while processing stub neo4j plan.
type stubNeo4jPlan struct {
	// operator retains the operator while stubNeo4jPlan is assembled or evaluated.
	operator string
	// arguments retains the arguments while stubNeo4jPlan is assembled or evaluated.
	arguments map[string]any
	// identifiers retains the identifiers while stubNeo4jPlan is assembled or evaluated.
	identifiers []string
	// children retains the children while stubNeo4jPlan is assembled or evaluated.
	children []neo4jcore.Plan
}

// Operator prepares or inspects test evidence for operator.
func (s stubNeo4jPlan) Operator() string { return s.operator }

// Arguments prepares or inspects test evidence for arguments.
func (s stubNeo4jPlan) Arguments() map[string]any { return s.arguments }

// Identifiers prepares or inspects test evidence for identifiers.
func (s stubNeo4jPlan) Identifiers() []string { return s.identifiers }

// Children prepares or inspects test evidence for children.
func (s stubNeo4jPlan) Children() []neo4jcore.Plan { return s.children }

// stubNeo4jProfiledPlan groups state that must remain consistent while processing stub neo4j profiled plan.
type stubNeo4jProfiledPlan struct {
	// operator retains the operator while stubNeo4jProfiledPlan is assembled or evaluated.
	operator string
	// arguments retains the arguments while stubNeo4jProfiledPlan is assembled or evaluated.
	arguments map[string]any
	// identifiers retains the identifiers while stubNeo4jProfiledPlan is assembled or evaluated.
	identifiers []string
	// dbHits retains the db hits while stubNeo4jProfiledPlan is assembled or evaluated.
	dbHits int64
	// records retains the records while stubNeo4jProfiledPlan is assembled or evaluated.
	records int64
	// children retains the children while stubNeo4jProfiledPlan is assembled or evaluated.
	children []neo4jcore.ProfiledPlan
	// pageCacheMisses retains the page cache misses while stubNeo4jProfiledPlan is assembled or evaluated.
	pageCacheMisses int64
	// pageCacheHits retains the page cache hits while stubNeo4jProfiledPlan is assembled or evaluated.
	pageCacheHits int64
	// pageCacheHitRatio retains the page cache hit ratio while stubNeo4jProfiledPlan is assembled or evaluated.
	pageCacheHitRatio float64
	// timeNS retains the time ns while stubNeo4jProfiledPlan is assembled or evaluated.
	timeNS int64
}

// Operator prepares or inspects test evidence for operator.
func (s stubNeo4jProfiledPlan) Operator() string { return s.operator }

// Arguments prepares or inspects test evidence for arguments.
func (s stubNeo4jProfiledPlan) Arguments() map[string]any { return s.arguments }

// Identifiers prepares or inspects test evidence for identifiers.
func (s stubNeo4jProfiledPlan) Identifiers() []string { return s.identifiers }

// DbHits prepares or inspects test evidence for db hits.
func (s stubNeo4jProfiledPlan) DbHits() int64 { return s.dbHits }

// Records prepares or inspects test evidence for records.
func (s stubNeo4jProfiledPlan) Records() int64 { return s.records }

// Children prepares or inspects test evidence for children.
func (s stubNeo4jProfiledPlan) Children() []neo4jcore.ProfiledPlan { return s.children }

// PageCacheMisses prepares or inspects test evidence for page cache misses.
func (s stubNeo4jProfiledPlan) PageCacheMisses() int64 { return s.pageCacheMisses }

// PageCacheHits prepares or inspects test evidence for page cache hits.
func (s stubNeo4jProfiledPlan) PageCacheHits() int64 { return s.pageCacheHits }

// PageCacheHitRatio derives the statistical value used to evaluate page cache hit ratio.
func (s stubNeo4jProfiledPlan) PageCacheHitRatio() float64 { return s.pageCacheHitRatio }

// Time prepares or inspects test evidence for time.
func (s stubNeo4jProfiledPlan) Time() int64 { return s.timeNS }
