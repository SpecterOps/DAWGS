package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildSummaryRanksPostgresPlansAndCountsSignals(t *testing.T) {
	records := []PlanRecord{{
		Driver:           "pg",
		Source:           "cases/a.json",
		Name:             "low",
		Cypher:           "match (n) return n",
		PGPlan:           []string{"Seq Scan on node_1  (cost=0.00..10.50 rows=1 width=8)", "Filter: satisfied"},
		PGOperators:      []string{"Seq Scan on node_1", "Filter: satisfied"},
		PlannedLowerings: []string{"ProjectionPruning"},
		AppliedLowerings: []string{"ProjectionPruning"},
	}, {
		Driver:           "pg",
		Source:           "cases/b.json",
		Name:             "high",
		Cypher:           "match p=()-[*]->() return p",
		PGPlan:           []string{"Recursive Union  (cost=0.00..99.25 rows=1 width=8)", "SubPlan 1", "Function Scan on unnest _path"},
		PGOperators:      []string{"Recursive Union", "Function Scan on unnest _path"},
		PlannedLowerings: []string{"LatePathMaterialization"},
		AppliedLowerings: []string{"LatePathMaterialization"},
	}, {
		Driver:         "neo4j",
		Source:         "cases/a.json",
		Name:           "neo",
		Cypher:         "match (n) return n",
		Neo4jOperators: []string{"ProduceResults@neo4j", "AllNodesScan@neo4j"},
	}, {
		Driver: "pg",
		Source: "cases/error.json",
		Name:   "error",
		Error:  "expected error",
	}}

	summary := buildSummary(records, 1)

	require.Equal(t, []DriverSummary{{
		Driver:  "neo4j",
		Records: 1,
	}, {
		Driver:  "pg",
		Records: 3,
		Errors:  1,
	}}, summary.Drivers)
	require.Len(t, summary.TopPostgresPlans, 1)
	require.Equal(t, "high", summary.TopPostgresPlans[0].Name)
	require.Contains(t, summary.FeatureCounts, Count{Name: "PostgreSQL Recursive Union", Count: 1})
	require.Contains(t, summary.FeatureCounts, Count{Name: "PostgreSQL SubPlan", Count: 1})
	require.Contains(t, summary.FeatureCounts, Count{Name: "PostgreSQL Function Scan on unnest", Count: 1})
	require.Contains(t, summary.FeatureCounts, Count{Name: "PostgreSQL traversal satisfied filter", Count: 1})
	require.Contains(t, summary.PostgresOperators, Count{Name: "Seq Scan", Count: 1})
	require.Contains(t, summary.Neo4jOperators, Count{Name: "ProduceResults@neo4j", Count: 1})
	require.Contains(t, summary.PlannedLowerings, Count{Name: "LatePathMaterialization", Count: 1})
	require.Contains(t, summary.Errors, PlanError{
		Driver: "pg",
		Source: "cases/error.json",
		Name:   "error",
		Error:  "expected error",
	})
}

func TestWriteMarkdownSummaryEscapesPipes(t *testing.T) {
	summary := PlanSummary{
		Drivers: []DriverSummary{{Driver: "pg", Records: 1}},
		TopPostgresPlans: []CostedPlan{{
			Cost:     1.25,
			Source:   "cases/a.json",
			Name:     "pipe | name",
			PlanRoot: "Seq Scan on node_1",
		}},
	}

	var out bytes.Buffer
	require.NoError(t, writeMarkdownSummary(&out, summary))
	require.Contains(t, out.String(), "pipe \\| name")
}

func TestPostgresEstimatedCost(t *testing.T) {
	require.Equal(t, 1180526.82, postgresEstimatedCost("Hash Join  (cost=4136.05..1180526.82 rows=32097 width=68)"))
	require.Zero(t, postgresEstimatedCost("not a plan"))
}
