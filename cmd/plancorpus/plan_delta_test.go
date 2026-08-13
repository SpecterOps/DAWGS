package main

import (
	"path/filepath"
	"testing"

	"github.com/specterops/dawgs/testutil"
	"github.com/stretchr/testify/require"
)

// TestBuildPlanDeltaReportPairsByWorkloadAndPreservesSemanticDifferences verifies
// stable pairing, plan fingerprints, direction classification, and opaque Neo4j
// shortest-path work.
func TestBuildPlanDeltaReportPairsByWorkloadAndPreservesSemanticDifferences(t *testing.T) {
	query := CorpusQuery{
		Source:  "cases/shortest.json",
		Dataset: "shortest",
		Name:    "bound",
		Cypher:  "MATCH p = shortestPath((root)-[*1..4]->(terminal)) RETURN p",
		Params:  map[string]any{"root_id": int64(1), "terminal_id": int64(2)},
	}
	workload := workloadFingerprint(query)
	pgPlan := []string{
		"Function Scan on shortest_path_compact  (cost=0.25..0.26 rows=1 width=8)",
		"Index Scan using node_id_idx on node root  (cost=0.10..1.00 rows=1 width=8)",
		"Index Cond: (start_id = root.id)",
	}
	neoPlan := &Neo4jPlanNode{
		Operator:  "ProduceResults",
		Arguments: map[string]string{"EstimatedRows": "1"},
		Children: []Neo4jPlanNode{{
			Operator:  "ShortestPath",
			Arguments: map[string]string{"EstimatedRows": "1", "Details": "(terminal)<-[*]-(root)"},
			Children: []Neo4jPlanNode{{
				Operator:  "NodeByIdSeek",
				Arguments: map[string]string{"Details": "terminal"},
			}},
		}},
	}
	records := []PlanRecord{{
		SchemaVersion:     planRecordSchemaVersion,
		Driver:            pgDriverName(),
		Source:            query.Source,
		Dataset:           query.Dataset,
		Name:              query.Name,
		WorkloadSHA256:    workload,
		Cypher:            query.Cypher,
		PGPlan:            pgPlan,
		PGPlanFingerprint: postgresPlanFingerprint(pgPlan),
	}, {
		SchemaVersion:        planRecordSchemaVersion,
		Driver:               neo4jDriverName(),
		Source:               query.Source,
		Dataset:              query.Dataset,
		Name:                 query.Name,
		WorkloadSHA256:       workload,
		Cypher:               query.Cypher,
		Neo4jPlan:            neoPlan,
		Neo4jPlanFingerprint: neo4jPlanFingerprint(neoPlan),
	}}

	report, err := buildPlanDeltaReport(records)
	require.NoError(t, err)
	require.Equal(t, planDeltaSchemaVersion, report.Version)
	require.Len(t, report.Records, 1)
	delta := report.Records[0]
	require.True(t, delta.Complete)
	require.Empty(t, delta.IncompleteReason)
	require.Equal(t, "shortest_path", delta.Postgres.OperatorFamily)
	require.Equal(t, "shortest_path", delta.Neo4j.OperatorFamily)
	require.Equal(t, "opaque", delta.Neo4j.InternalTraversalWork)
	require.True(t, delta.OppositeStartingSides)
	require.NotEmpty(t, delta.Postgres.PlanFingerprint)
	require.NotEmpty(t, delta.Neo4j.PlanFingerprint)
	require.NotEmpty(t, delta.PairSHA256)
	require.NotEmpty(t, report.RankedFindings)
	require.Equal(t, "opposite_starting_side", report.RankedFindings[0].Category)
}

// TestBuildPlanDeltaReportKeepsSourceRevisionsSeparate verifies captures from different source trees cannot silently pair.
func TestBuildPlanDeltaReportKeepsSourceRevisionsSeparate(t *testing.T) {
	postgres := PlanRecord{
		Driver: pgDriverName(), Source: "cases/a.json", Name: "a", WorkloadSHA256: "workload",
		PGPlanFingerprint: "pg-plan", Metadata: testutil.BaselineMetadata{DAWGSVersion: "revision-a"},
	}
	neo4j := PlanRecord{
		Driver: neo4jDriverName(), Source: "cases/a.json", Name: "a", WorkloadSHA256: "workload",
		Neo4jPlanFingerprint: "neo-plan", Metadata: testutil.BaselineMetadata{DAWGSVersion: "revision-b"},
	}
	report, err := buildPlanDeltaReport([]PlanRecord{postgres, neo4j})
	require.NoError(t, err)
	require.Len(t, report.Records, 2)
	require.False(t, report.Records[0].Complete)
	require.False(t, report.Records[1].Complete)
}

// TestBuildPlanDeltaReportRetainsIncompletePairs verifies union-based pairing.
func TestBuildPlanDeltaReportRetainsIncompletePairs(t *testing.T) {
	report, err := buildPlanDeltaReport([]PlanRecord{{
		Driver:            pgDriverName(),
		Source:            "cases/a.json",
		Name:              "a",
		WorkloadSHA256:    "workload",
		PGPlan:            []string{"Result  (cost=0.00..0.01 rows=1 width=4)"},
		PGPlanFingerprint: "pg-plan",
	}})

	require.NoError(t, err)
	require.Len(t, report.Records, 1)
	require.False(t, report.Records[0].Complete)
	require.Equal(t, "missing_neo4j", report.Records[0].IncompleteReason)
	require.NotNil(t, report.Records[0].Postgres)
	require.Nil(t, report.Records[0].Neo4j)
}

// TestBuildPlanDeltaReportRejectsDuplicateBackendSides verifies ambiguous pairing fails closed.
func TestBuildPlanDeltaReportRejectsDuplicateBackendSides(t *testing.T) {
	_, err := buildPlanDeltaReport([]PlanRecord{{Driver: pgDriverName(), WorkloadSHA256: "same"}, {Driver: pgDriverName(), WorkloadSHA256: "same"}})
	require.ErrorContains(t, err, "duplicate PostgreSQL")
}

// TestWritePlanDeltaReportWritesVersionedJSON verifies portable serialization.
func TestWritePlanDeltaReportWritesVersionedJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delta.json")
	require.NoError(t, writePlanDeltaReport(path, PlanDeltaReport{Version: planDeltaSchemaVersion}))
	require.FileExists(t, path)
}

// TestWorkloadFingerprintIgnoresPhysicalValuesButIncludesTypeShape verifies independently loaded backend IDs pair safely.
func TestWorkloadFingerprintIgnoresPhysicalValuesButIncludesTypeShape(t *testing.T) {
	base := CorpusQuery{Source: "cases/a.json", Name: "a", Cypher: "RETURN $id", Params: map[string]any{"id": int64(1)}}
	otherID := base
	otherID.Params = map[string]any{"id": int64(999)}
	otherType := base
	otherType.Params = map[string]any{"id": "1"}

	require.Equal(t, workloadFingerprint(base), workloadFingerprint(otherID))
	require.NotEqual(t, workloadFingerprint(base), workloadFingerprint(otherType))
}

// TestNeo4jPlanFingerprintExcludesProfileMeasurements verifies replay counters do not make an identical plan shape look like a different plan.
func TestNeo4jPlanFingerprintExcludesProfileMeasurements(t *testing.T) {
	firstRows, secondRows := int64(1), int64(99)
	first := &Neo4jPlanNode{
		Operator:   "ProduceResults@neo4j",
		Arguments:  map[string]string{"EstimatedRows": "1", "Rows": "1", "Details": "n"},
		ActualRows: &firstRows,
		DBHits:     &firstRows,
		Children:   []Neo4jPlanNode{{Operator: "NodeByLabelScan", Arguments: map[string]string{"Details": "n:Node"}}},
	}
	second := &Neo4jPlanNode{
		Operator:   "ProduceResults@neo4j@neo4j",
		Arguments:  map[string]string{"EstimatedRows": "1", "Rows": "99", "Details": "n"},
		ActualRows: &secondRows,
		DBHits:     &secondRows,
		Children:   []Neo4jPlanNode{{Operator: "NodeByLabelScan@neo4j", Arguments: map[string]string{"Details": "n:Node"}}},
	}

	require.Equal(t, neo4jPlanFingerprint(first), neo4jPlanFingerprint(second))
	second.Children[0].Operator = "NodeIndexSeek"
	require.NotEqual(t, neo4jPlanFingerprint(first), neo4jPlanFingerprint(second))
}
