//go:build manual_integration

package integration

import (
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/stretchr/testify/require"
)

// pathBatchNeo4jStudyRecord keeps the client measurements comparable to the
// PostgreSQL study without representing unavailable server measurements as zero.
type pathBatchNeo4jStudyRecord struct {
	Backend       string                 `json:"backend"`
	Fixture       string                 `json:"fixture"`
	PropertyBytes int                    `json:"property_bytes_per_entity"`
	Paths         int                    `json:"available_paths"`
	Nodes         int                    `json:"fixture_nodes"`
	Edges         int                    `json:"fixture_edges"`
	Query         string                 `json:"query"`
	Samples       []pathBatchStudySample `json:"samples"`
	WarmupSamples []pathBatchStudySample `json:"warmup_samples"`
	Median        pathBatchStudySample   `json:"median"`
	Memory        pathBatchStudyMemory   `json:"memory"`
}

// TestNeo4jPathBatchReference measures native path values with the same fixtures,
// client boundary and separate memory replay as the PostgreSQL experiment. It
// validates full values and LIMIT membership without machine-dependent timing
// thresholds. Every fixture is rolled back after its measurements.
func TestNeo4jPathBatchReference(t *testing.T) {
	if os.Getenv("PATH_BATCH_STUDY") != "1" {
		t.Skip("set PATH_BATCH_STUDY=1 for the path materialization experiment")
	}
	session := Open(t, Options{RequireDriver: neo4j.DriverName, SkipIfNoConnection: true,
		SkipIfDriverMismatch: true, CleanupMode: CloseOnly, GraphName: "path_batch_experiment",
		ExtraNodeKinds: graph.Kinds{graph.StringKind(pathBatchNodeKind)},
		ExtraEdgeKinds: graph.StringsToKinds([]string{pathBatchEdgeKindA, pathBatchEdgeKindB})})
	var records []pathBatchNeo4jStudyRecord
	for _, scenario := range []struct {
		name  string
		width int
		bytes int
	}{
		{"shared", 4, 0}, {"low_overlap", 512, 0},
		{"shared", 4, 2048}, {"low_overlap", 512, 2048},
		{"shared_scale", 8, 0},
	} {
		t.Run(fmt.Sprintf("%s/properties=%d", scenario.name, scenario.bytes), func(t *testing.T) {
			var fixture *opengraph.Graph
			var count int
			if scenario.name == "low_overlap" {
				fixture, count = pathBatchLowOverlapFixture(scenario.width, scenario.bytes)
			} else {
				fixture, count = pathBatchSharedFixture(scenario.width, scenario.bytes)
			}
			require.NoError(t, session.WithRollbackFixture(t, fixture, true, func(tx graph.Transaction, ids opengraph.IDMap) error {
				baseCypher := "MATCH p=(s:PathBatchNode)-[:PathBatchA|PathBatchB*3..3]->(e) WHERE id(s)=$root RETURN p"
				parameters := map[string]any{"root": ids["root"].Int64()}
				queries := []pathBatchStudyQuery{
					{"current_full", baseCypher, parameters, count},
					{"current_limit1", baseCypher + " LIMIT 1", parameters, 1},
				}
				expected := pathBatchFullValues(t, tx.Raw(baseCypher, parameters))
				require.Len(t, expected, count)
				for _, query := range queries {
					actual := pathBatchFullValues(t, tx.Raw(query.sql, query.params))
					require.Len(t, actual, query.rows, query.name)
					if query.rows == count {
						require.Equal(t, expected, actual, query.name)
					} else {
						index := sort.SearchStrings(expected, actual[0])
						require.True(t, index < len(expected) && expected[index] == actual[0], query.name)
					}
				}
				samples := make([][]pathBatchStudySample, len(queries))
				warmups := make([][]pathBatchStudySample, len(queries))
				// Match PostgreSQL's six warmup cycles and five measured cycles.
				// Preserve the warmup samples so transitions remain reviewable.
				for round := range 11 {
					for offset := range len(queries) {
						index := (round + offset) % len(queries)
						sample := pathBatchStudyConsume(t, tx, queries[index])
						if round >= 6 {
							samples[index] = append(samples[index], sample)
						} else {
							warmups[index] = append(warmups[index], sample)
						}
					}
				}
				for index, query := range queries {
					record := pathBatchNeo4jStudyRecord{Backend: neo4j.DriverName,
						Fixture: scenario.name, PropertyBytes: scenario.bytes,
						Paths: count, Nodes: len(fixture.Nodes), Edges: len(fixture.Edges), Query: query.name,
						Samples: samples[index], WarmupSamples: warmups[index], Median: pathBatchStudyMedian(samples[index]),
						Memory: pathBatchStudyClientMemory(t, tx, query)}
					records = append(records, record)
					t.Logf("%s: first=%.3fms total=%.3fms allocated=%.2fMiB sampled-extra-heap=%.2fMiB",
						query.name, record.Median.FirstRowMS, record.Median.TotalMS,
						float64(record.Memory.TotalAllocBytes)/(1<<20), float64(record.Memory.SampledExtraHeapBytes)/(1<<20))
				}
				return nil
			}))
		})
	}
	pathBatchStudyWrite(t, "neo4j-results.json", records)
}
