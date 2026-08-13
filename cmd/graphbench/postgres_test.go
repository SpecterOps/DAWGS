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
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/testutil"
	"github.com/stretchr/testify/require"
)

func TestPostgresProductionManifestBuildsExactGuardedOptions(t *testing.T) {
	query := "MATCH p = allShortestPaths((s)-[:Traverse*1..8]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p"
	digest := strings.Repeat("0", 64)
	manifest := PromotionManifest{
		Version: promotionManifestVersion, Candidate: "ASP-I1-U-DAG+MAT-M0", SelectorVersion: "asp-i1-test-v1",
		ExecutionBoundary: "guarded_dual_arm", FallbackExecutor: "ASP-A1-DAG",
		SourceCommit: "commit", SourceSHA256: digest, BinarySHA256: digest, CorpusSHA256: digest,
		Caps:    map[string]int64{"state_limit": 10, "predecessor_limit": 20, "enumeration_limit": 30, "output_bytes_limit": 40},
		Buckets: []PromotionBucket{{Name: "outbound-depth8", QuerySHA256: []string{pg.TraversalPolicyQuerySHA256(query)}, Direction: "outbound", ObservationMode: "all_paths", MinimumDepth: 1, MaximumDepth: 8, RelationshipKindCount: 1, QualificationSplit: []string{"training", "holdout"}}},
	}
	raw, err := json.Marshal(manifest)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "manifest.json")
	require.NoError(t, os.WriteFile(path, raw, 0o600))

	runner := &postgresSQLRunner{}
	require.NoError(t, runner.setProductionManifest(path))
	options, err := runner.productionOptions(query)
	require.NoError(t, err)
	require.Equal(t, "ASP-I1-U-DAG+MAT-M0", string(options.ShortestPathExecutor))
	require.Equal(t, int64(10), options.ShortestPathCaps.StateLimit)
	require.Equal(t, int64(8), options.AuthorizedBucket.MaximumDepth)
	require.Equal(t, "asp-i1-test-v1", options.SelectorVersion)
	_, err = runner.productionOptions(query + " RETURN 1")
	require.ErrorContains(t, err, "absent from the provisional production manifest")
}

// TestResolveCaseParams verifies that scalar, explicit-list, and generated-list fixture keys become ordered int64 IDs without disturbing ordinary parameters.
func TestResolveCaseParams(t *testing.T) {
	params, err := resolveCaseParams(ScaleCase{
		Params: map[string]any{
			"name": "value",
		},
		NodeParams: map[string]string{
			"start_id": "n1",
		},
		NodeListParams: map[string][]string{
			"end_ids": {"n2", "n1"},
		},
		GeneratedNodeListParams: map[string]testutil.GeneratedNodeListParam{
			"generated_ids": {
				Prefix:  "generated",
				Count:   2,
				Include: []string{"n2"},
			},
		},
	}, opengraph.IDMap{
		"n1":           graph.ID(42),
		"n2":           graph.ID(84),
		"generated-00": graph.ID(126),
		"generated-01": graph.ID(168),
	})

	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"name":          "value",
		"start_id":      int64(42),
		"end_ids":       []int64{84, 42},
		"generated_ids": []int64{84, 126, 168},
	}, params)
}

// TestScaleCaseDecodesTypedDatetimeParameter verifies that the corpus JSON datetime envelope becomes a UTC time value rather than an untyped map.
func TestScaleCaseDecodesTypedDatetimeParameter(t *testing.T) {
	var testCase ScaleCase
	require.NoError(t, json.Unmarshal([]byte(`{
		"name":"typed-time",
		"dataset":"base",
		"category":"lookup",
		"cypher":"MATCH (n) WHERE n.lastseen < $threshold RETURN n",
		"params":{"threshold":{"$type":"datetime","value":"2026-01-02T03:04:05Z"}},
		"candidate_modes":["postgres_sql"]
	}`), &testCase))

	require.Equal(t, time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC), testCase.Params["threshold"])
}

// TestParsePostgresPlanMetrics verifies parsing of planning/execution milliseconds and every shared, local, and temporary buffer counter from text plans.
func TestParsePostgresPlanMetrics(t *testing.T) {
	metrics := parsePostgresPlanMetrics([]string{
		"Nested Loop  (actual rows=1 loops=1)",
		"  Buffers: shared hit=12 read=3 dirtied=2 written=1, local hit=7 read=6 dirtied=5 written=4, temp read=3 written=2",
		"Planning Time: 1.250 ms",
		"Execution Time: 9.750 ms",
	})

	require.NotNil(t, metrics.PlanningMS)
	require.Equal(t, 1.25, *metrics.PlanningMS)
	require.NotNil(t, metrics.ExecutionMS)
	require.Equal(t, 9.75, *metrics.ExecutionMS)
	require.Equal(t, Buffers{
		SharedHit:     12,
		SharedRead:    3,
		SharedDirtied: 2,
		SharedWritten: 1,
		LocalHit:      7,
		LocalRead:     6,
		LocalDirtied:  5,
		LocalWritten:  4,
		TempRead:      3,
		TempWritten:   2,
	}, metrics.Buffers)
}

// TestGeneratedDatasetVariantsAreParameterizedAndRepeatable verifies deterministic generation for equal names and propagation of configured payload size into fixed-suffix nodes.
func TestGeneratedDatasetVariantsAreParameterizedAndRepeatable(t *testing.T) {
	first := generatedDataset("generated_shortest_paths_d4_f16")
	second := generatedDataset("generated_shortest_paths_d4_f16")
	require.NotNil(t, first)
	require.Equal(t, first, second)

	fixedSuffix := generatedDataset("generated_fixed_suffix_expansion_d2_f10_v2_p4096")
	require.NotNil(t, fixedSuffix)
	require.Contains(t, fixedSuffix.Nodes[0].Properties["payload"], "xxxx")
}

// TestCompactBidirectionalRunsRequireRepeatableSnapshot verifies runner setup
// opts into stable snapshots exactly when a forced or reference B1/B2 arm can run.
func TestCompactBidirectionalRunsRequireRepeatableSnapshot(t *testing.T) {
	require.True(t, compactBidirectionalSnapshotRequired(false, nil, "SP-B1-C-ALT-NODE-D"))
	require.True(t, compactBidirectionalSnapshotRequired(false, nil, "SP-B2-C-MIN-LEVEL-WE+MAT-M0"))
	require.True(t, compactBidirectionalSnapshotRequired(false, nil, "ASP-B1-DAG-ALT-NODE"))
	require.True(t, compactBidirectionalSnapshotRequired(false, nil, "ASP-B2-DAG-MIN-LEVEL"))
	require.True(t, compactBidirectionalSnapshotRequired(true, nil, ""))
	require.True(t, compactBidirectionalSnapshotRequired(true, []string{"sp_b1_strict_alternating_distance"}, ""))
	require.True(t, compactBidirectionalSnapshotRequired(true, []string{"asp_b2_bidirectional_dag_smaller_frontier_m0"}, ""))
	require.False(t, compactBidirectionalSnapshotRequired(false, nil, "SP-S4-C-D"))
	require.False(t, compactBidirectionalSnapshotRequired(true, []string{"s4_canonical_source_distance"}, ""))
}

// TestFixtureMetadataIncludesCardinalityAndChecksum verifies that generated fixtures expose their configuration, nonzero entity counts, and a full SHA-256 content digest.
func TestFixtureMetadataIncludesCardinalityAndChecksum(t *testing.T) {
	metadata, err := fixtureMetadata("unused", "generated_shortest_paths_d4_f16")
	require.NoError(t, err)
	require.Equal(t, "generated_shortest_paths_d4_f16", metadata.Configuration)
	require.Positive(t, metadata.NodeCount)
	require.Positive(t, metadata.EdgeCount)
	require.Len(t, metadata.Checksum, 64)
}
