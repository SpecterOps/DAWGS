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

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/testutil"
	"github.com/stretchr/testify/require"
)

// TestPostgresProductionManifestBuildsExactGuardedOptions verifies postgres production manifest builds exact guarded options behavior.
func TestPostgresProductionManifestBuildsExactGuardedOptions(t *testing.T) {
	query := "MATCH p = allShortestPaths((s)-[:Traverse*1..8]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p"
	digest := strings.Repeat("0", 64)
	manifest := PromotionManifest{
		Version:           promotionManifestVersion,
		Candidate:         "ASP-I1-U-DAG+MAT-M0",
		SelectorVersion:   "asp-i1-test-v1",
		ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor:  "ASP-A1-DAG",
		SourceCommit:      "commit",
		SourceSHA256:      digest,
		BinarySHA256:      digest,
		CorpusSHA256:      digest,
		Caps:              map[string]int64{"state_limit": 10, "predecessor_limit": 20, "enumeration_limit": 30, "output_bytes_limit": 40},
		Buckets: []PromotionBucket{{
			Name:                  "outbound-depth8",
			QuerySHA256:           []string{pg.TraversalPolicyQuerySHA256(query)},
			Direction:             "outbound",
			ObservationMode:       "all_paths",
			MinimumDepth:          1,
			MaximumDepth:          8,
			RelationshipKindCount: 1,
			QualificationSplit:    []string{"training", "holdout"},
		}},
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

func TestPostgresProductionManifestRejectsV1GuardedDistanceActivation(t *testing.T) {
	query := "MATCH p = shortestPath((s)<-[:Traverse*1..32]-(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN length(p)"
	digest := strings.Repeat("0", 64)
	manifest := PromotionManifest{
		Version: promotionManifestVersion, Candidate: string(optimize.ShortestPathExecutorI2GuardedDistance),
		SelectorVersion: optimize.ShortestPathSelectorStaticV8HiddenFanIn, ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor: string(optimize.ShortestPathExecutorS4CanonicalDistance), SourceCommit: "commit",
		SourceSHA256: digest, BinarySHA256: digest, CorpusSHA256: digest,
		Caps: spI2PromotionCaps(),
		Buckets: []PromotionBucket{{
			Name: "hidden-fan-in-depth32", QuerySHA256: []string{pg.TraversalPolicyQuerySHA256(query)},
			Direction: "inbound", ObservationMode: "distance", MinimumDepth: 1, MaximumDepth: 32,
			RelationshipKindCount: 1, QualificationSplit: []string{"training", "holdout"},
		}},
	}
	raw, err := json.Marshal(manifest)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "manifest.json")
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	runner := &postgresSQLRunner{}
	require.ErrorContains(t, runner.setProductionManifest(path), "terminally rejected")
}

// TestPostgresProductionManifestRequiresStaticV6CanonicalInboundBucket verifies postgres production manifest requires static v6 canonical inbound bucket behavior.
func TestPostgresProductionManifestRequiresStaticV6CanonicalInboundBucket(t *testing.T) {
	query := "MATCH p = shortestPath((s)<-[:Traverse*1..64]-(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p"
	digest := strings.Repeat("0", 64)
	base := PromotionManifest{
		Version:           promotionManifestVersion,
		Candidate:         string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		SelectorVersion:   optimize.ShortestPathSelectorStaticV6,
		ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor:  string(optimize.ShortestPathExecutorS4CanonicalWitness),
		SourceCommit:      "commit",
		SourceSHA256:      digest,
		BinarySHA256:      digest,
		CorpusSHA256:      digest,
		Caps:              map[string]int64{"state_limit": 10, "predecessor_limit": 20, "enumeration_limit": 30, "output_bytes_limit": 40},
		Buckets: []PromotionBucket{{
			Name:                  "canonical-inbound-depth64",
			QuerySHA256:           []string{pg.TraversalPolicyQuerySHA256(query)},
			Direction:             "inbound",
			ObservationMode:       "one_path",
			MinimumDepth:          1,
			MaximumDepth:          64,
			RelationshipKindCount: 1,
			QualificationSplit:    []string{"training", "holdout"},
		}},
	}
	write := func(t *testing.T, manifest PromotionManifest) string {
		t.Helper()
		raw, err := json.Marshal(manifest)
		require.NoError(t, err)
		path := filepath.Join(t.TempDir(), "manifest.json")
		require.NoError(t, os.WriteFile(path, raw, 0o600))
		return path
	}

	runner := &postgresSQLRunner{}
	require.NoError(t, runner.setProductionManifest(write(t, base)))
	options, err := runner.productionOptions(query)
	require.NoError(t, err)
	require.Equal(t, optimize.ShortestPathSelectorStaticV6, options.SelectorVersion)
	require.Equal(t, int64(64), options.AuthorizedBucket.MaximumDepth)

	tests := map[string]func(*PromotionManifest){
		"selector": func(manifest *PromotionManifest) { manifest.SelectorVersion = "sp-static-v5-contained" },
		"outbound": func(manifest *PromotionManifest) { manifest.Buckets[0].Direction = "outbound" },
		"maximum":  func(manifest *PromotionManifest) { manifest.Buckets[0].MaximumDepth = 63 },
		"kinds":    func(manifest *PromotionManifest) { manifest.Buckets[0].RelationshipKindCount = 2 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			manifest := base
			manifest.Caps = clonePromotionCaps(base.Caps)
			manifest.Buckets = clonePromotionBuckets(base.Buckets)
			mutate(&manifest)
			require.Error(t, (&postgresSQLRunner{}).setProductionManifest(write(t, manifest)))
		})
	}
}

// TestPostgresProductionManifestBuildsOrientationOptionsWithoutShortestPathFields verifies postgres production manifest builds orientation options without shortest path fields behavior.
func TestPostgresProductionManifestBuildsOrientationOptionsWithoutShortestPathFields(t *testing.T) {
	query := "MATCH (r)-[:Expand*0..16]->()-[:Suffix]->(e) WHERE id(r) = $root_id RETURN id(e)"
	digest := strings.Repeat("0", 64)
	manifest := PromotionManifest{
		Version:           promotionManifestVersion,
		Candidate:         string(optimize.ExpansionSearchPolicyOrientationProbeV1),
		SelectorVersion:   "orientation-probe-v1",
		ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor:  string(optimize.ExpansionSearchStepwiseForward),
		SourceCommit:      "commit",
		SourceSHA256:      digest,
		BinarySHA256:      digest,
		CorpusSHA256:      digest,
		Caps:              orientationPromotionCaps(),
		Buckets: []PromotionBucket{{
			Name:                  "outbound-fixed-suffix",
			QuerySHA256:           []string{pg.TraversalPolicyQuerySHA256(query)},
			Direction:             "outbound",
			ObservationMode:       "endpoint_ids",
			MinimumDepth:          0,
			MaximumDepth:          16,
			RelationshipKindCount: 1,
			QualificationSplit:    []string{"training", "holdout"},
		}},
	}
	raw, err := json.Marshal(manifest)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "manifest.json")
	require.NoError(t, os.WriteFile(path, raw, 0o600))

	runner := &postgresSQLRunner{}
	require.NoError(t, runner.setProductionManifest(path))
	options, err := runner.productionOptions(query)
	require.NoError(t, err)
	require.True(t, options.EnableExpansionOrientation)
	require.Empty(t, options.ShortestPathExecutor)
	require.Nil(t, options.ShortestPathCaps)
	require.Equal(t, int64(16), options.AuthorizedBucket.MaximumDepth)
	require.Equal(t, "orientation-probe-v1", options.SelectorVersion)
}

// TestPostgresProductionManifestRejectsNonExactOrientationContract verifies postgres production manifest rejects non exact orientation contract behavior.
func TestPostgresProductionManifestRejectsNonExactOrientationContract(t *testing.T) {
	digest := strings.Repeat("0", 64)
	base := PromotionManifest{
		Version:           promotionManifestVersion,
		Candidate:         string(optimize.ExpansionSearchPolicyOrientationProbeV1),
		SelectorVersion:   "orientation-probe-v1",
		ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor:  string(optimize.ExpansionSearchStepwiseForward),
		SourceCommit:      "commit",
		SourceSHA256:      digest,
		BinarySHA256:      digest,
		CorpusSHA256:      digest,
		Caps:              orientationPromotionCaps(),
		Buckets: []PromotionBucket{{
			Name:               "fixed-suffix",
			QuerySHA256:        []string{digest},
			QualificationSplit: []string{"training", "holdout"},
		}},
	}
	tests := []struct {
		// name retains the name while anonymous record is assembled or evaluated.
		name string
		// mutate retains the mutate while anonymous record is assembled or evaluated.
		mutate func(*PromotionManifest)
		// err retains the err while anonymous record is assembled or evaluated.
		err string
	}{
		{
			name:   "fallback",
			mutate: func(manifest *PromotionManifest) { manifest.FallbackExecutor = "EXPANSION-SUFFIX-SEEDED-REVERSE" },
			err:    "unsupported candidate/fallback pair",
		},
		{
			name:   "extra cap",
			mutate: func(manifest *PromotionManifest) { manifest.Caps["extra_limit"] = 1 },
			err:    "orientation-probe-v1 requires exactly four immutable caps",
		},
		{
			name:   "missing cap",
			mutate: func(manifest *PromotionManifest) { delete(manifest.Caps, "root_row_limit") },
			err:    "orientation-probe-v1 requires exactly four immutable caps",
		},
		{
			name:   "wrong cap",
			mutate: func(manifest *PromotionManifest) { manifest.Caps["state_limit"]-- },
			err:    "orientation-probe-v1 cap state_limit must equal 4096",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manifest := base
			manifest.Caps = clonePromotionCaps(base.Caps)
			test.mutate(&manifest)
			raw, err := json.Marshal(manifest)
			require.NoError(t, err)
			path := filepath.Join(t.TempDir(), "manifest.json")
			require.NoError(t, os.WriteFile(path, raw, 0o600))
			err = (&postgresSQLRunner{}).setProductionManifest(path)
			require.ErrorContains(t, err, test.err)
		})
	}
}

// TestPostgresProductionManifestCarriesOrientationProbeV2IntoTranslation verifies
// provisional production measurement cannot silently fall back to v1.
func TestPostgresProductionManifestCarriesOrientationProbeV2IntoTranslation(t *testing.T) {
	query := "MATCH (r)-[:Expand*0..16]->()-[:Suffix]->(e) WHERE id(r) = $root_id RETURN id(e)"
	digest := strings.Repeat("0", 64)
	manifest := PromotionManifest{
		Version:           promotionManifestVersion,
		Candidate:         string(optimize.ExpansionSearchPolicyOrientationProbeV2),
		SelectorVersion:   string(optimize.ExpansionSearchPolicyOrientationProbeV2),
		ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor:  string(optimize.ExpansionSearchStepwiseForward),
		SourceCommit:      "commit",
		SourceSHA256:      digest,
		BinarySHA256:      digest,
		CorpusSHA256:      digest,
		Caps:              orientationPromotionCaps(),
		Buckets: []PromotionBucket{{
			Name:                  "outbound-fixed-suffix-v2",
			QuerySHA256:           []string{pg.TraversalPolicyQuerySHA256(query)},
			Direction:             "outbound",
			ObservationMode:       "endpoint_ids",
			MinimumDepth:          0,
			MaximumDepth:          16,
			RelationshipKindCount: 1,
			QualificationSplit:    []string{"training", "holdout"},
		}},
	}
	raw, err := json.Marshal(manifest)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "manifest.json")
	require.NoError(t, os.WriteFile(path, raw, 0o600))

	runner := &postgresSQLRunner{}
	require.NoError(t, runner.setProductionManifest(path))
	options, err := runner.productionOptions(query)
	require.NoError(t, err)
	require.True(t, options.EnableExpansionOrientation)
	require.Equal(t, optimize.ExpansionSearchPolicyOrientationProbeV2, options.ExpansionOrientationPolicy)
	require.Equal(t, string(optimize.ExpansionSearchPolicyOrientationProbeV2), options.SelectorVersion)
}

// TestPostgresProductionManifestSQLAnchorIsTwoPass verifies a provisional
// manifest may omit the SQL anchor only to derive it, while a populated anchor
// is checked against the exact SQL emitted by production translation.
func TestPostgresProductionManifestSQLAnchorIsTwoPass(t *testing.T) {
	query := "MATCH p = shortestPath((s)<-[:Traverse*1..64]-(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p"
	digest := strings.Repeat("0", 64)
	manifest := PromotionManifest{
		Version: promotionManifestVersion, Candidate: string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		SelectorVersion: optimize.ShortestPathSelectorStaticV6, ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor: string(optimize.ShortestPathExecutorS4CanonicalWitness), SourceCommit: "commit",
		SourceSHA256: digest, BinarySHA256: digest, CorpusSHA256: digest,
		Caps: map[string]int64{"state_limit": 10, "predecessor_limit": 20, "enumeration_limit": 30, "output_bytes_limit": 40},
		Buckets: []PromotionBucket{{
			Name: "canonical-inbound-depth64", QuerySHA256: []string{pg.TraversalPolicyQuerySHA256(query)},
			Direction: "inbound", ObservationMode: "one_path", MinimumDepth: 1, MaximumDepth: 64,
			RelationshipKindCount: 1, QualificationSplit: []string{"training", "holdout"},
		}},
	}
	write := func(value PromotionManifest) string {
		raw, err := json.Marshal(value)
		require.NoError(t, err)
		path := filepath.Join(t.TempDir(), "manifest.json")
		require.NoError(t, os.WriteFile(path, raw, 0o600))
		return path
	}

	preflight := &postgresSQLRunner{}
	require.NoError(t, preflight.setProductionManifest(write(manifest)))
	require.Empty(t, preflight.productionManifest.OperationalCandidateSQLSHA256)

	manifest.OperationalCandidateSQLSHA256 = strings.Repeat("f", 64)
	formal := &postgresSQLRunner{}
	require.NoError(t, formal.setProductionManifest(write(manifest)))
	require.ErrorContains(t, verifyProductionManifestSQLAnchor(formal.productionManifest, "select 1"), "does not match provisional manifest anchor")
	formal.productionManifest.OperationalCandidateSQLSHA256 = sqlFingerprint("select 1")
	require.NoError(t, verifyProductionManifestSQLAnchor(formal.productionManifest, "select 1"))

	multipleQueries := manifest
	multipleQueries.Buckets = clonePromotionBuckets(manifest.Buckets)
	multipleQueries.Buckets[0].QuerySHA256 = append(multipleQueries.Buckets[0].QuerySHA256, strings.Repeat("e", 64))
	require.ErrorContains(t, (&postgresSQLRunner{}).setProductionManifest(write(multipleQueries)), "requires exactly one authorized query digest")

	manifest.OperationalCandidateSQLSHA256 = "NOT-A-DIGEST"
	require.ErrorContains(t, (&postgresSQLRunner{}).setProductionManifest(write(manifest)), "must be a lowercase SHA-256 digest")
}

func TestPostgresProductionManifestRejectsAmbiguousSetsAndJSON(t *testing.T) {
	query := "MATCH p = shortestPath((s)<-[:Traverse*1..64]-(e)) RETURN p"
	digest := strings.Repeat("a", 64)
	base := PromotionManifest{
		Version: promotionManifestVersion, Candidate: string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		SelectorVersion: optimize.ShortestPathSelectorStaticV6, ExecutionBoundary: "guarded_dual_arm",
		FallbackExecutor: string(optimize.ShortestPathExecutorS4CanonicalWitness), SourceCommit: "commit",
		SourceSHA256: digest, BinarySHA256: digest, CorpusSHA256: digest,
		Caps:    map[string]int64{"state_limit": 10, "predecessor_limit": 20, "enumeration_limit": 30, "output_bytes_limit": 40},
		Buckets: []PromotionBucket{{Name: "qualified-query", QuerySHA256: []string{pg.TraversalPolicyQuerySHA256(query)}, Direction: "inbound", ObservationMode: "one_path", MinimumDepth: 1, MaximumDepth: 64, RelationshipKindCount: 1, QualificationSplit: []string{"training", "holdout"}}},
	}
	write := func(raw []byte) string {
		path := filepath.Join(t.TempDir(), "manifest.json")
		require.NoError(t, os.WriteFile(path, raw, 0o600))
		return path
	}
	encode := func(manifest PromotionManifest) []byte {
		raw, err := json.Marshal(manifest)
		require.NoError(t, err)
		return raw
	}

	duplicateSplit := base
	duplicateSplit.Buckets = clonePromotionBuckets(base.Buckets)
	duplicateSplit.Buckets[0].QualificationSplit = []string{"training", "training", "holdout"}
	require.ErrorContains(t, (&postgresSQLRunner{}).setProductionManifest(write(encode(duplicateSplit))), "exactly one training and one holdout")

	duplicateQuery := base
	duplicateQuery.Buckets = clonePromotionBuckets(base.Buckets)
	duplicateQuery.Buckets[0].QuerySHA256 = append(duplicateQuery.Buckets[0].QuerySHA256, duplicateQuery.Buckets[0].QuerySHA256[0])
	require.ErrorContains(t, (&postgresSQLRunner{}).setProductionManifest(write(encode(duplicateQuery))), "authorized more than once")

	duplicateKey := strings.Replace(string(encode(base)), `"version":2`, `"version":2,"version":2`, 1)
	require.ErrorContains(t, (&postgresSQLRunner{}).setProductionManifest(write([]byte(duplicateKey))), "duplicate JSON object key")
}

// TestPostgresReadTransactionOptionsMatchEveryStableSnapshotMode verifies postgres read transaction options match every stable snapshot mode behavior.
func TestPostgresReadTransactionOptionsMatchEveryStableSnapshotMode(t *testing.T) {
	require.Empty(t, (&postgresSQLRunner{}).readTransactionOptions())

	for name, runner := range map[string]*postgresSQLRunner{
		"explicit benchmark flag": {repeatableRead: true},
		"production manifest":     {productionManifest: &PromotionManifest{}},
	} {
		t.Run(name, func(t *testing.T) {
			options := runner.readTransactionOptions()
			require.Len(t, options, 1)

			pgConfig := &pg.Config{}
			transactionConfig := &graph.TransactionConfig{DriverConfig: pgConfig}
			options[0](transactionConfig)
			require.Equal(t, pgx.RepeatableRead, pgConfig.Options.IsoLevel)
			require.Equal(t, pgx.ReadWrite, pgConfig.Options.AccessMode)
		})
	}
}

func TestSuffixReverseGuardToolOptionsAreExecutableAndAttested(t *testing.T) {
	options := translate.ToolOptions{EnableExpansionSuffixReverseGuard: true}
	require.True(t, hasForcedToolOptions(options))

	outcome := translate.TargetLoweringOutcome{
		TargetKind:    "traversal",
		Family:        "fixed_suffix_expansion",
		Candidate:     string(optimize.ExpansionSearchSuffixSeededReverse),
		EmittedPolicy: string(optimize.ExpansionSearchPolicySuffixReverseGuardV1),
		Selected:      string(optimize.ExpansionSearchStepwiseForward),
	}
	require.Equal(t, string(optimize.ExpansionSearchSuffixSeededReverse), timedRuntimeAttestationIdentity(translate.Result{
		Optimization: translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{outcome}},
	}))
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
