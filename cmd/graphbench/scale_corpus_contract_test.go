// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
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
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/testutil"
	"github.com/stretchr/testify/require"
)

// scaleCorpusRequiredIDs lists representative corpus cases required by the regression contract.
var scaleCorpusRequiredIDs = []string{
	"REC-01", "REC-02", "REC-04", "REC-06", "REC-08",
	"TRUST-01", "TRUST-02",
	"PRUNE-01", "PRUNE-02", "PRUNE-03", "PRUNE-04",
	"HOP-01", "HOP-02", "HOP-03", "HOP-04", "HOP-05", "HOP-07", "HOP-09",
	"SCAN-01", "SCAN-02", "SCAN-03", "SCAN-04", "SCAN-05", "SCAN-07", "SCAN-08",
	"LOOKUP-02", "LOOKUP-04", "LOOKUP-05", "LOOKUP-09", "LOOKUP-11", "LOOKUP-13", "LOOKUP-15", "LOOKUP-16",
}

// TestGeneratedScaleCasesParseAndExecuteRealBackends verifies that each generated family has parseable Cypher and an explicit support decision for PostgreSQL and Neo4j.
func TestGeneratedScaleCasesParseAndExecuteRealBackends(t *testing.T) {
	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)

	covered := map[string]int{}
	for _, testCase := range corpus.Cases {
		if !strings.HasPrefix(testCase.Dataset, "generated_shortest_paths_") && !strings.HasPrefix(testCase.Dataset, "generated_fixed_suffix_expansion_") && !strings.HasPrefix(testCase.Dataset, "generated_endpoint_seeded_expansion_") {
			continue
		}
		_, err := frontend.ParseCypher(frontend.NewContext(), testCase.Cypher)
		require.NoError(t, err, testCase.Name)
		_, postgresUnsupported := testCase.UnsupportedReason(ModePostgresSQL)
		_, neo4jUnsupported := testCase.UnsupportedReason(ModeNeo4j)
		require.True(t, testCase.Supports(ModePostgresSQL) || postgresUnsupported, testCase.Name)
		require.True(t, testCase.Supports(ModeNeo4j) || neo4jUnsupported, testCase.Name)
		if strings.HasPrefix(testCase.Dataset, "generated_shortest_paths_") {
			covered["shortest"]++
		} else if strings.HasPrefix(testCase.Dataset, "generated_fixed_suffix_expansion_") {
			covered["fixed_suffix_expansion"]++
		} else {
			covered["endpoint_seeded_expansion"]++
		}
	}
	require.Positive(t, covered["shortest"])
	require.Positive(t, covered["fixed_suffix_expansion"])
	require.Positive(t, covered["endpoint_seeded_expansion"])
}

// TestGeneratedFixedSuffixV3OrientationCorpusFreezesTrainingAndHoldoutMatrices
// verifies exact cohort sizes, independent training dimensions, fresh holdout
// depths, canonical cohort tags, and graph-derived result cardinalities.
func TestGeneratedFixedSuffixV3OrientationCorpusFreezesTrainingAndHoldoutMatrices(t *testing.T) {
	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)

	type fraction struct{ reachable, fanout int }
	trainingDepths := map[int]bool{}
	trainingFanouts := map[int]bool{}
	trainingFractions := map[fraction]bool{}
	trainingDisconnected := map[int]bool{}
	trainingFanIn := map[int]bool{}
	trainingMultiplicity := map[int]bool{}
	trainingRoots := map[int]bool{}
	trainingObservations := map[string]bool{}
	trainingBoundaryControls := map[[2]bool]bool{}
	trainingZeroDepth := map[bool]bool{}
	trainingPayloads := map[int]bool{}
	holdoutDepths := map[int]bool{}
	declaredCohort := map[performanceKey]string{}
	trainingCount, holdoutCount := 0, 0

	for _, testCase := range corpus.Cases {
		if !strings.HasPrefix(testCase.Dataset, "generated_fixed_suffix_expansion_v3_") {
			continue
		}

		config, ok := parseFixedSuffixExpansionV3DatasetName(testCase.Dataset)
		require.True(t, ok, testCase.Name)
		require.NotNil(t, testCase.Expected.RowCount, testCase.Name)
		require.NotNil(t, testCase.Shape.MaxDepth, testCase.Name)
		require.Equal(t, config.ExpansionDepth, *testCase.Shape.MaxDepth, testCase.Name)
		if testCase.Expected.ResultKind == "path_set" {
			require.Len(t, testCase.Expected.PathRows, int(*testCase.Expected.RowCount),
				testCase.Name+" must predeclare every stable path observation")
			require.True(t, newCaseResult(testCase, ModePostgresSQL, testCase.Params).StableObservation, testCase.Name)
		}
		declaredCohort[performanceKey{dataset: testCase.Dataset, name: testCase.Name, backend: ModePostgresSQL}] = testCase.Shape.QualificationSplit

		metadata, err := fixtureMetadata("unused", testCase.Dataset)
		require.NoError(t, err, testCase.Name)
		require.NotNil(t, metadata.FixedSuffixExpansion, testCase.Name)
		require.Equal(t, metadata.FixedSuffixExpansion.CompleteOutputTrails, *testCase.Expected.RowCount, testCase.Name)

		trainingTag := slices.Contains(testCase.Tags, "orientation-v2-training")
		holdoutTag := slices.Contains(testCase.Tags, "orientation-v2-holdout")
		require.NotEqual(t, trainingTag, holdoutTag, testCase.Name)
		switch testCase.Shape.QualificationSplit {
		case "training":
			require.True(t, trainingTag, testCase.Name)
			require.False(t, holdoutTag, testCase.Name)
			trainingCount++
			trainingDepths[config.ExpansionDepth] = true
			trainingFanouts[config.Fanout] = true
			trainingFractions[fraction{reachable: *config.ExactReachableSuffixSources, fanout: config.Fanout}] = true
			trainingDisconnected[config.DisconnectedSuffixSources] = true
			trainingFanIn[config.ReverseFanIn] = true
			trainingMultiplicity[config.SuffixPathsPerBoundary] = true
			trainingRoots[config.RootMatchCount] = true
			observation := "endpoint"
			if testCase.Observes.Paths {
				observation = "path"
			}
			trainingObservations[observation] = true
			trainingBoundaryControls[[2]bool{config.AddProductiveBoundaryCycle, config.AddProductiveBoundarySelfLoop}] = true
			trainingZeroDepth[*config.RootHasZeroDepthSuffix] = true
			trainingPayloads[config.PropertyPayloadSize] = true
		case "holdout":
			require.False(t, trainingTag, testCase.Name)
			require.True(t, holdoutTag, testCase.Name)
			holdoutCount++
			holdoutDepths[config.ExpansionDepth] = true
		default:
			t.Fatalf("%s has invalid v3 orientation split %q", testCase.Name, testCase.Shape.QualificationSplit)
		}
	}

	require.Equal(t, 8, trainingCount)
	require.Equal(t, 4, holdoutCount)
	require.GreaterOrEqual(t, len(trainingDepths), 4)
	require.GreaterOrEqual(t, len(trainingFanouts), 4)
	require.GreaterOrEqual(t, len(trainingFractions), 4)
	require.GreaterOrEqual(t, len(trainingDisconnected), 4)
	require.GreaterOrEqual(t, len(trainingFanIn), 3)
	require.GreaterOrEqual(t, len(trainingMultiplicity), 3)
	require.GreaterOrEqual(t, len(trainingRoots), 4)
	require.Equal(t, map[string]bool{"endpoint": true, "path": true}, trainingObservations)
	require.Equal(t, map[bool]bool{false: true, true: true}, trainingZeroDepth)
	require.GreaterOrEqual(t, len(trainingPayloads), 3)
	for _, combination := range [][2]bool{{false, false}, {true, false}, {false, true}, {true, true}} {
		require.True(t, trainingBoundaryControls[combination], "missing cycle/self-loop combination %v", combination)
	}
	require.Equal(t, map[int]bool{7: true, 11: true, 13: true, 15: true}, holdoutDepths)
	for depth := range holdoutDepths {
		require.False(t, trainingDepths[depth], "holdout depth %d is already present in training", depth)
	}
	require.Len(t, orientationV2CanonicalCases, len(declaredCohort))
	for _, frozen := range orientationV2CanonicalCases {
		require.Equal(t, frozen.split, declaredCohort[performanceKey{dataset: frozen.dataset, name: frozen.name, backend: ModePostgresSQL}], frozen.name)
	}
	canonical, err := canonicalOrientationV2Cohort()
	require.NoError(t, err)
	_, trainingSelection, err := selectScaleCorpus(corpus, CorpusSelectors{Tags: []string{"orientation-v2-training"}})
	require.NoError(t, err)
	require.Equal(t, canonical.trainingDeclarationSHA256, trainingSelection.DeclarationSHA256)
	_, confirmationSelection, err := selectScaleCorpus(corpus, CorpusSelectors{Tags: []string{"orientation-v2-training", "orientation-v2-holdout"}})
	require.NoError(t, err)
	require.Equal(t, canonical.declarationSHA256, confirmationSelection.DeclarationSHA256)
}

// TestEndpointSeededExpansionCorpusCoversGuardOutcomes verifies corpus representatives for admitted execution plus endpoint-guard and state-guard overflow fallbacks.
func TestEndpointSeededExpansionCorpusCoversGuardOutcomes(t *testing.T) {
	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)
	required := map[string]bool{"guard-admitted": false, "endpoint-guard-overflow": false, "state-guard-overflow": false}
	for _, testCase := range corpus.Cases {
		if testCase.Category != "generated_endpoint_seeded_expansion" {
			continue
		}
		for tag := range required {
			if slices.Contains(testCase.Tags, tag) {
				required[tag] = true
			}
		}
	}
	for tag, found := range required {
		require.True(t, found, "endpoint-seeded corpus is missing %s", tag)
	}
}

// TestGeneratedShortestDistanceCorpusCoversQualificationEnvelope verifies distance cases spanning deep, wide, inbound, disconnected, cyclic, parallel-edge, and self-loop shapes.
func TestGeneratedShortestDistanceCorpusCoversQualificationEnvelope(t *testing.T) {
	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)

	requiredTags := map[string]bool{
		"depth-32": false, "depth-64": false, "fanout-512": false, "fanout-1000": false,
		"inbound": false, "disconnected": false, "cycle": false, "parallel-edges": false, "self-loop": false,
	}
	for _, testCase := range corpus.Cases {
		if testCase.Category != "generated_shortest_path" || !slices.Contains(testCase.Tags, "distance") {
			continue
		}
		for tag := range requiredTags {
			if slices.Contains(testCase.Tags, tag) {
				requiredTags[tag] = true
			}
		}
	}
	for tag, covered := range requiredTags {
		require.True(t, covered, "shortest distance corpus is missing %s", tag)
	}
}

// TestGeneratedShortestPathCorpusCoversMaterializerEnvelope verifies hydrated-path cases spanning deep, wide, inbound, zero-depth, disconnected, cyclic, parallel-edge, and self-loop shapes.
func TestGeneratedShortestPathCorpusCoversMaterializerEnvelope(t *testing.T) {
	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)

	requiredTags := map[string]bool{
		"depth-32": false, "depth-64": false, "fanout-512": false, "fanout-1000": false,
		"inbound": false, "zero-depth": false, "disconnected": false, "cycle": false, "parallel-edges": false, "self-loop": false,
	}
	for _, testCase := range corpus.Cases {
		if testCase.Category != "generated_shortest_path" || !slices.Contains(testCase.Tags, "path") {
			continue
		}
		for tag := range requiredTags {
			if slices.Contains(testCase.Tags, tag) {
				requiredTags[tag] = true
			}
		}
	}
	for tag, covered := range requiredTags {
		require.True(t, covered, "shortest path corpus is missing %s", tag)
	}
}

// TestGeneratedAllShortestCorpusCoversInlineQualificationEnvelope keeps the
// training corpus broad enough to qualify early-stop behavior independently
// from the frozen depth-8 holdouts. Cap-threshold branch execution is covered
// by the live guarded-statement integration tests because corpus cases do not
// override immutable production caps.
func TestGeneratedAllShortestCorpusCoversInlineQualificationEnvelope(t *testing.T) {
	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)

	requiredTraining := map[string]bool{
		"early-depth-1": false, "early-depth-2": false, "early-depth-3": false,
		"max-16": false, "max-64": false, "inbound": false,
		"cycle-dead-tail": false, "reconvergence": false, "disconnected": false,
	}
	hasQualifiedHoldout := false
	for _, testCase := range corpus.Cases {
		if testCase.Category != "generated_shortest_path_v2" || !slices.Contains(testCase.Tags, "all-shortest") {
			continue
		}
		if testCase.Shape.QualificationSplit == "holdout" && testCase.Shape.RelationshipKindCount == 1 && testCase.Shape.MaxDepth != nil && *testCase.Shape.MaxDepth >= 3 {
			hasQualifiedHoldout = true
		}
		if testCase.Shape.QualificationSplit != "training" {
			continue
		}
		for tag := range requiredTraining {
			if slices.Contains(testCase.Tags, tag) {
				requiredTraining[tag] = true
			}
		}
	}
	for tag, covered := range requiredTraining {
		require.True(t, covered, "all-shortest training corpus is missing %s", tag)
	}
	require.True(t, hasQualifiedHoldout, "all-shortest corpus lacks a typed single-kind holdout at maximum depth 3 or greater")
}

// scaleCorpusCaseID joins a scale case's dataset and name into its contract identifier.
func scaleCorpusCaseID(name string) string {
	if separator := strings.IndexByte(name, '_'); separator >= 0 {
		return name[:separator]
	}
	return name
}

// scaleCorpusRequiredIDSet returns the required representative scale-case identifiers as a set.
func scaleCorpusRequiredIDSet() map[string]struct{} {
	required := make(map[string]struct{}, len(scaleCorpusRequiredIDs))
	for _, id := range scaleCorpusRequiredIDs {
		required[id] = struct{}{}
	}
	return required
}

// TestScaleCorpusRequiredRepresentativesDeclareCardinality verifies every required query-form tag is present and declares row counts or complete mutation cardinalities.
func TestScaleCorpusRequiredRepresentativesDeclareCardinality(t *testing.T) {
	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)

	required := scaleCorpusRequiredIDSet()
	covered := map[string]int{}
	for _, testCase := range corpus.Cases {
		id := scaleCorpusCaseID(testCase.Name)
		if _, isRequired := required[id]; !isRequired {
			continue
		}

		covered[id]++
		require.Contains(t, testCase.Tags, id, "%s must retain its stable query-form tag", testCase.Name)
		if testCase.WriteScenario == nil {
			require.NotNil(t, testCase.Expected.RowCount, "%s must declare expected row cardinality", testCase.Name)
		} else {
			require.NotNil(t, testCase.WriteScenario.ExpectedMatched, "%s must declare expected matched cardinality", testCase.Name)
			require.NotNil(t, testCase.WriteScenario.ExpectedAffected, "%s must declare expected affected cardinality", testCase.Name)
		}
	}

	for _, id := range scaleCorpusRequiredIDs {
		require.Positive(t, covered[id], "required scale corpus is missing %s", id)
	}
}

// TestScaleCorpusDistinguishesProjectionClasses verifies that ID-only, shallow, and fully hydrated tags agree with result kind and observation requirements.
func TestScaleCorpusDistinguishesProjectionClasses(t *testing.T) {
	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)

	requiredClasses := map[string]bool{
		"projection-id-only":          false,
		"projection-shallow-ids-kind": false,
		"projection-full-hydration":   false,
	}
	for _, testCase := range corpus.Cases {
		for _, tag := range testCase.Tags {
			if _, required := requiredClasses[tag]; !required {
				continue
			}

			requiredClasses[tag] = true
			switch tag {
			case "projection-id-only":
				require.Equal(t, "id_set", testCase.Expected.ResultKind)
				require.False(t, testCase.Observes.Nodes)
				require.False(t, testCase.Observes.Relationships)
				require.False(t, testCase.Observes.Properties)
			case "projection-shallow-ids-kind":
				require.Equal(t, "shallow_ids_kind", testCase.Expected.ResultKind)
				require.False(t, testCase.Observes.Nodes)
				require.False(t, testCase.Observes.Relationships)
				require.False(t, testCase.Observes.Properties)
			case "projection-full-hydration":
				require.True(t, testCase.Observes.Nodes || testCase.Observes.Relationships)
				require.True(t, testCase.Observes.Properties)
			}
		}
	}

	for projectionClass, found := range requiredClasses {
		require.True(t, found, "scale corpus is missing %s", projectionClass)
	}
}

// TestFixedSuffixExpansionIDRowsUseStableFixtureIdentitiesAndPreserveDuplicates verifies four identical logical endpoint pairs remain explicit expected rows rather than being deduplicated or backend-ID based.
func TestFixedSuffixExpansionIDRowsUseStableFixtureIdentitiesAndPreserveDuplicates(t *testing.T) {
	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)

	for _, testCase := range corpus.Cases {
		if testCase.Name != "fixed_suffix_expansion_endpoint_ids" {
			continue
		}

		require.Equal(t, [][]string{
			{"fse-head", "fse-terminal"},
			{"fse-head", "fse-terminal"},
			{"fse-head", "fse-terminal"},
			{"fse-head", "fse-terminal"},
		}, testCase.Expected.IDRows)
		return
	}

	t.Fatal("fixed_suffix_expansion_endpoint_ids case not found")
}

// TestGeneratedSPI1InboundV1CorpusFreezesTrainingAndUnopenedHoldoutMatrices
// verifies the preregistered canonical-witness cohort without executing or
// inspecting any holdout timing. The contract binds exact generated topology,
// stable path observations, split tags, query identity, and selection digests.
func TestGeneratedSPI1InboundV1CorpusFreezesTrainingAndUnopenedHoldoutMatrices(t *testing.T) {
	const (
		query       = "MATCH p = shortestPath((r)<-[:Traverse*1..64]-(e)) WHERE id(r) = $root_id AND id(e) = $end_id RETURN p"
		querySHA256 = "1024577967901503995d4ec0c76540e96b65f4d25e015ccb6eeffb500a5596f9"
	)

	type expectedCase struct {
		dataset       string
		config        testutil.ShortestPathScaleV2Config
		fixtureSHA256 string
		split         string
		target        string
		resultDepth   int
		stateClass    string
		extraTags     []string
	}
	expected := map[string]expectedCase{
		"GSP-I1-V1-TRAIN-D04-FI016-full": {
			dataset:       "generated_shortest_paths_v2_d4_o0_r4_fo0_fi16_l2_k0_t0_w0_x4_p0_c0_s0",
			config:        spI1InboundFixtureConfig(4, 4, 16, 2, 4),
			fixtureSHA256: "29b0c923d7e3312ba1f19d09076006692dfc66379a524d160da8d74d9c7c3889",
			split:         "training",
			target:        "sp-v2-inbound-end",
			resultDepth:   4,
			stateClass:    "inbound_predecessor_full_depth_fanin_16",
		},
		"GSP-I1-V1-TRAIN-D16-FI256-early-d04": {
			dataset:       "generated_shortest_paths_v2_d16_o0_r8_fo0_fi256_l8_k0_t0_w0_x16_p0_c0_s0",
			config:        spI1InboundFixtureConfig(16, 8, 256, 8, 16),
			fixtureSHA256: "a297da4f7be1cb8621d173cd763e1fcc902b560e23d8fdfbbc9565d10c308bce",
			split:         "training",
			target:        "sp-v2-inbound-linear-04",
			resultDepth:   4,
			stateClass:    "inbound_predecessor_early_target_fanin_256",
			extraTags:     []string{"early-target", "early-depth-4"},
		},
		"GSP-I1-V1-TRAIN-D16-FI256-full": {
			dataset:       "generated_shortest_paths_v2_d16_o0_r8_fo0_fi256_l8_k0_t0_w0_x16_p0_c0_s0",
			config:        spI1InboundFixtureConfig(16, 8, 256, 8, 16),
			fixtureSHA256: "a297da4f7be1cb8621d173cd763e1fcc902b560e23d8fdfbbc9565d10c308bce",
			split:         "training",
			target:        "sp-v2-inbound-end",
			resultDepth:   16,
			stateClass:    "inbound_predecessor_full_depth_fanin_256",
		},
		"GSP-I1-V1-TRAIN-D16-FI256-disconnected": {
			dataset:       "generated_shortest_paths_v2_d16_o0_r8_fo0_fi256_l8_k0_t0_w0_x16_p0_c0_s0",
			config:        spI1InboundFixtureConfig(16, 8, 256, 8, 16),
			fixtureSHA256: "a297da4f7be1cb8621d173cd763e1fcc902b560e23d8fdfbbc9565d10c308bce",
			split:         "training",
			target:        "sp-v2-disconnected-end",
			resultDepth:   -1,
			stateClass:    "inbound_predecessor_disconnected_fanin_256",
			extraTags:     []string{"disconnected", "max-miss"},
		},
		"GSP-I1-V1-HOLDOUT-D08-FI031-full": {
			dataset:       "generated_shortest_paths_v2_d8_o0_r3_fo0_fi31_l3_k0_t0_w0_x7_p0_c0_s0",
			config:        spI1InboundFixtureConfig(8, 3, 31, 3, 7),
			fixtureSHA256: "47acf96f7862e639a8a33bc28f2c9b9e4457320e44c8e88b0b06ab2f25691e63",
			split:         "holdout",
			target:        "sp-v2-inbound-end",
			resultDepth:   8,
			stateClass:    "inbound_predecessor_full_depth_fanin_31",
		},
		"GSP-I1-V1-HOLDOUT-D32-FI191-full": {
			dataset:       "generated_shortest_paths_v2_d32_o0_r11_fo0_fi191_l21_k0_t0_w0_x13_p0_c0_s0",
			config:        spI1InboundFixtureConfig(32, 11, 191, 21, 13),
			fixtureSHA256: "da33b5d223d8513ff4af240613a8f976e12be6b398536bb9b4f8d5a184d9443b",
			split:         "holdout",
			target:        "sp-v2-inbound-end",
			resultDepth:   32,
			stateClass:    "inbound_predecessor_full_depth_fanin_191",
		},
		"GSP-I1-V1-HOLDOUT-D32-FI191-disconnected": {
			dataset:       "generated_shortest_paths_v2_d32_o0_r11_fo0_fi191_l21_k0_t0_w0_x13_p0_c0_s0",
			config:        spI1InboundFixtureConfig(32, 11, 191, 21, 13),
			fixtureSHA256: "da33b5d223d8513ff4af240613a8f976e12be6b398536bb9b4f8d5a184d9443b",
			split:         "holdout",
			target:        "sp-v2-disconnected-end",
			resultDepth:   -1,
			stateClass:    "inbound_predecessor_disconnected_fanin_191",
			extraTags:     []string{"disconnected", "max-miss"},
		},
	}

	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)
	seen := map[string]bool{}
	trainingDepths, holdoutDepths := map[int]bool{}, map[int]bool{}
	trainingCount, holdoutCount := 0, 0
	for _, testCase := range corpus.Cases {
		trainingTag := slices.Contains(testCase.Tags, "sp-i1-inbound-v1-training")
		holdoutTag := slices.Contains(testCase.Tags, "sp-i1-inbound-v1-holdout")
		if !trainingTag && !holdoutTag {
			continue
		}
		require.NotEqual(t, trainingTag, holdoutTag, testCase.Name)
		contract, found := expected[testCase.Name]
		require.True(t, found, "unexpected SP-I1 inbound-v1 declaration %s", testCase.Name)
		require.False(t, seen[testCase.Name], testCase.Name)
		seen[testCase.Name] = true

		require.True(t, strings.HasSuffix(testCase.Source, "/cases/generated_sp_i1_inbound_v1.json"), testCase.Name)
		require.Equal(t, contract.dataset, testCase.Dataset, testCase.Name)
		require.Equal(t, "generated_shortest_path_v2", testCase.Category, testCase.Name)
		require.Equal(t, query, testCase.Cypher, testCase.Name)
		require.Equal(t, querySHA256, pg.TraversalPolicyQuerySHA256(testCase.Cypher), testCase.Name)
		require.Equal(t, map[string]string{"root_id": "sp-v2-inbound-root", "end_id": contract.target}, testCase.NodeParams, testCase.Name)
		require.Equal(t, []ExecutionMode{ModePostgresSQL, ModeNeo4j}, testCase.CandidateModes, testCase.Name)
		require.Empty(t, testCase.UnsupportedModes, testCase.Name)
		require.Equal(t, ObservedValues{Paths: true, Nodes: true, Relationships: true, Properties: true}, testCase.Observes, testCase.Name)

		shape := testCase.Shape
		require.Equal(t, contract.split, shape.QualificationSplit, testCase.Name)
		require.Equal(t, "forbidden", shape.FallbackExpectation, testCase.Name)
		require.Equal(t, "bound_id", shape.RootPredicate, testCase.Name)
		require.Equal(t, "bound_id", shape.TerminalPredicate, testCase.Name)
		require.Equal(t, []string{"Traverse"}, shape.EdgeKinds, testCase.Name)
		require.Equal(t, "inbound", shape.Direction, testCase.Name)
		require.Equal(t, 1, shape.RelationshipKindCount, testCase.Name)
		require.Equal(t, "normal", shape.FixtureTier, testCase.Name)
		require.Equal(t, contract.stateClass, shape.ExpectedStateClass, testCase.Name)
		require.NotNil(t, shape.MinDepth, testCase.Name)
		require.NotNil(t, shape.MaxDepth, testCase.Name)
		require.Equal(t, 1, *shape.MinDepth, testCase.Name)
		require.Equal(t, 64, *shape.MaxDepth, testCase.Name)
		require.True(t, shape.PathMaterializationRequired, testCase.Name)

		config, ok := parseShortestPathV2DatasetName(testCase.Dataset)
		require.True(t, ok, testCase.Name)
		require.Equal(t, contract.config, config, testCase.Name)
		metadata, err := fixtureMetadata("unused", testCase.Dataset)
		require.NoError(t, err, testCase.Name)
		require.Equal(t, contract.fixtureSHA256, metadata.Checksum, testCase.Name)
		require.NotNil(t, metadata.Shortest, testCase.Name)
		require.Equal(t, int64(config.Depth), metadata.Shortest.ExpectedMinimumDistance, testCase.Name)

		expectedTags := []string{"generated", "v2", "normal-tier", "path", "inbound", "hidden-fan-in"}
		expectedTags = append(expectedTags, contract.extraTags...)
		if contract.split == "training" {
			trainingCount++
			trainingDepths[config.Depth] = true
			expectedTags = append(expectedTags, "sp-i1-inbound-v1-training")
		} else {
			holdoutCount++
			holdoutDepths[config.Depth] = true
			expectedTags = append(expectedTags, "holdout", "sp-i1-inbound-v1-holdout")
		}
		require.Equal(t, expectedTags, testCase.Tags, testCase.Name)

		require.NotNil(t, testCase.Expected.RowCount, testCase.Name)
		require.Equal(t, "path_set", testCase.Expected.ResultKind, testCase.Name)
		if contract.resultDepth < 0 {
			require.Zero(t, *testCase.Expected.RowCount, testCase.Name)
			require.Empty(t, testCase.Expected.PathRows, testCase.Name)
			require.Equal(t, "empty", shape.ResultCardinalityClass, testCase.Name)
		} else {
			require.Equal(t, int64(1), *testCase.Expected.RowCount, testCase.Name)
			require.Equal(t, []ExpectedPath{spI1InboundExpectedPath(config.Depth, contract.resultDepth)}, testCase.Expected.PathRows, testCase.Name)
			require.Equal(t, "singleton", shape.ResultCardinalityClass, testCase.Name)
		}
	}

	require.Len(t, seen, 7)
	for name := range expected {
		require.True(t, seen[name], "missing SP-I1 inbound-v1 declaration %s", name)
	}
	require.Len(t, spI1CanonicalCases, len(expected))
	canonicalSeen := map[string]bool{}
	for _, canonical := range spI1CanonicalCases {
		contract, found := expected[canonical.name]
		require.True(t, found, "unexpected frozen SP-I1 case %s", canonical.name)
		require.False(t, canonicalSeen[canonical.name], canonical.name)
		canonicalSeen[canonical.name] = true
		require.Equal(t, contract.dataset, canonical.dataset, canonical.name)
		require.Equal(t, contract.split, canonical.split, canonical.name)
	}
	require.Equal(t, seen, canonicalSeen, "corpus and qualification reporter must freeze the same SP-I1 cases")
	require.Equal(t, 4, trainingCount)
	require.Equal(t, 3, holdoutCount)
	require.Equal(t, map[int]bool{4: true, 16: true}, trainingDepths)
	require.Equal(t, map[int]bool{8: true, 32: true}, holdoutDepths)
	for depth := range holdoutDepths {
		require.False(t, trainingDepths[depth], "holdout depth %d is present in training", depth)
	}

	training, trainingSelection, err := selectScaleCorpus(corpus, CorpusSelectors{Tags: []string{"sp-i1-inbound-v1-training"}})
	require.NoError(t, err)
	require.Len(t, training.Cases, 4)
	require.True(t, trainingSelection.DiagnosticOnly)
	require.Equal(t, 8, trainingSelection.SelectedDeclarationCount)
	require.Equal(t, "1162e6563678dad742d8fe89d250936862b4a73deab247cde4b5ddebdfdd93ce", trainingSelection.DeclarationSHA256)
	require.Equal(t, "cc07b55331e15f4e268043d1ed36abf7deec7217771a1b30913db6e738d27f7a", resolvedSelectionSHA256(trainingSelection.Resolved))
	require.Equal(t, "3da3c4b1cea3fa64fbaa1958f7bf8048639241522ccf6e46defd10d2d8c9ccd6", spI1InboundRuntimeCorpusIdentity(training))

	confirmation, confirmationSelection, err := selectScaleCorpus(corpus, CorpusSelectors{Tags: []string{"sp-i1-inbound-v1-training", "sp-i1-inbound-v1-holdout"}})
	require.NoError(t, err)
	require.Len(t, confirmation.Cases, 7)
	require.True(t, confirmationSelection.DiagnosticOnly)
	require.Equal(t, 14, confirmationSelection.SelectedDeclarationCount)
	require.Equal(t, "31f6041f342b3ed8059d4d1396a76f073c3fc877472d06632a8bad16b5a4cbfd", confirmationSelection.DeclarationSHA256)
	require.Equal(t, "16a8756a7c32695f0314b3552c80d2a500226c7a44c57847c916a96e775aa0c5", resolvedSelectionSHA256(confirmationSelection.Resolved))
	require.Equal(t, "219ee26cae52d8b81c6c91f9c517692c544ef4cec1aa9b9314fbc4e8f5ad3c5c", spI1InboundRuntimeCorpusIdentity(confirmation))
}

func spI1InboundFixtureConfig(depth, rootFanIn, intermediateFanIn, fanInLevel, disconnectedWidth int) testutil.ShortestPathScaleV2Config {
	return testutil.ShortestPathScaleV2Config{
		Depth:                    depth,
		ReverseRootFanIn:         rootFanIn,
		IntermediateReverseFanIn: intermediateFanIn,
		FanInLevel:               fanInLevel,
		DisconnectedWidth:        disconnectedWidth,
	}
}

func spI1InboundExpectedPath(fixtureDepth, resultDepth int) ExpectedPath {
	nodes := []string{"sp-v2-inbound-root"}
	for level := 1; level < resultDepth; level++ {
		nodes = append(nodes, fmt.Sprintf("sp-v2-inbound-linear-%02d", level))
	}
	if resultDepth == fixtureDepth {
		nodes = append(nodes, "sp-v2-inbound-end")
	} else {
		nodes = append(nodes, fmt.Sprintf("sp-v2-inbound-linear-%02d", resultDepth))
	}
	kinds := make([]string, resultDepth)
	keys := make([]string, resultDepth)
	for idx := range resultDepth {
		kinds[idx] = "Traverse"
		keys[idx] = fmt.Sprintf("inbound-primary-%02d", fixtureDepth-idx)
	}
	return ExpectedPath{Nodes: nodes, RelationshipKinds: kinds, RelationshipKeys: keys}
}

// spI1InboundRuntimeCorpusIdentity normalizes the package-test corpus root to
// the repository-root spelling used by GraphBench capture commands.
func spI1InboundRuntimeCorpusIdentity(corpus ScaleCorpus) string {
	canonical := ScaleCorpus{Cases: append([]ScaleCase(nil), corpus.Cases...)}
	for idx := range canonical.Cases {
		if offset := strings.Index(canonical.Cases[idx].Source, "benchmark/testdata/scale/"); offset >= 0 {
			canonical.Cases[idx].Source = canonical.Cases[idx].Source[offset:]
		}
	}
	return corpusIdentity(canonical)
}
