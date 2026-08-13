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
	"slices"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
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
