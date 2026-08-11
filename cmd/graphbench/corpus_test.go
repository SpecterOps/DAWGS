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
	"fmt"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/testutil"
	"github.com/stretchr/testify/require"
)

// TestLoadScaleCorpus verifies that every loaded case identifies its source, declares PostgreSQL support status, and excludes the reference-only AGE mode.
func TestLoadScaleCorpus(t *testing.T) {
	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)
	require.NotEmpty(t, corpus.Cases)

	for _, testCase := range corpus.Cases {
		require.NotEqual(t, "", testCase.Source)
		_, explicitlyUnsupported := testCase.UnsupportedReason(ModePostgresSQL)
		require.True(t, testCase.Supports(ModePostgresSQL) || explicitlyUnsupported,
			"postgres_sql should be a candidate or explicitly unsupported for %s", testCase.Name)
		require.False(t, testCase.Supports(ExecutionMode("age")), "AGE is a reference design only for %s", testCase.Name)
	}
}

// TestValidateScaleCaseRequiresConsistentUnsupportedModes verifies that a backend cannot be both runnable and unsupported and that every exclusion has a reason.
func TestValidateScaleCaseRequiresConsistentUnsupportedModes(t *testing.T) {
	testCase := ScaleCase{
		Name:             "directionless",
		Dataset:          "base",
		Category:         "shortest_path",
		Cypher:           "MATCH p = shortestPath((a)-[*]-(b)) RETURN p",
		CandidateModes:   []ExecutionMode{ModeNeo4j},
		UnsupportedModes: map[ExecutionMode]string{ModePostgresSQL: "translator does not support this form"},
	}

	require.NoError(t, validateScaleCase(testCase))
	testCase.CandidateModes = append(testCase.CandidateModes, ModePostgresSQL)
	require.ErrorContains(t, validateScaleCase(testCase), "both candidate and unsupported")
	testCase.CandidateModes = []ExecutionMode{ModeNeo4j}
	testCase.UnsupportedModes[ModePostgresSQL] = ""
	require.ErrorContains(t, validateScaleCase(testCase), "requires a reason")
}

// TestScaleCorpusDatasets verifies that corpus dataset discovery removes repeated names and returns a deterministic lexical order.
func TestScaleCorpusDatasets(t *testing.T) {
	corpus := ScaleCorpus{
		Cases: []ScaleCase{
			{Name: "a", Dataset: "base", Category: "counts", Cypher: "return 1", CandidateModes: []ExecutionMode{ModePostgresSQL}},
			{
				Name:           "b",
				Dataset:        "fixed_suffix_expansion_fanout",
				Category:       "counts",
				Cypher:         "return 1",
				CandidateModes: []ExecutionMode{ModePostgresSQL},
			},
			{Name: "c", Dataset: "base", Category: "counts", Cypher: "return 1", CandidateModes: []ExecutionMode{ModePostgresSQL}},
		},
	}

	require.Equal(t, []string{"base", "fixed_suffix_expansion_fanout"}, scaleCorpusDatasets(corpus))
}

// TestGeneratedReconciliationDatasetRegistersThirtyKinds verifies that reconciliation generation exposes every RecKind01 through RecKind30 relationship kind.
func TestGeneratedReconciliationDatasetRegistersThirtyKinds(t *testing.T) {
	doc, err := parseDataset("unused", testutil.ReconciliationScaleDataset)
	require.NoError(t, err)
	_, edgeKinds := doc.Graph.Kinds()

	for idx := 1; idx <= 30; idx++ {
		require.Contains(t, edgeKinds, graph.StringKind(fmt.Sprintf("RecKind%02d", idx)))
	}
}

// TestGeneratedTrustPruningDatasetRegistersProductionShapes verifies that trust-pruning fixtures contain the domain, candidate, same-forest, cross-forest, and batch labels used by production queries.
func TestGeneratedTrustPruningDatasetRegistersProductionShapes(t *testing.T) {
	doc, err := parseDataset("unused", testutil.TrustPruningScaleDataset)
	require.NoError(t, err)
	nodeKinds, edgeKinds := doc.Graph.Kinds()

	require.Contains(t, nodeKinds, graph.StringKind("Domain"))
	require.Contains(t, nodeKinds, graph.StringKind("PruneCandidate"))
	require.Contains(t, edgeKinds, graph.StringKind("SameForestTrust"))
	require.Contains(t, edgeKinds, graph.StringKind("CrossForestTrust"))
	require.Contains(t, edgeKinds, graph.StringKind("PruneBatch"))
}

// TestGeneratedHopDatasetRegistersThirtyKindsAndEndpointSets verifies that hop fixtures expose both endpoint node classes, all thirty numbered relationship kinds, and the set-membership edge.
func TestGeneratedHopDatasetRegistersThirtyKindsAndEndpointSets(t *testing.T) {
	doc, err := parseDataset("unused", testutil.HopScaleDataset)
	require.NoError(t, err)
	nodeKinds, edgeKinds := doc.Graph.Kinds()

	require.Contains(t, nodeKinds, graph.StringKind("HopIDEndpoint"))
	require.Contains(t, nodeKinds, graph.StringKind("HopTemplate"))
	for idx := 1; idx <= 30; idx++ {
		require.Contains(t, edgeKinds, graph.StringKind(fmt.Sprintf("HopKind%02d", idx)))
	}
	require.Contains(t, edgeKinds, graph.StringKind("HopSetEdge"))
}

// TestGeneratedScanLookupDatasetRegistersWideAndLargeShapes verifies that scan fixtures contain the base, role, and hydration nodes plus every relationship kind used by wide lookup plans.
func TestGeneratedScanLookupDatasetRegistersWideAndLargeShapes(t *testing.T) {
	doc, err := parseDataset("unused", testutil.ScanLookupScaleDataset)
	require.NoError(t, err)
	nodeKinds, edgeKinds := doc.Graph.Kinds()

	require.Contains(t, nodeKinds, graph.StringKind("ADBase"))
	require.Contains(t, nodeKinds, graph.StringKind("AZRole"))
	require.Contains(t, nodeKinds, graph.StringKind("Hydrate"))
	require.Contains(t, edgeKinds, graph.StringKind("ScanPostProcessed"))
	require.Contains(t, edgeKinds, graph.StringKind("Contains"))
	for idx := 1; idx <= 9; idx++ {
		require.Contains(t, edgeKinds, graph.StringKind(fmt.Sprintf("ScanEdge%02d", idx)))
	}
}

// TestGeneratedShortestPathDatasetRegistersMatrixShapes verifies that shortest-path generation produces nonempty nodes and both generic and typed traversal relationships.
func TestGeneratedShortestPathDatasetRegistersMatrixShapes(t *testing.T) {
	doc, err := parseDataset("unused", testutil.ShortestPathScaleDataset)
	require.NoError(t, err)
	nodeKinds, edgeKinds := doc.Graph.Kinds()

	require.Contains(t, nodeKinds, graph.StringKind("ShortestNode"))
	require.Contains(t, edgeKinds, graph.StringKind("Traverse"))
	require.Contains(t, edgeKinds, graph.StringKind("TypedTraverse"))
	require.NotEmpty(t, doc.Graph.Nodes)
}

// TestGeneratedFixedSuffixExpansionDatasetRegistersSuffixAndDecoyShapes verifies that fixed-suffix fixtures register all path stages and the wrong-entry decoy needed to detect over-broad matching.
func TestGeneratedFixedSuffixExpansionDatasetRegistersSuffixAndDecoyShapes(t *testing.T) {
	doc, err := parseDataset("unused", testutil.FixedSuffixExpansionScaleDataset)
	require.NoError(t, err)
	nodeKinds, edgeKinds := doc.Graph.Kinds()

	for _, kind := range []string{"ExpansionRoot", "ExpansionNode", "SuffixHead", "SuffixMiddle", "SuffixTerminal"} {
		require.Contains(t, nodeKinds, graph.StringKind(kind))
	}
	for _, kind := range []string{"Expand", "EnterSuffix", "ContinueSuffix", "CompleteSuffix", "WrongEnterSuffix"} {
		require.Contains(t, edgeKinds, graph.StringKind(kind))
	}
}

// TestValidateScaleCaseRequiresCompleteWriteScenario verifies that destructive cases include at least one post-state assertion after selection and affected-count expectations.
func TestValidateScaleCaseRequiresCompleteWriteScenario(t *testing.T) {
	zero := int64(0)
	testCase := ScaleCase{
		Name:           "write",
		Dataset:        "base",
		Category:       "delete",
		Cypher:         "MATCH (n) DELETE n",
		CandidateModes: []ExecutionMode{ModePostgresSQL},
		WriteScenario: &WriteScenario{
			SelectionCypher:  "MATCH (n) RETURN n",
			AffectedEntity:   "node",
			ExpectedMatched:  &zero,
			ExpectedAffected: &zero,
			PostState: []ScaleStateQuery{{
				Name:     "survivors",
				Cypher:   "MATCH (n) RETURN n",
				Expected: ExpectedResult{RowCount: &zero},
			}},
		},
	}

	require.NoError(t, validateScaleCase(testCase))
	testCase.WriteScenario.PostState = nil
	require.ErrorContains(t, validateScaleCase(testCase), "post_state is required")
}

// TestSelectScaleCorpusUsesExactSelectorsAndMarksDiagnostics verifies that partial dataset/tag selection records omitted declarations, marks the manifest diagnostic-only, and rejects unresolved exact selectors.
func TestSelectScaleCorpusUsesExactSelectorsAndMarksDiagnostics(t *testing.T) {
	corpus := ScaleCorpus{
		Cases: []ScaleCase{
			{
				Name:           "lookup",
				Dataset:        "base",
				Category:       "lookup",
				Tags:           []string{"primary"},
				CandidateModes: []ExecutionMode{ModePostgresSQL},
			},
			{
				Name:           "control",
				Dataset:        "base",
				Category:       "lookup",
				Tags:           []string{"control"},
				CandidateModes: []ExecutionMode{ModePostgresSQL, ModeNeo4j},
			},
			{
				Name:           "other",
				Dataset:        "other",
				Category:       "count",
				CandidateModes: []ExecutionMode{ModePostgresSQL},
			},
		},
	}

	selected, manifest, err := selectScaleCorpus(corpus, CorpusSelectors{
		Datasets: []string{"base"},
		Tags:     []string{"primary", "control"},
	})
	require.NoError(t, err)
	require.Len(t, selected.Cases, 2)
	require.True(t, manifest.DiagnosticOnly)
	require.Equal(t, 1, manifest.OmittedDeclarationCount)
	require.NotEmpty(t, manifest.DeclarationSHA256)

	_, _, err = selectScaleCorpus(corpus, CorpusSelectors{Cases: []string{"missing"}})
	require.ErrorContains(t, err, "unknown case selector")
}

// TestSelectScaleCorpusRejectsAmbiguousExactNames verifies that a bare case selector cannot choose between identically named cases from different datasets.
func TestSelectScaleCorpusRejectsAmbiguousExactNames(t *testing.T) {
	corpus := ScaleCorpus{
		Cases: []ScaleCase{{
			Name:    "same",
			Dataset: "one",
		}, {
			Name:    "same",
			Dataset: "two",
		}},
	}
	_, _, err := selectScaleCorpus(corpus, CorpusSelectors{Cases: []string{"same"}})
	require.ErrorContains(t, err, "ambiguous case selector")
}
