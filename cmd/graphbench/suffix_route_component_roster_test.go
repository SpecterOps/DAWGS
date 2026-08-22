// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSuffixRouteComponentV1RosterFreezesFreshOpenTargetAndControls keeps the
// direct-component preflight isolated from every terminal fixed-suffix cohort.
func TestSuffixRouteComponentV1RosterFreezesFreshOpenTargetAndControls(t *testing.T) {
	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)
	selected, _, err := selectScaleCorpus(corpus, CorpusSelectors{Tags: []string{"suffix-route-component-v1"}})
	require.NoError(t, err)
	require.Len(t, selected.Cases, 11)

	seenDatasets := map[string]bool{}
	targets, controls := 0, 0
	classes := map[string]bool{}
	for _, testCase := range selected.Cases {
		require.Equal(t, "training", testCase.Shape.QualificationSplit, testCase.Name)
		require.Equal(t, "forbidden", testCase.Shape.FallbackExpectation, testCase.Name)
		require.True(t, testCase.Supports(ModePostgresSQL), testCase.Name)
		require.False(t, testCase.Supports(ModeNeo4j), testCase.Name)
		require.False(t, seenDatasets[testCase.Dataset], "dataset identity reused: %s", testCase.Dataset)
		seenDatasets[testCase.Dataset] = true
		if testCase.Expected.ResultKind == "id_rows" {
			require.Len(t, testCase.Expected.IDRows, int(*testCase.Expected.RowCount), testCase.Name)
		} else {
			require.Len(t, testCase.Expected.PathRows, int(*testCase.Expected.RowCount), testCase.Name)
		}
		metadata, err := fixtureMetadata("unused", testCase.Dataset)
		require.NoError(t, err, testCase.Name)
		require.NotNil(t, metadata.FixedSuffixExpansion, testCase.Name)
		require.Equal(t, metadata.FixedSuffixExpansion.CompleteOutputTrails, *testCase.Expected.RowCount, testCase.Name)
		require.False(t, slices.Contains(testCase.Tags, "orientation-v2-training"), testCase.Name)
		require.False(t, slices.Contains(testCase.Tags, "orientation-v2-holdout"), testCase.Name)
		require.False(t, slices.Contains(testCase.Tags, "suffix-reverse-retry-v1-training"), testCase.Name)

		switch testCase.Shape.QualificationRole {
		case "efficacy_target":
			targets++
		case "adverse_control":
			controls++
		default:
			t.Fatalf("%s has unexpected roster role %q", testCase.Name, testCase.Shape.QualificationRole)
		}
		for _, tag := range testCase.Tags {
			classes[tag] = true
		}
	}
	require.Equal(t, 2, targets)
	require.Equal(t, 9, controls)
	for _, class := range []string{"sparse-suffix", "high-reverse-fanin", "dense-suffix", "no-path", "suffix-cap-511", "suffix-cap-512", "suffix-cap-513", "productive-cycle", "productive-self-loop", "multiple-path"} {
		require.True(t, classes[class], class)
	}
}
