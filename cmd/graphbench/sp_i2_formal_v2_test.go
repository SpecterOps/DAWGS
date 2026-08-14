// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSPI2V2FormalCorpusFreezesRolesAndDisjointCohorts(t *testing.T) {
	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)
	cohort, err := canonicalSPI2V2FormalCohort()
	require.NoError(t, err)

	seenTraining, seenHoldout := map[performanceKey]bool{}, map[performanceKey]bool{}
	v1HoldoutDatasets := map[string]bool{}
	for _, declaration := range spI2CanonicalCases {
		if declaration.split == "holdout" {
			v1HoldoutDatasets[declaration.dataset] = true
		}
	}
	for _, testCase := range corpus.Cases {
		key := performanceKey{dataset: testCase.Dataset, name: testCase.Name, backend: ModePostgresSQL}
		role, training := cohort.trainingKeys[key]
		if !training {
			role, _ = cohort.holdoutKeys[key]
		}
		if role == "" {
			continue
		}
		require.Equal(t, role, testCase.Shape.QualificationRole, testCase.Name)
		require.False(t, testCase.Shape.PathMaterializationRequired, testCase.Name)
		require.Equal(t, "forbidden", testCase.Shape.FallbackExpectation, testCase.Name)
		require.Equal(t, []ExecutionMode{ModePostgresSQL, ModeNeo4j}, testCase.CandidateModes, testCase.Name)
		require.NotNil(t, testCase.Expected.RowCount, testCase.Name)
		_, err := fixtureMetadata("unused", testCase.Dataset)
		require.NoError(t, err, testCase.Name)
		if training {
			require.Equal(t, "training", testCase.Shape.QualificationSplit)
			require.True(t, slices.Contains(testCase.Tags, spI2V2TrainingTag))
			seenTraining[key] = true
		} else {
			require.Equal(t, "holdout", testCase.Shape.QualificationSplit)
			require.True(t, slices.Contains(testCase.Tags, spI2V2HoldoutTag))
			require.False(t, v1HoldoutDatasets[testCase.Dataset], testCase.Name)
			seenHoldout[key] = true
		}
	}
	require.Len(t, seenTraining, 8)
	require.Len(t, seenHoldout, 6)
}

func TestSPI2V2FormalHoldoutsAreProtectedAsASeparateGeneration(t *testing.T) {
	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)
	cohort, err := canonicalSPI2V2FormalCohort()
	require.NoError(t, err)

	ordinary, _, err := selectRunnableScaleCorpusWithSPI2Protection(corpus, CorpusSelectors{})
	require.NoError(t, err)
	for _, testCase := range ordinary.Cases {
		_, protected := cohort.holdoutKeys[performanceKey{dataset: testCase.Dataset, name: testCase.Name, backend: ModePostgresSQL}]
		require.False(t, protected, testCase.Name)
	}

	holdout, holdoutSelection, err := selectRunnableScaleCorpusWithSPI2Protection(corpus, CorpusSelectors{Tags: []string{spI2V2HoldoutTag}})
	require.NoError(t, err)
	require.Len(t, holdout.Cases, 6)
	require.True(t, selectedCorpusContainsSPI2V2FormalCase(holdout))
	training, trainingSelection, err := selectRunnableScaleCorpusWithSPI2Protection(corpus, CorpusSelectors{Tags: []string{spI2V2TrainingTag}})
	require.NoError(t, err)
	require.Len(t, training.Cases, 8)
	require.True(t, selectedCorpusContainsSPI2V2FormalCase(training))
	require.Equal(t, spI2V2TrainingCorpusSHA256, corpusIdentity(training))
	require.Equal(t, spI2V2TrainingDeclarationSHA256, trainingSelection.DeclarationSHA256)
	require.Equal(t, spI2V2TrainingResolvedSHA256, resolvedSelectionSHA256(trainingSelection.Resolved))
	require.Equal(t, spI2V2HoldoutCorpusSHA256, corpusIdentity(holdout))
	require.Equal(t, spI2V2HoldoutDeclarationSHA256, holdoutSelection.DeclarationSHA256)
	require.Equal(t, spI2V2HoldoutResolvedSHA256, resolvedSelectionSHA256(holdoutSelection.Resolved))
	fullSelectionCases, fullSelection, err := selectRunnableScaleCorpusWithSPI2Protection(corpus, CorpusSelectors{Tags: []string{spI2V2TrainingTag, spI2V2HoldoutTag}})
	require.NoError(t, err)
	require.Len(t, fullSelectionCases.Cases, 14)
	require.Equal(t, spI2V2FullCorpusSHA256, corpusIdentity(fullSelectionCases))
	require.Equal(t, spI2V2FullDeclarationSHA256, fullSelection.DeclarationSHA256)
	require.Equal(t, spI2V2FullResolvedSHA256, resolvedSelectionSHA256(fullSelection.Resolved))

	_, _, err = selectRunnableScaleCorpusWithSPI2Protection(corpus, CorpusSelectors{Tags: []string{spI2TrainingTag, spI2V2TrainingTag}})
	require.ErrorContains(t, err, "cannot be mixed")
}
