// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/stretchr/testify/require"
)

func TestCreateSPI2V2DevelopmentReportRejectsInputOverwrite(t *testing.T) {
	artifact := filepath.Join(t.TempDir(), "tournament.jsonl")
	_, err := createSPI2V2DevelopmentReport("../../benchmark/testdata/scale", artifact, artifact)
	require.ErrorContains(t, err, "must not overwrite")
}

func TestEvaluateSPI2V2DevelopmentSeriesSelectsEligibleArmByDeclaredRanking(t *testing.T) {
	series := spI2V2DevelopmentReportTestSeries()
	report, err := evaluateSPI2V2DevelopmentSeries(series, spI2V2DevelopmentReportOptions{confidence: .975, bootstrapReplicates: 100})
	require.NoError(t, err)
	require.False(t, report.PromotionEligible)
	require.Equal(t, string(optimize.ShortestPathExecutorI2GuardedDistanceV2E1DP), report.SelectedExecutor)
	require.Len(t, report.Arms, 5)
	for _, arm := range report.Arms[1:] {
		require.True(t, arm.Eligible, arm.Reasons)
		require.NotNil(t, arm.Ranking)
		require.Len(t, arm.Cases, 6)
	}
	require.Len(t, report.Arms[1].Cases[0].Contrasts, 1)
	require.Len(t, report.Arms[2].Cases[0].Contrasts, 2)
	require.Len(t, report.Arms[3].Cases[0].Contrasts, 2)
	require.Len(t, report.Arms[4].Cases[0].Contrasts, 3)
}

func TestEvaluateSPI2V2DevelopmentSeriesRejectsPlanningRegressionAndCombinedParent(t *testing.T) {
	series := spI2V2DevelopmentReportTestSeries()
	e1d := optimize.ShortestPathExecutorI2GuardedDistanceV2E1D
	for key := range series {
		series[key][e1d].planning = constantSPI2V2DevelopmentRounds(1100*time.Microsecond, 1)
	}
	report, err := evaluateSPI2V2DevelopmentSeries(series, spI2V2DevelopmentReportOptions{confidence: .975, bootstrapReplicates: 100})
	require.NoError(t, err)
	require.False(t, report.Arms[2].Eligible)
	require.Contains(t, strings.Join(report.Arms[2].Reasons, " "), "planning-time limit")
	require.False(t, report.Arms[4].Eligible)
	require.Contains(t, strings.Join(report.Arms[4].Reasons, " "), "parent")
	require.Equal(t, string(optimize.ShortestPathExecutorI2GuardedDistanceV2E1P), report.SelectedExecutor)
}

func TestEvaluateSPI2V2DevelopmentSeriesFallsBackToE0(t *testing.T) {
	series := spI2V2DevelopmentReportTestSeries()
	for key := range series {
		for _, arm := range spI2V2DevelopmentArms[1:] {
			series[key][arm].samples = constantSPI2V2DevelopmentRounds(1100*time.Microsecond, 100)
		}
	}
	report, err := evaluateSPI2V2DevelopmentSeries(series, spI2V2DevelopmentReportOptions{confidence: .975, bootstrapReplicates: 100})
	require.NoError(t, err)
	require.Equal(t, string(optimize.ShortestPathExecutorI2GuardedDistanceV2E0), report.SelectedExecutor)
}

func TestLessSPI2V2DevelopmentRankingUsesEveryDeclaredTieBreaker(t *testing.T) {
	base := SPI2V2DevelopmentRanking{PlanNodeScore: 10, PlanningRatioUpper: 1, P95RatioUpper: 1, FixedOrder: 2}
	require.True(t, lessSPI2V2DevelopmentRanking(SPI2V2DevelopmentRanking{PlanNodeScore: 9}, base))
	require.True(t, lessSPI2V2DevelopmentRanking(SPI2V2DevelopmentRanking{PlanNodeScore: 10, PlanningRatioUpper: .99}, base))
	require.True(t, lessSPI2V2DevelopmentRanking(SPI2V2DevelopmentRanking{PlanNodeScore: 10, PlanningRatioUpper: 1, P95RatioUpper: .99}, base))
	require.True(t, lessSPI2V2DevelopmentRanking(SPI2V2DevelopmentRanking{PlanNodeScore: 10, PlanningRatioUpper: 1, P95RatioUpper: 1, FixedOrder: 1}, base))
}

func TestValidateSPI2V2DevelopmentReportRecordRejectsPlanningAndPlanTampering(t *testing.T) {
	records := spI2V2ComponentTestRecords(t, optimize.ShortestPathExecutorI2GuardedDistanceV2E1D)
	record := records[0]
	declarations, err := canonicalSPI2Declarations()
	require.NoError(t, err)
	key := performanceKey{dataset: record.Dataset, name: record.Name, backend: record.ExecutionMode}
	require.NoError(t, validateSPI2V2DevelopmentReportRecord(record, optimize.ShortestPathExecutorI2GuardedDistanceV2E1D, declarations[key]))

	planningMS := *record.PostgresMetrics.PlanningMS
	record.PostgresMetrics.PlanningMS = nil
	require.ErrorContains(t, validateSPI2V2DevelopmentReportRecord(record, optimize.ShortestPathExecutorI2GuardedDistanceV2E1D, declarations[key]), "canonical")

	record = records[0]
	record.PostgresMetrics.PlanningMS = &planningMS
	record.PostgresPlanJSON = []byte(`[{"Planning Time":1.0}]`)
	require.ErrorContains(t, validateSPI2V2DevelopmentReportRecord(record, optimize.ShortestPathExecutorI2GuardedDistanceV2E1D, declarations[key]), "contradicts")
}

func spI2V2DevelopmentReportTestSeries() map[performanceKey]map[optimize.ShortestPathExecutor]*spI2V2DevelopmentSeries {
	series := make(map[performanceKey]map[optimize.ShortestPathExecutor]*spI2V2DevelopmentSeries)
	caseNames := []string{"cycle-control", "case-2", "case-3", "case-4", "case-5", "case-6"}
	for _, name := range caseNames {
		key := performanceKey{dataset: "fixture", name: name, backend: ModePostgresSQL}
		series[key] = make(map[optimize.ShortestPathExecutor]*spI2V2DevelopmentSeries)
		for armIndex, arm := range spI2V2DevelopmentArms {
			latency := 1000 * time.Microsecond
			if name == "cycle-control" {
				switch arm {
				case optimize.ShortestPathExecutorI2GuardedDistanceV2E1:
					latency = 900 * time.Microsecond
				case optimize.ShortestPathExecutorI2GuardedDistanceV2E1D, optimize.ShortestPathExecutorI2GuardedDistanceV2E1P, optimize.ShortestPathExecutorI2GuardedDistanceV2E1DP:
					latency = 800 * time.Microsecond
				}
			}
			series[key][arm] = &spI2V2DevelopmentSeries{
				samples: constantSPI2V2DevelopmentRounds(latency, 100), planning: constantSPI2V2DevelopmentRounds(time.Millisecond, 1),
				maxPlanNodes: 5 - armIndex,
			}
		}
	}
	return series
}

func constantSPI2V2DevelopmentRounds(value time.Duration, samples int) roundSamples {
	result := make(roundSamples, 10)
	for round := 1; round <= 10; round++ {
		result[round] = make([]time.Duration, samples)
		for index := range result[round] {
			result[round][index] = value
		}
	}
	return result
}
