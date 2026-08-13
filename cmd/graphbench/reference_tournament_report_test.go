// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestBuildReferenceTournamentReportQualifiesStableHoldoutWinner verifies build reference tournament report qualifies stable holdout winner behavior.
func TestBuildReferenceTournamentReportQualifiesStableHoldoutWinner(t *testing.T) {
	arms := []string{"expand_into_pair_join", "expand_into_lower_degree_scan", "expand_into_pair_cache"}
	var records []CaseResult
	for round := 1; round <= 12; round++ {
		for _, split := range []string{"training", "holdout"} {
			records = append(records, referenceTournamentRecord(arms, round, split, map[string]time.Duration{
				arms[0]: 10 * time.Millisecond,
				arms[1]: 7 * time.Millisecond,
				arms[2]: 5 * time.Millisecond,
			}))
		}
	}

	report, err := buildReferenceTournamentReport(records, ReferenceTournamentOptions{
		Seed:           1,
		BootstrapCount: 100,
		Confidence:     .975,
		Arms:           arms,
		Protocol:       referencePairProtocolConfirmation,
	})
	require.NoError(t, err)
	require.True(t, report.Passed)
	require.True(t, report.PromotionEligible)
	require.True(t, report.TrainingPassed)
	require.True(t, report.HoldoutPassed)
	require.Equal(t, arms[2], report.Winner)
	require.Len(t, report.Cases, 2)
	for _, entry := range report.Cases {
		require.True(t, entry.Passed)
		require.Equal(t, arms[2], entry.Winner)
	}
}

// TestBuildReferenceTournamentReportRejectsOrderAndWinnerDrift verifies build reference tournament report rejects order and winner drift behavior.
func TestBuildReferenceTournamentReportRejectsOrderAndWinnerDrift(t *testing.T) {
	arms := []string{"expand_into_pair_join", "expand_into_lower_degree_scan", "expand_into_pair_cache"}
	badOrder := referenceTournamentRecord(arms, 1, "training", map[string]time.Duration{
		arms[0]: 10 * time.Millisecond, arms[1]: 7 * time.Millisecond, arms[2]: 5 * time.Millisecond,
	})
	badOrder.PostgresReferences[0].MeasurementOrder = 99
	_, err := buildReferenceTournamentReport([]CaseResult{badOrder}, ReferenceTournamentOptions{
		Confidence: .975,
		Arms:       arms,
		Protocol:   referencePairProtocolDiscovery,
	})
	require.ErrorContains(t, err, "Williams order")

	var records []CaseResult
	for round := 1; round <= 10; round++ {
		records = append(records,
			referenceTournamentRecord(arms, round, "training", map[string]time.Duration{
				arms[0]: 10 * time.Millisecond, arms[1]: 5 * time.Millisecond, arms[2]: 7 * time.Millisecond,
			}),
			referenceTournamentRecord(arms, round, "holdout", map[string]time.Duration{
				arms[0]: 10 * time.Millisecond, arms[1]: 7 * time.Millisecond, arms[2]: 5 * time.Millisecond,
			}),
		)
	}
	report, err := buildReferenceTournamentReport(records, ReferenceTournamentOptions{
		Seed:           1,
		BootstrapCount: 100,
		Confidence:     .975,
		Arms:           arms,
		Protocol:       referencePairProtocolConfirmation,
	})
	require.NoError(t, err)
	require.False(t, report.Passed)
	require.False(t, report.PromotionEligible)
	require.Empty(t, report.Winner)
}

// referenceTournamentRecord prepares or inspects test evidence for reference tournament record.
func referenceTournamentRecord(arms []string, round int, split string, durations map[string]time.Duration) CaseResult {
	record := CaseResult{
		Environment: &RunEnvironment{
			Round:            round,
			WarmupIterations: 20,
		},
		Dataset:       "tournament",
		Name:          "case-" + split,
		Shape:         WorkloadShape{QualificationSplit: split},
		ExecutionMode: ModePostgresSQL,
		Status:        StatusOK,
		RowCount:      1,
		ObservedRows:  []string{"row"},
	}
	base := make([]postgresReferenceSpec, len(arms))
	for idx, arm := range arms {
		base[idx].name = arm
	}
	orders := map[string]int{}
	for idx, spec := range referenceSpecsForRound(base, round) {
		orders[spec.name] = idx + 2
	}
	for _, arm := range arms {
		samples := make([]LatencySample, 50)
		for idx := range samples {
			samples[idx] = LatencySample{
				Classification: "warm",
				Duration:       durations[arm] + time.Duration(idx),
			}
		}
		record.PostgresReferences = append(record.PostgresReferences, PostgresReferenceResult{
			Name:               arm,
			Architecture:       arm,
			ImplementationID:   arm + "-v1",
			SQLFingerprint:     arm + "-sql-v1",
			Boundary:           "relationships",
			FullComparator:     true,
			SemanticValidation: "exact_public_observation",
			MeasurementOrder:   orders[arm],
			RowCount:           1,
			ObservedRows:       []string{"row"},
			Stats: DurationStats{
				WarmupIterations: 20,
				Samples:          samples,
			},
		})
	}
	return record
}
