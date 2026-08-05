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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildPerfGateReportPassesTargetAndComparableGates(t *testing.T) {
	baseline := []CaseResult{
		perfGateRecord("one_shortest_path_bound_pair", ModePostgresSQL, 10*time.Millisecond, 5, 30),
		perfGateRecord("one_shortest_path_bound_pair", ModeNeo4j, 3*time.Millisecond, 5, 30),
	}
	candidate := []CaseResult{
		perfGateRecord("one_shortest_path_bound_pair", ModePostgresSQL, 3*time.Millisecond, 5, 30),
		perfGateRecord("one_shortest_path_bound_pair", ModeNeo4j, 2*time.Millisecond, 5, 30),
	}

	report, err := buildPerfGateReport(baseline, candidate, PerfGateOptions{
		Seed:                42,
		Confidence:          0.95,
		RegressionThreshold: 0.20,
		BootstrapCount:      250,
	})

	require.NoError(t, err)
	require.True(t, report.Passed)
	require.Len(t, report.Cases, 2)
	postgres := findPerfGateCase(t, report.Cases, ModePostgresSQL)
	require.InDelta(t, 0.3, postgres.MedianRatio.Estimate, 0.0001)
	require.NotNil(t, postgres.P95Ratio)
	require.NotNil(t, postgres.BackendRatio)
	require.InDelta(t, 1.5, postgres.BackendRatio.Estimate, 0.0001)
}

func TestBuildPerfGateReportFailsRegressionAndInsufficientP95(t *testing.T) {
	baseline := []CaseResult{perfGateRecord("ordinary_case", ModePostgresSQL, 10*time.Millisecond, 5, 10)}
	candidate := []CaseResult{perfGateRecord("ordinary_case", ModePostgresSQL, 13*time.Millisecond, 5, 10)}

	report, err := buildPerfGateReport(baseline, candidate, PerfGateOptions{
		Seed:                7,
		Confidence:          0.95,
		RegressionThreshold: 0.20,
		BootstrapCount:      100,
	})

	require.NoError(t, err)
	require.False(t, report.Passed)
	require.Len(t, report.Cases, 1)
	require.ErrorContains(t, reasonsError(report.Cases[0].Reasons), "median regression")
	require.ErrorContains(t, reasonsError(report.Cases[0].Reasons), "at least 150 warm samples")
}

func TestBuildPerfGateReportRequiresMatchedRounds(t *testing.T) {
	baseline := []CaseResult{perfGateRecord("ordinary_case", ModePostgresSQL, 10*time.Millisecond, 4, 40)}
	candidate := []CaseResult{perfGateRecord("ordinary_case", ModePostgresSQL, 9*time.Millisecond, 4, 40)}

	report, err := buildPerfGateReport(baseline, candidate, PerfGateOptions{
		Seed:                1,
		Confidence:          0.95,
		RegressionThreshold: 0.20,
		BootstrapCount:      100,
	})

	require.NoError(t, err)
	require.False(t, report.Passed)
	require.ErrorContains(t, reasonsError(report.Cases[0].Reasons), "at least 5 matched rounds")
}

func perfGateRecord(name string, mode ExecutionMode, duration time.Duration, rounds, samplesPerRound int) CaseResult {
	record := CaseResult{
		Dataset:       "fixture",
		Name:          name,
		ExecutionMode: mode,
		Status:        StatusOK,
	}
	for round := 1; round <= rounds; round++ {
		for iteration := 1; iteration <= samplesPerRound; iteration++ {
			record.Stats.Samples = append(record.Stats.Samples, LatencySample{
				Round:          round,
				Iteration:      iteration,
				Classification: "warm",
				Duration:       duration,
			})
		}
	}
	return record
}

func findPerfGateCase(t *testing.T, cases []PerfGateCase, mode ExecutionMode) PerfGateCase {
	t.Helper()
	for _, gateCase := range cases {
		if gateCase.Backend == mode {
			return gateCase
		}
	}
	t.Fatalf("missing %s gate case", mode)
	return PerfGateCase{}
}

func reasonsError(reasons []string) error {
	return fmt.Errorf("%s", strings.Join(reasons, "; "))
}
