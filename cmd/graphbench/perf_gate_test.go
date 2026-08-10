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

func TestBuildPerfGateReportTreatsNeo4jAsCorrectnessOracle(t *testing.T) {
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
	neo4j := findPerfGateCase(t, report.Cases, ModeNeo4j)
	require.True(t, neo4j.OracleOnly)
	require.Nil(t, neo4j.P95Ratio)
}

func TestBuildPerfGateReportFailsMissingDeclaredPostgresCase(t *testing.T) {
	baseline := []CaseResult{perfGateRecord("present", ModePostgresSQL, time.Millisecond, 5, 30)}
	candidate := []CaseResult{perfGateRecord("present", ModePostgresSQL, time.Millisecond, 5, 30)}

	report, err := buildPerfGateReport(baseline, candidate, PerfGateOptions{
		Seed:                1,
		Confidence:          0.95,
		RegressionThreshold: 0.20,
		BootstrapCount:      100,
		DeclaredBackends: []DeclaredCaseBackend{
			{
				Dataset: "fixture",
				Name:    "present",
				Backend: ModePostgresSQL,
			},
			{
				Dataset: "fixture",
				Name:    "missing",
				Backend: ModePostgresSQL,
			},
		},
	})

	require.NoError(t, err)
	require.False(t, report.Passed)
	require.NotEmpty(t, report.DeclarationSHA256)
	var missing PerfGateCase
	for _, gateCase := range report.Cases {
		if gateCase.Name == "missing" {
			missing = gateCase
		}
	}
	require.Equal(t, "missing", missing.CandidateStatus)
	require.ErrorContains(t, reasonsError(missing.Reasons), "required candidate record status is missing")
}

func TestBuildPerfGateReportAppliesMaterialityOnlyToDeclaredTargets(t *testing.T) {
	baseline := []CaseResult{perfGateRecord("target", ModePostgresSQL, 10*time.Millisecond, 5, 30)}
	candidate := []CaseResult{perfGateRecord("target", ModePostgresSQL, 9_700*time.Microsecond, 5, 30)}

	report, err := buildPerfGateReport(baseline, candidate, PerfGateOptions{
		Seed:                1,
		Confidence:          0.95,
		RegressionThreshold: 0.20,
		BootstrapCount:      100,
		TargetNames:         []string{"target"},
		MaterialityRatio:    0.95,
		MaterialityAbsolute: 100 * time.Microsecond,
	})

	require.NoError(t, err)
	require.True(t, report.Passed, "%v", report.Cases[0].Reasons)
	require.NotNil(t, report.Cases[0].MedianSaving)
	require.Equal(t, 300*time.Microsecond, report.Cases[0].MedianSaving.Lower)
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

func TestUnsupportedDeclarationAffectsChecksumWithoutRequiringARecord(t *testing.T) {
	declared := []DeclaredCaseBackend{
		{
			Dataset: "fixture",
			Name:    "directionless",
			Backend: ModeNeo4j,
		},
		{
			Dataset:           "fixture",
			Name:              "directionless",
			Backend:           ModePostgresSQL,
			UnsupportedReason: "unsupported form",
		},
	}
	records := []CaseResult{perfGateRecord("directionless", ModeNeo4j, time.Millisecond, 1, 1)}

	report, err := buildPerfGateReport(records, records, PerfGateOptions{
		Seed:                1,
		Confidence:          0.95,
		RegressionThreshold: 0.20,
		BootstrapCount:      10,
		DeclaredBackends:    declared,
	})
	require.NoError(t, err)
	require.True(t, report.Passed)
	require.Len(t, report.Cases, 1)

	changed := append([]DeclaredCaseBackend(nil), declared...)
	changed[1].UnsupportedReason = "different reason"
	require.NotEqual(t, declarationSHA256(declared), declarationSHA256(changed))
}

func TestValidatePerformanceArtifactSelectionsRefusesDiagnosticsFromCompleteGate(t *testing.T) {
	manifest := &SelectionManifest{
		DiagnosticOnly:    true,
		DeclarationSHA256: "subset",
	}
	left := []CaseResult{{
		Dataset: "fixture",
		Name:    "case",
		Environment: &RunEnvironment{
			Selection: manifest,
		},
	}}
	right := []CaseResult{{
		Dataset: "fixture",
		Name:    "case",
		Environment: &RunEnvironment{
			Selection: manifest,
		},
	}}

	require.ErrorContains(t, validatePerformanceArtifactSelections(left, right, false), "refused")
	require.NoError(t, validatePerformanceArtifactSelections(left, right, true))
	right[0].Environment.Selection = &SelectionManifest{
		DiagnosticOnly:    true,
		DeclarationSHA256: "different",
	}
	require.ErrorContains(t, validatePerformanceArtifactSelections(left, right, true), "declarations differ")
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
