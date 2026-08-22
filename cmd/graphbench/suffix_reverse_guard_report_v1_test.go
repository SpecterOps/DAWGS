// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/stretchr/testify/require"
)

// TestSuffixReverseGuardFeasibilityPassesSixRoundStopGate verifies the report
// is an early three-arm decision, not a qualification or holdout report.
func TestSuffixReverseGuardFeasibilityPassesSixRoundStopGate(t *testing.T) {
	incumbent, reverse, guarded := suffixReverseGuardTestArtifacts("training", time.Millisecond, 500*time.Microsecond, 550*time.Microsecond)
	report, err := buildSuffixReverseGuardFeasibilityReport(
		incumbent, reverse, guarded, testAAReportForRecords(t, incumbent),
		SuffixReverseGuardFeasibilityOptions{Seed: 7, Confidence: defaultConfidenceLevel, BootstrapCount: 100},
	)
	require.NoError(t, err)
	require.True(t, report.Passed)
	require.Equal(t, "training_feasibility", report.Protocol)
	require.Equal(t, string(optimize.ExpansionSearchPolicySuffixReverseGuardV1), report.Policy)
	require.Len(t, report.Cases, 2)
	for _, reportCase := range report.Cases {
		require.True(t, reportCase.GuardOverhead.Passed)
		require.True(t, reportCase.FastestExactRegret.Passed)
		require.True(t, reportCase.ForwardImprovement.Passed)
	}
}

// TestSuffixReverseGuardFeasibilityRequiresExactSelectionCohort verifies both
// key membership and each arm's schema-v2 selection declaration are sealed.
func TestSuffixReverseGuardFeasibilityRequiresExactSelectionCohort(t *testing.T) {
	incumbent, reverse, guarded := suffixReverseGuardTestArtifacts("training", time.Millisecond, 500*time.Microsecond, 550*time.Microsecond)
	keepFirstCase := func(records []CaseResult) []CaseResult {
		result := records[:0]
		for _, record := range records {
			if record.Name == suffixReverseGuardFeasibilityCases[0].name {
				result = append(result, record)
			}
		}
		return result
	}
	_, err := buildSuffixReverseGuardFeasibilityReport(
		keepFirstCase(incumbent), keepFirstCase(reverse), keepFirstCase(guarded), testAAReportForRecords(t, keepFirstCase(incumbent)),
		SuffixReverseGuardFeasibilityOptions{Seed: 29, Confidence: defaultConfidenceLevel, BootstrapCount: 10},
	)
	require.ErrorContains(t, err, "exact two-case training cohort")

	incumbent, reverse, guarded = suffixReverseGuardTestArtifacts("training", time.Millisecond, 500*time.Microsecond, 550*time.Microsecond)
	for index := range reverse {
		reverse[index].Environment.Selection.DeclarationSHA256 = testSHA("9")
	}
	_, err = buildSuffixReverseGuardFeasibilityReport(
		incumbent, reverse, guarded, testAAReportForRecords(t, incumbent),
		SuffixReverseGuardFeasibilityOptions{Seed: 29, Confidence: defaultConfidenceLevel, BootstrapCount: 10},
	)
	require.ErrorContains(t, err, "exact two-case declaration")
}

// TestSuffixReverseGuardFeasibilityRequiresPhysicalAAChronology ensures a
// label-balanced legacy A/A report cannot calibrate the stop gate without
// artifact-bound validation of its source process intervals.
func TestSuffixReverseGuardFeasibilityRequiresPhysicalAAChronology(t *testing.T) {
	incumbent, reverse, guarded := suffixReverseGuardTestArtifacts("training", time.Millisecond, 500*time.Microsecond, 550*time.Microsecond)
	aa := testAAReportForRecords(t, incumbent)
	aa.PhysicalChronology = nil
	_, err := buildSuffixReverseGuardFeasibilityReport(
		incumbent, reverse, guarded, aa,
		SuffixReverseGuardFeasibilityOptions{Seed: 41, Confidence: defaultConfidenceLevel, BootstrapCount: 10},
	)
	require.ErrorContains(t, err, "physical chronology provenance")
}

// TestSuffixReverseGuardFeasibilityAcceptsExactCompileTimeForwardFallback
// preserves the incumbent tuple emitted when static lowering requests reverse
// but safely falls back to exact stepwise forward at compile time.
func TestSuffixReverseGuardFeasibilityAcceptsExactCompileTimeForwardFallback(t *testing.T) {
	incumbent, reverse, guarded := suffixReverseGuardTestArtifacts("training", time.Millisecond, 500*time.Microsecond, 550*time.Microsecond)
	forward := string(optimize.ExpansionSearchStepwiseForward)
	requested := string(optimize.ExpansionSearchSuffixSeededReverse)
	for recordIndex := range incumbent {
		summary := &incumbent[recordIndex].TraversalTelemetry.Summary
		summary.RequestedIdentity = requested
		summary.RuntimeBranch = "compile_time_fallback"
		summary.FallbackExecuted = boolPointer(true)
		summary.FallbackIdentity = forward
		summary.Provenance["fallback_identity"] = "test"
		for sampleIndex := range incumbent[recordIndex].Stats.Samples {
			sample := &incumbent[recordIndex].Stats.Samples[sampleIndex]
			if sample.Classification == "warm" && sample.Duration > 0 {
				sample.RequestedIdentity = requested
				sample.RuntimeBranch = "compile_time_fallback"
				sample.FallbackExecuted = boolPointer(true)
			}
		}
	}
	report, err := buildSuffixReverseGuardFeasibilityReport(
		incumbent, reverse, guarded, testAAReportForRecords(t, incumbent),
		SuffixReverseGuardFeasibilityOptions{Seed: 31, Confidence: defaultConfidenceLevel, BootstrapCount: 10},
	)
	require.NoError(t, err)
	require.True(t, report.Passed)
}

// TestSuffixReverseGuardFeasibilityRejectsOverheadAndProtectedHoldout verifies
// the predeclared stop gate cannot be relaxed or fed v2/future holdout timing.
func TestSuffixReverseGuardFeasibilityRejectsOverheadAndProtectedHoldout(t *testing.T) {
	incumbent, reverse, guarded := suffixReverseGuardTestArtifacts("training", time.Millisecond, 500*time.Microsecond, 750*time.Microsecond)
	report, err := buildSuffixReverseGuardFeasibilityReport(
		incumbent, reverse, guarded, testAAReportForRecords(t, incumbent),
		SuffixReverseGuardFeasibilityOptions{Seed: 11, Confidence: defaultConfidenceLevel, BootstrapCount: 50},
	)
	require.NoError(t, err)
	require.False(t, report.Passed)
	require.Contains(t, report.Cases[0].Reasons, "guard overhead exceeds the 1.10/100us stop gate")

	for _, records := range [][]CaseResult{incumbent, reverse, guarded} {
		for index := range records {
			records[index].Shape.QualificationSplit = "holdout"
		}
	}
	_, err = buildSuffixReverseGuardFeasibilityReport(
		incumbent, reverse, guarded, testAAReportForRecords(t, incumbent),
		SuffixReverseGuardFeasibilityOptions{Seed: 11, Confidence: defaultConfidenceLevel, BootstrapCount: 10},
	)
	require.ErrorContains(t, err, "training-only full-path")

	incumbent, reverse, guarded = suffixReverseGuardTestArtifacts("diagnostic", time.Millisecond, 500*time.Microsecond, 550*time.Microsecond)
	_, err = buildSuffixReverseGuardFeasibilityReport(
		incumbent, reverse, guarded, testAAReportForRecords(t, incumbent),
		SuffixReverseGuardFeasibilityOptions{Seed: 11, Confidence: defaultConfidenceLevel, BootstrapCount: 10},
	)
	require.ErrorContains(t, err, "predeclared training split")
}

// TestSuffixReverseGuardFeasibilityFailsClosedOnDiagnosticSubstitution verifies
// feasibility cannot be manufactured from summary-only, cap-substituted, or
// typed-counter evidence that differs from the measured PostgreSQL plan.
func TestSuffixReverseGuardFeasibilityFailsClosedOnDiagnosticSubstitution(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*CaseResult)
		problem string
	}{
		{
			name: "summary only",
			mutate: func(record *CaseResult) {
				record.TraversalTelemetry.Level = TraversalTelemetryLevelSummary
				record.TraversalTelemetry.Diagnostic = nil
			},
			problem: "complete untimed diagnostic replay",
		},
		{
			name: "cap substitution",
			mutate: func(record *CaseResult) {
				record.TraversalTelemetry.Summary.Caps["suffix_rows"]--
			},
			problem: "immutable suffix/state caps",
		},
		{
			name: "typed counter substitution",
			mutate: func(record *CaseResult) {
				*record.TraversalTelemetry.Diagnostic.Counters.SuffixGuard.StateRows++
			},
			problem: "not bound to its measured plan",
		},
		{
			name: "inactive arm work",
			mutate: func(record *CaseResult) {
				record.TraversalTelemetry.Diagnostic.PlanReplay.Counters["suffix_guard_fallback_executor_loops"] = 1
			},
			problem: "did not suppress the fallback executor",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			incumbent, reverse, guarded := suffixReverseGuardTestArtifacts("training", time.Millisecond, 500*time.Microsecond, 550*time.Microsecond)
			test.mutate(&guarded[0])
			_, err := buildSuffixReverseGuardFeasibilityReport(
				incumbent, reverse, guarded, testAAReportForRecords(t, incumbent),
				SuffixReverseGuardFeasibilityOptions{Seed: 19, Confidence: defaultConfidenceLevel, BootstrapCount: 10},
			)
			require.ErrorContains(t, err, test.problem)
		})
	}
}

// TestSuffixReverseGuardFeasibilityRequiresAllWilliamsOrders prevents a
// position-balanced but repeated three-arm schedule from passing as the
// predeclared six-order design.
func TestSuffixReverseGuardFeasibilityRequiresAllWilliamsOrders(t *testing.T) {
	incumbent, reverse, guarded := suffixReverseGuardTestArtifacts("training", time.Millisecond, 500*time.Microsecond, 550*time.Microsecond)
	artifacts := [][]CaseResult{incumbent, reverse, guarded}
	setRoundOrder := func(records []CaseResult, round, order int) {
		for recordIndex := range records {
			record := &records[recordIndex]
			if record.Environment.Round != round {
				continue
			}
			record.Environment.ArmOrder = order
			roundStart := time.Unix(1_700_000_000+int64(round)*10, 0).UTC()
			record.Environment.StartedAt = roundStart.Add(time.Duration(order-1) * 2 * time.Second)
			record.Environment.EndedAt = record.Environment.StartedAt.Add(time.Second)
			for sampleIndex := range record.Stats.Samples {
				record.Stats.Samples[sampleIndex].ArmOrder = order
			}
		}
	}
	// Repeat the first three cyclic orders. Every arm still occupies every
	// position twice, and the physical timestamps follow the substituted
	// labels, so only exact schedule validation detects substitution.
	for index, orders := range [][3]int{{1, 2, 3}, {2, 3, 1}, {3, 1, 2}} {
		round := index + 4
		for artifactIndex, order := range orders {
			setRoundOrder(artifacts[artifactIndex], round, order)
		}
	}
	_, err := buildSuffixReverseGuardFeasibilityReport(
		incumbent, reverse, guarded, testAAReportForRecords(t, incumbent),
		SuffixReverseGuardFeasibilityOptions{Seed: 23, Confidence: defaultConfidenceLevel, BootstrapCount: 10},
	)
	require.ErrorContains(t, err, "all six doubled-Williams arm orders exactly once")
}

// TestSuffixReverseGuardFeasibilityRejectsPhysicalScheduleTampering verifies
// labels cannot manufacture a doubled-Williams study whose arm processes did
// not execute sequentially in the declared order and round chronology.
func TestSuffixReverseGuardFeasibilityRejectsPhysicalScheduleTampering(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func([]CaseResult, []CaseResult, []CaseResult)
		problem string
	}{
		{
			name: "block differs from round",
			mutate: func(incumbent, _, _ []CaseResult) {
				for index := range incumbent {
					if incumbent[index].Environment.Round == 2 {
						incumbent[index].Environment.Block = 1
						for sampleIndex := range incumbent[index].Stats.Samples {
							incumbent[index].Stats.Samples[sampleIndex].Block = 1
						}
					}
				}
			},
			problem: "requires block equal to round",
		},
		{
			name: "missing invocation timestamp",
			mutate: func(_, reverse, _ []CaseResult) {
				for index := range reverse {
					if reverse[index].Environment.Round == 1 {
						reverse[index].Environment.StartedAt = time.Time{}
					}
				}
			},
			problem: "malformed invocation timestamps",
		},
		{
			name: "mixed cohort invocation",
			mutate: func(incumbent, _, _ []CaseResult) {
				incumbent[0].Environment.StartedAt = incumbent[0].Environment.StartedAt.Add(100 * time.Millisecond)
				incumbent[0].Environment.EndedAt = incumbent[0].Environment.EndedAt.Add(100 * time.Millisecond)
			},
			problem: "mixes invocation identities across the exact cohort",
		},
		{
			name: "declared arm order contradicts execution",
			mutate: func(incumbent, reverse, _ []CaseResult) {
				firstStarted := incumbent[0].Environment.StartedAt
				for index := range reverse {
					if reverse[index].Environment.Round == 1 {
						reverse[index].Environment.StartedAt = firstStarted.Add(500 * time.Millisecond)
						reverse[index].Environment.EndedAt = firstStarted.Add(1500 * time.Millisecond)
					}
				}
			},
			problem: "arm timestamps contradict the declared execution order",
		},
		{
			name: "round overlaps prior round",
			mutate: func(incumbent, reverse, guarded []CaseResult) {
				artifacts := [][]CaseResult{incumbent, reverse, guarded}
				priorEnded := time.Time{}
				for _, records := range artifacts {
					for index := range records {
						environment := records[index].Environment
						if environment.Round == 1 && environment.ArmOrder == 3 {
							priorEnded = environment.EndedAt
						}
					}
				}
				firstStarted := priorEnded.Add(-500 * time.Millisecond)
				for _, records := range artifacts {
					for index := range records {
						environment := records[index].Environment
						if environment.Round != 2 {
							continue
						}
						environment.StartedAt = firstStarted.Add(time.Duration(environment.ArmOrder-1) * 2 * time.Second)
						environment.EndedAt = environment.StartedAt.Add(time.Second)
					}
				}
			},
			problem: "overlaps or predates the prior round",
		},
		{
			name: "mixed run UUID across rounds",
			mutate: func(incumbent, reverse, guarded []CaseResult) {
				for _, records := range [][]CaseResult{incumbent, reverse, guarded} {
					for index := range records {
						if records[index].Environment.Round != 6 {
							continue
						}
						records[index].Environment.RunUUID = "substituted-run"
						for sampleIndex := range records[index].Stats.Samples {
							records[index].Stats.Samples[sampleIndex].RunUUID = "substituted-run"
						}
					}
				}
			},
			problem: "mix run UUIDs across arms or rounds",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			incumbent, reverse, guarded := suffixReverseGuardTestArtifacts("training", time.Millisecond, 500*time.Microsecond, 550*time.Microsecond)
			aa := testAAReportForRecords(t, incumbent)
			test.mutate(incumbent, reverse, guarded)
			_, err := buildSuffixReverseGuardFeasibilityReport(
				incumbent, reverse, guarded, aa,
				SuffixReverseGuardFeasibilityOptions{Seed: 37, Confidence: defaultConfidenceLevel, BootstrapCount: 10},
			)
			require.ErrorContains(t, err, test.problem)
		})
	}
}

func TestParseConfigAcceptsAndIsolatesSuffixGuardFeasibilityReport(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-suffix-guard-incumbent-artifact", "forward.jsonl",
		"-suffix-guard-reverse-artifact", "reverse.jsonl",
		"-suffix-guard-guarded-artifact", "guarded.jsonl",
		"-suffix-guard-aa", "aa.json",
		"-suffix-guard-output", "report.json",
	}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, "guarded.jsonl", cfg.SuffixGuardGuardedArtifact)

	for _, args := range [][]string{
		{"-suffix-guard-output", "report.json"},
		{
			"-suffix-guard-incumbent-artifact", "same.jsonl",
			"-suffix-guard-reverse-artifact", "same.jsonl",
			"-suffix-guard-guarded-artifact", "guarded.jsonl",
			"-suffix-guard-aa", "aa.json",
			"-suffix-guard-output", "report.json",
		},
	} {
		_, err := parseConfig(args, func(string) string { return "" })
		require.Error(t, err, args)
	}
}

func TestSuffixGuardFeasibilityRejectsReceiptFromAnotherInvocation(t *testing.T) {
	incumbent, reverse, guarded := suffixReverseGuardTestArtifacts("training", time.Millisecond, 500*time.Microsecond, 550*time.Microsecond)
	guarded[0].Stats.Samples[0].RuntimeReceiptEvents[0].InvocationID = "other-invocation"
	_, err := buildSuffixReverseGuardFeasibilityReport(
		incumbent, reverse, guarded, testAAReportForRecords(t, incumbent),
		SuffixReverseGuardFeasibilityOptions{Seed: 7, Confidence: defaultConfidenceLevel, BootstrapCount: 10},
	)
	require.ErrorContains(t, err, "not bound to its timed invocation")
}

func suffixReverseGuardTestArtifacts(split string, incumbentDuration, reverseDuration, guardedDuration time.Duration) ([]CaseResult, []CaseResult, []CaseResult) {
	orders := [][3]int{{1, 2, 3}, {2, 3, 1}, {3, 1, 2}, {3, 2, 1}, {1, 3, 2}, {2, 1, 3}}
	incumbent := make([]CaseResult, 0, len(orders)*len(suffixReverseGuardFeasibilityCases))
	reverse := make([]CaseResult, 0, len(orders)*len(suffixReverseGuardFeasibilityCases))
	guarded := make([]CaseResult, 0, len(orders)*len(suffixReverseGuardFeasibilityCases))
	for caseIndex, testCase := range suffixReverseGuardFeasibilityCases {
		for index, order := range orders {
			round := index + 1
			incumbentRecord := orientationSelectorV2Record(round, order[0], "incumbent", split, "", incumbentDuration, false)
			reverseRecord := orientationSelectorV2Record(round, order[1], "reverse", split, "", reverseDuration, false)
			guardedRecord := orientationSelectorV2Record(round, order[2], "guarded", split, string(optimize.ExpansionSearchSuffixSeededReverse), guardedDuration, false)
			for _, record := range []*CaseResult{&incumbentRecord, &reverseRecord, &guardedRecord} {
				record.Dataset = testCase.dataset
				record.Name = testCase.name
				record.Fixture.Dataset = testCase.dataset
				record.WorkloadSHA256 = sqlFingerprint("suffix-guard-workload-" + testCase.name)
				record.Fixture.Checksum = sqlFingerprint("suffix-guard-fixture-" + testCase.name)
				record.PostgresEnvironment.NodeRelationBytes += int64(caseIndex + 1)
				record.PostgresEnvironment.EdgeRelationBytes += int64(caseIndex + 1)
			}
			metrics := suffixGuardTestMetrics(1, 0, 1, 0, 2, 9)
			telemetry, err := buildPostgresCaseTraversalTelemetry(
				translate.OptimizationSummary{TargetOutcomes: []translate.TargetLoweringOutcome{suffixGuardTestOutcome()}},
				metrics, "9123", TraversalTelemetryLevelDiagnostic,
			)
			if err != nil {
				panic(err)
			}
			enrichSuffixGuardTraversalTelemetry(telemetry, metrics, guardedRecord.RowCount, guardedRecord.ObservedRows)
			guardedRecord.TraversalTelemetry = telemetry
			guardedRecord.PostgresMetrics = &metrics
			incumbent = append(incumbent, incumbentRecord)
			reverse = append(reverse, reverseRecord)
			guarded = append(guarded, guardedRecord)
		}
	}
	for _, records := range [][]CaseResult{incumbent, reverse, guarded} {
		for index := range records {
			records[index].Cypher = "MATCH p=(root)-[:Edge*1..5]->()-[:Edge]->()-[:Edge]->(terminal) WHERE root.key = $root_key RETURN p"
			records[index].Shape.PathMaterializationRequired = true
			records[index].Environment.RunUUID = "suffix-guard-run"
			roundStart := time.Unix(1_700_000_000+int64(records[index].Environment.Round)*10, 0).UTC()
			records[index].Environment.StartedAt = roundStart.Add(time.Duration(records[index].Environment.ArmOrder-1) * 2 * time.Second)
			records[index].Environment.EndedAt = records[index].Environment.StartedAt.Add(time.Second)
			records[index].Stats.WarmupIterations = 5
			records[index].Environment.WarmupIterations = 5
			records[index].Stats.Samples = records[index].Stats.Samples[:10]
			for sampleIndex := range records[index].Stats.Samples {
				records[index].Stats.Samples[sampleIndex].RunUUID = "suffix-guard-run"
				if records[index].Stats.Samples[sampleIndex].RuntimeAttestation == "timed_invocation" {
					invocation := fmt.Sprintf("suffix-guard-%d-%d", records[index].Stats.Samples[sampleIndex].Round, sampleIndex+1)
					records[index].Stats.Samples[sampleIndex].RuntimeInvocationID = invocation
					records[index].Stats.Samples[sampleIndex].ConnectionID = "901"
					for eventIndex := range records[index].Stats.Samples[sampleIndex].RuntimeReceiptEvents {
						records[index].Stats.Samples[sampleIndex].RuntimeReceiptEvents[eventIndex].InvocationID = invocation
					}
				}
			}
		}
	}
	stampSuffixReverseGuardSelections(incumbent, reverse, guarded)
	return incumbent, reverse, guarded
}

func stampSuffixReverseGuardSelections(artifacts ...[]CaseResult) {
	declared := make([]DeclaredCaseBackend, 0, 2*len(suffixReverseGuardFeasibilityCases))
	resolved := make([]ResolvedCaseSelector, 0, len(suffixReverseGuardFeasibilityCases))
	for _, testCase := range suffixReverseGuardFeasibilityCases {
		for _, backend := range []ExecutionMode{ModePostgresSQL, ModeNeo4j} {
			declared = append(declared, DeclaredCaseBackend{Dataset: testCase.dataset, Name: testCase.name, Backend: backend})
		}
		resolved = append(resolved, ResolvedCaseSelector{Dataset: testCase.dataset, Name: testCase.name, Category: "generated_fixed_suffix_expansion"})
	}
	selection := SelectionManifest{
		Version: selectionManifestVersion, Resolved: resolved, DiagnosticOnly: true,
		FullDeclarationCount: len(declared), SelectedDeclarationCount: len(declared), DeclarationSHA256: declarationSHA256(declared),
	}
	for _, records := range artifacts {
		for index := range records {
			copy := selection
			copy.Resolved = append([]ResolvedCaseSelector(nil), selection.Resolved...)
			records[index].Environment.Selection = &copy
		}
	}
}
