// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	pgdriver "github.com/specterops/dawgs/drivers/pg"
)

const suffixReverseGuardFeasibilityVersion = 1

// SuffixReverseGuardFeasibilityOptions configures the deliberately bounded,
// training-only stop gate. It cannot freeze or authorize production evidence.
type SuffixReverseGuardFeasibilityOptions struct {
	Seed           int64
	Confidence     float64
	BootstrapCount int
}

// SuffixReverseGuardImprovementGate records material forward improvement and
// p95 containment for one reverse-favorable full-path workload.
type SuffixReverseGuardImprovementGate struct {
	MedianRatio      RatioInterval    `json:"median_ratio_to_forward"`
	MedianSaving     DurationInterval `json:"median_saving_from_forward"`
	P95Ratio         RatioInterval    `json:"p95_ratio_to_forward"`
	RatioUpperLimit  float64          `json:"median_ratio_upper_limit"`
	SavingLowerLimit time.Duration    `json:"median_saving_lower_limit"`
	P95UpperLimit    float64          `json:"p95_ratio_upper_limit"`
	Passed           bool             `json:"passed"`
}

// SuffixReverseGuardFeasibilityCase records the three-arm go/no-go decision
// for one already-open, training-only workload.
type SuffixReverseGuardFeasibilityCase struct {
	Dataset                  string                            `json:"dataset"`
	Name                     string                            `json:"name"`
	QualificationSplit       string                            `json:"qualification_split"`
	QuerySHA256              string                            `json:"query_sha256"`
	Rounds                   int                               `json:"matched_rounds"`
	ExactObservationsMatched bool                              `json:"exact_observations_matched"`
	RuntimeIdentity          string                            `json:"runtime_identity"`
	RuntimeBranch            string                            `json:"runtime_branch"`
	Overflow                 bool                              `json:"overflow"`
	FallbackExecuted         bool                              `json:"fallback_executed"`
	GuardOverhead            OrientationLatencyGate            `json:"guard_overhead_to_exact_reverse"`
	FastestExactRegret       OrientationLatencyGate            `json:"regret_to_fastest_exact"`
	ForwardImprovement       SuffixReverseGuardImprovementGate `json:"improvement_over_forward"`
	Passed                   bool                              `json:"passed"`
	Reasons                  []string                          `json:"reasons,omitempty"`
}

// SuffixReverseGuardFeasibilityReport is negative- or positive-decision
// evidence for whether fresh qualification tooling is warranted. Passed does
// not mean qualified or production-authorized.
type SuffixReverseGuardFeasibilityReport struct {
	Version                 int                                 `json:"version"`
	Policy                  string                              `json:"policy"`
	SelectorVersion         string                              `json:"selector_version"`
	Protocol                string                              `json:"protocol"`
	Seed                    int64                               `json:"seed"`
	Confidence              float64                             `json:"confidence_level"`
	SourceCommit            string                              `json:"source_commit"`
	DirtyDiffSHA256         string                              `json:"dirty_diff_sha256"`
	BinarySHA256            string                              `json:"binary_sha256"`
	CorpusSHA256            string                              `json:"corpus_sha256"`
	IncumbentArtifactSHA256 string                              `json:"incumbent_artifact_sha256,omitempty"`
	ReverseArtifactSHA256   string                              `json:"reverse_artifact_sha256,omitempty"`
	GuardedArtifactSHA256   string                              `json:"guarded_artifact_sha256,omitempty"`
	AAReportSHA256          string                              `json:"aa_report_sha256,omitempty"`
	Caps                    map[string]int64                    `json:"caps"`
	GuardRatioUpperLimit    float64                             `json:"guard_ratio_upper_limit"`
	GuardAbsoluteUpperLimit time.Duration                       `json:"guard_absolute_upper_limit"`
	ForwardRatioUpperLimit  float64                             `json:"forward_ratio_upper_limit"`
	ForwardSavingLowerLimit time.Duration                       `json:"forward_saving_lower_limit"`
	ForwardP95UpperLimit    float64                             `json:"forward_p95_upper_limit"`
	EvidencePassed          bool                                `json:"evidence_passed"`
	Passed                  bool                                `json:"passed"`
	Cases                   []SuffixReverseGuardFeasibilityCase `json:"cases"`
}

type suffixReverseGuardSeries struct {
	incumbent roundSamples
	reverse   roundSamples
	guarded   roundSamples
	runtime   string
	branch    string
	overflow  bool
	fallback  bool
	observed  bool
	querySHA  string
	split     string
}

// suffixReverseGuardInvocationIdentity binds every case in one arm capture to
// the physical GraphBench process interval that executed that scheduled arm.
// The feasibility protocol runs the exact two-case cohort once per arm, so all
// records for an arm/round must carry this same identity and interval.
type suffixReverseGuardInvocationIdentity struct {
	round     int
	block     int
	order     int
	arm       string
	runUUID   string
	startedAt time.Time
	endedAt   time.Time
}

var suffixReverseGuardFeasibilityCases = []struct {
	dataset string
	name    string
}{
	{
		dataset: "generated_fixed_suffix_expansion_v3_d2_f4_r0_x2_i0_m1_q4_z1_c1_s1_p0",
		name:    "GFSE-V3-TRAIN-Q4-C1-S1-productive_cycle_self_loop_path",
	},
	{
		dataset: "generated_fixed_suffix_expansion_v3_d5_f8_r4_x3_i0_m1_q3_z0_c0_s0_p0",
		name:    "GFSE-V3-TRAIN-D05-F008-R4-X3-I0-M1-Q3-path",
	},
}

// buildSuffixReverseGuardFeasibilityReport evaluates the actual production-
// shaped guard, not a stripped SQL proxy. Protected holdout records are
// rejected before their timing can influence this decision.
func buildSuffixReverseGuardFeasibilityReport(
	incumbentRecords, reverseRecords, guardedRecords []CaseResult,
	aa *AAResolutionReport,
	options SuffixReverseGuardFeasibilityOptions,
) (SuffixReverseGuardFeasibilityReport, error) {
	if options.Confidence <= 0 || options.Confidence >= 1 {
		return SuffixReverseGuardFeasibilityReport{}, fmt.Errorf("confidence level must be between 0 and 1")
	}
	if options.BootstrapCount == 0 {
		options.BootstrapCount = defaultBootstrapCount
	}
	if options.BootstrapCount < 1 {
		return SuffixReverseGuardFeasibilityReport{}, fmt.Errorf("bootstrap count must be positive")
	}
	if err := validateAAResolutionEvidence(aa, incumbentRecords, options.Confidence); err != nil {
		return SuffixReverseGuardFeasibilityReport{}, fmt.Errorf("incumbent A/A evidence: %w", err)
	}
	if err := validateOrientationV2AAEvidence(aa, incumbentRecords); err != nil {
		return SuffixReverseGuardFeasibilityReport{}, fmt.Errorf("incumbent A/A environment: %w", err)
	}
	identity, err := validateOrientationV2EvidenceIdentity(incumbentRecords, reverseRecords, guardedRecords)
	if err != nil {
		return SuffixReverseGuardFeasibilityReport{}, err
	}
	series, keys, err := collectSuffixReverseGuardSeries(incumbentRecords, reverseRecords, guardedRecords)
	if err != nil {
		return SuffixReverseGuardFeasibilityReport{}, err
	}
	if err := validateSuffixReverseGuardFeasibilityCohort(keys, incumbentRecords, reverseRecords, guardedRecords); err != nil {
		return SuffixReverseGuardFeasibilityReport{}, err
	}
	if err := validateSuffixReverseGuardRunSchedule(incumbentRecords, reverseRecords, guardedRecords, len(keys)); err != nil {
		return SuffixReverseGuardFeasibilityReport{}, err
	}
	report := SuffixReverseGuardFeasibilityReport{
		Version:         suffixReverseGuardFeasibilityVersion,
		Policy:          string(optimize.ExpansionSearchPolicySuffixReverseGuardV1),
		SelectorVersion: optimize.ExpansionSearchSelectorFixedSuffixPathV1,
		Protocol:        "training_feasibility",
		Seed:            options.Seed,
		Confidence:      options.Confidence,
		SourceCommit:    identity.sourceCommit,
		DirtyDiffSHA256: identity.dirtyDiffSHA256,
		BinarySHA256:    identity.binarySHA256,
		CorpusSHA256:    identity.corpusSHA256,
		Caps: map[string]int64{
			"suffix_rows": optimize.ExpansionSearchSuffixReverseGuardSuffixRowLimit,
			"state_rows":  optimize.ExpansionSearchSuffixReverseGuardStateLimit,
		},
		GuardRatioUpperLimit: 1.10, GuardAbsoluteUpperLimit: 100 * time.Microsecond,
		ForwardRatioUpperLimit: .95, ForwardSavingLowerLimit: 100 * time.Microsecond, ForwardP95UpperLimit: 1.05,
		EvidencePassed: true,
	}
	gateOptions := PerfGateOptions{Seed: options.Seed, Confidence: options.Confidence, BootstrapCount: options.BootstrapCount}
	for index, key := range keys {
		current := series[key]
		if current.split == "holdout" {
			return SuffixReverseGuardFeasibilityReport{}, fmt.Errorf("%s/%s feasibility input opens a protected holdout", key.dataset, key.name)
		}
		if current.split != "training" {
			return SuffixReverseGuardFeasibilityReport{}, fmt.Errorf("%s/%s feasibility input must use the predeclared training split, got %q", key.dataset, key.name, current.split)
		}
		rounds := sortedRounds(current.incumbent)
		if len(rounds) != 6 || !slices.Equal(rounds, sortedRounds(current.reverse)) || !slices.Equal(rounds, sortedRounds(current.guarded)) {
			return SuffixReverseGuardFeasibilityReport{}, fmt.Errorf("%s/%s requires exactly six matched three-arm rounds", key.dataset, key.name)
		}
		for _, round := range rounds {
			if len(current.incumbent[round]) != 10 || len(current.reverse[round]) != 10 || len(current.guarded[round]) != 10 {
				return SuffixReverseGuardFeasibilityReport{}, fmt.Errorf("%s/%s round %d requires exactly ten samples per arm", key.dataset, key.name, round)
			}
		}
		if err := validateSuffixReverseGuardArmOrder(incumbentRecords, reverseRecords, guardedRecords, key, rounds); err != nil {
			return SuffixReverseGuardFeasibilityReport{}, err
		}
		seed := options.Seed + int64(index)*7919
		_, aaFloor, err := aaTimingFloor(aa, key, false, 0)
		if err != nil {
			return SuffixReverseGuardFeasibilityReport{}, err
		}
		absoluteFloor := max(report.GuardAbsoluteUpperLimit, aaFloor)
		guardOverhead := orientationLatencyGate(
			string(optimize.ExpansionSearchSuffixSeededReverse), report.Policy,
			current.reverse, current.guarded, report.GuardRatioUpperLimit, report.GuardAbsoluteUpperLimit, seed, gateOptions,
		)
		fastestIdentity, fastest := fastestOrientationExactArm(current.incumbent, current.reverse)
		fastestRegret := orientationLatencyGate(fastestIdentity, report.Policy, fastest, current.guarded, report.GuardRatioUpperLimit, absoluteFloor, seed+3, gateOptions)
		improvement := SuffixReverseGuardImprovementGate{
			MedianRatio:     bootstrapRoundMedianRatio(current.incumbent, current.guarded, seed+6, gateOptions),
			MedianSaving:    bootstrapRoundMedianSaving(current.incumbent, current.guarded, seed+7, gateOptions),
			P95Ratio:        bootstrapStratifiedP95Ratio(current.incumbent, current.guarded, seed+8, gateOptions),
			RatioUpperLimit: report.ForwardRatioUpperLimit, SavingLowerLimit: report.ForwardSavingLowerLimit, P95UpperLimit: report.ForwardP95UpperLimit,
		}
		improvement.Passed = (improvement.MedianRatio.Upper <= improvement.RatioUpperLimit || improvement.MedianSaving.Lower >= improvement.SavingLowerLimit) &&
			improvement.P95Ratio.Upper <= improvement.P95UpperLimit
		entry := SuffixReverseGuardFeasibilityCase{
			Dataset: key.dataset, Name: key.name, QualificationSplit: current.split, QuerySHA256: current.querySHA, Rounds: len(rounds),
			ExactObservationsMatched: true, RuntimeIdentity: current.runtime, RuntimeBranch: current.branch,
			Overflow: current.overflow, FallbackExecuted: current.fallback, GuardOverhead: guardOverhead,
			FastestExactRegret: fastestRegret, ForwardImprovement: improvement,
		}
		if current.runtime != string(optimize.ExpansionSearchSuffixSeededReverse) || current.branch != "suffix_seeded_reverse" || current.overflow || current.fallback {
			entry.Reasons = append(entry.Reasons, "normal feasibility case did not execute the admitted reverse candidate without fallback")
		}
		if !guardOverhead.Passed {
			entry.Reasons = append(entry.Reasons, "guard overhead exceeds the 1.10/100us stop gate")
		}
		if !fastestRegret.Passed {
			entry.Reasons = append(entry.Reasons, "guard regret exceeds the 1.10/A/A-calibrated stop gate")
		}
		if !improvement.Passed {
			entry.Reasons = append(entry.Reasons, "guard does not materially improve forward p50 with contained p95")
		}
		entry.Passed = len(entry.Reasons) == 0
		if !entry.Passed {
			report.EvidencePassed = false
		}
		report.Cases = append(report.Cases, entry)
	}
	report.Passed = len(report.Cases) > 0 && report.EvidencePassed
	return report, nil
}

// validateSuffixReverseGuardFeasibilityCohort binds this early decision to the
// two already-open complete-path V3 training declarations. All three arms
// must carry the same schema-v2 selection declaration and resolved manifest;
// filtered timing from any other training workload is rejected.
func validateSuffixReverseGuardFeasibilityCohort(keys []performanceKey, artifacts ...[]CaseResult) error {
	expected := map[performanceKey]struct{}{}
	declared := make([]DeclaredCaseBackend, 0, 2*len(suffixReverseGuardFeasibilityCases))
	for _, testCase := range suffixReverseGuardFeasibilityCases {
		expected[performanceKey{dataset: testCase.dataset, name: testCase.name, backend: ModePostgresSQL}] = struct{}{}
		for _, backend := range []ExecutionMode{ModePostgresSQL, ModeNeo4j} {
			declared = append(declared, DeclaredCaseBackend{Dataset: testCase.dataset, Name: testCase.name, Backend: backend})
		}
	}
	if !orientationV2KeySetsEqual(expected, performanceKeySet(keys)) {
		return fmt.Errorf("suffix-reverse feasibility does not contain the exact two-case training cohort")
	}
	expectedDeclaration := declarationSHA256(declared)
	expectedResolved := ""
	for index, records := range artifacts {
		selection, err := selectionIdentity(records)
		if err != nil {
			return fmt.Errorf("suffix-reverse feasibility arm %d selection: %w", index+1, err)
		}
		if err := validateSelectionManifestAccounting(selection); err != nil {
			return fmt.Errorf("suffix-reverse feasibility arm %d selection accounting: %w", index+1, err)
		}
		if selection.Version != selectionManifestVersion || !selection.DiagnosticOnly ||
			selection.SelectedDeclarationCount != 2*len(expected) || len(selection.Resolved) != len(expected) ||
			selection.FullDeclarationCount != selection.SelectedDeclarationCount+selection.OmittedDeclarationCount ||
			selection.DeclarationSHA256 != expectedDeclaration {
			return fmt.Errorf("suffix-reverse feasibility selection does not bind the exact two-case declaration")
		}
		resolved := map[performanceKey]struct{}{}
		for _, item := range selection.Resolved {
			if item.Category != "generated_fixed_suffix_expansion" {
				return fmt.Errorf("suffix-reverse feasibility selection contains category %q", item.Category)
			}
			resolved[performanceKey{dataset: item.Dataset, name: item.Name, backend: ModePostgresSQL}] = struct{}{}
		}
		if !orientationV2KeySetsEqual(expected, resolved) {
			return fmt.Errorf("suffix-reverse feasibility selection does not resolve the exact two-case cohort")
		}
		resolvedSHA := resolvedSelectionSHA256(selection.Resolved)
		if expectedResolved == "" {
			expectedResolved = resolvedSHA
		} else if expectedResolved != resolvedSHA {
			return fmt.Errorf("suffix-reverse feasibility arms mix resolved selection identities")
		}
	}
	return nil
}

func collectSuffixReverseGuardSeries(
	incumbentRecords, reverseRecords, guardedRecords []CaseResult,
) (map[performanceKey]*suffixReverseGuardSeries, []performanceKey, error) {
	artifacts := []struct {
		name    string
		records []CaseResult
	}{{"incumbent", incumbentRecords}, {"reverse", reverseRecords}, {"guarded", guardedRecords}}
	keySets := make([]map[performanceKey]struct{}, len(artifacts))
	for index, artifact := range artifacts {
		keys, err := orientationV2ArtifactKeys(artifact.name, artifact.records)
		if err != nil {
			return nil, nil, err
		}
		keySets[index] = keys
	}
	if !orientationV2KeySetsEqual(keySets[0], keySets[1]) || !orientationV2KeySetsEqual(keySets[0], keySets[2]) {
		return nil, nil, fmt.Errorf("suffix-reverse guard three-arm case sets do not match")
	}
	series := map[performanceKey]*suffixReverseGuardSeries{}
	for key := range keySets[0] {
		series[key] = &suffixReverseGuardSeries{incumbent: roundSamples{}, reverse: roundSamples{}, guarded: roundSamples{}}
	}
	for _, artifact := range artifacts {
		seen := map[performanceKey]map[int]struct{}{}
		for _, record := range artifact.records {
			key := performanceKey{dataset: record.Dataset, name: record.Name, backend: record.ExecutionMode}
			current := series[key]
			if current == nil {
				return nil, nil, fmt.Errorf("%s artifact contains unexpected case %s/%s", artifact.name, key.dataset, key.name)
			}
			if err := validateSuffixReverseGuardRecord(record, artifact.name); err != nil {
				return nil, nil, err
			}
			round, err := orientationV2RecordRound(record)
			if err != nil {
				return nil, nil, err
			}
			if seen[key] == nil {
				seen[key] = map[int]struct{}{}
			}
			if _, duplicate := seen[key][round]; duplicate {
				return nil, nil, fmt.Errorf("%s/%s %s artifact duplicates round %d", key.dataset, key.name, artifact.name, round)
			}
			seen[key][round] = struct{}{}
			if strings.TrimSpace(record.Cypher) == "" {
				return nil, nil, fmt.Errorf("%s/%s lacks exact Cypher for query-SHA binding", key.dataset, key.name)
			}
			querySHA := pgdriver.TraversalPolicyQuerySHA256(record.Cypher)
			if current.querySHA == "" {
				current.querySHA, current.split = querySHA, record.Shape.QualificationSplit
			} else if current.querySHA != querySHA || current.split != record.Shape.QualificationSplit {
				return nil, nil, fmt.Errorf("%s/%s changes workload digest or split across arms", key.dataset, key.name)
			}
			switch artifact.name {
			case "incumbent":
				appendOrientationWarmSamples(current.incumbent, record)
			case "reverse":
				appendOrientationWarmSamples(current.reverse, record)
			case "guarded":
				summary := record.TraversalTelemetry.Summary
				if current.observed && (current.runtime != summary.RuntimeIdentity || current.branch != summary.RuntimeBranch || current.overflow != *summary.Overflow || current.fallback != *summary.FallbackExecuted) {
					return nil, nil, fmt.Errorf("%s/%s changes guarded runtime outcome across rounds", key.dataset, key.name)
				}
				current.runtime, current.branch, current.overflow, current.fallback, current.observed =
					summary.RuntimeIdentity, summary.RuntimeBranch, *summary.Overflow, *summary.FallbackExecuted, true
				appendOrientationWarmSamples(current.guarded, record)
			}
		}
	}
	keys := sortedPerformanceKeys(keySets[0])
	for _, key := range keys {
		if !series[key].observed {
			return nil, nil, fmt.Errorf("%s/%s lacks guarded runtime evidence", key.dataset, key.name)
		}
		if err := validateOrientationExactObservations(key, incumbentRecords, reverseRecords, guardedRecords); err != nil {
			return nil, nil, err
		}
	}
	return series, keys, nil
}

func validateSuffixReverseGuardRecord(record CaseResult, arm string) error {
	if record.Status != StatusOK || record.Environment == nil || record.PostgresEnvironment == nil || record.TraversalTelemetry == nil {
		return fmt.Errorf("%s/%s %s arm lacks a successful telemetry-bearing PostgreSQL record", record.Dataset, record.Name, arm)
	}
	if record.Environment.ArtifactSchemaVersion != 2 || record.Environment.PoolSize != 1 || len(record.Environment.Concurrency) != 0 ||
		record.Environment.ExistingGraph || record.Fixture == nil || record.Fixture.Dataset != record.Dataset ||
		!lowercaseSHA256(record.Fixture.Checksum) || !record.Fixture.PhysicalValidated {
		return fmt.Errorf("%s/%s %s arm lacks the schema-v2 single-session physical-fixture contract", record.Dataset, record.Name, arm)
	}
	if !lowercaseSHA256(record.WorkloadSHA256) || !lowercaseSHA256(record.SQLFingerprint) ||
		len(record.Concurrency) != 0 || len(record.PostgresReferences) != 0 || record.ClientWaterfall != nil ||
		record.RawPGXWaterfall != nil || record.RawPGXRoundTrip != nil {
		return fmt.Errorf("%s/%s %s arm mixes incomplete identity with supplemental measurements", record.Dataset, record.Name, arm)
	}
	if !strings.EqualFold(strings.TrimSpace(record.PostgresEnvironment.TransactionIsolation), "repeatable read") ||
		!record.Shape.PathMaterializationRequired || record.Shape.QualificationSplit == "holdout" {
		return fmt.Errorf("%s/%s %s arm is outside the training-only full-path Repeatable Read envelope", record.Dataset, record.Name, arm)
	}
	environment := record.Environment
	if environment.Round < 1 || environment.Block != environment.Round {
		return fmt.Errorf("%s/%s %s arm requires block equal to round", record.Dataset, record.Name, arm)
	}
	if environment.Arm != arm || environment.ArmOrder < 1 || environment.ArmOrder > 3 || strings.TrimSpace(environment.RunUUID) == "" {
		return fmt.Errorf("%s/%s %s arm has malformed three-arm run metadata", record.Dataset, record.Name, arm)
	}
	if environment.StartedAt.IsZero() || environment.EndedAt.IsZero() || environment.EndedAt.Before(environment.StartedAt) {
		return fmt.Errorf("%s/%s %s arm has malformed invocation timestamps", record.Dataset, record.Name, arm)
	}
	if arm == "incumbent" || arm == "reverse" {
		if err := validateOrientationV2Record(record, arm); err != nil {
			return fmt.Errorf("%s/%s exact %s arm: %w", record.Dataset, record.Name, arm, err)
		}
	}
	if err := record.TraversalTelemetry.Validate(); err != nil {
		return fmt.Errorf("%s/%s %s arm telemetry: %w", record.Dataset, record.Name, arm, err)
	}
	summary := record.TraversalTelemetry.Summary
	if summary.RuntimeOutcomeAvailable == nil || !*summary.RuntimeOutcomeAvailable || summary.Overflow == nil || summary.FallbackExecuted == nil {
		return fmt.Errorf("%s/%s %s arm lacks a complete runtime outcome", record.Dataset, record.Name, arm)
	}
	forward, reverse := string(optimize.ExpansionSearchStepwiseForward), string(optimize.ExpansionSearchSuffixSeededReverse)
	switch arm {
	case "incumbent":
		// validateOrientationV2Record already restricts this to either a direct
		// selected-forward tuple or the exact compile-time fallback tuple. Both
		// execute the same incumbent SQL boundary and are valid exact controls.
		if summary.RuntimeIdentity != forward || summary.AppliedIdentity != forward || *summary.Overflow {
			return fmt.Errorf("%s/%s incumbent arm did not execute exact forward", record.Dataset, record.Name)
		}
	case "reverse":
		if summary.RuntimeIdentity != reverse || summary.AppliedIdentity != reverse || *summary.Overflow || *summary.FallbackExecuted {
			return fmt.Errorf("%s/%s reverse arm did not execute exact reverse", record.Dataset, record.Name)
		}
	case "guarded":
		if summary.EmittedIdentity != string(optimize.ExpansionSearchPolicySuffixReverseGuardV1) ||
			summary.SelectorVersion != optimize.ExpansionSearchSelectorFixedSuffixPathV1 ||
			summary.ExecutionBoundary != optimize.ExpansionSearchExecutionBoundaryGuardedDualArm ||
			summary.ObservationMode != string(optimize.ExpansionSearchObservationFullPath) || summary.WouldSelectIdentity != "" {
			return fmt.Errorf("%s/%s guarded arm does not prove suffix-reverse-guard-v1", record.Dataset, record.Name)
		}
		if err := validateSuffixReverseGuardDiagnostic(record); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown suffix-reverse guard arm %q", arm)
	}
	for _, sample := range record.Stats.Samples {
		if sample.Classification != "warm" || sample.Duration <= 0 {
			continue
		}
		if sample.Round != environment.Round || sample.Block != environment.Block || sample.Arm != environment.Arm ||
			sample.ArmOrder != environment.ArmOrder || sample.RunUUID != environment.RunUUID {
			return fmt.Errorf("%s/%s %s warm sample is outside its physical arm invocation", record.Dataset, record.Name, arm)
		}
		if sample.RequestedIdentity != summary.RequestedIdentity || sample.RuntimeIdentity != summary.RuntimeIdentity ||
			sample.RuntimeBranch != summary.RuntimeBranch || sample.FallbackExecuted == nil ||
			*sample.FallbackExecuted != *summary.FallbackExecuted {
			return fmt.Errorf("%s/%s %s warm sample contradicts runtime summary", record.Dataset, record.Name, arm)
		}
		if arm == "guarded" {
			if sample.RequestedIdentity != reverse || sample.RuntimeAttestation != "timed_invocation" || strings.TrimSpace(sample.RuntimeInvocationID) == "" ||
				strings.TrimSpace(sample.ConnectionID) == "" || validateRuntimeReceiptEvents(sample.RuntimeReceiptEvents, sample.RuntimeIdentity, sample.RuntimeBranch, sample.FallbackExecuted) != nil {
				return fmt.Errorf("%s/%s guarded warm sample lacks a valid timed receipt", record.Dataset, record.Name)
			}
			for _, event := range sample.RuntimeReceiptEvents {
				if event.InvocationID != sample.RuntimeInvocationID {
					return fmt.Errorf("%s/%s guarded warm sample receipt is not bound to its timed invocation", record.Dataset, record.Name)
				}
			}
		}
	}
	return nil
}

// validateSuffixReverseGuardRunSchedule proves that the doubled-Williams
// labels describe the order in which the three arm processes actually ran.
// It mirrors the SP-I2 chronology contract: blocks equal rounds, all selected
// cases share one invocation interval per arm/round, arms do not overlap, and
// later rounds neither overlap nor predate earlier rounds.
func validateSuffixReverseGuardRunSchedule(
	incumbentRecords, reverseRecords, guardedRecords []CaseResult,
	expectedCaseCount int,
) error {
	arms := []struct {
		name    string
		records []CaseResult
	}{{"incumbent", incumbentRecords}, {"reverse", reverseRecords}, {"guarded", guardedRecords}}

	invocations := make([]map[int]suffixReverseGuardInvocationIdentity, len(arms))
	for index, arm := range arms {
		invocations[index] = map[int]suffixReverseGuardInvocationIdentity{}
		caseCounts := map[int]int{}
		for _, record := range arm.records {
			if record.Environment == nil {
				return fmt.Errorf("%s/%s %s arm lacks invocation chronology", record.Dataset, record.Name, arm.name)
			}
			environment := record.Environment
			identity := suffixReverseGuardInvocationIdentity{
				round:     environment.Round,
				block:     environment.Block,
				order:     environment.ArmOrder,
				arm:       environment.Arm,
				runUUID:   environment.RunUUID,
				startedAt: environment.StartedAt,
				endedAt:   environment.EndedAt,
			}
			if identity.startedAt.IsZero() || identity.endedAt.IsZero() || identity.endedAt.Before(identity.startedAt) {
				return fmt.Errorf("suffix-reverse guard %s round %d has malformed invocation timestamps", arm.name, identity.round)
			}
			if prior, found := invocations[index][identity.round]; found && prior != identity {
				return fmt.Errorf("suffix-reverse guard %s round %d mixes invocation identities across the exact cohort", arm.name, identity.round)
			}
			invocations[index][identity.round] = identity
			caseCounts[identity.round]++
		}
		if len(invocations[index]) != 6 {
			return fmt.Errorf("suffix-reverse guard %s artifact does not contain exactly six physical round invocations", arm.name)
		}
		for round, count := range caseCounts {
			if count != expectedCaseCount {
				return fmt.Errorf("suffix-reverse guard %s round %d contains %d cases, expected %d", arm.name, round, count, expectedCaseCount)
			}
		}
	}

	positionCounts := make([][4]int, len(arms))
	schedule := map[string]int{}
	runUUID := ""
	var priorEnded time.Time
	for round := 1; round <= 6; round++ {
		orderedArms := [4]string{}
		orderedInvocations := [4]suffixReverseGuardInvocationIdentity{}
		seenPositions := map[int]struct{}{}
		seenNames := map[string]struct{}{}
		for index, arm := range arms {
			current, found := invocations[index][round]
			if !found {
				return fmt.Errorf("suffix-reverse guard invocation schedule must use contiguous rounds 1 through 6")
			}
			if current.block != round {
				return fmt.Errorf("suffix-reverse guard round %d requires block equal to round", round)
			}
			if current.arm != arm.name || current.order < 1 || current.order > 3 || strings.TrimSpace(current.runUUID) == "" {
				return fmt.Errorf("suffix-reverse guard round %d has malformed %s invocation identity", round, arm.name)
			}
			if _, duplicate := seenPositions[current.order]; duplicate {
				return fmt.Errorf("suffix-reverse guard round %d duplicates a physical three-arm position", round)
			}
			if _, duplicate := seenNames[current.arm]; duplicate {
				return fmt.Errorf("suffix-reverse guard round %d duplicates a physical arm identity", round)
			}
			seenPositions[current.order], seenNames[current.arm] = struct{}{}, struct{}{}
			positionCounts[index][current.order]++
			orderedArms[current.order], orderedInvocations[current.order] = current.arm, current
			if runUUID == "" {
				runUUID = current.runUUID
			} else if runUUID != current.runUUID {
				return fmt.Errorf("suffix-reverse guard artifacts mix run UUIDs across arms or rounds")
			}
		}
		if len(seenPositions) != 3 || len(seenNames) != 3 {
			return fmt.Errorf("suffix-reverse guard round %d lacks a complete physical three-arm block", round)
		}
		for position := 2; position <= 3; position++ {
			if orderedInvocations[position-1].endedAt.After(orderedInvocations[position].startedAt) {
				return fmt.Errorf("suffix-reverse guard round %d arm timestamps contradict the declared execution order", round)
			}
		}
		if !priorEnded.IsZero() && priorEnded.After(orderedInvocations[1].startedAt) {
			return fmt.Errorf("suffix-reverse guard round %d overlaps or predates the prior round", round)
		}
		priorEnded = orderedInvocations[3].endedAt
		schedule[strings.Join(orderedArms[1:], "/")]++
	}
	for index, counts := range positionCounts {
		if counts[1] != 2 || counts[2] != 2 || counts[3] != 2 {
			return fmt.Errorf("suffix-reverse guard %s arm does not physically follow the six-round doubled-Williams schedule", arms[index].name)
		}
	}
	if len(schedule) != 6 {
		return fmt.Errorf("suffix-reverse guard physical schedule does not contain all six doubled-Williams arm orders exactly once")
	}
	for _, count := range schedule {
		if count != 1 {
			return fmt.Errorf("suffix-reverse guard physical schedule repeats a doubled-Williams arm order")
		}
	}
	return nil
}

// validateSuffixReverseGuardDiagnostic binds feasibility timing to the exact
// immutable caps, the independent suffix-guard counter family, and a
// marker-first plan shape that proves the inactive arm did not initialize.
func validateSuffixReverseGuardDiagnostic(record CaseResult) error {
	telemetry := record.TraversalTelemetry
	if telemetry.Level != TraversalTelemetryLevelDiagnostic || telemetry.Diagnostic == nil ||
		telemetry.Diagnostic.CounterStatus != TraversalTelemetryCounterStatusComplete || telemetry.Diagnostic.PlanReplay == nil {
		return fmt.Errorf("%s/%s guarded arm lacks a complete untimed diagnostic replay", record.Dataset, record.Name)
	}
	if record.PostgresMetrics == nil {
		return fmt.Errorf("%s/%s guarded arm lacks the measured PostgreSQL plan used by its replay", record.Dataset, record.Name)
	}
	for _, family := range []TraversalTelemetryFamily{
		TraversalTelemetryFamilySuffixGuard, TraversalTelemetryFamilyOrdinary, TraversalTelemetryFamilyHydration,
	} {
		if !slices.Contains(telemetry.Diagnostic.RequiredFamilies, family) {
			return fmt.Errorf("%s/%s guarded arm lacks required %s telemetry", record.Dataset, record.Name, family)
		}
	}
	if telemetry.Diagnostic.Counters.Orientation != nil {
		return fmt.Errorf("%s/%s guarded arm incorrectly carries orientation topology counters", record.Dataset, record.Name)
	}
	summary := telemetry.Summary
	if len(summary.Caps) != 2 ||
		summary.Caps["suffix_rows"] != optimize.ExpansionSearchSuffixReverseGuardSuffixRowLimit ||
		summary.Caps["state_rows"] != optimize.ExpansionSearchSuffixReverseGuardStateLimit {
		return fmt.Errorf("%s/%s guarded arm does not bind the immutable suffix/state caps", record.Dataset, record.Name)
	}
	gate := ResourceGateCase{}
	appendSuffixGuardAttributionReasons(&gate, telemetry.Diagnostic)
	if len(gate.Reasons) > 0 {
		return fmt.Errorf("%s/%s guarded arm plan attribution: %s", record.Dataset, record.Name, strings.Join(gate.Reasons, "; "))
	}

	counters := telemetry.Diagnostic.Counters.SuffixGuard
	if counters == nil {
		return fmt.Errorf("%s/%s guarded arm lacks typed suffix-guard counters", record.Dataset, record.Name)
	}
	planCounters := telemetry.Diagnostic.PlanReplay.Counters
	derivedCounters := postgresTraversalPlanReplay(*record.PostgresMetrics).Counters
	for name, typed := range map[string]*int64{
		"suffix_guard_root_presence_rows":       counters.RootPresenceRows,
		"suffix_guard_suffix_rows":              counters.SuffixRows,
		"suffix_guard_boundary_rows":            counters.DistinctBoundaryRows,
		"suffix_guard_state_rows":               counters.StateRows,
		"suffix_guard_output_rows":              counters.OutputRows,
		"suffix_guard_candidate_marker_rows":    counters.CandidateMarkerRows,
		"suffix_guard_fallback_marker_rows":     counters.FallbackMarkerRows,
		"suffix_guard_candidate_branch_rows":    counters.CandidateBranchRows,
		"suffix_guard_fallback_branch_rows":     counters.FallbackBranchRows,
		"suffix_guard_candidate_executor_loops": counters.CandidateExecutorLoops,
		"suffix_guard_fallback_executor_loops":  counters.FallbackExecutorLoops,
	} {
		planValue, planPresent := planCounters[name]
		derivedValue, derivedPresent := derivedCounters[name]
		if typed == nil || !planPresent || !derivedPresent || *typed != planValue || planValue != derivedValue {
			return fmt.Errorf("%s/%s guarded arm counter %s is not bound to its measured plan", record.Dataset, record.Name, name)
		}
	}
	rootRows, suffixRows := *counters.RootPresenceRows, *counters.SuffixRows
	boundaryRows, stateRows, outputRows := *counters.DistinctBoundaryRows, *counters.StateRows, *counters.OutputRows
	suffixOverflow := suffixRows > optimize.ExpansionSearchSuffixReverseGuardSuffixRowLimit
	stateOverflow := stateRows > optimize.ExpansionSearchSuffixReverseGuardStateLimit
	overflow := suffixOverflow || stateOverflow
	if rootRows < 0 || rootRows > 1 || suffixRows < 0 ||
		suffixRows > optimize.ExpansionSearchSuffixReverseGuardSuffixRowLimit+1 ||
		boundaryRows < 0 || boundaryRows > suffixRows || stateRows < 0 ||
		stateRows > optimize.ExpansionSearchSuffixReverseGuardStateLimit+1 || outputRows != record.RowCount {
		return fmt.Errorf("%s/%s guarded arm has impossible bounded-relation counters", record.Dataset, record.Name)
	}
	if rootRows == 0 && (suffixRows != 0 || boundaryRows != 0 || stateRows != 0) {
		return fmt.Errorf("%s/%s guarded arm performed suffix/reverse work without a bound root", record.Dataset, record.Name)
	}
	if suffixOverflow && (boundaryRows != 0 || stateRows != 0) {
		return fmt.Errorf("%s/%s guarded arm performed reverse work after suffix overflow", record.Dataset, record.Name)
	}
	if *counters.SuffixOverflow != suffixOverflow || *counters.StateOverflow != stateOverflow ||
		*summary.Overflow != overflow || *summary.FallbackExecuted != overflow {
		return fmt.Errorf("%s/%s guarded arm overflow flags contradict its cap+1 counters", record.Dataset, record.Name)
	}
	if !overflow {
		if summary.RuntimeIdentity != string(optimize.ExpansionSearchSuffixSeededReverse) || summary.RuntimeBranch != "suffix_seeded_reverse" ||
			*counters.CandidateMarkerRows != 1 || *counters.FallbackMarkerRows != 0 {
			return fmt.Errorf("%s/%s guarded arm did not execute the admitted reverse candidate", record.Dataset, record.Name)
		}
		return nil
	}
	expectedBranch := "exact_forward_state_overflow"
	if suffixOverflow {
		expectedBranch = "exact_forward_suffix_overflow"
	}
	if summary.RuntimeIdentity != string(optimize.ExpansionSearchStepwiseForward) || summary.RuntimeBranch != expectedBranch ||
		summary.FallbackIdentity != string(optimize.ExpansionSearchStepwiseForward) ||
		*counters.CandidateMarkerRows != 0 || *counters.FallbackMarkerRows != 1 {
		return fmt.Errorf("%s/%s guarded arm did not execute the exact overflow fallback", record.Dataset, record.Name)
	}
	return nil
}

func validateSuffixReverseGuardArmOrder(
	incumbentRecords, reverseRecords, guardedRecords []CaseResult,
	key performanceKey,
	rounds []int,
) error {
	arms := []struct {
		name    string
		records []CaseResult
	}{{"incumbent", incumbentRecords}, {"reverse", reverseRecords}, {"guarded", guardedRecords}}
	evidence := make([]map[int]pairedRoundEvidence, len(arms))
	positions := make([][4]int, len(arms))
	schedule := map[string]int{}
	for index, arm := range arms {
		current, err := collectPairedRoundEvidence(arm.records, key)
		if err != nil {
			return err
		}
		evidence[index] = current
	}
	for _, round := range rounds {
		seenPositions, seenNames := map[int]struct{}{}, map[string]struct{}{}
		orderedArms := [4]string{}
		block, runUUID := 0, ""
		for index, arm := range arms {
			current, found := evidence[index][round]
			if !found || current.Warmups != 5 || current.Arm != arm.name || current.ArmOrder < 1 || current.ArmOrder > 3 {
				return fmt.Errorf("%s/%s round %d lacks %s arm identity, warmups, or order", key.dataset, key.name, round, arm.name)
			}
			if _, duplicate := seenPositions[current.ArmOrder]; duplicate {
				return fmt.Errorf("%s/%s round %d duplicates a three-arm position", key.dataset, key.name, round)
			}
			if _, duplicate := seenNames[current.Arm]; duplicate {
				return fmt.Errorf("%s/%s round %d duplicates an arm label", key.dataset, key.name, round)
			}
			seenPositions[current.ArmOrder], seenNames[current.Arm], positions[index][current.ArmOrder] = struct{}{}, struct{}{}, positions[index][current.ArmOrder]+1
			orderedArms[current.ArmOrder] = current.Arm
			if block == 0 {
				block, runUUID = current.Block, current.RunUUID
			} else if current.Block != block || current.RunUUID != runUUID {
				return fmt.Errorf("%s/%s round %d has mismatched block or run UUID", key.dataset, key.name, round)
			}
		}
		if block < 1 || runUUID == "" || len(seenPositions) != 3 || len(seenNames) != 3 {
			return fmt.Errorf("%s/%s round %d lacks a complete three-arm block", key.dataset, key.name, round)
		}
		schedule[strings.Join(orderedArms[1:], "/")]++
	}
	for index, counts := range positions {
		if counts[1] != 2 || counts[2] != 2 || counts[3] != 2 {
			return fmt.Errorf("%s/%s %s arm does not follow the six-round doubled-Williams schedule", key.dataset, key.name, arms[index].name)
		}
	}
	if len(schedule) != 6 {
		return fmt.Errorf("%s/%s does not contain all six doubled-Williams arm orders exactly once", key.dataset, key.name)
	}
	for _, count := range schedule {
		if count != 1 {
			return fmt.Errorf("%s/%s repeats a doubled-Williams arm order", key.dataset, key.name)
		}
	}
	return nil
}

// createSuffixReverseGuardFeasibilityReport loads, evaluates, and writes the
// bounded stop-gate artifact.
func createSuffixReverseGuardFeasibilityReport(
	incumbentPath, reversePath, guardedPath, aaPath, outputPath string,
	options SuffixReverseGuardFeasibilityOptions,
) (bool, error) {
	incumbent, err := readJSONLFile(incumbentPath)
	if err != nil {
		return false, fmt.Errorf("read suffix-guard incumbent artifact: %w", err)
	}
	reverse, err := readJSONLFile(reversePath)
	if err != nil {
		return false, fmt.Errorf("read suffix-guard reverse artifact: %w", err)
	}
	guarded, err := readJSONLFile(guardedPath)
	if err != nil {
		return false, fmt.Errorf("read suffix-guard guarded artifact: %w", err)
	}
	aa, aaSHA, err := loadAAResolutionReport(aaPath)
	if err != nil {
		return false, fmt.Errorf("read suffix-guard A/A report: %w", err)
	}
	report, err := buildSuffixReverseGuardFeasibilityReport(incumbent, reverse, guarded, aa, options)
	if err != nil {
		return false, err
	}
	for destination, source := range map[*string]string{
		&report.IncumbentArtifactSHA256: incumbentPath, &report.ReverseArtifactSHA256: reversePath,
		&report.GuardedArtifactSHA256: guardedPath, &report.AAReportSHA256: aaPath,
	} {
		digest, err := fileSHA256(source)
		if err != nil {
			return false, err
		}
		*destination = digest
	}
	report.AAReportSHA256 = aaSHA
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return false, err
	}
	if outputPath == "" {
		_, err = os.Stdout.Write(append(raw, '\n'))
	} else {
		err = os.WriteFile(outputPath, append(raw, '\n'), 0o644)
	}
	return report.Passed, err
}
