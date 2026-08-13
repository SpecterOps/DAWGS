// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
)

const orientationSelectorReportV2Version = 2

type OrientationSelectorV2FreezeManifest struct {
	Version                 int              `json:"version"`
	Policy                  string           `json:"policy"`
	Formula                 string           `json:"formula"`
	Caps                    map[string]int64 `json:"caps"`
	SourceCommit            string           `json:"source_commit"`
	DirtyDiffSHA256         string           `json:"dirty_diff_sha256"`
	BinarySHA256            string           `json:"binary_sha256"`
	CohortDeclarationSHA256 string           `json:"cohort_declaration_sha256"`
	DiscoveryReportSHA256   string           `json:"discovery_report_sha256"`
}

// OrientationSelectorV2ReportOptions configures the immutable four-arm v2
// qualification workflow independently of the retained v1 shadow report.
type OrientationSelectorV2ReportOptions struct {
	Seed           int64
	Confidence     float64
	BootstrapCount int
	Protocol       string
	Freeze         *OrientationSelectorV2FreezeManifest
	Discovery      *OrientationSelectorV2Report
}

// OrientationLatencyGateV2 makes conditional applicability explicit. A
// reverse-selected shadow comparison remains visible but cannot qualify or
// disqualify the guarded selector.
type OrientationLatencyGateV2 struct {
	Applicable bool `json:"applicable"`
	OrientationLatencyGate
}

// OrientationSelectorV2Case records exact runtime attribution and the three
// frozen latency gates for one training, holdout, or diagnostic case.
type OrientationSelectorV2Case struct {
	Dataset                  string                   `json:"dataset"`
	Name                     string                   `json:"name"`
	QualificationSplit       string                   `json:"qualification_split"`
	QualificationRole        string                   `json:"qualification_role"`
	ThresholdTuningEligible  bool                     `json:"threshold_tuning_eligible"`
	QualificationEligible    bool                     `json:"qualification_eligible"`
	Rounds                   int                      `json:"matched_rounds"`
	WouldSelectIdentity      string                   `json:"would_select_identity"`
	FastestExactIdentity     string                   `json:"fastest_exact_identity"`
	GuardedRuntimeIdentity   string                   `json:"guarded_runtime_identity"`
	GuardedRuntimeBranch     string                   `json:"guarded_runtime_branch"`
	Overflow                 bool                     `json:"overflow"`
	FallbackExecuted         bool                     `json:"fallback_executed"`
	ExactObservationsMatched bool                     `json:"exact_observations_matched"`
	ShadowForwardOverhead    OrientationLatencyGateV2 `json:"shadow_forward_overhead"`
	GuardedSelectedOverhead  OrientationLatencyGate   `json:"guarded_selected_overhead"`
	GuardedFastestRegret     OrientationLatencyGate   `json:"guarded_fastest_regret"`
	Passed                   bool                     `json:"passed"`
	Reasons                  []string                 `json:"reasons,omitempty"`
}

// OrientationSelectorV2Report binds the immutable selector, source, binary,
// corpus, four timing artifacts, host A/A floor, and qualification outcome.
type OrientationSelectorV2Report struct {
	Version                   int                         `json:"version"`
	Policy                    string                      `json:"policy"`
	Protocol                  string                      `json:"protocol"`
	Seed                      int64                       `json:"seed"`
	Confidence                float64                     `json:"confidence_level"`
	SourceCommit              string                      `json:"source_commit"`
	DirtyDiffSHA256           string                      `json:"dirty_diff_sha256"`
	BinarySHA256              string                      `json:"binary_sha256"`
	CorpusSHA256              string                      `json:"corpus_sha256"`
	CohortDeclarationSHA256   string                      `json:"cohort_declaration_sha256"`
	FreezeManifestSHA256      string                      `json:"freeze_manifest_sha256,omitempty"`
	Formula                   string                      `json:"formula"`
	Caps                      map[string]int64            `json:"caps"`
	ShadowArtifactSHA256      string                      `json:"shadow_artifact_sha256,omitempty"`
	IncumbentArtifactSHA256   string                      `json:"incumbent_artifact_sha256,omitempty"`
	ReverseArtifactSHA256     string                      `json:"reverse_artifact_sha256,omitempty"`
	GuardedArtifactSHA256     string                      `json:"guarded_artifact_sha256,omitempty"`
	AAReportSHA256            string                      `json:"aa_report_sha256,omitempty"`
	ShadowForwardRatioLimit   float64                     `json:"shadow_forward_ratio_upper_limit"`
	GuardedSelectedRatioLimit float64                     `json:"guarded_selected_ratio_upper_limit"`
	GuardedFastestRatioLimit  float64                     `json:"guarded_fastest_ratio_upper_limit"`
	OverheadAbsoluteLimit     time.Duration               `json:"overhead_absolute_limit"`
	EvidencePassed            bool                        `json:"evidence_passed"`
	TrainingCases             int                         `json:"training_cases"`
	HoldoutCases              int                         `json:"holdout_cases"`
	TrainingPassed            bool                        `json:"training_passed"`
	HoldoutPassed             bool                        `json:"holdout_passed"`
	QualificationPassed       bool                        `json:"qualification_passed"`
	Cases                     []OrientationSelectorV2Case `json:"cases"`
}

type orientationSelectorV2Series struct {
	shadow          roundSamples
	incumbent       roundSamples
	reverse         roundSamples
	guarded         roundSamples
	wouldSelect     string
	shadowOverflow  bool
	shadowObserved  bool
	guardedRuntime  string
	guardedBranch   string
	overflow        bool
	fallback        bool
	guardedObserved bool
}

type orientationSelectorV2Identity struct {
	sourceCommit    string
	dirtyDiffSHA256 string
	binarySHA256    string
	corpusSHA256    string
}

// buildOrientationSelectorV2Report evaluates matched shadow, exact forward,
// exact reverse, and actual guarded statements. V1 remains a separate schema
// and code path so new evidence cannot reinterpret its historical result.
func buildOrientationSelectorV2Report(
	shadowRecords, incumbentRecords, reverseRecords, guardedRecords []CaseResult,
	aa *AAResolutionReport,
	options OrientationSelectorV2ReportOptions,
) (OrientationSelectorV2Report, error) {
	if options.Confidence <= 0 || options.Confidence >= 1 {
		return OrientationSelectorV2Report{}, fmt.Errorf("confidence level must be between 0 and 1")
	}
	if options.BootstrapCount == 0 {
		options.BootstrapCount = defaultBootstrapCount
	}
	if options.BootstrapCount < 1 {
		return OrientationSelectorV2Report{}, fmt.Errorf("bootstrap count must be positive")
	}
	protocol := options.Protocol
	if protocol == "" {
		protocol = referencePairProtocolConfirmation
	}
	minimumWarmups, minimumRounds, maximumRounds, minimumSamples := 20, 10, 20, 50
	if protocol == referencePairProtocolDiscovery {
		minimumWarmups, minimumRounds, maximumRounds, minimumSamples = 5, 5, 20, 10
	} else if protocol != referencePairProtocolConfirmation {
		return OrientationSelectorV2Report{}, fmt.Errorf("unsupported orientation selector v2 protocol %q", protocol)
	}

	if err := validateAAResolutionEvidence(aa, incumbentRecords, options.Confidence); err != nil {
		return OrientationSelectorV2Report{}, fmt.Errorf("incumbent A/A evidence: %w", err)
	}
	if err := validateOrientationV2AAEvidence(aa, incumbentRecords); err != nil {
		return OrientationSelectorV2Report{}, fmt.Errorf("incumbent A/A environment: %w", err)
	}
	incumbentHost, err := artifactHostFingerprint(incumbentRecords)
	if err != nil {
		return OrientationSelectorV2Report{}, err
	}
	for name, records := range map[string][]CaseResult{
		"shadow": shadowRecords, "reverse": reverseRecords, "guarded": guardedRecords,
	} {
		host, err := artifactHostFingerprint(records)
		if err != nil {
			return OrientationSelectorV2Report{}, fmt.Errorf("%s artifact host: %w", name, err)
		}
		if host != incumbentHost {
			return OrientationSelectorV2Report{}, fmt.Errorf("%s artifact host does not match incumbent host", name)
		}
	}
	identity, err := validateOrientationV2EvidenceIdentity(shadowRecords, incumbentRecords, reverseRecords, guardedRecords)
	if err != nil {
		return OrientationSelectorV2Report{}, err
	}

	series, keys, err := collectOrientationSelectorV2Series(shadowRecords, incumbentRecords, reverseRecords, guardedRecords)
	if err != nil {
		return OrientationSelectorV2Report{}, err
	}
	cohortDeclarationSHA256, err := validateOrientationV2Cohort(keys, shadowRecords, incumbentRecords, reverseRecords, guardedRecords, protocol)
	if err != nil {
		return OrientationSelectorV2Report{}, err
	}
	report := OrientationSelectorV2Report{
		Version:                 orientationSelectorReportV2Version,
		Policy:                  string(optimize.ExpansionSearchPolicyOrientationProbeV2),
		Protocol:                protocol,
		Seed:                    options.Seed,
		Confidence:              options.Confidence,
		SourceCommit:            identity.sourceCommit,
		DirtyDiffSHA256:         identity.dirtyDiffSHA256,
		BinarySHA256:            identity.binarySHA256,
		CorpusSHA256:            identity.corpusSHA256,
		CohortDeclarationSHA256: cohortDeclarationSHA256,
		Formula:                 "F2=root_rows+maximum_depth*forward_degree_rows;R2=suffix_rows+boundary_rows+reverse_degree_rows;reverse=complete&&4*R2<3*F2",
		Caps: map[string]int64{
			"root_row_limit":               optimize.ExpansionSearchOrientationRootRowLimit,
			"reverse_seed_row_limit":       optimize.ExpansionSearchOrientationReverseSeedRowLimit,
			"directional_degree_row_limit": optimize.ExpansionSearchOrientationDirectionalDegreeRowLimit,
			"state_limit":                  optimize.ExpansionSearchOrientationStateLimit,
		},
		ShadowForwardRatioLimit:   1.10,
		GuardedSelectedRatioLimit: 1.10,
		GuardedFastestRatioLimit:  1.10,
		OverheadAbsoluteLimit:     100 * time.Microsecond,
		EvidencePassed:            true,
	}
	if protocol == referencePairProtocolConfirmation {
		if err := validateOrientationV2Freeze(options.Freeze, options.Discovery, report); err != nil {
			return OrientationSelectorV2Report{}, err
		}
	}
	trainingPassed, holdoutPassed := true, true
	gateOptions := PerfGateOptions{Seed: options.Seed, Confidence: options.Confidence, BootstrapCount: options.BootstrapCount}
	for index, key := range keys {
		current := series[key]
		if err := requireOrientationV2RoundSets(key, current); err != nil {
			return OrientationSelectorV2Report{}, err
		}
		rounds := sortedRounds(current.shadow)
		if len(rounds) < minimumRounds || len(rounds) > maximumRounds {
			return OrientationSelectorV2Report{}, fmt.Errorf("%s/%s requires %d-%d matched orientation-v2 rounds, got %d", key.dataset, key.name, minimumRounds, maximumRounds, len(rounds))
		}
		for _, round := range rounds {
			if len(current.shadow[round]) < minimumSamples || len(current.incumbent[round]) < minimumSamples ||
				len(current.reverse[round]) < minimumSamples || len(current.guarded[round]) < minimumSamples {
				return OrientationSelectorV2Report{}, fmt.Errorf("%s/%s round %d requires %d samples per orientation-v2 arm", key.dataset, key.name, round, minimumSamples)
			}
		}
		if err := validateOrientationV2ArmOrder(shadowRecords, incumbentRecords, reverseRecords, guardedRecords, key, rounds, minimumWarmups); err != nil {
			return OrientationSelectorV2Report{}, err
		}

		split, err := qualificationSplit(key, shadowRecords, incumbentRecords, reverseRecords, guardedRecords)
		if err != nil {
			return OrientationSelectorV2Report{}, err
		}
		role, tuningEligible, qualificationEligible := orientationQualificationRole(split, protocol)
		if qualificationEligible && !strings.HasPrefix(key.dataset, "generated_fixed_suffix_expansion_v3_") {
			return OrientationSelectorV2Report{}, fmt.Errorf("%s/%s qualification evidence is not from the frozen fixed-suffix v3 corpus", key.dataset, key.name)
		}
		fastestIdentity, fastest := fastestOrientationExactArm(current.incumbent, current.reverse)
		selectedIdentity, selected := string(optimize.ExpansionSearchStepwiseForward), current.incumbent
		if current.wouldSelect == string(optimize.ExpansionSearchSuffixSeededReverse) {
			selectedIdentity, selected = string(optimize.ExpansionSearchSuffixSeededReverse), current.reverse
		}
		seed := options.Seed + int64(index)*7919
		_, selectorFloorAbsolute, err := aaTimingFloor(aa, key, false, 0)
		if err != nil {
			return OrientationSelectorV2Report{}, err
		}
		shadowGate := orientationLatencyGate(
			string(optimize.ExpansionSearchStepwiseForward),
			string(optimize.ExpansionSearchPolicyOrientationProbeV2)+":shadow",
			current.incumbent,
			current.shadow,
			report.ShadowForwardRatioLimit,
			report.OverheadAbsoluteLimit,
			seed,
			gateOptions,
		)
		shadowApplicable := current.wouldSelect == string(optimize.ExpansionSearchStepwiseForward)
		guardedSelected := orientationLatencyGate(
			selectedIdentity,
			string(optimize.ExpansionSearchPolicyOrientationProbeV2)+":"+current.guardedRuntime,
			selected,
			current.guarded,
			report.GuardedSelectedRatioLimit,
			report.OverheadAbsoluteLimit,
			seed+3,
			gateOptions,
		)
		guardedFastest := orientationLatencyGate(
			fastestIdentity,
			string(optimize.ExpansionSearchPolicyOrientationProbeV2)+":"+current.guardedRuntime,
			fastest,
			current.guarded,
			report.GuardedFastestRatioLimit,
			selectorFloorAbsolute,
			seed+6,
			gateOptions,
		)
		entry := OrientationSelectorV2Case{
			Dataset:                  key.dataset,
			Name:                     key.name,
			QualificationSplit:       split,
			QualificationRole:        role,
			ThresholdTuningEligible:  tuningEligible,
			QualificationEligible:    qualificationEligible,
			Rounds:                   len(rounds),
			WouldSelectIdentity:      current.wouldSelect,
			FastestExactIdentity:     fastestIdentity,
			GuardedRuntimeIdentity:   current.guardedRuntime,
			GuardedRuntimeBranch:     current.guardedBranch,
			Overflow:                 current.overflow,
			FallbackExecuted:         current.fallback,
			ExactObservationsMatched: true,
			ShadowForwardOverhead: OrientationLatencyGateV2{
				Applicable: shadowApplicable, OrientationLatencyGate: shadowGate,
			},
			GuardedSelectedOverhead: guardedSelected,
			GuardedFastestRegret:    guardedFastest,
			Passed:                  (!shadowApplicable || shadowGate.Passed) && guardedSelected.Passed && guardedFastest.Passed,
		}
		if shadowApplicable && !shadowGate.Passed {
			entry.Reasons = append(entry.Reasons, "forward-selected shadow overhead exceeds 10% and 100us")
		}
		if !guardedSelected.Passed {
			entry.Reasons = append(entry.Reasons, "guarded selected-arm overhead exceeds 10% and 100us")
		}
		if !guardedFastest.Passed {
			entry.Reasons = append(entry.Reasons, "guarded fastest-arm regret exceeds the 1.10/A/A floor")
		}
		if !entry.Passed {
			report.EvidencePassed = false
		}
		if qualificationEligible {
			switch split {
			case "training":
				report.TrainingCases++
				trainingPassed = trainingPassed && entry.Passed
			case "holdout":
				report.HoldoutCases++
				holdoutPassed = holdoutPassed && entry.Passed
			}
		}
		report.Cases = append(report.Cases, entry)
	}
	report.TrainingPassed = protocol == referencePairProtocolConfirmation && report.TrainingCases > 0 && trainingPassed
	report.HoldoutPassed = protocol == referencePairProtocolConfirmation && report.HoldoutCases > 0 && holdoutPassed
	if protocol == referencePairProtocolConfirmation && (report.TrainingCases != 8 || report.HoldoutCases != 4) {
		return OrientationSelectorV2Report{}, fmt.Errorf("orientation-v2 confirmation requires exactly 8 training and 4 holdout cases, got %d/%d", report.TrainingCases, report.HoldoutCases)
	}
	report.QualificationPassed = report.TrainingPassed && report.HoldoutPassed
	return report, nil
}

func collectOrientationSelectorV2Series(
	shadowRecords, incumbentRecords, reverseRecords, guardedRecords []CaseResult,
) (map[performanceKey]*orientationSelectorV2Series, []performanceKey, error) {
	artifacts := []struct {
		name    string
		records []CaseResult
	}{
		{name: "shadow", records: shadowRecords},
		{name: "incumbent", records: incumbentRecords},
		{name: "reverse", records: reverseRecords},
		{name: "guarded", records: guardedRecords},
	}
	keySets := make([]map[performanceKey]struct{}, len(artifacts))
	for index, artifact := range artifacts {
		keys, err := orientationV2ArtifactKeys(artifact.name, artifact.records)
		if err != nil {
			return nil, nil, err
		}
		keySets[index] = keys
	}
	for index := 1; index < len(keySets); index++ {
		if !orientationV2KeySetsEqual(keySets[0], keySets[index]) {
			return nil, nil, fmt.Errorf("orientation-v2 %s artifact case set does not match shadow artifact", artifacts[index].name)
		}
	}

	series := make(map[performanceKey]*orientationSelectorV2Series, len(keySets[0]))
	for key := range keySets[0] {
		series[key] = &orientationSelectorV2Series{
			shadow: roundSamples{}, incumbent: roundSamples{}, reverse: roundSamples{}, guarded: roundSamples{},
		}
	}
	for _, artifact := range artifacts {
		seenRounds := map[performanceKey]map[int]struct{}{}
		for _, record := range artifact.records {
			key := performanceKey{dataset: record.Dataset, name: record.Name, backend: record.ExecutionMode}
			current := series[key]
			if current == nil {
				return nil, nil, fmt.Errorf("orientation-v2 %s artifact contains unexpected case %s/%s", artifact.name, key.dataset, key.name)
			}
			if err := validateOrientationV2Record(record, artifact.name); err != nil {
				return nil, nil, err
			}
			round, err := orientationV2RecordRound(record)
			if err != nil {
				return nil, nil, err
			}
			if seenRounds[key] == nil {
				seenRounds[key] = map[int]struct{}{}
			}
			if _, duplicate := seenRounds[key][round]; duplicate {
				return nil, nil, fmt.Errorf("%s/%s %s artifact duplicates round %d", key.dataset, key.name, artifact.name, round)
			}
			seenRounds[key][round] = struct{}{}
			switch artifact.name {
			case "shadow":
				choice := record.TraversalTelemetry.Summary.WouldSelectIdentity
				shadowOverflow := *record.TraversalTelemetry.Summary.Overflow
				if current.shadowObserved && (current.wouldSelect != choice || current.shadowOverflow != shadowOverflow) {
					return nil, nil, fmt.Errorf("%s/%s changes shadow would_select identity across rounds", key.dataset, key.name)
				}
				current.wouldSelect, current.shadowOverflow, current.shadowObserved = choice, shadowOverflow, true
				appendOrientationWarmSamples(current.shadow, record)
			case "incumbent":
				appendOrientationWarmSamples(current.incumbent, record)
			case "reverse":
				appendOrientationWarmSamples(current.reverse, record)
			case "guarded":
				summary := record.TraversalTelemetry.Summary
				if current.guardedObserved &&
					(current.guardedRuntime != summary.RuntimeIdentity || current.guardedBranch != summary.RuntimeBranch ||
						current.overflow != *summary.Overflow || current.fallback != *summary.FallbackExecuted) {
					return nil, nil, fmt.Errorf("%s/%s changes guarded runtime outcome across rounds", key.dataset, key.name)
				}
				current.guardedRuntime, current.guardedBranch = summary.RuntimeIdentity, summary.RuntimeBranch
				current.overflow, current.fallback, current.guardedObserved = *summary.Overflow, *summary.FallbackExecuted, true
				appendOrientationWarmSamples(current.guarded, record)
			}
		}
	}

	keys := sortedPerformanceKeys(keySets[0])
	for _, key := range keys {
		current := series[key]
		if !current.shadowObserved || current.wouldSelect == "" || !current.guardedObserved {
			return nil, nil, fmt.Errorf("%s/%s lacks attributable shadow or guarded records", key.dataset, key.name)
		}
		if err := validateOrientationV2RuntimeConsistency(key, current); err != nil {
			return nil, nil, err
		}
		if err := validateOrientationExactObservations(key, shadowRecords, incumbentRecords, reverseRecords, guardedRecords); err != nil {
			return nil, nil, err
		}
	}
	return series, keys, nil
}

func orientationV2ArtifactKeys(name string, records []CaseResult) (map[performanceKey]struct{}, error) {
	keys := map[performanceKey]struct{}{}
	if len(records) == 0 {
		return nil, fmt.Errorf("orientation-v2 %s artifact is empty", name)
	}
	for _, record := range records {
		if record.ExecutionMode != ModePostgresSQL {
			return nil, fmt.Errorf("orientation-v2 %s artifact contains non-PostgreSQL record %s/%s", name, record.Dataset, record.Name)
		}
		if record.Dataset == "" || record.Name == "" || !hasWarmLatencySample(record) {
			return nil, fmt.Errorf("orientation-v2 %s artifact contains an incomplete timing record", name)
		}
		keys[performanceKey{dataset: record.Dataset, name: record.Name, backend: record.ExecutionMode}] = struct{}{}
	}
	return keys, nil
}

func orientationV2KeySetsEqual(left, right map[performanceKey]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for key := range left {
		if _, found := right[key]; !found {
			return false
		}
	}
	return true
}

func validateOrientationV2Cohort(
	keys []performanceKey,
	shadowRecords, incumbentRecords, reverseRecords, guardedRecords []CaseResult,
	protocol string,
) (string, error) {
	cohortDeclarationSHA256 := ""
	for name, records := range map[string][]CaseResult{
		"shadow": shadowRecords, "incumbent": incumbentRecords, "reverse": reverseRecords, "guarded": guardedRecords,
	} {
		selection, err := selectionIdentity(records)
		if err != nil {
			return "", fmt.Errorf("orientation-v2 %s selection: %w", name, err)
		}
		if cohortDeclarationSHA256 == "" {
			cohortDeclarationSHA256 = selection.DeclarationSHA256
		}
		if selection.Version != selectionManifestVersion || !lowercaseSHA256(selection.DeclarationSHA256) ||
			selection.DeclarationSHA256 != cohortDeclarationSHA256 || !selection.DiagnosticOnly ||
			selection.SelectedDeclarationCount != 2*len(keys) || len(selection.Resolved) != len(keys) ||
			selection.FullDeclarationCount != selection.SelectedDeclarationCount+selection.OmittedDeclarationCount {
			return "", fmt.Errorf("orientation-v2 %s selection does not bind the exact measured cohort", name)
		}
		resolved := make(map[performanceKey]struct{}, len(selection.Resolved))
		for _, item := range selection.Resolved {
			if item.Category != "generated_fixed_suffix_expansion" {
				return "", fmt.Errorf("orientation-v2 %s selection contains a non-v3 category", name)
			}
			resolved[performanceKey{dataset: item.Dataset, name: item.Name, backend: ModePostgresSQL}] = struct{}{}
		}
		for _, key := range keys {
			if _, found := resolved[key]; !found {
				return "", fmt.Errorf("orientation-v2 %s selection omits %s/%s", name, key.dataset, key.name)
			}
		}
	}

	if protocol == referencePairProtocolConfirmation {
		canonical, err := canonicalOrientationV2Cohort()
		if err != nil {
			return "", err
		}
		if cohortDeclarationSHA256 != canonical.declarationSHA256 || !orientationV2KeySetsEqual(canonical.keys, performanceKeySet(keys)) {
			return "", fmt.Errorf("orientation-v2 confirmation does not contain the exact frozen 8-training/4-holdout cohort")
		}
	}
	return cohortDeclarationSHA256, nil
}

type orientationV2CanonicalCohort struct {
	keys                      map[performanceKey]struct{}
	trainingKeys              map[performanceKey]struct{}
	declarationSHA256         string
	trainingDeclarationSHA256 string
}

var orientationV2CanonicalCases = []struct {
	dataset string
	name    string
	split   string
}{
	{"generated_fixed_suffix_expansion_v3_d2_f4_r0_x2_i0_m1_q1_z1_c0_s0_p0", "GFSE-V3-TRAIN-Q1-C0-S0-root_baseline", "training"},
	{"generated_fixed_suffix_expansion_v3_d2_f4_r0_x2_i0_m1_q4_z1_c0_s0_p0", "GFSE-V3-TRAIN-Q4-C0-S0-root_multiplicity", "training"},
	{"generated_fixed_suffix_expansion_v3_d2_f4_r0_x2_i0_m1_q4_z1_c1_s0_p0", "GFSE-V3-TRAIN-Q4-C1-S0-productive_cycle", "training"},
	{"generated_fixed_suffix_expansion_v3_d2_f4_r0_x2_i0_m1_q4_z1_c0_s1_p0", "GFSE-V3-TRAIN-Q4-C0-S1-productive_self_loop", "training"},
	{"generated_fixed_suffix_expansion_v3_d2_f4_r0_x2_i0_m1_q4_z1_c1_s1_p0", "GFSE-V3-TRAIN-Q4-C1-S1-productive_cycle_self_loop_path", "training"},
	{"generated_fixed_suffix_expansion_v3_d3_f6_r1_x0_i4_m2_q2_z0_c0_s0_p32", "GFSE-V3-TRAIN-D03-F006-R1-X0-I4-M2-Q2-endpoint", "training"},
	{"generated_fixed_suffix_expansion_v3_d5_f8_r4_x3_i0_m1_q3_z0_c0_s0_p0", "GFSE-V3-TRAIN-D05-F008-R4-X3-I0-M1-Q3-path", "training"},
	{"generated_fixed_suffix_expansion_v3_d6_f10_r10_x1_i7_m3_q1_z0_c0_s0_p64", "GFSE-V3-TRAIN-D06-F010-R10-X1-I7-M3-Q1-endpoint", "training"},
	{"generated_fixed_suffix_expansion_v3_d7_f5_r1_x3_i6_m2_q6_z0_c1_s1_p24", "GFSE-V3-HOLDOUT-D07-F005-R1-X3-I6-M2-Q6-C1-S1-path", "holdout"},
	{"generated_fixed_suffix_expansion_v3_d11_f7_r0_x4_i0_m3_q2_z1_c1_s0_p96", "GFSE-V3-HOLDOUT-D11-F007-R0-X4-I0-M3-Q2-C1-S0-endpoint", "holdout"},
	{"generated_fixed_suffix_expansion_v3_d13_f9_r4_x1_i2_m1_q7_z0_c0_s1_p8", "GFSE-V3-HOLDOUT-D13-F009-R4-X1-I2-M1-Q7-C0-S1-path", "holdout"},
	{"generated_fixed_suffix_expansion_v3_d15_f12_r6_x6_i9_m2_q3_z1_c0_s0_p128", "GFSE-V3-HOLDOUT-D15-F012-R6-X6-I9-M2-Q3-Z1-endpoint", "holdout"},
}

func canonicalOrientationV2Cohort() (orientationV2CanonicalCohort, error) {
	keys := map[performanceKey]struct{}{}
	trainingKeys := map[performanceKey]struct{}{}
	declared := make([]DeclaredCaseBackend, 0, 24)
	trainingDeclared := make([]DeclaredCaseBackend, 0, 16)
	training, holdout := 0, 0
	for _, testCase := range orientationV2CanonicalCases {
		key := performanceKey{dataset: testCase.dataset, name: testCase.name, backend: ModePostgresSQL}
		if _, duplicate := keys[key]; duplicate || !strings.HasPrefix(testCase.dataset, "generated_fixed_suffix_expansion_v3_") {
			return orientationV2CanonicalCohort{}, fmt.Errorf("frozen orientation-v2 cohort contains an invalid declaration")
		}
		keys[key] = struct{}{}
		for _, backend := range []ExecutionMode{ModePostgresSQL, ModeNeo4j} {
			declared = append(declared, DeclaredCaseBackend{Dataset: key.dataset, Name: key.name, Backend: backend})
		}
		if testCase.split == "training" {
			training++
			trainingKeys[key] = struct{}{}
			for _, backend := range []ExecutionMode{ModePostgresSQL, ModeNeo4j} {
				trainingDeclared = append(trainingDeclared, DeclaredCaseBackend{Dataset: key.dataset, Name: key.name, Backend: backend})
			}
		} else if testCase.split == "holdout" {
			holdout++
		} else {
			return orientationV2CanonicalCohort{}, fmt.Errorf("frozen orientation-v2 cohort contains an invalid split")
		}
	}
	if training != 8 || holdout != 4 || len(keys) != 12 {
		return orientationV2CanonicalCohort{}, fmt.Errorf("frozen orientation-v2 cohort must contain exactly 8 training and 4 holdout cases")
	}
	return orientationV2CanonicalCohort{
		keys: keys, trainingKeys: trainingKeys, declarationSHA256: declarationSHA256(declared),
		trainingDeclarationSHA256: declarationSHA256(trainingDeclared),
	}, nil
}

func performanceKeySet(keys []performanceKey) map[performanceKey]struct{} {
	result := make(map[performanceKey]struct{}, len(keys))
	for _, key := range keys {
		result[key] = struct{}{}
	}
	return result
}

func validateOrientationV2Freeze(freeze *OrientationSelectorV2FreezeManifest, discovery *OrientationSelectorV2Report, report OrientationSelectorV2Report) error {
	if freeze == nil || discovery == nil {
		return fmt.Errorf("orientation-v2 confirmation requires a discovery report and freeze manifest")
	}
	if freeze.Version != 1 || freeze.Policy != report.Policy || freeze.Formula != report.Formula ||
		freeze.SourceCommit != report.SourceCommit || freeze.DirtyDiffSHA256 != report.DirtyDiffSHA256 ||
		freeze.BinarySHA256 != report.BinarySHA256 || freeze.CohortDeclarationSHA256 != report.CohortDeclarationSHA256 ||
		!lowercaseSHA256(freeze.DiscoveryReportSHA256) || len(freeze.Caps) != len(report.Caps) {
		return fmt.Errorf("orientation-v2 confirmation identity differs from the frozen discovery")
	}
	if report.DirtyDiffSHA256 != cleanWorkingTreeSHA256() || discovery.Version != orientationSelectorReportV2Version ||
		discovery.Protocol != referencePairProtocolDiscovery || discovery.Policy != freeze.Policy || discovery.Formula != freeze.Formula ||
		discovery.SourceCommit != freeze.SourceCommit || discovery.DirtyDiffSHA256 != freeze.DirtyDiffSHA256 ||
		discovery.BinarySHA256 != freeze.BinarySHA256 || len(discovery.Cases) != 8 ||
		!lowercaseSHA256(discovery.ShadowArtifactSHA256) || !lowercaseSHA256(discovery.IncumbentArtifactSHA256) ||
		!lowercaseSHA256(discovery.ReverseArtifactSHA256) || !lowercaseSHA256(discovery.GuardedArtifactSHA256) ||
		!lowercaseSHA256(discovery.AAReportSHA256) {
		return fmt.Errorf("orientation-v2 discovery report does not prove the frozen clean training-only identity")
	}
	canonical, err := canonicalOrientationV2Cohort()
	if err != nil {
		return err
	}
	discoveryKeys := map[performanceKey]struct{}{}
	for _, entry := range discovery.Cases {
		if entry.QualificationSplit != "training" {
			return fmt.Errorf("orientation-v2 discovery report contains non-training timing")
		}
		discoveryKeys[performanceKey{dataset: entry.Dataset, name: entry.Name, backend: ModePostgresSQL}] = struct{}{}
	}
	if !orientationV2KeySetsEqual(discoveryKeys, canonical.trainingKeys) {
		return fmt.Errorf("orientation-v2 discovery report does not contain the exact frozen training cohort")
	}
	if discovery.CohortDeclarationSHA256 != canonical.trainingDeclarationSHA256 {
		return fmt.Errorf("orientation-v2 discovery report does not bind the exact frozen training declaration")
	}
	for name, value := range report.Caps {
		if freeze.Caps[name] != value || discovery.Caps[name] != value {
			return fmt.Errorf("orientation-v2 confirmation cap %s differs from the frozen discovery", name)
		}
	}
	return nil
}

func orientationV2RecordRound(record CaseResult) (int, error) {
	round := 0
	if record.Environment != nil {
		round = record.Environment.Round
	}
	for _, sample := range record.Stats.Samples {
		if sample.Classification != "warm" || sample.Duration <= 0 {
			continue
		}
		current := sample.Round
		if current == 0 {
			current = round
		}
		if current < 1 || (round != 0 && current != round) {
			return 0, fmt.Errorf("%s/%s has inconsistent orientation-v2 round metadata", record.Dataset, record.Name)
		}
		round = current
	}
	if round < 1 {
		return 0, fmt.Errorf("%s/%s has no orientation-v2 round identity", record.Dataset, record.Name)
	}
	return round, nil
}

func validateOrientationV2Record(record CaseResult, arm string) error {
	if record.Status != StatusOK || record.Environment == nil || record.PostgresEnvironment == nil || record.TraversalTelemetry == nil {
		return fmt.Errorf("%s/%s %s arm lacks a successful telemetry-bearing PostgreSQL record", record.Dataset, record.Name, arm)
	}
	if record.Environment.ArtifactSchemaVersion != 2 || record.Environment.PoolSize != 1 || len(record.Environment.Concurrency) != 0 {
		return fmt.Errorf("%s/%s %s arm lacks the schema-v2 single-session timing contract", record.Dataset, record.Name, arm)
	}
	if record.Environment.ExistingGraph || record.Fixture == nil || record.Fixture.Dataset != record.Dataset ||
		!lowercaseSHA256(record.Fixture.Checksum) || !record.Fixture.PhysicalValidated {
		return fmt.Errorf("%s/%s %s arm lacks one exact physically validated corpus fixture", record.Dataset, record.Name, arm)
	}
	if !lowercaseSHA256(record.WorkloadSHA256) || !lowercaseSHA256(record.SQLFingerprint) {
		return fmt.Errorf("%s/%s %s arm lacks canonical workload or SQL identity", record.Dataset, record.Name, arm)
	}
	if len(record.Concurrency) != 0 || len(record.PostgresReferences) != 0 || record.ClientWaterfall != nil ||
		record.RawPGXWaterfall != nil || record.RawPGXRoundTrip != nil {
		return fmt.Errorf("%s/%s %s arm mixes selector timing with supplemental PostgreSQL measurements", record.Dataset, record.Name, arm)
	}
	if !strings.EqualFold(strings.TrimSpace(record.PostgresEnvironment.TransactionIsolation), "repeatable read") {
		return fmt.Errorf("%s/%s %s arm was not measured under Repeatable Read", record.Dataset, record.Name, arm)
	}
	if err := record.TraversalTelemetry.Validate(); err != nil {
		return fmt.Errorf("%s/%s %s arm telemetry: %w", record.Dataset, record.Name, arm, err)
	}
	summary := record.TraversalTelemetry.Summary
	if summary.RuntimeOutcomeAvailable == nil || !*summary.RuntimeOutcomeAvailable || summary.Overflow == nil || summary.FallbackExecuted == nil {
		return fmt.Errorf("%s/%s %s arm lacks a complete runtime outcome", record.Dataset, record.Name, arm)
	}
	forward := string(optimize.ExpansionSearchStepwiseForward)
	reverse := string(optimize.ExpansionSearchSuffixSeededReverse)
	v2 := string(optimize.ExpansionSearchPolicyOrientationProbeV2)
	switch arm {
	case "shadow":
		if summary.EmittedIdentity != v2 || summary.SelectorVersion != v2 ||
			summary.ExecutionBoundary != optimize.ExpansionSearchExecutionBoundaryInlineStatement ||
			summary.RuntimeIdentity != forward || summary.AppliedIdentity != forward || summary.RuntimeBranch != "shadow_incumbent" ||
			(summary.WouldSelectIdentity != forward && summary.WouldSelectIdentity != reverse) || *summary.FallbackExecuted {
			return fmt.Errorf("%s/%s shadow telemetry does not prove orientation-probe-v2 incumbent-only execution", record.Dataset, record.Name)
		}
		if *summary.Overflow && summary.WouldSelectIdentity != forward {
			return fmt.Errorf("%s/%s overflowing shadow evidence did not fail closed to forward", record.Dataset, record.Name)
		}
	case "incumbent":
		if summary.EmittedIdentity != forward || summary.RuntimeIdentity != forward || summary.AppliedIdentity != forward ||
			summary.ExecutionBoundary != optimize.ExpansionSearchExecutionBoundaryInlineStatement || summary.WouldSelectIdentity != "" || *summary.Overflow {
			return fmt.Errorf("%s/%s incumbent artifact did not execute one exact forward statement", record.Dataset, record.Name)
		}
		validSelected := summary.RuntimeBranch == "selected" && !*summary.FallbackExecuted
		validCompileFallback := summary.RuntimeBranch == "compile_time_fallback" && *summary.FallbackExecuted && summary.FallbackIdentity == forward
		if !validSelected && !validCompileFallback {
			return fmt.Errorf("%s/%s incumbent artifact has an unsupported exact-arm runtime tuple", record.Dataset, record.Name)
		}
		if summary.SelectorVersion != "fixed-suffix-static-v1" {
			return fmt.Errorf("%s/%s incumbent artifact has an unexpected selector identity", record.Dataset, record.Name)
		}
	case "reverse":
		if summary.EmittedIdentity != reverse || summary.RuntimeIdentity != reverse || summary.AppliedIdentity != reverse ||
			summary.ExecutionBoundary != optimize.ExpansionSearchExecutionBoundaryInlineStatement || summary.WouldSelectIdentity != "" ||
			summary.RuntimeBranch != "selected" || *summary.Overflow || *summary.FallbackExecuted {
			return fmt.Errorf("%s/%s reverse artifact did not execute one exact forced-reverse statement", record.Dataset, record.Name)
		}
		if summary.SelectorVersion != "suffix-seeded-reverse-tool-v1" {
			return fmt.Errorf("%s/%s reverse artifact has an unexpected selector identity", record.Dataset, record.Name)
		}
	case "guarded":
		if summary.EmittedIdentity != v2 || summary.SelectorVersion != v2 ||
			summary.ExecutionBoundary != optimize.ExpansionSearchExecutionBoundaryGuardedDualArm || summary.WouldSelectIdentity != "" {
			return fmt.Errorf("%s/%s guarded artifact does not prove the orientation-probe-v2 dual-arm boundary", record.Dataset, record.Name)
		}
	default:
		return fmt.Errorf("unknown orientation-v2 arm %q", arm)
	}
	if err := validateOrientationV2SampleRuntime(record, arm); err != nil {
		return err
	}
	return nil
}

func validateOrientationV2SampleRuntime(record CaseResult, arm string) error {
	summary := record.TraversalTelemetry.Summary
	for _, sample := range record.Stats.Samples {
		if sample.Classification != "warm" || sample.Duration <= 0 {
			continue
		}
		if sample.RequestedIdentity != summary.RequestedIdentity || sample.RuntimeIdentity != summary.RuntimeIdentity ||
			sample.RuntimeBranch != summary.RuntimeBranch || sample.FallbackExecuted == nil ||
			*sample.FallbackExecuted != *summary.FallbackExecuted {
			return fmt.Errorf("%s/%s %s arm warm sample contradicts its runtime summary", record.Dataset, record.Name, arm)
		}
		switch arm {
		case "shadow", "guarded":
			if sample.RuntimeAttestation != "timed_invocation" {
				return fmt.Errorf("%s/%s %s arm warm sample lacks timed-invocation attribution", record.Dataset, record.Name, arm)
			}
			if err := validateRuntimeReceiptEvents(sample.RuntimeReceiptEvents, sample.RuntimeIdentity, sample.RuntimeBranch, sample.FallbackExecuted); err != nil {
				return fmt.Errorf("%s/%s %s arm warm sample receipt: %w", record.Dataset, record.Name, arm, err)
			}
		case "incumbent", "reverse":
			if sample.RuntimeAttestation != "same_case_invocation_local_replay" && sample.RuntimeAttestation != "timed_invocation" {
				return fmt.Errorf("%s/%s %s exact arm warm sample lacks runtime attribution", record.Dataset, record.Name, arm)
			}
			if sample.RuntimeAttestation == "timed_invocation" {
				if err := validateRuntimeReceiptEvents(sample.RuntimeReceiptEvents, sample.RuntimeIdentity, sample.RuntimeBranch, sample.FallbackExecuted); err != nil {
					return fmt.Errorf("%s/%s %s exact arm warm sample receipt: %w", record.Dataset, record.Name, arm, err)
				}
			} else if len(sample.RuntimeReceiptEvents) != 0 {
				return fmt.Errorf("%s/%s %s exact arm replay must not claim a timed receipt", record.Dataset, record.Name, arm)
			}
		}
	}
	return nil
}

func validateOrientationV2RuntimeConsistency(key performanceKey, current *orientationSelectorV2Series) error {
	forward := string(optimize.ExpansionSearchStepwiseForward)
	reverse := string(optimize.ExpansionSearchSuffixSeededReverse)
	if current.shadowOverflow && !current.overflow {
		return fmt.Errorf("%s/%s guarded evidence lost shadow probe overflow", key.dataset, key.name)
	}
	if current.overflow {
		choiceConsistent := current.shadowOverflow && current.wouldSelect == forward || !current.shadowOverflow && current.wouldSelect == reverse
		if !choiceConsistent || current.guardedRuntime != forward || current.guardedBranch != "exact_forward_incumbent" || !current.fallback {
			return fmt.Errorf("%s/%s guarded overflow did not execute the exact forward fallback", key.dataset, key.name)
		}
		return nil
	}
	if current.fallback {
		return fmt.Errorf("%s/%s guarded artifact reports fallback without overflow", key.dataset, key.name)
	}
	if current.wouldSelect == reverse {
		if current.guardedRuntime != reverse || current.guardedBranch != "suffix_seeded_reverse" {
			return fmt.Errorf("%s/%s guarded runtime does not match the shadow reverse choice", key.dataset, key.name)
		}
		return nil
	}
	if current.wouldSelect == forward && current.guardedRuntime == forward && current.guardedBranch == "exact_forward_incumbent" {
		return nil
	}
	return fmt.Errorf("%s/%s guarded runtime does not match the shadow forward choice", key.dataset, key.name)
}

func requireOrientationV2RoundSets(key performanceKey, current *orientationSelectorV2Series) error {
	expected := sortedRounds(current.shadow)
	for name, rounds := range map[string][]int{
		"incumbent": sortedRounds(current.incumbent),
		"reverse":   sortedRounds(current.reverse),
		"guarded":   sortedRounds(current.guarded),
	} {
		if !slices.Equal(expected, rounds) {
			return fmt.Errorf("%s/%s %s arm round set does not match shadow", key.dataset, key.name, name)
		}
	}
	return nil
}

func validateOrientationV2ArmOrder(
	shadowRecords, incumbentRecords, reverseRecords, guardedRecords []CaseResult,
	key performanceKey,
	rounds []int,
	minimumWarmups int,
) error {
	armRecords := []struct {
		name    string
		records []CaseResult
	}{
		{name: "shadow", records: shadowRecords},
		{name: "incumbent", records: incumbentRecords},
		{name: "reverse", records: reverseRecords},
		{name: "guarded", records: guardedRecords},
	}
	evidence := make([]map[int]pairedRoundEvidence, len(armRecords))
	positionCounts := make([][5]int, len(armRecords))
	for index, arm := range armRecords {
		current, err := collectPairedRoundEvidence(arm.records, key)
		if err != nil {
			return err
		}
		evidence[index] = current
	}
	for _, round := range rounds {
		seenPositions := map[int]struct{}{}
		seenNames := map[string]struct{}{}
		block, runUUID := 0, ""
		for index, arm := range armRecords {
			current, found := evidence[index][round]
			if !found || current.Warmups < minimumWarmups || current.Arm != arm.name {
				return fmt.Errorf("%s/%s round %d lacks %s arm identity or %d warmups", key.dataset, key.name, round, arm.name, minimumWarmups)
			}
			if current.ArmOrder < 1 || current.ArmOrder > 4 {
				return fmt.Errorf("%s/%s round %d has invalid four-arm order", key.dataset, key.name, round)
			}
			if _, duplicate := seenPositions[current.ArmOrder]; duplicate {
				return fmt.Errorf("%s/%s round %d has duplicate four-arm order", key.dataset, key.name, round)
			}
			if _, duplicate := seenNames[current.Arm]; duplicate {
				return fmt.Errorf("%s/%s round %d has indistinct four-arm labels", key.dataset, key.name, round)
			}
			seenPositions[current.ArmOrder] = struct{}{}
			seenNames[current.Arm] = struct{}{}
			positionCounts[index][current.ArmOrder]++
			if block == 0 {
				block, runUUID = current.Block, current.RunUUID
			} else if current.Block != block || current.RunUUID != runUUID {
				return fmt.Errorf("%s/%s round %d has mismatched four-arm block or run UUID", key.dataset, key.name, round)
			}
		}
		if block < 1 || runUUID == "" || len(seenPositions) != 4 || len(seenNames) != 4 {
			return fmt.Errorf("%s/%s round %d lacks a complete four-arm block", key.dataset, key.name, round)
		}
	}
	for index, counts := range positionCounts {
		minimum, maximum := counts[1], counts[1]
		for position := 2; position <= 4; position++ {
			minimum = min(minimum, counts[position])
			maximum = max(maximum, counts[position])
		}
		if maximum-minimum > 1 {
			return fmt.Errorf("%s/%s %s arm order is not position-balanced", key.dataset, key.name, armRecords[index].name)
		}
	}
	return nil
}

func validateOrientationV2EvidenceIdentity(artifacts ...[]CaseResult) (orientationSelectorV2Identity, error) {
	identity := orientationSelectorV2Identity{}
	var postgresEnvironment *PostgresEnvironment
	allRecords := make([]CaseResult, 0)
	for _, records := range artifacts {
		allRecords = append(allRecords, records...)
		for _, record := range records {
			if record.Environment == nil || record.PostgresEnvironment == nil {
				return orientationSelectorV2Identity{}, fmt.Errorf("%s/%s lacks orientation-v2 environment identity", record.Dataset, record.Name)
			}
			current := orientationSelectorV2Identity{
				sourceCommit: strings.TrimSpace(record.Environment.SourceCommit), dirtyDiffSHA256: record.Environment.DirtyDiffSHA256,
				binarySHA256: record.Environment.BinarySHA256, corpusSHA256: record.Environment.CorpusSHA256,
			}
			if current.sourceCommit == "" || current.sourceCommit == "unknown" ||
				!lowercaseSHA256(current.dirtyDiffSHA256) || !lowercaseSHA256(current.binarySHA256) || !lowercaseSHA256(current.corpusSHA256) {
				return orientationSelectorV2Identity{}, fmt.Errorf("%s/%s lacks frozen source, diff, binary, or corpus identity", record.Dataset, record.Name)
			}
			if identity.sourceCommit == "" {
				identity = current
			} else if identity != current {
				return orientationSelectorV2Identity{}, fmt.Errorf("orientation-v2 artifacts mix source, diff, binary, or corpus identities")
			}
			if postgresEnvironment == nil {
				copy := *record.PostgresEnvironment
				postgresEnvironment = &copy
			} else if !sameOrientationV2PostgresEnvironment(postgresEnvironment, record.PostgresEnvironment) {
				return orientationSelectorV2Identity{}, fmt.Errorf("orientation-v2 artifacts mix PostgreSQL environments")
			}
		}
	}
	keys, err := orientationV2ArtifactKeys("combined", allRecords)
	if err != nil {
		return orientationSelectorV2Identity{}, err
	}
	for key := range keys {
		postgresEnvironmentSHA256, err := postgresTimingEnvironmentSHA256ForKey(allRecords, key)
		if err != nil {
			return orientationSelectorV2Identity{}, err
		}
		fixtureSHA256, err := fixtureSHA256ForKey(allRecords, key)
		if err != nil {
			return orientationSelectorV2Identity{}, err
		}
		if !lowercaseSHA256(postgresEnvironmentSHA256) || !lowercaseSHA256(fixtureSHA256) {
			return orientationSelectorV2Identity{}, fmt.Errorf("%s/%s lacks frozen PostgreSQL or fixture identity", key.dataset, key.name)
		}
	}
	return identity, nil
}

func validateOrientationV2AAEvidence(report *AAResolutionReport, records []CaseResult) error {
	keys, err := orientationV2ArtifactKeys("incumbent", records)
	if err != nil {
		return err
	}
	entries := make(map[performanceKey]AAResolutionCase, len(report.Cases))
	for _, entry := range report.Cases {
		entries[performanceKey{dataset: entry.Dataset, name: entry.Name, backend: entry.Backend}] = entry
	}
	for key := range keys {
		entry, found := entries[key]
		if !found {
			return fmt.Errorf("A/A report has no environment evidence for %s/%s", key.dataset, key.name)
		}
		postgresEnvironmentSHA256, err := postgresTimingEnvironmentSHA256ForKey(records, key)
		if err != nil {
			return err
		}
		fixtureSHA256, err := fixtureSHA256ForKey(records, key)
		if err != nil {
			return err
		}
		if !lowercaseSHA256(entry.PostgresEnvironmentSHA256) || entry.PostgresEnvironmentSHA256 != postgresEnvironmentSHA256 {
			return fmt.Errorf("A/A PostgreSQL environment does not match %s/%s", key.dataset, key.name)
		}
		if !lowercaseSHA256(entry.FixtureSHA256) || entry.FixtureSHA256 != fixtureSHA256 {
			return fmt.Errorf("A/A fixture does not match %s/%s", key.dataset, key.name)
		}
	}
	return nil
}

func lowercaseSHA256(value string) bool {
	return value == strings.ToLower(value) && validSHA256(value)
}

func sameOrientationV2PostgresEnvironment(left, right *PostgresEnvironment) bool {
	return left.Version == right.Version && left.Database == right.Database &&
		left.PlanCacheMode == right.PlanCacheMode && left.TransactionIsolation == right.TransactionIsolation &&
		left.WorkMem == right.WorkMem && left.TempFileLimit == right.TempFileLimit &&
		left.GraphPartitionCount == right.GraphPartitionCount &&
		left.DatabaseOID == right.DatabaseOID && left.PostmasterStartedAt.Equal(right.PostmasterStartedAt) &&
		left.Autovacuum == right.Autovacuum &&
		left.SchemaFingerprint == right.SchemaFingerprint && left.IndexFingerprint == right.IndexFingerprint
}

// createOrientationSelectorV2Report loads four matched timing artifacts and
// one checksummed A/A report, then writes schema-v2 qualification evidence.
func createOrientationSelectorV2Report(
	shadowPath, incumbentPath, reversePath, guardedPath, aaPath, freezePath, discoveryReportPath, freezeOutputPath, outputPath string,
	options OrientationSelectorV2ReportOptions,
) (bool, error) {
	paths := []struct {
		name string
		path string
	}{
		{name: "shadow", path: shadowPath},
		{name: "incumbent", path: incumbentPath},
		{name: "reverse", path: reversePath},
		{name: "guarded", path: guardedPath},
	}
	artifacts := make([][]CaseResult, len(paths))
	for index, input := range paths {
		records, err := readJSONLFile(input.path)
		if err != nil {
			return false, fmt.Errorf("read orientation-v2 %s artifact: %w", input.name, err)
		}
		artifacts[index] = records
	}
	aa, aaSHA, err := loadAAResolutionReport(aaPath)
	if err != nil {
		return false, fmt.Errorf("read orientation-v2 A/A report: %w", err)
	}
	freezeSHA := ""
	if freezePath != "" {
		freeze, digest, err := loadOrientationSelectorV2FreezeManifest(freezePath)
		if err != nil {
			return false, fmt.Errorf("read orientation-v2 freeze manifest: %w", err)
		}
		options.Freeze = freeze
		freezeSHA = digest
		discovery, err := loadOrientationSelectorV2Report(discoveryReportPath)
		if err != nil {
			return false, fmt.Errorf("read orientation-v2 discovery report: %w", err)
		}
		if digest, err := fileSHA256(discoveryReportPath); err != nil {
			return false, err
		} else if digest != freeze.DiscoveryReportSHA256 {
			return false, fmt.Errorf("orientation-v2 discovery report digest does not match freeze manifest")
		}
		options.Discovery = discovery
	}
	report, err := buildOrientationSelectorV2Report(artifacts[0], artifacts[1], artifacts[2], artifacts[3], aa, options)
	if err != nil {
		return false, err
	}
	for index, input := range paths {
		digest, err := fileSHA256(input.path)
		if err != nil {
			return false, err
		}
		switch index {
		case 0:
			report.ShadowArtifactSHA256 = digest
		case 1:
			report.IncumbentArtifactSHA256 = digest
		case 2:
			report.ReverseArtifactSHA256 = digest
		case 3:
			report.GuardedArtifactSHA256 = digest
		}
	}
	report.AAReportSHA256 = aaSHA
	report.FreezeManifestSHA256 = freezeSHA
	if err := writeOrientationSelectorV2Report(outputPath, report); err != nil {
		return false, err
	}
	if options.Protocol == referencePairProtocolDiscovery {
		if err := writeOrientationSelectorV2FreezeManifest(freezeOutputPath, outputPath, report, artifacts...); err != nil {
			return false, err
		}
	}
	return report.QualificationPassed, nil
}

func loadOrientationSelectorV2Report(path string) (*OrientationSelectorV2Report, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	report := &OrientationSelectorV2Report{}
	if err := json.Unmarshal(raw, report); err != nil {
		return nil, fmt.Errorf("decode orientation-v2 discovery report: %w", err)
	}
	return report, nil
}

func loadOrientationSelectorV2FreezeManifest(path string) (*OrientationSelectorV2FreezeManifest, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	manifest := &OrientationSelectorV2FreezeManifest{}
	if err := json.Unmarshal(raw, manifest); err != nil {
		return nil, "", fmt.Errorf("decode orientation-v2 freeze manifest: %w", err)
	}
	digest := sha256.Sum256(raw)
	return manifest, hex.EncodeToString(digest[:]), nil
}

func writeOrientationSelectorV2FreezeManifest(path, discoveryReportPath string, report OrientationSelectorV2Report, artifacts ...[]CaseResult) error {
	if path == "" || discoveryReportPath == "" {
		return fmt.Errorf("orientation-v2 discovery freeze requires report and manifest output paths")
	}
	canonical, err := canonicalOrientationV2Cohort()
	if err != nil {
		return err
	}
	training := map[performanceKey]struct{}{}
	for _, record := range artifacts[0] {
		key := performanceKey{dataset: record.Dataset, name: record.Name, backend: record.ExecutionMode}
		if record.Shape.QualificationSplit == "training" {
			training[key] = struct{}{}
		}
	}
	if !orientationV2KeySetsEqual(training, canonical.trainingKeys) || report.CohortDeclarationSHA256 != canonical.trainingDeclarationSHA256 {
		return fmt.Errorf("orientation-v2 discovery freeze requires the exact eight canonical training cases and no holdouts")
	}
	for _, records := range artifacts {
		for _, record := range records {
			if record.Shape.QualificationSplit != "training" {
				return fmt.Errorf("orientation-v2 discovery freeze cannot contain holdout or diagnostic timing")
			}
		}
	}
	if report.DirtyDiffSHA256 != cleanWorkingTreeSHA256() {
		return fmt.Errorf("orientation-v2 discovery freeze requires a clean source tree")
	}
	discoveryReportSHA256, err := fileSHA256(discoveryReportPath)
	if err != nil {
		return err
	}
	manifest := OrientationSelectorV2FreezeManifest{
		Version: 1, Policy: report.Policy, Formula: report.Formula, Caps: report.Caps,
		SourceCommit: report.SourceCommit, DirtyDiffSHA256: report.DirtyDiffSHA256, BinarySHA256: report.BinarySHA256,
		CohortDeclarationSHA256: canonical.declarationSHA256, DiscoveryReportSHA256: discoveryReportSHA256,
	}
	if err := ensureOutputDir(path); err != nil {
		return err
	}
	output, err := os.Create(path)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	encodeErr := encoder.Encode(manifest)
	closeErr := output.Close()
	if encodeErr != nil {
		return encodeErr
	}
	return closeErr
}

func writeOrientationSelectorV2Report(path string, report OrientationSelectorV2Report) (err error) {
	output := os.Stdout
	if path != "" {
		if err := ensureOutputDir(path); err != nil {
			return err
		}
		output, err = os.Create(path)
		if err != nil {
			return err
		}
		defer func() {
			if closeErr := output.Close(); err == nil && closeErr != nil {
				err = closeErr
			}
		}()
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}
