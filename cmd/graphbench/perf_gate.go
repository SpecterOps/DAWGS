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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"time"
)

const (
	// perfGateVersion identifies the serialized schema revision for perf gate.
	perfGateVersion = 5

	// defaultBootstrapCount sets the fallback number of resamples used to estimate confidence bounds.
	defaultBootstrapCount = 10_000

	// minimumGateRounds requires this many independent matched rounds before a workload may pass.
	minimumGateRounds = 5

	// minimumP95Samples requires this many warm samples per arm before the P95 ratio is gated.
	minimumP95Samples = 150

	// minimumDiscoveryWarmups requires the discovery protocol's untimed warmup floor.
	minimumDiscoveryWarmups = 5
)

// PerfGateOptions defines statistical confidence, materiality, targets, and declared backend coverage for gating.
type PerfGateOptions struct {
	// Seed controls deterministic random sampling.
	Seed int64
	// Confidence sets the confidence level used for statistical intervals.
	Confidence float64
	// RegressionThreshold sets the largest median ratio that is not considered a regression.
	RegressionThreshold float64
	// BootstrapCount sets the number of bootstrap resamples.
	BootstrapCount int
	// DeclaredBackends lists case/backend declarations that the performance gate must cover.
	DeclaredBackends []DeclaredCaseBackend
	// TargetNames restricts materiality requirements to the named workloads.
	TargetNames []string
	// MaterialityRatio sets the relative change required before a difference is material.
	MaterialityRatio float64
	// MaterialityAbsolute sets the absolute duration change required before a difference is material.
	MaterialityAbsolute time.Duration
	// DiagnosticMode allows incomplete diagnostic selections that cannot produce a release-gate pass.
	DiagnosticMode bool
	// AAReportPath selects the host A/A evidence loaded by artifact comparison mode.
	AAReportPath string
	// AAReport contains host-specific per-case timing resolution required for promotion.
	AAReport *AAResolutionReport
	// AAReportSHA256 identifies the exact A/A report supplied to the gate.
	AAReportSHA256 string
}

// RatioInterval describes a point estimate and confidence bounds for a latency ratio.
type RatioInterval struct {
	// Estimate supplies the estimate input to the RatioInterval contract.
	Estimate float64 `json:"estimate"`
	// Lower supplies the lower input to the RatioInterval contract.
	Lower float64 `json:"lower"`
	// Upper supplies the upper input to the RatioInterval contract.
	Upper float64 `json:"upper"`
}

// DurationInterval describes a duration estimate and its confidence bounds.
type DurationInterval struct {
	// Estimate supplies the estimate input to the DurationInterval contract.
	Estimate time.Duration `json:"estimate"`
	// Lower supplies the lower input to the DurationInterval contract.
	Lower time.Duration `json:"lower"`
	// Upper supplies the upper input to the DurationInterval contract.
	Upper time.Duration `json:"upper"`
}

// PerfGateCase reports matched sample evidence, bootstrap intervals, and classification for one gated workload.
type PerfGateCase struct {
	// Dataset identifies the fixture dataset.
	Dataset string `json:"dataset"`
	// Name identifies the case or record within its dataset.
	Name string `json:"name"`
	// Backend identifies the execution backend.
	Backend ExecutionMode `json:"backend"`
	// Tier identifies whether timing is gated or stress-diagnostic.
	Tier string `json:"tier"`
	// QualificationSplit identifies training, frozen holdout, or diagnostic evidence.
	QualificationSplit string `json:"qualification_split"`
	// TimingGated reports whether latency evidence contributes to promotion.
	TimingGated bool `json:"timing_gated"`
	// Rounds records the number of rounds.
	Rounds int `json:"rounds"`
	// BaselineSamples records warm timing samples available from the baseline arm.
	BaselineSamples int `json:"baseline_samples"`
	// CandidateSamples records warm timing samples available from the candidate arm.
	CandidateSamples int `json:"candidate_samples"`
	// BaselineStatus supplies the baseline status input to the PerfGateCase contract.
	BaselineStatus string `json:"baseline_status,omitempty"`
	// CandidateStatus supplies the candidate status input to the PerfGateCase contract.
	CandidateStatus string `json:"candidate_status,omitempty"`
	// OracleOnly marks a backend as a correctness oracle excluded from latency regression decisions.
	OracleOnly bool `json:"oracle_only,omitempty"`
	// MedianRatio reports the candidate-to-baseline median latency ratio and confidence bounds.
	MedianRatio RatioInterval `json:"median_ratio"`
	// P95Ratio reports the candidate-to-baseline P95 latency ratio and confidence bounds.
	P95Ratio *RatioInterval `json:"p95_ratio,omitempty"`
	// MedianSaving reports absolute median latency saved by the candidate.
	MedianSaving *DurationInterval `json:"median_saving,omitempty"`
	// MedianChange reports candidate-minus-baseline median latency.
	MedianChange *DurationInterval `json:"median_change,omitempty"`
	// P95Change reports candidate-minus-baseline P95 latency.
	P95Change *DurationInterval `json:"p95_change,omitempty"`
	// P50NoiseRatio supplies the p50 noise ratio input to the PerfGateCase contract.
	P50NoiseRatio float64 `json:"p50_noise_ratio,omitempty"`
	// P50NoiseAbsolute supplies the p50 noise absolute input to the PerfGateCase contract.
	P50NoiseAbsolute time.Duration `json:"p50_noise_absolute,omitempty"`
	// P95NoiseRatio supplies the p95 noise ratio input to the PerfGateCase contract.
	P95NoiseRatio float64 `json:"p95_noise_ratio,omitempty"`
	// P95NoiseAbsolute supplies the p95 noise absolute input to the PerfGateCase contract.
	P95NoiseAbsolute time.Duration `json:"p95_noise_absolute,omitempty"`
	// MaterialityRatio sets the relative change required before a difference is material.
	MaterialityRatio *float64 `json:"materiality_ratio_upper_limit,omitempty"`
	// MaterialityAbsolute sets the absolute duration change required before a difference is material.
	MaterialityAbsolute *time.Duration `json:"materiality_absolute_lower_limit,omitempty"`
	// Passed reports whether every required gate condition succeeded.
	Passed bool `json:"passed"`
	// Reasons lists explanations for the reported disposition.
	Reasons []string `json:"reasons,omitempty"`
	// CandidateRuntimeReceiptChains preserves complete measured candidate
	// branch chains used by the performance decision.
	CandidateRuntimeReceiptChains [][]RuntimeReceiptEvent `json:"candidate_runtime_receipt_chains,omitempty"`
}

// PerfGateReport contains baseline and candidate identities, gate policy, and every workload disposition.
type PerfGateReport struct {
	// Version identifies the serialized schema revision.
	Version int `json:"version"`
	// Seed controls deterministic random sampling.
	Seed int64 `json:"seed"`
	// Confidence sets the confidence level used for statistical intervals.
	Confidence float64 `json:"confidence_level"`
	// RegressionThreshold sets the largest median ratio that is not considered a regression.
	RegressionThreshold float64 `json:"regression_threshold"`
	// BaselineSHA256 identifies the exact baseline artifact evaluated by the gate.
	BaselineSHA256 string `json:"baseline_sha256"`
	// CandidateSHA256 identifies the exact candidate artifact evaluated by the gate.
	CandidateSHA256 string `json:"candidate_sha256"`
	// AAReportSHA256 identifies the exact host A/A resolution report evaluated by the gate.
	AAReportSHA256 string `json:"aa_report_sha256,omitempty"`
	// DeclarationSHA256 identifies the canonical set of declared workloads.
	DeclarationSHA256 string `json:"declaration_sha256,omitempty"`
	// Passed reports whether every required gate condition succeeded.
	Passed bool `json:"passed"`
	// PromotionEligible reports whether this complete, non-diagnostic evidence may support production promotion.
	PromotionEligible bool `json:"promotion_eligible"`
	// MaterialityRequired reports that promotion requires at least one explicitly named improvement target.
	MaterialityRequired bool `json:"materiality_required"`
	// MaterialityTargets supplies the materiality targets input to the PerfGateReport contract.
	MaterialityTargets int `json:"materiality_targets"`
	// MaterialityPassed reports whether every resolved target cleared the configured A/A-aware improvement floor.
	MaterialityPassed bool `json:"materiality_passed"`
	// QualificationRequired reports whether the artifact contains a prioritized traversal candidate that requires independent training and frozen-holdout gates.
	QualificationRequired bool `json:"qualification_required"`
	// TrainingCases records prioritized traversal cases gated on the selector-training partition.
	TrainingCases int `json:"training_cases"`
	// HoldoutCases records prioritized traversal cases gated on the frozen topology holdout.
	HoldoutCases int `json:"holdout_cases"`
	// TrainingPassed reports whether every observed prioritized training case passed.
	TrainingPassed bool `json:"training_passed"`
	// HoldoutPassed reports whether every observed prioritized holdout case passed.
	HoldoutPassed bool `json:"holdout_passed"`
	// QualificationPassed reports whether nonempty training and holdout partitions independently passed.
	QualificationPassed bool `json:"qualification_passed"`
	// QualificationFamilies contains the independent split disposition for each concrete traversal candidate family.
	QualificationFamilies []TraversalQualificationStatus `json:"qualification_families,omitempty"`
	// Cases contains the gate disposition and statistical evidence for each declared workload.
	Cases []PerfGateCase `json:"cases"`
}

// performanceKey identifies one dataset, case, and backend across performance artifacts.
type performanceKey struct {
	// dataset names the fixture shared by matched baseline and candidate records.
	dataset string
	// name identifies the workload case within its dataset.
	name string
	// backend separates independently gated execution modes for the same workload.
	backend ExecutionMode
}

// roundSamples groups positive warm durations by independent measurement round.
type roundSamples map[int][]time.Duration

// comparePerformanceArtifacts validates two artifacts, writes their performance-gate report, and returns its pass status.
func comparePerformanceArtifacts(baselinePath, candidatePath, outputPath string, options PerfGateOptions) (bool, error) {
	baseline, err := readJSONLFile(baselinePath)
	if err != nil {
		return false, fmt.Errorf("read baseline: %w", err)
	}

	candidate, err := readJSONLFile(candidatePath)
	if err != nil {
		return false, fmt.Errorf("read candidate: %w", err)
	}
	if err := validatePerformanceArtifactSelections(baseline, candidate, options.DiagnosticMode); err != nil {
		return false, err
	}
	if options.AAReportPath != "" {
		options.AAReport, options.AAReportSHA256, err = loadAAResolutionReport(options.AAReportPath)
		if err != nil {
			return false, fmt.Errorf("load performance-gate A/A evidence: %w", err)
		}
	}

	baselineChecksum, err := fileSHA256(baselinePath)
	if err != nil {
		return false, err
	}
	candidateChecksum, err := fileSHA256(candidatePath)
	if err != nil {
		return false, err
	}

	report, err := buildPerfGateReport(baseline, candidate, options)
	if err != nil {
		return false, err
	}
	report.BaselineSHA256 = baselineChecksum
	report.CandidateSHA256 = candidateChecksum

	if err := writePerfGateReport(outputPath, report); err != nil {
		return false, err
	}
	return report.Passed && report.PromotionEligible, nil
}

// validatePerformanceArtifactSelections rejects adaptive or diagnostic artifacts when complete-gate input is required.
func validatePerformanceArtifactSelections(baseline, candidate []CaseResult, diagnosticMode bool) error {
	if !diagnosticMode && (hasAdaptiveDiscoveryRecord(baseline) || hasAdaptiveDiscoveryRecord(candidate)) {
		return fmt.Errorf("adaptive-discovery artifacts are refused by the complete performance gate")
	}
	baselineSelection, baselineErr := selectionIdentity(baseline)
	candidateSelection, candidateErr := selectionIdentity(candidate)
	if baselineErr != nil || candidateErr != nil {
		if diagnosticMode {
			return fmt.Errorf("diagnostic comparison requires selection manifests in both artifacts")
		}
		return fmt.Errorf("complete performance gate requires selection manifests in both artifacts")
	}
	if err := validateSelectionManifestAccounting(baselineSelection); err != nil {
		return fmt.Errorf("baseline artifact %w", err)
	}
	if err := validateSelectionManifestAccounting(candidateSelection); err != nil {
		return fmt.Errorf("candidate artifact %w", err)
	}
	if baselineSelection.ProtectedDeclarationCount != candidateSelection.ProtectedDeclarationCount ||
		baselineSelection.ProtectedDeclarationSHA256 != candidateSelection.ProtectedDeclarationSHA256 {
		return fmt.Errorf("artifact protected declaration omissions differ")
	}
	if baselineSelection.DiagnosticOnly || candidateSelection.DiagnosticOnly {
		if !diagnosticMode {
			return fmt.Errorf("diagnostic-only artifacts are refused by the complete performance gate")
		}
		if !baselineSelection.DiagnosticOnly || !candidateSelection.DiagnosticOnly {
			return fmt.Errorf("diagnostic comparison requires two diagnostic-only artifacts")
		}
		if baselineSelection.DeclarationSHA256 != candidateSelection.DeclarationSHA256 {
			return fmt.Errorf("diagnostic artifact declarations differ: %s != %s", baselineSelection.DeclarationSHA256, candidateSelection.DeclarationSHA256)
		}
		return nil
	}
	if diagnosticMode {
		return fmt.Errorf("diagnostic comparison mode requires filtered diagnostic-only artifacts")
	}
	return nil
}

// hasAdaptiveDiscoveryRecord reports whether any record was produced by adaptive existing-graph discovery.
func hasAdaptiveDiscoveryRecord(records []CaseResult) bool {
	for _, record := range records {
		if record.ExistingGraph != nil && record.ExistingGraph.Adaptive {
			return true
		}
		if record.Environment != nil && record.Environment.Protocol == "adaptive_discovery" {
			return true
		}
	}
	return false
}

// buildPerfGateReport compares matched baseline and candidate samples and classifies each declared workload.
func buildPerfGateReport(baseline, candidate []CaseResult, options PerfGateOptions) (PerfGateReport, error) {
	if err := validatePerformanceWorkloadIdentity(baseline, candidate); err != nil {
		return PerfGateReport{}, err
	}
	if options.Confidence <= 0 || options.Confidence >= 1 {
		return PerfGateReport{}, fmt.Errorf("confidence level must be between 0 and 1")
	}
	if options.RegressionThreshold < 0 {
		return PerfGateReport{}, fmt.Errorf("regression threshold must not be negative")
	}
	if options.BootstrapCount == 0 {
		options.BootstrapCount = defaultBootstrapCount
	}
	if options.BootstrapCount < 1 {
		return PerfGateReport{}, fmt.Errorf("bootstrap count must be positive")
	}
	if options.MaterialityRatio == 0 {
		options.MaterialityRatio = 0.95
	}
	if options.MaterialityRatio <= 0 || options.MaterialityRatio >= 1 {
		return PerfGateReport{}, fmt.Errorf("materiality ratio must be between 0 and 1")
	}
	if options.MaterialityAbsolute == 0 {
		options.MaterialityAbsolute = 100 * time.Microsecond
	}
	if options.MaterialityAbsolute < 0 {
		return PerfGateReport{}, fmt.Errorf("materiality absolute duration must not be negative")
	}

	baselineSeries := collectWarmSeries(baseline)
	candidateSeries := collectWarmSeries(candidate)
	keys := declaredPerformanceKeys(options.DeclaredBackends, baseline, candidate)
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].dataset != keys[j].dataset {
			return keys[i].dataset < keys[j].dataset
		}
		if keys[i].name != keys[j].name {
			return keys[i].name < keys[j].name
		}
		return keys[i].backend < keys[j].backend
	})
	if len(keys) == 0 {
		return PerfGateReport{}, fmt.Errorf("artifacts and declaration contain no PostgreSQL or Neo4j cases")
	}
	tiers := make(map[performanceKey]string, len(keys))
	splits := make(map[performanceKey]string, len(keys))
	hasPromotionTiming := false
	for _, key := range keys {
		tier, err := timingTier(key, baseline, candidate)
		if err != nil {
			return PerfGateReport{}, err
		}
		tiers[key] = tier
		split, err := qualificationSplit(key, baseline, candidate)
		if err != nil {
			return PerfGateReport{}, err
		}
		splits[key] = split
		if key.backend == ModePostgresSQL && (tier == "normal" || tier == "envelope") && promotionTimingSplit(split) {
			hasPromotionTiming = true
		}
	}
	if hasPromotionTiming && !options.DiagnosticMode {
		if !validSHA256(options.AAReportSHA256) {
			return PerfGateReport{}, fmt.Errorf("complete performance gate requires a checksummed host A/A report")
		}
		if err := validateAAResolutionEvidence(options.AAReport, baseline, options.Confidence); err != nil {
			return PerfGateReport{}, fmt.Errorf("baseline A/A evidence: %w", err)
		}
		if err := validateAAResolutionEvidence(options.AAReport, candidate, options.Confidence); err != nil {
			return PerfGateReport{}, fmt.Errorf("candidate A/A evidence: %w", err)
		}
	} else if options.AAReport != nil {
		if !validSHA256(options.AAReportSHA256) {
			return PerfGateReport{}, fmt.Errorf("supplied A/A report checksum is malformed")
		}
		if err := validateAAResolutionEvidence(options.AAReport, baseline, options.Confidence); err != nil {
			return PerfGateReport{}, err
		}
	}
	targetNames := make(map[string]struct{}, len(options.TargetNames))
	for _, name := range options.TargetNames {
		targetNames[name] = struct{}{}
	}

	report := PerfGateReport{
		Version:             perfGateVersion,
		Seed:                options.Seed,
		Confidence:          options.Confidence,
		RegressionThreshold: options.RegressionThreshold,
		AAReportSHA256:      options.AAReportSHA256,
		Passed:              true,
		PromotionEligible:   !options.DiagnosticMode && hasPromotionTiming && len(targetNames) > 0,
		MaterialityRequired: hasPromotionTiming && !options.DiagnosticMode,
		MaterialityPassed:   len(targetNames) > 0,
		TrainingPassed:      true,
		HoldoutPassed:       true,
	}
	resolvedMaterialityTargets := map[string]struct{}{}
	qualification := map[string]*TraversalQualificationStatus{}
	if len(options.DeclaredBackends) > 0 {
		report.DeclarationSHA256 = declarationSHA256(options.DeclaredBackends)
	}
	for idx, key := range keys {
		baselineStatus := artifactCaseStatus(baseline, key)
		candidateStatus := artifactCaseStatus(candidate, key)
		baselineRounds, candidateRounds := matchedRounds(baselineSeries[key], candidateSeries[key])
		gateCase := PerfGateCase{
			Dataset:                       key.dataset,
			Name:                          key.name,
			Backend:                       key.backend,
			Tier:                          tiers[key],
			QualificationSplit:            splits[key],
			TimingGated:                   key.backend == ModePostgresSQL && (tiers[key] == "normal" || tiers[key] == "envelope") && promotionTimingSplit(splits[key]) && !options.DiagnosticMode,
			Rounds:                        len(baselineRounds),
			BaselineSamples:               sampleCount(baselineRounds),
			CandidateSamples:              sampleCount(candidateRounds),
			BaselineStatus:                baselineStatus,
			CandidateStatus:               candidateStatus,
			OracleOnly:                    key.backend == ModeNeo4j,
			Passed:                        true,
			CandidateRuntimeReceiptChains: caseRuntimeReceiptChains(candidate, key),
		}
		if candidateStatus != StatusOK {
			gateCase.Passed = false
			gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("required candidate record status is %s", candidateStatus))
		}
		if gateCase.TimingGated {
			if err := validateCandidateRuntimeEvidence(candidate, key); err != nil {
				gateCase.Passed = false
				gateCase.Reasons = append(gateCase.Reasons, err.Error())
			}
		}
		// Neo4j is a correctness oracle. A successful record means its untimed
		// exact observation checks passed; its latency never affects this gate.
		if key.backend == ModeNeo4j {
			if !gateCase.Passed {
				report.Passed = false
			}
			report.Cases = append(report.Cases, gateCase)
			continue
		}
		if baselineStatus != StatusOK {
			gateCase.Passed = false
			gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("required baseline record status is %s", baselineStatus))
		}
		if gateCase.TimingGated && len(baselineRounds) < minimumGateRounds {
			gateCase.Passed = false
			gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("need at least %d matched rounds, got %d", minimumGateRounds, len(baselineRounds)))
		}
		if gateCase.TimingGated && len(baselineRounds) > 0 {
			if err := validatePairedOrderEvidence(baseline, candidate, key, sortedRounds(baselineRounds), minimumDiscoveryWarmups); err != nil {
				return PerfGateReport{}, fmt.Errorf("invalid promotion evidence: %w", err)
			}
		}
		if tiers[key] == "stress" {
			gateCase.Reasons = append(gateCase.Reasons, "stress tier timing is diagnostic")
		}
		if splits[key] == "diagnostic" {
			gateCase.Reasons = append(gateCase.Reasons, "diagnostic qualification split is excluded from promotion timing")
		}

		gateCase.P50NoiseRatio, gateCase.P50NoiseAbsolute = minimumTimingNoiseRatio, minimumTimingNoiseAbsolute
		gateCase.P95NoiseRatio, gateCase.P95NoiseAbsolute = minimumTimingNoiseRatio, minimumTimingNoiseAbsolute
		if options.AAReport != nil {
			if ratio, absolute, err := aaTimingFloor(options.AAReport, key, false, options.RegressionThreshold); err == nil {
				gateCase.P50NoiseRatio, gateCase.P50NoiseAbsolute = ratio, absolute
			} else if gateCase.TimingGated {
				return PerfGateReport{}, err
			}
			if ratio, absolute, err := aaTimingFloor(options.AAReport, key, true, options.RegressionThreshold); err == nil {
				gateCase.P95NoiseRatio, gateCase.P95NoiseAbsolute = ratio, absolute
			} else if gateCase.TimingGated {
				return PerfGateReport{}, err
			}
		} else {
			gateCase.P50NoiseRatio = max(gateCase.P50NoiseRatio, options.RegressionThreshold)
			gateCase.P95NoiseRatio = max(gateCase.P95NoiseRatio, options.RegressionThreshold)
		}

		seed := options.Seed + int64(idx)*7919
		if len(baselineRounds) > 0 {
			gateCase.MedianRatio = bootstrapRoundMedianRatio(baselineRounds, candidateRounds, seed, options)
			saving := bootstrapRoundMedianSaving(baselineRounds, candidateRounds, seed+3, options)
			gateCase.MedianSaving = &saving
			change := negateDurationInterval(saving)
			gateCase.MedianChange = &change
			if gateCase.TimingGated && gateCase.MedianRatio.Lower > 1+gateCase.P50NoiseRatio && change.Lower > gateCase.P50NoiseAbsolute {
				gateCase.Passed = false
				gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("median regression exceeds host A/A floors: ratio lower %.4f > %.4f and change lower %s > %s", gateCase.MedianRatio.Lower, 1+gateCase.P50NoiseRatio, change.Lower, gateCase.P50NoiseAbsolute))
			}
		}

		if gateCase.BaselineSamples >= minimumP95Samples && gateCase.CandidateSamples >= minimumP95Samples {
			interval := bootstrapStratifiedP95Ratio(baselineRounds, candidateRounds, seed+1, options)
			gateCase.P95Ratio = &interval
			change := bootstrapStratifiedQuantileChange(baselineRounds, candidateRounds, 0.95, seed+2, options)
			gateCase.P95Change = &change
			if gateCase.TimingGated && interval.Lower > 1+gateCase.P95NoiseRatio && change.Lower > gateCase.P95NoiseAbsolute {
				gateCase.Passed = false
				gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("p95 regression exceeds host A/A floors: ratio lower %.4f > %.4f and change lower %s > %s", interval.Lower, 1+gateCase.P95NoiseRatio, change.Lower, gateCase.P95NoiseAbsolute))
			}
		} else if gateCase.TimingGated {
			gateCase.Passed = false
			gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("need at least %d warm samples per side for p95, got %d/%d", minimumP95Samples, gateCase.BaselineSamples, gateCase.CandidateSamples))
		}

		if _, isTarget := targetNames[key.name]; isTarget && gateCase.TimingGated && len(baselineRounds) > 0 {
			resolvedMaterialityTargets[key.name] = struct{}{}
			effectiveRatio := min(options.MaterialityRatio, 1-gateCase.P50NoiseRatio)
			effectiveAbsolute := max(options.MaterialityAbsolute, gateCase.P50NoiseAbsolute)
			gateCase.MaterialityRatio = &effectiveRatio
			gateCase.MaterialityAbsolute = &effectiveAbsolute
			materialRatio := gateCase.MedianRatio.Upper <= effectiveRatio
			materialAbsolute := gateCase.MedianSaving != nil && gateCase.MedianSaving.Lower >= effectiveAbsolute
			if !materialRatio && !materialAbsolute {
				gateCase.Passed = false
				report.MaterialityPassed = false
				gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("target improvement is not material: median ratio upper %.4f > %.4f and saving lower %s < %s", gateCase.MedianRatio.Upper, effectiveRatio, gateCase.MedianSaving.Lower, effectiveAbsolute))
			}
		}

		if !gateCase.Passed {
			report.Passed = false
		}
		if prioritizedTraversalKey(key, baseline, candidate) && gateCase.TimingGated {
			report.QualificationRequired = true
			family := traversalQualificationFamily(key, baseline, candidate)
			status := qualification[family]
			if status == nil {
				status = &TraversalQualificationStatus{
					Family:         family,
					TrainingPassed: true,
					HoldoutPassed:  true,
				}
				qualification[family] = status
			}
			switch gateCase.QualificationSplit {
			case "training":
				report.TrainingCases++
				report.TrainingPassed = report.TrainingPassed && gateCase.Passed
				status.TrainingCases++
				status.TrainingPassed = status.TrainingPassed && gateCase.Passed
			case "holdout":
				report.HoldoutCases++
				report.HoldoutPassed = report.HoldoutPassed && gateCase.Passed
				status.HoldoutCases++
				status.HoldoutPassed = status.HoldoutPassed && gateCase.Passed
			}
		}
		report.Cases = append(report.Cases, gateCase)
	}
	report.MaterialityTargets = len(resolvedMaterialityTargets)
	if report.MaterialityRequired {
		if len(targetNames) == 0 {
			report.MaterialityPassed = false
		}
		if report.MaterialityTargets != len(targetNames) {
			return PerfGateReport{}, fmt.Errorf("materiality targets resolved to %d timing-gated cases, expected %d", report.MaterialityTargets, len(targetNames))
		}
	}
	if report.QualificationRequired {
		families := make([]string, 0, len(qualification))
		for family := range qualification {
			families = append(families, family)
		}
		sort.Strings(families)
		for _, family := range families {
			status := qualification[family]
			status.TrainingPassed = status.TrainingPassed && status.TrainingCases > 0
			status.HoldoutPassed = status.HoldoutPassed && status.HoldoutCases > 0
			status.Passed = status.TrainingPassed && status.HoldoutPassed
			report.TrainingPassed = report.TrainingPassed && status.TrainingPassed
			report.HoldoutPassed = report.HoldoutPassed && status.HoldoutPassed
			report.QualificationFamilies = append(report.QualificationFamilies, *status)
		}
		report.QualificationPassed = report.TrainingPassed && report.HoldoutPassed
		report.Passed = report.Passed && report.QualificationPassed
	} else {
		report.TrainingPassed = false
		report.HoldoutPassed = false
	}
	if options.DiagnosticMode {
		report.Passed = false
	}
	report.PromotionEligible = report.PromotionEligible && report.Passed && report.MaterialityPassed

	return report, nil
}

// validatePerformanceWorkloadIdentity ensures matched artifacts describe identical logical workloads per case and backend.
func validatePerformanceWorkloadIdentity(baseline, candidate []CaseResult) error {
	collect := func(label string, records []CaseResult) (map[performanceKey]string, error) {
		identities := map[performanceKey]string{}
		for _, record := range records {
			if record.ExecutionMode != ModePostgresSQL && record.ExecutionMode != ModeNeo4j {
				continue
			}
			key := performanceKey{
				dataset: record.Dataset,
				name:    record.Name,
				backend: record.ExecutionMode,
			}
			if record.WorkloadSHA256 == "" {
				return nil, fmt.Errorf("%s artifact case %s/%s/%s has no workload identity", label, key.dataset, key.name, key.backend)
			}
			identityPayload := struct {
				// WorkloadSHA256 binds the compared samples to one logical workload declaration.
				WorkloadSHA256 string `json:"workload_sha256"`
				// ManifestSHA256 identifies the anchor manifest that authorized the run.
				ManifestSHA256 string `json:"manifest_sha256,omitempty"`
				// ContentIdentity binds resumable work to the logical contents of the live graph.
				ContentIdentity string `json:"content_identity,omitempty"`
				// FixtureChecksum identifies the loaded fixture contents.
				FixtureChecksum string `json:"fixture_checksum,omitempty"`
				// FixtureConfiguration captures generator settings used to construct the loaded fixture.
				FixtureConfiguration string `json:"fixture_configuration,omitempty"`
			}{WorkloadSHA256: record.WorkloadSHA256}
			if record.ExistingGraph != nil {
				identityPayload.ManifestSHA256 = record.ExistingGraph.ManifestSHA256
				identityPayload.ContentIdentity = record.ExistingGraph.ContentIdentity
			}
			if record.Fixture != nil {
				identityPayload.FixtureChecksum = record.Fixture.Checksum
				identityPayload.FixtureConfiguration = record.Fixture.Configuration
			}
			raw, _ := json.Marshal(identityPayload)
			digest := sha256.Sum256(raw)
			identity := hex.EncodeToString(digest[:])
			if present, found := identities[key]; found && present != identity {
				return nil, fmt.Errorf("%s artifact case %s/%s/%s mixes workload identities", label, key.dataset, key.name, key.backend)
			}
			identities[key] = identity
		}
		return identities, nil
	}

	baselineIdentities, err := collect("baseline", baseline)
	if err != nil {
		return err
	}
	candidateIdentities, err := collect("candidate", candidate)
	if err != nil {
		return err
	}
	for key, baselineIdentity := range baselineIdentities {
		if candidateIdentity, found := candidateIdentities[key]; found && candidateIdentity != baselineIdentity {
			return fmt.Errorf("logical workload differs for %s/%s/%s", key.dataset, key.name, key.backend)
		}
	}
	return nil
}

// declaredPerformanceKeys returns the unique case/backend keys that the performance gate must evaluate.
func declaredPerformanceKeys(declared []DeclaredCaseBackend, baseline, candidate []CaseResult) []performanceKey {
	unique := map[performanceKey]struct{}{}
	for _, item := range declared {
		if item.UnsupportedReason != "" {
			continue
		}
		if item.Backend == ModePostgresSQL || item.Backend == ModeNeo4j {
			unique[performanceKey{
				dataset: item.Dataset,
				name:    item.Name,
				backend: item.Backend,
			}] = struct{}{}
		}
	}

	if len(declared) == 0 {
		for _, records := range [][]CaseResult{baseline, candidate} {
			for _, record := range records {
				if record.ExecutionMode == ModePostgresSQL || record.ExecutionMode == ModeNeo4j {
					unique[performanceKey{
						dataset: record.Dataset,
						name:    record.Name,
						backend: record.ExecutionMode,
					}] = struct{}{}
				}
			}
		}
	}

	keys := make([]performanceKey, 0, len(unique))
	for key := range unique {
		keys = append(keys, key)
	}
	return keys
}

// artifactCaseStatus returns the first non-OK status for a declared case/backend pair, "missing" when no record exists, or OK when every matching record succeeded.
func artifactCaseStatus(records []CaseResult, key performanceKey) string {
	found := false
	for _, record := range records {
		if record.Dataset != key.dataset || record.Name != key.name || record.ExecutionMode != key.backend {
			continue
		}
		found = true
		if record.Status != StatusOK {
			return record.Status
		}
	}
	if !found {
		return "missing"
	}
	return StatusOK
}

// declarationSHA256 sorts declared case/backend contracts and hashes their canonical JSON so compared artifacts must describe the same workload set.
func declarationSHA256(declared []DeclaredCaseBackend) string {
	items := append([]DeclaredCaseBackend(nil), declared...)
	sort.Slice(items, func(i, j int) bool {
		if items[i].Dataset != items[j].Dataset {
			return items[i].Dataset < items[j].Dataset
		}
		if items[i].Name != items[j].Name {
			return items[i].Name < items[j].Name
		}
		if items[i].Backend != items[j].Backend {
			return items[i].Backend < items[j].Backend
		}
		return items[i].UnsupportedReason < items[j].UnsupportedReason
	})
	digest := sha256.New()
	for _, item := range items {
		fmt.Fprintf(digest, "%s\x00%s\x00%s\x00%s\n", item.Dataset, item.Name, item.Backend, item.UnsupportedReason)
	}

	return hex.EncodeToString(digest.Sum(nil))
}

// collectWarmSeries groups positive warm durations by case, backend, and round.
func collectWarmSeries(records []CaseResult) map[performanceKey]roundSamples {
	series := map[performanceKey]roundSamples{}
	for _, record := range records {
		if record.Status != StatusOK {
			continue
		}
		key := performanceKey{
			dataset: record.Dataset,
			name:    record.Name,
			backend: record.ExecutionMode,
		}

		for _, sample := range record.Stats.Samples {
			if sample.Classification != "warm" || sample.Duration <= 0 {
				continue
			}

			if series[key] == nil {
				series[key] = roundSamples{}
			}
			series[key][sample.Round] = append(series[key][sample.Round], sample.Duration)
		}
	}

	return series
}

// matchedRounds returns round numbers present in both measurement series.
func matchedRounds(baseline, candidate roundSamples) (roundSamples, roundSamples) {
	matchedBaseline := roundSamples{}
	matchedCandidate := roundSamples{}
	for round, baselineSamples := range baseline {
		candidateSamples, found := candidate[round]
		if !found || len(baselineSamples) == 0 || len(candidateSamples) == 0 {
			continue
		}

		matchedBaseline[round] = baselineSamples
		matchedCandidate[round] = candidateSamples
	}

	return matchedBaseline, matchedCandidate
}

// bootstrapRoundMedianRatio bootstraps the ratio between paired round medians.
func bootstrapRoundMedianRatio(baseline, candidate roundSamples, seed int64, options PerfGateOptions) RatioInterval {
	rounds := sortedRounds(baseline)
	baselineMedians := make([]float64, len(rounds))
	candidateMedians := make([]float64, len(rounds))
	for idx, round := range rounds {
		baselineMedians[idx] = durationQuantile(baseline[round], 0.5)
		candidateMedians[idx] = durationQuantile(candidate[round], 0.5)
	}
	estimate := quantile(candidateMedians, 0.5) / quantile(baselineMedians, 0.5)
	rng := rand.New(rand.NewSource(seed)) // #nosec G404 -- deterministic statistical resampling
	ratios := make([]float64, options.BootstrapCount)
	resampledBaseline := make([]float64, len(rounds))
	resampledCandidate := make([]float64, len(rounds))
	for iteration := range ratios {
		for idx := range rounds {
			selected := rng.Intn(len(rounds))
			resampledBaseline[idx] = baselineMedians[selected]
			resampledCandidate[idx] = candidateMedians[selected]
		}
		ratios[iteration] = quantile(resampledCandidate, 0.5) / quantile(resampledBaseline, 0.5)
	}
	return confidenceInterval(estimate, ratios, options.Confidence)
}

// bootstrapRoundMedianSaving bootstraps the absolute duration saved between paired round medians.
func bootstrapRoundMedianSaving(baseline, candidate roundSamples, seed int64, options PerfGateOptions) DurationInterval {
	rounds := sortedRounds(baseline)
	baselineMedians := make([]float64, len(rounds))
	candidateMedians := make([]float64, len(rounds))
	for idx, round := range rounds {
		baselineMedians[idx] = durationQuantile(baseline[round], 0.5)
		candidateMedians[idx] = durationQuantile(candidate[round], 0.5)
	}
	estimate := quantile(baselineMedians, 0.5) - quantile(candidateMedians, 0.5)
	rng := rand.New(rand.NewSource(seed)) // #nosec G404 -- deterministic statistical resampling
	savings := make([]float64, options.BootstrapCount)
	resampledBaseline := make([]float64, len(rounds))
	resampledCandidate := make([]float64, len(rounds))
	for iteration := range savings {
		for idx := range rounds {
			selected := rng.Intn(len(rounds))
			resampledBaseline[idx] = baselineMedians[selected]
			resampledCandidate[idx] = candidateMedians[selected]
		}
		savings[iteration] = quantile(resampledBaseline, 0.5) - quantile(resampledCandidate, 0.5)
	}
	interval := confidenceInterval(estimate, savings, options.Confidence)
	return DurationInterval{
		Estimate: time.Duration(interval.Estimate),
		Lower:    time.Duration(interval.Lower),
		Upper:    time.Duration(interval.Upper),
	}
}

// bootstrapStratifiedP95Ratio bootstraps a P95 ratio while preserving round strata.
func bootstrapStratifiedP95Ratio(baseline, candidate roundSamples, seed int64, options PerfGateOptions) RatioInterval {
	rounds := sortedRounds(baseline)
	estimate := durationQuantile(flattenSamples(candidate, rounds), 0.95) / durationQuantile(flattenSamples(baseline, rounds), 0.95)
	rng := rand.New(rand.NewSource(seed)) // #nosec G404 -- deterministic statistical resampling
	ratios := make([]float64, options.BootstrapCount)
	for iteration := range ratios {
		var resampledBaseline, resampledCandidate []time.Duration
		for _, round := range rounds {
			resampledBaseline = append(resampledBaseline, resampleDurations(rng, baseline[round])...)
			resampledCandidate = append(resampledCandidate, resampleDurations(rng, candidate[round])...)
		}
		ratios[iteration] = durationQuantile(resampledCandidate, 0.95) / durationQuantile(resampledBaseline, 0.95)
	}
	return confidenceInterval(estimate, ratios, options.Confidence)
}

// confidenceInterval returns the requested central interval from sorted bootstrap estimates.
func confidenceInterval(estimate float64, samples []float64, confidence float64) RatioInterval {
	alpha := (1 - confidence) / 2
	return RatioInterval{
		Estimate: estimate,
		Lower:    quantile(samples, alpha),
		Upper:    quantile(samples, 1-alpha),
	}
}

// durationQuantile returns a nearest-rank duration quantile from a copy of the samples.
func durationQuantile(values []time.Duration, probability float64) float64 {
	numeric := make([]float64, len(values))
	for idx, value := range values {
		numeric[idx] = float64(value)
	}
	return quantile(numeric, probability)
}

// quantile returns a nearest-rank quantile from sorted floating-point samples.
func quantile(values []float64, probability float64) float64 {
	ordered := append([]float64(nil), values...)
	sort.Float64s(ordered)
	if len(ordered) == 0 {
		return math.NaN()
	}
	index := int(math.Ceil(probability*float64(len(ordered)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(ordered) {
		index = len(ordered) - 1
	}
	return ordered[index]
}

// sortedRounds returns measurement round keys in ascending order.
func sortedRounds(samples roundSamples) []int {
	rounds := make([]int, 0, len(samples))
	for round := range samples {
		rounds = append(rounds, round)
	}
	sort.Ints(rounds)
	return rounds
}

// flattenSamples concatenates samples from the requested rounds in the supplied round order.
func flattenSamples(samples roundSamples, rounds []int) []time.Duration {
	var flattened []time.Duration
	for _, round := range rounds {
		flattened = append(flattened, samples[round]...)
	}
	return flattened
}

// resampleDurations draws a same-size bootstrap sample of durations with replacement.
func resampleDurations(rng *rand.Rand, values []time.Duration) []time.Duration {
	resampled := make([]time.Duration, len(values))
	for idx := range resampled {
		resampled[idx] = values[rng.Intn(len(values))]
	}
	return resampled
}

// sampleCount returns the total number of durations across all measurement rounds.
func sampleCount(samples roundSamples) int {
	count := 0
	for _, values := range samples {
		count += len(values)
	}
	return count
}

// fileSHA256 returns the SHA-256 digest of a file's contents.
func fileSHA256(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(content)
	return hex.EncodeToString(digest[:]), nil
}

// writePerfGateReport writes a performance-gate report to stdout or the requested file.
func writePerfGateReport(path string, report PerfGateReport) (err error) {
	var output *os.File
	if path == "" {
		output = os.Stdout
	} else {
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
