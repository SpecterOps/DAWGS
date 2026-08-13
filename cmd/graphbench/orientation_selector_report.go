// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sort"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
)

const orientationSelectorReportVersion = 1

// OrientationSelectorReportOptions configures the matched shadow/incumbent/
// reverse comparison and its frozen qualification protocol.
type OrientationSelectorReportOptions struct {
	Seed           int64
	Confidence     float64
	BootstrapCount int
	Protocol       string
}

// OrientationLatencyGate records one frozen relative-or-absolute latency
// rule. A case passes when either the ratio upper bound or absolute upper gap
// stays within its declared limit.
type OrientationLatencyGate struct {
	BaselineIdentity string           `json:"baseline_identity"`
	ObservedIdentity string           `json:"observed_identity"`
	BaselineSamples  int              `json:"baseline_samples"`
	ObservedSamples  int              `json:"observed_samples"`
	Ratio            RatioInterval    `json:"median_ratio"`
	AbsoluteChange   DurationInterval `json:"median_absolute_change"`
	RatioUpperLimit  float64          `json:"ratio_upper_limit"`
	AbsoluteFloor    time.Duration    `json:"absolute_floor"`
	AbsoluteGapUpper time.Duration    `json:"absolute_gap_upper"`
	Passed           bool             `json:"passed"`
}

// OrientationSelectorCase reports shadow attribution, exact-arm regret, and
// probe-only overhead for one topology bucket.
type OrientationSelectorCase struct {
	Dataset                  string                 `json:"dataset"`
	Name                     string                 `json:"name"`
	QualificationSplit       string                 `json:"qualification_split"`
	QualificationRole        string                 `json:"qualification_role"`
	ThresholdTuningEligible  bool                   `json:"threshold_tuning_eligible"`
	QualificationEligible    bool                   `json:"qualification_eligible"`
	Rounds                   int                    `json:"matched_rounds"`
	WouldSelectIdentity      string                 `json:"would_select_identity"`
	FastestExactIdentity     string                 `json:"fastest_exact_identity"`
	ExactObservationsMatched bool                   `json:"exact_observations_matched"`
	SelectorRegret           OrientationLatencyGate `json:"selector_regret"`
	ProbeOverhead            OrientationLatencyGate `json:"probe_overhead"`
	Passed                   bool                   `json:"passed"`
	Reasons                  []string               `json:"reasons,omitempty"`
}

// OrientationSelectorReport validates that shadow selection is attributable,
// low-regret, and cheap while the incumbent remains the only shadow execution
// arm. Diagnostic and legacy records never contribute to qualification.
type OrientationSelectorReport struct {
	Version                    int                       `json:"version"`
	Policy                     string                    `json:"policy"`
	Protocol                   string                    `json:"protocol"`
	Seed                       int64                     `json:"seed"`
	Confidence                 float64                   `json:"confidence_level"`
	ShadowArtifactSHA256       string                    `json:"shadow_artifact_sha256,omitempty"`
	IncumbentArtifactSHA256    string                    `json:"incumbent_artifact_sha256,omitempty"`
	ReverseArtifactSHA256      string                    `json:"reverse_artifact_sha256,omitempty"`
	AAReportSHA256             string                    `json:"aa_report_sha256,omitempty"`
	SelectorRegretRatioLimit   float64                   `json:"selector_regret_ratio_upper_limit"`
	ProbeOverheadRatioLimit    float64                   `json:"probe_overhead_ratio_upper_limit"`
	ProbeOverheadAbsoluteLimit time.Duration             `json:"probe_overhead_absolute_limit"`
	EvidencePassed             bool                      `json:"evidence_passed"`
	TrainingCases              int                       `json:"training_cases"`
	HoldoutCases               int                       `json:"holdout_cases"`
	TrainingPassed             bool                      `json:"training_passed"`
	HoldoutPassed              bool                      `json:"holdout_passed"`
	QualificationPassed        bool                      `json:"qualification_passed"`
	Cases                      []OrientationSelectorCase `json:"cases"`
}

type orientationSelectorSeries struct {
	shadow      roundSamples
	incumbent   roundSamples
	reverse     roundSamples
	wouldSelect string
}

// buildOrientationSelectorReport compares a true-shadow artifact with matched
// exact incumbent and forced-reverse artifacts. The shadow's public result and
// runtime identity must remain incumbent even when would_select names reverse.
func buildOrientationSelectorReport(
	shadowRecords, incumbentRecords, reverseRecords []CaseResult,
	aa *AAResolutionReport,
	options OrientationSelectorReportOptions,
) (OrientationSelectorReport, error) {
	if options.Confidence <= 0 || options.Confidence >= 1 {
		return OrientationSelectorReport{}, fmt.Errorf("confidence level must be between 0 and 1")
	}
	if options.BootstrapCount == 0 {
		options.BootstrapCount = defaultBootstrapCount
	}
	if options.BootstrapCount < 1 {
		return OrientationSelectorReport{}, fmt.Errorf("bootstrap count must be positive")
	}
	protocol := options.Protocol
	if protocol == "" {
		protocol = referencePairProtocolConfirmation
	}
	minimumWarmups, minimumRounds, maximumRounds, minimumSamples := 20, 10, 20, 50
	if protocol == referencePairProtocolDiscovery {
		minimumWarmups, minimumRounds, maximumRounds, minimumSamples = 5, 5, 20, 10
	} else if protocol != referencePairProtocolConfirmation {
		return OrientationSelectorReport{}, fmt.Errorf("unsupported orientation selector protocol %q", protocol)
	}

	if err := validateAAResolutionEvidence(aa, incumbentRecords, options.Confidence); err != nil {
		return OrientationSelectorReport{}, fmt.Errorf("incumbent A/A evidence: %w", err)
	}
	incumbentHost, err := artifactHostFingerprint(incumbentRecords)
	if err != nil {
		return OrientationSelectorReport{}, err
	}
	for name, records := range map[string][]CaseResult{"shadow": shadowRecords, "reverse": reverseRecords} {
		host, err := artifactHostFingerprint(records)
		if err != nil {
			return OrientationSelectorReport{}, fmt.Errorf("%s artifact host: %w", name, err)
		}
		if host != incumbentHost {
			return OrientationSelectorReport{}, fmt.Errorf("%s artifact host does not match incumbent host", name)
		}
	}

	series, keys, err := collectOrientationSelectorSeries(shadowRecords, incumbentRecords, reverseRecords)
	if err != nil {
		return OrientationSelectorReport{}, err
	}
	report := OrientationSelectorReport{
		Version:                    orientationSelectorReportVersion,
		Policy:                     string(optimize.ExpansionSearchPolicyOrientationProbeV1),
		Protocol:                   protocol,
		Seed:                       options.Seed,
		Confidence:                 options.Confidence,
		SelectorRegretRatioLimit:   1.10,
		ProbeOverheadRatioLimit:    1.10,
		ProbeOverheadAbsoluteLimit: 100 * time.Microsecond,
		EvidencePassed:             true,
	}
	trainingPassed, holdoutPassed := true, true
	gateOptions := PerfGateOptions{Seed: options.Seed, Confidence: options.Confidence, BootstrapCount: options.BootstrapCount}
	for index, key := range keys {
		current := series[key]
		shadow, incumbent := matchedRounds(current.shadow, current.incumbent)
		incumbent, reverse := matchedRounds(incumbent, current.reverse)
		shadow, incumbent = matchedRounds(shadow, incumbent)
		if len(shadow) < minimumRounds || len(shadow) > maximumRounds {
			return OrientationSelectorReport{}, fmt.Errorf("%s/%s requires %d-%d matched orientation rounds, got %d", key.dataset, key.name, minimumRounds, maximumRounds, len(shadow))
		}
		for _, round := range sortedRounds(shadow) {
			if len(shadow[round]) < minimumSamples || len(incumbent[round]) < minimumSamples || len(reverse[round]) < minimumSamples {
				return OrientationSelectorReport{}, fmt.Errorf("%s/%s round %d requires %d samples per orientation arm", key.dataset, key.name, round, minimumSamples)
			}
		}
		if err := validateOrientationArmOrder(shadowRecords, incumbentRecords, reverseRecords, key, sortedRounds(shadow), minimumWarmups); err != nil {
			return OrientationSelectorReport{}, err
		}

		split, err := qualificationSplit(key, shadowRecords, incumbentRecords, reverseRecords)
		if err != nil {
			return OrientationSelectorReport{}, err
		}
		role, tuningEligible, qualificationEligible := orientationQualificationRole(split, protocol)
		fastestIdentity, fastest := fastestOrientationExactArm(incumbent, reverse)
		selected := incumbent
		if current.wouldSelect == string(optimize.ExpansionSearchSuffixSeededReverse) {
			selected = reverse
		}
		seed := options.Seed + int64(index)*7919
		_, selectorFloorAbsolute, err := aaTimingFloor(aa, key, false, 0)
		if err != nil {
			return OrientationSelectorReport{}, err
		}
		selectorRegret := orientationLatencyGate(
			fastestIdentity,
			current.wouldSelect,
			fastest,
			selected,
			1.10,
			selectorFloorAbsolute,
			seed,
			gateOptions,
		)
		probeOverhead := orientationLatencyGate(
			string(optimize.ExpansionSearchStepwiseForward),
			string(optimize.ExpansionSearchPolicyOrientationProbeV1),
			incumbent,
			shadow,
			1.10,
			report.ProbeOverheadAbsoluteLimit,
			seed+3,
			gateOptions,
		)
		entry := OrientationSelectorCase{
			Dataset:                  key.dataset,
			Name:                     key.name,
			QualificationSplit:       split,
			QualificationRole:        role,
			ThresholdTuningEligible:  tuningEligible,
			QualificationEligible:    qualificationEligible,
			Rounds:                   len(shadow),
			WouldSelectIdentity:      current.wouldSelect,
			FastestExactIdentity:     fastestIdentity,
			ExactObservationsMatched: true,
			SelectorRegret:           selectorRegret,
			ProbeOverhead:            probeOverhead,
			Passed:                   selectorRegret.Passed && probeOverhead.Passed,
		}
		if !selectorRegret.Passed {
			entry.Reasons = append(entry.Reasons, "selector regret exceeds the 1.10/A/A floor")
		}
		if !probeOverhead.Passed {
			entry.Reasons = append(entry.Reasons, "shadow probe overhead exceeds 10% and 100us")
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
	report.QualificationPassed = report.TrainingPassed && report.HoldoutPassed
	return report, nil
}

func collectOrientationSelectorSeries(
	shadowRecords, incumbentRecords, reverseRecords []CaseResult,
) (map[performanceKey]*orientationSelectorSeries, []performanceKey, error) {
	series := map[performanceKey]*orientationSelectorSeries{}
	for _, record := range shadowRecords {
		if record.ExecutionMode != ModePostgresSQL || record.TraversalTelemetry == nil || record.TraversalTelemetry.Summary.WouldSelectIdentity == "" {
			continue
		}
		key := performanceKey{dataset: record.Dataset, name: record.Name, backend: record.ExecutionMode}
		if series[key] == nil {
			series[key] = &orientationSelectorSeries{shadow: roundSamples{}, incumbent: roundSamples{}, reverse: roundSamples{}}
		}
		if err := validateOrientationRecord(record, "shadow"); err != nil {
			return nil, nil, err
		}
		wouldSelect := record.TraversalTelemetry.Summary.WouldSelectIdentity
		if series[key].wouldSelect != "" && series[key].wouldSelect != wouldSelect {
			return nil, nil, fmt.Errorf("%s/%s changes shadow would_select identity across rounds", key.dataset, key.name)
		}
		series[key].wouldSelect = wouldSelect
		appendOrientationWarmSamples(series[key].shadow, record)
	}
	if len(series) == 0 {
		return nil, nil, fmt.Errorf("shadow artifact has no attributable orientation shadow records")
	}

	for arm, records := range map[string][]CaseResult{"incumbent": incumbentRecords, "reverse": reverseRecords} {
		for _, record := range records {
			key := performanceKey{dataset: record.Dataset, name: record.Name, backend: record.ExecutionMode}
			current := series[key]
			if current == nil {
				continue
			}
			if err := validateOrientationRecord(record, arm); err != nil {
				return nil, nil, err
			}
			if arm == "incumbent" {
				appendOrientationWarmSamples(current.incumbent, record)
			} else {
				appendOrientationWarmSamples(current.reverse, record)
			}
		}
	}

	keys := make([]performanceKey, 0, len(series))
	for key, current := range series {
		if len(current.incumbent) == 0 || len(current.reverse) == 0 {
			return nil, nil, fmt.Errorf("%s/%s lacks matched incumbent or forced-reverse records", key.dataset, key.name)
		}
		if err := validateOrientationExactObservations(key, shadowRecords, incumbentRecords, reverseRecords); err != nil {
			return nil, nil, err
		}
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].dataset < keys[j].dataset || keys[i].dataset == keys[j].dataset && keys[i].name < keys[j].name
	})
	return series, keys, nil
}

func validateOrientationRecord(record CaseResult, arm string) error {
	if record.Status != StatusOK || record.Environment == nil || record.TraversalTelemetry == nil {
		return fmt.Errorf("%s/%s %s arm lacks a successful telemetry-bearing record", record.Dataset, record.Name, arm)
	}
	summary := record.TraversalTelemetry.Summary
	forward := string(optimize.ExpansionSearchStepwiseForward)
	reverse := string(optimize.ExpansionSearchSuffixSeededReverse)
	switch arm {
	case "shadow":
		if summary.EmittedIdentity != string(optimize.ExpansionSearchPolicyOrientationProbeV1) ||
			summary.SelectorVersion != string(optimize.ExpansionSearchPolicyOrientationProbeV1) ||
			summary.RuntimeIdentity != forward || summary.AppliedIdentity != forward || summary.RuntimeBranch != "shadow_incumbent" ||
			(summary.WouldSelectIdentity != forward && summary.WouldSelectIdentity != reverse) ||
			summary.FallbackExecuted == nil || *summary.FallbackExecuted {
			return fmt.Errorf("%s/%s shadow telemetry does not prove incumbent-only orientation shadow execution", record.Dataset, record.Name)
		}
	case "incumbent":
		if summary.RuntimeIdentity != forward || summary.AppliedIdentity != forward || summary.WouldSelectIdentity != "" {
			return fmt.Errorf("%s/%s incumbent artifact did not execute the exact forward arm", record.Dataset, record.Name)
		}
	case "reverse":
		if summary.RuntimeIdentity != reverse || summary.AppliedIdentity != reverse || summary.WouldSelectIdentity != "" {
			return fmt.Errorf("%s/%s reverse artifact did not execute the exact forced reverse arm", record.Dataset, record.Name)
		}
	default:
		return fmt.Errorf("unknown orientation arm %q", arm)
	}
	return nil
}

func appendOrientationWarmSamples(series roundSamples, record CaseResult) {
	for _, sample := range record.Stats.Samples {
		if sample.Classification == "warm" && sample.Duration > 0 {
			series[sample.Round] = append(series[sample.Round], sample.Duration)
		}
	}
}

func validateOrientationExactObservations(key performanceKey, artifacts ...[]CaseResult) error {
	workload := ""
	var observed []string
	rowCount := int64(-1)
	binary := ""
	for _, records := range artifacts {
		matched := false
		armSQL := ""
		for _, record := range records {
			if record.Dataset != key.dataset || record.Name != key.name || record.ExecutionMode != key.backend {
				continue
			}
			matched = true
			if !record.StableObservation || record.WorkloadSHA256 == "" || record.SQLFingerprint == "" || record.Environment == nil || record.Environment.BinarySHA256 == "" {
				return fmt.Errorf("%s/%s lacks stable observation or executable/SQL identity", key.dataset, key.name)
			}
			if workload != "" && workload != record.WorkloadSHA256 {
				return fmt.Errorf("%s/%s workload identity differs across orientation arms", key.dataset, key.name)
			}
			workload = record.WorkloadSHA256
			if rowCount >= 0 && (rowCount != record.RowCount || !slices.Equal(observed, record.ObservedRows)) {
				return fmt.Errorf("%s/%s exact observations differ across orientation arms", key.dataset, key.name)
			}
			rowCount, observed = record.RowCount, append([]string(nil), record.ObservedRows...)
			if binary != "" && binary != record.Environment.BinarySHA256 {
				return fmt.Errorf("%s/%s executable identity differs across orientation arms", key.dataset, key.name)
			}
			binary = record.Environment.BinarySHA256
			if armSQL != "" && armSQL != record.SQLFingerprint {
				return fmt.Errorf("%s/%s SQL fingerprint changes within an orientation arm", key.dataset, key.name)
			}
			armSQL = record.SQLFingerprint
		}
		if !matched {
			return fmt.Errorf("%s/%s is missing from one orientation artifact", key.dataset, key.name)
		}
	}
	return nil
}

func validateOrientationArmOrder(
	shadowRecords, incumbentRecords, reverseRecords []CaseResult,
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
	}
	evidence := make([]map[int]pairedRoundEvidence, len(armRecords))
	positionCounts := make([][4]int, len(armRecords))
	for index, arm := range armRecords {
		current, err := collectPairedRoundEvidence(arm.records, key)
		if err != nil {
			return err
		}
		evidence[index] = current
	}
	for _, round := range rounds {
		seenPositions := map[int]struct{}{}
		block, runUUID := 0, ""
		for index, arm := range armRecords {
			current, found := evidence[index][round]
			if !found || current.Warmups < minimumWarmups || current.Arm == "" || current.Arm == "unlabeled" {
				return fmt.Errorf("%s/%s round %d lacks %s arm identity or %d warmups", key.dataset, key.name, round, arm.name, minimumWarmups)
			}
			if current.ArmOrder < 1 || current.ArmOrder > 3 {
				return fmt.Errorf("%s/%s round %d has invalid three-arm order", key.dataset, key.name, round)
			}
			if _, duplicate := seenPositions[current.ArmOrder]; duplicate {
				return fmt.Errorf("%s/%s round %d has duplicate three-arm order", key.dataset, key.name, round)
			}
			seenPositions[current.ArmOrder] = struct{}{}
			positionCounts[index][current.ArmOrder]++
			if block == 0 {
				block, runUUID = current.Block, current.RunUUID
			} else if current.Block != block || current.RunUUID != runUUID {
				return fmt.Errorf("%s/%s round %d has mismatched three-arm block or run UUID", key.dataset, key.name, round)
			}
		}
		if block < 1 || runUUID == "" {
			return fmt.Errorf("%s/%s round %d has missing three-arm block or run UUID", key.dataset, key.name, round)
		}
	}
	for index, counts := range positionCounts {
		minimum, maximum := counts[1], counts[1]
		for position := 2; position <= 3; position++ {
			minimum = min(minimum, counts[position])
			maximum = max(maximum, counts[position])
		}
		if maximum-minimum > 1 {
			return fmt.Errorf("%s/%s %s arm order is not position-balanced", key.dataset, key.name, armRecords[index].name)
		}
	}
	return nil
}

func orientationQualificationRole(split, protocol string) (role string, tuningEligible, qualificationEligible bool) {
	switch split {
	case "training":
		return "selector_training", true, protocol == referencePairProtocolConfirmation
	case "holdout":
		return "frozen_evaluation", false, protocol == referencePairProtocolConfirmation
	case "diagnostic":
		return "diagnostic_only", false, false
	default:
		return "legacy_diagnostic", false, false
	}
}

func fastestOrientationExactArm(incumbent, reverse roundSamples) (string, roundSamples) {
	if roundMedianEstimate(reverse) < roundMedianEstimate(incumbent) {
		return string(optimize.ExpansionSearchSuffixSeededReverse), reverse
	}
	return string(optimize.ExpansionSearchStepwiseForward), incumbent
}

func roundMedianEstimate(samples roundSamples) float64 {
	rounds := sortedRounds(samples)
	medians := make([]float64, 0, len(rounds))
	for _, round := range rounds {
		medians = append(medians, durationQuantile(samples[round], 0.5))
	}
	return quantile(medians, 0.5)
}

func orientationLatencyGate(
	baselineIdentity, observedIdentity string,
	baseline, observed roundSamples,
	ratioLimit float64,
	absoluteFloor time.Duration,
	seed int64,
	options PerfGateOptions,
) OrientationLatencyGate {
	ratio := bootstrapRoundMedianRatio(baseline, observed, seed, options)
	change := negateDurationInterval(bootstrapRoundMedianSaving(baseline, observed, seed+1, options))
	absoluteGapUpper := max(time.Duration(0), change.Upper)
	return OrientationLatencyGate{
		BaselineIdentity: baselineIdentity,
		ObservedIdentity: observedIdentity,
		BaselineSamples:  sampleCount(baseline),
		ObservedSamples:  sampleCount(observed),
		Ratio:            ratio,
		AbsoluteChange:   change,
		RatioUpperLimit:  ratioLimit,
		AbsoluteFloor:    absoluteFloor,
		AbsoluteGapUpper: absoluteGapUpper,
		Passed:           ratio.Upper <= ratioLimit || absoluteGapUpper <= absoluteFloor,
	}
}

// createOrientationSelectorReport loads the three exact arm artifacts and A/A
// calibration, builds the report, and writes an indented JSON document.
func createOrientationSelectorReport(
	shadowPath, incumbentPath, reversePath, aaPath, outputPath string,
	options OrientationSelectorReportOptions,
) (bool, error) {
	shadow, err := readJSONLFile(shadowPath)
	if err != nil {
		return false, fmt.Errorf("read orientation shadow artifact: %w", err)
	}
	incumbent, err := readJSONLFile(incumbentPath)
	if err != nil {
		return false, fmt.Errorf("read orientation incumbent artifact: %w", err)
	}
	reverse, err := readJSONLFile(reversePath)
	if err != nil {
		return false, fmt.Errorf("read orientation reverse artifact: %w", err)
	}
	aa, aaSHA, err := loadAAResolutionReport(aaPath)
	if err != nil {
		return false, fmt.Errorf("read orientation A/A report: %w", err)
	}
	report, err := buildOrientationSelectorReport(shadow, incumbent, reverse, aa, options)
	if err != nil {
		return false, err
	}
	report.ShadowArtifactSHA256, err = fileSHA256(shadowPath)
	if err != nil {
		return false, err
	}
	report.IncumbentArtifactSHA256, err = fileSHA256(incumbentPath)
	if err != nil {
		return false, err
	}
	report.ReverseArtifactSHA256, err = fileSHA256(reversePath)
	if err != nil {
		return false, err
	}
	report.AAReportSHA256 = aaSHA
	return report.QualificationPassed, writeOrientationSelectorReport(outputPath, report)
}

func writeOrientationSelectorReport(path string, report OrientationSelectorReport) (err error) {
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
