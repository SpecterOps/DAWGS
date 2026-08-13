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
	"math/rand"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
)

// confirmationReportVersion identifies the JSON schema emitted by confirmation reports.
const confirmationReportVersion = 4

// ConfirmationOptions selects the paired artifacts, cases, confidence level, and bootstrap seed used for confirmation.
type ConfirmationOptions struct {
	// Seed controls deterministic random sampling.
	Seed int64
	// Confidence sets the confidence level used for statistical intervals.
	Confidence float64
	// BootstrapCount sets the number of bootstrap resamples.
	BootstrapCount int
	// CaseNames restricts confirmation to the named workloads when nonempty.
	CaseNames []string
}

// ConfirmationMetric combines ratio, absolute-change, noise-floor, and classification evidence for one metric.
type ConfirmationMetric struct {
	// Ratio reports the candidate-to-baseline latency ratio.
	Ratio RatioInterval `json:"ratio"`
	// AbsoluteChange reports the estimated absolute duration change and confidence bounds.
	AbsoluteChange DurationInterval `json:"absolute_change"`
	// NoiseRatio records the relative A/A noise floor used for classification.
	NoiseRatio float64 `json:"noise_ratio"`
	// NoiseAbsolute records the absolute A/A noise floor used for classification.
	NoiseAbsolute time.Duration `json:"noise_absolute"`
	// Classification records the assigned measurement or result class.
	Classification string `json:"classification"`
}

// ConfirmationCase reports comparability, timing deltas, and the final disposition for one confirmed case.
type ConfirmationCase struct {
	// Dataset identifies the fixture dataset.
	Dataset string `json:"dataset"`
	// Name identifies the case or record within its dataset.
	Name string `json:"name"`
	// Backend identifies the execution backend.
	Backend ExecutionMode `json:"backend"`
	// Tier identifies whether latency is promotion-gated or stress-diagnostic.
	Tier string `json:"tier"`
	// QualificationSplit identifies training, frozen holdout, or diagnostic evidence.
	QualificationSplit string `json:"qualification_split"`
	// TimingGated reports whether timing evidence contributes to promotion.
	TimingGated bool `json:"timing_gated"`
	// MatchedRounds records rounds containing both left- and right-arm samples.
	MatchedRounds int `json:"matched_rounds"`
	// LeftSamples records warm samples accepted from the left confirmation arm.
	LeftSamples int `json:"left_samples"`
	// RightSamples records warm samples accepted from the right confirmation arm.
	RightSamples int `json:"right_samples"`
	// Comparable reports whether the paired measurements satisfy comparison prerequisites.
	Comparable bool `json:"comparable"`
	// Comparability lists reasons paired confirmation records are or are not comparable.
	Comparability []string `json:"comparability_reasons,omitempty"`
	// P50 contains median ratio, absolute-change, noise, and classification evidence.
	P50 ConfirmationMetric `json:"p50"`
	// P95 contains 95th-percentile ratio, absolute-change, noise, and classification evidence.
	P95 ConfirmationMetric `json:"p95"`
	// Disposition records the confirmation classification assigned to the case.
	Disposition string `json:"disposition"`
	// RightRuntimeReceiptChains preserves the candidate/right arm's complete
	// measured runtime branch chains.
	RightRuntimeReceiptChains [][]RuntimeReceiptEvent `json:"right_runtime_receipt_chains,omitempty"`
}

// ConfirmationReport contains paired-arm identities, A/A noise evidence, and per-case confirmation decisions.
type ConfirmationReport struct {
	// Version identifies the serialized schema revision.
	Version int `json:"version"`
	// Kind identifies the serialized confirmation-report format.
	Kind string `json:"kind"`
	// Seed controls deterministic random sampling.
	Seed int64 `json:"seed"`
	// Confidence sets the confidence level used for statistical intervals.
	Confidence float64 `json:"confidence_level"`
	// LeftArm identifies the artifact treated as the left confirmation arm.
	LeftArm string `json:"left_arm"`
	// RightArm identifies the artifact treated as the right confirmation arm.
	RightArm string `json:"right_arm"`
	// LeftSHA256 identifies the exact left-arm artifact evaluated by the report.
	LeftSHA256 string `json:"left_sha256"`
	// RightSHA256 identifies the exact right-arm artifact evaluated by the report.
	RightSHA256 string `json:"right_sha256"`
	// AAReport contains A/A noise evidence used to classify confirmation differences.
	AAReport string `json:"aa_report,omitempty"`
	// AAReportSHA256 identifies the exact A/A report used for classification.
	AAReportSHA256 string `json:"aa_report_sha256,omitempty"`
	// PromotionEligible reports whether every timing-gated causal case is comparable and P95-non-inferior.
	PromotionEligible bool `json:"promotion_eligible"`
	// QualificationRequired reports whether the artifact contains a prioritized traversal candidate that requires independent training and frozen-holdout confirmation.
	QualificationRequired bool `json:"qualification_required"`
	// TrainingCases records prioritized traversal cases confirmed on the selector-training partition.
	TrainingCases int `json:"training_cases"`
	// HoldoutCases records prioritized traversal cases confirmed on the frozen topology holdout.
	HoldoutCases int `json:"holdout_cases"`
	// TrainingPassed reports whether every observed prioritized training case cleared confirmation.
	TrainingPassed bool `json:"training_passed"`
	// HoldoutPassed reports whether every observed prioritized holdout case cleared confirmation.
	HoldoutPassed bool `json:"holdout_passed"`
	// QualificationPassed reports whether nonempty training and holdout partitions independently cleared confirmation.
	QualificationPassed bool `json:"qualification_passed"`
	// QualificationFamilies contains the independent split disposition for each concrete traversal candidate family.
	QualificationFamilies []TraversalQualificationStatus `json:"qualification_families,omitempty"`
	// Cases contains paired-arm evidence and the resulting disposition for each confirmed workload.
	Cases []ConfirmationCase `json:"cases"`
}

// createConfirmationReport loads both benchmark arms and optional A/A evidence, builds their comparison, and writes the resulting report.
func createConfirmationReport(leftPath, rightPath, aaPath, outputPath string, options ConfirmationOptions) error {
	left, err := readJSONLFile(leftPath)
	if err != nil {
		return fmt.Errorf("read left artifact: %w", err)
	}
	right, err := readJSONLFile(rightPath)
	if err != nil {
		return fmt.Errorf("read right artifact: %w", err)
	}
	var aa *AAResolutionReport
	aaSHA256 := ""
	if aaPath != "" {
		aa, aaSHA256, err = loadAAResolutionReport(aaPath)
		if err != nil {
			return fmt.Errorf("read A/A report: %w", err)
		}
	}
	report, err := buildConfirmationReport(left, right, aa, options)
	if err != nil {
		return err
	}
	report.LeftSHA256, err = fileSHA256(leftPath)
	if err != nil {
		return err
	}
	report.RightSHA256, err = fileSHA256(rightPath)
	if err != nil {
		return err
	}
	report.AAReport = aaPath
	report.AAReportSHA256 = aaSHA256
	return writeConfirmationReport(outputPath, report)
}

// buildConfirmationReport pairs comparable cases, derives confidence intervals and noise-adjusted classifications, and records why incomparable cases were skipped.
func buildConfirmationReport(left, right []CaseResult, aa *AAResolutionReport, options ConfirmationOptions) (ConfirmationReport, error) {
	if options.Confidence <= 0 || options.Confidence >= 1 {
		return ConfirmationReport{}, fmt.Errorf("confidence level must be between 0 and 1")
	}
	if options.BootstrapCount == 0 {
		options.BootstrapCount = defaultBootstrapCount
	}
	if options.BootstrapCount < 1 {
		return ConfirmationReport{}, fmt.Errorf("bootstrap count must be positive")
	}
	leftSeries, rightSeries := collectWarmSeries(left), collectWarmSeries(right)
	blockAA := sameExecutable(left, right)
	if !blockAA && len(options.CaseNames) == 0 {
		return ConfirmationReport{}, fmt.Errorf("causal confirmation requires exact primary case names")
	}
	if len(options.CaseNames) > 0 && len(options.CaseNames) <= 2 && options.Confidence < 0.975 {
		options.Confidence = 0.975
	}
	keys := make([]performanceKey, 0)
	for key := range leftSeries {
		if key.backend == ModePostgresSQL && rightSeries[key] != nil {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].dataset != keys[j].dataset {
			return keys[i].dataset < keys[j].dataset
		}
		return keys[i].name < keys[j].name
	})
	if len(options.CaseNames) > 0 {
		requested := map[string]bool{}
		for _, name := range options.CaseNames {
			requested[name] = false
		}
		filtered := keys[:0]
		for _, key := range keys {
			if _, ok := requested[key.name]; ok {
				requested[key.name] = true
				filtered = append(filtered, key)
			}
		}
		for name, found := range requested {
			if !found {
				return ConfirmationReport{}, fmt.Errorf("unknown confirmation case %q", name)
			}
		}
		keys = filtered
	}
	if len(keys) == 0 {
		return ConfirmationReport{}, fmt.Errorf("artifacts have no matched PostgreSQL warm series")
	}
	tiers := make(map[performanceKey]string, len(keys))
	splits := make(map[performanceKey]string, len(keys))
	requiresAA := false
	for _, key := range keys {
		tier, err := timingTier(key, left, right)
		if err != nil {
			return ConfirmationReport{}, err
		}
		tiers[key] = tier
		split, err := qualificationSplit(key, left, right)
		if err != nil {
			return ConfirmationReport{}, err
		}
		splits[key] = split
		if !blockAA && tier != "stress" && promotionTimingSplit(split) {
			requiresAA = true
		}
	}
	if requiresAA {
		if err := validateAAResolutionEvidence(aa, left, options.Confidence); err != nil {
			return ConfirmationReport{}, fmt.Errorf("left-arm A/A evidence: %w", err)
		}
		if err := validateAAResolutionEvidence(aa, right, options.Confidence); err != nil {
			return ConfirmationReport{}, fmt.Errorf("right-arm A/A evidence: %w", err)
		}
	} else if aa != nil {
		if err := validateAAResolutionEvidence(aa, left, options.Confidence); err != nil {
			return ConfirmationReport{}, err
		}
	}

	report := ConfirmationReport{
		Version:    confirmationReportVersion,
		Kind:       "causal_confirmation",
		Seed:       options.Seed,
		Confidence: options.Confidence,
	}
	report.LeftArm = artifactArm(left)
	report.RightArm = artifactArm(right)
	if blockAA {
		report.Kind = "block_reload_aa"
	}
	report.PromotionEligible = !blockAA && requiresAA
	report.TrainingPassed = true
	report.HoldoutPassed = true
	qualification := map[string]*TraversalQualificationStatus{}
	gateOptions := PerfGateOptions{
		Seed:           options.Seed,
		Confidence:     options.Confidence,
		BootstrapCount: options.BootstrapCount,
	}
	for idx, key := range keys {
		leftRounds, rightRounds := matchedRounds(leftSeries[key], rightSeries[key])
		timingGated := tiers[key] != "stress" && promotionTimingSplit(splits[key]) && !blockAA
		if timingGated && (len(leftRounds) < 10 || len(leftRounds) > 20) {
			return ConfirmationReport{}, fmt.Errorf("%s/%s requires 10-20 matched rounds, got %d", key.dataset, key.name, len(leftRounds))
		}
		for _, round := range sortedRounds(leftRounds) {
			if timingGated && (len(leftRounds[round]) < 50 || len(rightRounds[round]) < 50) {
				return ConfirmationReport{}, fmt.Errorf("%s/%s round %d requires at least 50 warm samples per arm", key.dataset, key.name, round)
			}
		}
		if timingGated {
			if err := validatePairedOrderEvidence(left, right, key, sortedRounds(leftRounds), 20); err != nil {
				return ConfirmationReport{}, fmt.Errorf("invalid confirmation evidence: %w", err)
			}
		}
		seed := options.Seed + int64(idx)*7919
		p50Ratio := bootstrapRoundMedianRatio(leftRounds, rightRounds, seed, gateOptions)
		p50Change := negateDurationInterval(bootstrapRoundMedianSaving(leftRounds, rightRounds, seed+1, gateOptions))
		p95Ratio := bootstrapStratifiedP95Ratio(leftRounds, rightRounds, seed+2, gateOptions)
		p95Change := bootstrapStratifiedQuantileChange(leftRounds, rightRounds, 0.95, seed+3, gateOptions)
		p50NoiseRatio, p50NoiseAbsolute := minimumTimingNoiseRatio, minimumTimingNoiseAbsolute
		p95NoiseRatio, p95NoiseAbsolute := minimumTimingNoiseRatio, minimumTimingNoiseAbsolute
		if aa != nil {
			if ratio, absolute, floorErr := aaTimingFloor(aa, key, false, 0); floorErr == nil {
				p50NoiseRatio, p50NoiseAbsolute = ratio, absolute
			} else if timingGated {
				return ConfirmationReport{}, floorErr
			}
			if ratio, absolute, floorErr := aaTimingFloor(aa, key, true, 0); floorErr == nil {
				p95NoiseRatio, p95NoiseAbsolute = ratio, absolute
			} else if timingGated {
				return ConfirmationReport{}, floorErr
			}
		}
		comparable, reasons := confirmationComparable(left, right, key)
		entry := ConfirmationCase{
			Dataset:                   key.dataset,
			Name:                      key.name,
			Backend:                   key.backend,
			Tier:                      tiers[key],
			QualificationSplit:        splits[key],
			TimingGated:               timingGated,
			MatchedRounds:             len(leftRounds),
			LeftSamples:               sampleCount(leftRounds),
			RightSamples:              sampleCount(rightRounds),
			Comparable:                comparable,
			Comparability:             reasons,
			RightRuntimeReceiptChains: caseRuntimeReceiptChains(right, key),
			P50:                       classifyConfirmationMetric(p50Ratio, p50Change, p50NoiseRatio, p50NoiseAbsolute),
			P95:                       classifyConfirmationMetric(p95Ratio, p95Change, p95NoiseRatio, p95NoiseAbsolute),
		}
		entry.Disposition = entry.P95.Classification
		if tiers[key] == "stress" {
			entry.Disposition = "stress_diagnostic"
		}
		if splits[key] == "diagnostic" {
			entry.Disposition = "qualification_diagnostic"
		}
		if !comparable {
			entry.Disposition = "fingerprint_mismatch"
		}
		if entry.TimingGated && (!entry.Comparable || entry.P95.Classification != "cleared_non_inferior") {
			report.PromotionEligible = false
		}
		if prioritizedTraversalKey(key, left, right) && entry.TimingGated {
			report.QualificationRequired = true
			passed := entry.Comparable && entry.P95.Classification == "cleared_non_inferior"
			family := traversalQualificationFamily(key, left, right)
			status := qualification[family]
			if status == nil {
				status = &TraversalQualificationStatus{Family: family, TrainingPassed: true, HoldoutPassed: true}
				qualification[family] = status
			}
			switch entry.QualificationSplit {
			case "training":
				report.TrainingCases++
				report.TrainingPassed = report.TrainingPassed && passed
				status.TrainingCases++
				status.TrainingPassed = status.TrainingPassed && passed
			case "holdout":
				report.HoldoutCases++
				report.HoldoutPassed = report.HoldoutPassed && passed
				status.HoldoutCases++
				status.HoldoutPassed = status.HoldoutPassed && passed
			}
		}
		report.Cases = append(report.Cases, entry)
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
		report.PromotionEligible = report.PromotionEligible && report.QualificationPassed
	} else {
		report.TrainingPassed = false
		report.HoldoutPassed = false
	}
	return report, nil
}

// classifyConfirmationMetric labels a confidence interval as regression, improvement, or inconclusive only when both relative and absolute noise floors are crossed.
func classifyConfirmationMetric(ratio RatioInterval, change DurationInterval, noiseRatio float64, noiseAbsolute time.Duration) ConfirmationMetric {
	classification := "inconclusive"
	if ratio.Lower > 1+noiseRatio && change.Lower > noiseAbsolute {
		classification = "confirmed"
	}
	if ratio.Upper <= 1+noiseRatio && change.Upper <= noiseAbsolute {
		classification = "cleared_non_inferior"
	}
	return ConfirmationMetric{
		Ratio:          ratio,
		AbsoluteChange: change,
		NoiseRatio:     noiseRatio,
		NoiseAbsolute:  noiseAbsolute,
		Classification: classification,
	}
}

// bootstrapStratifiedQuantileChange estimates a quantile delta and confidence interval by resampling within matching benchmark rounds.
func bootstrapStratifiedQuantileChange(left, right roundSamples, probability float64, seed int64, options PerfGateOptions) DurationInterval {
	rounds := sortedRounds(left)
	estimate := durationQuantile(flattenSamples(right, rounds), probability) - durationQuantile(flattenSamples(left, rounds), probability)
	rng := rand.New(rand.NewSource(seed)) // #nosec G404 -- deterministic statistical resampling
	changes := make([]float64, options.BootstrapCount)
	for idx := range changes {
		var sampledLeft, sampledRight []time.Duration
		for _, round := range rounds {
			sampledLeft = append(sampledLeft, resampleDurations(rng, left[round])...)
			sampledRight = append(sampledRight, resampleDurations(rng, right[round])...)
		}
		changes[idx] = durationQuantile(sampledRight, probability) - durationQuantile(sampledLeft, probability)
	}
	interval := confidenceInterval(estimate, changes, options.Confidence)
	return DurationInterval{
		Estimate: time.Duration(interval.Estimate),
		Lower:    time.Duration(interval.Lower),
		Upper:    time.Duration(interval.Upper),
	}
}

// negateDurationInterval reverses interval direction and swaps its bounds so left/right arm normalization preserves a valid ordered interval.
func negateDurationInterval(value DurationInterval) DurationInterval {
	return DurationInterval{
		Estimate: -value.Estimate,
		Lower:    -value.Upper,
		Upper:    -value.Lower,
	}
}

// confirmationComparable compares two confirmation records and returns every reason they cannot be paired.
func confirmationComparable(left, right []CaseResult, key performanceKey) (bool, []string) {
	leftRecords := matchingRecords(left, key)
	rightRecords := matchingRecords(right, key)
	var reasons []string
	if len(leftRecords) == 0 || len(rightRecords) == 0 {
		reasons = append(reasons, "missing record")
		return false, reasons
	}
	leftRecord, rightRecord := leftRecords[0], rightRecords[0]
	reasons = append(reasons, confirmationArmConsistency(leftRecords)...)
	reasons = append(reasons, confirmationArmConsistency(rightRecords)...)
	if leftRecord.Status != StatusOK || rightRecord.Status != StatusOK {
		reasons = append(reasons, "non-ok status")
	}
	if leftRecord.Fixture == nil || rightRecord.Fixture == nil || leftRecord.Fixture.Checksum != rightRecord.Fixture.Checksum {
		reasons = append(reasons, "fixture checksum differs")
	}
	if fmt.Sprint(leftRecord.ObservedRows) != fmt.Sprint(rightRecord.ObservedRows) {
		reasons = append(reasons, "exact observations differ")
	}
	if leftRecord.RowCount != rightRecord.RowCount {
		reasons = append(reasons, "row count differs")
	}
	if !comparablePostgresEnvironment(leftRecord.PostgresEnvironment, rightRecord.PostgresEnvironment) {
		reasons = append(reasons, "PostgreSQL settings or relation sizes differ")
	}
	return len(reasons) == 0, uniqueStrings(reasons)
}

// confirmationArmConsistency reports within-arm drift in environment, executable, and normalized PostgreSQL plan shape.
func confirmationArmConsistency(records []CaseResult) []string {
	if len(records) == 0 {
		return []string{"missing record"}
	}

	baseline := records[0]
	var reasons []string
	for _, record := range records[1:] {
		if record.Status != StatusOK {
			reasons = append(reasons, "non-ok status")
		}
		if record.SQLFingerprint != baseline.SQLFingerprint {
			reasons = append(reasons, "SQL fingerprint changes within arm")
		}
		if record.Fixture == nil || baseline.Fixture == nil || record.Fixture.Checksum != baseline.Fixture.Checksum {
			reasons = append(reasons, "fixture checksum differs")
		}
		if fmt.Sprint(record.ObservedRows) != fmt.Sprint(baseline.ObservedRows) {
			reasons = append(reasons, "exact observations differ")
		}
		if record.RowCount != baseline.RowCount {
			reasons = append(reasons, "row count differs")
		}
		if !comparablePostgresEnvironment(baseline.PostgresEnvironment, record.PostgresEnvironment) {
			reasons = append(reasons, "PostgreSQL settings or relation sizes differ")
		}
		if postgresPlanShapeSHA256(record.PostgresPlan) != postgresPlanShapeSHA256(baseline.PostgresPlan) {
			reasons = append(reasons, "intended plan shape changes within arm")
		}
	}
	return reasons
}

var (
	// volatilePlanDetails matches planner cost and runtime annotations that do not define structural plan shape.
	volatilePlanDetails = regexp.MustCompile(`\s+\((?:cost|actual)[^)]*\)`)

	// volatilePlanIDs matches generated bigint constants so dataset-specific IDs do not perturb plan-shape hashes.
	volatilePlanIDs = regexp.MustCompile(`'[0-9]+'::bigint`)

	// volatilePlanLine matches resource and timing summary lines excluded from structural plan-shape hashes.
	volatilePlanLine = regexp.MustCompile(`^(?:Buffers|Planning Time|Execution Time):`)
)

// postgresPlanShapeSHA256 hashes structural EXPLAIN lines after removing costs, runtime counters, transient IDs, and timing details; confirmation compares plan shape without treating volatile measurements as structural changes.
func postgresPlanShapeSHA256(plan []string) string {
	digest := sha256.New()
	for _, line := range plan {
		line = volatilePlanDetails.ReplaceAllString(line, "")
		line = volatilePlanIDs.ReplaceAllString(line, "'$id'::bigint")
		line = strings.TrimSpace(line)
		if line == "" || volatilePlanLine.MatchString(line) {
			continue
		}
		fmt.Fprintln(digest, line)
	}
	return hex.EncodeToString(digest.Sum(nil))
}

// matchingRecords selects successful measured records for one dataset, case, backend, and executor identity.
func matchingRecords(records []CaseResult, key performanceKey) []CaseResult {
	var matched []CaseResult
	for _, record := range records {
		if record.Dataset == key.dataset && record.Name == key.name && record.ExecutionMode == key.backend {
			matched = append(matched, record)
		}
	}
	return matched
}

// comparablePostgresEnvironment requires server version and normalized settings to match while tolerating absent environment metadata on both arms.
func comparablePostgresEnvironment(left, right *PostgresEnvironment) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.PlanCacheMode == right.PlanCacheMode && left.TransactionIsolation == right.TransactionIsolation && left.WorkMem == right.WorkMem && left.TempFileLimit == right.TempFileLimit &&
		left.GraphPartitionCount == right.GraphPartitionCount && left.NodeRelationBytes == right.NodeRelationBytes && left.EdgeRelationBytes == right.EdgeRelationBytes

}

// uniqueStrings removes duplicate diagnostic reasons while preserving their first-seen order.
func uniqueStrings(values []string) []string {
	seen := map[string]struct{}{}
	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, found := seen[value]; found {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

// artifactArm returns the first recorded benchmark arm label, or "unknown" when an artifact lacks environment metadata.
func artifactArm(records []CaseResult) string {
	for _, record := range records {
		if record.Environment != nil {
			return record.Environment.Arm
		}
	}
	return "unknown"
}

// sameExecutable identifies a true block/reload A/A treatment. A shared
// executable alone is insufficient because one GraphBench binary can emit
// different forced executors and SQL statements.
func sameExecutable(left, right []CaseResult) bool {
	leftIdentity := effectiveTreatmentIdentity(left)
	return leftIdentity != "" && leftIdentity == effectiveTreatmentIdentity(right)
}

func effectiveTreatmentIdentity(records []CaseResult) string {
	if len(records) == 0 || records[0].Environment == nil || records[0].Environment.BinarySHA256 == "" {
		return ""
	}
	identity := []string{"binary=" + records[0].Environment.BinarySHA256}
	for _, argument := range records[0].Environment.Invocation {
		if strings.Contains(argument, "postgres-force-shortest-executor") ||
			strings.Contains(argument, "postgres-force-expansion-strategy") ||
			strings.Contains(argument, "postgres-expansion-orientation") ||
			strings.Contains(argument, "reference-arm") {
			identity = append(identity, "option="+argument)
		}
	}
	fingerprints := make([]string, 0, len(records))
	for _, record := range records {
		fingerprints = append(fingerprints, record.Dataset+"/"+record.Name+"="+record.SQLFingerprint)
	}
	sort.Strings(fingerprints)
	identity = append(identity, fingerprints...)
	digest := sha256.Sum256([]byte(strings.Join(identity, "\n")))
	return hex.EncodeToString(digest[:])
}

// writeConfirmationReport emits indented JSON to stdout or atomically replaces the requested output file.
func writeConfirmationReport(path string, report ConfirmationReport) (err error) {
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
