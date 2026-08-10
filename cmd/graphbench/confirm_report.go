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

const confirmationReportVersion = 1

type ConfirmationOptions struct {
	Seed           int64
	Confidence     float64
	BootstrapCount int
	CaseNames      []string
}

type ConfirmationMetric struct {
	Ratio          RatioInterval    `json:"ratio"`
	AbsoluteChange DurationInterval `json:"absolute_change"`
	NoiseRatio     float64          `json:"noise_ratio"`
	NoiseAbsolute  time.Duration    `json:"noise_absolute"`
	Classification string           `json:"classification"`
}

type ConfirmationCase struct {
	Dataset       string             `json:"dataset"`
	Name          string             `json:"name"`
	Backend       ExecutionMode      `json:"backend"`
	MatchedRounds int                `json:"matched_rounds"`
	LeftSamples   int                `json:"left_samples"`
	RightSamples  int                `json:"right_samples"`
	Comparable    bool               `json:"comparable"`
	Comparability []string           `json:"comparability_reasons,omitempty"`
	P50           ConfirmationMetric `json:"p50"`
	P95           ConfirmationMetric `json:"p95"`
	Disposition   string             `json:"disposition"`
}

type ConfirmationReport struct {
	Version     int                `json:"version"`
	Kind        string             `json:"kind"`
	Seed        int64              `json:"seed"`
	Confidence  float64            `json:"confidence_level"`
	LeftArm     string             `json:"left_arm"`
	RightArm    string             `json:"right_arm"`
	LeftSHA256  string             `json:"left_sha256"`
	RightSHA256 string             `json:"right_sha256"`
	AAReport    string             `json:"aa_report,omitempty"`
	Cases       []ConfirmationCase `json:"cases"`
}

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
	if aaPath != "" {
		raw, err := os.ReadFile(aaPath)
		if err != nil {
			return fmt.Errorf("read A/A report: %w", err)
		}
		aa = &AAResolutionReport{}
		if err := json.Unmarshal(raw, aa); err != nil {
			return fmt.Errorf("decode A/A report: %w", err)
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
	return writeConfirmationReport(outputPath, report)
}

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
	aaReports := []*AAResolutionReport{}
	for _, artifact := range [][]CaseResult{left, right} {
		within, err := buildAAResolutionReport(artifact, PerfGateOptions{
			Seed:           options.Seed,
			Confidence:     options.Confidence,
			BootstrapCount: options.BootstrapCount,
		})
		if err != nil {
			return ConfirmationReport{}, fmt.Errorf("calculate within-run A/A: %w", err)
		}
		aaReports = append(aaReports, &within)
	}
	if aa != nil {
		aaReports = append(aaReports, aa)
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
	gateOptions := PerfGateOptions{
		Seed:           options.Seed,
		Confidence:     options.Confidence,
		BootstrapCount: options.BootstrapCount,
	}
	for idx, key := range keys {
		leftRounds, rightRounds := matchedRounds(leftSeries[key], rightSeries[key])
		if len(leftRounds) < 10 || len(leftRounds) > 20 {
			return ConfirmationReport{}, fmt.Errorf("%s/%s requires 10-20 matched rounds, got %d", key.dataset, key.name, len(leftRounds))
		}
		for _, round := range sortedRounds(leftRounds) {
			if len(leftRounds[round]) < 50 || len(rightRounds[round]) < 50 {
				return ConfirmationReport{}, fmt.Errorf("%s/%s round %d requires at least 50 warm samples per arm", key.dataset, key.name, round)
			}
		}
		seed := options.Seed + int64(idx)*7919
		p50Ratio := bootstrapRoundMedianRatio(leftRounds, rightRounds, seed, gateOptions)
		p50Change := negateDurationInterval(bootstrapRoundMedianSaving(leftRounds, rightRounds, seed+1, gateOptions))
		p95Ratio := bootstrapStratifiedP95Ratio(leftRounds, rightRounds, seed+2, gateOptions)
		p95Change := bootstrapStratifiedQuantileChange(leftRounds, rightRounds, 0.95, seed+3, gateOptions)
		p50NoiseRatio, p50NoiseAbsolute := confirmationNoise(aaReports, key, false)
		p95NoiseRatio, p95NoiseAbsolute := confirmationNoise(aaReports, key, true)
		comparable, reasons := confirmationComparable(left, right, key)
		entry := ConfirmationCase{
			Dataset:       key.dataset,
			Name:          key.name,
			Backend:       key.backend,
			MatchedRounds: len(leftRounds),
			LeftSamples:   sampleCount(leftRounds),
			RightSamples:  sampleCount(rightRounds),
			Comparable:    comparable,
			Comparability: reasons,
			P50:           classifyConfirmationMetric(p50Ratio, p50Change, p50NoiseRatio, p50NoiseAbsolute),
			P95:           classifyConfirmationMetric(p95Ratio, p95Change, p95NoiseRatio, p95NoiseAbsolute),
		}
		entry.Disposition = entry.P95.Classification
		if !comparable {
			entry.Disposition = "fingerprint_mismatch"
		}
		report.Cases = append(report.Cases, entry)
	}
	return report, nil
}

func confirmationNoise(reports []*AAResolutionReport, key performanceKey, p95 bool) (float64, time.Duration) {
	ratio, absolute := 0.05, 100*time.Microsecond
	for _, aa := range reports {
		if aa == nil {
			continue
		}
		for _, entry := range aa.Cases {
			if entry.Dataset != key.dataset || entry.Name != key.name || entry.Backend != key.backend {
				continue
			}
			metric := entry.P50
			if p95 {
				metric = entry.P95
			}
			if metric.RatioResolution > ratio {
				ratio = metric.RatioResolution
			}
			if metric.AbsoluteResolution > absolute {
				absolute = metric.AbsoluteResolution
			}
		}
	}
	return ratio, absolute
}

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

func negateDurationInterval(value DurationInterval) DurationInterval {
	return DurationInterval{
		Estimate: -value.Estimate,
		Lower:    -value.Upper,
		Upper:    -value.Lower,
	}
}

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
	volatilePlanDetails = regexp.MustCompile(`\s+\((?:cost|actual)[^)]*\)`)
	volatilePlanIDs     = regexp.MustCompile(`'[0-9]+'::bigint`)
	volatilePlanLine    = regexp.MustCompile(`^(?:Buffers|Planning Time|Execution Time):`)
)

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

func matchingRecords(records []CaseResult, key performanceKey) []CaseResult {
	var matched []CaseResult
	for _, record := range records {
		if record.Dataset == key.dataset && record.Name == key.name && record.ExecutionMode == key.backend {
			matched = append(matched, record)
		}
	}
	return matched
}

func comparablePostgresEnvironment(left, right *PostgresEnvironment) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.PlanCacheMode == right.PlanCacheMode && left.WorkMem == right.WorkMem && left.TempFileLimit == right.TempFileLimit &&
		left.GraphPartitionCount == right.GraphPartitionCount && left.NodeRelationBytes == right.NodeRelationBytes && left.EdgeRelationBytes == right.EdgeRelationBytes

}

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

func artifactArm(records []CaseResult) string {
	for _, record := range records {
		if record.Environment != nil {
			return record.Environment.Arm
		}
	}
	return "unknown"
}

func sameExecutable(left, right []CaseResult) bool {
	var leftHash, rightHash string
	for _, record := range left {
		if record.Environment != nil {
			leftHash = record.Environment.BinarySHA256
			break
		}
	}
	for _, record := range right {
		if record.Environment != nil {
			rightHash = record.Environment.BinarySHA256
			break
		}
	}
	return leftHash != "" && leftHash == rightHash
}

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
