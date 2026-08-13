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
	"math"
	"os"
	"sort"
	"time"
)

// aaReportVersion identifies the serialized schema revision for A/A report.
const aaReportVersion = 3

// AAMetricResolution captures relative and absolute within-arm noise for one latency quantile.
type AAMetricResolution struct {
	// Ratio reports the candidate-to-baseline latency ratio.
	Ratio RatioInterval `json:"ratio"`
	// RatioResolution records the relative A/A noise floor for ratio classification.
	RatioResolution float64 `json:"ratio_resolution"`
	// AbsoluteChange reports the paired candidate-minus-baseline A/A duration interval.
	AbsoluteChange DurationInterval `json:"absolute_change"`
	// AbsoluteResolution records the absolute A/A noise floor used for materiality decisions.
	AbsoluteResolution time.Duration `json:"absolute_resolution"`
}

// AAResolutionCase reports matched sample counts and median and P95 noise floors for one case.
type AAResolutionCase struct {
	// Dataset identifies the fixture dataset.
	Dataset string `json:"dataset"`
	// Name identifies the case or record within its dataset.
	Name string `json:"name"`
	// Backend identifies the execution backend.
	Backend ExecutionMode `json:"backend"`
	// WorkloadSHA256 binds the resolution to the exact logical workload declaration.
	WorkloadSHA256 string `json:"workload_sha256"`
	// Rounds records the number of independent measurement rounds.
	Rounds int `json:"rounds"`
	// SamplesPerArm records matched timing samples available from each A/A arm.
	SamplesPerArm int `json:"samples_per_arm"`
	// P50 records relative and absolute A/A noise at median latency.
	P50 AAMetricResolution `json:"p50"`
	// P95 records relative and absolute A/A noise at 95th-percentile latency.
	P95 AAMetricResolution `json:"p95"`
	// P99Gated reports whether the sample count is sufficient to enforce the P99 noise threshold.
	P99Gated bool `json:"p99_gated"`
	// P99Reason explains why P99 gating was applied or omitted.
	P99Reason string `json:"p99_reason,omitempty"`
}

// AAResolutionReport contains per-case A/A noise floors and the artifact identity used to derive them.
type AAResolutionReport struct {
	// Version identifies the serialized schema revision.
	Version int `json:"version"`
	// Seed controls deterministic random sampling.
	Seed int64 `json:"seed"`
	// Confidence sets the confidence level used for statistical intervals.
	Confidence float64 `json:"confidence_level"`
	// ArtifactSHA256 identifies the exact input artifact summarized by the report.
	ArtifactSHA256 string `json:"artifact_sha256"`
	// HostFingerprint identifies the host whose timing noise this report measures.
	HostFingerprint string `json:"host_fingerprint"`
	// MinimumRounds records the independent-round floor enforced by this report.
	MinimumRounds int `json:"minimum_rounds"`
	// MinimumSamplesPerArmPerRound records the sample floor enforced after splitting A/A arms.
	MinimumSamplesPerArmPerRound int `json:"minimum_samples_per_arm_per_round"`
	// OrderBalanced reports that the two explicitly executed A/A arms have complementary balanced first position.
	OrderBalanced bool `json:"order_balanced"`
	// MinimumP99SamplesPerArm sets the per-arm sample floor required before P99 gating.
	MinimumP99SamplesPerArm int `json:"minimum_p99_samples_per_arm"`
	// Cases contains per-workload A/A noise estimates and resolution thresholds.
	Cases []AAResolutionCase `json:"cases"`
}

// buildAAResolutionReport splits matched A/A samples and estimates per-case median and P95 noise floors.
func buildAAResolutionReport(records []CaseResult, options PerfGateOptions) (AAResolutionReport, error) {
	if options.Confidence <= 0 || options.Confidence >= 1 {
		return AAResolutionReport{}, fmt.Errorf("confidence level must be between 0 and 1")
	}
	if options.BootstrapCount == 0 {
		options.BootstrapCount = defaultBootstrapCount
	}
	if options.BootstrapCount < 1 {
		return AAResolutionReport{}, fmt.Errorf("bootstrap count must be positive")
	}
	hostFingerprint, err := artifactHostFingerprint(records)
	if err != nil {
		return AAResolutionReport{}, err
	}

	all, err := collectExplicitAASeries(records)
	if err != nil {
		return AAResolutionReport{}, err
	}
	keys := make([]performanceKey, 0, len(all))
	for key := range all {
		if key.backend == ModePostgresSQL {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].dataset != keys[j].dataset {
			return keys[i].dataset < keys[j].dataset
		}
		return keys[i].name < keys[j].name
	})
	if len(keys) == 0 {
		return AAResolutionReport{}, fmt.Errorf("artifact has no successful PostgreSQL warm samples")
	}

	report := AAResolutionReport{
		Version:                      aaReportVersion,
		Seed:                         options.Seed,
		Confidence:                   options.Confidence,
		HostFingerprint:              hostFingerprint,
		MinimumRounds:                minimumGateRounds,
		MinimumSamplesPerArmPerRound: 10,
		OrderBalanced:                true,
		MinimumP99SamplesPerArm:      10_000,
	}
	for idx, key := range keys {
		var (
			armA, armB = all[key][0], all[key][1]
			seed       = options.Seed + int64(idx)*7919
		)

		armA, armB = matchedRounds(armA, armB)
		if len(armA) < minimumGateRounds {
			return AAResolutionReport{}, fmt.Errorf("%s/%s requires at least %d A/A rounds, got %d", key.dataset, key.name, minimumGateRounds, len(armA))
		}
		for _, round := range sortedRounds(armA) {
			if len(armA[round]) < report.MinimumSamplesPerArmPerRound || len(armB[round]) < report.MinimumSamplesPerArmPerRound {
				return AAResolutionReport{}, fmt.Errorf("%s/%s round %d requires at least %d samples per A/A arm, got %d/%d", key.dataset, key.name, round, report.MinimumSamplesPerArmPerRound, len(armA[round]), len(armB[round]))
			}
		}

		var (
			p50        = bootstrapRoundMedianRatio(armA, armB, seed, options)
			p95        = bootstrapStratifiedP95Ratio(armA, armB, seed+1, options)
			p50Change  = negateDurationInterval(bootstrapRoundMedianSaving(armA, armB, seed+2, options))
			p95Change  = bootstrapStratifiedQuantileChange(armA, armB, 0.95, seed+3, options)
			armSamples = min(sampleCount(armA), sampleCount(armB))
		)

		workloadSHA256, err := workloadSHA256ForKey(records, key)
		if err != nil {
			return AAResolutionReport{}, err
		}
		entry := AAResolutionCase{
			Dataset:        key.dataset,
			Name:           key.name,
			Backend:        key.backend,
			WorkloadSHA256: workloadSHA256,
			Rounds:         len(armA),
			SamplesPerArm:  armSamples,
			P50:            aaMetricResolution(p50, p50Change),
			P95:            aaMetricResolution(p95, p95Change),
			P99Gated:       armSamples >= 10_000,
		}
		if !entry.P99Gated {
			entry.P99Reason = fmt.Sprintf("diagnostic only: need at least 10000 samples per A/A arm, got %d", armSamples)
		}

		report.Cases = append(report.Cases, entry)
	}

	return report, nil
}

// collectExplicitAASeries requires two independently executed arms with
// identical SQL and balanced block order. Splitting one timing stream into
// synthetic labels understates reload, connection, and first-order carryover
// noise and is therefore deliberately refused by the promotion-grade report.
func collectExplicitAASeries(records []CaseResult) (map[performanceKey][2]roundSamples, error) {
	type armIdentity struct {
		SQLFingerprint string
		WorkloadSHA256 string
	}
	type armSeries struct {
		identity armIdentity
		samples  roundSamples
		orders   map[int]int
		blocks   map[int]int
		runUUIDs map[int]string
	}

	byKey := map[performanceKey]map[string]*armSeries{}
	for _, record := range records {
		if record.Status != StatusOK || record.ExecutionMode != ModePostgresSQL {
			continue
		}
		key := performanceKey{dataset: record.Dataset, name: record.Name, backend: record.ExecutionMode}
		for _, sample := range record.Stats.Samples {
			if sample.Classification != "warm" || sample.Duration <= 0 {
				continue
			}
			if sample.Round < 1 || sample.Block < 1 || sample.Arm == "" || sample.Arm == "unlabeled" || sample.ArmOrder < 1 || sample.RunUUID == "" {
				return nil, fmt.Errorf("%s/%s has A/A sample without explicit round, block, arm, order, and run UUID", key.dataset, key.name)
			}
			arms := byKey[key]
			if arms == nil {
				arms = map[string]*armSeries{}
				byKey[key] = arms
			}
			arm := arms[sample.Arm]
			if arm == nil {
				arm = &armSeries{
					identity: armIdentity{SQLFingerprint: record.SQLFingerprint, WorkloadSHA256: record.WorkloadSHA256},
					samples:  roundSamples{}, orders: map[int]int{}, blocks: map[int]int{}, runUUIDs: map[int]string{},
				}
				arms[sample.Arm] = arm
			}
			identity := armIdentity{SQLFingerprint: record.SQLFingerprint, WorkloadSHA256: record.WorkloadSHA256}
			if arm.identity != identity || identity.SQLFingerprint == "" || identity.WorkloadSHA256 == "" {
				return nil, fmt.Errorf("%s/%s arm %q changes or lacks executable/workload identity", key.dataset, key.name, sample.Arm)
			}
			if prior, found := arm.orders[sample.Round]; found && prior != sample.ArmOrder {
				return nil, fmt.Errorf("%s/%s arm %q round %d changes order", key.dataset, key.name, sample.Arm, sample.Round)
			}
			if prior, found := arm.blocks[sample.Round]; found && prior != sample.Block {
				return nil, fmt.Errorf("%s/%s arm %q round %d changes block", key.dataset, key.name, sample.Arm, sample.Round)
			}
			if prior, found := arm.runUUIDs[sample.Round]; found && prior != sample.RunUUID {
				return nil, fmt.Errorf("%s/%s arm %q round %d changes run UUID", key.dataset, key.name, sample.Arm, sample.Round)
			}
			arm.orders[sample.Round] = sample.ArmOrder
			arm.blocks[sample.Round] = sample.Block
			arm.runUUIDs[sample.Round] = sample.RunUUID
			arm.samples[sample.Round] = append(arm.samples[sample.Round], sample.Duration)
		}
	}

	result := map[performanceKey][2]roundSamples{}
	for key, arms := range byKey {
		if len(arms) != 2 {
			return nil, fmt.Errorf("%s/%s requires exactly two explicit A/A arms, got %d", key.dataset, key.name, len(arms))
		}
		names := make([]string, 0, 2)
		for name := range arms {
			names = append(names, name)
		}
		sort.Strings(names)
		left, right := arms[names[0]], arms[names[1]]
		if left.identity != right.identity {
			return nil, fmt.Errorf("%s/%s A/A arms do not have identical SQL and workload identities", key.dataset, key.name)
		}
		leftSamples, rightSamples := matchedRounds(left.samples, right.samples)
		leftFirst := 0
		for _, round := range sortedRounds(leftSamples) {
			if left.blocks[round] != right.blocks[round] || left.runUUIDs[round] != right.runUUIDs[round] {
				return nil, fmt.Errorf("%s/%s round %d has mismatched A/A block or run identity", key.dataset, key.name, round)
			}
			if !((left.orders[round] == 1 && right.orders[round] == 2) || (left.orders[round] == 2 && right.orders[round] == 1)) {
				return nil, fmt.Errorf("%s/%s round %d lacks a complete two-arm A/A order", key.dataset, key.name, round)
			}
			if left.orders[round] == 1 {
				leftFirst++
			}
		}
		if rightFirst := len(leftSamples) - leftFirst; leftFirst-rightFirst > 1 || rightFirst-leftFirst > 1 {
			return nil, fmt.Errorf("%s/%s A/A order is not balanced: %d/%d", key.dataset, key.name, leftFirst, rightFirst)
		}
		result[key] = [2]roundSamples{leftSamples, rightSamples}
	}
	return result, nil
}

// aaMetricResolution returns the larger relative and absolute confidence-bound deviations observed between paired A/A samples.
func aaMetricResolution(interval RatioInterval, absoluteChange DurationInterval) AAMetricResolution {
	resolution := math.Max(math.Abs(1-interval.Lower), math.Abs(interval.Upper-1))
	return AAMetricResolution{
		Ratio:              interval,
		RatioResolution:    resolution,
		AbsoluteChange:     absoluteChange,
		AbsoluteResolution: max(absDuration(absoluteChange.Lower), absDuration(absoluteChange.Upper)),
	}
}

// writeAAResolutionReport writes an A/A resolution report as indented JSON.
func writeAAResolutionReport(path string, report AAResolutionReport) (err error) {
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

// createAAResolutionReport loads an artifact, builds its A/A resolution report, and writes the result.
func createAAResolutionReport(artifactPath, outputPath string, options PerfGateOptions) error {
	records, err := readJSONLFile(artifactPath)
	if err != nil {
		return err
	}
	report, err := buildAAResolutionReport(records, options)
	if err != nil {
		return err
	}
	report.ArtifactSHA256, err = fileSHA256(artifactPath)
	if err != nil {
		return err
	}
	return writeAAResolutionReport(outputPath, report)
}

// loadAAResolutionReport decodes a host A/A report and returns the report file's checksum.
func loadAAResolutionReport(path string) (*AAResolutionReport, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	report := &AAResolutionReport{}
	if err := json.Unmarshal(raw, report); err != nil {
		return nil, "", fmt.Errorf("decode A/A report: %w", err)
	}
	digest := sha256.Sum256(raw)
	return report, hex.EncodeToString(digest[:]), nil
}
