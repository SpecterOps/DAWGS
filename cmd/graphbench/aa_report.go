// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"time"
)

// aaReportVersion identifies the serialized schema revision for A/A report.
const aaReportVersion = 1

// AAMetricResolution captures relative and absolute within-arm noise for one latency quantile.
type AAMetricResolution struct {
	// Ratio reports the candidate-to-baseline latency ratio.
	Ratio RatioInterval `json:"ratio"`
	// RatioResolution records the relative A/A noise floor for ratio classification.
	RatioResolution float64 `json:"ratio_resolution"`
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

	all := collectWarmSeries(records)
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
		Version:                 aaReportVersion,
		Seed:                    options.Seed,
		Confidence:              options.Confidence,
		MinimumP99SamplesPerArm: 10_000,
	}
	for idx, key := range keys {
		var (
			armA, armB = splitAASeries(all[key])
			seed       = options.Seed + int64(idx)*7919
		)

		armA, armB = matchedRounds(armA, armB)
		if len(armA) == 0 {
			return AAResolutionReport{}, fmt.Errorf("%s/%s has fewer than two warm samples in every round", key.dataset, key.name)
		}

		var (
			p50        = bootstrapRoundMedianRatio(armA, armB, seed, options)
			p95        = bootstrapStratifiedP95Ratio(armA, armB, seed+1, options)
			armSamples = min(sampleCount(armA), sampleCount(armB))
		)

		entry := AAResolutionCase{
			Dataset:       key.dataset,
			Name:          key.name,
			Backend:       key.backend,
			Rounds:        len(armA),
			SamplesPerArm: armSamples,
			P50:           aaMetricResolution(p50, durationQuantile(flattenSamples(armA, sortedRounds(armA)), 0.50)),
			P95:           aaMetricResolution(p95, durationQuantile(flattenSamples(armA, sortedRounds(armA)), 0.95)),
			P99Gated:      armSamples >= 10_000,
		}
		if !entry.P99Gated {
			entry.P99Reason = fmt.Sprintf("diagnostic only: need at least 10000 samples per A/A arm, got %d", armSamples)
		}

		report.Cases = append(report.Cases, entry)
	}

	return report, nil
}

// splitAASeries separates A/A samples into the first and second measurements for each matched block.
func splitAASeries(samples roundSamples) (roundSamples, roundSamples) {
	var (
		armA = roundSamples{}
		armB = roundSamples{}
	)

	for round, values := range samples {
		for idx, value := range values {
			if idx%2 == 0 {
				armA[round] = append(armA[round], value)
			} else {
				armB[round] = append(armB[round], value)
			}
		}
	}

	return armA, armB
}

// aaMetricResolution returns the larger absolute difference between paired A/A metric samples.
func aaMetricResolution(interval RatioInterval, baselineQuantile float64) AAMetricResolution {
	resolution := math.Max(math.Abs(1-interval.Lower), math.Abs(interval.Upper-1))
	return AAMetricResolution{
		Ratio:              interval,
		RatioResolution:    resolution,
		AbsoluteResolution: time.Duration(resolution * baselineQuantile),
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
