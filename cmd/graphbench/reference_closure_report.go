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
	"slices"
	"sort"
	"time"
)

// referenceClosureReportVersion identifies the serialized schema revision for reference closure report.
const referenceClosureReportVersion = 1

// ReferenceClosureOptions selects the reference arm and ratio and absolute limits used for closure analysis.
type ReferenceClosureOptions struct {
	// Seed controls deterministic random sampling.
	Seed int64
	// Confidence sets the confidence level used for statistical intervals.
	Confidence float64
	// BootstrapCount sets the number of bootstrap resamples.
	BootstrapCount int
	// ReferenceName identifies the reference arm selected for closure analysis.
	ReferenceName string
	// RatioUpperLimit sets the largest production-to-reference median ratio accepted by closure analysis.
	RatioUpperLimit float64
	// AbsoluteResolution supplies the absolute resolution input to the ReferenceClosureOptions contract.
	AbsoluteResolution time.Duration
}

// ReferenceClosureCase reports paired production/reference samples, A/A floors, and closure disposition for one case.
type ReferenceClosureCase struct {
	// Dataset identifies the fixture dataset.
	Dataset string `json:"dataset"`
	// Name identifies the case or record within its dataset.
	Name string `json:"name"`
	// ReferenceName identifies the reference arm selected for closure analysis.
	ReferenceName string `json:"reference_name"`
	// ReferenceArchitecture supplies the reference architecture input to the ReferenceClosureCase contract.
	ReferenceArchitecture string `json:"reference_architecture"`
	// Rounds records the number of rounds.
	Rounds int `json:"rounds"`
	// ProductionSamples records warm timing samples available from production execution.
	ProductionSamples int `json:"production_samples"`
	// ReferenceSamples records warm timing samples available from the reference arm.
	ReferenceSamples int `json:"reference_samples"`
	// MedianRatio reports the candidate-to-baseline median latency ratio and confidence bounds.
	MedianRatio RatioInterval `json:"median_ratio"`
	// MedianChange reports the absolute median latency difference and confidence bounds.
	MedianChange DurationInterval `json:"median_change"`
	// AbsoluteGapUpper supplies the absolute gap upper input to the ReferenceClosureCase contract.
	AbsoluteGapUpper time.Duration `json:"absolute_gap_upper"`
	// RatioUpperLimit sets the largest production-to-reference median ratio accepted by closure analysis.
	RatioUpperLimit float64 `json:"ratio_upper_limit"`
	// AbsoluteFloor supplies the absolute floor input to the ReferenceClosureCase contract.
	AbsoluteFloor time.Duration `json:"absolute_floor"`
	// ProductionAAResolution records production-arm A/A noise used for closure materiality.
	ProductionAAResolution time.Duration `json:"production_aa_resolution"`
	// ReferenceAAResolution records reference-arm A/A noise used for closure materiality.
	ReferenceAAResolution time.Duration `json:"reference_aa_resolution"`
	// AbsoluteResolution supplies the absolute resolution input to the ReferenceClosureCase contract.
	AbsoluteResolution time.Duration `json:"absolute_resolution"`
	// Passed reports whether every required gate condition succeeded.
	Passed bool `json:"passed"`
	// Reasons lists explanations for the reported disposition.
	Reasons []string `json:"reasons,omitempty"`
	// ProductionRuntimeReceiptChains preserves the complete production branch
	// chain for every measured invocation used by closure.
	ProductionRuntimeReceiptChains [][]RuntimeReceiptEvent `json:"production_runtime_receipt_chains,omitempty"`
}

// ReferenceClosureReport contains artifact identity, thresholds, and per-case production/reference closure results.
type ReferenceClosureReport struct {
	// Version identifies the serialized schema revision.
	Version int `json:"version"`
	// Seed controls deterministic random sampling.
	Seed int64 `json:"seed"`
	// Confidence sets the confidence level used for statistical intervals.
	Confidence float64 `json:"confidence_level"`
	// ArtifactSHA256 identifies the exact input artifact summarized by the report.
	ArtifactSHA256 string `json:"artifact_sha256"`
	// ReferenceName identifies the reference arm selected for closure analysis.
	ReferenceName string `json:"reference_name"`
	// Passed reports whether every required gate condition succeeded.
	Passed bool `json:"passed"`
	// Cases contains production-to-reference closure evidence for each evaluated workload.
	Cases []ReferenceClosureCase `json:"cases"`
}

// buildReferenceClosureReport compares production and exact-reference samples under the closure protocol.
func buildReferenceClosureReport(records []CaseResult, options ReferenceClosureOptions) (ReferenceClosureReport, error) {
	if options.Confidence <= 0 || options.Confidence >= 1 {
		return ReferenceClosureReport{}, fmt.Errorf("confidence level must be between 0 and 1")
	}
	if options.BootstrapCount == 0 {
		options.BootstrapCount = defaultBootstrapCount
	}
	if options.BootstrapCount < 1 {
		return ReferenceClosureReport{}, fmt.Errorf("bootstrap count must be positive")
	}
	if options.ReferenceName == "" {
		options.ReferenceName = "s3_unidirectional_trail_cte"
	}
	if options.RatioUpperLimit == 0 {
		options.RatioUpperLimit = 1.10
	}
	if options.RatioUpperLimit <= 0 {
		return ReferenceClosureReport{}, fmt.Errorf("reference ratio upper limit must be positive")
	}
	if options.AbsoluteResolution == 0 {
		options.AbsoluteResolution = 100 * time.Microsecond
	}
	if options.AbsoluteResolution < 0 {
		return ReferenceClosureReport{}, fmt.Errorf("reference absolute resolution must not be negative")
	}

	// closureSeries groups production and reference samples with the architecture fixed across rounds.
	type closureSeries struct {
		// production groups production duration samples by measurement round.
		production roundSamples
		// reference groups reference-arm duration samples by measurement round.
		reference roundSamples
		// architecture retains the executor architecture that must remain stable across rounds.
		architecture string
	}
	series := map[performanceKey]*closureSeries{}
	seenRounds := map[performanceKey]map[int]struct{}{}
	for _, record := range records {
		if record.ExecutionMode != ModePostgresSQL {
			continue
		}
		if record.Status != StatusOK {
			return ReferenceClosureReport{}, fmt.Errorf("%s/%s has non-ok status %s", record.Dataset, record.Name, record.Status)
		}
		if record.Environment == nil {
			return ReferenceClosureReport{}, fmt.Errorf("%s/%s has no run environment", record.Dataset, record.Name)
		}
		if record.Environment.WarmupIterations < 20 {
			return ReferenceClosureReport{}, fmt.Errorf("%s/%s round %d requires at least 20 warmups, got %d", record.Dataset, record.Name, record.Environment.Round, record.Environment.WarmupIterations)
		}
		if record.RawPGXWaterfall == nil || record.RawPGXWaterfall.WarmupIterations < 20 {
			return ReferenceClosureReport{}, fmt.Errorf("%s/%s round %d lacks a 20-warmup production raw-pgx boundary", record.Dataset, record.Name, record.Environment.Round)
		}
		var reference *PostgresReferenceResult
		for idx := range record.PostgresReferences {
			if record.PostgresReferences[idx].Name == options.ReferenceName {
				reference = &record.PostgresReferences[idx]
				break
			}
		}
		if reference == nil {
			return ReferenceClosureReport{}, fmt.Errorf("%s/%s round %d is missing reference %s", record.Dataset, record.Name, record.Environment.Round, options.ReferenceName)
		}
		if !reference.FullComparator || reference.SemanticValidation != "exact_public_observation" {
			return ReferenceClosureReport{}, fmt.Errorf("%s/%s reference %s is not an exact full comparator", record.Dataset, record.Name, options.ReferenceName)
		}
		if reference.RowCount != record.RowCount || !slices.Equal(reference.ObservedRows, record.ObservedRows) {
			return ReferenceClosureReport{}, fmt.Errorf("%s/%s reference observation differs from production", record.Dataset, record.Name)
		}
		if reference.Stats.WarmupIterations < 20 {
			return ReferenceClosureReport{}, fmt.Errorf("%s/%s round %d reference requires at least 20 warmups, got %d", record.Dataset, record.Name, record.Environment.Round, reference.Stats.WarmupIterations)
		}
		expectedProductionOrder, expectedReferenceOrder := referenceClosureMeasurementOrder(true, record.Environment.Round)
		if record.RawPGXWaterfall.MeasurementOrder != expectedProductionOrder || reference.MeasurementOrder != expectedReferenceOrder {
			return ReferenceClosureReport{}, fmt.Errorf("%s/%s round %d lacks carryover-balanced production/reference order: got %d/%d, expected %d/%d", record.Dataset, record.Name, record.Environment.Round, record.RawPGXWaterfall.MeasurementOrder, reference.MeasurementOrder, expectedProductionOrder, expectedReferenceOrder)
		}

		key := performanceKey{
			dataset: record.Dataset,
			name:    record.Name,
			backend: ModePostgresSQL,
		}
		if seenRounds[key] == nil {
			seenRounds[key] = map[int]struct{}{}
		}
		if _, duplicate := seenRounds[key][record.Environment.Round]; duplicate {
			return ReferenceClosureReport{}, fmt.Errorf("%s/%s has duplicate round %d", record.Dataset, record.Name, record.Environment.Round)
		}
		seenRounds[key][record.Environment.Round] = struct{}{}

		if series[key] == nil {
			series[key] = &closureSeries{
				production:   roundSamples{},
				reference:    roundSamples{},
				architecture: reference.Architecture,
			}
		} else if series[key].architecture != reference.Architecture {
			return ReferenceClosureReport{}, fmt.Errorf("%s/%s reference architecture changed across rounds", record.Dataset, record.Name)
		}

		for _, sample := range record.RawPGXWaterfall.Samples {
			if sample.Total > 0 {
				series[key].production[record.Environment.Round] = append(series[key].production[record.Environment.Round], sample.Total)
			}
		}

		for _, sample := range reference.Stats.Samples {
			if sample.Classification == "warm" && sample.Duration > 0 {
				series[key].reference[record.Environment.Round] = append(series[key].reference[record.Environment.Round], sample.Duration)
			}
		}
	}

	if len(series) == 0 {
		return ReferenceClosureReport{}, fmt.Errorf("artifact has no successful PostgreSQL production/reference records")
	}

	keys := make([]performanceKey, 0, len(series))
	for key := range series {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].dataset != keys[j].dataset {
			return keys[i].dataset < keys[j].dataset
		}
		return keys[i].name < keys[j].name
	})
	report := ReferenceClosureReport{
		Version:       referenceClosureReportVersion,
		Seed:          options.Seed,
		Confidence:    options.Confidence,
		ReferenceName: options.ReferenceName,
		Passed:        true,
	}
	gateOptions := PerfGateOptions{
		Seed:           options.Seed,
		Confidence:     options.Confidence,
		BootstrapCount: options.BootstrapCount,
	}
	for idx, key := range keys {
		candidate, baseline := matchedRounds(series[key].production, series[key].reference)
		entry := ReferenceClosureCase{
			Dataset:                        key.dataset,
			Name:                           key.name,
			ReferenceName:                  options.ReferenceName,
			ReferenceArchitecture:          series[key].architecture,
			Rounds:                         len(candidate),
			ProductionSamples:              sampleCount(candidate),
			ReferenceSamples:               sampleCount(baseline),
			RatioUpperLimit:                options.RatioUpperLimit,
			AbsoluteFloor:                  options.AbsoluteResolution,
			Passed:                         true,
			ProductionRuntimeReceiptChains: caseRuntimeReceiptChains(records, key),
		}
		if entry.Rounds < 10 || entry.Rounds > 20 {
			entry.Passed = false
			entry.Reasons = append(entry.Reasons, fmt.Sprintf("requires 10-20 matched rounds, got %d", entry.Rounds))
		}
		for _, round := range sortedRounds(candidate) {
			if len(candidate[round]) < 50 || len(baseline[round]) < 50 {
				entry.Passed = false
				entry.Reasons = append(entry.Reasons, fmt.Sprintf("round %d requires at least 50 samples per side, got %d/%d", round, len(candidate[round]), len(baseline[round])))
			}
		}
		if entry.Rounds > 0 {
			seed := options.Seed + int64(idx)*7919
			entry.ProductionAAResolution = withinSessionAAResolution(candidate, seed+2, gateOptions)
			entry.ReferenceAAResolution = withinSessionAAResolution(baseline, seed+3, gateOptions)
			entry.AbsoluteResolution = max(options.AbsoluteResolution, entry.ProductionAAResolution, entry.ReferenceAAResolution)
			entry.MedianRatio = bootstrapRoundMedianRatio(baseline, candidate, seed, gateOptions)
			entry.MedianChange = negateDurationInterval(bootstrapRoundMedianSaving(baseline, candidate, seed+1, gateOptions))
			entry.AbsoluteGapUpper = max(absDuration(entry.MedianChange.Lower), absDuration(entry.MedianChange.Upper))
			if entry.MedianRatio.Upper > options.RatioUpperLimit && entry.AbsoluteGapUpper > entry.AbsoluteResolution {
				entry.Passed = false
				entry.Reasons = append(entry.Reasons, fmt.Sprintf("ratio upper %.4f exceeds %.4f and absolute gap upper %s exceeds effective resolution %s", entry.MedianRatio.Upper, options.RatioUpperLimit, entry.AbsoluteGapUpper, entry.AbsoluteResolution))
			}
		}
		if !entry.Passed {
			report.Passed = false
		}
		report.Cases = append(report.Cases, entry)
	}
	return report, nil
}

// withinSessionAAResolution returns the larger within-session A/A noise estimate for a case.
func withinSessionAAResolution(samples roundSamples, seed int64, options PerfGateOptions) time.Duration {
	armA, armB := splitInterleavedDiagnosticSeries(samples)
	armA, armB = matchedRounds(armA, armB)
	if len(armA) == 0 {
		return 0
	}
	interval := bootstrapRoundMedianSaving(armA, armB, seed, options)
	return max(absDuration(interval.Lower), absDuration(interval.Upper))
}

// splitInterleavedDiagnosticSeries estimates within-session resolution for the
// descriptive reference-closure report only. Promotion-grade host A/A evidence
// is built exclusively from explicit arms by collectExplicitAASeries.
func splitInterleavedDiagnosticSeries(samples roundSamples) (roundSamples, roundSamples) {
	armA, armB := roundSamples{}, roundSamples{}
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

// absDuration returns the magnitude of a signed duration.
func absDuration(value time.Duration) time.Duration {
	return time.Duration(math.Abs(float64(value)))
}

// createReferenceClosureReport loads benchmark records, builds a closure report, and writes it as JSON.
func createReferenceClosureReport(artifactPath, outputPath string, options ReferenceClosureOptions) (bool, error) {
	records, err := readJSONLFile(artifactPath)
	if err != nil {
		return false, err
	}
	report, err := buildReferenceClosureReport(records, options)
	if err != nil {
		return false, err
	}
	report.ArtifactSHA256, err = fileSHA256(artifactPath)
	if err != nil {
		return false, err
	}
	return report.Passed, writeReferenceClosureReport(outputPath, report)
}

// writeReferenceClosureReport writes a reference-closure report as indented JSON.
func writeReferenceClosureReport(path string, report ReferenceClosureReport) (err error) {
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
