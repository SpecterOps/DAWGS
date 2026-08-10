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

const referenceClosureReportVersion = 1

type ReferenceClosureOptions struct {
	Seed               int64
	Confidence         float64
	BootstrapCount     int
	ReferenceName      string
	RatioUpperLimit    float64
	AbsoluteResolution time.Duration
}

type ReferenceClosureCase struct {
	Dataset                string           `json:"dataset"`
	Name                   string           `json:"name"`
	ReferenceName          string           `json:"reference_name"`
	ReferenceArchitecture  string           `json:"reference_architecture"`
	Rounds                 int              `json:"rounds"`
	ProductionSamples      int              `json:"production_samples"`
	ReferenceSamples       int              `json:"reference_samples"`
	MedianRatio            RatioInterval    `json:"median_ratio"`
	MedianChange           DurationInterval `json:"median_change"`
	AbsoluteGapUpper       time.Duration    `json:"absolute_gap_upper"`
	RatioUpperLimit        float64          `json:"ratio_upper_limit"`
	AbsoluteFloor          time.Duration    `json:"absolute_floor"`
	ProductionAAResolution time.Duration    `json:"production_aa_resolution"`
	ReferenceAAResolution  time.Duration    `json:"reference_aa_resolution"`
	AbsoluteResolution     time.Duration    `json:"absolute_resolution"`
	Passed                 bool             `json:"passed"`
	Reasons                []string         `json:"reasons,omitempty"`
}

type ReferenceClosureReport struct {
	Version        int                    `json:"version"`
	Seed           int64                  `json:"seed"`
	Confidence     float64                `json:"confidence_level"`
	ArtifactSHA256 string                 `json:"artifact_sha256"`
	ReferenceName  string                 `json:"reference_name"`
	Passed         bool                   `json:"passed"`
	Cases          []ReferenceClosureCase `json:"cases"`
}

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

	type closureSeries struct {
		production   roundSamples
		reference    roundSamples
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
			Dataset:               key.dataset,
			Name:                  key.name,
			ReferenceName:         options.ReferenceName,
			ReferenceArchitecture: series[key].architecture,
			Rounds:                len(candidate),
			ProductionSamples:     sampleCount(candidate),
			ReferenceSamples:      sampleCount(baseline),
			RatioUpperLimit:       options.RatioUpperLimit,
			AbsoluteFloor:         options.AbsoluteResolution,
			Passed:                true,
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
			if entry.MedianRatio.Upper > options.RatioUpperLimit && entry.AbsoluteGapUpper > options.AbsoluteResolution {
				entry.Passed = false
				entry.Reasons = append(entry.Reasons, fmt.Sprintf("ratio upper %.4f exceeds %.4f and absolute gap upper %s exceeds %s", entry.MedianRatio.Upper, options.RatioUpperLimit, entry.AbsoluteGapUpper, options.AbsoluteResolution))
			}
		}
		if !entry.Passed {
			report.Passed = false
		}
		report.Cases = append(report.Cases, entry)
	}
	return report, nil
}

func withinSessionAAResolution(samples roundSamples, seed int64, options PerfGateOptions) time.Duration {
	armA, armB := splitAASeries(samples)
	armA, armB = matchedRounds(armA, armB)
	if len(armA) == 0 {
		return 0
	}
	interval := bootstrapRoundMedianSaving(armA, armB, seed, options)
	return max(absDuration(interval.Lower), absDuration(interval.Upper))
}

func absDuration(value time.Duration) time.Duration {
	return time.Duration(math.Abs(float64(value)))
}

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
