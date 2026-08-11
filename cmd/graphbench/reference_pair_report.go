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
)

const referencePairReportVersion = 2

const (
	referencePairProtocolConfirmation = "confirmation"
	referencePairProtocolDiscovery    = "discovery"
)

type ReferencePairOptions struct {
	Seed           int64
	Confidence     float64
	BootstrapCount int
	BaselineName   string
	CandidateName  string
	Protocol       string
}

type ReferencePairCase struct {
	Dataset                     string           `json:"dataset"`
	Name                        string           `json:"name"`
	Rounds                      int              `json:"rounds"`
	BaselineArchitecture        string           `json:"baseline_architecture"`
	CandidateArchitecture       string           `json:"candidate_architecture"`
	BaselineBoundary            string           `json:"baseline_boundary"`
	CandidateBoundary           string           `json:"candidate_boundary"`
	BaselineSemanticValidation  string           `json:"baseline_semantic_validation"`
	CandidateSemanticValidation string           `json:"candidate_semantic_validation"`
	BaselineSamples             int              `json:"baseline_samples"`
	CandidateSamples            int              `json:"candidate_samples"`
	MedianRatio                 RatioInterval    `json:"median_ratio"`
	P95Ratio                    RatioInterval    `json:"p95_ratio"`
	MedianChange                DurationInterval `json:"median_change"`
	BaselineAAResolution        time.Duration    `json:"baseline_aa_resolution"`
	CandidateAAResolution       time.Duration    `json:"candidate_aa_resolution"`
}

type ReferencePairReport struct {
	Version        int                 `json:"version"`
	Seed           int64               `json:"seed"`
	Confidence     float64             `json:"confidence_level"`
	ArtifactSHA256 string              `json:"artifact_sha256"`
	BaselineName   string              `json:"baseline_name"`
	CandidateName  string              `json:"candidate_name"`
	Protocol       string              `json:"protocol"`
	MinimumWarmups int                 `json:"minimum_warmups"`
	MinimumRounds  int                 `json:"minimum_rounds"`
	MaximumRounds  int                 `json:"maximum_rounds"`
	MinimumSamples int                 `json:"minimum_samples_per_round"`
	Cases          []ReferencePairCase `json:"cases"`
}

func buildReferencePairReport(records []CaseResult, options ReferencePairOptions) (ReferencePairReport, error) {
	if options.Confidence <= 0 || options.Confidence >= 1 {
		return ReferencePairReport{}, fmt.Errorf("confidence level must be between 0 and 1")
	}
	if options.BootstrapCount == 0 {
		options.BootstrapCount = defaultBootstrapCount
	}
	if options.BootstrapCount < 1 || options.BaselineName == "" || options.CandidateName == "" || options.BaselineName == options.CandidateName {
		return ReferencePairReport{}, fmt.Errorf("valid distinct baseline and candidate reference arms are required")
	}
	protocol := options.Protocol
	if protocol == "" {
		protocol = referencePairProtocolConfirmation
	}
	minimumWarmups, minimumRounds, maximumRounds, minimumSamples := 20, 10, 20, 50
	if protocol == referencePairProtocolDiscovery {
		minimumWarmups, minimumRounds, maximumRounds, minimumSamples = 5, 5, 20, 10
	} else if protocol != referencePairProtocolConfirmation {
		return ReferencePairReport{}, fmt.Errorf("unsupported reference-pair protocol %q", protocol)
	}
	type pairSeries struct {
		baseline, candidate                             roundSamples
		baselineArchitecture, candidateArchitecture     string
		baselineBoundary, candidateBoundary             string
		baselineValidation, candidateValidation         string
		baselineImplementation, candidateImplementation string
		baselineSQLFingerprint, candidateSQLFingerprint string
		binaryIdentity                                  string
		baselineFirst                                   map[int]bool
	}
	series := map[performanceKey]*pairSeries{}
	seen := map[performanceKey]map[int]struct{}{}
	for _, record := range records {
		if record.ExecutionMode != ModePostgresSQL {
			continue
		}
		if record.Status != StatusOK || record.Environment == nil || record.Environment.WarmupIterations < minimumWarmups {
			return ReferencePairReport{}, fmt.Errorf("%s/%s lacks a successful %d-warmup PostgreSQL record", record.Dataset, record.Name, minimumWarmups)
		}
		baseline := findReference(record.PostgresReferences, options.BaselineName)
		candidate := findReference(record.PostgresReferences, options.CandidateName)
		if baseline == nil || candidate == nil {
			return ReferencePairReport{}, fmt.Errorf("%s/%s round %d lacks reference pair %s/%s", record.Dataset, record.Name, record.Environment.Round, options.BaselineName, options.CandidateName)
		}
		fullComparators := baseline.FullComparator && candidate.FullComparator && baseline.SemanticValidation == "exact_public_observation" && candidate.SemanticValidation == "exact_public_observation"
		hydrationComparators := !baseline.FullComparator && !candidate.FullComparator && baseline.SemanticValidation == "precomputed_exact_path_inputs" && candidate.SemanticValidation == "precomputed_exact_path_inputs"
		orderedComparators := !baseline.FullComparator && !candidate.FullComparator && baseline.ObservationShape == "ordered_ids" && candidate.ObservationShape == "ordered_ids" && baseline.SemanticValidation == "exact_ordered_ids" && candidate.SemanticValidation == "exact_ordered_ids"
		if !fullComparators && !hydrationComparators && !orderedComparators {
			return ReferencePairReport{}, fmt.Errorf("%s/%s reference pair does not share an exact comparable boundary", record.Dataset, record.Name)
		}
		if (fullComparators || hydrationComparators) && (baseline.RowCount != record.RowCount || candidate.RowCount != record.RowCount || !slices.Equal(baseline.ObservedRows, record.ObservedRows) || !slices.Equal(candidate.ObservedRows, record.ObservedRows)) {
			return ReferencePairReport{}, fmt.Errorf("%s/%s reference-pair observation differs from production", record.Dataset, record.Name)
		}
		if orderedComparators && (baseline.RowCount != candidate.RowCount || !slices.Equal(baseline.ObservedRows, candidate.ObservedRows)) {
			return ReferencePairReport{}, fmt.Errorf("%s/%s ordered-ID reference-pair observations differ", record.Dataset, record.Name)
		}
		if baseline.ImplementationID == "" || candidate.ImplementationID == "" || baseline.SQLFingerprint == "" || candidate.SQLFingerprint == "" || record.Environment.BinarySHA256 == "" {
			return ReferencePairReport{}, fmt.Errorf("%s/%s round %d lacks complete reference-pair implementation identity", record.Dataset, record.Name, record.Environment.Round)
		}
		if baseline.Stats.WarmupIterations < minimumWarmups || candidate.Stats.WarmupIterations < minimumWarmups || baseline.MeasurementOrder <= 0 || candidate.MeasurementOrder <= 0 || baseline.MeasurementOrder == candidate.MeasurementOrder {
			return ReferencePairReport{}, fmt.Errorf("%s/%s round %d lacks warm, ordered reference-pair measurements", record.Dataset, record.Name, record.Environment.Round)
		}
		binaryIdentity := fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s", record.Environment.BinarySHA256, record.Environment.DirtyDiffSHA256, record.Environment.SourceCommit, record.Environment.GOOS, record.Environment.GOARCH)
		key := performanceKey{
			dataset: record.Dataset,
			name:    record.Name,
			backend: ModePostgresSQL,
		}
		if seen[key] == nil {
			seen[key] = map[int]struct{}{}
		}
		if _, duplicate := seen[key][record.Environment.Round]; duplicate {
			return ReferencePairReport{}, fmt.Errorf("%s/%s has duplicate round %d", record.Dataset, record.Name, record.Environment.Round)
		}
		seen[key][record.Environment.Round] = struct{}{}
		if series[key] == nil {
			series[key] = &pairSeries{
				baseline:                roundSamples{},
				candidate:               roundSamples{},
				baselineArchitecture:    baseline.Architecture,
				candidateArchitecture:   candidate.Architecture,
				baselineBoundary:        baseline.Boundary,
				candidateBoundary:       candidate.Boundary,
				baselineValidation:      baseline.SemanticValidation,
				candidateValidation:     candidate.SemanticValidation,
				baselineImplementation:  baseline.ImplementationID,
				candidateImplementation: candidate.ImplementationID,
				baselineSQLFingerprint:  baseline.SQLFingerprint,
				candidateSQLFingerprint: candidate.SQLFingerprint,
				binaryIdentity:          binaryIdentity,
				baselineFirst:           map[int]bool{},
			}
		} else if series[key].baselineArchitecture != baseline.Architecture || series[key].candidateArchitecture != candidate.Architecture ||
			series[key].baselineBoundary != baseline.Boundary || series[key].candidateBoundary != candidate.Boundary ||
			series[key].baselineValidation != baseline.SemanticValidation || series[key].candidateValidation != candidate.SemanticValidation ||
			series[key].baselineImplementation != baseline.ImplementationID || series[key].candidateImplementation != candidate.ImplementationID ||
			series[key].baselineSQLFingerprint != baseline.SQLFingerprint || series[key].candidateSQLFingerprint != candidate.SQLFingerprint ||
			series[key].binaryIdentity != binaryIdentity {
			return ReferencePairReport{}, fmt.Errorf("%s/%s reference-pair identity changed across rounds", record.Dataset, record.Name)
		}
		series[key].baselineFirst[record.Environment.Round] = baseline.MeasurementOrder < candidate.MeasurementOrder
		for _, sample := range baseline.Stats.Samples {
			if sample.Classification == "warm" && sample.Duration > 0 {
				series[key].baseline[record.Environment.Round] = append(series[key].baseline[record.Environment.Round], sample.Duration)
			}
		}
		for _, sample := range candidate.Stats.Samples {
			if sample.Classification == "warm" && sample.Duration > 0 {
				series[key].candidate[record.Environment.Round] = append(series[key].candidate[record.Environment.Round], sample.Duration)
			}
		}
	}
	if len(series) == 0 {
		return ReferencePairReport{}, fmt.Errorf("artifact has no PostgreSQL reference-pair records")
	}
	keys := make([]performanceKey, 0, len(series))
	for key := range series {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].dataset < keys[j].dataset || keys[i].dataset == keys[j].dataset && keys[i].name < keys[j].name
	})
	report := ReferencePairReport{
		Version:        referencePairReportVersion,
		Seed:           options.Seed,
		Confidence:     options.Confidence,
		BaselineName:   options.BaselineName,
		CandidateName:  options.CandidateName,
		Protocol:       protocol,
		MinimumWarmups: minimumWarmups,
		MinimumRounds:  minimumRounds,
		MaximumRounds:  maximumRounds,
		MinimumSamples: minimumSamples,
	}
	gateOptions := PerfGateOptions{
		Seed:           options.Seed,
		Confidence:     options.Confidence,
		BootstrapCount: options.BootstrapCount,
	}
	for idx, key := range keys {
		baseline, candidate := matchedRounds(series[key].baseline, series[key].candidate)
		if len(baseline) < minimumRounds || len(baseline) > maximumRounds {
			return ReferencePairReport{}, fmt.Errorf("%s/%s requires %d-%d matched rounds, got %d", key.dataset, key.name, minimumRounds, maximumRounds, len(baseline))
		}
		rounds := sortedRounds(baseline)
		baselineFirstCount := 0
		for roundIdx, round := range rounds {
			baselineFirst := series[key].baselineFirst[round]
			if baselineFirst {
				baselineFirstCount++
			}
			if roundIdx > 0 && series[key].baselineFirst[rounds[roundIdx-1]] == baselineFirst {
				return ReferencePairReport{}, fmt.Errorf("%s/%s reference-pair arm order does not alternate across rounds", key.dataset, key.name)
			}
		}
		candidateFirstCount := len(rounds) - baselineFirstCount
		if baselineFirstCount-candidateFirstCount > 1 || candidateFirstCount-baselineFirstCount > 1 {
			return ReferencePairReport{}, fmt.Errorf("%s/%s reference-pair arm order is not balanced", key.dataset, key.name)
		}
		for _, round := range rounds {
			if len(baseline[round]) < minimumSamples || len(candidate[round]) < minimumSamples {
				return ReferencePairReport{}, fmt.Errorf("%s/%s round %d requires %d samples per arm", key.dataset, key.name, round, minimumSamples)
			}
		}
		seed := options.Seed + int64(idx)*7919
		report.Cases = append(report.Cases, ReferencePairCase{
			Dataset:                     key.dataset,
			Name:                        key.name,
			Rounds:                      len(baseline),
			BaselineArchitecture:        series[key].baselineArchitecture,
			CandidateArchitecture:       series[key].candidateArchitecture,
			BaselineBoundary:            series[key].baselineBoundary,
			CandidateBoundary:           series[key].candidateBoundary,
			BaselineSemanticValidation:  series[key].baselineValidation,
			CandidateSemanticValidation: series[key].candidateValidation,
			BaselineSamples:             sampleCount(baseline),
			CandidateSamples:            sampleCount(candidate),
			MedianRatio:                 bootstrapRoundMedianRatio(baseline, candidate, seed, gateOptions),
			P95Ratio:                    bootstrapStratifiedP95Ratio(baseline, candidate, seed+4, gateOptions),
			MedianChange:                negateDurationInterval(bootstrapRoundMedianSaving(baseline, candidate, seed+1, gateOptions)),
			BaselineAAResolution:        withinSessionAAResolution(baseline, seed+2, gateOptions),
			CandidateAAResolution:       withinSessionAAResolution(candidate, seed+3, gateOptions),
		})
	}
	return report, nil
}

func findReference(references []PostgresReferenceResult, name string) *PostgresReferenceResult {
	for idx := range references {
		if references[idx].Name == name {
			return &references[idx]
		}
	}
	return nil
}

func createReferencePairReport(artifactPath, outputPath string, options ReferencePairOptions) error {
	records, err := readJSONLFile(artifactPath)
	if err != nil {
		return err
	}
	report, err := buildReferencePairReport(records, options)
	if err != nil {
		return err
	}
	report.ArtifactSHA256, err = fileSHA256(artifactPath)
	if err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	if outputPath == "" {
		_, err = os.Stdout.Write(encoded)
		return err
	}
	if err := ensureOutputDir(outputPath); err != nil {
		return err
	}
	return os.WriteFile(outputPath, encoded, 0o644)
}
