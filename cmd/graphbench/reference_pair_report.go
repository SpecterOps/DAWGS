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

// referencePairReportVersion identifies the serialized schema revision for reference pair report.
const referencePairReportVersion = 2

const (
	// referencePairProtocolConfirmation requires 20 warmups, 10 to 20 rounds, and 50 samples per arm and round.
	referencePairProtocolConfirmation = "confirmation"

	// referencePairProtocolDiscovery permits exploratory comparison with five warmups, five rounds, and ten samples per arm and round.
	referencePairProtocolDiscovery = "discovery"
)

// ReferencePairOptions selects two reference arms and the statistical protocol used for their paired comparison.
type ReferencePairOptions struct {
	// Seed controls deterministic random sampling.
	Seed int64
	// Confidence sets the confidence level used for statistical intervals.
	Confidence float64
	// BootstrapCount sets the number of bootstrap resamples.
	BootstrapCount int
	// BaselineName identifies the reference arm treated as the comparison baseline.
	BaselineName string
	// CandidateName identifies the reference arm evaluated against the baseline.
	CandidateName string
	// Protocol identifies the measurement protocol.
	Protocol string
}

// ReferencePairCase reports identity, sample, ratio, and absolute-change evidence for one reference-arm pair.
type ReferencePairCase struct {
	// Dataset identifies the fixture dataset.
	Dataset string `json:"dataset"`
	// Name identifies the case or record within its dataset.
	Name string `json:"name"`
	// Rounds records the number of independent measurement rounds.
	Rounds int `json:"rounds"`
	// BaselineArchitecture records the executor architecture declared by the baseline arm.
	BaselineArchitecture string `json:"baseline_architecture"`
	// CandidateArchitecture records the executor architecture declared by the candidate arm.
	CandidateArchitecture string `json:"candidate_architecture"`
	// BaselineBoundary records the portion of baseline execution included in its latency samples.
	BaselineBoundary string `json:"baseline_boundary"`
	// CandidateBoundary records the portion of candidate execution included in its latency samples.
	CandidateBoundary string `json:"candidate_boundary"`
	// BaselineSemanticValidation identifies the observation contract enforced for the baseline arm.
	BaselineSemanticValidation string `json:"baseline_semantic_validation"`
	// CandidateSemanticValidation identifies the observation contract enforced for the candidate arm.
	CandidateSemanticValidation string `json:"candidate_semantic_validation"`
	// BaselineSamples records warm timing samples available from the baseline arm.
	BaselineSamples int `json:"baseline_samples"`
	// CandidateSamples records warm timing samples available from the candidate arm.
	CandidateSamples int `json:"candidate_samples"`
	// MedianRatio reports the candidate-to-baseline median latency ratio and confidence bounds.
	MedianRatio RatioInterval `json:"median_ratio"`
	// P95Ratio reports the candidate-to-baseline P95 latency ratio and confidence bounds.
	P95Ratio RatioInterval `json:"p95_ratio"`
	// MedianChange reports the absolute median latency difference and confidence bounds.
	MedianChange DurationInterval `json:"median_change"`
	// BaselineAAResolution records the baseline arm's A/A-derived absolute noise floor.
	BaselineAAResolution time.Duration `json:"baseline_aa_resolution"`
	// CandidateAAResolution records the candidate arm's A/A-derived absolute noise floor.
	CandidateAAResolution time.Duration `json:"candidate_aa_resolution"`
}

// ReferencePairReport contains the input identity, protocol thresholds, and results of paired reference analysis.
type ReferencePairReport struct {
	// Version identifies the serialized schema revision.
	Version int `json:"version"`
	// Seed controls deterministic random sampling.
	Seed int64 `json:"seed"`
	// Confidence sets the confidence level used for statistical intervals.
	Confidence float64 `json:"confidence_level"`
	// ArtifactSHA256 identifies the exact input artifact summarized by the report.
	ArtifactSHA256 string `json:"artifact_sha256"`
	// BaselineName identifies the reference arm treated as the comparison baseline.
	BaselineName string `json:"baseline_name"`
	// CandidateName identifies the reference arm evaluated against the baseline.
	CandidateName string `json:"candidate_name"`
	// Protocol identifies the measurement protocol.
	Protocol string `json:"protocol"`
	// MinimumWarmups records the minimum untimed iterations required for each compared arm.
	MinimumWarmups int `json:"minimum_warmups"`
	// MinimumRounds records the minimum independent rounds required for comparison.
	MinimumRounds int `json:"minimum_rounds"`
	// MaximumRounds records the maximum rounds accepted by the selected protocol.
	MaximumRounds int `json:"maximum_rounds"`
	// MinimumSamples records the minimum warm samples required from each arm and round.
	MinimumSamples int `json:"minimum_samples_per_round"`
	// Cases contains paired statistical evidence for each workload present in the selected reference arms.
	Cases []ReferencePairCase `json:"cases"`
}

// buildReferencePairReport validates two reference arms and computes paired ratio and duration intervals by case.
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
	// pairSeries groups the two reference arms and the identities that must remain stable across rounds.
	type pairSeries struct {
		// baseline groups duration samples from the designated baseline arm by round.
		baseline roundSamples
		// candidate groups duration samples from the designated candidate arm by round.
		candidate roundSamples
		// baselineArchitecture identifies the execution architecture reported by the baseline arm.
		baselineArchitecture string
		// candidateArchitecture identifies the execution architecture reported by the candidate arm.
		candidateArchitecture string
		// baselineBoundary identifies the measurement boundary reported by the baseline arm.
		baselineBoundary string
		// candidateBoundary identifies the measurement boundary reported by the candidate arm.
		candidateBoundary string
		// baselineValidation retains the baseline observation contract that must remain stable across rounds.
		baselineValidation string
		// candidateValidation retains the candidate observation contract that must remain stable across rounds.
		candidateValidation string
		// baselineImplementation identifies the baseline reference implementation.
		baselineImplementation string
		// candidateImplementation identifies the candidate reference implementation.
		candidateImplementation string
		// baselineSQLFingerprint identifies the normalized SQL executed by the baseline arm.
		baselineSQLFingerprint string
		// candidateSQLFingerprint identifies the normalized SQL executed by the candidate arm.
		candidateSQLFingerprint string
		// binaryIdentity binds all paired rounds to the same executable and source state.
		binaryIdentity string
		// baselineFirst records by round whether the baseline arm executed before the candidate.
		baselineFirst map[int]bool
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

// findReference returns the named PostgreSQL reference result or nil when it is absent.
func findReference(references []PostgresReferenceResult, name string) *PostgresReferenceResult {
	for idx := range references {
		if references[idx].Name == name {
			return &references[idx]
		}
	}
	return nil
}

// createReferencePairReport loads benchmark records, builds a reference-pair report, and writes it as JSON.
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
