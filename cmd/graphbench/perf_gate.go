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
	perfGateVersion       = 1
	defaultBootstrapCount = 10_000
	minimumGateRounds     = 5
	minimumP95Samples     = 150
)

type PerfGateOptions struct {
	Seed                int64
	Confidence          float64
	RegressionThreshold float64
	BootstrapCount      int
}

type RatioInterval struct {
	Estimate float64 `json:"estimate"`
	Lower    float64 `json:"lower"`
	Upper    float64 `json:"upper"`
}

type PerfGateCase struct {
	Dataset             string         `json:"dataset"`
	Name                string         `json:"name"`
	Backend             ExecutionMode  `json:"backend"`
	Rounds              int            `json:"rounds"`
	BaselineSamples     int            `json:"baseline_samples"`
	CandidateSamples    int            `json:"candidate_samples"`
	MedianRatio         RatioInterval  `json:"median_ratio"`
	P95Ratio            *RatioInterval `json:"p95_ratio,omitempty"`
	TargetBaselineLimit *float64       `json:"target_baseline_upper_limit,omitempty"`
	BackendRatio        *RatioInterval `json:"postgres_neo4j_ratio,omitempty"`
	BackendRatioLimit   *float64       `json:"postgres_neo4j_upper_limit,omitempty"`
	Passed              bool           `json:"passed"`
	Reasons             []string       `json:"reasons,omitempty"`
}

type PerfGateReport struct {
	Version             int            `json:"version"`
	Seed                int64          `json:"seed"`
	Confidence          float64        `json:"confidence_level"`
	RegressionThreshold float64        `json:"regression_threshold"`
	BaselineSHA256      string         `json:"baseline_sha256"`
	CandidateSHA256     string         `json:"candidate_sha256"`
	Passed              bool           `json:"passed"`
	Cases               []PerfGateCase `json:"cases"`
}

type performanceKey struct {
	dataset string
	name    string
	backend ExecutionMode
}

type roundSamples map[int][]time.Duration

type targetGate struct {
	baselineUpper float64
	backendUpper  float64
}

var targetPerformanceGates = map[string]targetGate{
	"one_shortest_path_bound_pair": {baselineUpper: 0.40, backendUpper: 3.0},
	"adcs_p1_endpoint_ids":         {baselineUpper: 0.60, backendUpper: 2.0},
	"adcs_p1_path_observed":        {baselineUpper: 0.70, backendUpper: 2.5},
}

func comparePerformanceArtifacts(baselinePath, candidatePath, outputPath string, options PerfGateOptions) (bool, error) {
	baseline, err := readJSONLFile(baselinePath)
	if err != nil {
		return false, fmt.Errorf("read baseline: %w", err)
	}
	candidate, err := readJSONLFile(candidatePath)
	if err != nil {
		return false, fmt.Errorf("read candidate: %w", err)
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
	return report.Passed, nil
}

func buildPerfGateReport(baseline, candidate []CaseResult, options PerfGateOptions) (PerfGateReport, error) {
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

	baselineSeries := collectWarmSeries(baseline)
	candidateSeries := collectWarmSeries(candidate)
	keys := make([]performanceKey, 0, len(candidateSeries))
	for key := range candidateSeries {
		if _, found := baselineSeries[key]; found {
			keys = append(keys, key)
		}
	}
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
		return PerfGateReport{}, fmt.Errorf("artifacts have no comparable warm samples")
	}

	report := PerfGateReport{
		Version:             perfGateVersion,
		Seed:                options.Seed,
		Confidence:          options.Confidence,
		RegressionThreshold: options.RegressionThreshold,
		Passed:              true,
	}
	for idx, key := range keys {
		baselineRounds, candidateRounds := matchedRounds(baselineSeries[key], candidateSeries[key])
		gateCase := PerfGateCase{
			Dataset:          key.dataset,
			Name:             key.name,
			Backend:          key.backend,
			Rounds:           len(baselineRounds),
			BaselineSamples:  sampleCount(baselineRounds),
			CandidateSamples: sampleCount(candidateRounds),
			Passed:           true,
		}
		if len(baselineRounds) < minimumGateRounds {
			gateCase.Passed = false
			gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("need at least %d matched rounds, got %d", minimumGateRounds, len(baselineRounds)))
		}

		seed := options.Seed + int64(idx)*7919
		if len(baselineRounds) > 0 {
			gateCase.MedianRatio = bootstrapRoundMedianRatio(baselineRounds, candidateRounds, seed, options)
			if gateCase.MedianRatio.Lower > 1+options.RegressionThreshold {
				gateCase.Passed = false
				gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("median regression lower bound %.4f exceeds %.4f", gateCase.MedianRatio.Lower, 1+options.RegressionThreshold))
			}
		}

		if gateCase.BaselineSamples >= minimumP95Samples && gateCase.CandidateSamples >= minimumP95Samples {
			interval := bootstrapStratifiedP95Ratio(baselineRounds, candidateRounds, seed+1, options)
			gateCase.P95Ratio = &interval
			if interval.Lower > 1+options.RegressionThreshold {
				gateCase.Passed = false
				gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("p95 regression lower bound %.4f exceeds %.4f", interval.Lower, 1+options.RegressionThreshold))
			}
		} else {
			gateCase.Passed = false
			gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("need at least %d warm samples per side for p95, got %d/%d", minimumP95Samples, gateCase.BaselineSamples, gateCase.CandidateSamples))
		}

		if target, isTarget := targetPerformanceGates[key.name]; isTarget && key.backend == ModePostgresSQL {
			gateCase.TargetBaselineLimit = &target.baselineUpper
			if len(baselineRounds) > 0 && gateCase.MedianRatio.Upper > target.baselineUpper {
				gateCase.Passed = false
				gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("target median upper bound %.4f exceeds %.4f", gateCase.MedianRatio.Upper, target.baselineUpper))
			}

			neo4jKey := performanceKey{dataset: key.dataset, name: key.name, backend: ModeNeo4j}
			neo4jRounds, postgresRounds := matchedRounds(candidateSeries[neo4jKey], candidateSeries[key])
			gateCase.BackendRatioLimit = &target.backendUpper
			if len(neo4jRounds) < minimumGateRounds {
				gateCase.Passed = false
				gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("need at least %d matched PostgreSQL/Neo4j rounds, got %d", minimumGateRounds, len(neo4jRounds)))
			} else {
				// matchedRounds returns its first input as the denominator. Passing
				// Neo4j first therefore yields PostgreSQL/Neo4j.
				interval := bootstrapRoundMedianRatio(neo4jRounds, postgresRounds, seed+2, options)
				gateCase.BackendRatio = &interval
				if interval.Upper > target.backendUpper {
					gateCase.Passed = false
					gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("PostgreSQL/Neo4j upper bound %.4f exceeds %.4f", interval.Upper, target.backendUpper))
				}
			}
		}

		if !gateCase.Passed {
			report.Passed = false
		}
		report.Cases = append(report.Cases, gateCase)
	}

	return report, nil
}

func collectWarmSeries(records []CaseResult) map[performanceKey]roundSamples {
	series := map[performanceKey]roundSamples{}
	for _, record := range records {
		if record.Status != StatusOK {
			continue
		}
		key := performanceKey{dataset: record.Dataset, name: record.Name, backend: record.ExecutionMode}
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

func confidenceInterval(estimate float64, samples []float64, confidence float64) RatioInterval {
	alpha := (1 - confidence) / 2
	return RatioInterval{
		Estimate: estimate,
		Lower:    quantile(samples, alpha),
		Upper:    quantile(samples, 1-alpha),
	}
}

func durationQuantile(values []time.Duration, probability float64) float64 {
	numeric := make([]float64, len(values))
	for idx, value := range values {
		numeric[idx] = float64(value)
	}
	return quantile(numeric, probability)
}

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

func sortedRounds(samples roundSamples) []int {
	rounds := make([]int, 0, len(samples))
	for round := range samples {
		rounds = append(rounds, round)
	}
	sort.Ints(rounds)
	return rounds
}

func flattenSamples(samples roundSamples, rounds []int) []time.Duration {
	var flattened []time.Duration
	for _, round := range rounds {
		flattened = append(flattened, samples[round]...)
	}
	return flattened
}

func resampleDurations(rng *rand.Rand, values []time.Duration) []time.Duration {
	resampled := make([]time.Duration, len(values))
	for idx := range resampled {
		resampled[idx] = values[rng.Intn(len(values))]
	}
	return resampled
}

func sampleCount(samples roundSamples) int {
	count := 0
	for _, values := range samples {
		count += len(values)
	}
	return count
}

func fileSHA256(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(content)
	return hex.EncodeToString(digest[:]), nil
}

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
