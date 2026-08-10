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
	perfGateVersion       = 2
	defaultBootstrapCount = 10_000
	minimumGateRounds     = 5
	minimumP95Samples     = 150
)

type PerfGateOptions struct {
	Seed                int64
	Confidence          float64
	RegressionThreshold float64
	BootstrapCount      int
	DeclaredBackends    []DeclaredCaseBackend
	TargetNames         []string
	MaterialityRatio    float64
	MaterialityAbsolute time.Duration
	DiagnosticMode      bool
}

type RatioInterval struct {
	Estimate float64 `json:"estimate"`
	Lower    float64 `json:"lower"`
	Upper    float64 `json:"upper"`
}

type DurationInterval struct {
	Estimate time.Duration `json:"estimate"`
	Lower    time.Duration `json:"lower"`
	Upper    time.Duration `json:"upper"`
}

type PerfGateCase struct {
	Dataset             string            `json:"dataset"`
	Name                string            `json:"name"`
	Backend             ExecutionMode     `json:"backend"`
	Rounds              int               `json:"rounds"`
	BaselineSamples     int               `json:"baseline_samples"`
	CandidateSamples    int               `json:"candidate_samples"`
	BaselineStatus      string            `json:"baseline_status,omitempty"`
	CandidateStatus     string            `json:"candidate_status,omitempty"`
	OracleOnly          bool              `json:"oracle_only,omitempty"`
	MedianRatio         RatioInterval     `json:"median_ratio"`
	P95Ratio            *RatioInterval    `json:"p95_ratio,omitempty"`
	MedianSaving        *DurationInterval `json:"median_saving,omitempty"`
	MaterialityRatio    *float64          `json:"materiality_ratio_upper_limit,omitempty"`
	MaterialityAbsolute *time.Duration    `json:"materiality_absolute_lower_limit,omitempty"`
	Passed              bool              `json:"passed"`
	Reasons             []string          `json:"reasons,omitempty"`
}

type PerfGateReport struct {
	Version             int            `json:"version"`
	Seed                int64          `json:"seed"`
	Confidence          float64        `json:"confidence_level"`
	RegressionThreshold float64        `json:"regression_threshold"`
	BaselineSHA256      string         `json:"baseline_sha256"`
	CandidateSHA256     string         `json:"candidate_sha256"`
	DeclarationSHA256   string         `json:"declaration_sha256,omitempty"`
	Passed              bool           `json:"passed"`
	Cases               []PerfGateCase `json:"cases"`
}

type performanceKey struct {
	dataset string
	name    string
	backend ExecutionMode
}

type roundSamples map[int][]time.Duration

func comparePerformanceArtifacts(baselinePath, candidatePath, outputPath string, options PerfGateOptions) (bool, error) {
	baseline, err := readJSONLFile(baselinePath)
	if err != nil {
		return false, fmt.Errorf("read baseline: %w", err)
	}

	candidate, err := readJSONLFile(candidatePath)
	if err != nil {
		return false, fmt.Errorf("read candidate: %w", err)
	}
	if err := validatePerformanceArtifactSelections(baseline, candidate, options.DiagnosticMode); err != nil {
		return false, err
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

func validatePerformanceArtifactSelections(baseline, candidate []CaseResult, diagnosticMode bool) error {
	if !diagnosticMode && (hasAdaptiveDiscoveryRecord(baseline) || hasAdaptiveDiscoveryRecord(candidate)) {
		return fmt.Errorf("adaptive-discovery artifacts are refused by the complete performance gate")
	}
	baselineSelection, baselineErr := selectionIdentity(baseline)
	candidateSelection, candidateErr := selectionIdentity(candidate)
	// Version-1 historical artifacts predate selection manifests and remain
	// valid only for the ordinary complete-corpus gate.
	if baselineErr != nil || candidateErr != nil {
		if diagnosticMode {
			return fmt.Errorf("diagnostic comparison requires selection manifests in both artifacts")
		}
		return nil
	}
	if baselineSelection.DiagnosticOnly || candidateSelection.DiagnosticOnly {
		if !diagnosticMode {
			return fmt.Errorf("diagnostic-only artifacts are refused by the complete performance gate")
		}
		if !baselineSelection.DiagnosticOnly || !candidateSelection.DiagnosticOnly {
			return fmt.Errorf("diagnostic comparison requires two diagnostic-only artifacts")
		}
		if baselineSelection.DeclarationSHA256 != candidateSelection.DeclarationSHA256 {
			return fmt.Errorf("diagnostic artifact declarations differ: %s != %s", baselineSelection.DeclarationSHA256, candidateSelection.DeclarationSHA256)
		}
		return nil
	}
	if diagnosticMode {
		return fmt.Errorf("diagnostic comparison mode requires filtered diagnostic-only artifacts")
	}
	return nil
}

func hasAdaptiveDiscoveryRecord(records []CaseResult) bool {
	for _, record := range records {
		if record.ExistingGraph != nil && record.ExistingGraph.Adaptive {
			return true
		}
		if record.Environment != nil && record.Environment.Protocol == "adaptive_discovery" {
			return true
		}
	}
	return false
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
	if options.MaterialityRatio == 0 {
		options.MaterialityRatio = 0.95
	}
	if options.MaterialityRatio <= 0 || options.MaterialityRatio >= 1 {
		return PerfGateReport{}, fmt.Errorf("materiality ratio must be between 0 and 1")
	}
	if options.MaterialityAbsolute == 0 {
		options.MaterialityAbsolute = 100 * time.Microsecond
	}
	if options.MaterialityAbsolute < 0 {
		return PerfGateReport{}, fmt.Errorf("materiality absolute duration must not be negative")
	}

	baselineSeries := collectWarmSeries(baseline)
	candidateSeries := collectWarmSeries(candidate)
	keys := declaredPerformanceKeys(options.DeclaredBackends, baseline, candidate)
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
		return PerfGateReport{}, fmt.Errorf("artifacts and declaration contain no PostgreSQL or Neo4j cases")
	}
	targetNames := make(map[string]struct{}, len(options.TargetNames))
	for _, name := range options.TargetNames {
		targetNames[name] = struct{}{}
	}

	report := PerfGateReport{
		Version:             perfGateVersion,
		Seed:                options.Seed,
		Confidence:          options.Confidence,
		RegressionThreshold: options.RegressionThreshold,
		Passed:              true,
	}
	if len(options.DeclaredBackends) > 0 {
		report.DeclarationSHA256 = declarationSHA256(options.DeclaredBackends)
	}
	for idx, key := range keys {
		baselineStatus := artifactCaseStatus(baseline, key)
		candidateStatus := artifactCaseStatus(candidate, key)
		baselineRounds, candidateRounds := matchedRounds(baselineSeries[key], candidateSeries[key])
		gateCase := PerfGateCase{
			Dataset:          key.dataset,
			Name:             key.name,
			Backend:          key.backend,
			Rounds:           len(baselineRounds),
			BaselineSamples:  sampleCount(baselineRounds),
			CandidateSamples: sampleCount(candidateRounds),
			BaselineStatus:   baselineStatus,
			CandidateStatus:  candidateStatus,
			OracleOnly:       key.backend == ModeNeo4j,
			Passed:           true,
		}
		if candidateStatus != StatusOK {
			gateCase.Passed = false
			gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("required candidate record status is %s", candidateStatus))
		}
		// Neo4j is a correctness oracle. A successful record means its untimed
		// exact observation checks passed; its latency never affects this gate.
		if key.backend == ModeNeo4j {
			if !gateCase.Passed {
				report.Passed = false
			}
			report.Cases = append(report.Cases, gateCase)
			continue
		}
		if baselineStatus != StatusOK {
			gateCase.Passed = false
			gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("required baseline record status is %s", baselineStatus))
		}
		if len(baselineRounds) < minimumGateRounds {
			gateCase.Passed = false
			gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("need at least %d matched rounds, got %d", minimumGateRounds, len(baselineRounds)))
		}

		seed := options.Seed + int64(idx)*7919
		if len(baselineRounds) > 0 {
			gateCase.MedianRatio = bootstrapRoundMedianRatio(baselineRounds, candidateRounds, seed, options)
			saving := bootstrapRoundMedianSaving(baselineRounds, candidateRounds, seed+3, options)
			gateCase.MedianSaving = &saving
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

		if _, isTarget := targetNames[key.name]; isTarget && len(baselineRounds) > 0 {
			gateCase.MaterialityRatio = &options.MaterialityRatio
			gateCase.MaterialityAbsolute = &options.MaterialityAbsolute
			materialRatio := gateCase.MedianRatio.Upper <= options.MaterialityRatio
			materialAbsolute := gateCase.MedianSaving != nil && gateCase.MedianSaving.Lower >= options.MaterialityAbsolute
			if !materialRatio && !materialAbsolute {
				gateCase.Passed = false
				gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("target improvement is not material: median ratio upper %.4f > %.4f and saving lower %s < %s", gateCase.MedianRatio.Upper, options.MaterialityRatio, gateCase.MedianSaving.Lower, options.MaterialityAbsolute))
			}
		}

		if !gateCase.Passed {
			report.Passed = false
		}
		report.Cases = append(report.Cases, gateCase)
	}

	return report, nil
}

func declaredPerformanceKeys(declared []DeclaredCaseBackend, baseline, candidate []CaseResult) []performanceKey {
	unique := map[performanceKey]struct{}{}
	for _, item := range declared {
		if item.UnsupportedReason != "" {
			continue
		}
		if item.Backend == ModePostgresSQL || item.Backend == ModeNeo4j {
			unique[performanceKey{
				dataset: item.Dataset,
				name:    item.Name,
				backend: item.Backend,
			}] = struct{}{}
		}
	}
	if len(declared) == 0 {
		for _, records := range [][]CaseResult{baseline, candidate} {
			for _, record := range records {
				if record.ExecutionMode == ModePostgresSQL || record.ExecutionMode == ModeNeo4j {
					unique[performanceKey{
						dataset: record.Dataset,
						name:    record.Name,
						backend: record.ExecutionMode,
					}] = struct{}{}
				}
			}
		}
	}
	keys := make([]performanceKey, 0, len(unique))
	for key := range unique {
		keys = append(keys, key)
	}
	return keys
}

func artifactCaseStatus(records []CaseResult, key performanceKey) string {
	found := false
	for _, record := range records {
		if record.Dataset != key.dataset || record.Name != key.name || record.ExecutionMode != key.backend {
			continue
		}
		found = true
		if record.Status != StatusOK {
			return record.Status
		}
	}
	if !found {
		return "missing"
	}
	return StatusOK
}

func declarationSHA256(declared []DeclaredCaseBackend) string {
	items := append([]DeclaredCaseBackend(nil), declared...)
	sort.Slice(items, func(i, j int) bool {
		if items[i].Dataset != items[j].Dataset {
			return items[i].Dataset < items[j].Dataset
		}
		if items[i].Name != items[j].Name {
			return items[i].Name < items[j].Name
		}
		if items[i].Backend != items[j].Backend {
			return items[i].Backend < items[j].Backend
		}
		return items[i].UnsupportedReason < items[j].UnsupportedReason
	})
	digest := sha256.New()
	for _, item := range items {
		fmt.Fprintf(digest, "%s\x00%s\x00%s\x00%s\n", item.Dataset, item.Name, item.Backend, item.UnsupportedReason)
	}

	return hex.EncodeToString(digest.Sum(nil))
}

func collectWarmSeries(records []CaseResult) map[performanceKey]roundSamples {
	series := map[performanceKey]roundSamples{}
	for _, record := range records {
		if record.Status != StatusOK {
			continue
		}
		key := performanceKey{
			dataset: record.Dataset,
			name:    record.Name,
			backend: record.ExecutionMode,
		}

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

func bootstrapRoundMedianSaving(baseline, candidate roundSamples, seed int64, options PerfGateOptions) DurationInterval {
	rounds := sortedRounds(baseline)
	baselineMedians := make([]float64, len(rounds))
	candidateMedians := make([]float64, len(rounds))
	for idx, round := range rounds {
		baselineMedians[idx] = durationQuantile(baseline[round], 0.5)
		candidateMedians[idx] = durationQuantile(candidate[round], 0.5)
	}
	estimate := quantile(baselineMedians, 0.5) - quantile(candidateMedians, 0.5)
	rng := rand.New(rand.NewSource(seed)) // #nosec G404 -- deterministic statistical resampling
	savings := make([]float64, options.BootstrapCount)
	resampledBaseline := make([]float64, len(rounds))
	resampledCandidate := make([]float64, len(rounds))
	for iteration := range savings {
		for idx := range rounds {
			selected := rng.Intn(len(rounds))
			resampledBaseline[idx] = baselineMedians[selected]
			resampledCandidate[idx] = candidateMedians[selected]
		}
		savings[iteration] = quantile(resampledBaseline, 0.5) - quantile(resampledCandidate, 0.5)
	}
	interval := confidenceInterval(estimate, savings, options.Confidence)
	return DurationInterval{
		Estimate: time.Duration(interval.Estimate),
		Lower:    time.Duration(interval.Lower),
		Upper:    time.Duration(interval.Upper),
	}
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
