// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"fmt"
	"math"
	randv2 "math/rand/v2"
	"slices"
	"sort"
	"time"
)

const spI2HierBootstrapV2 = "sp-i2-hier-bootstrap-v2/chacha8-sha256"

// spI2TailIntervalV2 contains the conjunctive relative and absolute tail
// estimands produced from the same deterministic hierarchical draws.
type spI2TailIntervalV2 struct {
	Ratio  RatioInterval
	Change DurationInterval
}

// spI2BootstrapSeedV2 derives a metric-local deterministic ChaCha8 stream.
// Keeping dataset, case, and metric in the domain separation makes report
// iteration order irrelevant.
func spI2BootstrapSeedV2(dataset, caseName, metric string) [32]byte {
	return sha256.Sum256([]byte("sp-i2-tail-bootstrap-v2\x00" + "1" + "\x00" + dataset + "\x00" + caseName + "\x00" + metric))
}

// bootstrapSPI2HierarchicalTailV2 resamples round pairs together, then samples
// each arm independently within every selected round occurrence. Ratios are
// intervalled on the log scale; changes are candidate minus baseline.
func bootstrapSPI2HierarchicalTailV2(
	baseline, candidate roundSamples,
	dataset, caseName, metric string,
	probability, confidence float64,
	replicates int,
) (spI2TailIntervalV2, error) {
	rounds, err := validateSPI2HierarchicalInputs(baseline, candidate, probability, confidence, replicates)
	if err != nil {
		return spI2TailIntervalV2{}, err
	}
	baselinePoint := durationQuantile(flattenSamples(baseline, rounds), probability)
	candidatePoint := durationQuantile(flattenSamples(candidate, rounds), probability)
	if baselinePoint <= 0 || candidatePoint <= 0 {
		return spI2TailIntervalV2{}, fmt.Errorf("SP-I2 hierarchical ratio requires positive quantiles")
	}

	baselineIndexed := indexSPI2RoundSamples(baseline, rounds)
	candidateIndexed := indexSPI2RoundSamples(candidate, rounds)
	baselineCounts := make([]int, len(baselineIndexed.values))
	candidateCounts := make([]int, len(candidateIndexed.values))
	rng := randv2.New(randv2.NewChaCha8(spI2BootstrapSeedV2(dataset, caseName, metric)))
	logRatios := make([]float64, replicates)
	changes := make([]float64, replicates)
	for iteration := range replicates {
		clear(baselineCounts)
		clear(candidateCounts)
		baselineDrawCount, candidateDrawCount := 0, 0
		for range rounds {
			selected := rng.Uint64N(uint64(len(rounds)))
			baselineValues := baselineIndexed.rounds[selected]
			candidateValues := candidateIndexed.rounds[selected]
			for range baselineValues {
				baselineCounts[baselineValues[rng.Uint64N(uint64(len(baselineValues)))]]++
				baselineDrawCount++
			}
			for range candidateValues {
				candidateCounts[candidateValues[rng.Uint64N(uint64(len(candidateValues)))]]++
				candidateDrawCount++
			}
		}
		baselineDraw := countedSPI2Quantile(baselineIndexed.values, baselineCounts, baselineDrawCount, probability)
		candidateDraw := countedSPI2Quantile(candidateIndexed.values, candidateCounts, candidateDrawCount, probability)
		if baselineDraw <= 0 || candidateDraw <= 0 {
			return spI2TailIntervalV2{}, fmt.Errorf("SP-I2 hierarchical draw %d produced a non-positive quantile", iteration)
		}
		logRatios[iteration] = math.Log(candidateDraw / baselineDraw)
		changes[iteration] = candidateDraw - baselineDraw
	}

	alpha := (1 - confidence) / 2
	return spI2TailIntervalV2{
		Ratio: RatioInterval{
			Estimate: candidatePoint / baselinePoint,
			Lower:    math.Exp(quantile(logRatios, alpha)),
			Upper:    math.Exp(quantile(logRatios, 1-alpha)),
		},
		Change: DurationInterval{
			Estimate: time.Duration(candidatePoint - baselinePoint),
			Lower:    time.Duration(quantile(changes, alpha)),
			Upper:    time.Duration(quantile(changes, 1-alpha)),
		},
	}, nil
}

type spI2IndexedRoundSamples struct {
	values []time.Duration
	rounds [][]int
}

// indexSPI2RoundSamples converts native durations to stable sorted-value
// indexes. Bootstrap draws then increment counts and scan to the nearest-rank
// quantile instead of allocating and sorting pooled samples 100,000 times.
func indexSPI2RoundSamples(samples roundSamples, rounds []int) spI2IndexedRoundSamples {
	var values []time.Duration
	for _, round := range rounds {
		values = append(values, samples[round]...)
	}
	sort.Slice(values, func(left, right int) bool { return values[left] < values[right] })
	values = slices.Compact(values)
	valueIndex := make(map[time.Duration]int, len(values))
	for index, value := range values {
		valueIndex[value] = index
	}
	indexed := spI2IndexedRoundSamples{values: values, rounds: make([][]int, len(rounds))}
	for roundIndex, round := range rounds {
		indexed.rounds[roundIndex] = make([]int, len(samples[round]))
		for sampleIndex, value := range samples[round] {
			indexed.rounds[roundIndex][sampleIndex] = valueIndex[value]
		}
	}
	return indexed
}

func countedSPI2Quantile(values []time.Duration, counts []int, total int, probability float64) float64 {
	rank := int(math.Ceil(probability * float64(total)))
	if rank < 1 {
		rank = 1
	}
	seen := 0
	for index, count := range counts {
		seen += count
		if seen >= rank {
			return float64(values[index])
		}
	}
	return math.NaN()
}

// bootstrapSPI2RoundMedianV2 uses only paired round-median resampling, as
// preregistered. Saving is baseline minus candidate.
func bootstrapSPI2RoundMedianV2(
	baseline, candidate roundSamples,
	dataset, caseName, metric string,
	confidence float64,
	replicates int,
) (RatioInterval, DurationInterval, error) {
	rounds, err := validateSPI2HierarchicalInputs(baseline, candidate, 0.5, confidence, replicates)
	if err != nil {
		return RatioInterval{}, DurationInterval{}, err
	}
	baselineMedians := make([]float64, len(rounds))
	candidateMedians := make([]float64, len(rounds))
	for index, round := range rounds {
		baselineMedians[index] = durationQuantile(baseline[round], 0.5)
		candidateMedians[index] = durationQuantile(candidate[round], 0.5)
		if baselineMedians[index] <= 0 || candidateMedians[index] <= 0 {
			return RatioInterval{}, DurationInterval{}, fmt.Errorf("SP-I2 round medians must be positive")
		}
	}
	baselinePoint := quantile(baselineMedians, 0.5)
	candidatePoint := quantile(candidateMedians, 0.5)
	rng := randv2.New(randv2.NewChaCha8(spI2BootstrapSeedV2(dataset, caseName, metric)))
	logRatios := make([]float64, replicates)
	savings := make([]float64, replicates)
	resampledBaseline := make([]float64, len(rounds))
	resampledCandidate := make([]float64, len(rounds))
	for iteration := range replicates {
		for index := range rounds {
			selected := rng.Uint64N(uint64(len(rounds)))
			resampledBaseline[index] = baselineMedians[selected]
			resampledCandidate[index] = candidateMedians[selected]
		}
		baselineDraw := quantile(resampledBaseline, 0.5)
		candidateDraw := quantile(resampledCandidate, 0.5)
		logRatios[iteration] = math.Log(candidateDraw / baselineDraw)
		savings[iteration] = baselineDraw - candidateDraw
	}
	alpha := (1 - confidence) / 2
	return RatioInterval{
			Estimate: candidatePoint / baselinePoint,
			Lower:    math.Exp(quantile(logRatios, alpha)),
			Upper:    math.Exp(quantile(logRatios, 1-alpha)),
		}, DurationInterval{
			Estimate: time.Duration(baselinePoint - candidatePoint),
			Lower:    time.Duration(quantile(savings, alpha)),
			Upper:    time.Duration(quantile(savings, 1-alpha)),
		}, nil
}

func validateSPI2HierarchicalInputs(baseline, candidate roundSamples, probability, confidence float64, replicates int) ([]int, error) {
	if probability <= 0 || probability > 1 || math.IsNaN(probability) {
		return nil, fmt.Errorf("SP-I2 hierarchical probability must be in (0,1]")
	}
	if confidence <= 0 || confidence >= 1 || math.IsNaN(confidence) {
		return nil, fmt.Errorf("SP-I2 hierarchical confidence must be in (0,1)")
	}
	if replicates <= 0 {
		return nil, fmt.Errorf("SP-I2 hierarchical bootstrap count must be positive")
	}
	baselineRounds := sortedRounds(baseline)
	candidateRounds := sortedRounds(candidate)
	if len(baselineRounds) == 0 || !slices.Equal(baselineRounds, candidateRounds) {
		return nil, fmt.Errorf("SP-I2 hierarchical arms require identical nonempty round sets")
	}
	for _, round := range baselineRounds {
		if len(baseline[round]) == 0 || len(candidate[round]) == 0 {
			return nil, fmt.Errorf("SP-I2 hierarchical round %d contains an empty arm", round)
		}
		if len(baseline[round]) != len(candidate[round]) {
			return nil, fmt.Errorf("SP-I2 hierarchical round %d contains unequal arm sample counts", round)
		}
	}
	return baselineRounds, nil
}
