// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/hex"
	"fmt"
	"math"
	randv2 "math/rand/v2"
)

const spI2PowerSimulationV2 = "sp-i2-power-simulation-v2/chacha8-sha256-normal-pivot"

type SPI2PowerSimulationReportV2 struct {
	Schema         string                                `json:"schema"`
	Generation     string                                `json:"generation"`
	Implementation string                                `json:"implementation"`
	ProtocolSHA256 string                                `json:"protocol_sha256"`
	Passed         bool                                  `json:"passed"`
	Scenarios      []SPI2PowerSimulationScenarioReportV2 `json:"scenarios"`
}

type SPI2PowerSimulationScenarioReportV2 struct {
	Name                   string        `json:"name"`
	Kind                   string        `json:"kind"`
	Seed                   string        `json:"seed"`
	Runs                   int           `json:"runs"`
	SuccessfulDecisions    int           `json:"successful_decisions"`
	DecisionRate           float64       `json:"decision_rate"`
	DecisionWilson         RatioInterval `json:"decision_wilson_95"`
	CoveredIntervals       int           `json:"covered_intervals"`
	TotalIntervals         int           `json:"total_intervals"`
	CoverageRate           float64       `json:"coverage_rate"`
	CoverageWilson         RatioInterval `json:"coverage_wilson_95"`
	RequiredDecisionResult string        `json:"required_decision_result"`
	Passed                 bool          `json:"passed"`
}

type spI2SimulationIntervalV2 struct {
	lower     float64
	upper     float64
	trueValue float64
}

// buildSPI2PowerSimulationReportV2 executes the immutable prospective
// calibration matrix. The normal pivots are fitted to the archived V1/open
// traces, while the paired empirical drift vectors are resampled at the fixed
// 40-round design. Formal evidence continues to use the exact 100,000-draw
// hierarchical bootstrap; this simulation calibrates that frozen design
// without substituting a cheaper estimator in a formal report.
func buildSPI2PowerSimulationReportV2(protocol spI2ProtocolV2, protocolSHA256 string) (SPI2PowerSimulationReportV2, error) {
	if err := validateSPI2SimulationProtocolV2(protocol.Simulation); err != nil {
		return SPI2PowerSimulationReportV2{}, err
	}
	report := SPI2PowerSimulationReportV2{
		Schema:         "sp-i2-power-simulation-report-v2",
		Generation:     spI2GenerationV2,
		Implementation: spI2PowerSimulationV2,
		ProtocolSHA256: protocolSHA256,
		Passed:         true,
	}
	for _, scenario := range protocol.Simulation.Scenarios {
		result, err := simulateSPI2ScenarioV2(protocol, scenario)
		if err != nil {
			return SPI2PowerSimulationReportV2{}, err
		}
		report.Scenarios = append(report.Scenarios, result)
		report.Passed = report.Passed && result.Passed
	}
	return report, nil
}

func simulateSPI2ScenarioV2(protocol spI2ProtocolV2, scenario spI2SimulationScenarioV2) (SPI2PowerSimulationScenarioReportV2, error) {
	seedBytes, err := hex.DecodeString(scenario.Seed)
	if err != nil || len(seedBytes) != 32 {
		return SPI2PowerSimulationScenarioReportV2{}, fmt.Errorf("decode SP-I2 simulation seed for %s", scenario.Name)
	}
	var seed [32]byte
	copy(seed[:], seedBytes)
	rng := randv2.New(randv2.NewChaCha8(seed))
	runs := protocol.Simulation.RunsPerScenario
	successes, covered, intervals := 0, 0, 0
	for range runs {
		passed, nextCovered, nextIntervals := simulateSPI2StudyV2(protocol, scenario, rng)
		if passed {
			successes++
		}
		covered += nextCovered
		intervals += nextIntervals
	}
	decisionWilson := spI2WilsonIntervalV2(successes, runs)
	coverageWilson := spI2WilsonIntervalV2(covered, intervals)
	result := SPI2PowerSimulationScenarioReportV2{
		Name:                scenario.Name,
		Kind:                scenario.Kind,
		Seed:                scenario.Seed,
		Runs:                runs,
		SuccessfulDecisions: successes,
		DecisionRate:        float64(successes) / float64(runs),
		DecisionWilson:      decisionWilson,
		CoveredIntervals:    covered,
		TotalIntervals:      intervals,
		CoverageRate:        float64(covered) / float64(intervals),
		CoverageWilson:      coverageWilson,
	}
	coveragePass := coverageWilson.Lower <= protocol.Simulation.RequiredCoverage && coverageWilson.Upper >= protocol.Simulation.RequiredCoverage
	switch scenario.Kind {
	case "aa_power", "aa_order_power", "target_power", "control_power":
		result.RequiredDecisionResult = "wilson_lower>=0.90"
		result.Passed = decisionWilson.Lower >= protocol.Simulation.RequiredPowerLower && coveragePass
	case "aa_boundary", "aa_order_boundary":
		result.RequiredDecisionResult = "false_pass_rate<=0.015"
		result.Passed = result.DecisionRate <= protocol.Simulation.P95BoundaryFalsePassUpper && coveragePass
	case "target_boundary", "control_boundary":
		result.RequiredDecisionResult = "false_pass_rate<=0.0275"
		result.Passed = result.DecisionRate <= protocol.Simulation.DecisionFalsePassUpper && coveragePass
	default:
		return SPI2PowerSimulationScenarioReportV2{}, fmt.Errorf("unsupported SP-I2 simulation kind %q", scenario.Kind)
	}
	return result, nil
}

func simulateSPI2StudyV2(protocol spI2ProtocolV2, scenario spI2SimulationScenarioV2, rng *randv2.Rand) (bool, int, int) {
	z := 2.241402727604947
	logSE := protocol.Simulation.LogStandardErrors
	absSE := protocol.Simulation.AbsoluteStandardErrorsUS
	p50Drift := meanResampledSPI2DriftV2(rng, protocol.Simulation.P50RoundDrift, protocol.Design.Rounds)
	p95Drift := meanResampledSPI2DriftV2(rng, protocol.Simulation.P95RoundDrift, protocol.Design.Rounds)

	pooledP50 := simulatedSPI2RatioIntervalV2(rng, scenario.CandidateP50US/scenario.BaselineP50US, logSE.Pooled, z)
	pooledP95 := simulatedSPI2RatioIntervalV2(rng, scenario.CandidateP95US/scenario.BaselineP95US, logSE.Pooled, z)
	pooledP50Change := simulatedSPI2AbsoluteIntervalV2(rng, (scenario.CandidateP50US-scenario.BaselineP50US)*p50Drift, absSE.Pooled, z)
	pooledP95Change := simulatedSPI2AbsoluteIntervalV2(rng, (scenario.CandidateP95US-scenario.BaselineP95US)*p95Drift, absSE.Pooled, z)
	all := []spI2SimulationIntervalV2{pooledP50, pooledP95, pooledP50Change, pooledP95Change}

	if scenario.Kind == "target_power" || scenario.Kind == "target_boundary" {
		pass := (pooledP50.upper <= protocol.Gates.TargetMedianRatioUpper || -pooledP50Change.upper >= float64(protocol.Gates.TargetMedianSavingLowerUS)) &&
			pooledP95.upper <= protocol.Gates.P95RatioUpper
		covered, total := coveredSPI2IntervalsV2(all)
		return pass, covered, total
	}
	if scenario.Kind == "control_power" || scenario.Kind == "control_boundary" {
		pass := (pooledP50.upper <= protocol.Gates.ControlMedianRatioUpper || pooledP50Change.upper <= float64(protocol.Gates.ControlMedianOverheadUpperUS)) &&
			pooledP95.upper <= protocol.Gates.P95RatioUpper && pooledP95Change.upper <= float64(protocol.Gates.ControlP95OverheadUpperUS)
		covered, total := coveredSPI2IntervalsV2(all)
		return pass, covered, total
	}

	oddP50 := simulatedSPI2RatioIntervalV2(rng, scenario.CandidateP50US*scenario.OddCandidateMultiplier/scenario.BaselineP50US, logSE.OrderStratum, z)
	oddP95 := simulatedSPI2RatioIntervalV2(rng, scenario.CandidateP95US*scenario.OddCandidateMultiplier/scenario.BaselineP95US, logSE.OrderStratum, z)
	evenP50 := simulatedSPI2RatioIntervalV2(rng, scenario.CandidateP50US*scenario.EvenCandidateMultiplier/scenario.BaselineP50US, logSE.OrderStratum, z)
	evenP95 := simulatedSPI2RatioIntervalV2(rng, scenario.CandidateP95US*scenario.EvenCandidateMultiplier/scenario.BaselineP95US, logSE.OrderStratum, z)
	oddChange := simulatedSPI2AbsoluteIntervalV2(rng, (scenario.CandidateP95US*scenario.OddCandidateMultiplier-scenario.BaselineP95US)*p95Drift, absSE.OrderStratum, z)
	evenChange := simulatedSPI2AbsoluteIntervalV2(rng, (scenario.CandidateP95US*scenario.EvenCandidateMultiplier-scenario.BaselineP95US)*p95Drift, absSE.OrderStratum, z)
	all = append(all, oddP50, oddP95, evenP50, evenP95, oddChange, evenChange)
	firstPass := true
	for range 2 {
		firstRatio := simulatedSPI2RatioIntervalV2(rng, 1, logSE.FirstPosition, z)
		firstChange := simulatedSPI2AbsoluteIntervalV2(rng, 0, absSE.FirstPosition, z)
		all = append(all, firstRatio, firstChange)
		firstPass = firstPass && firstRatio.upper <= protocol.Gates.AAFirstPositionRatioUpper && firstChange.upper <= float64(protocol.Gates.AAFirstPositionOverheadUpperUS)
	}
	equivalence := protocol.Gates.AAEquivalenceRatio
	lower := 1 / equivalence
	pass := intervalInsideSPI2V2(pooledP50, lower, equivalence) && intervalInsideSPI2V2(pooledP95, lower, equivalence) && intervalInsideSPI2V2(pooledP95Change, -100, 100) &&
		intervalInsideSPI2V2(oddP50, lower, equivalence) && intervalInsideSPI2V2(oddP95, lower, equivalence) && intervalInsideSPI2V2(oddChange, -100, 100) &&
		intervalInsideSPI2V2(evenP50, lower, equivalence) && intervalInsideSPI2V2(evenP95, lower, equivalence) && intervalInsideSPI2V2(evenChange, -100, 100) && firstPass
	covered, total := coveredSPI2IntervalsV2(all)
	return pass, covered, total
}

func simulatedSPI2RatioIntervalV2(rng *randv2.Rand, truth, standardError, z float64) spI2SimulationIntervalV2 {
	estimate := math.Log(truth) + standardError*standardNormalSPI2V2(rng)
	return spI2SimulationIntervalV2{lower: math.Exp(estimate - z*standardError), upper: math.Exp(estimate + z*standardError), trueValue: truth}
}

func simulatedSPI2AbsoluteIntervalV2(rng *randv2.Rand, truth, standardError, z float64) spI2SimulationIntervalV2 {
	estimate := truth + standardError*standardNormalSPI2V2(rng)
	return spI2SimulationIntervalV2{lower: estimate - z*standardError, upper: estimate + z*standardError, trueValue: truth}
}

func standardNormalSPI2V2(rng *randv2.Rand) float64 {
	u1 := (float64(rng.Uint64()>>11) + 0.5) / (1 << 53)
	u2 := (float64(rng.Uint64()>>11) + 0.5) / (1 << 53)
	return math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
}

func meanResampledSPI2DriftV2(rng *randv2.Rand, drift []float64, blocks int) float64 {
	total := 0.0
	for range blocks {
		total += drift[rng.Uint64N(uint64(len(drift)))]
	}
	return total / float64(blocks)
}

func intervalInsideSPI2V2(interval spI2SimulationIntervalV2, lower, upper float64) bool {
	return interval.lower >= lower && interval.upper <= upper
}

func coveredSPI2IntervalsV2(intervals []spI2SimulationIntervalV2) (int, int) {
	covered := 0
	for _, interval := range intervals {
		if interval.lower <= interval.trueValue && interval.upper >= interval.trueValue {
			covered++
		}
	}
	return covered, len(intervals)
}

func spI2WilsonIntervalV2(successes, total int) RatioInterval {
	if total <= 0 {
		return RatioInterval{}
	}
	z := 1.959963984540054
	n := float64(total)
	p := float64(successes) / n
	denominator := 1 + z*z/n
	center := (p + z*z/(2*n)) / denominator
	half := z * math.Sqrt(p*(1-p)/n+z*z/(4*n*n)) / denominator
	return RatioInterval{Estimate: p, Lower: center - half, Upper: center + half}
}
