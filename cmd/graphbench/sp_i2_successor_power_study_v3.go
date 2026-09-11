// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
)

const spI2SuccessorPowerStudyV3Implementation = "sp-i2-power-simulation-v3/chacha8-sha256-normal-pivot"

type spI2SuccessorPowerStudyV3 struct {
	Schema         string                         `json:"schema"`
	Generation     string                         `json:"generation"`
	Implementation string                         `json:"implementation"`
	Status         string                         `json:"status"`
	ArchivedTrace  spI2SuccessorArchivedTraceV3   `json:"archived_trace"`
	Design         spI2SuccessorPowerDesignV3     `json:"design"`
	Statistics     spI2SuccessorPowerStatisticsV3 `json:"statistics"`
	Gates          spI2ProtocolGatesV2            `json:"gates"`
	Scenarios      []spI2SimulationScenarioV2     `json:"scenarios"`
}

type spI2SuccessorArchivedTraceV3 struct {
	SourceCommit          string `json:"source_commit"`
	BaselineTraceSHA256   string `json:"baseline_trace_sha256"`
	CandidateTraceSHA256  string `json:"candidate_trace_sha256"`
	Rounds                int    `json:"rounds"`
	CaseRecordsPerRound   int    `json:"case_records_per_round"`
	TimedSamplesPerRecord int    `json:"timed_samples_per_record"`
}

type spI2SuccessorPowerDesignV3 struct {
	Blocks                      int    `json:"blocks"`
	OrdinaryWarmups             int    `json:"ordinary_warmups"`
	TimedSamplesPerArmCaseBlock int    `json:"timed_samples_per_arm_case_block"`
	PoolSize                    int    `json:"pool_size"`
	Isolation                   string `json:"isolation"`
	ArmOrder                    string `json:"arm_order"`
}

type spI2SuccessorPowerStatisticsV3 struct {
	BootstrapConfidence       float64 `json:"bootstrap_confidence"`
	BootstrapReplicates       int     `json:"bootstrap_replicates"`
	Quantile                  string  `json:"quantile"`
	WilsonConfidence          float64 `json:"wilson_confidence"`
	SimulationRunsPerScenario int     `json:"simulation_runs_per_scenario"`
	RequiredPowerLower        float64 `json:"required_power_lower"`
	RequiredCoverage          float64 `json:"required_coverage"`
	P95BoundaryFalsePassUpper float64 `json:"p95_boundary_false_pass_upper"`
	DecisionFalsePassUpper    float64 `json:"decision_false_pass_upper"`
	TraceRescalingTransform   string  `json:"trace_rescaling_transform"`
}

type SPI2SuccessorPowerStudyReportV3 struct {
	Schema                 string                                `json:"schema"`
	Generation             string                                `json:"generation"`
	Implementation         string                                `json:"implementation"`
	ProtocolSHA256         string                                `json:"protocol_sha256"`
	CalibrationScale       float64                               `json:"calibration_scale"`
	LogStandardErrors      spI2SimulationErrorsV2                `json:"log_standard_errors"`
	AbsoluteStandardErrors spI2SimulationErrorsV2                `json:"absolute_standard_errors_us"`
	Passed                 bool                                  `json:"passed"`
	Scenarios              []SPI2PowerSimulationScenarioReportV2 `json:"scenarios"`
}

func loadSPI2SuccessorPowerStudyV3(path string) (spI2SuccessorPowerStudyV3, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return spI2SuccessorPowerStudyV3{}, "", fmt.Errorf("read SP-I2 successor power study: %w", err)
	}
	var study spI2SuccessorPowerStudyV3
	if err := json.Unmarshal(raw, &study); err != nil {
		return spI2SuccessorPowerStudyV3{}, "", fmt.Errorf("decode SP-I2 successor power study: %w", err)
	}
	if err := validateSPI2SuccessorPowerStudyV3(study); err != nil {
		return spI2SuccessorPowerStudyV3{}, "", err
	}
	digest := sha256.Sum256(raw)
	return study, hex.EncodeToString(digest[:]), nil
}

func validateSPI2SuccessorPowerStudyV3(study spI2SuccessorPowerStudyV3) error {
	if study.Schema != "sp-i2-successor-power-study-v3" || study.Generation != "sp-i2-distance-v3-power-study" ||
		study.Implementation != spI2SuccessorPowerStudyV3Implementation || study.Status != "prospective" {
		return fmt.Errorf("SP-I2 successor power study identity is invalid")
	}
	trace := study.ArchivedTrace
	if trace.SourceCommit != "3865cbc57758b7b20b7ffe431f27235873422eed" ||
		trace.BaselineTraceSHA256 != "ac3ceb27ee92e3f4e21e3994ff9ee82d483b8081e9d44ddcef8e695ffdb1b6d0" ||
		trace.CandidateTraceSHA256 != "f6d79e81bdaafedaa95568d57140c14e0808fbb6fc261387abc916081137785a" ||
		trace.Rounds != 20 || trace.CaseRecordsPerRound != 12 || trace.TimedSamplesPerRecord != 10 {
		return fmt.Errorf("SP-I2 successor power study archive contract is invalid")
	}
	design := study.Design
	if design.Blocks != 800 || design.OrdinaryWarmups != 25 || design.TimedSamplesPerArmCaseBlock != 100 ||
		design.PoolSize != 1 || design.Isolation != "repeatable_read" ||
		design.ArmOrder != "odd_incumbent_then_candidate_even_candidate_then_incumbent" {
		return fmt.Errorf("SP-I2 successor power study design is invalid")
	}
	stats := study.Statistics
	if stats.BootstrapConfidence != 0.975 || stats.BootstrapReplicates != 100_000 || stats.Quantile != "nearest_rank" ||
		stats.WilsonConfidence != 0.95 || stats.SimulationRunsPerScenario != 20_000 || stats.RequiredPowerLower != 0.90 ||
		stats.RequiredCoverage != 0.975 || stats.P95BoundaryFalsePassUpper != 0.015 || stats.DecisionFalsePassUpper != 0.0275 ||
		stats.TraceRescalingTransform != "scaled_v2_calibration_then_paired_empirical_round_drift" {
		return fmt.Errorf("SP-I2 successor power study statistics are invalid")
	}
	if study.Gates != (spI2ProtocolGatesV2{TargetMedianRatioUpper: 0.95, TargetMedianSavingLowerUS: 100, ControlMedianRatioUpper: 1.10, ControlMedianOverheadUpperUS: 100, P95RatioUpper: 1.05, ControlP95OverheadUpperUS: 100, AAEquivalenceRatio: 1.05, AAFirstPositionRatioUpper: 1.10, AAFirstPositionOverheadUpperUS: 100}) {
		return fmt.Errorf("SP-I2 successor power study gates are invalid")
	}
	if len(study.Scenarios) != 11 {
		return fmt.Errorf("SP-I2 successor power study scenario count is invalid")
	}
	for _, scenario := range study.Scenarios {
		seed := sha256.Sum256([]byte("sp-i2-power-study-v3\x00" + scenario.Name))
		if scenario.Name == "" || scenario.BaselineP50US <= 0 || scenario.BaselineP95US <= scenario.BaselineP50US ||
			scenario.CandidateP50US <= 0 || scenario.CandidateP95US <= scenario.CandidateP50US || scenario.Seed != hex.EncodeToString(seed[:]) {
			return fmt.Errorf("SP-I2 successor power study scenario %q is invalid", scenario.Name)
		}
	}
	return nil
}

func buildSPI2SuccessorPowerStudyReportV3(study spI2SuccessorPowerStudyV3, protocolSHA256, baselinePath, candidatePath string) (SPI2SuccessorPowerStudyReportV3, error) {
	if err := validateSPI2SuccessorPowerStudyV3(study); err != nil {
		return SPI2SuccessorPowerStudyReportV3{}, err
	}
	baselineSHA256, err := fileSHA256(baselinePath)
	if err != nil || baselineSHA256 != study.ArchivedTrace.BaselineTraceSHA256 {
		return SPI2SuccessorPowerStudyReportV3{}, fmt.Errorf("SP-I2 successor power study baseline trace digest is invalid")
	}
	candidateSHA256, err := fileSHA256(candidatePath)
	if err != nil || candidateSHA256 != study.ArchivedTrace.CandidateTraceSHA256 {
		return SPI2SuccessorPowerStudyReportV3{}, fmt.Errorf("SP-I2 successor power study candidate trace digest is invalid")
	}
	baseline, err := readJSONLFile(baselinePath)
	if err != nil {
		return SPI2SuccessorPowerStudyReportV3{}, err
	}
	candidate, err := readJSONLFile(candidatePath)
	if err != nil {
		return SPI2SuccessorPowerStudyReportV3{}, err
	}
	records := append(append([]CaseResult(nil), baseline...), candidate...)
	if err := verifySPI2SimulationTraceIdentityV2(records, study.ArchivedTrace.SourceCommit); err != nil {
		return SPI2SuccessorPowerStudyReportV3{}, err
	}
	calibration, err := deriveSPI2SimulationErrorsV2(baseline)
	if err != nil {
		return SPI2SuccessorPowerStudyReportV3{}, err
	}
	scale := math.Sqrt(float64(40*100) / float64(study.Design.Blocks*study.Design.TimedSamplesPerArmCaseBlock))
	model := spI2ProtocolV2{Design: spI2ProtocolDesignV2{Rounds: study.Design.Blocks}, Gates: study.Gates, Simulation: spI2ProtocolSimulationV2{
		RunsPerScenario: study.Statistics.SimulationRunsPerScenario, RequiredPowerLower: study.Statistics.RequiredPowerLower,
		RequiredCoverage: study.Statistics.RequiredCoverage, P95BoundaryFalsePassUpper: study.Statistics.P95BoundaryFalsePassUpper,
		DecisionFalsePassUpper: study.Statistics.DecisionFalsePassUpper, LogStandardErrors: scaleSPI2SimulationErrorsV3(calibration.log, scale),
		AbsoluteStandardErrorsUS: scaleSPI2SimulationErrorsV3(calibration.absolute, scale), Scenarios: study.Scenarios,
	}}
	model.Simulation.P50RoundDrift, err = spI2RoundDriftV2(records, false)
	if err != nil {
		return SPI2SuccessorPowerStudyReportV3{}, err
	}
	model.Simulation.P95RoundDrift, err = spI2RoundDriftV2(records, true)
	if err != nil {
		return SPI2SuccessorPowerStudyReportV3{}, err
	}
	report := SPI2SuccessorPowerStudyReportV3{Schema: "sp-i2-successor-power-study-report-v3", Generation: study.Generation, Implementation: study.Implementation, ProtocolSHA256: protocolSHA256, CalibrationScale: scale, LogStandardErrors: model.Simulation.LogStandardErrors, AbsoluteStandardErrors: model.Simulation.AbsoluteStandardErrorsUS, Passed: true}
	for _, scenario := range study.Scenarios {
		result, err := simulateSPI2ScenarioV2(model, scenario)
		if err != nil {
			return SPI2SuccessorPowerStudyReportV3{}, err
		}
		report.Scenarios = append(report.Scenarios, result)
		report.Passed = report.Passed && result.Passed
	}
	return report, nil
}

func scaleSPI2SimulationErrorsV3(errors spI2SimulationErrorsV2, scale float64) spI2SimulationErrorsV2 {
	return spI2SimulationErrorsV2{Pooled: errors.Pooled * scale, OrderStratum: errors.OrderStratum * scale, FirstPosition: errors.FirstPosition * scale}
}
