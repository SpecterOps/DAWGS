// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"
)

const spI2V1CycleControl = "GSP-I2-V1-TRAIN-cycle-control"

func createSPI2PowerSimulationReportV2(corpusRoot, baselineTrace, candidateTrace, output string) (SPI2PowerSimulationReportV2, error) {
	protocol, protocolSHA256, err := loadSPI2ProtocolV2(corpusRoot + "/protocols/sp_i2_distance_v2.json")
	if err != nil {
		return SPI2PowerSimulationReportV2{}, err
	}
	if err := verifySPI2SimulationCalibrationV2(protocol, baselineTrace, candidateTrace); err != nil {
		return SPI2PowerSimulationReportV2{}, err
	}
	report, err := buildSPI2PowerSimulationReportV2(protocol, protocolSHA256)
	if err != nil {
		return SPI2PowerSimulationReportV2{}, err
	}
	if err := writeSPI2PowerSimulationReportV2(output, report); err != nil {
		return SPI2PowerSimulationReportV2{}, err
	}
	return report, nil
}

func verifySPI2SimulationCalibrationV2(protocol spI2ProtocolV2, baselinePath, candidatePath string) error {
	if baselinePath == "" || candidatePath == "" {
		return fmt.Errorf("SP-I2 V2 simulation requires both archived V1 trace artifacts")
	}
	baselineSHA256, err := fileSHA256(baselinePath)
	if err != nil {
		return fmt.Errorf("hash SP-I2 simulation baseline trace: %w", err)
	}
	candidateSHA256, err := fileSHA256(candidatePath)
	if err != nil {
		return fmt.Errorf("hash SP-I2 simulation candidate trace: %w", err)
	}
	if baselineSHA256 != protocol.Simulation.BaselineTraceSHA256 || candidateSHA256 != protocol.Simulation.CandidateTraceSHA256 {
		return fmt.Errorf("SP-I2 simulation trace digest differs from the frozen protocol")
	}
	baseline, err := readJSONLFile(baselinePath)
	if err != nil {
		return fmt.Errorf("read SP-I2 simulation baseline trace: %w", err)
	}
	candidate, err := readJSONLFile(candidatePath)
	if err != nil {
		return fmt.Errorf("read SP-I2 simulation candidate trace: %w", err)
	}
	records := append(append([]CaseResult(nil), baseline...), candidate...)
	if err := verifySPI2SimulationTraceIdentityV2(records, protocol.Simulation.SourceCommit); err != nil {
		return err
	}
	p50Drift, err := spI2RoundDriftV2(records, false)
	if err != nil {
		return err
	}
	p95Drift, err := spI2RoundDriftV2(records, true)
	if err != nil {
		return err
	}
	if !equalSPI2FloatVectorsV2(p50Drift, protocol.Simulation.P50RoundDrift) || !equalSPI2FloatVectorsV2(p95Drift, protocol.Simulation.P95RoundDrift) {
		return fmt.Errorf("SP-I2 simulation round-drift vectors differ from the frozen protocol")
	}
	calibration, err := deriveSPI2SimulationErrorsV2(baseline)
	if err != nil {
		return err
	}
	if calibration.log != protocol.Simulation.LogStandardErrors || calibration.absolute != protocol.Simulation.AbsoluteStandardErrorsUS {
		return fmt.Errorf("SP-I2 simulation uncertainty calibration differs from the frozen protocol: log=%+v absolute=%+v", calibration.log, calibration.absolute)
	}
	return nil
}

func verifySPI2SimulationTraceIdentityV2(records []CaseResult, sourceCommit string) error {
	if len(records) != 240 {
		return fmt.Errorf("SP-I2 simulation traces require exactly 240 case records")
	}
	for _, record := range records {
		if !strings.Contains(record.Metadata.DAWGSVersion, sourceCommit) || record.Environment.Round < 1 || record.Environment.Round > 20 ||
			record.Stats.Iterations != 10 || len(record.Stats.Samples) < 10 {
			return fmt.Errorf("SP-I2 simulation trace identity or fixed V1 design is invalid")
		}
	}
	return nil
}

func spI2RoundDriftV2(records []CaseResult, p95 bool) ([]float64, error) {
	logs := make([][]float64, 20)
	for _, record := range records {
		value := record.Stats.Median
		if p95 {
			value = record.Stats.P95
		}
		if value <= 0 {
			return nil, fmt.Errorf("SP-I2 simulation trace contains a non-positive quantile")
		}
		logs[record.Environment.Round-1] = append(logs[record.Environment.Round-1], math.Log(float64(value)))
	}
	roundMeans := make([]float64, len(logs))
	grand := 0.0
	for index, values := range logs {
		if len(values) != 12 {
			return nil, fmt.Errorf("SP-I2 simulation round %d requires exactly 12 trace records", index+1)
		}
		for _, value := range values {
			roundMeans[index] += value
		}
		roundMeans[index] /= float64(len(values))
		grand += roundMeans[index]
	}
	grand /= float64(len(roundMeans))
	for index := range roundMeans {
		roundMeans[index] = math.Exp(roundMeans[index] - grand)
	}
	return roundMeans, nil
}

type spI2DerivedSimulationErrorsV2 struct {
	log      spI2SimulationErrorsV2
	absolute spI2SimulationErrorsV2
}

func deriveSPI2SimulationErrorsV2(records []CaseResult) (spI2DerivedSimulationErrorsV2, error) {
	var pooled []time.Duration
	cycleRecords := make([]CaseResult, 0, 20)
	for _, record := range records {
		if record.Name != spI2V1CycleControl {
			continue
		}
		cycleRecords = append(cycleRecords, record)
		for _, sample := range record.Stats.Samples {
			if sample.Classification == "warm" {
				pooled = append(pooled, sample.Duration)
			}
		}
	}
	if len(cycleRecords) != 20 || len(pooled) != 200 {
		return spI2DerivedSimulationErrorsV2{}, fmt.Errorf("SP-I2 simulation calibration requires the complete 20-round V1 cycle control")
	}
	sort.Slice(pooled, func(left, right int) bool { return pooled[left] < pooled[right] })
	p50 := float64(pooled[99])
	p95 := float64(pooled[189])
	exponent := math.Log(2) / math.Log(p95/p50)
	calibrated := roundSamples{}
	for _, record := range cycleRecords {
		for _, sample := range record.Stats.Samples {
			if sample.Classification != "warm" {
				continue
			}
			microseconds := time.Duration(1000 * math.Pow(float64(sample.Duration)/p50, exponent))
			for range 10 {
				calibrated[record.Environment.Round] = append(calibrated[record.Environment.Round], microseconds*time.Microsecond)
			}
		}
		calibrated[record.Environment.Round+20] = append([]time.Duration(nil), calibrated[record.Environment.Round]...)
	}
	interval, err := bootstrapSPI2HierarchicalTailV2(calibrated, calibrated, "simulation-calibration", "v1-cycle-control", "p95", 0.95, 0.975, 100_000)
	if err != nil {
		return spI2DerivedSimulationErrorsV2{}, err
	}
	z := 2.241402727604947
	pooledLog := roundSPI2CalibrationUpV2((math.Log(interval.Ratio.Upper)-math.Log(interval.Ratio.Lower))/(2*z), 1_000_000)
	pooledAbsolute := roundSPI2CalibrationUpV2(math.Max(math.Abs(float64(interval.Change.Lower/time.Microsecond)), math.Abs(float64(interval.Change.Upper/time.Microsecond)))/z, 1_000)
	stratumLog := roundSPI2CalibrationUpV2(pooledLog*math.Sqrt2, 1_000_000)
	stratumAbsolute := roundSPI2CalibrationUpV2(pooledAbsolute*math.Sqrt2, 1_000)
	return spI2DerivedSimulationErrorsV2{
		log:      spI2SimulationErrorsV2{Pooled: pooledLog, OrderStratum: stratumLog, FirstPosition: stratumLog},
		absolute: spI2SimulationErrorsV2{Pooled: pooledAbsolute, OrderStratum: stratumAbsolute, FirstPosition: stratumAbsolute},
	}, nil
}

func roundSPI2CalibrationUpV2(value, precision float64) float64 {
	return math.Ceil(value*precision) / precision
}

func equalSPI2FloatVectorsV2(left, right []float64) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if math.Abs(left[index]-right[index]) > 1e-12 {
			return false
		}
	}
	return true
}

func writeSPI2PowerSimulationReportV2(path string, report SPI2PowerSimulationReportV2) (err error) {
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
