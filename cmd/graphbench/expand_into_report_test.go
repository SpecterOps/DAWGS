// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestBuildExpandIntoStudyReportValidatesThreeArmEvidence verifies exactness, Williams order, ratios, winners, and physical-plan classification.
func TestBuildExpandIntoStudyReportValidatesThreeArmEvidence(t *testing.T) {
	var records []CaseResult
	for round := 1; round <= 5; round++ {
		orderSpecs := make([]postgresReferenceSpec, len(expandIntoStudyArms))
		for idx, name := range expandIntoStudyArms {
			orderSpecs[idx].name = name
		}
		ordered := referenceSpecsForRound(orderSpecs, round)
		orders := map[string]int{}
		for idx, spec := range ordered {
			orders[spec.name] = idx + 2
		}
		record := CaseResult{
			Environment:         &RunEnvironment{Round: round, WarmupIterations: 5},
			PostgresEnvironment: &PostgresEnvironment{PlanCacheMode: "force_custom_plan"},
			Dataset:             "expand_into", Name: "pair", Category: "expand_into_one_hop",
			Shape: WorkloadShape{FixtureTier: "normal", QualificationSplit: "training"}, ExecutionMode: ModePostgresSQL, Status: StatusOK,
			RowCount: 1, ObservedRows: []string{`["edge"]`},
		}
		for idx, name := range expandIntoStudyArms {
			duration := time.Duration(100-idx*10) * time.Microsecond
			var samples []LatencySample
			for sample := 0; sample < 10; sample++ {
				samples = append(samples, LatencySample{Classification: "warm", Duration: duration + time.Duration(sample)})
			}
			plan := []string{"Nested Loop  (cost=0.00..1.00 rows=1 width=8)", "  ->  Index Scan using edge_start_id_idx on edge  (cost=0.00..1.00 rows=1 width=8)", "        Index Cond: (start_id = input_pairs.start_id)"}
			if name == "expand_into_pair_cache" {
				plan = []string{"Hash Join  (cost=0.00..1.00 rows=1 width=8)", "  ->  Memoize  (cost=0.00..1.00 rows=1 width=8)"}
			}
			record.PostgresReferences = append(record.PostgresReferences, PostgresReferenceResult{
				SchemaVersion: postgresReferenceSchemaVersion, Name: name, Architecture: "architecture-" + name,
				ImplementationID: name + "-v1", StateShape: "state", ObservationShape: "relationships",
				SemanticValidation: "exact_public_observation", Boundary: "relationships", TimingBoundary: "raw_pgx",
				FullComparator: true, MeasurementOrder: orders[name], SQL: "select '" + name + "'", SQLFingerprint: name,
				RowCount: 1, ObservedRows: []string{`["edge"]`}, Stats: DurationStats{WarmupIterations: 5, Samples: samples},
				PostgresPlan: plan,
			})
		}
		records = append(records, record)
	}

	report, err := buildExpandIntoStudyReport(records, ExpandIntoStudyOptions{Seed: 1, Confidence: .975, BootstrapCount: 100, Protocol: referencePairProtocolDiscovery})
	require.NoError(t, err)
	require.True(t, report.Passed)
	require.Equal(t, 1, report.TrainingCases)
	require.Zero(t, report.HoldoutCases)
	require.True(t, report.TrainingPassed)
	require.False(t, report.HoldoutPassed)
	require.False(t, report.QualificationPassed)
	require.Len(t, report.Cases, 1)
	entry := report.Cases[0]
	require.Equal(t, "expand_into_pair_cache", entry.Winner)
	require.Len(t, entry.ArmResults, 3)
	require.Nil(t, entry.ArmResults[0].MedianRatioToDirect)
	require.NotNil(t, entry.ArmResults[1].MedianRatioToDirect)
	require.True(t, entry.ArmResults[0].PlanModes[0].ParameterizedIndex)
	require.True(t, entry.ArmResults[2].PlanModes[0].Memoize)
	require.True(t, entry.ArmResults[2].PlanModes[0].HashJoin)
	require.Equal(t, "training", entry.QualificationSplit)

	artifactPath := filepath.Join(t.TempDir(), "expand-into.jsonl")
	outputPath := filepath.Join(t.TempDir(), "expand-into.json")
	require.NoError(t, writeJSONLFile(artifactPath, records))
	require.NoError(t, createExpandIntoStudyReport(artifactPath, outputPath, ExpandIntoStudyOptions{
		Seed: 1, Confidence: .975, BootstrapCount: 100, Protocol: referencePairProtocolDiscovery,
	}))
	content, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	var written ExpandIntoStudyReport
	require.NoError(t, json.Unmarshal(content, &written))
	require.True(t, written.Passed)
	require.True(t, validSHA256(written.ArtifactSHA256))
	require.Equal(t, referencePairProtocolDiscovery, written.Protocol)

	var confirmationRecords []CaseResult
	for round := 1; round <= 10; round++ {
		record := records[(round-1)%len(records)]
		record.Environment = &RunEnvironment{Round: round, WarmupIterations: 20}
		planModes := []string{"auto", "force_custom_plan", "force_generic_plan"}
		record.PostgresEnvironment = &PostgresEnvironment{PlanCacheMode: planModes[(round-1)%len(planModes)]}
		record.PostgresReferences = append([]PostgresReferenceResult(nil), record.PostgresReferences...)
		orderSpecs := make([]postgresReferenceSpec, len(expandIntoStudyArms))
		for idx, name := range expandIntoStudyArms {
			orderSpecs[idx].name = name
		}
		orders := map[string]int{}
		for idx, spec := range referenceSpecsForRound(orderSpecs, round) {
			orders[spec.name] = idx + 2
		}
		for idx := range record.PostgresReferences {
			reference := &record.PostgresReferences[idx]
			reference.MeasurementOrder = orders[reference.Name]
			reference.Stats.WarmupIterations = 20
			duration := reference.Stats.Samples[0].Duration
			reference.Stats.Samples = make([]LatencySample, 50)
			for sample := range reference.Stats.Samples {
				reference.Stats.Samples[sample] = LatencySample{Classification: "warm", Duration: duration + time.Duration(sample)}
			}
		}
		confirmationRecords = append(confirmationRecords, record)
		holdout := record
		holdout.Name = "pair-holdout"
		holdout.Shape.QualificationSplit = "holdout"
		confirmationRecords = append(confirmationRecords, holdout)
	}
	confirmation, err := buildExpandIntoStudyReport(confirmationRecords, ExpandIntoStudyOptions{
		Seed: 1, Confidence: .975, BootstrapCount: 100, Protocol: referencePairProtocolConfirmation,
	})
	require.NoError(t, err)
	require.True(t, confirmation.Passed)
	require.Equal(t, 1, confirmation.TrainingCases)
	require.Equal(t, 1, confirmation.HoldoutCases)
	require.True(t, confirmation.TrainingPassed)
	require.True(t, confirmation.HoldoutPassed)
	require.True(t, confirmation.QualificationPassed)
	require.Equal(t, referencePairProtocolConfirmation, confirmation.Protocol)
	var trainingOnly []CaseResult
	for _, record := range confirmationRecords {
		if record.Shape.QualificationSplit == "training" {
			trainingOnly = append(trainingOnly, record)
		}
	}
	trainingOnlyReport, err := buildExpandIntoStudyReport(trainingOnly, ExpandIntoStudyOptions{
		Seed: 1, Confidence: .975, BootstrapCount: 100, Protocol: referencePairProtocolConfirmation,
	})
	require.NoError(t, err)
	require.False(t, trainingOnlyReport.Passed)
	require.True(t, trainingOnlyReport.TrainingPassed)
	require.False(t, trainingOnlyReport.HoldoutPassed)
	require.False(t, trainingOnlyReport.QualificationPassed)

	for idx := range confirmationRecords {
		confirmationRecords[idx].PostgresEnvironment = &PostgresEnvironment{PlanCacheMode: "force_custom_plan"}
	}
	incompleteModes, err := buildExpandIntoStudyReport(confirmationRecords, ExpandIntoStudyOptions{
		Seed: 1, Confidence: .975, BootstrapCount: 100, Protocol: referencePairProtocolConfirmation,
	})
	require.NoError(t, err)
	require.False(t, incompleteModes.Passed)
	require.Contains(t, incompleteModes.Cases[0].Reasons, "confirmation requires plan_cache_mode=auto")
	require.Contains(t, incompleteModes.Cases[0].Reasons, "confirmation requires plan_cache_mode=force_generic_plan")
}

// TestBuildExpandIntoStudyReportFailsClosedOnObservationOrOrderMismatch verifies plan evidence cannot qualify without exact rows and declared carryover order.
func TestBuildExpandIntoStudyReportFailsClosedOnObservationOrOrderMismatch(t *testing.T) {
	record := CaseResult{
		Environment: &RunEnvironment{Round: 1, WarmupIterations: 5}, Dataset: "expand_into", Name: "pair",
		Category: "expand_into_one_hop", Shape: WorkloadShape{FixtureTier: "normal", QualificationSplit: "training"},
		ExecutionMode: ModePostgresSQL, Status: StatusOK, RowCount: 1, ObservedRows: []string{"public"},
	}
	for _, name := range expandIntoStudyArms {
		record.PostgresReferences = append(record.PostgresReferences, PostgresReferenceResult{
			Name: name, Architecture: name, ImplementationID: name, FullComparator: true,
			SemanticValidation: "exact_public_observation", RowCount: 1, ObservedRows: []string{"different"},
			Stats: DurationStats{WarmupIterations: 5}, MeasurementOrder: 2,
		})
	}
	_, err := buildExpandIntoStudyReport([]CaseResult{record}, ExpandIntoStudyOptions{Confidence: .975, Protocol: referencePairProtocolDiscovery})
	require.ErrorContains(t, err, "not an exact public comparator")
}

// TestCreateExpandIntoStudyReportPersistsAndRejectsIncompleteEvidence verifies
// a durable diagnostic report cannot be mistaken for a successful gate.
func TestCreateExpandIntoStudyReportPersistsAndRejectsIncompleteEvidence(t *testing.T) {
	record := CaseResult{
		Environment: &RunEnvironment{Round: 1, WarmupIterations: 5}, Dataset: "expand_into", Name: "pair",
		Category: "expand_into_one_hop", Shape: WorkloadShape{FixtureTier: "normal", QualificationSplit: "training"}, ExecutionMode: ModePostgresSQL,
		Status: StatusOK, RowCount: 1, ObservedRows: []string{`["edge"]`},
	}
	orderSpecs := make([]postgresReferenceSpec, len(expandIntoStudyArms))
	for idx, name := range expandIntoStudyArms {
		orderSpecs[idx].name = name
	}
	orders := map[string]int{}
	for idx, spec := range referenceSpecsForRound(orderSpecs, 1) {
		orders[spec.name] = idx + 2
	}
	for _, name := range expandIntoStudyArms {
		record.PostgresReferences = append(record.PostgresReferences, PostgresReferenceResult{
			Name: name, Architecture: name, ImplementationID: name + "-v1", StateShape: "state",
			ObservationShape: "relationships", Boundary: "relationships", TimingBoundary: "raw_pgx",
			FullComparator: true, SemanticValidation: "exact_public_observation", RowCount: 1,
			ObservedRows: []string{`["edge"]`}, SQL: "select '" + name + "'", MeasurementOrder: orders[name],
			Stats: DurationStats{WarmupIterations: 5, Samples: []LatencySample{{Classification: "warm", Duration: time.Millisecond}}},
		})
	}
	artifactPath := filepath.Join(t.TempDir(), "incomplete.jsonl")
	outputPath := filepath.Join(t.TempDir(), "report.json")
	require.NoError(t, writeJSONLFile(artifactPath, []CaseResult{record}))
	require.ErrorContains(t, createExpandIntoStudyReport(artifactPath, outputPath, ExpandIntoStudyOptions{
		Seed: 1, Confidence: .975, BootstrapCount: 100, Protocol: referencePairProtocolDiscovery,
	}), "did not pass")

	content, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	var report ExpandIntoStudyReport
	require.NoError(t, json.Unmarshal(content, &report))
	require.False(t, report.Passed)
}
