// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
)

type spI2V2DevelopmentStudy string

const (
	spI2V2StudyReadiness  spI2V2DevelopmentStudy = "readiness"
	spI2V2StudyTournament spI2V2DevelopmentStudy = "tournament"
)

var spI2V2DevelopmentArms = []optimize.ShortestPathExecutor{
	optimize.ShortestPathExecutorI2GuardedDistanceV2E0,
	optimize.ShortestPathExecutorI2GuardedDistanceV2E1,
	optimize.ShortestPathExecutorI2GuardedDistanceV2E1D,
	optimize.ShortestPathExecutorI2GuardedDistanceV2E1P,
	optimize.ShortestPathExecutorI2GuardedDistanceV2E1DP,
}

var spI2V2ReadinessArms = []optimize.ShortestPathExecutor{
	optimize.ShortestPathExecutorS4CanonicalDistance,
	optimize.ShortestPathExecutorI2GuardedDistanceV2E0,
}

// spI2V2DevelopmentOrder returns the preregistered doubled five-arm Williams
// row. Across ten rounds every arm occupies every physical position twice.
func spI2V2DevelopmentOrder(round int) ([]optimize.ShortestPathExecutor, error) {
	if round < 1 || round > 10 {
		return nil, fmt.Errorf("SP-I2 V2 development round must be in 1..10")
	}
	schedule := [10][5]int{
		{0, 1, 4, 2, 3}, {1, 2, 0, 3, 4}, {2, 3, 1, 4, 0}, {3, 4, 2, 0, 1}, {4, 0, 3, 1, 2},
		{3, 2, 4, 1, 0}, {4, 3, 0, 2, 1}, {0, 4, 1, 3, 2}, {1, 0, 2, 4, 3}, {2, 1, 3, 0, 4},
	}
	row := schedule[round-1]
	ordered := make([]optimize.ShortestPathExecutor, len(row))
	for position, arm := range row {
		ordered[position] = spI2V2DevelopmentArms[arm]
	}
	return ordered, nil
}

// spI2V2ReadinessOrder alternates the supplemental control pair so each arm
// occupies each physical position five times across the fixed ten rounds.
func spI2V2ReadinessOrder(round int) ([]optimize.ShortestPathExecutor, error) {
	if round < 1 || round > 10 {
		return nil, fmt.Errorf("SP-I2 V2 readiness round must be in 1..10")
	}
	if round%2 == 1 {
		return slices.Clone(spI2V2ReadinessArms), nil
	}
	return []optimize.ShortestPathExecutor{spI2V2ReadinessArms[1], spI2V2ReadinessArms[0]}, nil
}

// validateSPI2V2DevelopmentCaptureConfig freezes one invocation's position in
// the open-corpus component tournament before any database setup occurs.
func validateSPI2V2DevelopmentCaptureConfig(cfg config) error {
	if cfg.SPI2V2ReadinessComparison {
		return fmt.Errorf("SP-I2 V2 development tournament and readiness comparison are mutually exclusive")
	}
	if cfg.SPI2Generation != spI2GenerationV2 {
		return fmt.Errorf("SP-I2 V2 development tournament requires generation %q", spI2GenerationV2)
	}
	if len(cfg.Modes) != 1 || cfg.Modes[0] != ModePostgresSQL || cfg.ExistingGraph || cfg.Discovery {
		return fmt.Errorf("SP-I2 V2 development tournament requires one managed PostgreSQL mode")
	}
	if cfg.Iterations != 100 || cfg.WarmupIterations != 25 || cfg.PoolSize != 1 || len(cfg.Concurrency) != 0 {
		return fmt.Errorf("SP-I2 V2 development tournament requires exactly 100 samples, 25 warmups, pool size 1, and no concurrency block")
	}
	if cfg.Block != cfg.Round || cfg.ArmOrder < 1 || cfg.ArmOrder > 5 || strings.TrimSpace(cfg.RunUUID) == "" {
		return fmt.Errorf("SP-I2 V2 development tournament requires block equal to round, a five-arm order, and an explicit shared run UUID")
	}
	if len(cfg.Tags) != 1 || cfg.Tags[0] != spI2TrainingTag || len(cfg.Cases) != 0 || len(cfg.Datasets) != 0 || len(cfg.Categories) != 0 {
		return fmt.Errorf("SP-I2 V2 development tournament is restricted to the six open V1 training cases")
	}
	executor := optimize.ShortestPathExecutor(cfg.PostgresForceShortest)
	if !slices.Contains(spI2V2DevelopmentArms, executor) {
		return fmt.Errorf("SP-I2 V2 development tournament must force a declared E0/E1 component arm")
	}
	order, err := spI2V2DevelopmentOrder(cfg.Round)
	if err != nil {
		return err
	}
	expectedOrder := slices.Index(order, executor) + 1
	if cfg.Arm != string(executor) || cfg.ArmOrder != expectedOrder {
		return fmt.Errorf("SP-I2 V2 development round %d requires arm %q at order %d", cfg.Round, executor, expectedOrder)
	}
	if !cfg.PostgresRepeatableRead || cfg.PostgresTraversalTelemetry != postgresTraversalTelemetryDiagnostic ||
		cfg.PostgresProductionManifest != "" || cfg.PostgresForceExpansion != "" ||
		cfg.PostgresExpansionOrientationShadow || cfg.PostgresExpansionOrientationTournament ||
		cfg.PostgresReferences || len(cfg.PostgresReferenceArms) != 0 || cfg.Baseline != "" ||
		cfg.BundleDir != "" || len(cfg.BundleEvidence) != 0 || cfg.SPI2Freeze != "" || cfg.SPI2DiscoveryReport != "" {
		return fmt.Errorf("SP-I2 V2 development tournament requires forced Repeatable Read with diagnostic telemetry and no supplemental or protected-evidence arms")
	}
	if cfg.OutputJSONL == "" || cfg.Round > 1 && !cfg.AppendJSONL {
		return fmt.Errorf("SP-I2 V2 development tournament requires a JSONL output and append mode after round 1")
	}
	return nil
}

// validateSPI2V2ReadinessCaptureConfig freezes one invocation's position in
// the supplemental open-corpus E0/S4 comparison before database setup.
func validateSPI2V2ReadinessCaptureConfig(cfg config) error {
	if cfg.SPI2V2DevelopmentTournament {
		return fmt.Errorf("SP-I2 V2 readiness comparison and development tournament are mutually exclusive")
	}
	if cfg.SPI2Generation != spI2GenerationV2 {
		return fmt.Errorf("SP-I2 V2 readiness comparison requires generation %q", spI2GenerationV2)
	}
	if len(cfg.Modes) != 1 || cfg.Modes[0] != ModePostgresSQL || cfg.ExistingGraph || cfg.Discovery {
		return fmt.Errorf("SP-I2 V2 readiness comparison requires one managed PostgreSQL mode")
	}
	if cfg.Iterations != 100 || cfg.WarmupIterations != 25 || cfg.PoolSize != 1 || len(cfg.Concurrency) != 0 {
		return fmt.Errorf("SP-I2 V2 readiness comparison requires exactly 100 samples, 25 warmups, pool size 1, and no concurrency block")
	}
	if cfg.Block != cfg.Round || cfg.ArmOrder < 1 || cfg.ArmOrder > 2 || strings.TrimSpace(cfg.RunUUID) == "" {
		return fmt.Errorf("SP-I2 V2 readiness comparison requires block equal to round, a two-arm order, and an explicit shared run UUID")
	}
	if len(cfg.Tags) != 1 || cfg.Tags[0] != spI2TrainingTag || len(cfg.Cases) != 0 || len(cfg.Datasets) != 0 || len(cfg.Categories) != 0 {
		return fmt.Errorf("SP-I2 V2 readiness comparison is restricted to the six open V1 training cases")
	}
	executor := optimize.ShortestPathExecutor(cfg.PostgresForceShortest)
	if !slices.Contains(spI2V2ReadinessArms, executor) {
		return fmt.Errorf("SP-I2 V2 readiness comparison must force exact S4 distance or E0")
	}
	order, err := spI2V2ReadinessOrder(cfg.Round)
	if err != nil {
		return err
	}
	expectedOrder := slices.Index(order, executor) + 1
	if cfg.Arm != string(executor) || cfg.ArmOrder != expectedOrder {
		return fmt.Errorf("SP-I2 V2 readiness round %d requires arm %q at order %d", cfg.Round, executor, expectedOrder)
	}
	if !cfg.PostgresRepeatableRead || cfg.PostgresTraversalTelemetry != postgresTraversalTelemetryDiagnostic ||
		cfg.PostgresProductionManifest != "" || cfg.PostgresForceExpansion != "" ||
		cfg.PostgresExpansionOrientationShadow || cfg.PostgresExpansionOrientationTournament ||
		cfg.PostgresReferences || len(cfg.PostgresReferenceArms) != 0 || cfg.Baseline != "" ||
		cfg.BundleDir != "" || len(cfg.BundleEvidence) != 0 || cfg.SPI2Freeze != "" || cfg.SPI2DiscoveryReport != "" {
		return fmt.Errorf("SP-I2 V2 readiness comparison requires forced Repeatable Read with diagnostic telemetry and no supplemental or protected-evidence arms")
	}
	if cfg.OutputJSONL == "" || cfg.Round > 1 && !cfg.AppendJSONL {
		return fmt.Errorf("SP-I2 V2 readiness comparison requires a JSONL output and append mode after round 1")
	}
	return nil
}

type spI2V2DevelopmentRecordKey struct {
	caseKey performanceKey
	round   int
	arm     optimize.ShortestPathExecutor
}

type spI2V2DevelopmentInvocation struct {
	order     int
	startedAt time.Time
	endedAt   time.Time
}

// validateSPI2V2DevelopmentEvidence rejects partial, relabeled, replayed, or
// out-of-order raw development artifacts before statistical interpretation.
func validateSPI2V2DevelopmentEvidence(records []CaseResult, study spI2V2DevelopmentStudy) error {
	cohort, err := canonicalSPI2Cohort()
	if err != nil {
		return err
	}
	var arms []optimize.ShortestPathExecutor
	switch study {
	case spI2V2StudyReadiness:
		arms = spI2V2ReadinessArms
	case spI2V2StudyTournament:
		arms = spI2V2DevelopmentArms
	default:
		return fmt.Errorf("unknown SP-I2 V2 development study %q", study)
	}
	expectedRecords := len(cohort.trainingKeys) * 10 * len(arms)
	if len(records) != expectedRecords {
		return fmt.Errorf("SP-I2 V2 %s artifact contains %d records, expected exactly %d", study, len(records), expectedRecords)
	}
	seenRecords := make(map[spI2V2DevelopmentRecordKey]struct{}, expectedRecords)
	seenInvocations := make(map[string]struct{}, expectedRecords*100)
	invocations := make(map[int]map[optimize.ShortestPathExecutor]spI2V2DevelopmentInvocation, 10)
	runUUID := ""
	for _, record := range records {
		key := performanceKey{dataset: record.Dataset, name: record.Name, backend: record.ExecutionMode}
		if _, expected := cohort.trainingKeys[key]; !expected {
			return fmt.Errorf("SP-I2 V2 %s artifact contains unexpected case %s/%s", study, record.Dataset, record.Name)
		}
		if record.ExecutionMode != ModePostgresSQL || record.Status != StatusOK || record.Environment == nil ||
			record.Environment.ArtifactSchemaVersion != 2 || record.Environment.PoolSize != 1 ||
			len(record.Environment.Concurrency) != 0 || record.Environment.ExistingGraph ||
			record.Environment.WarmupIterations != 25 || record.Stats.WarmupIterations != 25 ||
			record.Stats.Iterations != 100 || len(record.Stats.Samples) != 100 ||
			len(record.Concurrency) != 0 || len(record.PostgresReferences) != 0 || record.ClientWaterfall != nil ||
			record.RawPGXWaterfall != nil || record.RawPGXRoundTrip != nil || record.Baseline != nil {
			return fmt.Errorf("%s/%s lacks the exact single-session SP-I2 V2 %s measurement contract", record.Dataset, record.Name, study)
		}
		environment := record.Environment
		arm := optimize.ShortestPathExecutor(environment.Arm)
		if !slices.Contains(arms, arm) || environment.Block != environment.Round || environment.Round < 1 || environment.Round > 10 ||
			strings.TrimSpace(environment.RunUUID) == "" || environment.StartedAt.IsZero() || environment.EndedAt.Before(environment.StartedAt) {
			return fmt.Errorf("%s/%s has malformed SP-I2 V2 %s invocation metadata", record.Dataset, record.Name, study)
		}
		order, err := spI2V2StudyOrder(study, environment.Round)
		if err != nil {
			return err
		}
		expectedOrder := slices.Index(order, arm) + 1
		if environment.ArmOrder != expectedOrder {
			return fmt.Errorf("SP-I2 V2 %s round %d requires arm %q at order %d", study, environment.Round, arm, expectedOrder)
		}
		if runUUID == "" {
			runUUID = environment.RunUUID
		} else if runUUID != environment.RunUUID {
			return fmt.Errorf("SP-I2 V2 %s artifact mixes run UUIDs", study)
		}
		recordKey := spI2V2DevelopmentRecordKey{caseKey: key, round: environment.Round, arm: arm}
		if _, duplicate := seenRecords[recordKey]; duplicate {
			return fmt.Errorf("SP-I2 V2 %s artifact duplicates %s/%s round %d arm %q", study, record.Dataset, record.Name, environment.Round, arm)
		}
		seenRecords[recordKey] = struct{}{}
		if invocations[environment.Round] == nil {
			invocations[environment.Round] = map[optimize.ShortestPathExecutor]spI2V2DevelopmentInvocation{}
		}
		invocation := spI2V2DevelopmentInvocation{order: environment.ArmOrder, startedAt: environment.StartedAt, endedAt: environment.EndedAt}
		if prior, found := invocations[environment.Round][arm]; found && prior != invocation {
			return fmt.Errorf("SP-I2 V2 %s round %d arm %q mixes invocation chronology", study, environment.Round, arm)
		}
		invocations[environment.Round][arm] = invocation
		if err := validateSPI2V2DevelopmentSamples(record, arm, seenInvocations); err != nil {
			return err
		}
	}
	var priorRoundEnded time.Time
	for round := 1; round <= 10; round++ {
		order, err := spI2V2StudyOrder(study, round)
		if err != nil {
			return err
		}
		var priorEnded time.Time
		for position, arm := range order {
			invocation, found := invocations[round][arm]
			if !found || invocation.order != position+1 {
				return fmt.Errorf("SP-I2 V2 %s round %d omits scheduled arm %q", study, round, arm)
			}
			if !priorEnded.IsZero() && priorEnded.After(invocation.startedAt) {
				return fmt.Errorf("SP-I2 V2 %s round %d arm chronology contradicts the fixed order", study, round)
			}
			if position == 0 && !priorRoundEnded.IsZero() && priorRoundEnded.After(invocation.startedAt) {
				return fmt.Errorf("SP-I2 V2 %s round %d overlaps or predates the prior round", study, round)
			}
			priorEnded = invocation.endedAt
		}
		priorRoundEnded = priorEnded
	}
	return nil
}

func validateSPI2V2DevelopmentArtifact(path string, study spI2V2DevelopmentStudy) error {
	records, err := readJSONLFile(path)
	if err != nil {
		return fmt.Errorf("read artifact: %w", err)
	}
	return validateSPI2V2DevelopmentEvidence(records, study)
}

func spI2V2StudyOrder(study spI2V2DevelopmentStudy, round int) ([]optimize.ShortestPathExecutor, error) {
	if study == spI2V2StudyReadiness {
		return spI2V2ReadinessOrder(round)
	}
	if study == spI2V2StudyTournament {
		return spI2V2DevelopmentOrder(round)
	}
	return nil, fmt.Errorf("unknown SP-I2 V2 development study %q", study)
}

func validateSPI2V2DevelopmentSamples(record CaseResult, arm optimize.ShortestPathExecutor, seenInvocations map[string]struct{}) error {
	environment := record.Environment
	receipt := record.Stats.ReceiptStabilization
	if receipt == nil || strings.TrimSpace(receipt.InvocationID) == "" || receipt.RequestedIdentity != string(arm) ||
		receipt.RuntimeIdentity != string(arm) || strings.TrimSpace(receipt.RuntimeBranch) == "" ||
		receipt.FallbackExecuted == nil || *receipt.FallbackExecuted || len(receipt.Events) == 0 {
		return fmt.Errorf("%s/%s arm %q lacks one exact excluded stabilization receipt", record.Dataset, record.Name, arm)
	}
	if _, duplicate := seenInvocations[receipt.InvocationID]; duplicate {
		return fmt.Errorf("SP-I2 V2 development evidence reuses invocation identity %q", receipt.InvocationID)
	}
	seenInvocations[receipt.InvocationID] = struct{}{}
	if err := validateSPI2V2ReceiptEvents(receipt.InvocationID, receipt.RuntimeIdentity, receipt.RuntimeBranch, receipt.Events); err != nil {
		return fmt.Errorf("%s/%s stabilization receipt: %w", record.Dataset, record.Name, err)
	}
	iterations := make(map[int]struct{}, 100)
	for _, sample := range record.Stats.Samples {
		if sample.Classification != "warm" || sample.Duration <= 0 || sample.Iteration < 1 || sample.Iteration > 100 ||
			sample.Dataset != record.Dataset || sample.Case != record.Name || sample.Backend != ModePostgresSQL ||
			sample.Round != environment.Round || sample.Block != environment.Block || sample.Arm != environment.Arm ||
			sample.ArmOrder != environment.ArmOrder || sample.RunUUID != environment.RunUUID || strings.TrimSpace(sample.ConnectionID) == "" ||
			sample.RequestedIdentity != string(arm) || sample.RuntimeIdentity != string(arm) || strings.TrimSpace(sample.RuntimeBranch) == "" ||
			sample.FallbackExecuted == nil || *sample.FallbackExecuted || strings.TrimSpace(sample.RuntimeAttestation) == "" ||
			strings.TrimSpace(sample.RuntimeInvocationID) == "" || len(sample.RuntimeReceiptEvents) == 0 {
			return fmt.Errorf("%s/%s arm %q contains a sample outside its exact invocation identity", record.Dataset, record.Name, arm)
		}
		if _, duplicate := iterations[sample.Iteration]; duplicate {
			return fmt.Errorf("%s/%s arm %q duplicates timed iteration %d", record.Dataset, record.Name, arm, sample.Iteration)
		}
		iterations[sample.Iteration] = struct{}{}
		if _, duplicate := seenInvocations[sample.RuntimeInvocationID]; duplicate {
			return fmt.Errorf("SP-I2 V2 development evidence reuses invocation identity %q", sample.RuntimeInvocationID)
		}
		seenInvocations[sample.RuntimeInvocationID] = struct{}{}
		if err := validateSPI2V2ReceiptEvents(sample.RuntimeInvocationID, sample.RuntimeIdentity, sample.RuntimeBranch, sample.RuntimeReceiptEvents); err != nil {
			return fmt.Errorf("%s/%s timed iteration %d: %w", record.Dataset, record.Name, sample.Iteration, err)
		}
	}
	return nil
}

func validateSPI2V2ReceiptEvents(invocationID, runtimeIdentity, runtimeBranch string, events []RuntimeReceiptEvent) error {
	for index, event := range events {
		if event.InvocationID != invocationID || event.Ordinal != index+1 || strings.TrimSpace(event.RuntimeIdentity) == "" || strings.TrimSpace(event.RuntimeBranch) == "" {
			return fmt.Errorf("runtime receipt event chain is malformed")
		}
	}
	last := events[len(events)-1]
	if last.RuntimeIdentity != runtimeIdentity || last.RuntimeBranch != runtimeBranch || last.FallbackExecuted {
		return fmt.Errorf("runtime receipt terminal event contradicts the invocation outcome")
	}
	return nil
}
