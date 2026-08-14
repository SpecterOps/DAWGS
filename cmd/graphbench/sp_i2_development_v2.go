// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"slices"
	"strings"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
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
