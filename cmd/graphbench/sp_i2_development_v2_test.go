// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/stretchr/testify/require"
)

func TestSPI2V2DevelopmentOrderBalancesEveryArmAndPosition(t *testing.T) {
	positionCounts := make(map[optimize.ShortestPathExecutor]map[int]int, len(spI2V2DevelopmentArms))
	for round := 1; round <= 10; round++ {
		order, err := spI2V2DevelopmentOrder(round)
		require.NoError(t, err)
		require.Len(t, order, 5)
		require.ElementsMatch(t, spI2V2DevelopmentArms, order)
		for position, arm := range order {
			if positionCounts[arm] == nil {
				positionCounts[arm] = map[int]int{}
			}
			positionCounts[arm][position+1]++
		}
	}
	for _, arm := range spI2V2DevelopmentArms {
		for position := 1; position <= 5; position++ {
			require.Equal(t, 2, positionCounts[arm][position], "%s position %d", arm, position)
		}
	}
	_, err := spI2V2DevelopmentOrder(0)
	require.Error(t, err)
	_, err = spI2V2DevelopmentOrder(11)
	require.Error(t, err)
}

func TestSPI2V2ReadinessOrderBalancesEveryArmAndPosition(t *testing.T) {
	positionCounts := make(map[optimize.ShortestPathExecutor]map[int]int, len(spI2V2ReadinessArms))
	for round := 1; round <= 10; round++ {
		order, err := spI2V2ReadinessOrder(round)
		require.NoError(t, err)
		require.Len(t, order, 2)
		require.ElementsMatch(t, spI2V2ReadinessArms, order)
		for position, arm := range order {
			if positionCounts[arm] == nil {
				positionCounts[arm] = map[int]int{}
			}
			positionCounts[arm][position+1]++
		}
	}
	for _, arm := range spI2V2ReadinessArms {
		require.Equal(t, 5, positionCounts[arm][1], arm)
		require.Equal(t, 5, positionCounts[arm][2], arm)
	}
	_, err := spI2V2ReadinessOrder(0)
	require.Error(t, err)
	_, err = spI2V2ReadinessOrder(11)
	require.Error(t, err)
}

func TestValidateSPI2V2DevelopmentCaptureConfig(t *testing.T) {
	order, err := spI2V2DevelopmentOrder(4)
	require.NoError(t, err)
	executor := optimize.ShortestPathExecutorI2GuardedDistanceV2E1D
	expectedOrder := 0
	for position, arm := range order {
		if arm == executor {
			expectedOrder = position + 1
		}
	}
	cfg := config{
		SPI2Generation:              spI2GenerationV2,
		SPI2V2DevelopmentTournament: true,
		Modes:                       []ExecutionMode{ModePostgresSQL},
		Iterations:                  100,
		WarmupIterations:            25,
		PoolSize:                    1,
		Round:                       4,
		Block:                       4,
		Arm:                         string(executor),
		ArmOrder:                    expectedOrder,
		RunUUID:                     "development-series",
		Tags:                        []string{spI2TrainingTag},
		OutputJSONL:                 "development.jsonl",
		AppendJSONL:                 true,
		PostgresForceShortest:       string(executor),
		PostgresRepeatableRead:      true,
		PostgresTraversalTelemetry:  postgresTraversalTelemetryDiagnostic,
	}
	require.NoError(t, validateSPI2V2DevelopmentCaptureConfig(cfg))

	mutations := []func(*config){
		func(cfg *config) { cfg.SPI2Generation = spI2GenerationV1 },
		func(cfg *config) { cfg.Iterations = 99 },
		func(cfg *config) { cfg.WarmupIterations = 24 },
		func(cfg *config) { cfg.ArmOrder = expectedOrder%5 + 1 },
		func(cfg *config) { cfg.Arm = "alias" },
		func(cfg *config) { cfg.Tags = []string{spI2HoldoutTag} },
		func(cfg *config) {
			cfg.PostgresForceShortest = string(optimize.ShortestPathExecutorI2GuardedDistanceV2)
		},
		func(cfg *config) { cfg.PostgresRepeatableRead = false },
		func(cfg *config) { cfg.PostgresReferences = true },
		func(cfg *config) { cfg.SPI2Freeze = "freeze.json" },
	}
	for _, mutate := range mutations {
		copy := cfg
		mutate(&copy)
		require.Error(t, validateSPI2V2DevelopmentCaptureConfig(copy))
	}
}

func TestValidateSPI2V2ReadinessCaptureConfig(t *testing.T) {
	executor := optimize.ShortestPathExecutorS4CanonicalDistance
	cfg := config{
		SPI2Generation:             spI2GenerationV2,
		SPI2V2ReadinessComparison:  true,
		Modes:                      []ExecutionMode{ModePostgresSQL},
		Iterations:                 100,
		WarmupIterations:           25,
		PoolSize:                   1,
		Round:                      3,
		Block:                      3,
		Arm:                        string(executor),
		ArmOrder:                   1,
		RunUUID:                    "readiness-series",
		Tags:                       []string{spI2TrainingTag},
		OutputJSONL:                "readiness.jsonl",
		AppendJSONL:                true,
		PostgresForceShortest:      string(executor),
		PostgresRepeatableRead:     true,
		PostgresTraversalTelemetry: postgresTraversalTelemetryDiagnostic,
	}
	require.NoError(t, validateSPI2V2ReadinessCaptureConfig(cfg))

	mutations := []func(*config){
		func(cfg *config) { cfg.SPI2Generation = spI2GenerationV1 },
		func(cfg *config) { cfg.Iterations = 101 },
		func(cfg *config) { cfg.Round = 11; cfg.Block = 11 },
		func(cfg *config) { cfg.ArmOrder = 2 },
		func(cfg *config) { cfg.Arm = "alias" },
		func(cfg *config) { cfg.Tags = []string{spI2HoldoutTag} },
		func(cfg *config) {
			cfg.PostgresForceShortest = string(optimize.ShortestPathExecutorI2GuardedDistanceV2E1)
		},
		func(cfg *config) { cfg.PostgresRepeatableRead = false },
		func(cfg *config) { cfg.PostgresReferences = true },
		func(cfg *config) { cfg.SPI2V2DevelopmentTournament = true },
	}
	for _, mutate := range mutations {
		copy := cfg
		mutate(&copy)
		require.Error(t, validateSPI2V2ReadinessCaptureConfig(copy))
	}
}
