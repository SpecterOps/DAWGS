// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

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

func TestValidateSPI2V2ComponentCheckCaptureConfig(t *testing.T) {
	executor := optimize.ShortestPathExecutorI2GuardedDistanceV2E1D
	cfg := config{
		SPI2Generation:             spI2GenerationV2,
		SPI2V2ComponentCheck:       true,
		Modes:                      []ExecutionMode{ModePostgresSQL},
		Iterations:                 1,
		WarmupIterations:           1,
		PoolSize:                   1,
		Round:                      1,
		Block:                      1,
		Arm:                        string(executor),
		ArmOrder:                   1,
		RunUUID:                    "component-check",
		Tags:                       []string{spI2TrainingTag},
		OutputJSONL:                "component.jsonl",
		PostgresForceShortest:      string(executor),
		PostgresRepeatableRead:     true,
		PostgresTraversalTelemetry: postgresTraversalTelemetryDiagnostic,
	}
	require.NoError(t, validateSPI2V2ComponentCheckCaptureConfig(cfg))

	for _, mutate := range []func(*config){
		func(cfg *config) { cfg.Iterations = 2 },
		func(cfg *config) { cfg.WarmupIterations = 0 },
		func(cfg *config) { cfg.Tags = []string{spI2HoldoutTag} },
		func(cfg *config) {
			cfg.PostgresForceShortest = string(optimize.ShortestPathExecutorI2GuardedDistanceV2E1)
		},
		func(cfg *config) { cfg.Arm = "alias" },
		func(cfg *config) { cfg.PostgresRepeatableRead = false },
		func(cfg *config) { cfg.SPI2V2DevelopmentTournament = true },
		func(cfg *config) { cfg.SPI2V2ComponentAuthorization = "authorization.json" },
	} {
		copy := cfg
		mutate(&copy)
		require.Error(t, validateSPI2V2ComponentCheckCaptureConfig(copy))
	}
}

func TestValidateSPI2V2DevelopmentCaptureRequiresAuthorizationForCombinedArm(t *testing.T) {
	order, err := spI2V2DevelopmentOrder(1)
	require.NoError(t, err)
	executor := optimize.ShortestPathExecutorI2GuardedDistanceV2E1DP
	cfg := config{
		SPI2Generation:              spI2GenerationV2,
		SPI2V2DevelopmentTournament: true,
		Modes:                       []ExecutionMode{ModePostgresSQL},
		Iterations:                  100,
		WarmupIterations:            25,
		PoolSize:                    1,
		Round:                       1,
		Block:                       1,
		Arm:                         string(executor),
		ArmOrder:                    slices.Index(order, executor) + 1,
		RunUUID:                     "development-series",
		Tags:                        []string{spI2TrainingTag},
		OutputJSONL:                 "development.jsonl",
		PostgresForceShortest:       string(executor),
		PostgresRepeatableRead:      true,
		PostgresTraversalTelemetry:  postgresTraversalTelemetryDiagnostic,
	}
	require.ErrorContains(t, validateSPI2V2DevelopmentCaptureConfig(cfg), "requires an exact E1D/E1P component authorization")

	_, protocolSHA256, err := loadSPI2ProtocolV2("../../benchmark/testdata/scale/protocols/sp_i2_distance_v2.json")
	require.NoError(t, err)
	authorization := validSPI2V2ComponentAuthorization(protocolSHA256)
	authorization.SourceCommit = commandOutput("git", "rev-parse", "HEAD")
	authorization.DirtyDiffSHA256 = workingTreeSHA256()
	authorization.BinarySHA256 = executableSHA256()
	authorizationPath := filepath.Join(t.TempDir(), "authorization.json")
	require.NoError(t, writeIndentedJSON(authorizationPath, authorization))
	cfg.CorpusRoot = "../../benchmark/testdata/scale"
	cfg.SPI2V2ComponentAuthorization = authorizationPath
	require.NoError(t, validateSPI2V2DevelopmentCaptureConfig(cfg))

	authorization.BinarySHA256 = strings.Repeat("f", 64)
	require.NoError(t, writeIndentedJSON(authorizationPath, authorization))
	require.ErrorContains(t, validateSPI2V2DevelopmentCaptureConfig(cfg), "does not bind the current source tree and executable")
}

func TestValidateSPI2V2DevelopmentEvidenceAcceptsCompleteStudies(t *testing.T) {
	require.NoError(t, validateSPI2V2DevelopmentEvidence(spI2V2DevelopmentTestRecords(t, spI2V2StudyReadiness), spI2V2StudyReadiness))
	require.NoError(t, validateSPI2V2DevelopmentEvidence(spI2V2DevelopmentTestRecords(t, spI2V2StudyTournament), spI2V2StudyTournament))
}

func TestValidateSPI2V2DevelopmentArtifact(t *testing.T) {
	artifact := filepath.Join(t.TempDir(), "readiness.jsonl")
	require.NoError(t, writeJSONLFile(artifact, spI2V2DevelopmentTestRecords(t, spI2V2StudyReadiness)))
	require.NoError(t, validateSPI2V2DevelopmentArtifact(artifact, spI2V2StudyReadiness))
}

func TestValidateSPI2V2DevelopmentEvidenceRejectsTampering(t *testing.T) {
	tests := map[string]func([]CaseResult) []CaseResult{
		"missing record": func(records []CaseResult) []CaseResult {
			return records[:len(records)-1]
		},
		"wrong sample count": func(records []CaseResult) []CaseResult {
			records[0].Stats.Samples = records[0].Stats.Samples[:99]
			return records
		},
		"wrong arm order": func(records []CaseResult) []CaseResult {
			records[0].Environment.ArmOrder = 2
			return records
		},
		"mixed run UUID": func(records []CaseResult) []CaseResult {
			records[0].Environment.RunUUID = "other"
			return records
		},
		"relabeled requested identity": func(records []CaseResult) []CaseResult {
			records[0].Stats.Samples[0].RequestedIdentity = "other"
			return records
		},
		"missing stabilization": func(records []CaseResult) []CaseResult {
			records[0].Stats.ReceiptStabilization = nil
			return records
		},
		"replayed timed invocation": func(records []CaseResult) []CaseResult {
			first := records[0].Stats.Samples[0].RuntimeInvocationID
			records[0].Stats.Samples[1].RuntimeInvocationID = first
			records[0].Stats.Samples[1].RuntimeReceiptEvents[0].InvocationID = first
			return records
		},
		"unexpected case": func(records []CaseResult) []CaseResult {
			records[0].Name = "holdout"
			return records
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			records := mutate(spI2V2DevelopmentTestRecords(t, spI2V2StudyReadiness))
			require.Error(t, validateSPI2V2DevelopmentEvidence(records, spI2V2StudyReadiness))
		})
	}
}

func spI2V2DevelopmentTestRecords(t *testing.T, study spI2V2DevelopmentStudy) []CaseResult {
	t.Helper()
	cohort, err := canonicalSPI2Cohort()
	require.NoError(t, err)
	arms := spI2V2DevelopmentArms
	if study == spI2V2StudyReadiness {
		arms = spI2V2ReadinessArms
	}
	records := make([]CaseResult, 0, len(cohort.trainingKeys)*10*len(arms))
	base := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	fallback := false
	for round := 1; round <= 10; round++ {
		order, err := spI2V2StudyOrder(study, round)
		require.NoError(t, err)
		for position, arm := range order {
			startedAt := base.Add(time.Duration(round)*time.Hour + time.Duration(position)*2*time.Minute)
			endedAt := startedAt.Add(time.Minute)
			for key := range cohort.trainingKeys {
				prefix := fmt.Sprintf("%s-%d-%s-%s", study, round, arm, key.name)
				receiptID := prefix + "-stabilization"
				record := CaseResult{
					Dataset:       key.dataset,
					Name:          key.name,
					ExecutionMode: ModePostgresSQL,
					Status:        StatusOK,
					Environment: &RunEnvironment{
						ArtifactSchemaVersion: 2,
						RunUUID:               "development-series",
						Arm:                   string(arm),
						ArmOrder:              position + 1,
						Block:                 round,
						Round:                 round,
						StartedAt:             startedAt,
						EndedAt:               endedAt,
						WarmupIterations:      25,
						PoolSize:              1,
					},
					Stats: DurationStats{
						Iterations:       100,
						WarmupIterations: 25,
						ReceiptStabilization: &RuntimeStabilizationReceipt{
							InvocationID:      receiptID,
							RequestedIdentity: string(arm),
							RuntimeIdentity:   string(arm),
							RuntimeBranch:     "selected",
							FallbackExecuted:  &fallback,
							Events: []RuntimeReceiptEvent{{
								InvocationID:     receiptID,
								Ordinal:          1,
								RuntimeIdentity:  string(arm),
								RuntimeBranch:    "selected",
								FallbackExecuted: false,
							}},
						},
					},
				}
				for iteration := 1; iteration <= 100; iteration++ {
					invocationID := fmt.Sprintf("%s-%d", prefix, iteration)
					record.Stats.Samples = append(record.Stats.Samples, LatencySample{
						Round:               round,
						Block:               round,
						Arm:                 string(arm),
						ArmOrder:            position + 1,
						RunUUID:             "development-series",
						Iteration:           iteration,
						Case:                key.name,
						Dataset:             key.dataset,
						Backend:             ModePostgresSQL,
						ConnectionID:        "connection-1",
						Classification:      "warm",
						Duration:            time.Millisecond,
						RequestedIdentity:   string(arm),
						RuntimeIdentity:     string(arm),
						RuntimeBranch:       "selected",
						FallbackExecuted:    &fallback,
						RuntimeAttestation:  "receipt",
						RuntimeInvocationID: invocationID,
						RuntimeReceiptEvents: []RuntimeReceiptEvent{{
							InvocationID:     invocationID,
							Ordinal:          1,
							RuntimeIdentity:  string(arm),
							RuntimeBranch:    "selected",
							FallbackExecuted: false,
						}},
					})
				}
				records = append(records, record)
			}
		}
	}
	return records
}
