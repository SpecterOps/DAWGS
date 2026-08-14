// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"slices"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
)

const (
	spI2GenerationV1 = "sp-i2-distance-v1"
	spI2GenerationV2 = "sp-i2-distance-v2"
)

type spI2ProtocolV2 struct {
	Schema                            string                   `json:"schema"`
	Generation                        string                   `json:"generation"`
	Status                            string                   `json:"status"`
	ProductionDefault                 string                   `json:"production_default"`
	Identities                        spI2ProtocolIdentitiesV2 `json:"identities"`
	DevelopmentExecutors              []string                 `json:"development_executors"`
	SelectedArchitecture              string                   `json:"selected_architecture"`
	Limits                            spI2ProtocolLimitsV2     `json:"limits"`
	Design                            spI2ProtocolDesignV2     `json:"design"`
	Gates                             spI2ProtocolGatesV2      `json:"gates"`
	OperationalDesign                 spI2OperationalDesignV2  `json:"operational_design"`
	Bootstrap                         spI2ProtocolBootstrapV2  `json:"bootstrap"`
	HostAdmission                     spI2HostAdmissionV2      `json:"host_admission"`
	MultiplicityRule                  string                   `json:"multiplicity_rule"`
	V1EvidenceReuse                   bool                     `json:"v1_evidence_reuse"`
	HoldoutAuthorizationBeforeDBSetup bool                     `json:"holdout_authorization_required_before_database_setup"`
}

type spI2V1Rejection struct {
	Schema                string             `json:"schema"`
	Generation            string             `json:"generation"`
	Executor              string             `json:"executor"`
	Policy                string             `json:"policy"`
	Selector              string             `json:"selector"`
	SourceCommit          string             `json:"source_commit"`
	DiscoveryReportSHA256 string             `json:"discovery_report_sha256"`
	FailedGate            spI2V1RejectedGate `json:"failed_gate"`
	FreezeCreated         bool               `json:"freeze_created"`
	HoldoutOpened         bool               `json:"holdout_opened"`
	Terminal              bool               `json:"terminal"`
}

type spI2V1RejectedGate struct {
	Metric   string  `json:"metric"`
	Observed float64 `json:"observed"`
	Limit    float64 `json:"limit"`
}

type spI2ProtocolIdentitiesV2 struct {
	Executor                  string `json:"executor"`
	Policy                    string `json:"policy"`
	Selector                  string `json:"selector"`
	FallbackExecutor          string `json:"fallback_executor"`
	FallbackInternalExecutor  string `json:"fallback_internal_executor"`
	TrainingTag               string `json:"training_tag"`
	HoldoutTag                string `json:"holdout_tag"`
	DevelopmentTag            string `json:"development_tag"`
	QualificationSchema       string `json:"qualification_schema"`
	FreezeSchema              string `json:"freeze_schema"`
	AASchema                  string `json:"aa_schema"`
	PromotionManifestSchema   int    `json:"promotion_manifest_schema"`
	RollbackSwitch            string `json:"rollback_switch"`
	StatisticalImplementation string `json:"statistical_implementation"`
}

type spI2ProtocolLimitsV2 struct {
	StateRows    int64 `json:"state_rows"`
	FrontierRows int64 `json:"frontier_rows"`
	MaximumDepth int64 `json:"maximum_depth"`
	MinimumDepth int64 `json:"minimum_depth"`
}

type spI2ProtocolDesignV2 struct {
	Seed                   int     `json:"seed"`
	ConfidenceLevel        float64 `json:"confidence_level"`
	BootstrapReplicates    int     `json:"bootstrap_replicates"`
	Rounds                 int     `json:"rounds"`
	OrdinaryWarmups        int     `json:"ordinary_warmups"`
	AttestedStabilizations int     `json:"attested_stabilizations"`
	TimedSamplesPerRound   int     `json:"timed_samples_per_round"`
	PoolSize               int     `json:"pool_size"`
	Isolation              string  `json:"isolation"`
	ArmOrder               string  `json:"arm_order"`
}

type spI2ProtocolGatesV2 struct {
	TargetMedianRatioUpper         float64 `json:"target_median_ratio_upper"`
	TargetMedianSavingLowerUS      int64   `json:"target_median_saving_lower_us"`
	ControlMedianRatioUpper        float64 `json:"control_median_ratio_upper"`
	ControlMedianOverheadUpperUS   int64   `json:"control_median_overhead_upper_us"`
	P95RatioUpper                  float64 `json:"p95_ratio_upper"`
	ControlP95OverheadUpperUS      int64   `json:"control_p95_overhead_upper_us"`
	AAEquivalenceRatio             float64 `json:"aa_equivalence_ratio"`
	AAFirstPositionRatioUpper      float64 `json:"aa_first_position_ratio_upper"`
	AAFirstPositionOverheadUpperUS int64   `json:"aa_first_position_overhead_upper_us"`
	SessionFirstP95RatioUpper      float64 `json:"session_first_p95_ratio_upper"`
	SessionFirstP95OverheadUpperUS int64   `json:"session_first_p95_overhead_upper_us"`
}

type spI2OperationalDesignV2 struct {
	Blocks                       int    `json:"blocks"`
	FreshSessionsPerArmCaseBlock int    `json:"fresh_sessions_per_arm_case_block"`
	SamplesPerArmCase            int    `json:"samples_per_arm_case"`
	PlanCacheMode                string `json:"plan_cache_mode"`
}

type spI2ProtocolBootstrapV2 struct {
	Domain                string   `json:"domain"`
	CaseOrder             []string `json:"case_order"`
	RatioScale            string   `json:"ratio_scale"`
	LowerPercentile       float64  `json:"lower_percentile"`
	UpperPercentile       float64  `json:"upper_percentile"`
	Quantile              string   `json:"quantile"`
	RoundResampling       string   `json:"round_resampling"`
	WithinRoundResampling string   `json:"within_round_resampling"`
}

type spI2HostAdmissionV2 struct {
	Sequence                              []string                    `json:"sequence"`
	MaximumS4Remediations                 int                         `json:"maximum_s4_remediations"`
	CandidateEpochLockedOnFirstInvocation bool                        `json:"candidate_epoch_locked_on_first_invocation"`
	MachineThresholds                     spI2HostMachineThresholdsV2 `json:"machine_thresholds"`
	RemediableCauses                      []string                    `json:"remediable_causes"`
}

type spI2HostMachineThresholdsV2 struct {
	RunnerProcessOverlapCount int     `json:"runner_process_overlap_count"`
	ThermalThrottleEvents     int     `json:"thermal_throttle_events"`
	MaximumStealTimePercent   float64 `json:"maximum_steal_time_percent"`
	CPUGovernor               string  `json:"cpu_governor"`
	PostgreSQLSessionSettings string  `json:"postgresql_session_settings"`
}

func loadSPI2ProtocolV2(path string) (spI2ProtocolV2, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return spI2ProtocolV2{}, "", fmt.Errorf("read SP-I2 V2 protocol: %w", err)
	}
	var protocol spI2ProtocolV2
	if err := decodePromotionEvidence(raw, &protocol); err != nil {
		return spI2ProtocolV2{}, "", fmt.Errorf("decode SP-I2 V2 protocol: %w", err)
	}
	if err := validateSPI2ProtocolV2(protocol); err != nil {
		return spI2ProtocolV2{}, "", err
	}
	digest := sha256.Sum256(raw)
	return protocol, hex.EncodeToString(digest[:]), nil
}

func loadSPI2V1Rejection(path string) (spI2V1Rejection, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return spI2V1Rejection{}, fmt.Errorf("read SP-I2 V1 rejection: %w", err)
	}
	var rejection spI2V1Rejection
	if err := decodePromotionEvidence(raw, &rejection); err != nil {
		return spI2V1Rejection{}, fmt.Errorf("decode SP-I2 V1 rejection: %w", err)
	}
	if rejection.Schema != "sp-i2-terminal-rejection-v1" || rejection.Generation != spI2GenerationV1 ||
		rejection.Executor != string(optimize.ShortestPathExecutorI2GuardedDistance) || rejection.Policy != optimize.ShortestPathPolicyI2DistanceGuardedV1 ||
		rejection.Selector != optimize.ShortestPathSelectorStaticV8HiddenFanIn || rejection.SourceCommit != "3865cbc57758b7b20b7ffe431f27235873422eed" ||
		rejection.DiscoveryReportSHA256 != "f80b0f54624de79e9161673f7c9971662bcd5286bf70829176febc6de2681309" ||
		rejection.FailedGate.Metric != "p95_ratio_upper" || rejection.FailedGate.Observed != 1.2528773826285173 || rejection.FailedGate.Limit != 1.05 ||
		rejection.FreezeCreated || rejection.HoldoutOpened || !rejection.Terminal {
		return spI2V1Rejection{}, fmt.Errorf("SP-I2 V1 terminal rejection declaration is invalid")
	}
	return rejection, nil
}

func validateSPI2ProtocolV2(protocol spI2ProtocolV2) error {
	expectedDevelopment := []string{
		string(optimize.ShortestPathExecutorI2GuardedDistanceV2E0),
		string(optimize.ShortestPathExecutorI2GuardedDistanceV2E1),
		string(optimize.ShortestPathExecutorI2GuardedDistanceV2E1D),
		string(optimize.ShortestPathExecutorI2GuardedDistanceV2E1P),
		string(optimize.ShortestPathExecutorI2GuardedDistanceV2E1DP),
	}
	if protocol.Schema != "sp-i2-tail-protocol-v2" || protocol.Generation != spI2GenerationV2 || protocol.ProductionDefault != "off" {
		return fmt.Errorf("SP-I2 V2 protocol identity is invalid")
	}
	identities := protocol.Identities
	if identities.Executor != string(optimize.ShortestPathExecutorI2GuardedDistanceV2) ||
		identities.Policy != optimize.ShortestPathPolicyI2DistanceGuardedV2 ||
		identities.Selector != optimize.ShortestPathSelectorStaticV9HiddenFanInTail ||
		identities.FallbackExecutor != string(optimize.ShortestPathExecutorS4CanonicalDistance) ||
		identities.StatisticalImplementation != spI2HierBootstrapV2 ||
		identities.PromotionManifestSchema != 3 || identities.RollbackSwitch != "DisableInlineSPDistance" {
		return fmt.Errorf("SP-I2 V2 protocol compiled identities do not match the declaration")
	}
	if identities.Executor == string(optimize.ShortestPathExecutorI2GuardedDistance) ||
		identities.Policy == optimize.ShortestPathPolicyI2DistanceGuardedV1 ||
		identities.Selector == optimize.ShortestPathSelectorStaticV8HiddenFanIn {
		return fmt.Errorf("SP-I2 V2 protocol collides with V1 identity")
	}
	if !slices.Equal(protocol.DevelopmentExecutors, expectedDevelopment) || slices.Contains(protocol.DevelopmentExecutors, identities.Executor) {
		return fmt.Errorf("SP-I2 V2 development executor registry is invalid")
	}
	if protocol.SelectedArchitecture != "E1" || protocol.Limits.StateRows != optimize.ShortestPathI2QualifiedStateLimit ||
		protocol.Limits.FrontierRows != optimize.ShortestPathI2QualifiedFrontierLimit || protocol.Limits.MinimumDepth != 1 || protocol.Limits.MaximumDepth != 64 {
		return fmt.Errorf("SP-I2 V2 architecture or cap contract is invalid")
	}
	if protocol.Design.Seed != 1 || protocol.Design.ConfidenceLevel != 0.975 || protocol.Design.BootstrapReplicates != 100_000 ||
		protocol.Design.Rounds != 40 || protocol.Design.OrdinaryWarmups != 25 || protocol.Design.AttestedStabilizations != 1 ||
		protocol.Design.TimedSamplesPerRound != 100 || protocol.Design.PoolSize != 1 || protocol.Design.Isolation != "repeatable_read" {
		return fmt.Errorf("SP-I2 V2 fixed capture design is invalid")
	}
	if protocol.Bootstrap.Domain != "sp-i2-tail-bootstrap-v2" || !slices.Equal(protocol.Bootstrap.CaseOrder, []string{"dataset", "case"}) ||
		protocol.Bootstrap.RatioScale != "log" || protocol.Bootstrap.LowerPercentile != 0.0125 || protocol.Bootstrap.UpperPercentile != 0.9875 ||
		protocol.Bootstrap.Quantile != "nearest_rank" || protocol.Bootstrap.RoundResampling != "paired" ||
		protocol.Bootstrap.WithinRoundResampling != "independent_by_arm" {
		return fmt.Errorf("SP-I2 V2 bootstrap declaration is invalid")
	}
	thresholds := protocol.HostAdmission.MachineThresholds
	if !slices.Equal(protocol.HostAdmission.Sequence, []string{"S4/S4", "V2/V2", "S4/V2"}) || protocol.HostAdmission.MaximumS4Remediations != 1 ||
		!protocol.HostAdmission.CandidateEpochLockedOnFirstInvocation || thresholds.RunnerProcessOverlapCount != 0 || thresholds.ThermalThrottleEvents != 0 ||
		thresholds.MaximumStealTimePercent != 1 || thresholds.CPUGovernor != "performance" || thresholds.PostgreSQLSessionSettings != "exact_match" {
		return fmt.Errorf("SP-I2 V2 host admission contract is invalid")
	}
	if protocol.V1EvidenceReuse || !protocol.HoldoutAuthorizationBeforeDBSetup || protocol.MultiplicityRule != "intersection_union_all_cases_must_pass" {
		return fmt.Errorf("SP-I2 V2 evidence isolation contract is invalid")
	}
	return nil
}
