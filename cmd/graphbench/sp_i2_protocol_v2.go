// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
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
	Corpus                            spI2ProtocolCorpusV2     `json:"corpus"`
	Simulation                        spI2ProtocolSimulationV2 `json:"simulation"`
	MultiplicityRule                  string                   `json:"multiplicity_rule"`
	V1EvidenceReuse                   bool                     `json:"v1_evidence_reuse"`
	HoldoutAuthorizationBeforeDBSetup bool                     `json:"holdout_authorization_required_before_database_setup"`
}

type spI2ProtocolSimulationV2 struct {
	Implementation            string                     `json:"implementation"`
	RunsPerScenario           int                        `json:"runs_per_scenario"`
	WilsonConfidence          float64                    `json:"wilson_confidence"`
	RequiredPowerLower        float64                    `json:"required_power_lower"`
	RequiredCoverage          float64                    `json:"required_coverage"`
	P95BoundaryFalsePassUpper float64                    `json:"p95_boundary_false_pass_upper"`
	DecisionFalsePassUpper    float64                    `json:"decision_false_pass_upper"`
	TraceRescalingTransform   string                     `json:"trace_rescaling_transform"`
	SourceCommit              string                     `json:"source_commit"`
	BaselineTraceSHA256       string                     `json:"baseline_trace_sha256"`
	CandidateTraceSHA256      string                     `json:"candidate_trace_sha256"`
	P50RoundDrift             []float64                  `json:"p50_round_drift"`
	P95RoundDrift             []float64                  `json:"p95_round_drift"`
	LogStandardErrors         spI2SimulationErrorsV2     `json:"log_standard_errors"`
	AbsoluteStandardErrorsUS  spI2SimulationErrorsV2     `json:"absolute_standard_errors_us"`
	Scenarios                 []spI2SimulationScenarioV2 `json:"scenarios"`
}

type spI2SimulationErrorsV2 struct {
	Pooled        float64 `json:"pooled"`
	OrderStratum  float64 `json:"order_stratum"`
	FirstPosition float64 `json:"first_position"`
}

type spI2SimulationScenarioV2 struct {
	Name                    string  `json:"name"`
	Kind                    string  `json:"kind"`
	BaselineP50US           float64 `json:"baseline_p50_us"`
	BaselineP95US           float64 `json:"baseline_p95_us"`
	CandidateP50US          float64 `json:"candidate_p50_us"`
	CandidateP95US          float64 `json:"candidate_p95_us"`
	OddCandidateMultiplier  float64 `json:"odd_candidate_multiplier"`
	EvenCandidateMultiplier float64 `json:"even_candidate_multiplier"`
	Seed                    string  `json:"seed"`
}

type spI2ProtocolCorpusV2 struct {
	Source                    string `json:"source"`
	TrainingCases             int    `json:"training_cases"`
	HoldoutCases              int    `json:"holdout_cases"`
	TrainingCorpusSHA256      string `json:"training_corpus_sha256"`
	HoldoutCorpusSHA256       string `json:"holdout_corpus_sha256"`
	FullCorpusSHA256          string `json:"full_corpus_sha256"`
	TrainingDeclarationSHA256 string `json:"training_declaration_sha256"`
	HoldoutDeclarationSHA256  string `json:"holdout_declaration_sha256"`
	FullDeclarationSHA256     string `json:"full_declaration_sha256"`
	TrainingResolvedSHA256    string `json:"training_resolved_sha256"`
	HoldoutResolvedSHA256     string `json:"holdout_resolved_sha256"`
	FullResolvedSHA256        string `json:"full_resolved_sha256"`
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

type spI2V2Rejection struct {
	Schema                       string               `json:"schema"`
	Generation                   string               `json:"generation"`
	SourceCommit                 string               `json:"source_commit"`
	ProtocolSHA256               string               `json:"protocol_sha256"`
	SimulationReportSHA256       string               `json:"simulation_report_sha256"`
	SimulationImplementation     string               `json:"simulation_implementation"`
	RunsPerScenario              int                  `json:"runs_per_scenario"`
	FailedGates                  []spI2V2RejectedGate `json:"failed_gates"`
	CoverageCalibrated           bool                 `json:"coverage_calibrated"`
	FormalAAStarted              bool                 `json:"formal_aa_started"`
	CapturePlanCreated           bool                 `json:"capture_plan_created"`
	SealedPreregistrationCreated bool                 `json:"sealed_preregistration_created"`
	HoldoutOpened                bool                 `json:"holdout_opened"`
	ProductionActivated          bool                 `json:"production_activated"`
	SuccessorProtocolRequired    bool                 `json:"successor_protocol_required"`
	Terminal                     bool                 `json:"terminal"`
}

type spI2V2RejectedGate struct {
	Scenario string  `json:"scenario"`
	Metric   string  `json:"metric"`
	Observed float64 `json:"observed"`
	Required float64 `json:"required"`
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

func loadSPI2V2Rejection(path string) (spI2V2Rejection, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return spI2V2Rejection{}, fmt.Errorf("read SP-I2 V2 rejection: %w", err)
	}
	var rejection spI2V2Rejection
	if err := decodePromotionEvidence(raw, &rejection); err != nil {
		return spI2V2Rejection{}, fmt.Errorf("decode SP-I2 V2 rejection: %w", err)
	}
	expected := []spI2V2RejectedGate{
		{Scenario: "aa_identity", Metric: "admission_power_wilson_lower", Observed: 0, Required: 0.9},
		{Scenario: "target_power", Metric: "full_decision_power_wilson_lower", Observed: 0.4724809842358317, Required: 0.9},
		{Scenario: "control_power", Metric: "full_decision_power_wilson_lower", Observed: 0.5053708806725798, Required: 0.9},
		{Scenario: "aa_order_odd_high", Metric: "admission_power_wilson_lower", Observed: 0, Required: 0.9},
		{Scenario: "aa_order_even_high", Metric: "admission_power_wilson_lower", Observed: 0, Required: 0.9},
	}
	if rejection.Schema != "sp-i2-terminal-rejection-v2" || rejection.Generation != spI2GenerationV2 ||
		rejection.SourceCommit != "5df040c2992dd92cf0480beed887c4068c3052b2" ||
		rejection.ProtocolSHA256 != "17cddc5100bc4f523122b0664ec63d3b4954ae2c01000f04864f10fdd00e1e89" ||
		rejection.SimulationReportSHA256 != "cbf4fc593a0adfa72ead23f4f391d530790a474a292a9cc47788a18048b17875" ||
		rejection.SimulationImplementation != spI2PowerSimulationV2 || rejection.RunsPerScenario != 20_000 ||
		!slices.Equal(rejection.FailedGates, expected) || !rejection.CoverageCalibrated || rejection.FormalAAStarted ||
		rejection.CapturePlanCreated || rejection.SealedPreregistrationCreated || rejection.HoldoutOpened || rejection.ProductionActivated ||
		!rejection.SuccessorProtocolRequired || !rejection.Terminal {
		return spI2V2Rejection{}, fmt.Errorf("SP-I2 V2 terminal rejection declaration is invalid")
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
	if protocol.Schema != "sp-i2-tail-protocol-v2" || protocol.Generation != spI2GenerationV2 || protocol.Status != "terminated_inadequate_power" || protocol.ProductionDefault != "off" {
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
	corpus := protocol.Corpus
	if corpus.Source != "cases/generated_sp_i2_distance_v2.json" || corpus.TrainingCases != 8 || corpus.HoldoutCases != 6 ||
		corpus.TrainingCorpusSHA256 != spI2V2TrainingCorpusSHA256 || corpus.HoldoutCorpusSHA256 != spI2V2HoldoutCorpusSHA256 ||
		corpus.FullCorpusSHA256 != spI2V2FullCorpusSHA256 || corpus.TrainingDeclarationSHA256 != spI2V2TrainingDeclarationSHA256 ||
		corpus.HoldoutDeclarationSHA256 != spI2V2HoldoutDeclarationSHA256 || corpus.FullDeclarationSHA256 != spI2V2FullDeclarationSHA256 ||
		corpus.TrainingResolvedSHA256 != spI2V2TrainingResolvedSHA256 || corpus.HoldoutResolvedSHA256 != spI2V2HoldoutResolvedSHA256 ||
		corpus.FullResolvedSHA256 != spI2V2FullResolvedSHA256 {
		return fmt.Errorf("SP-I2 V2 formal corpus contract is invalid")
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
	if err := validateSPI2SimulationProtocolV2(protocol.Simulation); err != nil {
		return err
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

func validateSPI2SimulationProtocolV2(simulation spI2ProtocolSimulationV2) error {
	if simulation.Implementation != spI2PowerSimulationV2 || simulation.RunsPerScenario != 20_000 ||
		simulation.WilsonConfidence != 0.95 || simulation.RequiredPowerLower != 0.90 || simulation.RequiredCoverage != 0.975 ||
		simulation.P95BoundaryFalsePassUpper != 0.015 || simulation.DecisionFalsePassUpper != 0.0275 ||
		simulation.TraceRescalingTransform != "piecewise_log_quantile_anchor_then_paired_empirical_round_drift" ||
		simulation.SourceCommit != "3865cbc57758b7b20b7ffe431f27235873422eed" ||
		simulation.BaselineTraceSHA256 != "ac3ceb27ee92e3f4e21e3994ff9ee82d483b8081e9d44ddcef8e695ffdb1b6d0" ||
		simulation.CandidateTraceSHA256 != "f6d79e81bdaafedaa95568d57140c14e0808fbb6fc261387abc916081137785a" ||
		len(simulation.P50RoundDrift) != 20 || len(simulation.P95RoundDrift) != 20 || len(simulation.Scenarios) != 11 {
		return fmt.Errorf("SP-I2 V2 simulation declaration is invalid")
	}
	if simulation.LogStandardErrors != (spI2SimulationErrorsV2{Pooled: 0.025959, OrderStratum: 0.036712, FirstPosition: 0.036712}) ||
		simulation.AbsoluteStandardErrorsUS != (spI2SimulationErrorsV2{Pooled: 59.338, OrderStratum: 83.917, FirstPosition: 83.917}) {
		return fmt.Errorf("SP-I2 V2 simulation error calibration is invalid")
	}
	expectedKinds := map[string]int{"aa_power": 1, "aa_boundary": 2, "target_power": 1, "target_boundary": 1, "control_power": 1, "control_boundary": 1, "aa_order_power": 2, "aa_order_boundary": 2}
	observedKinds := map[string]int{}
	seen := map[string]struct{}{}
	for _, scenario := range simulation.Scenarios {
		if scenario.Name == "" || scenario.Seed == "" || scenario.BaselineP50US <= 0 || scenario.BaselineP95US <= scenario.BaselineP50US ||
			scenario.CandidateP50US <= 0 || scenario.CandidateP95US <= scenario.CandidateP50US {
			return fmt.Errorf("SP-I2 V2 simulation scenario is invalid")
		}
		if _, duplicate := seen[scenario.Name]; duplicate {
			return fmt.Errorf("SP-I2 V2 simulation scenario %q is duplicated", scenario.Name)
		}
		seen[scenario.Name] = struct{}{}
		observedKinds[scenario.Kind]++
		seed := sha256.Sum256([]byte("sp-i2-power-simulation-v2\x00" + scenario.Name))
		if scenario.Seed != hex.EncodeToString(seed[:]) {
			return fmt.Errorf("SP-I2 V2 simulation scenario %q seed is invalid", scenario.Name)
		}
	}
	if !maps.Equal(observedKinds, expectedKinds) {
		return fmt.Errorf("SP-I2 V2 simulation matrix is incomplete")
	}
	return nil
}
