// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
)

const spI2V2ComponentAuthorizationSchema = "sp-i2-v2-component-authorization-v1"

type SPI2V2ComponentAuthorization struct {
	Schema                    string                             `json:"schema"`
	Generation                string                             `json:"generation"`
	ProtocolDeclarationSHA256 string                             `json:"protocol_declaration_sha256"`
	SourceCommit              string                             `json:"source_commit"`
	DirtyDiffSHA256           string                             `json:"dirty_diff_sha256"`
	BinarySHA256              string                             `json:"binary_sha256"`
	AuthorizedExecutor        string                             `json:"authorized_executor"`
	Components                []SPI2V2ComponentAuthorizationCase `json:"components"`
	Passed                    bool                               `json:"passed"`
}

type SPI2V2ComponentAuthorizationCase struct {
	Executor          string `json:"executor"`
	ArtifactSHA256    string `json:"artifact_sha256"`
	Cases             int    `json:"cases"`
	SemanticPassed    bool   `json:"semantic_passed"`
	PlanPassed        bool   `json:"plan_passed"`
	ResourcePassed    bool   `json:"resource_passed"`
	ReceiptPassed     bool   `json:"receipt_passed"`
	FallbackFree      bool   `json:"fallback_free"`
	CanonicalPlanSeen bool   `json:"canonical_plan_seen"`
}

type spI2V2ComponentSourceIdentity struct {
	sourceCommit    string
	dirtyDiffSHA256 string
	binarySHA256    string
}

func createSPI2V2ComponentAuthorization(corpusRoot, e1dArtifact, e1pArtifact, output string) (bool, error) {
	protocolPath := filepath.Join(corpusRoot, "protocols", "sp_i2_distance_v2.json")
	_, protocolSHA256, err := loadSPI2ProtocolV2(protocolPath)
	if err != nil {
		return false, err
	}
	type componentInput struct {
		executor optimize.ShortestPathExecutor
		path     string
	}
	inputs := []componentInput{
		{executor: optimize.ShortestPathExecutorI2GuardedDistanceV2E1D, path: e1dArtifact},
		{executor: optimize.ShortestPathExecutorI2GuardedDistanceV2E1P, path: e1pArtifact},
	}
	authorization := SPI2V2ComponentAuthorization{
		Schema:                    spI2V2ComponentAuthorizationSchema,
		Generation:                spI2GenerationV2,
		ProtocolDeclarationSHA256: protocolSHA256,
		AuthorizedExecutor:        string(optimize.ShortestPathExecutorI2GuardedDistanceV2E1DP),
		Passed:                    true,
	}
	var source spI2V2ComponentSourceIdentity
	for _, input := range inputs {
		records, err := readJSONLFile(input.path)
		if err != nil {
			return false, fmt.Errorf("read %s component artifact: %w", input.executor, err)
		}
		component, componentSource, err := validateSPI2V2ComponentEvidence(records, input.executor)
		if err != nil {
			return false, err
		}
		component.ArtifactSHA256, err = fileSHA256(input.path)
		if err != nil {
			return false, err
		}
		if source.sourceCommit == "" {
			source = componentSource
		} else if source != componentSource {
			return false, fmt.Errorf("SP-I2 V2 component artifacts do not share one source and binary identity")
		}
		authorization.Components = append(authorization.Components, component)
	}
	authorization.SourceCommit = source.sourceCommit
	authorization.DirtyDiffSHA256 = source.dirtyDiffSHA256
	authorization.BinarySHA256 = source.binarySHA256
	if err := validateSPI2V2ComponentAuthorization(authorization, protocolSHA256); err != nil {
		return false, err
	}
	if output == "" {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(authorization); err != nil {
			return false, err
		}
	} else if err := writeIndentedJSON(output, authorization); err != nil {
		return false, err
	}
	return true, nil
}

func validateSPI2V2ComponentEvidence(records []CaseResult, executor optimize.ShortestPathExecutor) (SPI2V2ComponentAuthorizationCase, spI2V2ComponentSourceIdentity, error) {
	if executor != optimize.ShortestPathExecutorI2GuardedDistanceV2E1D && executor != optimize.ShortestPathExecutorI2GuardedDistanceV2E1P {
		return SPI2V2ComponentAuthorizationCase{}, spI2V2ComponentSourceIdentity{}, fmt.Errorf("unsupported SP-I2 V2 component executor %q", executor)
	}
	cohort, err := canonicalSPI2Cohort()
	if err != nil {
		return SPI2V2ComponentAuthorizationCase{}, spI2V2ComponentSourceIdentity{}, err
	}
	declarations, err := canonicalSPI2Declarations()
	if err != nil {
		return SPI2V2ComponentAuthorizationCase{}, spI2V2ComponentSourceIdentity{}, err
	}
	if len(records) != len(cohort.trainingKeys) {
		return SPI2V2ComponentAuthorizationCase{}, spI2V2ComponentSourceIdentity{}, fmt.Errorf("SP-I2 V2 %s component check contains %d records, expected exactly %d", executor, len(records), len(cohort.trainingKeys))
	}
	component := SPI2V2ComponentAuthorizationCase{
		Executor:          string(executor),
		Cases:             len(records),
		SemanticPassed:    true,
		PlanPassed:        true,
		ResourcePassed:    true,
		ReceiptPassed:     true,
		FallbackFree:      true,
		CanonicalPlanSeen: true,
	}
	seen := make(map[performanceKey]struct{}, len(records))
	seenInvocations := make(map[string]struct{}, len(records)*2)
	var source spI2V2ComponentSourceIdentity
	for _, record := range records {
		key := performanceKey{dataset: record.Dataset, name: record.Name, backend: record.ExecutionMode}
		if _, expected := cohort.trainingKeys[key]; !expected {
			return SPI2V2ComponentAuthorizationCase{}, spI2V2ComponentSourceIdentity{}, fmt.Errorf("SP-I2 V2 %s component check contains unexpected case %s/%s", executor, record.Dataset, record.Name)
		}
		if _, duplicate := seen[key]; duplicate {
			return SPI2V2ComponentAuthorizationCase{}, spI2V2ComponentSourceIdentity{}, fmt.Errorf("SP-I2 V2 %s component check duplicates %s/%s", executor, record.Dataset, record.Name)
		}
		seen[key] = struct{}{}
		declaration := declarations[key]
		if err := validateSPI2V2ComponentRecord(record, executor, declaration, seenInvocations); err != nil {
			return SPI2V2ComponentAuthorizationCase{}, spI2V2ComponentSourceIdentity{}, err
		}
		currentSource := spI2V2ComponentSourceIdentity{
			sourceCommit:    record.Environment.SourceCommit,
			dirtyDiffSHA256: record.Environment.DirtyDiffSHA256,
			binarySHA256:    record.Environment.BinarySHA256,
		}
		if source.sourceCommit == "" {
			source = currentSource
		} else if source != currentSource {
			return SPI2V2ComponentAuthorizationCase{}, spI2V2ComponentSourceIdentity{}, fmt.Errorf("SP-I2 V2 %s component check mixes source or binary identities", executor)
		}
	}
	if strings.TrimSpace(source.sourceCommit) == "" || !lowercaseSHA256(source.dirtyDiffSHA256) || !lowercaseSHA256(source.binarySHA256) {
		return SPI2V2ComponentAuthorizationCase{}, spI2V2ComponentSourceIdentity{}, fmt.Errorf("SP-I2 V2 %s component check lacks a complete source and binary identity", executor)
	}
	return component, source, nil
}

func validateSPI2V2ComponentRecord(record CaseResult, executor optimize.ShortestPathExecutor, declaration spI2CanonicalDeclaration, seenInvocations map[string]struct{}) error {
	if record.ExecutionMode != ModePostgresSQL || record.Status != StatusOK || record.Environment == nil || record.PostgresEnvironment == nil || record.Fixture == nil ||
		record.PostgresMetrics == nil || record.TraversalTelemetry == nil || record.Optimization == nil || record.Environment.ArtifactSchemaVersion != 2 ||
		record.Environment.PoolSize != 1 || len(record.Environment.Concurrency) != 0 || record.Environment.ExistingGraph ||
		record.Environment.WarmupIterations != 1 || record.Stats.WarmupIterations != 1 || record.Stats.Iterations != 1 || len(record.Stats.Samples) != 1 ||
		record.Environment.Round != 1 || record.Environment.Block != 1 || record.Environment.ArmOrder != 1 ||
		record.Environment.Arm != string(executor) || strings.TrimSpace(record.Environment.RunUUID) == "" {
		return fmt.Errorf("%s/%s lacks the exact SP-I2 V2 %s component-check measurement contract", record.Dataset, record.Name, executor)
	}
	if !strings.EqualFold(strings.TrimSpace(record.PostgresEnvironment.TransactionIsolation), "repeatable read") ||
		len(record.PostgresPlanJSON) == 0 || record.PostgresMetrics.PlanningMS == nil || *record.PostgresMetrics.PlanningMS <= 0 || len(record.PostgresMetrics.PlanNodes) == 0 {
		return fmt.Errorf("%s/%s SP-I2 V2 %s component check lacks one canonical planned Repeatable Read observation", record.Dataset, record.Name, executor)
	}
	testCase := declaration.testCase
	testCase.Source = record.Source
	expected := newCaseResult(testCase, ModePostgresSQL, nil)
	attachFixtureMetadata(&expected, *record.Fixture)
	if filepath.Base(record.Source) != "generated_sp_i2_distance_v1.json" || record.Category != testCase.Category || record.Cypher != testCase.Cypher ||
		record.WorkloadSHA256 != expected.WorkloadSHA256 || !reflect.DeepEqual(record.NodeParams, testCase.NodeParams) ||
		!reflect.DeepEqual(record.NodeListParams, testCase.NodeListParams) || !reflect.DeepEqual(record.Shape, testCase.Shape) ||
		!record.StableObservation || record.ExpectedRowCount == nil || testCase.Expected.RowCount == nil ||
		*record.ExpectedRowCount != *testCase.Expected.RowCount || record.RowCount != *testCase.Expected.RowCount {
		return fmt.Errorf("%s/%s SP-I2 V2 %s component check changes the exact open-corpus semantic contract", record.Dataset, record.Name, executor)
	}
	if err := validateExpectedObservations(testCase.Expected, record.ObservedRows); err != nil {
		return fmt.Errorf("%s/%s SP-I2 V2 %s component observation: %w", record.Dataset, record.Name, executor, err)
	}
	if err := validateSPI2V2DevelopmentSamples(record, executor, seenInvocations); err != nil {
		return err
	}
	telemetry := record.TraversalTelemetry
	if err := ValidateTraversalExecutionTelemetry(telemetry); err != nil {
		return fmt.Errorf("%s/%s SP-I2 V2 %s telemetry: %w", record.Dataset, record.Name, executor, err)
	}
	summary := telemetry.Summary
	if telemetry.Level != TraversalTelemetryLevelDiagnostic || telemetry.Diagnostic == nil ||
		telemetry.Diagnostic.CounterStatus != TraversalTelemetryCounterStatusComplete || telemetry.Diagnostic.PlanReplay == nil ||
		summary.RequestedIdentity != string(executor) || summary.RuntimeIdentity != string(executor) || summary.AppliedIdentity != string(executor) ||
		summary.EmittedIdentity != optimize.ShortestPathPolicyI2DistanceGuardedV2 || summary.FallbackExecuted == nil || *summary.FallbackExecuted ||
		summary.Overflow == nil || *summary.Overflow || summary.RuntimeOutcomeAvailable == nil || !*summary.RuntimeOutcomeAvailable {
		return fmt.Errorf("%s/%s SP-I2 V2 %s component check lacks one exact non-fallback runtime receipt", record.Dataset, record.Name, executor)
	}
	gateCase := evaluateProductionResourceGateCase(record)
	if !gateCase.Passed {
		return fmt.Errorf("%s/%s SP-I2 V2 %s component resource/plan invariants failed: %s", record.Dataset, record.Name, executor, strings.Join(gateCase.Reasons, "; "))
	}
	if executor == optimize.ShortestPathExecutorI2GuardedDistanceV2E1D && strings.Contains(record.Name, "cycle-control") {
		plan := telemetry.Diagnostic.PlanReplay.Counters
		for _, counter := range []string{"recursive_rows", "recursive_loops", "reverse_edge_probe_loops"} {
			if _, present := plan[counter]; !present {
				return fmt.Errorf("%s/%s SP-I2 V2 E1D direct-cycle plan lacks exact zero counter %s", record.Dataset, record.Name, counter)
			}
		}
		if summary.RuntimeBranch != "inline_direct_distance" || plan["sp_i2_direct_rows"] != 1 || plan["sp_i2_distance_rows"] != 0 ||
			plan["recursive_rows"] != 0 || plan["recursive_loops"] != 0 || plan["reverse_edge_probe_loops"] != 0 || plan["sp_i2_admission_rows"] != 0 ||
			plan["sp_i2_admission_loops"] != 0 || plan["sp_i2_fallback_executor_loops"] != 0 {
			return fmt.Errorf("%s/%s SP-I2 V2 E1D direct-cycle plan did not suppress recursive, admission, and fallback work", record.Dataset, record.Name)
		}
	}
	if executor == optimize.ShortestPathExecutorI2GuardedDistanceV2E1P &&
		(record.Shape.PathMaterializationRequired || record.PostgresMetrics.HydrationRows != 0 || record.PostgresMetrics.HydrationLoops != 0) {
		return fmt.Errorf("%s/%s SP-I2 V2 E1P component check unexpectedly hydrates a scalar result", record.Dataset, record.Name)
	}
	return nil
}

func loadSPI2V2ComponentAuthorization(path, protocolSHA256 string) (SPI2V2ComponentAuthorization, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return SPI2V2ComponentAuthorization{}, err
	}
	var authorization SPI2V2ComponentAuthorization
	if err := decodePromotionEvidence(raw, &authorization); err != nil {
		return SPI2V2ComponentAuthorization{}, fmt.Errorf("decode SP-I2 V2 component authorization: %w", err)
	}
	if err := validateSPI2V2ComponentAuthorization(authorization, protocolSHA256); err != nil {
		return SPI2V2ComponentAuthorization{}, err
	}
	return authorization, nil
}

func validateSPI2V2ComponentAuthorization(authorization SPI2V2ComponentAuthorization, protocolSHA256 string) error {
	expectedExecutors := []string{
		string(optimize.ShortestPathExecutorI2GuardedDistanceV2E1D),
		string(optimize.ShortestPathExecutorI2GuardedDistanceV2E1P),
	}
	if authorization.Schema != spI2V2ComponentAuthorizationSchema || authorization.Generation != spI2GenerationV2 ||
		authorization.ProtocolDeclarationSHA256 != protocolSHA256 || !lowercaseSHA256(protocolSHA256) ||
		authorization.AuthorizedExecutor != string(optimize.ShortestPathExecutorI2GuardedDistanceV2E1DP) || !authorization.Passed ||
		strings.TrimSpace(authorization.SourceCommit) == "" || !lowercaseSHA256(authorization.DirtyDiffSHA256) || !lowercaseSHA256(authorization.BinarySHA256) ||
		len(authorization.Components) != len(expectedExecutors) {
		return fmt.Errorf("SP-I2 V2 component authorization identity is invalid")
	}
	for index, component := range authorization.Components {
		if component.Executor != expectedExecutors[index] || !lowercaseSHA256(component.ArtifactSHA256) || component.Cases != 6 ||
			!component.SemanticPassed || !component.PlanPassed || !component.ResourcePassed || !component.ReceiptPassed ||
			!component.FallbackFree || !component.CanonicalPlanSeen {
			return fmt.Errorf("SP-I2 V2 component authorization does not prove exact E1D/E1P eligibility")
		}
	}
	return nil
}

func validateSPI2V2ComponentAuthorizationForCapture(path, corpusRoot string) error {
	_, protocolSHA256, err := loadSPI2ProtocolV2(filepath.Join(corpusRoot, "protocols", "sp_i2_distance_v2.json"))
	if err != nil {
		return err
	}
	authorization, err := loadSPI2V2ComponentAuthorization(path, protocolSHA256)
	if err != nil {
		return err
	}
	if authorization.SourceCommit != commandOutput("git", "rev-parse", "HEAD") ||
		authorization.DirtyDiffSHA256 != workingTreeSHA256() || authorization.BinarySHA256 != executableSHA256() {
		return fmt.Errorf("SP-I2 V2 component authorization does not bind the current source tree and executable")
	}
	return nil
}

func spI2V2ComponentExecutors() []optimize.ShortestPathExecutor {
	return []optimize.ShortestPathExecutor{
		optimize.ShortestPathExecutorI2GuardedDistanceV2E1D,
		optimize.ShortestPathExecutorI2GuardedDistanceV2E1P,
	}
}

func validSPI2V2ComponentExecutor(executor optimize.ShortestPathExecutor) bool {
	return slices.Contains(spI2V2ComponentExecutors(), executor)
}
