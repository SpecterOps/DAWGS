// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	pgdriver "github.com/specterops/dawgs/drivers/pg"
)

const (
	// operationalGateVersion identifies the serialized operational-evidence schema.
	operationalGateVersion = 2

	// OperationalScenarioCandidateMatrix exercises an admitted candidate under
	// one pool-size, concurrency, and PostgreSQL plan-cache-mode cell.
	OperationalScenarioCandidateMatrix OperationalEvidenceScenario = "candidate_matrix"
	// OperationalScenarioLowWorkMem proves admitted execution under constrained work_mem.
	OperationalScenarioLowWorkMem OperationalEvidenceScenario = "low_work_mem"
	// OperationalScenarioCancellation proves bounded cancellation and same-session recovery.
	OperationalScenarioCancellation OperationalEvidenceScenario = "cancellation_replay"
	// OperationalScenarioConcurrentWriter proves Repeatable Read stability across a concurrent commit.
	OperationalScenarioConcurrentWriter OperationalEvidenceScenario = "repeatable_read_concurrent_writer"
	// OperationalScenarioSessionIsolation proves invocation-local state does not cross sessions.
	OperationalScenarioSessionIsolation OperationalEvidenceScenario = "session_isolation"
	// OperationalScenarioForcedOverflow proves the candidate's exact overflow fallback receipt chain.
	OperationalScenarioForcedOverflow OperationalEvidenceScenario = "forced_overflow_fallback"
)

var (
	defaultOperationalPoolSizes      = []int{1, 2, 8}
	defaultOperationalConcurrency    = []int{1, 8, 16}
	defaultOperationalPlanCacheModes = []string{"auto", "force_custom_plan", "force_generic_plan"}
)

// OperationalEvidenceScenario identifies the independently validated operational proof in a record.
type OperationalEvidenceScenario string

// OperationalGateRequirements freezes the generic operational contract while
// allowing a policy identity to differ from the candidate arm it dispatches.
type OperationalGateRequirements struct {
	// CandidateRuntimeIdentity is the admitted executor recorded at the timed invocation boundary.
	CandidateRuntimeIdentity string `json:"candidate_runtime_identity"`
	// FallbackRuntimeIdentity is the first exact fallback required in an overflow receipt chain.
	FallbackRuntimeIdentity string `json:"fallback_runtime_identity"`
	// PoolSizes contains every required connection-pool size.
	PoolSizes []int `json:"pool_sizes"`
	// ConcurrencyLevels contains every required concurrent worker count.
	ConcurrencyLevels []int `json:"concurrency_levels"`
	// PlanCacheModes contains every required PostgreSQL plan_cache_mode.
	PlanCacheModes []string `json:"plan_cache_modes"`
	// LowWorkMemMaximumBytes is the largest work_mem setting accepted as constrained evidence.
	LowWorkMemMaximumBytes int64 `json:"low_work_mem_maximum_bytes"`
	// CancellationMaximum is the exclusive cancellation-latency ceiling.
	CancellationMaximum time.Duration `json:"cancellation_maximum"`
	// RequireCleanSource rejects operational evidence captured from a dirty source tree.
	RequireCleanSource bool `json:"require_clean_source"`
	// CandidateSQLFingerprint repeats the independently frozen manifest identity
	// anchor for every non-overflow scenario. It cannot introduce a new digest.
	CandidateSQLFingerprint string `json:"candidate_sql_fingerprint"`
}

// defaultOperationalGateRequirements returns the promotion-grade operational matrix.
func defaultOperationalGateRequirements(candidateRuntimeIdentity, fallbackRuntimeIdentity string) OperationalGateRequirements {
	return OperationalGateRequirements{
		CandidateRuntimeIdentity: candidateRuntimeIdentity,
		FallbackRuntimeIdentity:  fallbackRuntimeIdentity,
		PoolSizes:                append([]int(nil), defaultOperationalPoolSizes...),
		ConcurrencyLevels:        append([]int(nil), defaultOperationalConcurrency...),
		PlanCacheModes:           append([]string(nil), defaultOperationalPlanCacheModes...),
		LowWorkMemMaximumBytes:   64 * 1024,
		CancellationMaximum:      250 * time.Millisecond,
		RequireCleanSource:       true,
	}
}

// OperationalCancellationEvidence records the expected timeout and the
// successful replay performed after rollback on the same PostgreSQL backend.
type OperationalCancellationEvidence struct {
	SQLState               string        `json:"sql_state"`
	Latency                time.Duration `json:"latency"`
	TransactionRolledBack  bool          `json:"transaction_rolled_back"`
	CancelledBackendPID    uint32        `json:"cancelled_backend_pid"`
	ReplayBackendPID       uint32        `json:"replay_backend_pid"`
	ReplaySucceeded        bool          `json:"replay_succeeded"`
	ReplayCandidateReceipt LatencySample `json:"replay_candidate_receipt"`
}

// OperationalSnapshotEvidence records a reader snapshot before and after a
// distinct concurrent writer commits.
type OperationalSnapshotEvidence struct {
	ReaderBackendPID            uint32 `json:"reader_backend_pid"`
	WriterBackendPID            uint32 `json:"writer_backend_pid"`
	ReaderIsolation             string `json:"reader_isolation"`
	WriterAffectedRows          int64  `json:"writer_affected_rows"`
	WriterCommitted             bool   `json:"writer_committed"`
	ObservationBeforeSHA256     string `json:"observation_before_sha256"`
	ObservationAfterSHA256      string `json:"observation_after_sha256"`
	PostCommitObservationSHA256 string `json:"post_commit_observation_sha256"`
}

// OperationalSessionIsolationEvidence records two invocation-local sessions
// and the rows each session could observe from the other's invocation.
type OperationalSessionIsolationEvidence struct {
	SessionABackendPID       uint32        `json:"session_a_backend_pid"`
	SessionBBackendPID       uint32        `json:"session_b_backend_pid"`
	SessionAInvocationID     string        `json:"session_a_invocation_id"`
	SessionBInvocationID     string        `json:"session_b_invocation_id"`
	SessionAOwnRows          int64         `json:"session_a_own_rows"`
	SessionBOwnRows          int64         `json:"session_b_own_rows"`
	SessionAObservedBRows    int64         `json:"session_a_observed_b_rows"`
	SessionBObservedARows    int64         `json:"session_b_observed_a_rows"`
	SessionACandidateReceipt LatencySample `json:"session_a_candidate_receipt"`
	SessionBCandidateReceipt LatencySample `json:"session_b_candidate_receipt"`
}

// OperationalEvidenceRecord binds one proof to the exact promotion identity,
// source archive, benchmark environment, case, and runtime receipts.
type OperationalEvidenceRecord struct {
	ID                string                               `json:"id"`
	Scenario          OperationalEvidenceScenario          `json:"scenario"`
	PromotionIdentity PromotionEvidenceIdentity            `json:"promotion_identity"`
	SourceSHA256      string                               `json:"source_sha256"`
	Concurrency       int                                  `json:"concurrency,omitempty"`
	Result            CaseResult                           `json:"result"`
	Cancellation      *OperationalCancellationEvidence     `json:"cancellation,omitempty"`
	Snapshot          *OperationalSnapshotEvidence         `json:"snapshot,omitempty"`
	SessionIsolation  *OperationalSessionIsolationEvidence `json:"session_isolation,omitempty"`
}

// OperationalGateInput is the strict, portable source document consumed by
// the operational report generator.
type OperationalGateInput struct {
	Version           int                         `json:"version"`
	PromotionIdentity PromotionEvidenceIdentity   `json:"promotion_identity"`
	Requirements      OperationalGateRequirements `json:"requirements"`
	Records           []OperationalEvidenceRecord `json:"records"`
}

// OperationalMatrixCell identifies one required candidate execution cell.
type OperationalMatrixCell struct {
	PoolSize      int    `json:"pool_size"`
	Concurrency   int    `json:"concurrency"`
	PlanCacheMode string `json:"plan_cache_mode"`
}

// OperationalGateCoverage reports independently machine-checkable coverage of
// the matrix and each non-matrix operational proof.
type OperationalGateCoverage struct {
	RequiredMatrixCells    int                     `json:"required_matrix_cells"`
	ObservedMatrixCells    int                     `json:"observed_matrix_cells"`
	MissingMatrixCells     []OperationalMatrixCell `json:"missing_matrix_cells,omitempty"`
	LowWorkMem             bool                    `json:"low_work_mem"`
	CancellationReplay     bool                    `json:"cancellation_replay"`
	RepeatableReadWriter   bool                    `json:"repeatable_read_concurrent_writer"`
	SessionIsolation       bool                    `json:"session_isolation"`
	ForcedOverflowFallback bool                    `json:"forced_overflow_fallback"`
}

// OperationalGateRecord reports validation of one source evidence record.
type OperationalGateRecord struct {
	ID            string                      `json:"id"`
	Scenario      OperationalEvidenceScenario `json:"scenario"`
	Dataset       string                      `json:"dataset,omitempty"`
	Name          string                      `json:"name,omitempty"`
	PoolSize      int                         `json:"pool_size,omitempty"`
	Concurrency   int                         `json:"concurrency,omitempty"`
	PlanCacheMode string                      `json:"plan_cache_mode,omitempty"`
	WorkMemBytes  int64                       `json:"work_mem_bytes,omitempty"`
	Passed        bool                        `json:"passed"`
	Reasons       []string                    `json:"reasons,omitempty"`
}

// OperationalGateReport is the promotion-manifest operational evidence role.
// PromotionIdentity is deliberately repeated verbatim for manifest closure.
type OperationalGateReport struct {
	Version           int                         `json:"version"`
	PromotionIdentity PromotionEvidenceIdentity   `json:"promotion_identity"`
	Requirements      OperationalGateRequirements `json:"requirements"`
	// Input retains the complete source evidence so final promotion
	// verification can independently rebuild every gate decision.
	Input OperationalGateInput `json:"input"`
	// InputSHA256 binds Input's canonical JSON representation. It detects raw
	// evidence changes even when a forged summary is left untouched.
	InputSHA256 string                  `json:"input_sha256"`
	Passed      bool                    `json:"passed"`
	Coverage    OperationalGateCoverage `json:"coverage"`
	Records     []OperationalGateRecord `json:"records"`
	Reasons     []string                `json:"reasons,omitempty"`
}

// buildOperationalGateReport validates operational evidence without relying
// on filenames, CLI arguments, or human interpretation of integration logs.
func buildOperationalGateReport(identity PromotionEvidenceIdentity, requirements OperationalGateRequirements, records []OperationalEvidenceRecord) OperationalGateReport {
	input, inputSHA256, inputErr := canonicalOperationalGateInput(identity, requirements, records)
	if inputErr == nil {
		identity = input.PromotionIdentity
		requirements = input.Requirements
		records = input.Records
	}
	report := OperationalGateReport{
		Version:           operationalGateVersion,
		PromotionIdentity: cloneOperationalPromotionIdentity(identity),
		Requirements:      cloneOperationalRequirements(requirements),
		Input:             input,
		InputSHA256:       inputSHA256,
		Passed:            true,
	}
	if inputErr != nil {
		report.Reasons = append(report.Reasons, "operational input cannot be canonically embedded: "+inputErr.Error())
	}
	report.Reasons = append(report.Reasons, validateOperationalIdentity(identity)...)
	report.Reasons = append(report.Reasons, validateOperationalRequirements(identity, requirements)...)
	if len(records) == 0 {
		report.Reasons = append(report.Reasons, "operational evidence is empty")
	}

	validMatrix := map[OperationalMatrixCell]struct{}{}
	validScenarios := map[OperationalEvidenceScenario]bool{}
	seenMatrix := map[OperationalMatrixCell]int{}
	seenScenarios := map[OperationalEvidenceScenario]int{}
	seenIDs := map[string]struct{}{}
	operationalWorkload := ""
	operationalCandidateSQL := ""
	operationalTranslationTarget := ""
	var databaseIdentity *PostgresEnvironment

	for _, record := range records {
		decision := OperationalGateRecord{
			ID:          record.ID,
			Scenario:    record.Scenario,
			Dataset:     record.Result.Dataset,
			Name:        record.Result.Name,
			Concurrency: record.Concurrency,
		}
		if record.Result.Environment != nil {
			decision.PoolSize = record.Result.Environment.PoolSize
		}
		if record.Result.PostgresEnvironment != nil {
			decision.PlanCacheMode = normalizedPlanCacheMode(record.Result.PostgresEnvironment.PlanCacheMode)
			if workMemBytes, err := parsePostgresMemoryBytes(record.Result.PostgresEnvironment.WorkMem); err != nil {
				decision.Reasons = append(decision.Reasons, "invalid PostgreSQL work_mem: "+err.Error())
			} else {
				decision.WorkMemBytes = workMemBytes
			}
		}

		if strings.TrimSpace(record.ID) == "" {
			decision.Reasons = append(decision.Reasons, "record id is missing")
		} else if _, duplicate := seenIDs[record.ID]; duplicate {
			decision.Reasons = append(decision.Reasons, "record id is duplicated")
		} else {
			seenIDs[record.ID] = struct{}{}
		}
		decision.Reasons = append(decision.Reasons, validateOperationalRecordBinding(identity, requirements, record)...)
		workload, workloadErr := operationalWorkloadBinding(record.Result)
		if workloadErr != nil {
			decision.Reasons = append(decision.Reasons, "operational workload binding: "+workloadErr.Error())
		} else if operationalWorkload == "" {
			operationalWorkload = workload
		} else if workload != operationalWorkload {
			decision.Reasons = append(decision.Reasons, "operational scenarios do not use one exact authorized workload")
		}
		translationTarget, translationErr := operationalTranslationTargetBinding(record.Result)
		if translationErr != nil {
			decision.Reasons = append(decision.Reasons, "operational translation binding: "+translationErr.Error())
		} else if operationalTranslationTarget == "" {
			operationalTranslationTarget = translationTarget
		} else if translationTarget != operationalTranslationTarget {
			decision.Reasons = append(decision.Reasons, "operational scenarios do not use one exact authorized translation target")
		}
		if record.Scenario != OperationalScenarioForcedOverflow {
			if operationalCandidateSQL == "" {
				operationalCandidateSQL = record.Result.SQLFingerprint
			} else if record.Result.SQLFingerprint != operationalCandidateSQL {
				decision.Reasons = append(decision.Reasons, "non-overflow operational scenarios do not use one exact candidate SQL")
			}
		}
		if record.Result.PostgresEnvironment != nil {
			if databaseIdentity == nil {
				copy := *record.Result.PostgresEnvironment
				databaseIdentity = &copy
			} else if !sameOperationalDatabase(databaseIdentity, record.Result.PostgresEnvironment) {
				decision.Reasons = append(decision.Reasons, "PostgreSQL database identity differs across operational records")
			}
		}

		switch record.Scenario {
		case OperationalScenarioCandidateMatrix:
			decision.Reasons = append(decision.Reasons, validateOperationalMatrixCell(record, requirements)...)
			decision.Reasons = append(decision.Reasons, validateOperationalConcurrencyBlock(record)...)
			poolSize := 0
			if record.Result.Environment != nil {
				poolSize = record.Result.Environment.PoolSize
			}
			decision.Reasons = append(decision.Reasons, validateOperationalCandidateResult(record.Result, identity, requirements, poolSize == 1)...)
			cell := OperationalMatrixCell{PoolSize: decision.PoolSize, Concurrency: decision.Concurrency, PlanCacheMode: decision.PlanCacheMode}
			seenMatrix[cell]++
			if seenMatrix[cell] > 1 {
				decision.Reasons = append(decision.Reasons, "candidate matrix cell is duplicated")
			}
		case OperationalScenarioLowWorkMem:
			seenScenarios[record.Scenario]++
			if seenScenarios[record.Scenario] > 1 {
				decision.Reasons = append(decision.Reasons, "operational scenario is duplicated")
			}
			decision.Reasons = append(decision.Reasons, validateOperationalCandidateResult(record.Result, identity, requirements, true)...)
			if decision.WorkMemBytes <= 0 || decision.WorkMemBytes > requirements.LowWorkMemMaximumBytes {
				decision.Reasons = append(decision.Reasons, fmt.Sprintf("work_mem exceeds constrained ceiling %d bytes", requirements.LowWorkMemMaximumBytes))
			}
		case OperationalScenarioCancellation:
			seenScenarios[record.Scenario]++
			if seenScenarios[record.Scenario] > 1 {
				decision.Reasons = append(decision.Reasons, "operational scenario is duplicated")
			}
			decision.Reasons = append(decision.Reasons, validateOperationalCandidateResult(record.Result, identity, requirements, true)...)
			decision.Reasons = append(decision.Reasons, validateOperationalCancellation(record.Cancellation, record.Result, requirements)...)
		case OperationalScenarioConcurrentWriter:
			seenScenarios[record.Scenario]++
			if seenScenarios[record.Scenario] > 1 {
				decision.Reasons = append(decision.Reasons, "operational scenario is duplicated")
			}
			decision.Reasons = append(decision.Reasons, validateOperationalCandidateResult(record.Result, identity, requirements, true)...)
			decision.Reasons = append(decision.Reasons, validateOperationalSnapshot(record.Snapshot)...)
		case OperationalScenarioSessionIsolation:
			seenScenarios[record.Scenario]++
			if seenScenarios[record.Scenario] > 1 {
				decision.Reasons = append(decision.Reasons, "operational scenario is duplicated")
			}
			decision.Reasons = append(decision.Reasons, validateOperationalCandidateResult(record.Result, identity, requirements, true)...)
			decision.Reasons = append(decision.Reasons, validateOperationalSessionIsolation(record.SessionIsolation, record.Result, requirements)...)
		case OperationalScenarioForcedOverflow:
			seenScenarios[record.Scenario]++
			if seenScenarios[record.Scenario] > 1 {
				decision.Reasons = append(decision.Reasons, "operational scenario is duplicated")
			}
			decision.Reasons = append(decision.Reasons, validateOperationalFallbackResult(record.Result, identity, requirements)...)
		default:
			decision.Reasons = append(decision.Reasons, fmt.Sprintf("unsupported operational scenario %q", record.Scenario))
		}

		decision.Passed = len(decision.Reasons) == 0
		if !decision.Passed {
			report.Passed = false
		} else {
			validScenarios[record.Scenario] = true
			if record.Scenario == OperationalScenarioCandidateMatrix {
				validMatrix[OperationalMatrixCell{
					PoolSize:      decision.PoolSize,
					Concurrency:   decision.Concurrency,
					PlanCacheMode: decision.PlanCacheMode,
				}] = struct{}{}
			}
		}
		report.Records = append(report.Records, decision)
	}
	if len(records) != len(defaultOperationalPoolSizes)*len(defaultOperationalConcurrency)*len(defaultOperationalPlanCacheModes)+5 {
		report.Reasons = append(report.Reasons, "operational evidence must contain exactly 32 records")
	}

	for _, poolSize := range requirements.PoolSizes {
		for _, concurrency := range requirements.ConcurrencyLevels {
			for _, mode := range requirements.PlanCacheModes {
				cell := OperationalMatrixCell{PoolSize: poolSize, Concurrency: concurrency, PlanCacheMode: normalizedPlanCacheMode(mode)}
				report.Coverage.RequiredMatrixCells++
				if _, found := validMatrix[cell]; found {
					report.Coverage.ObservedMatrixCells++
				} else {
					report.Coverage.MissingMatrixCells = append(report.Coverage.MissingMatrixCells, cell)
					report.Reasons = append(report.Reasons, fmt.Sprintf("candidate matrix is missing pool_size=%d concurrency=%d plan_cache_mode=%s", poolSize, concurrency, cell.PlanCacheMode))
				}
			}
		}
	}
	report.Coverage.LowWorkMem = validScenarios[OperationalScenarioLowWorkMem]
	report.Coverage.CancellationReplay = validScenarios[OperationalScenarioCancellation]
	report.Coverage.RepeatableReadWriter = validScenarios[OperationalScenarioConcurrentWriter]
	report.Coverage.SessionIsolation = validScenarios[OperationalScenarioSessionIsolation]
	report.Coverage.ForcedOverflowFallback = validScenarios[OperationalScenarioForcedOverflow]
	for _, required := range []struct {
		scenario OperationalEvidenceScenario
		present  bool
	}{
		{scenario: OperationalScenarioLowWorkMem, present: report.Coverage.LowWorkMem},
		{scenario: OperationalScenarioCancellation, present: report.Coverage.CancellationReplay},
		{scenario: OperationalScenarioConcurrentWriter, present: report.Coverage.RepeatableReadWriter},
		{scenario: OperationalScenarioSessionIsolation, present: report.Coverage.SessionIsolation},
		{scenario: OperationalScenarioForcedOverflow, present: report.Coverage.ForcedOverflowFallback},
	} {
		if !required.present {
			report.Reasons = append(report.Reasons, fmt.Sprintf("required operational scenario %s is missing valid evidence", required.scenario))
		}
	}
	if len(report.Reasons) > 0 {
		report.Passed = false
	}
	return report
}

// canonicalOperationalGateInput creates an immutable, JSON-round-tripped copy
// of the exact evidence evaluated by the report and returns the digest used by
// final promotion verification. Evaluating the copy also ensures producer and
// verifier observe the same JSON number and timestamp representations.
func canonicalOperationalGateInput(identity PromotionEvidenceIdentity, requirements OperationalGateRequirements, records []OperationalEvidenceRecord) (OperationalGateInput, string, error) {
	source := OperationalGateInput{
		Version:           operationalGateVersion,
		PromotionIdentity: cloneOperationalPromotionIdentity(identity),
		Requirements:      cloneOperationalRequirements(requirements),
		Records:           records,
	}
	raw, err := json.Marshal(source)
	if err != nil {
		return source, "", fmt.Errorf("encode: %w", err)
	}
	input, err := decodeOperationalGateInput(bytes.NewReader(raw))
	if err != nil {
		return source, "", err
	}
	digest, err := operationalGateInputSHA256(input)
	if err != nil {
		return input, "", err
	}
	return input, digest, nil
}

// operationalGateInputSHA256 hashes the canonical JSON representation of the
// complete embedded operational evidence document.
func operationalGateInputSHA256(input OperationalGateInput) (string, error) {
	raw, err := json.Marshal(input)
	if err != nil {
		return "", fmt.Errorf("encode canonical operational input: %w", err)
	}
	digest := sha256.Sum256(raw)
	return fmt.Sprintf("%x", digest), nil
}

// validateRecomputedOperationalGateReport is the final-promotion trust
// boundary. It verifies the embedded source evidence, independently rebuilds
// the report, and compares every substantive decision with the serialized
// summary. A checksum-consistent but fabricated passing summary therefore
// cannot authorize promotion.
func validateRecomputedOperationalGateReport(report OperationalGateReport, expectedIdentity PromotionEvidenceIdentity) error {
	if report.Version != operationalGateVersion {
		return fmt.Errorf("operational report version must be %d", operationalGateVersion)
	}
	if !reflect.DeepEqual(report.PromotionIdentity, expectedIdentity) {
		return fmt.Errorf("operational report promotion identity does not match manifest")
	}
	if report.Input.Version != operationalGateVersion {
		return fmt.Errorf("embedded operational input version must be %d", operationalGateVersion)
	}
	if !reflect.DeepEqual(report.Input.PromotionIdentity, expectedIdentity) ||
		!reflect.DeepEqual(report.Input.PromotionIdentity, report.PromotionIdentity) {
		return fmt.Errorf("embedded operational input promotion identity does not match report and manifest")
	}
	if !reflect.DeepEqual(report.Input.Requirements, report.Requirements) {
		return fmt.Errorf("embedded operational input requirements do not match report")
	}
	if !lowercaseSHA256(report.InputSHA256) {
		return fmt.Errorf("operational report input_sha256 is not a canonical SHA-256 digest")
	}
	inputSHA256, err := operationalGateInputSHA256(report.Input)
	if err != nil {
		return err
	}
	if inputSHA256 != report.InputSHA256 {
		return fmt.Errorf("operational report embedded input SHA-256 does not match")
	}

	recomputed := buildOperationalGateReport(report.Input.PromotionIdentity, report.Input.Requirements, report.Input.Records)
	if recomputed.InputSHA256 != report.InputSHA256 || !reflect.DeepEqual(recomputed.Input, report.Input) {
		return fmt.Errorf("operational report embedded input is not canonical")
	}
	if !reflect.DeepEqual(report.Coverage, recomputed.Coverage) {
		return fmt.Errorf("operational report coverage differs from recomputed input")
	}
	if !reflect.DeepEqual(report.Records, recomputed.Records) {
		return fmt.Errorf("operational report record decisions differ from recomputed input")
	}
	if report.Passed != recomputed.Passed || !reflect.DeepEqual(report.Reasons, recomputed.Reasons) {
		return fmt.Errorf("operational report passing disposition differs from recomputed input")
	}
	if !recomputed.Passed {
		return fmt.Errorf("recomputed operational input did not pass: %s", strings.Join(recomputed.Reasons, "; "))
	}
	return nil
}

// loadOperationalGateInput strictly decodes one operational evidence
// document. Unknown fields and concatenated JSON are rejected so misspelled
// proof fields cannot be silently treated as absent evidence.
func loadOperationalGateInput(path string) (OperationalGateInput, error) {
	var input OperationalGateInput
	if strings.TrimSpace(path) == "" {
		return input, fmt.Errorf("operational gate requires an explicit input path")
	}
	file, err := os.Open(path)
	if err != nil {
		return input, fmt.Errorf("read operational gate input: %w", err)
	}
	defer file.Close()

	return decodeOperationalGateInput(file)
}

func decodeOperationalGateInput(reader io.Reader) (OperationalGateInput, error) {
	var input OperationalGateInput
	raw, err := io.ReadAll(reader)
	if err != nil {
		return OperationalGateInput{}, fmt.Errorf("read operational gate input: %w", err)
	}
	if err := rejectDuplicateJSONObjectKeys(raw); err != nil {
		return OperationalGateInput{}, fmt.Errorf("decode operational gate input: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return OperationalGateInput{}, fmt.Errorf("decode operational gate input: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return OperationalGateInput{}, fmt.Errorf("operational gate input contains trailing JSON data")
		}
		return OperationalGateInput{}, fmt.Errorf("decode trailing operational gate input: %w", err)
	}
	if input.Version != operationalGateVersion {
		return OperationalGateInput{}, fmt.Errorf("operational gate input version must be %d, got %d", operationalGateVersion, input.Version)
	}
	return input, nil
}

// createOperationalGateReport loads a strict evidence document, evaluates it,
// and writes the passing or failing machine-verifiable report.
func createOperationalGateReport(inputPath, outputPath string) (bool, error) {
	input, err := loadOperationalGateInput(inputPath)
	if err != nil {
		return false, err
	}
	if err := validateOperationalGatePaths(inputPath, outputPath); err != nil {
		return false, err
	}
	report := buildOperationalGateReport(input.PromotionIdentity, input.Requirements, input.Records)
	if err := writeOperationalGateReport(outputPath, report); err != nil {
		return false, err
	}
	return report.Passed, nil
}

// validateOperationalGatePaths prevents report creation from replacing or
// aliasing the immutable source evidence document.
func validateOperationalGatePaths(inputPath, outputPath string) error {
	if strings.TrimSpace(outputPath) == "" {
		return nil
	}
	inputAbsolute, err := filepath.Abs(filepath.Clean(inputPath))
	if err != nil {
		return fmt.Errorf("resolve operational gate input: %w", err)
	}
	if evaluated, err := filepath.EvalSymlinks(inputAbsolute); err == nil {
		inputAbsolute = evaluated
	}
	outputAbsolute, err := filepath.Abs(filepath.Clean(outputPath))
	if err != nil {
		return fmt.Errorf("resolve operational gate output: %w", err)
	}
	if evaluated, err := filepath.EvalSymlinks(outputAbsolute); err == nil {
		outputAbsolute = evaluated
	} else if evaluatedParent, parentErr := filepath.EvalSymlinks(filepath.Dir(outputAbsolute)); parentErr == nil {
		outputAbsolute = filepath.Join(evaluatedParent, filepath.Base(outputAbsolute))
	}
	if inputAbsolute == outputAbsolute {
		return fmt.Errorf("operational gate input and output must use distinct paths")
	}
	inputInfo, inputErr := os.Stat(inputPath)
	outputInfo, outputErr := os.Stat(outputPath)
	if inputErr == nil && outputErr == nil && os.SameFile(inputInfo, outputInfo) {
		return fmt.Errorf("operational gate input and output must not alias the same file")
	}
	if outputErr != nil && !os.IsNotExist(outputErr) {
		return fmt.Errorf("inspect operational gate output: %w", outputErr)
	}
	return nil
}

// writeOperationalGateReport writes a manifest-consumable operational report.
func writeOperationalGateReport(path string, report OperationalGateReport) (err error) {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("operational gate requires an explicit report output path")
	}
	if err := ensureOutputDir(path); err != nil {
		return err
	}
	output, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := output.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

// validateOperationalIdentity rejects incomplete promotion bindings before any
// operational observation can be considered.
func validateOperationalIdentity(identity PromotionEvidenceIdentity) []string {
	var reasons []string
	for name, value := range map[string]string{
		"candidate": identity.Candidate, "selector_version": identity.SelectorVersion,
		"execution_boundary": identity.ExecutionBoundary, "source_commit": identity.SourceCommit,
		"fallback_executor": identity.FallbackExecutor,
	} {
		if strings.TrimSpace(value) == "" {
			reasons = append(reasons, "promotion identity "+name+" is missing")
		}
	}
	for name, value := range map[string]string{
		"source_sha256": identity.SourceSHA256, "binary_sha256": identity.BinarySHA256, "corpus_sha256": identity.CorpusSHA256,
	} {
		if !lowercaseSHA256(value) {
			reasons = append(reasons, "promotion identity "+name+" is not a canonical SHA-256 digest")
		}
	}
	if len(identity.Caps) == 0 {
		reasons = append(reasons, "promotion identity caps are missing")
	}
	if len(identity.Buckets) == 0 {
		reasons = append(reasons, "promotion identity buckets are missing")
	}
	sort.Strings(reasons)
	return reasons
}

// validateOperationalRequirements validates the report's frozen matrix declaration.
func validateOperationalRequirements(identity PromotionEvidenceIdentity, requirements OperationalGateRequirements) []string {
	var reasons []string
	expectedCandidate, supported := operationalCandidateRuntimeIdentity(identity.Candidate)
	if !supported {
		reasons = append(reasons, "promotion candidate has no registered operational runtime mapping")
	} else if requirements.CandidateRuntimeIdentity != expectedCandidate {
		reasons = append(reasons, "candidate runtime identity differs from the registered promotion candidate mapping")
	} else if strings.TrimSpace(requirements.CandidateRuntimeIdentity) == "" {
		reasons = append(reasons, "candidate runtime identity is missing")
	}
	if strings.TrimSpace(requirements.FallbackRuntimeIdentity) == "" {
		reasons = append(reasons, "fallback runtime identity is missing")
	} else if requirements.FallbackRuntimeIdentity != identity.FallbackExecutor {
		reasons = append(reasons, "fallback runtime identity differs from promotion fallback executor")
	}
	if requirements.CancellationMaximum <= 0 || requirements.CancellationMaximum > 250*time.Millisecond {
		reasons = append(reasons, "cancellation maximum must be positive and no greater than 250ms")
	}
	if requirements.LowWorkMemMaximumBytes <= 0 || requirements.LowWorkMemMaximumBytes > 64*1024 {
		reasons = append(reasons, "low work_mem ceiling must be positive and no greater than 64kB")
	}
	if !requirements.RequireCleanSource {
		reasons = append(reasons, "operational evidence must require a clean source tree")
	}
	if !lowercaseSHA256(identity.OperationalCandidateSQLSHA256) {
		reasons = append(reasons, "promotion identity operational candidate SQL SHA-256 must be a canonical digest")
	}
	if !lowercaseSHA256(requirements.CandidateSQLFingerprint) {
		reasons = append(reasons, "operational candidate SQL fingerprint must be a canonical SHA-256 digest")
	} else if requirements.CandidateSQLFingerprint != identity.OperationalCandidateSQLSHA256 {
		reasons = append(reasons, "operational candidate SQL fingerprint differs from the promotion identity anchor")
	}
	if !slices.Equal(requirements.PoolSizes, defaultOperationalPoolSizes) {
		reasons = append(reasons, "operational pool-size matrix must be exactly 1,2,8")
	}
	if !slices.Equal(requirements.ConcurrencyLevels, defaultOperationalConcurrency) {
		reasons = append(reasons, "operational concurrency matrix must be exactly 1,8,16")
	}
	if len(requirements.PlanCacheModes) != len(defaultOperationalPlanCacheModes) {
		reasons = append(reasons, "operational plan-cache matrix is incomplete")
	} else if !slices.Equal(requirements.PlanCacheModes, defaultOperationalPlanCacheModes) {
		reasons = append(reasons, "operational plan-cache matrix must be exactly auto,force_custom_plan,force_generic_plan")
	}
	sort.Strings(reasons)
	return reasons
}

// validateOperationalRecordBinding enforces source, binary, corpus, and exact
// promotion identity on every independently captured record.
func validateOperationalRecordBinding(identity PromotionEvidenceIdentity, requirements OperationalGateRequirements, record OperationalEvidenceRecord) []string {
	var reasons []string
	if !reflect.DeepEqual(record.PromotionIdentity, identity) {
		reasons = append(reasons, "record promotion identity does not match report")
	}
	if record.SourceSHA256 != identity.SourceSHA256 {
		reasons = append(reasons, "record source archive does not match promotion identity")
	}
	result := record.Result
	if result.ExecutionMode != ModePostgresSQL {
		reasons = append(reasons, "operational record is not PostgreSQL SQL execution")
	}
	if result.Status != StatusOK {
		reasons = append(reasons, "operational record status is not ok")
	}
	if strings.TrimSpace(result.Source) == "" || strings.TrimSpace(result.Dataset) == "" || strings.TrimSpace(result.Name) == "" ||
		strings.TrimSpace(result.Category) == "" || !lowercaseSHA256(result.WorkloadSHA256) {
		reasons = append(reasons, "operational record lacks a bound workload identity")
	}
	reasons = append(reasons, validateOperationalAuthorizedWorkload(identity, requirements, record)...)
	if !result.StableObservation {
		reasons = append(reasons, "operational record lacks a stable observation")
	}
	if result.Environment == nil {
		reasons = append(reasons, "run environment is missing")
	} else {
		if result.Environment.ArtifactSchemaVersion != 2 {
			reasons = append(reasons, "operational evidence requires artifact schema v2")
		}
		if result.Environment.SourceCommit != identity.SourceCommit {
			reasons = append(reasons, "run source commit does not match promotion identity")
		}
		if result.Environment.BinarySHA256 != identity.BinarySHA256 {
			reasons = append(reasons, "run binary does not match promotion identity")
		}
		if result.Environment.CorpusSHA256 != identity.CorpusSHA256 {
			reasons = append(reasons, "run corpus does not match promotion identity")
		}
		if requirements.RequireCleanSource && result.Environment.DirtyDiffSHA256 != cleanWorkingTreeSHA256() {
			reasons = append(reasons, "operational evidence was captured from a dirty source tree")
		}
	}
	if result.Fixture == nil || result.Fixture.Dataset != result.Dataset || !lowercaseSHA256(result.Fixture.Checksum) ||
		strings.TrimSpace(result.Fixture.Configuration) == "" || !result.Fixture.PhysicalValidated ||
		result.Fixture.NodeCount <= 0 || result.Fixture.EdgeCount <= 0 ||
		result.Fixture.PhysicalNodeCount != int64(result.Fixture.NodeCount) ||
		result.Fixture.PhysicalEdgeCount != int64(result.Fixture.EdgeCount) {
		reasons = append(reasons, "operational record lacks one physically validated fixture identity")
	}
	if result.PostgresEnvironment == nil {
		reasons = append(reasons, "PostgreSQL environment is missing")
	} else if !strings.EqualFold(strings.TrimSpace(result.PostgresEnvironment.TransactionIsolation), "repeatable read") {
		reasons = append(reasons, "operational evidence requires Repeatable Read")
	}
	return reasons
}

// operationalCandidateRuntimeIdentity freezes the exact executor arm that an
// operational matrix must exercise for every promotable policy. A policy may
// emit a different identity from the executor it admits, but callers cannot
// choose that mapping in their evidence document.
func operationalCandidateRuntimeIdentity(candidate string) (string, bool) {
	switch candidate {
	case string(optimize.ShortestPathExecutorASPI1DAG),
		string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness),
		string(optimize.ShortestPathExecutorI2GuardedDistance):
		return candidate, true
	case string(optimize.ExpansionSearchPolicyOrientationProbeV1),
		string(optimize.ExpansionSearchPolicyOrientationProbeV2):
		return string(optimize.ExpansionSearchSuffixSeededReverse), true
	default:
		return "", false
	}
}

// validateOperationalAuthorizedWorkload binds every operational scenario to
// one exact manifest query bucket and to the SQL and workload shape actually
// measured by GraphBench.
func validateOperationalAuthorizedWorkload(identity PromotionEvidenceIdentity, requirements OperationalGateRequirements, record OperationalEvidenceRecord) []string {
	var reasons []string
	result := record.Result
	cypherQuery := strings.TrimSpace(result.Cypher)
	if cypherQuery == "" {
		return []string{"operational record has no Cypher query to authorize"}
	}
	querySHA256 := pgdriver.TraversalPolicyQuerySHA256(cypherQuery)
	var matches []PromotionBucket
	for _, bucket := range identity.Buckets {
		if slices.Contains(bucket.QuerySHA256, querySHA256) {
			matches = append(matches, bucket)
		}
	}
	if len(matches) != 1 {
		return []string{fmt.Sprintf("operational query must match exactly one promotion bucket, matched %d", len(matches))}
	}
	bucket := matches[0]
	shape := result.Shape
	if !isOrientationProbePolicy(identity.Candidate) &&
		(shape.MinDepth == nil || shape.MaxDepth == nil || *shape.MinDepth != bucket.MinimumDepth || *shape.MaxDepth != bucket.MaximumDepth ||
			shape.Direction != bucket.Direction || shape.RelationshipKindCount != bucket.RelationshipKindCount ||
			len(shape.EdgeKinds) != shape.RelationshipKindCount || (len(shape.EdgeKinds) == 0) != bucket.UntypedRelationship) {
		reasons = append(reasons, "operational workload shape differs from its authorized promotion bucket")
	}
	if !slices.Contains(bucket.QualificationSplit, shape.QualificationSplit) {
		reasons = append(reasons, "operational workload split is not authorized by its promotion bucket")
	}
	if result.TraversalTelemetry == nil || result.TraversalTelemetry.Summary.ObservationMode != bucket.ObservationMode {
		reasons = append(reasons, "operational observation mode differs from its authorized promotion bucket")
	}
	if strings.TrimSpace(result.SQL) == "" || !lowercaseSHA256(result.SQLFingerprint) || result.SQLFingerprint != sqlFingerprint(result.SQL) {
		reasons = append(reasons, "operational SQL fingerprint is missing or does not bind the measured SQL")
	}
	if record.Scenario != OperationalScenarioForcedOverflow && result.SQLFingerprint != identity.OperationalCandidateSQLSHA256 {
		reasons = append(reasons, "operational SQL fingerprint differs from the independently frozen production candidate SQL")
	}
	reasons = append(reasons, validateOperationalTranslationBinding(identity, bucket, record)...)
	return reasons
}

// validateOperationalTranslationBinding ties rendered SQL to the exact
// production-canary target carried by GraphBench's EXPLAIN translation. This
// prevents a self-consistent fingerprint over unrelated SQL from satisfying
// an authorized Cypher query.
func validateOperationalTranslationBinding(identity PromotionEvidenceIdentity, bucket PromotionBucket, record OperationalEvidenceRecord) []string {
	result := record.Result
	if result.Optimization == nil {
		return []string{"operational record lacks optimization target evidence"}
	}
	outcome, ok := singleTraversalOutcome(result.Optimization.TargetOutcomes)
	if !ok {
		return []string{"operational record must contain one exact traversal optimization target"}
	}
	if outcome.TargetKind != "traversal" {
		return []string{"operational optimization target is not a traversal"}
	}
	if isOrientationProbePolicy(identity.Candidate) {
		return validateOperationalOrientationTarget(identity, bucket, record, outcome)
	}
	return validateOperationalShortestTarget(identity, bucket, record, outcome)
}

func validateOperationalShortestTarget(identity PromotionEvidenceIdentity, bucket PromotionBucket, record OperationalEvidenceRecord, outcome translate.TargetLoweringOutcome) []string {
	if outcome.Family != "SP" && outcome.Family != "ASP" {
		return []string{"operational optimization target is not an authorized SP/ASP traversal"}
	}
	if outcome.MinimumDepth == nil || outcome.MaximumDepth == nil || *outcome.MinimumDepth != int64(bucket.MinimumDepth) || *outcome.MaximumDepth != int64(bucket.MaximumDepth) ||
		outcome.Direction != bucket.Direction || outcome.ObservationMode != bucket.ObservationMode ||
		outcome.RelationshipKindCount != bucket.RelationshipKindCount || outcome.UntypedRelationship != bucket.UntypedRelationship {
		return []string{"operational optimization target differs from its authorized promotion bucket"}
	}
	if outcome.Candidate != identity.Candidate || outcome.Selected != identity.Candidate || outcome.Applied != identity.Candidate ||
		outcome.Fallback != identity.FallbackExecutor || outcome.SelectorVersion != identity.SelectorVersion ||
		outcome.ExecutionBoundary != identity.ExecutionBoundary || outcome.SelectionMode != "production_canary" ||
		outcome.EmittedPolicy != operationalCandidatePolicy(identity.Candidate) ||
		len(outcome.EmittedCandidates) != 2 || !slices.Contains(outcome.EmittedCandidates, identity.Candidate) ||
		!slices.Contains(outcome.EmittedCandidates, identity.FallbackExecutor) ||
		!slices.Contains(outcome.PlannedCandidates, identity.Candidate) || !slices.Contains(outcome.PlannedCandidates, identity.FallbackExecutor) ||
		outcome.Eligible == nil || !*outcome.Eligible || outcome.StaticallyEligible == nil || !*outcome.StaticallyEligible {
		return []string{"operational optimization target does not prove the exact production candidate policy"}
	}
	if reasons := validateOperationalTargetCaps(identity, outcome, record.Scenario == OperationalScenarioForcedOverflow); len(reasons) != 0 {
		return reasons
	}
	return nil
}

func validateOperationalOrientationTarget(identity PromotionEvidenceIdentity, bucket PromotionBucket, record OperationalEvidenceRecord, outcome translate.TargetLoweringOutcome) []string {
	forward := string(optimize.ExpansionSearchStepwiseForward)
	reverse := string(optimize.ExpansionSearchSuffixSeededReverse)
	if outcome.Family != "fixed_suffix_expansion" || outcome.MinimumDepth == nil || outcome.MaximumDepth == nil ||
		*outcome.MinimumDepth != int64(bucket.MinimumDepth) || *outcome.MaximumDepth != int64(bucket.MaximumDepth) ||
		outcome.ObservationMode != bucket.ObservationMode || bucket.Direction != "outbound" ||
		bucket.RelationshipKindCount != 1 || bucket.UntypedRelationship || !operationalEligibilityFact(outcome, "qualified_fixed_suffix_topology") {
		return []string{"operational orientation target differs from its authorized promotion bucket"}
	}
	if outcome.Candidate != reverse || outcome.Selected != forward || outcome.Applied != forward || outcome.Fallback != forward ||
		outcome.EmittedPolicy != identity.Candidate || outcome.SelectorVersion != identity.SelectorVersion ||
		outcome.ExecutionBoundary != identity.ExecutionBoundary || outcome.SelectionMode != "production_canary" ||
		len(outcome.EmittedCandidates) != 2 || !slices.Contains(outcome.EmittedCandidates, reverse) || !slices.Contains(outcome.EmittedCandidates, forward) ||
		!slices.Contains(outcome.PlannedCandidates, reverse) || !slices.Contains(outcome.PlannedCandidates, forward) ||
		outcome.Eligible == nil || !*outcome.Eligible || outcome.StaticallyEligible == nil || !*outcome.StaticallyEligible {
		return []string{"operational orientation target does not prove the exact production policy"}
	}
	forcedOverflow := record.Scenario == OperationalScenarioForcedOverflow
	if outcome.ProbeCaps == nil || outcome.Admission == nil || !outcome.Admission.RequiresCompleteProbes || string(outcome.Admission.FallbackStrategy) != forward {
		return []string{"operational orientation target lacks its complete bounded admission"}
	}
	actualCaps := map[string]int64{
		"root_row_limit":               outcome.ProbeCaps.RootRowLimit,
		"reverse_seed_row_limit":       outcome.ProbeCaps.ReverseSeedRowLimit,
		"directional_degree_row_limit": outcome.ProbeCaps.DirectionalDegreeRowLimit,
		"state_limit":                  outcome.Admission.StateLimit,
	}
	for name, expected := range identity.Caps {
		actual, found := actualCaps[name]
		if !found || (!forcedOverflow && actual != expected) || (forcedOverflow && (actual <= 0 || actual > expected)) {
			return []string{"operational orientation target cap differs from promotion identity: " + name}
		}
	}
	if len(actualCaps) != len(identity.Caps) || outcome.StateLimit != outcome.Admission.StateLimit {
		return []string{"operational orientation target contains an unauthorized cap contract"}
	}
	return nil
}

func operationalEligibilityFact(outcome translate.TargetLoweringOutcome, name string) bool {
	for _, fact := range outcome.EligibilityFacts {
		if fact.Name == name {
			return fact.Eligible
		}
	}
	return false
}

func operationalCandidatePolicy(candidate string) string {
	switch candidate {
	case string(optimize.ShortestPathExecutorASPI1DAG):
		return optimize.ShortestPathPolicyASPI1GuardedV1
	case string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness):
		return optimize.ShortestPathPolicyI1CanonicalGuardedV1
	case string(optimize.ShortestPathExecutorI2GuardedDistance):
		return optimize.ShortestPathPolicyI2DistanceGuardedV1
	case string(optimize.ExpansionSearchPolicyOrientationProbeV1), string(optimize.ExpansionSearchPolicyOrientationProbeV2):
		return candidate
	default:
		return ""
	}
}

// validateOperationalTargetCaps accepts the manifest caps verbatim for every
// normal scenario. Forced overflow may lower positive caps to make overflow
// deterministic, but it may not change the target, policy, or add dimensions.
func validateOperationalTargetCaps(identity PromotionEvidenceIdentity, outcome translate.TargetLoweringOutcome, forcedOverflow bool) []string {
	actual := map[string]int64{
		"state_limit": outcome.StateLimit, "frontier_limit": outcome.FrontierLimit,
		"predecessor_limit": outcome.PredecessorLimit, "enumeration_limit": outcome.EnumerationLimit,
		"output_bytes_limit": outcome.OutputBytesLimit,
	}
	for name, value := range actual {
		expected, required := identity.Caps[name]
		if !required {
			if value != 0 {
				return []string{"operational optimization target contains an unauthorized cap " + name}
			}
			continue
		}
		if forcedOverflow {
			if value <= 0 || value > expected {
				return []string{"forced-overflow optimization cap is not a positive bounded variant of " + name}
			}
		} else if value != expected {
			return []string{"operational optimization target cap differs from promotion identity: " + name}
		}
	}
	return nil
}

// operationalTranslationTargetBinding excludes only the cap values that the
// forced-overflow scenario is explicitly allowed to reduce.
func operationalTranslationTargetBinding(result CaseResult) (string, error) {
	if result.Optimization == nil {
		return "", fmt.Errorf("optimization target evidence is missing")
	}
	outcome, ok := singleTraversalOutcome(result.Optimization.TargetOutcomes)
	if !ok {
		return "", fmt.Errorf("one exact traversal optimization target is required")
	}
	outcome.StateLimit = 0
	outcome.FrontierLimit = 0
	outcome.PredecessorLimit = 0
	outcome.EnumerationLimit = 0
	outcome.OutputBytesLimit = 0
	if outcome.ProbeCaps != nil {
		probeCaps := *outcome.ProbeCaps
		probeCaps.RootRowLimit = 0
		probeCaps.ReverseSeedRowLimit = 0
		probeCaps.DirectionalDegreeRowLimit = 0
		probeCaps.SurvivalRowLimit = 0
		outcome.ProbeCaps = &probeCaps
	}
	if outcome.Admission != nil {
		admission := *outcome.Admission
		admission.StateLimit = 0
		outcome.Admission = &admission
	}
	raw, err := json.Marshal(outcome)
	if err != nil {
		return "", fmt.Errorf("encode optimization target: %w", err)
	}
	return sqlFingerprint(string(raw)), nil
}

// operationalWorkloadBinding hashes every logical and resolved workload input
// that must remain identical across scenarios. Rendered SQL is deliberately
// excluded because forced-overflow evidence may change only guarded cap
// literals; validateOperationalTranslationBinding independently proves that
// both SQL variants describe the same authorized translation target.
func operationalWorkloadBinding(result CaseResult) (string, error) {
	var fixture any
	if result.Fixture != nil {
		fixture = struct {
			Dataset                 string                                      `json:"dataset"`
			Checksum                string                                      `json:"checksum"`
			NodeCount               int                                         `json:"node_count"`
			EdgeCount               int                                         `json:"edge_count"`
			PhysicalNodeCount       int64                                       `json:"physical_node_count"`
			PhysicalEdgeCount       int64                                       `json:"physical_edge_count"`
			Configuration           string                                      `json:"configuration"`
			Shortest                *ShortestFixtureExpectations                `json:"shortest"`
			FixedSuffixExpansion    *FixedSuffixExpansionFixtureExpectations    `json:"fixed_suffix_expansion"`
			EndpointSeededExpansion *EndpointSeededExpansionFixtureExpectations `json:"endpoint_seeded_expansion"`
		}{
			Dataset: result.Fixture.Dataset, Checksum: result.Fixture.Checksum,
			NodeCount: result.Fixture.NodeCount, EdgeCount: result.Fixture.EdgeCount,
			PhysicalNodeCount: result.Fixture.PhysicalNodeCount, PhysicalEdgeCount: result.Fixture.PhysicalEdgeCount,
			Configuration: result.Fixture.Configuration, Shortest: result.Fixture.Shortest,
			FixedSuffixExpansion:    result.Fixture.FixedSuffixExpansion,
			EndpointSeededExpansion: result.Fixture.EndpointSeededExpansion,
		}
	}
	payload := struct {
		Version        int                 `json:"version"`
		Source         string              `json:"source"`
		Dataset        string              `json:"dataset"`
		Name           string              `json:"name"`
		WorkloadSHA256 string              `json:"workload_sha256"`
		QuerySHA256    string              `json:"query_sha256"`
		Params         map[string]any      `json:"params"`
		NodeParams     map[string]string   `json:"node_params"`
		NodeListParams map[string][]string `json:"node_list_params"`
		Fixture        any                 `json:"fixture"`
	}{
		Version: 1, Source: result.Source, Dataset: result.Dataset, Name: result.Name,
		WorkloadSHA256: result.WorkloadSHA256,
		QuerySHA256:    pgdriver.TraversalPolicyQuerySHA256(result.Cypher),
		Params:         result.Params, NodeParams: result.NodeParams, NodeListParams: result.NodeListParams,
		Fixture: fixture,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode canonical workload: %w", err)
	}
	return sqlFingerprint(string(raw)), nil
}

// validateOperationalMatrixCell rejects records outside the frozen Cartesian
// matrix instead of silently treating them as harmless extra evidence.
func validateOperationalMatrixCell(record OperationalEvidenceRecord, requirements OperationalGateRequirements) []string {
	if record.Result.Environment == nil || record.Result.PostgresEnvironment == nil {
		return nil
	}
	poolSize := record.Result.Environment.PoolSize
	mode := normalizedPlanCacheMode(record.Result.PostgresEnvironment.PlanCacheMode)
	if !slices.Contains(requirements.PoolSizes, poolSize) ||
		!slices.Contains(requirements.ConcurrencyLevels, record.Concurrency) ||
		!slices.Contains(requirements.PlanCacheModes, mode) {
		return []string{fmt.Sprintf("candidate matrix record is outside the required matrix: pool_size=%d concurrency=%d plan_cache_mode=%s", poolSize, record.Concurrency, mode)}
	}
	return nil
}

// validateOperationalConcurrencyBlock proves the declared matrix cell was
// actually executed and drained successfully rather than merely labeled.
func validateOperationalConcurrencyBlock(record OperationalEvidenceRecord) []string {
	var reasons []string
	if record.Result.Environment == nil || record.Concurrency <= 0 {
		return []string{"candidate matrix lacks positive pool and concurrency settings"}
	}
	var matches []ConcurrencyBlock
	for _, block := range record.Result.Concurrency {
		if block.PoolSize == record.Result.Environment.PoolSize && block.Concurrency == record.Concurrency {
			matches = append(matches, block)
		}
	}
	if len(matches) != 1 {
		return []string{fmt.Sprintf("candidate matrix requires exactly one matching concurrency block, found %d", len(matches))}
	}
	block := matches[0]
	iterations := record.Result.Stats.Iterations
	expectedOperations := record.Concurrency * iterations
	if iterations <= 0 || block.Operations != expectedOperations || len(block.Samples) != block.Operations {
		reasons = append(reasons, "concurrency block lacks a complete successful operation set")
	}
	if block.Wall <= 0 || block.QPS <= 0 {
		reasons = append(reasons, "concurrency block lacks positive wall time or throughput")
	}
	workers := make(map[int]struct{}, record.Concurrency)
	workerIterations := make(map[[2]int]struct{}, expectedOperations)
	connections := make(map[string]struct{})
	coldConnections := make(map[string]int)
	for _, sample := range block.Samples {
		connectionID := strings.TrimSpace(sample.ConnectionID)
		if connectionID == "" || sample.Total <= 0 || sample.ExecuteDrain <= 0 || sample.Total < sample.ExecuteDrain {
			reasons = append(reasons, "concurrency sample lacks connection and execution evidence")
			break
		}
		if pid, err := strconv.ParseUint(connectionID, 10, 32); err != nil || pid == 0 {
			reasons = append(reasons, "concurrency sample connection is not a PostgreSQL backend PID")
			break
		}
		if sample.Worker < 1 || sample.Worker > record.Concurrency {
			reasons = append(reasons, "concurrency sample identifies a worker outside the declared range")
			break
		}
		if sample.Iteration < 1 || sample.Iteration > iterations {
			reasons = append(reasons, "concurrency sample identifies an iteration outside the measured range")
			break
		}
		key := [2]int{sample.Worker, sample.Iteration}
		if _, duplicate := workerIterations[key]; duplicate {
			reasons = append(reasons, "concurrency block duplicates a worker iteration")
			break
		}
		workerIterations[key] = struct{}{}
		if sample.Classification != "cold-session" && sample.Classification != "warm-session" {
			reasons = append(reasons, "concurrency sample has a non-producer session classification")
			break
		}
		if sample.Classification == "cold-session" {
			coldConnections[connectionID]++
		}
		connections[connectionID] = struct{}{}
		workers[sample.Worker] = struct{}{}
	}
	if len(workers) != record.Concurrency {
		reasons = append(reasons, fmt.Sprintf("concurrency block exercised %d of %d declared workers", len(workers), record.Concurrency))
	}
	if len(workerIterations) != expectedOperations {
		reasons = append(reasons, fmt.Sprintf("concurrency block completed %d of %d worker iterations", len(workerIterations), expectedOperations))
	}
	if len(connections) == 0 || len(connections) > record.Result.Environment.PoolSize || len(connections) > record.Concurrency {
		reasons = append(reasons, "concurrency block connection usage exceeds its pool or worker bounds")
	}
	for connectionID := range connections {
		if coldConnections[connectionID] != 1 {
			reasons = append(reasons, "concurrency sample session classification contradicts connection reuse")
			break
		}
	}
	return reasons
}

// validateOperationalCandidateResult validates admitted execution. Single-pool
// and exceptional records require exact per-invocation receipts. Larger-pool
// matrix records instead retain GraphBench's honest replay attribution and are
// proven by the independently validated concurrency block.
func validateOperationalCandidateResult(result CaseResult, identity PromotionEvidenceIdentity, requirements OperationalGateRequirements, requireTimedReceipts bool) []string {
	var reasons []string
	if result.TraversalTelemetry == nil {
		return []string{"candidate traversal telemetry is missing"}
	}
	if err := ValidateTraversalExecutionTelemetry(result.TraversalTelemetry); err != nil {
		reasons = append(reasons, "candidate traversal telemetry: "+err.Error())
	}
	reasons = append(reasons, validateOperationalSPI2Attribution(result, identity)...)
	summary := result.TraversalTelemetry.Summary
	if summary.RuntimeOutcomeAvailable == nil || !*summary.RuntimeOutcomeAvailable {
		reasons = append(reasons, "candidate runtime outcome is unavailable")
	}
	if summary.RequestedIdentity != requirements.CandidateRuntimeIdentity || summary.RuntimeIdentity != requirements.CandidateRuntimeIdentity || summary.AppliedIdentity != requirements.CandidateRuntimeIdentity {
		reasons = append(reasons, "candidate summary does not identify admitted candidate execution")
	}
	if summary.SelectorVersion != identity.SelectorVersion || summary.ExecutionBoundary != identity.ExecutionBoundary {
		reasons = append(reasons, "candidate summary selector or execution boundary differs from promotion identity")
	}
	if identity.Candidate != requirements.CandidateRuntimeIdentity && summary.EmittedIdentity != identity.Candidate {
		reasons = append(reasons, "candidate summary emitted policy differs from promotion identity")
	}
	if summary.FallbackExecuted == nil || *summary.FallbackExecuted {
		reasons = append(reasons, "candidate summary executed or omitted fallback outcome")
	}
	if summary.Overflow == nil || *summary.Overflow {
		reasons = append(reasons, "candidate summary overflow outcome is not false")
	}
	if strings.TrimSpace(summary.RuntimeBranch) == "" || summary.RuntimeBranch == "mixed" || summary.RuntimeBranch == "runtime_outcome_unavailable" {
		reasons = append(reasons, "candidate runtime branch is unavailable or mixed")
	}
	warm := operationalWarmSamples(result)
	if len(warm) == 0 {
		reasons = append(reasons, "candidate record has no warm samples")
	}
	for _, sample := range warm {
		if requireTimedReceipts {
			reasons = append(reasons, validateOperationalCandidateSample(sample, result, requirements)...)
		} else {
			reasons = append(reasons, validateOperationalPooledCandidateSample(sample, result, requirements)...)
		}
	}
	return reasons
}

// validateOperationalPooledCandidateSample accepts only the producer's honest
// pool>1 replay metadata. A submitted timed receipt would falsely imply one
// session-local attestor covered a measurement that may use many sessions.
func validateOperationalPooledCandidateSample(sample LatencySample, result CaseResult, requirements OperationalGateRequirements) []string {
	var reasons []string
	if sample.RequestedIdentity != requirements.CandidateRuntimeIdentity || sample.RuntimeIdentity != requirements.CandidateRuntimeIdentity ||
		sample.FallbackExecuted == nil || *sample.FallbackExecuted {
		reasons = append(reasons, "pooled candidate sample does not match admitted replay outcome")
	}
	if sample.RuntimeAttestation != "same_case_invocation_local_replay" || sample.RuntimeInvocationID != "" ||
		len(sample.RuntimeReceiptEvents) != 0 || sample.ConnectionID != "" {
		reasons = append(reasons, "pooled candidate sample must retain non-attested GraphBench replay metadata")
	}
	if sample.Dataset != result.Dataset || sample.Case != result.Name || sample.Backend != ModePostgresSQL {
		reasons = append(reasons, "pooled candidate sample workload identity differs from its record")
	}
	return reasons
}

// validateOperationalFallbackResult validates exact forced-overflow selection,
// including nested fallback chains whose terminal executor follows the manifest fallback.
func validateOperationalFallbackResult(result CaseResult, identity PromotionEvidenceIdentity, requirements OperationalGateRequirements) []string {
	var reasons []string
	if result.TraversalTelemetry == nil {
		return []string{"overflow traversal telemetry is missing"}
	}
	if err := ValidateTraversalExecutionTelemetry(result.TraversalTelemetry); err != nil {
		reasons = append(reasons, "overflow traversal telemetry: "+err.Error())
	}
	reasons = append(reasons, validateOperationalSPI2Attribution(result, identity)...)
	summary := result.TraversalTelemetry.Summary
	if summary.RuntimeOutcomeAvailable == nil || !*summary.RuntimeOutcomeAvailable {
		reasons = append(reasons, "overflow runtime outcome is unavailable")
	}
	if summary.RequestedIdentity != requirements.CandidateRuntimeIdentity || summary.RuntimeIdentity != requirements.FallbackRuntimeIdentity || summary.AppliedIdentity != requirements.FallbackRuntimeIdentity || summary.FallbackIdentity != requirements.FallbackRuntimeIdentity {
		reasons = append(reasons, "overflow summary does not identify the exact configured fallback")
	}
	if summary.SelectorVersion != identity.SelectorVersion || summary.ExecutionBoundary != identity.ExecutionBoundary {
		reasons = append(reasons, "overflow summary selector or execution boundary differs from promotion identity")
	}
	if identity.Candidate != requirements.CandidateRuntimeIdentity && summary.EmittedIdentity != identity.Candidate {
		reasons = append(reasons, "overflow summary emitted policy differs from promotion identity")
	}
	if summary.FallbackExecuted == nil || !*summary.FallbackExecuted || summary.Overflow == nil || !*summary.Overflow {
		reasons = append(reasons, "overflow summary lacks true overflow and fallback outcomes")
	}
	warm := operationalWarmSamples(result)
	if len(warm) == 0 {
		reasons = append(reasons, "overflow record has no warm timed samples")
	}
	for _, sample := range warm {
		if sample.RequestedIdentity != requirements.CandidateRuntimeIdentity || sample.FallbackExecuted == nil || !*sample.FallbackExecuted || sample.RuntimeAttestation != "timed_invocation" || strings.TrimSpace(sample.RuntimeInvocationID) == "" {
			reasons = append(reasons, "overflow warm sample lacks candidate request and fallback attribution")
			continue
		}
		if err := validateRuntimeReceiptEvents(sample.RuntimeReceiptEvents, sample.RuntimeIdentity, sample.RuntimeBranch, sample.FallbackExecuted); err != nil {
			reasons = append(reasons, "overflow warm sample receipt chain: "+err.Error())
			continue
		}
		if reason := validateOperationalEventInvocation(sample); reason != "" {
			reasons = append(reasons, reason)
		}
		if !receiptChainContainsIdentity(sample.RuntimeReceiptEvents, requirements.FallbackRuntimeIdentity, true) {
			reasons = append(reasons, "overflow receipt chain does not contain the exact configured fallback")
		}
	}
	return reasons
}

// validateOperationalSPI2Attribution requires each SP-I2 operational record
// to carry the same exact diagnostic proof used by resource qualification.
// Summary-only identity claims cannot establish that the inactive statement
// arm stayed uninitialized or that the selected arm produced the public rows.
func validateOperationalSPI2Attribution(result CaseResult, identity PromotionEvidenceIdentity) []string {
	if identity.Candidate != string(optimize.ShortestPathExecutorI2GuardedDistance) {
		return nil
	}

	telemetry := result.TraversalTelemetry
	if telemetry == nil {
		return []string{"SP-I2 operational attribution telemetry is missing"}
	}
	var reasons []string
	if telemetry.Level != TraversalTelemetryLevelDiagnostic || telemetry.Diagnostic == nil {
		reasons = append(reasons, "SP-I2 operational records require an untimed diagnostic replay")
	} else if telemetry.Diagnostic.CounterStatus != TraversalTelemetryCounterStatusComplete {
		reasons = append(reasons, "SP-I2 operational records require complete diagnostic counters")
	}

	contract, _ := guardedInlineResourceContractForArchitecture(string(optimize.ShortestPathExecutorI2GuardedDistance))
	gateCase := &ResourceGateCase{}
	appendGuardedInlineResourceBindingReasons(gateCase, result, contract)
	appendInlineDistanceAttributionReasons(gateCase, telemetry)
	for _, reason := range gateCase.Reasons {
		reasons = append(reasons, "SP-I2 operational attribution: "+reason)
	}
	return reasons
}

// validateOperationalCandidateSample validates one candidate receipt independently of its enclosing scenario.
func validateOperationalCandidateSample(sample LatencySample, result CaseResult, requirements OperationalGateRequirements) []string {
	var reasons []string
	if sample.RequestedIdentity != requirements.CandidateRuntimeIdentity || sample.RuntimeIdentity != requirements.CandidateRuntimeIdentity || sample.FallbackExecuted == nil || *sample.FallbackExecuted {
		reasons = append(reasons, "candidate warm sample does not identify singular admitted execution")
	}
	if sample.RuntimeAttestation != "timed_invocation" || strings.TrimSpace(sample.RuntimeInvocationID) == "" || strings.TrimSpace(sample.ConnectionID) == "" {
		reasons = append(reasons, "candidate warm sample lacks timed invocation and connection attribution")
	}
	if sample.Dataset != result.Dataset || sample.Case != result.Name || sample.Backend != ModePostgresSQL {
		reasons = append(reasons, "candidate warm sample workload identity differs from its record")
	}
	if err := validateRuntimeReceiptEvents(sample.RuntimeReceiptEvents, sample.RuntimeIdentity, sample.RuntimeBranch, sample.FallbackExecuted); err != nil {
		reasons = append(reasons, "candidate warm sample receipt chain: "+err.Error())
	} else if reason := validateOperationalEventInvocation(sample); reason != "" {
		reasons = append(reasons, reason)
	}
	return reasons
}

// validateOperationalCancellation validates timeout, rollback, same-PID reuse,
// and the candidate receipt emitted by the successful replay.
func validateOperationalCancellation(evidence *OperationalCancellationEvidence, result CaseResult, requirements OperationalGateRequirements) []string {
	if evidence == nil {
		return []string{"cancellation evidence is missing"}
	}
	var reasons []string
	if evidence.SQLState != "57014" {
		reasons = append(reasons, "cancellation did not report PostgreSQL SQLSTATE 57014")
	}
	if evidence.Latency <= 0 || evidence.Latency >= requirements.CancellationMaximum {
		reasons = append(reasons, fmt.Sprintf("cancellation latency must be positive and below %s", requirements.CancellationMaximum))
	}
	if !evidence.TransactionRolledBack {
		reasons = append(reasons, "cancelled transaction was not rolled back")
	}
	if evidence.CancelledBackendPID == 0 || evidence.CancelledBackendPID != evidence.ReplayBackendPID {
		reasons = append(reasons, "post-rollback replay did not reuse the cancelled backend PID")
	}
	if !evidence.ReplaySucceeded {
		reasons = append(reasons, "post-rollback replay did not succeed")
	}
	if evidence.ReplayCandidateReceipt.ConnectionID != strconv.FormatUint(uint64(evidence.ReplayBackendPID), 10) {
		reasons = append(reasons, "post-rollback replay receipt is not bound to the reused backend PID")
	}
	reasons = append(reasons, validateOperationalCandidateSample(evidence.ReplayCandidateReceipt, result, requirements)...)
	return reasons
}

// validateOperationalSnapshot validates a stable Repeatable Read observation while a distinct writer commits.
func validateOperationalSnapshot(evidence *OperationalSnapshotEvidence) []string {
	if evidence == nil {
		return []string{"concurrent-writer snapshot evidence is missing"}
	}
	var reasons []string
	if !strings.EqualFold(strings.TrimSpace(evidence.ReaderIsolation), "repeatable read") {
		reasons = append(reasons, "concurrent-writer reader did not use Repeatable Read")
	}
	if evidence.ReaderBackendPID == 0 || evidence.WriterBackendPID == 0 || evidence.ReaderBackendPID == evidence.WriterBackendPID {
		reasons = append(reasons, "concurrent writer was not a distinct PostgreSQL backend")
	}
	if !evidence.WriterCommitted {
		reasons = append(reasons, "concurrent writer did not commit")
	}
	if evidence.WriterAffectedRows <= 0 {
		reasons = append(reasons, "concurrent writer did not affect any rows")
	}
	if !lowercaseSHA256(evidence.ObservationBeforeSHA256) || evidence.ObservationBeforeSHA256 != evidence.ObservationAfterSHA256 {
		reasons = append(reasons, "reader observation changed across the concurrent commit")
	}
	if !lowercaseSHA256(evidence.PostCommitObservationSHA256) || evidence.PostCommitObservationSHA256 == evidence.ObservationBeforeSHA256 {
		reasons = append(reasons, "post-transaction observation does not prove the concurrent writer changed visible state")
	}
	return reasons
}

// validateOperationalSessionIsolation validates independent invocation IDs,
// distinct sessions, own-row visibility, and zero cross-session visibility.
func validateOperationalSessionIsolation(evidence *OperationalSessionIsolationEvidence, result CaseResult, requirements OperationalGateRequirements) []string {
	if evidence == nil {
		return []string{"session-isolation evidence is missing"}
	}
	var reasons []string
	if evidence.SessionABackendPID == 0 || evidence.SessionBBackendPID == 0 || evidence.SessionABackendPID == evidence.SessionBBackendPID {
		reasons = append(reasons, "session-isolation evidence does not use distinct PostgreSQL backends")
	}
	if strings.TrimSpace(evidence.SessionAInvocationID) == "" || strings.TrimSpace(evidence.SessionBInvocationID) == "" || evidence.SessionAInvocationID == evidence.SessionBInvocationID {
		reasons = append(reasons, "session-isolation evidence lacks distinct invocation IDs")
	}
	if evidence.SessionAOwnRows <= 0 || evidence.SessionBOwnRows <= 0 || evidence.SessionAObservedBRows != 0 || evidence.SessionBObservedARows != 0 {
		reasons = append(reasons, "session-local evidence contains missing own rows or cross-session rows")
	}
	for _, receipt := range []struct {
		name       string
		pid        uint32
		invocation string
		sample     LatencySample
	}{
		{name: "session A", pid: evidence.SessionABackendPID, invocation: evidence.SessionAInvocationID, sample: evidence.SessionACandidateReceipt},
		{name: "session B", pid: evidence.SessionBBackendPID, invocation: evidence.SessionBInvocationID, sample: evidence.SessionBCandidateReceipt},
	} {
		if receipt.sample.ConnectionID != strconv.FormatUint(uint64(receipt.pid), 10) || receipt.sample.RuntimeInvocationID != receipt.invocation {
			reasons = append(reasons, receipt.name+" receipt does not match its backend and invocation")
		}
		reasons = append(reasons, validateOperationalCandidateSample(receipt.sample, result, requirements)...)
	}
	return reasons
}

// operationalWarmSamples returns only timed warm samples used for runtime attribution.
func operationalWarmSamples(result CaseResult) []LatencySample {
	var samples []LatencySample
	for _, sample := range result.Stats.Samples {
		if sample.Classification == "warm" && sample.Duration > 0 {
			samples = append(samples, sample)
		}
	}
	return samples
}

// validateOperationalEventInvocation binds every receipt event to the timed invocation.
func validateOperationalEventInvocation(sample LatencySample) string {
	for _, event := range sample.RuntimeReceiptEvents {
		if event.InvocationID != sample.RuntimeInvocationID {
			return "runtime receipt event is not bound to its timed invocation"
		}
	}
	return ""
}

// receiptChainContainsIdentity reports whether a fallback identity appears in an ordered receipt chain.
func receiptChainContainsIdentity(events []RuntimeReceiptEvent, identity string, fallback bool) bool {
	for _, event := range events {
		if event.RuntimeIdentity == identity && event.FallbackExecuted == fallback {
			return true
		}
	}
	return false
}

// normalizedPlanCacheMode returns the canonical PostgreSQL mode spelling.
func normalizedPlanCacheMode(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

// parsePostgresMemoryBytes parses the integral PostgreSQL memory-setting forms
// emitted by current_setting, including the server-minimum 64kB work_mem.
func parsePostgresMemoryBytes(value string) (int64, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, fmt.Errorf("setting is empty")
	}
	index := 0
	for index < len(trimmed) && trimmed[index] >= '0' && trimmed[index] <= '9' {
		index++
	}
	if index == 0 {
		return 0, fmt.Errorf("setting %q has no integral value", value)
	}
	amount, err := strconv.ParseInt(trimmed[:index], 10, 64)
	if err != nil || amount <= 0 {
		return 0, fmt.Errorf("setting %q has an invalid value", value)
	}
	unit := strings.ToLower(strings.TrimSpace(trimmed[index:]))
	multiplier := int64(1)
	switch unit {
	case "", "b":
	case "kb":
		multiplier = 1024
	case "mb":
		multiplier = 1024 * 1024
	case "gb":
		multiplier = 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("setting %q has an unsupported unit", value)
	}
	if amount > (1<<63-1)/multiplier {
		return 0, fmt.Errorf("setting %q overflows bytes", value)
	}
	return amount * multiplier, nil
}

// sameOperationalDatabase compares server and schema identity while allowing
// plan_cache_mode and work_mem to vary across required matrix cells.
func sameOperationalDatabase(left, right *PostgresEnvironment) bool {
	return left.Version == right.Version && left.Database == right.Database &&
		left.TempFileLimit == right.TempFileLimit && left.GraphPartitionCount == right.GraphPartitionCount &&
		left.PostmasterStartedAt.Equal(right.PostmasterStartedAt) && left.DatabaseOID == right.DatabaseOID &&
		left.Autovacuum == right.Autovacuum && left.SchemaFingerprint == right.SchemaFingerprint &&
		left.IndexFingerprint == right.IndexFingerprint
}

// cloneOperationalPromotionIdentity prevents callers from mutating a completed report through shared maps or slices.
func cloneOperationalPromotionIdentity(identity PromotionEvidenceIdentity) PromotionEvidenceIdentity {
	identity.Caps = clonePromotionCaps(identity.Caps)
	identity.Buckets = clonePromotionBuckets(identity.Buckets)
	return identity
}

// cloneOperationalRequirements prevents callers from mutating a completed report through shared slices.
func cloneOperationalRequirements(requirements OperationalGateRequirements) OperationalGateRequirements {
	requirements.PoolSizes = append([]int(nil), requirements.PoolSizes...)
	requirements.ConcurrencyLevels = append([]int(nil), requirements.ConcurrencyLevels...)
	requirements.PlanCacheModes = append([]string(nil), requirements.PlanCacheModes...)
	return requirements
}
