// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTraversalExecutionTelemetrySummaryValidation verifies traversal execution telemetry summary validation behavior.
func TestTraversalExecutionTelemetrySummaryValidation(t *testing.T) {
	telemetry := validTraversalTelemetry()

	require.NoError(t, telemetry.Validate())

	telemetry.Summary.Overflow = nil
	err := telemetry.Validate()
	require.ErrorContains(t, err, "summary.overflow is missing")

	telemetry = validTraversalTelemetry()
	delete(telemetry.Summary.Provenance, "runtime_identity")
	err = telemetry.Validate()
	require.ErrorContains(t, err, "summary.runtime_identity provenance is missing")
}

// TestTraversalExecutionTelemetrySummaryRejectsContradictoryIdentityChain verifies traversal execution telemetry summary rejects contradictory identity chain behavior.
func TestTraversalExecutionTelemetrySummaryRejectsContradictoryIdentityChain(t *testing.T) {
	telemetry := validTraversalTelemetry()
	telemetry.Summary.RuntimeIdentity = "unplanned-v1"

	require.ErrorContains(t, telemetry.Validate(), "summary.runtime_identity is not a planned identity")

	telemetry = validTraversalTelemetry()
	telemetry.Summary.FallbackExecuted = telemetryBool(true)
	telemetry.Summary.FallbackIdentity = "incumbent-v1"
	telemetry.Summary.Provenance["fallback_identity"] = "executor.fallback_identity"

	require.ErrorContains(t, telemetry.Validate(), "summary.applied_identity must equal fallback_identity")
}

// TestTraversalExecutionTelemetryDiagnosticRequiresPointerCountersAndProvenance verifies traversal execution telemetry diagnostic requires pointer counters and provenance behavior.
func TestTraversalExecutionTelemetryDiagnosticRequiresPointerCountersAndProvenance(t *testing.T) {
	telemetry := validTraversalTelemetry()
	telemetry.Level = TraversalTelemetryLevelDiagnostic
	telemetry.Diagnostic = ordinaryDiagnostic()

	require.NoError(t, telemetry.Validate())

	telemetry.Diagnostic.Counters.Ordinary.RecursiveRows = nil
	err := telemetry.Validate()
	require.ErrorContains(t, err, "diagnostic.counters.ordinary.recursive_rows is missing")

	telemetry.Diagnostic.Counters.Ordinary.RecursiveRows = telemetryInt64(0)
	delete(telemetry.Diagnostic.Provenance, "ordinary.recursive_rows")
	err = telemetry.Validate()
	require.ErrorContains(t, err, "diagnostic.counters.ordinary.recursive_rows provenance is missing")
}

// TestTraversalExecutionTelemetryDiagnosticCannotBeTimed verifies traversal execution telemetry diagnostic cannot be timed behavior.
func TestTraversalExecutionTelemetryDiagnosticCannotBeTimed(t *testing.T) {
	telemetry := validTraversalTelemetry()
	telemetry.Level = TraversalTelemetryLevelDiagnostic
	telemetry.Diagnostic = ordinaryDiagnostic()
	telemetry.Diagnostic.TimedSample = telemetryBool(true)

	require.ErrorContains(t, telemetry.Validate(), "diagnostic.timed_sample must be false")
}

// TestTraversalExecutionTelemetryValidatesSuffixGuardIndependently verifies
// suffix-guard evidence does not require fabricated orientation scores while
// still failing closed on a missing sentinel outcome.
func TestTraversalExecutionTelemetryValidatesSuffixGuardIndependently(t *testing.T) {
	telemetry := validTraversalTelemetry()
	telemetry.Level = TraversalTelemetryLevelDiagnostic
	provenance := map[string]string{}
	for _, name := range []string{
		"root_presence_rows", "suffix_rows", "distinct_boundary_rows", "state_rows", "output_rows", "candidate_marker_rows",
		"fallback_marker_rows", "candidate_branch_rows", "fallback_branch_rows", "candidate_executor_loops", "fallback_executor_loops",
		"suffix_overflow", "state_overflow",
	} {
		provenance["suffix_guard."+name] = "plan." + name
	}
	telemetry.Diagnostic = &TraversalExecutionDiagnostic{
		InvocationID: "invocation-1", ConnectionID: "backend-123", TimedSample: telemetryBool(false),
		RequiredFamilies: []TraversalTelemetryFamily{TraversalTelemetryFamilySuffixGuard}, CounterStatus: TraversalTelemetryCounterStatusComplete,
		Counters: TraversalDiagnosticCounters{SuffixGuard: &SuffixGuardTraversalCounters{
			RootPresenceRows: telemetryInt64(1), SuffixRows: telemetryInt64(2), DistinctBoundaryRows: telemetryInt64(1), StateRows: telemetryInt64(9),
			OutputRows: telemetryInt64(1), CandidateMarkerRows: telemetryInt64(1), FallbackMarkerRows: telemetryInt64(0),
			CandidateBranchRows: telemetryInt64(1), FallbackBranchRows: telemetryInt64(0), CandidateExecutorLoops: telemetryInt64(1),
			FallbackExecutorLoops: telemetryInt64(0), SuffixOverflow: telemetryBool(false), StateOverflow: telemetryBool(false),
		}}, Provenance: provenance,
	}
	require.NoError(t, telemetry.Validate())
	telemetry.Diagnostic.Counters.SuffixGuard.StateOverflow = nil
	require.ErrorContains(t, telemetry.Validate(), "suffix_guard.state_overflow is missing")
}

// TestTraversalExecutionTelemetryAttachmentsSerializeVersionedSchema verifies traversal execution telemetry attachments serialize versioned schema behavior.
func TestTraversalExecutionTelemetryAttachmentsSerializeVersionedSchema(t *testing.T) {
	telemetry := validTraversalTelemetry()
	encoded, err := json.Marshal(struct {
		// Case supplies the case input to the anonymous record contract.
		Case CaseResult `json:"case"`
		// Reference supplies the reference input to the anonymous record contract.
		Reference PostgresReferenceResult `json:"reference"`
	}{
		Case:      CaseResult{TraversalTelemetry: &telemetry},
		Reference: PostgresReferenceResult{TraversalTelemetry: &telemetry},
	})

	require.NoError(t, err)
	require.Contains(t, string(encoded), `"traversal_execution_telemetry":{"schema_version":2`)
}

// validTraversalTelemetry returns a self-consistent telemetry fixture for the requested architecture.
func validTraversalTelemetry() TraversalExecutionTelemetry {
	return TraversalExecutionTelemetry{
		SchemaVersion: TraversalExecutionTelemetrySchemaVersion,
		Level:         TraversalTelemetryLevelSummary,
		Summary: TraversalExecutionSummary{
			RequestedIdentity: "requested-v1",
			PlannedIdentities: []string{"candidate-v1", "incumbent-v1"},
			EmittedIdentity:   "policy-v1",
			RuntimeIdentity:   "candidate-v1",
			AppliedIdentity:   "candidate-v1",
			SelectorVersion:   "selector-v1",
			SchedulerVersion:  "scheduler-v1",
			Caps:              map[string]int64{"state": 32},
			RuntimeBranch:     "candidate",
			Overflow:          telemetryBool(false),
			FallbackExecuted:  telemetryBool(false),
			Provenance: map[string]string{
				"requested_identity": "optimizer.request",
				"planned_identities": "optimizer.candidates",
				"emitted_identity":   "translator.policy",
				"runtime_identity":   "executor.branch",
				"applied_identity":   "executor.applied",
				"selector_version":   "optimizer.selector",
				"scheduler_version":  "executor.scheduler",
				"caps.state":         "policy.state_cap",
				"runtime_branch":     "executor.branch",
				"overflow":           "executor.guard",
				"fallback_executed":  "executor.fallback",
			},
		},
	}
}

// ordinaryDiagnostic prepares or inspects test evidence for ordinary diagnostic.
func ordinaryDiagnostic() *TraversalExecutionDiagnostic {
	provenance := map[string]string{}
	for _, name := range []string{
		"roots", "edge_candidates", "admitted_states", "relationship_repeat_rejects", "recursive_rows",
		"peak_state", "emitted_trails", "hydration_rows",
	} {
		provenance["ordinary."+name] = "traversal_recursive_cte." + name
	}

	return &TraversalExecutionDiagnostic{
		InvocationID:     "invocation-1",
		ConnectionID:     "backend-123",
		TimedSample:      telemetryBool(false),
		RequiredFamilies: []TraversalTelemetryFamily{TraversalTelemetryFamilyOrdinary},
		CounterStatus:    TraversalTelemetryCounterStatusComplete,
		Counters: TraversalDiagnosticCounters{
			Ordinary: &OrdinaryTraversalCounters{
				Roots:                     telemetryInt64(0),
				EdgeCandidates:            telemetryInt64(0),
				AdmittedStates:            telemetryInt64(0),
				RelationshipRepeatRejects: telemetryInt64(0),
				RecursiveRows:             telemetryInt64(0),
				PeakState:                 telemetryInt64(0),
				EmittedTrails:             telemetryInt64(0),
				HydrationRows:             telemetryInt64(0),
			},
		},
		Provenance: provenance,
	}
}

// telemetryInt64 prepares or inspects test evidence for telemetry int64.
func telemetryInt64(value int64) *int64 {
	return &value
}

// telemetryBool prepares or inspects test evidence for telemetry bool.
func telemetryBool(value bool) *bool {
	return &value
}
