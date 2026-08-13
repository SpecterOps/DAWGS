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

func TestTraversalExecutionTelemetryDiagnosticCannotBeTimed(t *testing.T) {
	telemetry := validTraversalTelemetry()
	telemetry.Level = TraversalTelemetryLevelDiagnostic
	telemetry.Diagnostic = ordinaryDiagnostic()
	telemetry.Diagnostic.TimedSample = telemetryBool(true)

	require.ErrorContains(t, telemetry.Validate(), "diagnostic.timed_sample must be false")
}

func TestTraversalExecutionTelemetryAttachmentsSerializeVersionedSchema(t *testing.T) {
	telemetry := validTraversalTelemetry()
	encoded, err := json.Marshal(struct {
		Case      CaseResult              `json:"case"`
		Reference PostgresReferenceResult `json:"reference"`
	}{
		Case:      CaseResult{TraversalTelemetry: &telemetry},
		Reference: PostgresReferenceResult{TraversalTelemetry: &telemetry},
	})

	require.NoError(t, err)
	require.Contains(t, string(encoded), `"traversal_execution_telemetry":{"schema_version":2`)
}

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

func telemetryInt64(value int64) *int64 {
	return &value
}

func telemetryBool(value bool) *bool {
	return &value
}
