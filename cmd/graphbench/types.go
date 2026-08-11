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
	"fmt"
	"slices"
	"strings"

	"github.com/specterops/dawgs/testutil"
)

const (
	// ModePostgresSQL selects translated PostgreSQL execution.
	ModePostgresSQL ExecutionMode = "postgres_sql"

	// ModeLocalTraversal selects in-process traversal execution.
	ModeLocalTraversal ExecutionMode = "local_traversal"

	// ModeNeo4j selects Neo4j execution.
	ModeNeo4j ExecutionMode = "neo4j"
)

// validExecutionModes lists every execution mode accepted by graphbench.
var validExecutionModes = []ExecutionMode{
	ModePostgresSQL,
	ModeLocalTraversal,
	ModeNeo4j,
}

type ExecutionMode string

// Valid reports whether the execution mode is one of the supported backend modes.
func (s ExecutionMode) Valid() bool {
	return slices.Contains(validExecutionModes, s)
}

// parseExecutionMode returns the execution mode named by text or an error for unsupported values.
func parseExecutionMode(raw string) (ExecutionMode, error) {
	mode := ExecutionMode(strings.TrimSpace(raw))
	if mode.Valid() {
		return mode, nil
	}

	return "", fmt.Errorf("unsupported execution mode %q", raw)
}

// ScaleCorpus contains the ordered benchmark cases loaded from the scale corpus.
type ScaleCorpus struct {
	// Cases contains loaded workloads in deterministic corpus order.
	Cases []ScaleCase
}

// DeclaredCaseBackend identifies one case/backend combination and any declared unsupported reason.
type DeclaredCaseBackend struct {
	// Dataset identifies the fixture dataset.
	Dataset string
	// Name identifies the case or record within its dataset.
	Name string
	// Backend identifies the execution backend.
	Backend ExecutionMode
	// UnsupportedReason explains why a declared case cannot run on the selected backend.
	UnsupportedReason string
}

// DeclaredBackends expands a scale case into the backend declarations consumed during gate validation.
func (s ScaleCorpus) DeclaredBackends() []DeclaredCaseBackend {
	declared := make([]DeclaredCaseBackend, 0, len(s.Cases)*2)
	for _, testCase := range s.Cases {
		for _, backend := range testCase.CandidateModes {
			declared = append(declared, DeclaredCaseBackend{
				Dataset: testCase.Dataset,
				Name:    testCase.Name,
				Backend: backend,
			})
		}
		for backend, reason := range testCase.UnsupportedModes {
			declared = append(declared, DeclaredCaseBackend{
				Dataset:           testCase.Dataset,
				Name:              testCase.Name,
				Backend:           backend,
				UnsupportedReason: reason,
			})
		}
	}
	return declared
}

// ScaleCaseFile models the JSON envelope containing a group of scale cases.
type ScaleCaseFile struct {
	// Cases contains the workload declarations decoded from one corpus file.
	Cases []ScaleCase `json:"cases"`
}

// ScaleCase declares one executable workload, its parameters, backend support, and exact expectations.
type ScaleCase struct {
	// Source identifies the source corpus file.
	Source string `json:"-"`
	// Name identifies the case or record within its dataset.
	Name string `json:"name"`
	// Dataset identifies the fixture dataset.
	Dataset string `json:"dataset"`
	// Category groups cases by workload category.
	Category string `json:"category"`
	// Cypher contains the Cypher statement under test.
	Cypher string `json:"cypher"`
	// Params supplies literal query parameters.
	Params testutil.Params `json:"params,omitempty"`
	// NodeParams maps query parameters to fixture node keys.
	NodeParams map[string]string `json:"node_params,omitempty"`
	// NodeListParams maps query parameters to ordered fixture node-key lists.
	NodeListParams map[string][]string `json:"node_list_params,omitempty"`
	// GeneratedNodeListParams maps query parameters to generated fixture node sets.
	GeneratedNodeListParams map[string]testutil.GeneratedNodeListParam `json:"generated_node_list_params,omitempty"`
	// Expected defines the required observable result.
	Expected ExpectedResult `json:"expected"`
	// Observes identifies the normalized observation contract declared by the scale case.
	Observes ObservedValues `json:"observes"`
	// Shape describes the workload shape used for selection and comparison.
	Shape WorkloadShape `json:"shape"`
	// CandidateModes lists backends expected to participate in cross-backend comparison.
	CandidateModes []ExecutionMode `json:"candidate_modes"`
	// UnsupportedModes maps unsupported execution modes to their declared reasons.
	UnsupportedModes map[ExecutionMode]string `json:"unsupported_modes,omitempty"`
	// Tags lists selectors attached to the case.
	Tags []string `json:"tags,omitempty"`
	// ReferenceDesign documents reference arms and validation boundaries applicable to the scale case.
	ReferenceDesign *ReferenceDesign `json:"reference_design,omitempty"`
	// WriteScenario defines the mutation and post-state checks measured for the scale case.
	WriteScenario *WriteScenario `json:"write_scenario,omitempty"`
}

// ExpectedResult defines the row cardinality and normalized scalar, ID-row, or path observations a case must return.
type ExpectedResult struct {
	// RowCount records the number of rows produced.
	RowCount *int64 `json:"row_count,omitempty"`
	// ScalarInt sets the required scalar result when ResultKind is scalar_int.
	ScalarInt *int64 `json:"scalar_int,omitempty"`
	// ResultKind identifies how returned values must be normalized.
	ResultKind string `json:"result_kind,omitempty"`
	// IDRows contains the expected ordered identifier rows.
	IDRows [][]string `json:"id_rows,omitempty"`
	// PathRows contains the expected stable paths.
	PathRows []ExpectedPath `json:"path_rows,omitempty"`
}

// ExpectedPath defines one expected stable node and relationship sequence.
type ExpectedPath struct {
	// Nodes contains the stable node sequence.
	Nodes []string `json:"nodes"`
	// RelationshipKinds contains the expected relationship-kind sequence.
	RelationshipKinds []string `json:"relationship_kinds"`
	// RelationshipKeys contains the expected fixture relationship-key sequence.
	RelationshipKeys []string `json:"relationship_keys,omitempty"`
}

// WriteScenario defines a measured mutation and the state checks that validate it.
type WriteScenario struct {
	// SelectionCypher contains the write-selection Cypher statement.
	SelectionCypher string `json:"selection_cypher"`
	// Params supplies literal query parameters.
	Params testutil.Params `json:"params,omitempty"`
	// NodeParams maps query parameters to fixture node keys.
	NodeParams map[string]string `json:"node_params,omitempty"`
	// NodeListParams maps query parameters to ordered fixture node-key lists.
	NodeListParams map[string][]string `json:"node_list_params,omitempty"`
	// GeneratedNodeListParams maps query parameters to generated fixture node sets.
	GeneratedNodeListParams map[string]testutil.GeneratedNodeListParam `json:"generated_node_list_params,omitempty"`
	// AffectedEntity identifies the entity class counted after a write.
	AffectedEntity string `json:"affected_entity"`
	// ExpectedMatched sets the required number of matched entities.
	ExpectedMatched *int64 `json:"expected_matched"`
	// ExpectedAffected sets the required number of affected entities.
	ExpectedAffected *int64 `json:"expected_affected"`
	// PostState defines the state query evaluated after a write.
	PostState []ScaleStateQuery `json:"post_state"`
}

// ScaleStateQuery defines a post-mutation query and its scalar or row-count expectation.
type ScaleStateQuery struct {
	// Name labels the post-write state assertion in diagnostics and results.
	Name string `json:"name"`
	// Cypher contains the Cypher statement under test.
	Cypher string `json:"cypher"`
	// Params supplies literal query parameters.
	Params testutil.Params `json:"params,omitempty"`
	// NodeParams maps query parameters to fixture node keys.
	NodeParams map[string]string `json:"node_params,omitempty"`
	// NodeListParams maps query parameters to ordered fixture node-key lists.
	NodeListParams map[string][]string `json:"node_list_params,omitempty"`
	// GeneratedNodeListParams maps query parameters to generated fixture node sets.
	GeneratedNodeListParams map[string]testutil.GeneratedNodeListParam `json:"generated_node_list_params,omitempty"`
	// Expected defines the required observable result.
	Expected ExpectedResult `json:"expected"`
}

// ObservedValues declares which entity and path features a case exposes for normalized comparison.
type ObservedValues struct {
	// Paths reports whether the normalized result includes materialized paths.
	Paths bool `json:"paths"`
	// Nodes reports whether the normalized result includes node values.
	Nodes bool `json:"nodes"`
	// Relationships reports whether the normalized result includes relationship values.
	Relationships bool `json:"relationships"`
	// Properties reports whether normalized entity observations include properties.
	Properties bool `json:"properties"`
}

// WorkloadShape describes traversal depth, direction, projection, and expected complexity.
type WorkloadShape struct {
	// RootPredicate describes how the traversal root is constrained.
	RootPredicate string `json:"root_predicate,omitempty"`
	// TerminalPredicate describes how the traversal terminal is constrained.
	TerminalPredicate string `json:"terminal_predicate,omitempty"`
	// EdgeKinds lists the relationship kinds traversed by the workload.
	EdgeKinds []string `json:"edge_kinds,omitempty"`
	// Direction sets the traversal direction.
	Direction string `json:"direction,omitempty"`
	// RelationshipKindCount records the number of relationship kinds in the workload.
	RelationshipKindCount int `json:"relationship_kind_count,omitempty"`
	// FixtureTier identifies the fixture scale tier.
	FixtureTier string `json:"fixture_tier,omitempty"`
	// ExpectedStateClass identifies the expected recursive-state complexity class.
	ExpectedStateClass string `json:"expected_state_class,omitempty"`
	// ResultCardinalityClass identifies the expected result-cardinality class.
	ResultCardinalityClass string `json:"result_cardinality_class,omitempty"`
	// MinDepth is the shallowest traversal depth permitted by the workload.
	MinDepth *int `json:"min_depth,omitempty"`
	// MaxDepth sets the maximum traversal depth.
	MaxDepth *int `json:"max_depth,omitempty"`
	// PathMaterializationRequired reports whether the workload must materialize complete paths.
	PathMaterializationRequired bool `json:"path_materialization_required"`
}

// ReferenceDesign documents the independent reference implementations applicable to a case.
type ReferenceDesign struct {
	// AGERelevance documents how the reference design relates to Apache AGE execution.
	AGERelevance []string `json:"age_relevance,omitempty"`
	// Notes contains human-readable caveats attached to the artifact or case.
	Notes string `json:"notes,omitempty"`
}

// Supports reports whether the case declares the requested execution mode as a candidate backend.
func (s ScaleCase) Supports(mode ExecutionMode) bool {
	return slices.Contains(s.CandidateModes, mode)
}

// UnsupportedReason returns the declared reason that a scale case cannot run in the requested mode.
func (s ScaleCase) UnsupportedReason(mode ExecutionMode) (string, bool) {
	reason, unsupported := s.UnsupportedModes[mode]
	return reason, unsupported
}
