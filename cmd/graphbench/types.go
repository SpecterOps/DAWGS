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
	ModePostgresSQL    ExecutionMode = "postgres_sql"
	ModeLocalTraversal ExecutionMode = "local_traversal"
	ModeNeo4j          ExecutionMode = "neo4j"
)

var validExecutionModes = []ExecutionMode{
	ModePostgresSQL,
	ModeLocalTraversal,
	ModeNeo4j,
}

type ExecutionMode string

func (s ExecutionMode) Valid() bool {
	return slices.Contains(validExecutionModes, s)
}

func parseExecutionMode(raw string) (ExecutionMode, error) {
	mode := ExecutionMode(strings.TrimSpace(raw))
	if mode.Valid() {
		return mode, nil
	}

	return "", fmt.Errorf("unsupported execution mode %q", raw)
}

type ScaleCorpus struct {
	Cases []ScaleCase
}

// DeclaredCaseBackend is the version-controlled case/backend contract used by
// the performance gate. CandidateModes is deliberately the source of truth:
// adding, removing, or marking a backend unsupported therefore changes the
// corpus declaration in the same review as the benchmark case.
type DeclaredCaseBackend struct {
	Dataset           string
	Name              string
	Backend           ExecutionMode
	UnsupportedReason string
}

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
				Dataset: testCase.Dataset, Name: testCase.Name, Backend: backend, UnsupportedReason: reason,
			})
		}
	}
	return declared
}

type ScaleCaseFile struct {
	Cases []ScaleCase `json:"cases"`
}

type ScaleCase struct {
	Source                  string                                     `json:"-"`
	Name                    string                                     `json:"name"`
	Dataset                 string                                     `json:"dataset"`
	Category                string                                     `json:"category"`
	Cypher                  string                                     `json:"cypher"`
	Params                  testutil.Params                            `json:"params,omitempty"`
	NodeParams              map[string]string                          `json:"node_params,omitempty"`
	NodeListParams          map[string][]string                        `json:"node_list_params,omitempty"`
	GeneratedNodeListParams map[string]testutil.GeneratedNodeListParam `json:"generated_node_list_params,omitempty"`
	Expected                ExpectedResult                             `json:"expected"`
	Observes                ObservedValues                             `json:"observes"`
	Shape                   WorkloadShape                              `json:"shape"`
	CandidateModes          []ExecutionMode                            `json:"candidate_modes"`
	UnsupportedModes        map[ExecutionMode]string                   `json:"unsupported_modes,omitempty"`
	Tags                    []string                                   `json:"tags,omitempty"`
	ReferenceDesign         *ReferenceDesign                           `json:"reference_design,omitempty"`
	WriteScenario           *WriteScenario                             `json:"write_scenario,omitempty"`
}

type ExpectedResult struct {
	RowCount   *int64         `json:"row_count,omitempty"`
	ScalarInt  *int64         `json:"scalar_int,omitempty"`
	ResultKind string         `json:"result_kind,omitempty"`
	IDRows     [][]string     `json:"id_rows,omitempty"`
	PathRows   []ExpectedPath `json:"path_rows,omitempty"`
}

type ExpectedPath struct {
	Nodes             []string `json:"nodes"`
	RelationshipKinds []string `json:"relationship_kinds"`
	RelationshipKeys  []string `json:"relationship_keys,omitempty"`
}

type WriteScenario struct {
	SelectionCypher         string                                     `json:"selection_cypher"`
	Params                  testutil.Params                            `json:"params,omitempty"`
	NodeParams              map[string]string                          `json:"node_params,omitempty"`
	NodeListParams          map[string][]string                        `json:"node_list_params,omitempty"`
	GeneratedNodeListParams map[string]testutil.GeneratedNodeListParam `json:"generated_node_list_params,omitempty"`
	AffectedEntity          string                                     `json:"affected_entity"`
	ExpectedMatched         *int64                                     `json:"expected_matched"`
	ExpectedAffected        *int64                                     `json:"expected_affected"`
	PostState               []ScaleStateQuery                          `json:"post_state"`
}

type ScaleStateQuery struct {
	Name                    string                                     `json:"name"`
	Cypher                  string                                     `json:"cypher"`
	Params                  testutil.Params                            `json:"params,omitempty"`
	NodeParams              map[string]string                          `json:"node_params,omitempty"`
	NodeListParams          map[string][]string                        `json:"node_list_params,omitempty"`
	GeneratedNodeListParams map[string]testutil.GeneratedNodeListParam `json:"generated_node_list_params,omitempty"`
	Expected                ExpectedResult                             `json:"expected"`
}

type ObservedValues struct {
	Paths         bool `json:"paths"`
	Nodes         bool `json:"nodes"`
	Relationships bool `json:"relationships"`
	Properties    bool `json:"properties"`
}

type WorkloadShape struct {
	RootPredicate               string   `json:"root_predicate,omitempty"`
	TerminalPredicate           string   `json:"terminal_predicate,omitempty"`
	EdgeKinds                   []string `json:"edge_kinds,omitempty"`
	Direction                   string   `json:"direction,omitempty"`
	RelationshipKindCount       int      `json:"relationship_kind_count,omitempty"`
	FixtureTier                 string   `json:"fixture_tier,omitempty"`
	ExpectedStateClass          string   `json:"expected_state_class,omitempty"`
	ResultCardinalityClass      string   `json:"result_cardinality_class,omitempty"`
	MinDepth                    *int     `json:"min_depth,omitempty"`
	MaxDepth                    *int     `json:"max_depth,omitempty"`
	PathMaterializationRequired bool     `json:"path_materialization_required"`
}

type ReferenceDesign struct {
	AGERelevance []string `json:"age_relevance,omitempty"`
	Notes        string   `json:"notes,omitempty"`
}

func (s ScaleCase) Supports(mode ExecutionMode) bool {
	return slices.Contains(s.CandidateModes, mode)
}

func (s ScaleCase) UnsupportedReason(mode ExecutionMode) (string, bool) {
	reason, unsupported := s.UnsupportedModes[mode]
	return reason, unsupported
}
