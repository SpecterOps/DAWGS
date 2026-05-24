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

type ScaleCaseFile struct {
	Cases []ScaleCase `json:"cases"`
}

type ScaleCase struct {
	Source          string            `json:"-"`
	Name            string            `json:"name"`
	Dataset         string            `json:"dataset"`
	Category        string            `json:"category"`
	Cypher          string            `json:"cypher"`
	Params          map[string]any    `json:"params,omitempty"`
	NodeParams      map[string]string `json:"node_params,omitempty"`
	Expected        ExpectedResult    `json:"expected"`
	Observes        ObservedValues    `json:"observes"`
	Shape           WorkloadShape     `json:"shape"`
	CandidateModes  []ExecutionMode   `json:"candidate_modes"`
	Tags            []string          `json:"tags,omitempty"`
	ReferenceDesign *ReferenceDesign  `json:"reference_design,omitempty"`
}

type ExpectedResult struct {
	RowCount   *int64 `json:"row_count,omitempty"`
	ResultKind string `json:"result_kind,omitempty"`
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
