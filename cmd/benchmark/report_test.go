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
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
)

func TestWriteJSONEmitsBaselineFriendlyReport(t *testing.T) {
	distinctRows := int64(2)
	duplicateRows := int64(0)
	loweringPlan := optimize.LoweringPlan{
		ProjectionPruning: []optimize.ProjectionPruningDecision{{
			Target: optimize.TraversalStepTarget{
				QueryPartIndex: 0,
				ClauseIndex:    0,
				PatternIndex:   0,
				StepIndex:      0,
			},
			ReferencedSymbols: []string{"m"},
		}},
	}

	report := Report{
		Driver:     "pg",
		GitRef:     "abc123",
		Date:       "2026-05-14",
		Iterations: 3,
		Results: []Result{{
			Section:           "Traversal",
			Dataset:           "base",
			Label:             "depth 1",
			RowCount:          2,
			DistinctRowCount:  &distinctRows,
			DuplicateRowCount: &duplicateRows,
			Explain: &ExplainResult{
				SQL:  "select 1;",
				Plan: []string{"Result  (actual rows=1 loops=1)"},
				Optimization: translate.OptimizationSummary{
					Rules: []optimize.RuleResult{{
						Name:    "ExpansionSuffixPushdown",
						Applied: true,
					}},
					Lowerings:    loweringPlan.Decisions(),
					LoweringPlan: &loweringPlan,
				},
			},
			Stats: Stats{
				Median: 10 * time.Millisecond,
				P95:    20 * time.Millisecond,
				Max:    30 * time.Millisecond,
			},
		}},
	}

	var output bytes.Buffer
	if err := writeJSON(&output, report); err != nil {
		t.Fatalf("write JSON: %v", err)
	}

	text := output.String()
	for _, expected := range []string{
		`"driver": "pg"`,
		`"git_ref": "abc123"`,
		`"median": 10000000`,
		`"row_count": 2`,
		`"distinct_row_count": 2`,
		`"duplicate_row_count": 0`,
		`"sql": "select 1;"`,
		`"optimization": {`,
		`"name": "ExpansionSuffixPushdown"`,
		`"applied": true`,
		`"lowerings": [`,
		`"name": "ProjectionPruning"`,
		`"lowering_plan": {`,
		`"projection_pruning": [`,
		`"referenced_symbols": [`,
		`"section": "Traversal"`,
	} {
		if !strings.Contains(text, expected) {
			t.Fatalf("JSON report missing %q:\n%s", expected, text)
		}
	}
}

func TestWriteMarkdownIncludesDiagnosticColumns(t *testing.T) {
	distinctRows := int64(2)
	duplicateRows := int64(0)

	report := Report{
		Driver:     "pg",
		GitRef:     "abc123",
		Date:       "2026-05-14",
		Iterations: 3,
		Results: []Result{{
			Section:           "ADCS Fanout",
			Dataset:           "adcs_fanout",
			Label:             "combined",
			RowCount:          2,
			DistinctRowCount:  &distinctRows,
			DuplicateRowCount: &duplicateRows,
			Explain:           &ExplainResult{Plan: []string{"Result"}},
			Stats: Stats{
				Median: 10 * time.Millisecond,
				P95:    20 * time.Millisecond,
				Max:    30 * time.Millisecond,
			},
		}},
	}

	var output bytes.Buffer
	if err := writeMarkdown(&output, report); err != nil {
		t.Fatalf("write markdown: %v", err)
	}

	text := output.String()
	for _, expected := range []string{
		"Distinct Rows",
		"Duplicate Rows",
		"| ADCS Fanout / combined | adcs_fanout | 2 | 2 | 0 | 10.0ms | 20.0ms | 30.0ms | captured |",
	} {
		if !strings.Contains(text, expected) {
			t.Fatalf("markdown report missing %q:\n%s", expected, text)
		}
	}
}

func TestValidateIterationsRejectsZero(t *testing.T) {
	if err := validateIterations(0); err == nil {
		t.Fatal("expected zero iterations to be rejected")
	}

	if err := validateIterations(1); err != nil {
		t.Fatalf("expected one iteration to be valid: %v", err)
	}
}
