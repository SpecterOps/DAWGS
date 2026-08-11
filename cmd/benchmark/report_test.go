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
	"github.com/stretchr/testify/require"
)

// TestWriteJSONEmitsBaselineFriendlyReport verifies that JSON retains row diagnostics, timing values, SQL, and every optimizer decision needed for baseline comparisons.
func TestWriteJSONEmitsBaselineFriendlyReport(t *testing.T) {
	var (
		distinctRows  = int64(2)
		duplicateRows = int64(0)
		loweringPlan  = optimize.LoweringPlan{
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
		report = Report{
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
						PlannedLowerings: loweringPlan.Decisions(),
						Lowerings: []optimize.LoweringDecision{{
							Name: "ProjectionPruning",
						}},
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
		output bytes.Buffer
	)

	require.NoError(t, writeJSON(&output, report))

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
		`"planned_lowerings": [`,
		`"lowerings": [`,
		`"name": "ProjectionPruning"`,
		`"lowering_plan": {`,
		`"projection_pruning": [`,
		`"referenced_symbols": [`,
		`"section": "Traversal"`,
	} {
		require.Contains(t, text, expected)
	}
}

// TestWriteMarkdownIncludesDiagnosticColumns verifies that Markdown exposes distinct and duplicate row counts alongside timing and plan-capture status.
func TestWriteMarkdownIncludesDiagnosticColumns(t *testing.T) {
	var (
		distinctRows  = int64(2)
		duplicateRows = int64(0)
		report        = Report{
			Driver:     "pg",
			GitRef:     "abc123",
			Date:       "2026-05-14",
			Iterations: 3,
			Results: []Result{{
				Section:           "Fixed Suffix Expansion Fanout",
				Dataset:           "fixed_suffix_expansion_fanout",
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
		output bytes.Buffer
	)

	require.NoError(t, writeMarkdown(&output, report))

	text := output.String()
	for _, expected := range []string{
		"Distinct Rows",
		"Duplicate Rows",
		"| Fixed Suffix Expansion Fanout / combined | fixed_suffix_expansion_fanout | 2 | 2 | 0 | 10.0ms | 20.0ms | 30.0ms | captured |",
	} {
		require.Contains(t, text, expected)
	}
}

// TestValidateIterationsRejectsZero verifies that benchmark execution requires at least one measured iteration.
func TestValidateIterationsRejectsZero(t *testing.T) {
	require.Error(t, validateIterations(0))
	require.NoError(t, validateIterations(1))
}

// TestWriteReportRejectsUnknownFormat verifies that report dispatch fails instead of silently choosing a serializer for an unsupported format.
func TestWriteReportRejectsUnknownFormat(t *testing.T) {
	err := writeReport(&bytes.Buffer{}, Report{}, "xml")
	require.ErrorContains(t, err, "unsupported output format")
}

// TestWriteJSON verifies that JSON dispatch preserves the selected driver and emits raw duration samples in nanoseconds.
func TestWriteJSON(t *testing.T) {
	report := testReport()
	var out bytes.Buffer

	require.NoError(t, writeReport(&out, report, reportFormatJSON))

	require.Contains(t, out.String(), `"driver": "pg"`)
	require.Contains(t, out.String(), `"samples": [`)
	require.Contains(t, out.String(), `1000000`)
}

// TestWriteBenchfmt verifies that benchfmt output carries platform metadata, a stable benchmark name, and one ns/op observation per sample.
func TestWriteBenchfmt(t *testing.T) {
	report := testReport()
	var out bytes.Buffer

	require.NoError(t, writeReport(&out, report, reportFormatBenchfmt))

	output := out.String()
	require.Contains(t, output, "goos: ")
	require.Contains(t, output, "goarch: ")
	require.Contains(t, output, "pkg: github.com/specterops/dawgs/cmd/benchmark")
	require.Contains(t, output, "BenchmarkDawgsIntegration/pg/base/Match_Nodes/base-")
	require.Contains(t, output, "\t1\t1000000 ns/op")
	require.Contains(t, output, "\t1\t2000000 ns/op")
}

// TestSanitizeBenchNamePart verifies that benchmark labels normalize whitespace and arrows without destroying hierarchy separators, and that empty labels receive a fallback.
func TestSanitizeBenchNamePart(t *testing.T) {
	require.Equal(t, "Shortest_Paths", sanitizeBenchNamePart("Shortest Paths"))
	require.Equal(t, "n1_-_n3", sanitizeBenchNamePart("n1 -> n3"))
	require.Equal(t, "local/phantom", sanitizeBenchNamePart("local/phantom"))
	require.Equal(t, "unknown", sanitizeBenchNamePart(""))
}

// TestWriteMarkdownOmitsSamples verifies that Markdown reports aggregate timings without leaking the raw nanosecond sample series.
func TestWriteMarkdownOmitsSamples(t *testing.T) {
	report := testReport()
	var out bytes.Buffer

	require.NoError(t, writeReport(&out, report, reportFormatMarkdown))

	output := out.String()
	require.Contains(t, output, "| Match Nodes | base | 2 | - | - | 2.0ms | 2.0ms | 2.0ms | - |")
	require.False(t, strings.Contains(output, "1000000"))
}

// testReport returns a representative report used by serializer tests.
func testReport() Report {
	return Report{
		Driver:     "pg",
		GitRef:     "abcdef0",
		Date:       "2026-05-11",
		Iterations: 2,
		Results: []Result{{
			Section:  "Match Nodes",
			Dataset:  "base",
			Label:    "base",
			RowCount: 2,
			Stats: Stats{
				Median: 2 * time.Millisecond,
				P95:    2 * time.Millisecond,
				Max:    2 * time.Millisecond,
			},
			Samples: []time.Duration{
				time.Millisecond,
				2 * time.Millisecond,
			},
		}},
	}
}
