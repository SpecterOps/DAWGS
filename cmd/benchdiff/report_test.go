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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWriteRunReportIncludesTopLevelFindings(t *testing.T) {
	outputPath := filepath.Join(t.TempDir(), "report.md")
	summary := runSummary{
		Config: resolvedConfig{
			config: config{
				BaseRef:        "main",
				TargetRef:      "HEAD",
				Kind:           benchKindUnit,
				Packages:       "./...",
				Bench:          ".",
				BenchCount:     3,
				Benchtime:      "1s",
				FailRegression: "10%",
			},
			BaseShortSHA:   "abc1234",
			TargetShortSHA: "def5678",
			OutDirAbs:      filepath.Dir(outputPath),
			Threshold:      10,
		},
		GoVersion:  "go-test",
		StartedAt:  time.Date(2026, 5, 11, 1, 2, 3, 0, time.UTC),
		FinishedAt: time.Date(2026, 5, 11, 1, 2, 4, 0, time.UTC),
		Comparisons: []comparison{{
			Name:          "Unit Benchmarks",
			BaseFile:      filepath.Join(filepath.Dir(outputPath), "unit", "base.txt"),
			TargetFile:    filepath.Join(filepath.Dir(outputPath), "unit", "target.txt"),
			BenchstatFile: filepath.Join(filepath.Dir(outputPath), "unit", "benchstat.txt"),
			Benchstat:     "benchstat output\n",
			Findings: comparisonFindings{
				Compared:  3,
				Unchanged: 1,
				Regressions: []benchmarkFinding{{
					Name:           "BenchmarkSlow-12",
					BaseMedianNS:   100,
					TargetMedianNS: 150,
					DeltaPercent:   50,
				}},
				Improvements: []benchmarkFinding{{
					Name:           "BenchmarkFast-12",
					BaseMedianNS:   200,
					TargetMedianNS: 100,
					DeltaPercent:   -50,
				}},
				OnlyBase:   []string{"BenchmarkRemoved-12"},
				OnlyTarget: []string{"BenchmarkAdded-12"},
			},
			Regressions: []regression{{
				Name:           "BenchmarkSlow-12",
				BaseMedianNS:   100,
				TargetMedianNS: 150,
				Percent:        50,
			}},
		}},
	}

	require.NoError(t, writeRunReport(outputPath, summary))

	report, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	output := string(report)

	require.Contains(t, output, "## Findings")
	require.Contains(t, output, "### Unit Benchmarks")
	require.Contains(t, output, "- Compared 3 matching benchmarks.")
	require.Contains(t, output, "- Median regressions: 1; median improvements: 1; unchanged: 1.")
	require.Contains(t, output, "- Only in base: `BenchmarkRemoved-12`.")
	require.Contains(t, output, "- Only in target: `BenchmarkAdded-12`.")
	require.Contains(t, output, "#### Top Median Regressions")
	require.Contains(t, output, "| `BenchmarkSlow-12` | 100ns | 150ns | +50.00% |")
	require.Contains(t, output, "#### Top Median Improvements")
	require.Contains(t, output, "| `BenchmarkFast-12` | 200ns | 100ns | -50.00% |")
	require.Contains(t, output, "## Unit Benchmarks")
	require.Contains(t, output, "benchstat output")
}
