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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseBenchfmtNS(t *testing.T) {
	samples := parseBenchfmtNS([]byte(`
goos: linux
BenchmarkThing-12                 10       100.5 ns/op       1 B/op
BenchmarkThing-12                 10       120 ns/op         1 B/op
BenchmarkOther/sub-12              1       200 ns/op
`))

	require.Equal(t, []float64{100.5, 120}, samples["BenchmarkThing-12"])
	require.Equal(t, []float64{200}, samples["BenchmarkOther/sub-12"])
}

func TestSummarizeFindings(t *testing.T) {
	base := benchmarkSamples{
		"BenchmarkRegression-12":  {100, 110, 120},
		"BenchmarkImprovement-12": {200, 200, 200},
		"BenchmarkSame-12":        {100, 100, 100},
		"BenchmarkOnlyBase-12":    {50},
	}
	target := benchmarkSamples{
		"BenchmarkRegression-12":  {140, 150, 160},
		"BenchmarkImprovement-12": {100, 100, 100},
		"BenchmarkSame-12":        {100, 100, 100},
		"BenchmarkOnlyTarget-12":  {75},
	}

	findings := summarizeFindings(base, target)

	require.Equal(t, 3, findings.Compared)
	require.Equal(t, 1, findings.Unchanged)
	require.Equal(t, []string{"BenchmarkOnlyBase-12"}, findings.OnlyBase)
	require.Equal(t, []string{"BenchmarkOnlyTarget-12"}, findings.OnlyTarget)
	require.Len(t, findings.Regressions, 1)
	require.Equal(t, "BenchmarkRegression-12", findings.Regressions[0].Name)
	require.InDelta(t, 36.36, findings.Regressions[0].DeltaPercent, 0.01)
	require.Len(t, findings.Improvements, 1)
	require.Equal(t, "BenchmarkImprovement-12", findings.Improvements[0].Name)
	require.Equal(t, -50.0, findings.Improvements[0].DeltaPercent)

	regressions := findings.regressionsOver(10)
	require.Len(t, regressions, 1)
	require.Equal(t, "BenchmarkRegression-12", regressions[0].Name)
	require.InDelta(t, 36.36, regressions[0].Percent, 0.01)
}

func TestParseBenchmarkMarkdown(t *testing.T) {
	rows := parseBenchmarkMarkdown([]byte(`
| Query | Dataset | Median | P95 | Max |
|-------|---------|-------:|----:|----:|
| Match Nodes | base | 0.14ms | 0.22ms | 0.31ms |
| Match Edges | base | 464ms | 604ms | 604ms |
`))

	require.Equal(t, []markdownBenchmarkRow{
		{Query: "Match Nodes", Dataset: "base", Median: 140 * time.Microsecond},
		{Query: "Match Edges", Dataset: "base", Median: 464 * time.Millisecond},
	}, rows)
}

func TestWriteIntegrationBenchfmt(t *testing.T) {
	var out bytes.Buffer
	rows := []markdownBenchmarkRow{{
		Query:   "Shortest Paths / n1 -> n3",
		Dataset: "base",
		Median:  time.Millisecond,
	}}

	require.NoError(t, writeIntegrationBenchfmt(&out, "pg", rows))
	require.Contains(t, out.String(), "BenchmarkDawgsIntegration/pg/base/Shortest_Paths_/_n1_-_n3-")
	require.Contains(t, out.String(), "\t1\t1000000 ns/op")
}

func TestParseRegressionThreshold(t *testing.T) {
	threshold, err := parseRegressionThreshold("10%")
	require.NoError(t, err)
	require.Equal(t, 10.0, threshold)

	threshold, err = parseRegressionThreshold("2.5")
	require.NoError(t, err)
	require.Equal(t, 2.5, threshold)

	_, err = parseRegressionThreshold("-1")
	require.Error(t, err)
}

func TestValidateBenchtime(t *testing.T) {
	require.NoError(t, validateBenchtime("1s"))
	require.NoError(t, validateBenchtime("100x"))
	require.Error(t, validateBenchtime("0x"))
	require.Error(t, validateBenchtime("soon"))
}
