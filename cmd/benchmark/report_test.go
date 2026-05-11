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

	"github.com/stretchr/testify/require"
)

func TestWriteReportRejectsUnknownFormat(t *testing.T) {
	err := writeReport(&bytes.Buffer{}, Report{}, "xml")
	require.ErrorContains(t, err, "unsupported output format")
}

func TestWriteJSON(t *testing.T) {
	report := testReport()
	var out bytes.Buffer

	require.NoError(t, writeReport(&out, report, reportFormatJSON))

	require.Contains(t, out.String(), `"Driver": "pg"`)
	require.Contains(t, out.String(), `"Samples": [`)
	require.Contains(t, out.String(), `1000000`)
}

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

func TestSanitizeBenchNamePart(t *testing.T) {
	require.Equal(t, "Shortest_Paths", sanitizeBenchNamePart("Shortest Paths"))
	require.Equal(t, "n1_-_n3", sanitizeBenchNamePart("n1 -> n3"))
	require.Equal(t, "local/phantom", sanitizeBenchNamePart("local/phantom"))
	require.Equal(t, "unknown", sanitizeBenchNamePart(""))
}

func TestWriteMarkdownOmitsSamples(t *testing.T) {
	report := testReport()
	var out bytes.Buffer

	require.NoError(t, writeReport(&out, report, reportFormatMarkdown))

	output := out.String()
	require.Contains(t, output, "| Match Nodes | base | 2.0ms | 2.0ms | 2.0ms |")
	require.False(t, strings.Contains(output, "1000000"))
}

func testReport() Report {
	return Report{
		Driver:     "pg",
		GitRef:     "abcdef0",
		Date:       "2026-05-11",
		Iterations: 2,
		Results: []Result{{
			Section: "Match Nodes",
			Dataset: "base",
			Label:   "base",
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
