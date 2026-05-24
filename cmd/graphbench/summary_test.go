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
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestApplyBaseline(t *testing.T) {
	var (
		dir  = t.TempDir()
		path = filepath.Join(dir, "baseline.jsonl")
	)

	require.NoError(t, writeJSONLFile(path, []CaseResult{{
		Dataset:       "base",
		Name:          "case",
		ExecutionMode: ModePostgresSQL,
		Stats: DurationStats{
			Iterations: 1,
			Median:     10 * time.Millisecond,
		},
	}}))

	records := []CaseResult{{
		Dataset:       "base",
		Name:          "case",
		ExecutionMode: ModePostgresSQL,
		Stats: DurationStats{
			Iterations: 1,
			Median:     15 * time.Millisecond,
		},
	}}

	require.NoError(t, applyBaseline(path, records))
	require.NotNil(t, records[0].Baseline)
	require.Equal(t, 1.5, records[0].Baseline.Ratio)
	require.Equal(t, 5*time.Millisecond, records[0].Baseline.Change)
}

func TestWriteMarkdownSummary(t *testing.T) {
	var (
		summary = buildSummary([]CaseResult{
			{
				Dataset:       "base",
				Name:          "case",
				Category:      "counts",
				ExecutionMode: ModePostgresSQL,
				Status:        StatusOK,
				RowCount:      1,
				Stats: DurationStats{
					Iterations: 1,
					Median:     2 * time.Millisecond,
				},
			},
			{
				Dataset:        "base",
				Name:           "case",
				Category:       "counts",
				ExecutionMode:  ModeLocalTraversal,
				Status:         StatusNotImplemented,
				FallbackReason: localTraversalUnavailableReason,
			},
		})
		output bytes.Buffer
	)

	require.NoError(t, writeMarkdownSummary(&output, summary))
	require.Contains(t, output.String(), "| case | base | counts | 2.0ms; rows=1 | not_implemented; local traversal executor unavailable | - |")
}
