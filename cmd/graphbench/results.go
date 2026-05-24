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
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
)

const (
	StatusOK          = "ok"
	StatusRowMismatch = "row_mismatch"
	StatusError       = "error"
)

type DurationStats struct {
	Iterations int           `json:"iterations"`
	Median     time.Duration `json:"median"`
	P95        time.Duration `json:"p95"`
	Max        time.Duration `json:"max"`
}

type PostgresPlanMetrics struct {
	PlanningMS  *float64 `json:"planning_ms,omitempty"`
	ExecutionMS *float64 `json:"execution_ms,omitempty"`
	Buffers     Buffers  `json:"buffers,omitempty"`
}

type Buffers struct {
	SharedHit     int64 `json:"shared_hit,omitempty"`
	SharedRead    int64 `json:"shared_read,omitempty"`
	SharedDirtied int64 `json:"shared_dirtied,omitempty"`
	TempRead      int64 `json:"temp_read,omitempty"`
	TempWritten   int64 `json:"temp_written,omitempty"`
}

type CaseResult struct {
	Source           string                         `json:"source"`
	Dataset          string                         `json:"dataset"`
	Name             string                         `json:"name"`
	Category         string                         `json:"category"`
	ExecutionMode    ExecutionMode                  `json:"execution_mode"`
	Status           string                         `json:"status"`
	Cypher           string                         `json:"cypher"`
	Params           map[string]any                 `json:"params,omitempty"`
	ExpectedRowCount *int64                         `json:"expected_row_count,omitempty"`
	RowCount         int64                          `json:"row_count,omitempty"`
	Stats            DurationStats                  `json:"stats,omitempty"`
	SQL              string                         `json:"sql,omitempty"`
	PostgresPlan     []string                       `json:"postgres_plan,omitempty"`
	PostgresMetrics  *PostgresPlanMetrics           `json:"postgres_metrics,omitempty"`
	Optimization     *translate.OptimizationSummary `json:"optimization,omitempty"`
	Error            string                         `json:"error,omitempty"`
}

func newCaseResult(testCase ScaleCase, mode ExecutionMode, params map[string]any) CaseResult {
	return CaseResult{
		Source:           testCase.Source,
		Dataset:          testCase.Dataset,
		Name:             testCase.Name,
		Category:         testCase.Category,
		ExecutionMode:    mode,
		Status:           StatusOK,
		Cypher:           testCase.Cypher,
		Params:           params,
		ExpectedRowCount: testCase.Expected.RowCount,
	}
}

func computeDurationStats(durations []time.Duration) DurationStats {
	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})

	n := len(durations)
	return DurationStats{
		Iterations: n,
		Median:     durations[n/2],
		P95:        durations[n*95/100],
		Max:        durations[n-1],
	}
}

func applyRowExpectation(result *CaseResult) {
	if result.ExpectedRowCount != nil && result.RowCount != *result.ExpectedRowCount {
		result.Status = StatusRowMismatch
		result.Error = fmt.Sprintf("expected %d rows, got %d", *result.ExpectedRowCount, result.RowCount)
	}
}

func writeJSONLFile(path string, records []CaseResult) error {
	if path == "" {
		return writeJSONL(os.Stdout, records)
	}

	output, err := os.Create(path)
	if err != nil {
		return err
	}
	defer output.Close()

	return writeJSONL(output, records)
}

func writeJSONL(w io.Writer, records []CaseResult) error {
	encoder := json.NewEncoder(w)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			return err
		}
	}

	return nil
}
