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
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/specterops/dawgs/graph"
)

type ExplainFunc func(ctx context.Context, tx graph.Transaction, cypher string) (*ExplainResult, error)

type RunOptions struct {
	Explain ExplainFunc
}

// Stats holds computed timing statistics for a scenario.
type Stats struct {
	Median time.Duration `json:"median"`
	P95    time.Duration `json:"p95"`
	Max    time.Duration `json:"max"`
}

// Result is one row in the report.
type Result struct {
	Section           string         `json:"section"`
	Dataset           string         `json:"dataset"`
	Label             string         `json:"label"`
	RowCount          int64          `json:"row_count"`
	DistinctRowCount  *int64         `json:"distinct_row_count,omitempty"`
	DuplicateRowCount *int64         `json:"duplicate_row_count,omitempty"`
	Explain           *ExplainResult `json:"explain,omitempty"`
	Stats             Stats          `json:"stats"`
}

// runScenario executes a scenario N times and returns timing stats.
func runScenario(ctx context.Context, db graph.Database, s Scenario, iterations int, options RunOptions) (Result, error) {
	if err := validateIterations(iterations); err != nil {
		return Result{}, err
	}

	// Warm-up: one untimed run.
	var measurement Measurement
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		nextMeasurement, err := s.Query(tx)
		measurement = nextMeasurement
		return err
	}); err != nil {
		return Result{}, err
	}

	durations := make([]time.Duration, iterations)

	for i := range iterations {
		start := time.Now()
		if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
			_, err := s.Query(tx)
			return err
		}); err != nil {
			return Result{}, err
		}
		durations[i] = time.Since(start)
	}

	result := Result{
		Section:           s.Section,
		Dataset:           s.Dataset,
		Label:             s.Label,
		RowCount:          measurement.RowCount,
		DistinctRowCount:  measurement.DistinctRowCount,
		DuplicateRowCount: measurement.DuplicateRowCount,
		Stats:             computeStats(durations),
	}

	if options.Explain != nil && s.Cypher != "" {
		if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
			explain, err := options.Explain(ctx, tx, s.Cypher)
			result.Explain = explain
			return err
		}); err != nil {
			return Result{}, err
		}
	}

	return result, nil
}

func validateIterations(iterations int) error {
	if iterations < 1 {
		return fmt.Errorf("iterations must be at least 1")
	}

	return nil
}

func computeStats(durations []time.Duration) Stats {
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

	n := len(durations)

	return Stats{
		Median: durations[n/2],
		P95:    durations[n*95/100],
		Max:    durations[n-1],
	}
}
