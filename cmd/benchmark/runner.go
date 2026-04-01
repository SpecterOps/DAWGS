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
	"sort"
	"time"

	"github.com/specterops/dawgs/graph"
)

// Stats holds computed timing statistics for a scenario.
type Stats struct {
	Median time.Duration
	P95    time.Duration
	Max    time.Duration
}

// Result is one row in the report.
type Result struct {
	Section string
	Dataset string
	Label   string
	Stats   Stats
}

// runScenario executes a scenario N times and returns timing stats.
func runScenario(ctx context.Context, db graph.Database, s Scenario, iterations int) (Result, error) {
	// Warm-up: one untimed run.
	if err := db.ReadTransaction(ctx, s.Query); err != nil {
		return Result{}, err
	}

	durations := make([]time.Duration, iterations)

	for i := range iterations {
		start := time.Now()
		if err := db.ReadTransaction(ctx, s.Query); err != nil {
			return Result{}, err
		}
		durations[i] = time.Since(start)
	}

	return Result{
		Section: s.Section,
		Dataset: s.Dataset,
		Label:   s.Label,
		Stats:   computeStats(durations),
	}, nil
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
