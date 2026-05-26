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
	"time"

	"github.com/specterops/dawgs/graph"
)

func countCypherRows(tx graph.Transaction, cypher string, params map[string]any) (int64, error) {
	result := tx.Query(cypher, params)
	defer result.Close()

	var rowCount int64
	for result.Next() {
		rowCount++
	}

	return rowCount, result.Error()
}

func measureCypher(ctx context.Context, db graph.Database, cypher string, params map[string]any, iterations int) (int64, DurationStats, error) {
	if iterations < 1 {
		return 0, DurationStats{}, fmt.Errorf("iterations must be at least 1")
	}

	var warmupRows int64
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		warmupRows, err = countCypherRows(tx, cypher, params)
		return err
	}); err != nil {
		return 0, DurationStats{}, err
	}

	durations := make([]time.Duration, iterations)
	for idx := range iterations {
		start := time.Now()
		if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
			_, err := countCypherRows(tx, cypher, params)
			return err
		}); err != nil {
			return 0, DurationStats{}, err
		}
		durations[idx] = time.Since(start)
	}

	stats, err := computeDurationStats(durations)
	if err != nil {
		return 0, DurationStats{}, err
	}

	return warmupRows, stats, nil
}
