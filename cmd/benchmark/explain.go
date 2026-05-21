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

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/graph"
)

// ExplainResult captures PostgreSQL-specific plan diagnostics for a scenario.
type ExplainResult struct {
	SQL          string                        `json:"sql"`
	Plan         []string                      `json:"plan"`
	Optimization translate.OptimizationSummary `json:"optimization"`
}

func newPostgresExplainer(kindMapper pgsql.KindMapper, graphID int32) ExplainFunc {
	return func(ctx context.Context, tx graph.Transaction, cypherQuery string) (*ExplainResult, error) {
		regularQuery, err := frontend.ParseCypher(frontend.NewContext(), cypherQuery)
		if err != nil {
			return nil, err
		}

		translation, err := translate.Translate(ctx, regularQuery, kindMapper, nil, graphID)
		if err != nil {
			return nil, err
		}

		sqlQuery, err := translate.Translated(translation)
		if err != nil {
			return nil, err
		}

		result := tx.Raw("EXPLAIN (ANALYZE, BUFFERS) "+sqlQuery, translation.Parameters)
		defer result.Close()

		var plan []string
		for result.Next() {
			values := result.Values()
			if len(values) == 0 {
				continue
			}

			plan = append(plan, fmt.Sprint(values[0]))
		}

		if err := result.Error(); err != nil {
			return nil, err
		}

		return &ExplainResult{
			SQL:          sqlQuery,
			Plan:         plan,
			Optimization: translation.Optimization,
		}, nil
	}
}
