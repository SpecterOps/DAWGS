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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/graph"
)

// ExplainResult captures PostgreSQL-specific plan diagnostics for a scenario.
type ExplainResult struct {
	SQL            string                        `json:"sql"`
	SQLFingerprint string                        `json:"sql_fingerprint"`
	Plan           []string                      `json:"plan"`
	Optimization   translate.OptimizationSummary `json:"optimization"`
	PostgreSQL     PostgreSQLExplainMetrics      `json:"postgresql"`
}

// PostgreSQLExplainMetrics contains structured server-side timings and
// configuration emitted by EXPLAIN. It is intentionally independent of the
// human-readable plan text so benchmark comparisons need not parse it.
type PostgreSQLExplainMetrics struct {
	PlanningTime  time.Duration                     `json:"planning_time"`
	ExecutionTime time.Duration                     `json:"execution_time"`
	Settings      map[string]string                 `json:"settings,omitempty"`
	Stages        map[string]PostgreSQLExplainStage `json:"stages,omitempty"`
}

// PostgreSQLExplainStage is an additive node timing derived from the exact
// JSON plan. It carries no SQL, parameters, or result values.
type PostgreSQLExplainStage struct {
	NodeType string        `json:"node_type"`
	Rows     int64         `json:"rows"`
	Loops    int64         `json:"loops"`
	Total    time.Duration `json:"total"`
}

type postgresExplainDocument struct {
	PlanningTime  float64           `json:"Planning Time"`
	ExecutionTime float64           `json:"Execution Time"`
	Settings      map[string]string `json:"Settings"`
}

func newPostgresExplainer(kindMapper pgsql.KindMapper, graphID int32) ExplainFunc {
	return newPostgresExplainerWithExecutor(kindMapper, graphID, "")
}

func newPostgresExplainerWithExecutor(kindMapper pgsql.KindMapper, graphID int32, executor optimize.ShortestPathExecutor) ExplainFunc {
	return func(ctx context.Context, tx graph.Transaction, scenario Scenario) (*ExplainResult, error) {
		regularQuery, err := frontend.ParseCypher(frontend.NewContext(), scenario.Cypher)
		if err != nil {
			return nil, err
		}

		var translation translate.Result
		if executor != "" && strings.Contains(strings.ToLower(scenario.Cypher), "shortestpath") {
			translation, err = translate.TranslateForTool(ctx, regularQuery, kindMapper, scenario.Parameters, graphID, translate.ToolOptions{ForceShortestPathExecutor: executor})
		} else {
			translation, err = translate.Translate(ctx, regularQuery, kindMapper, scenario.Parameters, graphID)
		}
		if err != nil {
			return nil, err
		}

		sqlQuery, err := translate.Translated(translation)
		if err != nil {
			return nil, err
		}

		result := tx.Raw("EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON) "+sqlQuery, translation.Parameters)
		defer result.Close()

		var plan []string
		var metrics PostgreSQLExplainMetrics
		for result.Next() {
			values := result.Values()
			if len(values) == 0 {
				continue
			}

			rawPlan := explainValueString(values[0])
			plan = append(plan, rawPlan)
			if parsed, err := parsePostgreSQLExplainMetrics(rawPlan); err == nil {
				metrics = parsed
			}
		}

		if err := result.Error(); err != nil {
			return nil, err
		}

		return &ExplainResult{
			SQL:            sqlQuery,
			SQLFingerprint: sqlFingerprint(sqlQuery),
			Plan:           plan,
			Optimization:   translation.Optimization,
			PostgreSQL:     metrics,
		}, nil
	}
}

func explainValueString(value any) string {
	switch typed := value.(type) {
	case []byte:
		return string(typed)
	case string:
		return typed
	default:
		if encoded, err := json.Marshal(value); err == nil {
			return string(encoded)
		}
		return fmt.Sprint(value)
	}
}

func sqlFingerprint(sqlQuery string) string {
	digest := sha256.Sum256([]byte(sqlQuery))
	return hex.EncodeToString(digest[:])
}

func parsePostgreSQLExplainMetrics(raw string) (PostgreSQLExplainMetrics, error) {
	var documents []postgresExplainDocument
	if err := json.Unmarshal([]byte(raw), &documents); err != nil {
		return PostgreSQLExplainMetrics{}, err
	}
	if len(documents) == 0 {
		return PostgreSQLExplainMetrics{}, fmt.Errorf("PostgreSQL EXPLAIN JSON is empty")
	}
	metrics := PostgreSQLExplainMetrics{
		PlanningTime:  time.Duration(documents[0].PlanningTime * float64(time.Millisecond)),
		ExecutionTime: time.Duration(documents[0].ExecutionTime * float64(time.Millisecond)),
		Settings:      documents[0].Settings,
		Stages:        map[string]PostgreSQLExplainStage{},
	}
	var rawDocuments []map[string]any
	if err := json.Unmarshal([]byte(raw), &rawDocuments); err != nil {
		return PostgreSQLExplainMetrics{}, err
	}
	if len(rawDocuments) > 0 {
		collectPostgreSQLExplainStages(rawDocuments[0]["Plan"], metrics.Stages)
	}
	if len(metrics.Stages) == 0 {
		metrics.Stages = nil
	}
	return metrics, nil
}

func collectPostgreSQLExplainStages(value any, stages map[string]PostgreSQLExplainStage) {
	node, ok := value.(map[string]any)
	if !ok {
		return
	}
	name := ""
	for _, key := range []string{"CTE Name", "Function Name", "Subplan Name"} {
		if candidate, ok := node[key].(string); ok && candidate != "" {
			name = candidate
			break
		}
	}
	if name != "" {
		stage := PostgreSQLExplainStage{NodeType: stringExplainValue(node["Node Type"]), Rows: int64ExplainValue(node["Actual Rows"]), Loops: int64ExplainValue(node["Actual Loops"])}
		stage.Total = time.Duration(floatExplainValue(node["Actual Total Time"]) * float64(time.Millisecond) * float64(stage.Loops))
		if existing, found := stages[name]; found {
			existing.Rows += stage.Rows
			existing.Loops += stage.Loops
			existing.Total += stage.Total
			stages[name] = existing
		} else {
			stages[name] = stage
		}
	}
	if children, ok := node["Plans"].([]any); ok {
		for _, child := range children {
			collectPostgreSQLExplainStages(child, stages)
		}
	}
}

func stringExplainValue(value any) string {
	result, _ := value.(string)
	return result
}

func floatExplainValue(value any) float64 {
	result, _ := value.(float64)
	return result
}

func int64ExplainValue(value any) int64 {
	return int64(floatExplainValue(value))
}
