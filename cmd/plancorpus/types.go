package main

import "github.com/specterops/dawgs/cypher/models/pgsql/translate"

type PlanRecord struct {
	Driver           string                         `json:"driver"`
	Source           string                         `json:"source"`
	Dataset          string                         `json:"dataset,omitempty"`
	Name             string                         `json:"name"`
	Cypher           string                         `json:"cypher"`
	Params           map[string]any                 `json:"params,omitempty"`
	SQL              string                         `json:"sql,omitempty"`
	PGPlan           []string                       `json:"pg_plan,omitempty"`
	PGOperators      []string                       `json:"pg_operators,omitempty"`
	Neo4jPlan        *Neo4jPlanNode                 `json:"neo4j_plan,omitempty"`
	Neo4jOperators   []string                       `json:"neo4j_operators,omitempty"`
	PlannedLowerings []string                       `json:"planned_lowerings,omitempty"`
	AppliedLowerings []string                       `json:"applied_lowerings,omitempty"`
	SkippedLowerings []translate.SkippedLowering    `json:"skipped_lowerings,omitempty"`
	Optimization     *translate.OptimizationSummary `json:"optimization,omitempty"`
	Error            string                         `json:"error,omitempty"`
}

type Neo4jPlanNode struct {
	Operator    string            `json:"operator"`
	Arguments   map[string]string `json:"arguments,omitempty"`
	Identifiers []string          `json:"identifiers,omitempty"`
	Children    []Neo4jPlanNode   `json:"children,omitempty"`
}

type CorpusQuery struct {
	Source  string
	Dataset string
	Name    string
	Cypher  string
	Params  map[string]any
}
