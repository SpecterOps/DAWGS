package main

import (
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/testutil"
)

// PlanRecord captures a query plan together with workload, fixture, and environment identity.
type PlanRecord struct {
	// Metadata captures build and baseline metadata.
	Metadata testutil.BaselineMetadata `json:"metadata"`
	// Driver identifies the database driver that produced the plan or summary.
	Driver string `json:"driver"`
	// Source identifies the source corpus file.
	Source string `json:"source"`
	// Dataset identifies the fixture dataset.
	Dataset string `json:"dataset,omitempty"`
	// Name identifies the case or record within its dataset.
	Name string `json:"name"`
	// Cypher contains the Cypher statement under test.
	Cypher string `json:"cypher"`
	// Params supplies literal query parameters.
	Params map[string]any `json:"params,omitempty"`
	// SQL contains the rendered SQL statement.
	SQL string `json:"sql,omitempty"`
	// PGPlan contains the normalized PostgreSQL text plan.
	PGPlan []string `json:"pg_plan,omitempty"`
	// PGOperators lists normalized PostgreSQL operators found in the captured plan.
	PGOperators []string `json:"pg_operators,omitempty"`
	// Neo4jPlan contains the normalized Neo4j operator tree.
	Neo4jPlan *Neo4jPlanNode `json:"neo4j_plan,omitempty"`
	// Neo4jOperators lists normalized Neo4j operators found in the captured plan.
	Neo4jOperators []string `json:"neo4j_operators,omitempty"`
	// PlannedLowerings lists SQL lowering opportunities identified before optimization.
	PlannedLowerings []string `json:"planned_lowerings,omitempty"`
	// AppliedLowerings lists SQL lowerings actually applied during translation.
	AppliedLowerings []string `json:"applied_lowerings,omitempty"`
	// SkippedLowerings lists identified SQL lowerings not applied.
	SkippedLowerings []translate.SkippedLowering `json:"skipped_lowerings,omitempty"`
	// Optimization captures translation optimization and lowering decisions.
	Optimization *translate.OptimizationSummary `json:"optimization,omitempty"`
	// Error records the failure message when the operation did not succeed.
	Error string `json:"error,omitempty"`
}

// Neo4jPlanNode models the recursive operator tree returned by Neo4j EXPLAIN.
type Neo4jPlanNode struct {
	// Operator identifies the backend plan operator at this node.
	Operator string `json:"operator"`
	// Arguments maps backend plan argument names to stable string representations.
	Arguments map[string]string `json:"arguments,omitempty"`
	// Identifiers lists variables or identifiers referenced by the Neo4j plan node.
	Identifiers []string `json:"identifiers,omitempty"`
	// Children contains child Neo4j plan operators in backend order.
	Children []Neo4jPlanNode `json:"children,omitempty"`
}

// CorpusQuery defines one corpus query and the fixture parameters needed to execute it.
type CorpusQuery struct {
	// Source identifies the source corpus file.
	Source string
	// Dataset identifies the fixture dataset.
	Dataset string
	// Name identifies the case or record within its dataset.
	Name string
	// Cypher contains the Cypher statement under test.
	Cypher string
	// Params supplies literal query parameters.
	Params map[string]any
}
