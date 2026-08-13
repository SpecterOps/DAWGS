package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
)

const planDeltaSchemaVersion = 2

var planRowsPattern = regexp.MustCompile(`\brows=([0-9]+)\b`)

// workloadFingerprint hashes backend-independent query identity and parameter
// type shape. Physical fixture IDs are deliberately excluded so captures from
// independently loaded backends still pair.
func workloadFingerprint(query CorpusQuery) string {
	parameterTypes := make(map[string]string, len(query.Params))
	for name, value := range query.Params {
		if value == nil {
			parameterTypes[name] = "nil"
		} else {
			parameterTypes[name] = reflect.TypeOf(value).String()
		}
	}

	return jsonFingerprint(struct {
		Source         string            `json:"source"`
		Dataset        string            `json:"dataset,omitempty"`
		Name           string            `json:"name"`
		Cypher         string            `json:"cypher"`
		ParameterTypes map[string]string `json:"parameter_types,omitempty"`
	}{
		Source:         query.Source,
		Dataset:        query.Dataset,
		Name:           query.Name,
		Cypher:         strings.TrimSpace(query.Cypher),
		ParameterTypes: parameterTypes,
	})
}

// postgresPlanFingerprint hashes one normalized PostgreSQL text plan.
func postgresPlanFingerprint(plan []string) string {
	if len(plan) == 0 {
		return ""
	}
	return jsonFingerprint(plan)
}

// neo4jPlanFingerprint hashes one normalized Neo4j plan tree.
func neo4jPlanFingerprint(plan *Neo4jPlanNode) string {
	if plan == nil {
		return ""
	}
	type fingerprintNode struct {
		Operator    string            `json:"operator"`
		Arguments   map[string]string `json:"arguments,omitempty"`
		Identifiers []string          `json:"identifiers,omitempty"`
		Children    []fingerprintNode `json:"children,omitempty"`
	}
	var project func(Neo4jPlanNode) fingerprintNode
	project = func(node Neo4jPlanNode) fingerprintNode {
		projected := fingerprintNode{
			Operator:    normalizeNeo4jOperator(node.Operator),
			Arguments:   structuralNeo4jArguments(node.Arguments),
			Identifiers: append([]string(nil), node.Identifiers...),
		}
		for _, child := range node.Children {
			projected.Children = append(projected.Children, project(child))
		}
		return projected
	}
	return jsonFingerprint(project(*plan))
}

// structuralNeo4jArguments removes execution-only counters from a PROFILE so
// the plan fingerprint remains stable when the same operator tree is replayed.
func structuralNeo4jArguments(arguments map[string]string) map[string]string {
	if len(arguments) == 0 {
		return nil
	}
	filtered := map[string]string{}
	for name, value := range arguments {
		canonical := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(name, "_", ""), " ", ""))
		switch canonical {
		case "rows", "dbhits", "pagecachehits", "pagecachemisses", "time", "timens", "actualrows", "actualloops":
			continue
		default:
			filtered[name] = value
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

// jsonFingerprint returns a stable SHA-256 digest for a JSON-serializable value.
func jsonFingerprint(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// buildPlanDeltaReport pairs records by workload union so a missing backend is
// preserved as evidence instead of disappearing through intersection-only reporting.
func buildPlanDeltaReport(records []PlanRecord) (PlanDeltaReport, error) {
	type pair struct {
		postgres *PlanRecord
		neo4j    *PlanRecord
	}

	type pairKey struct {
		workload string
		revision string
	}
	pairs := map[pairKey]pair{}
	for idx := range records {
		record := &records[idx]
		if record.WorkloadSHA256 == "" {
			record.WorkloadSHA256 = workloadFingerprint(CorpusQuery{
				Source:  record.Source,
				Dataset: record.Dataset,
				Name:    record.Name,
				Cypher:  record.Cypher,
				Params:  record.Params,
			})
		}
		key := pairKey{workload: record.WorkloadSHA256, revision: record.Metadata.DAWGSVersion}
		next := pairs[key]
		switch record.Driver {
		case pgDriverName():
			if next.postgres != nil {
				return PlanDeltaReport{}, fmt.Errorf("duplicate PostgreSQL plan for workload %s at source revision %q", record.WorkloadSHA256, key.revision)
			}
			next.postgres = record
		case neo4jDriverName():
			if next.neo4j != nil {
				return PlanDeltaReport{}, fmt.Errorf("duplicate Neo4j plan for workload %s at source revision %q", record.WorkloadSHA256, key.revision)
			}
			next.neo4j = record
		default:
			return PlanDeltaReport{}, fmt.Errorf("unsupported plan-delta driver %q", record.Driver)
		}
		pairs[key] = next
	}

	report := PlanDeltaReport{Version: planDeltaSchemaVersion}
	for key, next := range pairs {
		identity := next.postgres
		if identity == nil {
			identity = next.neo4j
		}
		delta := PlanDeltaRecord{
			Dataset:        identity.Dataset,
			Source:         identity.Source,
			Name:           identity.Name,
			WorkloadSHA256: key.workload,
			SourceRevision: key.revision,
		}
		if next.postgres != nil {
			plan := semanticPostgresPlan(*next.postgres)
			delta.Postgres = &plan
		}
		if next.neo4j != nil {
			plan := semanticNeo4jPlan(*next.neo4j)
			delta.Neo4j = &plan
		}
		delta.Complete, delta.IncompleteReason = planDeltaCompleteness(delta)
		if delta.Postgres != nil && delta.Neo4j != nil {
			delta.OppositeStartingSides = comparableDifferent(accessSide(delta.Postgres.StartingAccess), accessSide(delta.Neo4j.StartingAccess))
			delta.OppositePhysicalDirections = comparableDifferent(delta.Postgres.PhysicalDirection, delta.Neo4j.PhysicalDirection)
			delta.Neo4jReorderedPattern = neo4jReorderedPattern(identity.Cypher, delta.Neo4j.StartingAccess)
			delta.ChosenSideDidLessObservedWork = lessObservedSeedWork(delta.Neo4j)
			delta.SeedEstimateQError = estimateQError(delta.Postgres.EstimatedSeeds, delta.Neo4j.EstimatedSeeds)
			delta.TraversalEstimateQError = estimateQError(delta.Postgres.EstimatedTraversal, delta.Neo4j.EstimatedTraversal)
			delta.OutputEstimateQError = estimateQError(delta.Postgres.EstimatedOutput, delta.Neo4j.EstimatedOutput)
			delta.PredicatePlacementMoved = predicatePlacementMoved(delta.Postgres.PredicatePlacement, delta.Neo4j.PredicatePlacement)
			delta.HydrationEstimateQError = estimateQError(delta.Postgres.EstimatedHydration, delta.Neo4j.EstimatedHydration)
		}
		delta.PairSHA256 = planDeltaPairFingerprint(delta)
		report.Records = append(report.Records, delta)
	}

	sort.Slice(report.Records, func(i, j int) bool {
		left, right := report.Records[i], report.Records[j]
		if left.Dataset != right.Dataset {
			return left.Dataset < right.Dataset
		}
		if left.Source != right.Source {
			return left.Source < right.Source
		}
		return left.Name < right.Name
	})
	report.RankedFindings = rankPlanDeltaFindings(report.Records)
	return report, nil
}

// planDeltaPairFingerprint binds source and both backend plan identities without embedding raw plans.
func planDeltaPairFingerprint(delta PlanDeltaRecord) string {
	postgresFingerprint, neo4jFingerprint := "", ""
	if delta.Postgres != nil {
		postgresFingerprint = delta.Postgres.PlanFingerprint
	}
	if delta.Neo4j != nil {
		neo4jFingerprint = delta.Neo4j.PlanFingerprint
	}
	return jsonFingerprint(struct {
		Dataset             string `json:"dataset,omitempty"`
		Source              string `json:"source"`
		Name                string `json:"name"`
		WorkloadSHA256      string `json:"workload_sha256"`
		SourceRevision      string `json:"source_revision,omitempty"`
		PostgresFingerprint string `json:"postgres_plan_fingerprint,omitempty"`
		Neo4jFingerprint    string `json:"neo4j_plan_fingerprint,omitempty"`
	}{
		Dataset: delta.Dataset, Source: delta.Source, Name: delta.Name,
		WorkloadSHA256: delta.WorkloadSHA256, SourceRevision: delta.SourceRevision,
		PostgresFingerprint: postgresFingerprint, Neo4jFingerprint: neo4jFingerprint,
	})
}

// accessSide maps backend-specific access labels onto a root/terminal side when possible.
func accessSide(access string) string {
	lower := strings.ToLower(access)
	switch {
	case strings.Contains(lower, "terminal"), strings.Contains(lower, "target"), strings.Contains(lower, " n1"), strings.Contains(lower, "(n1"):
		return "terminal"
	case strings.Contains(lower, "root"), strings.Contains(lower, "source"), strings.Contains(lower, " n0"), strings.Contains(lower, "(n0"):
		return "root"
	default:
		return ""
	}
}

// planDeltaCompleteness reports whether both sides contain successful plan evidence.
func planDeltaCompleteness(delta PlanDeltaRecord) (bool, string) {
	var reasons []string
	if delta.Postgres == nil {
		reasons = append(reasons, "missing_postgres")
	} else if delta.Postgres.Error != "" || delta.Postgres.PlanFingerprint == "" {
		reasons = append(reasons, "failed_postgres")
	}
	if delta.Neo4j == nil {
		reasons = append(reasons, "missing_neo4j")
	} else if delta.Neo4j.Error != "" || delta.Neo4j.PlanFingerprint == "" {
		reasons = append(reasons, "failed_neo4j")
	}
	return len(reasons) == 0, strings.Join(reasons, ",")
}

// comparableDifferent compares nonempty semantic labels.
func comparableDifferent(left, right string) bool {
	return left != "" && right != "" && left != right
}

// neo4jReorderedPattern reports a conservative endpoint reversal relative to the textual first relationship.
func neo4jReorderedPattern(cypherQuery, startingAccess string) bool {
	if logicalDirection(cypherQuery) == "" {
		return false
	}
	return accessSide(startingAccess) == "terminal"
}

// lessObservedSeedWork compares profiled leaf work only when both endpoint leaves expose it.
func lessObservedSeedWork(plan *SemanticPlan) *bool {
	if plan == nil || plan.ObservedSeedWork == nil || plan.ObservedAlternativeSeedWork == nil {
		return nil
	}
	value := *plan.ObservedSeedWork <= *plan.ObservedAlternativeSeedWork
	return &value
}

// estimateQError reports symmetric disagreement between two positive backend estimates.
func estimateQError(left, right *float64) *float64 {
	if left == nil || right == nil || *left <= 0 || *right <= 0 {
		return nil
	}
	value := math.Max(*left / *right, *right / *left)
	return &value
}

// predicatePlacementMoved compares normalized predicate-bearing stage families rather than raw backend syntax.
func predicatePlacementMoved(postgres, neo4j []string) bool {
	if len(postgres) == 0 && len(neo4j) == 0 {
		return false
	}
	postgresStages := normalizedPredicateStages(postgres)
	neo4jStages := normalizedPredicateStages(neo4j)
	return !reflect.DeepEqual(postgresStages, neo4jStages)
}

// normalizedPredicateStages reduces backend syntax to access/filter/join stage counts.
func normalizedPredicateStages(stages []string) map[string]int {
	normalized := map[string]int{}
	for _, stage := range stages {
		lower := strings.ToLower(stage)
		switch {
		case strings.Contains(lower, "join filter"), strings.Contains(lower, "apply"):
			normalized["join"]++
		case strings.Contains(lower, "index cond"), strings.Contains(lower, "seek"):
			normalized["access"]++
		default:
			normalized["filter"]++
		}
	}
	return normalized
}

// neo4jNodeObservedWork prefers DB hits and otherwise uses profiled output rows.
func neo4jNodeObservedWork(node Neo4jPlanNode) *int64 {
	if node.DBHits != nil {
		return node.DBHits
	}
	return node.ActualRows
}

// rankPlanDeltaFindings produces category-local scores and a stable global review order.
func rankPlanDeltaFindings(records []PlanDeltaRecord) []PlanDeltaFinding {
	var findings []PlanDeltaFinding
	appendFinding := func(record PlanDeltaRecord, category string, score float64, summary string) {
		findings = append(findings, PlanDeltaFinding{
			Category: category, Dataset: record.Dataset, Source: record.Source, Name: record.Name,
			PairSHA256: record.PairSHA256, Score: score, Summary: summary,
		})
	}
	for _, record := range records {
		if !record.Complete {
			appendFinding(record, "incomplete_pair", math.MaxFloat64, record.IncompleteReason)
			continue
		}
		if record.OppositeStartingSides {
			summary := "backends start from opposite endpoint sides"
			if record.ChosenSideDidLessObservedWork != nil {
				summary += fmt.Sprintf("; Neo4j lower-work choice=%t", *record.ChosenSideDidLessObservedWork)
			}
			appendFinding(record, "opposite_starting_side", 1, summary)
		}
		for category, value := range map[string]*float64{
			"seed_estimate_disagreement":      record.SeedEstimateQError,
			"traversal_estimate_disagreement": record.TraversalEstimateQError,
			"output_estimate_disagreement":    record.OutputEstimateQError,
			"hydration_estimate_disagreement": record.HydrationEstimateQError,
		} {
			if value != nil && *value > 1 {
				appendFinding(record, category, *value, fmt.Sprintf("backend estimate Q-error %.4g", *value))
			}
		}
		if record.PredicatePlacementMoved {
			appendFinding(record, "predicate_placement_move", 1, "predicate-bearing stage families differ")
		}
		if record.Postgres != nil && (record.Postgres.FallbackReason != "" || len(record.Postgres.ProbeCaps) > 0) {
			summary := "bounded candidate or fallback is present"
			if record.Postgres.FallbackReason != "" {
				summary = "fallback: " + record.Postgres.FallbackReason
			}
			appendFinding(record, "fallback_or_cap", float64(len(record.Postgres.ProbeCaps)+1), summary)
		}
	}
	categoryPriority := map[string]int{
		"incomplete_pair": 0, "fallback_or_cap": 1, "opposite_starting_side": 2,
		"traversal_estimate_disagreement": 3, "seed_estimate_disagreement": 4,
		"output_estimate_disagreement": 5, "predicate_placement_move": 6, "hydration_estimate_disagreement": 7,
	}
	sort.Slice(findings, func(i, j int) bool {
		leftPriority, rightPriority := categoryPriority[findings[i].Category], categoryPriority[findings[j].Category]
		if leftPriority != rightPriority {
			return leftPriority < rightPriority
		}
		if findings[i].Score != findings[j].Score {
			return findings[i].Score > findings[j].Score
		}
		if findings[i].Dataset != findings[j].Dataset {
			return findings[i].Dataset < findings[j].Dataset
		}
		if findings[i].Source != findings[j].Source {
			return findings[i].Source < findings[j].Source
		}
		return findings[i].Name < findings[j].Name
	})
	for idx := range findings {
		findings[idx].Rank = idx + 1
	}
	return findings
}

// semanticPostgresPlan projects PostgreSQL operators and translator outcomes
// onto backend-neutral traversal stages.
func semanticPostgresPlan(record PlanRecord) SemanticPlan {
	plan := SemanticPlan{
		Driver:               record.Driver,
		PlanFingerprint:      record.PGPlanFingerprint,
		LogicalDirection:     logicalDirection(record.Cypher),
		PhysicalDirection:    postgresPhysicalDirection(record.PGPlan),
		PredicatePlacement:   postgresPredicatePlacement(record.PGPlan),
		EndpointBinding:      postgresEndpointBinding(record.PGPlan),
		OperatorFamily:       postgresOperatorFamily(record.PGPlan),
		RuntimeIdentityKnown: false,
		Error:                record.Error,
		RawOptimization:      record.Optimization,
	}
	accesses := postgresAccesses(record.PGPlan)
	if len(accesses) > 0 {
		plan.StartingAccess = accesses[0]
		plan.EstimatedSeeds = postgresRowsEstimate(accesses[0])
	}
	if len(accesses) > 1 {
		plan.TerminalAccess = accesses[1]
	}
	if len(record.PGPlan) > 0 {
		plan.EstimatedOutput = postgresRowsEstimate(record.PGPlan[0])
	}
	for _, line := range record.PGPlan {
		lower := strings.ToLower(line)
		if strings.Contains(line, "Recursive Union") || strings.Contains(lower, "shortest_path") {
			plan.EstimatedTraversal = postgresRowsEstimate(line)
		}
		if plan.EstimatedHydration == nil && (strings.Contains(lower, "hydrat") || strings.Contains(lower, "materializ")) {
			plan.EstimatedHydration = postgresRowsEstimate(line)
		}
	}
	plan.PlannedIdentity, plan.EmittedIdentity, plan.PlannedCandidates, plan.EmittedCandidates,
		plan.FallbackIdentity, plan.FallbackReason, plan.SelectorVersion, plan.ProbeCaps = postgresPlanIdentities(record.Optimization)
	return plan
}

// semanticNeo4jPlan projects the ordered Neo4j tree onto comparable stages.
func semanticNeo4jPlan(record PlanRecord) SemanticPlan {
	plan := SemanticPlan{
		Driver:               record.Driver,
		PlanFingerprint:      record.Neo4jPlanFingerprint,
		LogicalDirection:     logicalDirection(record.Cypher),
		PhysicalDirection:    neo4jPhysicalDirection(record.Neo4jPlan),
		PredicatePlacement:   neo4jPredicatePlacement(record.Neo4jPlan),
		EndpointBinding:      neo4jEndpointBinding(record.Neo4jPlan),
		OperatorFamily:       neo4jOperatorFamily(record.Neo4jPlan),
		RuntimeIdentityKnown: false,
		Error:                record.Error,
	}
	if record.Neo4jPlan == nil {
		return plan
	}
	leaves := neo4jLeaves(*record.Neo4jPlan)
	if len(leaves) > 0 {
		plan.StartingAccess = neo4jAccessLabel(leaves[0])
		plan.EstimatedSeeds = neo4jEstimatedRows(leaves[0])
		plan.ObservedSeedWork = neo4jNodeObservedWork(leaves[0])
	}
	if len(leaves) > 1 {
		plan.TerminalAccess = neo4jAccessLabel(leaves[1])
		plan.ObservedAlternativeSeedWork = neo4jNodeObservedWork(leaves[1])
	}
	plan.EstimatedOutput = neo4jEstimatedRows(*record.Neo4jPlan)
	plan.ActualOutput = record.Neo4jPlan.ActualRows
	plan.OutputQError = qError(plan.EstimatedOutput, plan.ActualOutput)
	var traversal *Neo4jPlanNode
	walkNeo4jPlan(*record.Neo4jPlan, func(node Neo4jPlanNode) {
		if traversal == nil && (strings.Contains(node.Operator, "Expand") || strings.Contains(node.Operator, "ShortestPath")) {
			copyNode := node
			traversal = &copyNode
		}
		lower := strings.ToLower(node.Operator + " " + node.Arguments["Details"])
		if strings.Contains(lower, "project") || strings.Contains(lower, "materializ") || strings.Contains(lower, "path") && !strings.Contains(lower, "shortestpath") {
			if plan.EstimatedHydration == nil {
				plan.EstimatedHydration = neo4jEstimatedRows(node)
			}
			if plan.ObservedHydrationRows == nil {
				plan.ObservedHydrationRows = node.ActualRows
			}
		}
	})
	if traversal != nil {
		plan.EstimatedTraversal = neo4jEstimatedRows(*traversal)
		plan.ObservedTraversalWork = traversal.DBHits
		if strings.Contains(traversal.Operator, "ShortestPath") {
			plan.InternalTraversalWork = "opaque"
		}
	}
	return plan
}

// postgresPlanIdentities returns selected/emitted identities, complete candidate sets, fallback, selector, and bounded probe caps.
func postgresPlanIdentities(optimization *translate.OptimizationSummary) (string, string, []string, []string, string, string, string, map[string]int64) {
	if optimization == nil {
		return "", "", nil, nil, "", "", "", nil
	}
	for _, outcome := range optimization.TargetOutcomes {
		if outcome.Family == "SP" || outcome.Family == "ASP" || strings.Contains(outcome.Family, "expansion") {
			caps := map[string]int64{}
			if outcome.ProbeCaps != nil {
				caps["root_rows"] = outcome.ProbeCaps.RootRowLimit
				caps["reverse_seed_rows"] = outcome.ProbeCaps.ReverseSeedRowLimit
				caps["directional_degree_rows"] = outcome.ProbeCaps.DirectionalDegreeRowLimit
				caps["survival_rows"] = outcome.ProbeCaps.SurvivalRowLimit
			}
			if outcome.StateLimit > 0 {
				caps["state_rows"] = outcome.StateLimit
			}
			if outcome.EndpointLimit > 0 {
				caps["endpoint_rows"] = outcome.EndpointLimit
			}
			for name, value := range caps {
				if value <= 0 {
					delete(caps, name)
				}
			}
			if len(caps) == 0 {
				caps = nil
			}
			return outcome.Selected, outcome.Applied,
				append([]string(nil), outcome.PlannedCandidates...), append([]string(nil), outcome.EmittedCandidates...),
				outcome.Fallback, outcome.SkipReason, outcome.SelectorVersion, caps
		}
	}
	return "", "", nil, nil, "", "", "", nil
}

// postgresAccesses returns leaf access lines in execution order.
func postgresAccesses(plan []string) []string {
	var accesses []string
	for idx := len(plan) - 1; idx >= 0; idx-- {
		line := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(plan[idx]), "->"))
		if strings.Contains(line, " Scan") && !strings.Contains(line, "CTE Scan") && !strings.Contains(line, "Subquery Scan") {
			accesses = append(accesses, line)
		}
	}
	return accesses
}

// postgresRowsEstimate extracts a planner row estimate from a text-plan line.
func postgresRowsEstimate(line string) *float64 {
	match := planRowsPattern.FindStringSubmatch(line)
	if len(match) != 2 {
		return nil
	}
	value, err := strconv.ParseFloat(match[1], 64)
	if err != nil {
		return nil
	}
	return &value
}

// postgresPhysicalDirection identifies the adjacency endpoint used by a plan.
func postgresPhysicalDirection(plan []string) string {
	joined := strings.ToLower(strings.Join(plan, "\n"))
	start, end := strings.Contains(joined, "start_id"), strings.Contains(joined, "end_id")
	switch {
	case start && end:
		return "mixed"
	case start:
		return "start_id"
	case end:
		return "end_id"
	default:
		return ""
	}
}

// postgresPredicatePlacement lists plan stages containing filters.
func postgresPredicatePlacement(plan []string) []string {
	var stages []string
	for _, line := range plan {
		trimmed := strings.TrimSpace(line)
		if strings.Contains(trimmed, "Filter:") || strings.Contains(trimmed, "Index Cond:") || strings.Contains(trimmed, "Join Filter:") {
			stages = append(stages, trimmed)
		}
	}
	return stages
}

// postgresEndpointBinding classifies evidence that a bound endpoint pair is materialized.
func postgresEndpointBinding(plan []string) string {
	joined := strings.ToLower(strings.Join(plan, "\n"))
	if strings.Contains(joined, "pair_filter") || strings.Contains(joined, "cartesian") {
		return "both_before_traversal"
	}
	if strings.Contains(joined, "terminal_filter") {
		return "terminal_before_traversal"
	}
	return ""
}

// postgresOperatorFamily classifies PostgreSQL traversal execution.
func postgresOperatorFamily(plan []string) string {
	joined := strings.ToLower(strings.Join(plan, "\n"))
	switch {
	case strings.Contains(joined, "all_shortest_paths"):
		return "all_shortest_paths"
	case strings.Contains(joined, "shortest_path"):
		return "shortest_path"
	case strings.Contains(joined, "recursive union"):
		return "ordinary_expand"
	case strings.Contains(joined, "edge"):
		return "fixed_hop"
	default:
		return ""
	}
}

// logicalDirection extracts the first directed relationship orientation.
func logicalDirection(cypherQuery string) string {
	compact := strings.ReplaceAll(cypherQuery, " ", "")
	switch {
	case strings.Contains(compact, "]->"):
		return "outbound"
	case strings.Contains(compact, "<-["):
		return "inbound"
	case strings.Contains(compact, "]-[") || strings.Contains(compact, "]-"):
		return "directionless"
	default:
		return ""
	}
}

// neo4jLeaves returns leaf operators in backend child order.
func neo4jLeaves(root Neo4jPlanNode) []Neo4jPlanNode {
	var leaves []Neo4jPlanNode
	walkNeo4jPlan(root, func(node Neo4jPlanNode) {
		if len(node.Children) == 0 {
			leaves = append(leaves, node)
		}
	})
	return leaves
}

// walkNeo4jPlan visits a plan in parent-before-child order while retaining backend child order.
func walkNeo4jPlan(root Neo4jPlanNode, visit func(Neo4jPlanNode)) {
	visit(root)
	for _, child := range root.Children {
		walkNeo4jPlan(child, visit)
	}
}

// neo4jAccessLabel renders an access operator with its stable details.
func neo4jAccessLabel(node Neo4jPlanNode) string {
	details := node.Arguments["Details"]
	if details == "" {
		return node.Operator
	}
	return node.Operator + ": " + details
}

// neo4jEstimatedRows returns an estimate from a typed field or stable argument.
func neo4jEstimatedRows(node Neo4jPlanNode) *float64 {
	if node.EstimatedRows != nil {
		return node.EstimatedRows
	}
	value, err := strconv.ParseFloat(node.Arguments["EstimatedRows"], 64)
	if err != nil {
		return nil
	}
	return &value
}

// neo4jPhysicalDirection classifies expansion direction from operator details.
func neo4jPhysicalDirection(root *Neo4jPlanNode) string {
	if root == nil {
		return ""
	}
	var directions []string
	walkNeo4jPlan(*root, func(node Neo4jPlanNode) {
		if !strings.Contains(node.Operator, "Expand") && !strings.Contains(node.Operator, "ShortestPath") {
			return
		}
		details := strings.ToLower(node.Arguments["Details"])
		switch {
		case strings.Contains(details, "incoming") || strings.Contains(details, "<-"):
			directions = append(directions, "incoming")
		case strings.Contains(details, "outgoing") || strings.Contains(details, "->"):
			directions = append(directions, "outgoing")
		}
	})
	if len(directions) == 0 {
		return ""
	}
	for _, direction := range directions[1:] {
		if direction != directions[0] {
			return "mixed"
		}
	}
	return directions[0]
}

// neo4jPredicatePlacement lists operators whose details expose predicates.
func neo4jPredicatePlacement(root *Neo4jPlanNode) []string {
	if root == nil {
		return nil
	}
	var stages []string
	walkNeo4jPlan(*root, func(node Neo4jPlanNode) {
		if strings.Contains(node.Operator, "Filter") || strings.Contains(node.Operator, "Seek") {
			stages = append(stages, neo4jAccessLabel(node))
		}
	})
	return stages
}

// neo4jEndpointBinding recognizes the pair-producing plan boundary.
func neo4jEndpointBinding(root *Neo4jPlanNode) string {
	if root == nil {
		return ""
	}
	bound := ""
	walkNeo4jPlan(*root, func(node Neo4jPlanNode) {
		if strings.Contains(node.Operator, "CartesianProduct") || strings.Contains(node.Operator, "Apply") {
			bound = "both_before_traversal"
		}
	})
	return bound
}

// neo4jOperatorFamily classifies Neo4j traversal operators.
func neo4jOperatorFamily(root *Neo4jPlanNode) string {
	if root == nil {
		return ""
	}
	family := ""
	walkNeo4jPlan(*root, func(node Neo4jPlanNode) {
		switch {
		case strings.Contains(node.Operator, "ShortestPath"):
			family = "shortest_path"
		case family == "" && strings.Contains(node.Operator, "VarLengthExpand"):
			family = "ordinary_expand"
		case family == "" && strings.Contains(node.Operator, "Expand"):
			family = "fixed_hop"
		}
	})
	return family
}

// qError returns symmetric estimate error when both values are positive.
func qError(estimated *float64, actual *int64) *float64 {
	if estimated == nil || actual == nil || *estimated <= 0 || *actual <= 0 {
		return nil
	}
	value := math.Max(*estimated/float64(*actual), float64(*actual)/(*estimated))
	return &value
}

// writePlanDeltaReport writes one indented, newline-terminated paired report.
func writePlanDeltaReport(path string, report PlanDeltaReport) error {
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0o644); err != nil {
		return fmt.Errorf("write plan delta %s: %w", path, err)
	}
	return nil
}
