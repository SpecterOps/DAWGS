package optimize

import (
	"sort"

	"github.com/specterops/dawgs/cypher/models/cypher"
)

type ConservativePatternReorderingRule struct{}

func (s ConservativePatternReorderingRule) Name() string {
	return "ConservativePatternReordering"
}

func (s ConservativePatternReorderingRule) Apply(plan *Plan) (bool, error) {
	if plan == nil || plan.Query == nil || plan.Query.SingleQuery == nil {
		return false, nil
	}

	if plan.Query.SingleQuery.MultiPartQuery != nil {
		return reorderMultiPartQuery(plan.Query.SingleQuery.MultiPartQuery, plan.Analysis), nil
	}

	if plan.Query.SingleQuery.SinglePartQuery != nil {
		return reorderSinglePartQuery(plan.Query.SingleQuery.SinglePartQuery, plan.Analysis), nil
	}

	return false, nil
}

type reorderCandidate struct {
	clause *cypher.ReadingClause
	rank   int
	index  int
}

func reorderMultiPartQuery(query *cypher.MultiPartQuery, analysis Analysis) bool {
	var applied bool

	for partIndex, part := range query.Parts {
		if part == nil {
			continue
		}

		if queryPart, ok := analysisQueryPart(analysis, partIndex); ok {
			applied = reorderReadingClauses(part.ReadingClauses, queryPart.Regions) || applied
		}
	}

	if query.SinglePartQuery != nil {
		if queryPart, ok := analysisQueryPart(analysis, len(query.Parts)); ok {
			applied = reorderReadingClauses(query.SinglePartQuery.ReadingClauses, queryPart.Regions) || applied
		}
	}

	return applied
}

func reorderSinglePartQuery(query *cypher.SinglePartQuery, analysis Analysis) bool {
	if queryPart, ok := analysisQueryPart(analysis, 0); ok {
		return reorderReadingClauses(query.ReadingClauses, queryPart.Regions)
	}

	return false
}

func analysisQueryPart(analysis Analysis, index int) (QueryPart, bool) {
	for _, queryPart := range analysis.QueryParts {
		if queryPart.Index == index {
			return queryPart, true
		}
	}

	return QueryPart{}, false
}

func reorderReadingClauses(readingClauses []*cypher.ReadingClause, regions []Region) bool {
	var applied bool

	for _, region := range regions {
		if region.StartClause < 0 || region.EndClause >= len(readingClauses) || region.StartClause >= region.EndClause {
			continue
		}

		applied = reorderRegion(readingClauses[region.StartClause:region.EndClause+1]) || applied
	}

	return applied
}

func reorderRegion(regionClauses []*cypher.ReadingClause) bool {
	candidates := make([]reorderCandidate, len(regionClauses))
	declaredBefore := map[string]struct{}{}

	for idx, clause := range regionClauses {
		candidates[idx] = reorderCandidate{
			clause: clause,
			rank:   matchClauseRank(clause, declaredBefore),
			index:  idx,
		}

		for _, binding := range bindingsForReadingClause(idx, clause) {
			declaredBefore[binding.Symbol] = struct{}{}
		}
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].rank < candidates[j].rank
	})

	var applied bool
	for idx, candidate := range candidates {
		if regionClauses[idx] != candidate.clause {
			applied = true
			regionClauses[idx] = candidate.clause
		}
	}

	return applied
}

func matchClauseRank(readingClause *cypher.ReadingClause, declaredBefore map[string]struct{}) int {
	if isIndependentNodeAnchor(readingClause, declaredBefore) {
		return 0
	}

	return 1
}

func isIndependentNodeAnchor(readingClause *cypher.ReadingClause, declaredBefore map[string]struct{}) bool {
	if readingClause == nil || readingClause.Match == nil {
		return false
	}

	match := readingClause.Match
	if match.Optional || len(match.Pattern) != 1 {
		return false
	}

	nodePattern, ok := singleNodePattern(match.Pattern[0])
	if !ok || nodePattern.Variable == nil || nodePattern.Variable.Symbol == "" {
		return false
	}

	if _, alreadyDeclared := declaredBefore[nodePattern.Variable.Symbol]; alreadyDeclared {
		return false
	}

	if !isSelectiveNodeAnchor(nodePattern, match.Where) {
		return false
	}

	declared := bindingSymbolSet(bindingsForMatch(0, match))
	for _, dependency := range localMatchDependencies(match) {
		if _, isLocal := declared[dependency]; !isLocal {
			return false
		}
	}

	return true
}

func singleNodePattern(pattern *cypher.PatternPart) (*cypher.NodePattern, bool) {
	if pattern == nil || pattern.Variable != nil || len(pattern.PatternElements) != 1 {
		return nil, false
	}

	return pattern.PatternElements[0].AsNodePattern()
}

func isSelectiveNodeAnchor(nodePattern *cypher.NodePattern, where *cypher.Where) bool {
	return len(nodePattern.Kinds) > 0 || nodePattern.Properties != nil || wherePredicateCount(where) > 0
}

func localMatchDependencies(match *cypher.Match) []string {
	if match == nil {
		return nil
	}

	var dependencies []string
	for _, pattern := range match.Pattern {
		if pattern == nil {
			continue
		}

		for _, element := range pattern.PatternElements {
			if element == nil {
				continue
			}

			if nodePattern, ok := element.AsNodePattern(); ok {
				dependencies = append(dependencies, sortedDependencies(nodePattern.Properties)...)
			} else if relationshipPattern, ok := element.AsRelationshipPattern(); ok {
				dependencies = append(dependencies, sortedDependencies(relationshipPattern.Properties)...)
			}
		}
	}

	dependencies = append(dependencies, dependenciesForMatch(match)...)
	return sortedUniqueStrings(dependencies)
}

func bindingSymbolSet(bindings []Binding) map[string]struct{} {
	symbols := make(map[string]struct{}, len(bindings))
	for _, binding := range bindings {
		symbols[binding.Symbol] = struct{}{}
	}

	return symbols
}

func bindingsForReadingClause(clauseIndex int, readingClause *cypher.ReadingClause) []Binding {
	if readingClause == nil || readingClause.Match == nil {
		return nil
	}

	return bindingsForMatch(clauseIndex, readingClause.Match)
}
