package optimize

import "github.com/specterops/dawgs/cypher/models/cypher"

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
	clause       *cypher.ReadingClause
	declarations map[string]struct{}
	dependencies map[string]struct{}
	movable      bool
	score        boundSourceSelectivity
	index        int
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

		applied = reorderRegion(readingClauses[region.StartClause:region.EndClause+1], declaredBeforeClause(readingClauses, region.StartClause)) || applied
	}

	return applied
}

func reorderRegion(regionClauses []*cypher.ReadingClause, declaredBeforeRegion map[string]struct{}) bool {
	candidates := reorderCandidates(regionClauses, declaredBeforeRegion)

	reordered := reorderCandidateSegments(candidates, declaredBeforeRegion)

	var applied bool
	for idx, candidate := range reordered {
		if regionClauses[idx] != candidate.clause {
			applied = true
			regionClauses[idx] = candidate.clause
		}
	}

	return applied
}

func declaredBeforeClause(readingClauses []*cypher.ReadingClause, clauseIndex int) map[string]struct{} {
	declared := map[string]struct{}{}
	for idx := 0; idx < clauseIndex && idx < len(readingClauses); idx++ {
		for _, binding := range bindingsForReadingClause(idx, readingClauses[idx]) {
			declared[binding.Symbol] = struct{}{}
		}
	}

	return declared
}

func reorderCandidates(regionClauses []*cypher.ReadingClause, declaredBeforeRegion map[string]struct{}) []reorderCandidate {
	var (
		candidates       = make([]reorderCandidate, len(regionClauses))
		firstDeclaration = copyStringSet(declaredBeforeRegion)
		regionSymbols    = map[string]struct{}{}
	)

	for idx, clause := range regionClauses {
		for _, binding := range bindingsForReadingClause(idx, clause) {
			regionSymbols[binding.Symbol] = struct{}{}
		}
	}

	for idx, clause := range regionClauses {
		var (
			declarations = map[string]struct{}{}
			dependencies = map[string]struct{}{}
		)

		for _, binding := range bindingsForReadingClause(idx, clause) {
			if _, declared := firstDeclaration[binding.Symbol]; declared {
				dependencies[binding.Symbol] = struct{}{}
				continue
			}

			firstDeclaration[binding.Symbol] = struct{}{}
			declarations[binding.Symbol] = struct{}{}
		}

		var match *cypher.Match
		if clause != nil {
			match = clause.Match
		}

		for _, dependency := range localMatchDependencies(match) {
			dependencies[dependency] = struct{}{}
		}

		movable := true
		for dependency := range dependencies {
			if _, declaredBefore := declaredBeforeRegion[dependency]; declaredBefore {
				continue
			}
			if _, declaredInRegion := regionSymbols[dependency]; declaredInRegion {
				continue
			}

			movable = false
			break
		}

		candidates[idx] = reorderCandidate{
			clause:       clause,
			declarations: declarations,
			dependencies: dependencies,
			movable:      movable,
			score:        matchClauseSelectivity(clause),
			index:        idx,
		}
	}

	return candidates
}

func reorderCandidateSegments(candidates []reorderCandidate, declaredBeforeRegion map[string]struct{}) []reorderCandidate {
	var (
		reordered = make([]reorderCandidate, 0, len(candidates))
		available = copyStringSet(declaredBeforeRegion)
		segment   []reorderCandidate
	)

	flushSegment := func() {
		if len(segment) == 0 {
			return
		}

		nextSegment := reorderMovableCandidates(segment, available)
		for _, candidate := range nextSegment {
			mergeStringSet(available, candidate.declarations)
		}

		reordered = append(reordered, nextSegment...)
		segment = nil
	}

	for _, candidate := range candidates {
		if !candidate.movable {
			flushSegment()
			reordered = append(reordered, candidate)
			mergeStringSet(available, candidate.declarations)
			continue
		}

		segment = append(segment, candidate)
	}

	flushSegment()
	return reordered
}

func reorderMovableCandidates(candidates []reorderCandidate, available map[string]struct{}) []reorderCandidate {
	var (
		reordered = make([]reorderCandidate, 0, len(candidates))
		remaining = append([]reorderCandidate(nil), candidates...)
	)

	for len(remaining) > 0 {
		nextIndex := bestSchedulableCandidateIndex(remaining, available)
		if nextIndex < 0 {
			reordered = append(reordered, remaining...)
			break
		}

		next := remaining[nextIndex]
		reordered = append(reordered, next)
		mergeStringSet(available, next.declarations)
		remaining = append(remaining[:nextIndex], remaining[nextIndex+1:]...)
	}

	return reordered
}

func bestSchedulableCandidateIndex(candidates []reorderCandidate, available map[string]struct{}) int {
	bestIndex := -1
	for idx, candidate := range candidates {
		if !candidateDependenciesSatisfied(candidate, available) {
			continue
		}

		if bestIndex < 0 ||
			candidate.score > candidates[bestIndex].score ||
			(candidate.score == candidates[bestIndex].score && candidate.index < candidates[bestIndex].index) {
			bestIndex = idx
		}
	}

	return bestIndex
}

func candidateDependenciesSatisfied(candidate reorderCandidate, available map[string]struct{}) bool {
	for dependency := range candidate.dependencies {
		if _, declared := available[dependency]; declared {
			continue
		}
		if _, declaredByCandidate := candidate.declarations[dependency]; declaredByCandidate {
			continue
		}

		return false
	}

	return true
}

func matchClauseSelectivity(readingClause *cypher.ReadingClause) boundSourceSelectivity {
	if readingClause == nil || readingClause.Match == nil {
		return boundSourceSelectivityNone
	}

	var selectivity boundSourceSelectivity
	for _, patternPart := range readingClause.Match.Pattern {
		for _, nodePattern := range nodePatternsForPattern(patternPart) {
			mergeSelectivityValue(&selectivity, nodePatternSelectivity(nodePattern, false))
		}

		for _, step := range traversalStepsForPattern(patternPart) {
			if step.Relationship == nil {
				continue
			}

			if len(step.Relationship.Kinds) > 0 {
				mergeSelectivityValue(&selectivity, boundSourceSelectivityKindOnly)
			}
			mergeSelectivityValue(&selectivity, propertyConstraintSelectivity(step.Relationship.Properties))
		}
	}

	if readingClause.Match.Where != nil {
		for _, expression := range readingClause.Match.Where.Expressions {
			for _, term := range cypherConjunctionTerms(expression) {
				if _, termSelectivity, ok := propertyPredicateSelectivity(term); ok {
					mergeSelectivityValue(&selectivity, termSelectivity)
				} else {
					mergeSelectivityValue(&selectivity, boundSourceSelectivityPredicate)
				}
			}
		}
	}

	return selectivity
}

func singleNodePattern(pattern *cypher.PatternPart) (*cypher.NodePattern, bool) {
	if pattern == nil || pattern.Variable != nil || len(pattern.PatternElements) != 1 {
		return nil, false
	}

	return pattern.PatternElements[0].AsNodePattern()
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

func bindingsForReadingClause(clauseIndex int, readingClause *cypher.ReadingClause) []Binding {
	if readingClause == nil || readingClause.Match == nil {
		return nil
	}

	return bindingsForMatch(clauseIndex, readingClause.Match)
}

func mergeStringSet(dst map[string]struct{}, src map[string]struct{}) {
	for value := range src {
		dst[value] = struct{}{}
	}
}
