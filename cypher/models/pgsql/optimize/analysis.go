package optimize

import (
	"fmt"
	"sort"
	"strings"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/walk"
)

type QueryPartKind string

const (
	QueryPartKindSingle QueryPartKind = "single"
	QueryPartKindMulti  QueryPartKind = "multi"
)

type BarrierKind string

const (
	BarrierKindReturn        BarrierKind = "return"
	BarrierKindWith          BarrierKind = "with"
	BarrierKindUnwind        BarrierKind = "unwind"
	BarrierKindOptionalMatch BarrierKind = "optional_match"
	BarrierKindUpdate        BarrierKind = "update"
)

type BindingKind string

const (
	BindingKindNode         BindingKind = "node"
	BindingKindRelationship BindingKind = "relationship"
	BindingKindPath         BindingKind = "path"
)

type Analysis struct {
	QueryParts []QueryPart
}

type QueryPart struct {
	Index                  int
	Kind                   QueryPartKind
	Regions                []Region
	Barriers               []Barrier
	ProjectionDependencies []string
}

type Region struct {
	QueryPartIndex int
	StartClause    int
	EndClause      int
	Clauses        []MatchClause
	Bindings       []Binding
	PathVariables  []PathVariable
	Predicates     []Predicate
}

type MatchClause struct {
	Index          int
	PatternCount   int
	WherePredicate int
}

type Barrier struct {
	QueryPartIndex int
	ClauseIndex    int
	Kind           BarrierKind
	Dependencies   []string
}

type Binding struct {
	Symbol       string
	Kind         BindingKind
	ClauseIndex  int
	PatternIndex int
}

type PathVariable struct {
	Symbol            string
	ClauseIndex       int
	PatternIndex      int
	NodeCount         int
	RelationshipCount int
	VariableLength    bool
	Dependencies      []string
}

type Predicate struct {
	ClauseIndex     int
	ExpressionIndex int
	Dependencies    []string
}

func Analyze(query *cypher.RegularQuery) Analysis {
	if query == nil || query.SingleQuery == nil {
		return Analysis{}
	}

	if query.SingleQuery.MultiPartQuery != nil {
		return analyzeMultiPartQuery(query.SingleQuery.MultiPartQuery)
	}

	if query.SingleQuery.SinglePartQuery != nil {
		return Analysis{
			QueryParts: []QueryPart{
				analyzeSinglePartQuery(0, QueryPartKindSingle, query.SingleQuery.SinglePartQuery),
			},
		}
	}

	return Analysis{}
}

func (s Analysis) Diagnostics() []string {
	var lines []string

	for _, queryPart := range s.QueryParts {
		lines = append(lines, fmt.Sprintf(
			"query_part[%d] kind=%s projection_deps=%s",
			queryPart.Index,
			queryPart.Kind,
			strings.Join(queryPart.ProjectionDependencies, ","),
		))

		for regionIndex, region := range queryPart.Regions {
			lines = append(lines, fmt.Sprintf(
				"region[%d] part=%d clauses=%d..%d matches=%d bindings=%s paths=%s predicates=%s",
				regionIndex,
				region.QueryPartIndex,
				region.StartClause,
				region.EndClause,
				len(region.Clauses),
				formatBindings(region.Bindings),
				formatPathVariables(region.PathVariables),
				formatPredicates(region.Predicates),
			))
		}

		for barrierIndex, barrier := range queryPart.Barriers {
			lines = append(lines, fmt.Sprintf(
				"barrier[%d] part=%d clause=%d kind=%s deps=%s",
				barrierIndex,
				barrier.QueryPartIndex,
				barrier.ClauseIndex,
				barrier.Kind,
				strings.Join(barrier.Dependencies, ","),
			))
		}
	}

	return lines
}

func (s Analysis) String() string {
	return strings.Join(s.Diagnostics(), "\n")
}

func analyzeMultiPartQuery(query *cypher.MultiPartQuery) Analysis {
	var analysis Analysis

	for idx, part := range query.Parts {
		analysis.QueryParts = append(analysis.QueryParts, analyzeMultiPartQueryPart(idx, part))
	}

	if query.SinglePartQuery != nil {
		analysis.QueryParts = append(analysis.QueryParts, analyzeSinglePartQuery(len(query.Parts), QueryPartKindSingle, query.SinglePartQuery))
	}

	return analysis
}

func analyzeMultiPartQueryPart(index int, part *cypher.MultiPartQueryPart) QueryPart {
	queryPart := QueryPart{
		Index: index,
		Kind:  QueryPartKindMulti,
	}

	queryPart.Regions, queryPart.Barriers = analyzeReadingClauses(index, part.ReadingClauses)

	if len(part.UpdatingClauses) > 0 {
		queryPart.Barriers = append(queryPart.Barriers, Barrier{
			QueryPartIndex: index,
			ClauseIndex:    len(part.ReadingClauses),
			Kind:           BarrierKindUpdate,
		})
	}

	if part.With != nil {
		queryPart.ProjectionDependencies = projectionDependencies(part.With.Projection)
		queryPart.Barriers = append(queryPart.Barriers, Barrier{
			QueryPartIndex: index,
			ClauseIndex:    len(part.ReadingClauses) + len(part.UpdatingClauses),
			Kind:           BarrierKindWith,
			Dependencies:   queryPart.ProjectionDependencies,
		})
	}

	return queryPart
}

func analyzeSinglePartQuery(index int, kind QueryPartKind, part *cypher.SinglePartQuery) QueryPart {
	queryPart := QueryPart{
		Index: index,
		Kind:  kind,
	}

	queryPart.Regions, queryPart.Barriers = analyzeReadingClauses(index, part.ReadingClauses)

	if len(part.UpdatingClauses) > 0 {
		queryPart.Barriers = append(queryPart.Barriers, Barrier{
			QueryPartIndex: index,
			ClauseIndex:    len(part.ReadingClauses),
			Kind:           BarrierKindUpdate,
		})
	}

	if part.Return != nil {
		queryPart.ProjectionDependencies = projectionDependencies(part.Return.Projection)
		queryPart.Barriers = append(queryPart.Barriers, Barrier{
			QueryPartIndex: index,
			ClauseIndex:    len(part.ReadingClauses) + len(part.UpdatingClauses),
			Kind:           BarrierKindReturn,
			Dependencies:   queryPart.ProjectionDependencies,
		})
	}

	return queryPart
}

func analyzeReadingClauses(queryPartIndex int, readingClauses []*cypher.ReadingClause) ([]Region, []Barrier) {
	var (
		regions       []Region
		barriers      []Barrier
		currentRegion *Region
	)

	closeRegion := func() {
		if currentRegion != nil {
			regions = append(regions, *currentRegion)
			currentRegion = nil
		}
	}

	for clauseIndex, readingClause := range readingClauses {
		if readingClause == nil || readingClause.Unwind != nil {
			closeRegion()
			barriers = append(barriers, Barrier{
				QueryPartIndex: queryPartIndex,
				ClauseIndex:    clauseIndex,
				Kind:           BarrierKindUnwind,
				Dependencies:   dependenciesForReadingClause(readingClause),
			})
			continue
		}

		match := readingClause.Match
		if match == nil {
			continue
		}

		if match.Optional {
			closeRegion()
			barriers = append(barriers, Barrier{
				QueryPartIndex: queryPartIndex,
				ClauseIndex:    clauseIndex,
				Kind:           BarrierKindOptionalMatch,
				Dependencies:   dependenciesForMatch(match),
			})
			continue
		}

		if currentRegion == nil {
			currentRegion = &Region{
				QueryPartIndex: queryPartIndex,
				StartClause:    clauseIndex,
				EndClause:      clauseIndex,
			}
		}

		currentRegion.EndClause = clauseIndex
		currentRegion.Clauses = append(currentRegion.Clauses, MatchClause{
			Index:          clauseIndex,
			PatternCount:   len(match.Pattern),
			WherePredicate: wherePredicateCount(match.Where),
		})
		currentRegion.Bindings = mergeBindings(currentRegion.Bindings, bindingsForMatch(clauseIndex, match))
		currentRegion.PathVariables = mergePathVariables(currentRegion.PathVariables, pathVariablesForMatch(clauseIndex, match))
		currentRegion.Predicates = append(currentRegion.Predicates, predicatesForWhere(clauseIndex, match.Where)...)
	}

	closeRegion()

	return regions, barriers
}

func dependenciesForReadingClause(readingClause *cypher.ReadingClause) []string {
	if readingClause == nil {
		return nil
	}

	if readingClause.Match != nil {
		return dependenciesForMatch(readingClause.Match)
	}

	if readingClause.Unwind != nil {
		return sortedDependencies(readingClause.Unwind.Expression)
	}

	return nil
}

func dependenciesForMatch(match *cypher.Match) []string {
	var deps []string

	if match == nil {
		return nil
	}

	for _, predicate := range predicatesForWhere(0, match.Where) {
		deps = append(deps, predicate.Dependencies...)
	}

	return sortedUniqueStrings(deps)
}

func bindingsForMatch(clauseIndex int, match *cypher.Match) []Binding {
	var bindings []Binding

	for patternIndex, pattern := range match.Pattern {
		if pattern == nil {
			continue
		}

		if pattern.Variable != nil && pattern.Variable.Symbol != "" {
			bindings = append(bindings, Binding{
				Symbol:       pattern.Variable.Symbol,
				Kind:         BindingKindPath,
				ClauseIndex:  clauseIndex,
				PatternIndex: patternIndex,
			})
		}

		for _, element := range pattern.PatternElements {
			if nodePattern, isNodePattern := element.AsNodePattern(); isNodePattern {
				if nodePattern.Variable != nil && nodePattern.Variable.Symbol != "" {
					bindings = append(bindings, Binding{
						Symbol:       nodePattern.Variable.Symbol,
						Kind:         BindingKindNode,
						ClauseIndex:  clauseIndex,
						PatternIndex: patternIndex,
					})
				}
			} else if relationshipPattern, isRelationshipPattern := element.AsRelationshipPattern(); isRelationshipPattern {
				if relationshipPattern.Variable != nil && relationshipPattern.Variable.Symbol != "" {
					bindings = append(bindings, Binding{
						Symbol:       relationshipPattern.Variable.Symbol,
						Kind:         BindingKindRelationship,
						ClauseIndex:  clauseIndex,
						PatternIndex: patternIndex,
					})
				}
			}
		}
	}

	return bindings
}

func pathVariablesForMatch(clauseIndex int, match *cypher.Match) []PathVariable {
	var pathVariables []PathVariable

	for patternIndex, pattern := range match.Pattern {
		if pattern == nil || pattern.Variable == nil || pattern.Variable.Symbol == "" {
			continue
		}

		pathVariable := PathVariable{
			Symbol:       pattern.Variable.Symbol,
			ClauseIndex:  clauseIndex,
			PatternIndex: patternIndex,
			Dependencies: patternDependencies(pattern),
		}

		for _, element := range pattern.PatternElements {
			if element.IsNodePattern() {
				pathVariable.NodeCount++
			} else if relationshipPattern, isRelationshipPattern := element.AsRelationshipPattern(); isRelationshipPattern {
				pathVariable.RelationshipCount++

				if relationshipPattern.Range != nil {
					pathVariable.VariableLength = true
				}
			}
		}

		pathVariables = append(pathVariables, pathVariable)
	}

	return pathVariables
}

func patternDependencies(pattern *cypher.PatternPart) []string {
	var dependencies []string

	for _, element := range pattern.PatternElements {
		if nodePattern, isNodePattern := element.AsNodePattern(); isNodePattern {
			if nodePattern.Variable != nil && nodePattern.Variable.Symbol != "" {
				dependencies = append(dependencies, nodePattern.Variable.Symbol)
			}
		} else if relationshipPattern, isRelationshipPattern := element.AsRelationshipPattern(); isRelationshipPattern {
			if relationshipPattern.Variable != nil && relationshipPattern.Variable.Symbol != "" {
				dependencies = append(dependencies, relationshipPattern.Variable.Symbol)
			}
		}
	}

	return sortedUniqueStrings(dependencies)
}

func predicatesForWhere(clauseIndex int, where *cypher.Where) []Predicate {
	if where == nil {
		return nil
	}

	var predicates []Predicate
	for expressionIndex, expression := range where.GetAll() {
		predicates = append(predicates, Predicate{
			ClauseIndex:     clauseIndex,
			ExpressionIndex: expressionIndex,
			Dependencies:    sortedDependencies(expression),
		})
	}

	return predicates
}

func projectionDependencies(projection *cypher.Projection) []string {
	if projection == nil {
		return nil
	}

	var dependencies []string
	for _, item := range projection.Items {
		dependencies = append(dependencies, sortedDependencies(item)...)
	}

	if projection.Order != nil {
		dependencies = append(dependencies, sortedDependencies(projection.Order)...)
	}

	if projection.Skip != nil {
		dependencies = append(dependencies, sortedDependencies(projection.Skip)...)
	}

	if projection.Limit != nil {
		dependencies = append(dependencies, sortedDependencies(projection.Limit)...)
	}

	return sortedUniqueStrings(dependencies)
}

func sortedDependencies(node cypher.SyntaxNode) []string {
	dependencies := map[string]struct{}{}

	if node == nil {
		return nil
	}

	_ = walk.Cypher(node, walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, _ walk.VisitorHandler) {
		if variable, isVariable := node.(*cypher.Variable); isVariable && variable.Symbol != "" && variable.Symbol != cypher.TokenLiteralAsterisk {
			dependencies[variable.Symbol] = struct{}{}
		}
	}))

	return sortedMapKeys(dependencies)
}

func wherePredicateCount(where *cypher.Where) int {
	if where == nil {
		return 0
	}

	return where.Len()
}

func mergeBindings(existing []Binding, next []Binding) []Binding {
	seen := map[string]struct{}{}
	for _, binding := range existing {
		seen[bindingKey(binding)] = struct{}{}
	}

	for _, binding := range next {
		key := bindingKey(binding)
		if _, hasBinding := seen[key]; hasBinding {
			continue
		}

		existing = append(existing, binding)
		seen[key] = struct{}{}
	}

	return existing
}

func mergePathVariables(existing []PathVariable, next []PathVariable) []PathVariable {
	seen := map[string]struct{}{}
	for _, pathVariable := range existing {
		seen[pathVariable.Symbol] = struct{}{}
	}

	for _, pathVariable := range next {
		if _, hasPathVariable := seen[pathVariable.Symbol]; hasPathVariable {
			continue
		}

		existing = append(existing, pathVariable)
		seen[pathVariable.Symbol] = struct{}{}
	}

	return existing
}

func bindingKey(binding Binding) string {
	return string(binding.Kind) + ":" + binding.Symbol
}

func sortedMapKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	return keys
}

func sortedUniqueStrings(values []string) []string {
	seen := map[string]struct{}{}

	for _, value := range values {
		if value != "" {
			seen[value] = struct{}{}
		}
	}

	return sortedMapKeys(seen)
}

func formatBindings(bindings []Binding) string {
	if len(bindings) == 0 {
		return ""
	}

	items := make([]string, 0, len(bindings))
	for _, binding := range bindings {
		items = append(items, fmt.Sprintf("%s:%s", binding.Symbol, binding.Kind))
	}

	return strings.Join(items, ",")
}

func formatPathVariables(pathVariables []PathVariable) string {
	if len(pathVariables) == 0 {
		return ""
	}

	items := make([]string, 0, len(pathVariables))
	for _, pathVariable := range pathVariables {
		items = append(items, pathVariable.Symbol)
	}

	return strings.Join(items, ",")
}

func formatPredicates(predicates []Predicate) string {
	if len(predicates) == 0 {
		return ""
	}

	items := make([]string, 0, len(predicates))
	for _, predicate := range predicates {
		items = append(items, strings.Join(predicate.Dependencies, "|"))
	}

	return strings.Join(items, ",")
}
