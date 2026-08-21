package optimize

import (
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
)

// InboundTraversalReversalRule reverses a qualifying multi-step traversal pattern so that the
// search is driven from the more selective terminal endpoint inward, rather than expanding
// outward from an unconstrained source through a leading unbounded variable-length expansion.
//
// A pattern such as:
//
//	MATCH p = (s:User)-[:MemberOf*0..]->(:Group)-[:AdminTo]->(d:Computer)
//	WHERE s.samaccountname =~ '(?i).*[ge]$' AND d.operatingsystem CONTAINS "WINDOWS SERVER"
//	RETURN p
//
// is rewritten to drive from d:Computer inbound toward s:User by reversing the pattern element
// order and each relationship direction. The reversed pattern is flagged via
// PatternPart.PathDirectionReversed so path materialization can restore the original
// left-to-right order for RETURN p.
type InboundTraversalReversalRule struct{}

func (s InboundTraversalReversalRule) Name() string {
	return "InboundTraversalReversal"
}

func (s InboundTraversalReversalRule) Apply(plan *Plan) (bool, error) {
	if plan == nil || plan.Query == nil || plan.Query.SingleQuery == nil {
		return false, nil
	}

	singleQuery := plan.Query.SingleQuery

	switch {
	case singleQuery.SinglePartQuery != nil:
		return reverseInboundTraversalSinglePartQuery(singleQuery.SinglePartQuery, map[string]struct{}{}), nil
	case singleQuery.MultiPartQuery != nil:
		return reverseInboundTraversalMultiPartQuery(singleQuery.MultiPartQuery), nil
	default:
		return false, nil
	}
}

// reverseInboundTraversalMultiPartQuery processes each single-part segment of a multi-part query,
// including traversals that follow a WITH projection, while carrying the symbols bound by earlier
// segments forward so a reversal is never applied to a source endpoint provided by a prior segment.
func reverseInboundTraversalMultiPartQuery(query *cypher.MultiPartQuery) bool {
	if query == nil {
		return false
	}

	var (
		applied         bool
		declaredSymbols = map[string]struct{}{}
	)

	for _, part := range query.Parts {
		if part == nil {
			continue
		}

		if reverseInboundTraversalReadingClauses(part.ReadingClauses, declaredSymbols) {
			applied = true
		}

		declareReadingClauseSymbols(declaredSymbols, part.ReadingClauses)

		if part.With != nil {
			declaredSymbols, _ = carryProjectionSelectivity(part.With.Projection, declaredSymbols, map[string]boundSourceSelectivity{})
		}
	}

	if finalPart := query.SinglePartQuery; finalPart != nil {
		if reverseInboundTraversalSinglePartQuery(finalPart, declaredSymbols) {
			applied = true
		}
	}

	return applied
}

func reverseInboundTraversalSinglePartQuery(query *cypher.SinglePartQuery, declaredSymbols map[string]struct{}) bool {
	if query == nil {
		return false
	}

	return reverseInboundTraversalReadingClauses(query.ReadingClauses, declaredSymbols)
}

func reverseInboundTraversalReadingClauses(readingClauses []*cypher.ReadingClause, declaredSymbols map[string]struct{}) bool {
	var applied bool

	for _, readingClause := range readingClauses {
		if readingClause == nil || readingClause.Match == nil {
			continue
		}

		match := readingClause.Match
		if !match.Optional {
			searchSymbols := whereSearchPredicateSymbols(match)

			for _, patternPart := range match.Pattern {
				if reverseInboundTraversalPatternPart(patternPart, declaredSymbols, searchSymbols) {
					applied = true
				}
			}
		}

		declareMatchSymbols(declaredSymbols, match)
	}

	return applied
}

// reverseInboundTraversalPatternPart reverses a single pattern part if it qualifies. A pattern
// qualifies when it is a non-shortest-path traversal with more than one step, a leading
// unbounded variable-length expansion with a concrete direction whose source endpoint is neither
// externally bound nor more selective than the constrained terminal endpoint.
func reverseInboundTraversalPatternPart(patternPart *cypher.PatternPart, declaredSymbols, searchSymbols map[string]struct{}) bool {
	if !inboundTraversalReversalCandidate(patternPart, declaredSymbols, searchSymbols) {
		return false
	}

	reversePatternElements(patternPart)
	patternPart.PathDirectionReversed = !patternPart.PathDirectionReversed
	return true
}

func inboundTraversalReversalCandidate(patternPart *cypher.PatternPart, declaredSymbols, searchSymbols map[string]struct{}) bool {
	if patternPart == nil ||
		patternPart.ShortestPathPattern ||
		patternPart.AllShortestPathsPattern {
		return false
	}

	steps := traversalStepsForPattern(patternPart)
	if len(steps) < 2 {
		return false
	}

	leadingStep := steps[0]
	if leadingStep.Relationship == nil ||
		leadingStep.Relationship.Range == nil ||
		leadingStep.Relationship.Range.EndIndex != nil ||
		leadingStep.Relationship.Direction == graph.DirectionBoth ||
		leadingStep.Relationship.Variable != nil {
		return false
	}

	var (
		sourceNode   = steps[0].LeftNode
		terminalNode = steps[len(steps)-1].RightNode
		sourceSymbol = variableSymbol(sourceNode.Variable)
		terminalSym  = variableSymbol(terminalNode.Variable)
	)

	// The source endpoint must not be bound by a prior clause; reversing would break the
	// established drive order for an externally provided source.
	if sourceSymbol != "" {
		if _, bound := declaredSymbols[sourceSymbol]; bound {
			return false
		}
	}

	// The terminal endpoint must carry a search constraint to make anchoring there worthwhile.
	if !endpointHasSearchConstraint(terminalNode, terminalSym, searchSymbols) {
		return false
	}

	var (
		sourceSelectivity   = endpointSelectivity(sourceNode, sourceSymbol, searchSymbols)
		terminalSelectivity = endpointSelectivity(terminalNode, terminalSym, searchSymbols)
	)

	// Only reverse when the terminal endpoint is at least as selective as the source. This keeps
	// the drive anchored at the endpoint expected to prune the recursive expansion earliest.
	return terminalSelectivity >= sourceSelectivity
}

func endpointSelectivity(nodePattern *cypher.NodePattern, symbol string, searchSymbols map[string]struct{}) boundSourceSelectivity {
	selectivity := nodePatternSelectivity(nodePattern, false)
	if _, constrained := searchSymbols[symbol]; constrained && symbol != "" {
		mergeSelectivityValue(&selectivity, boundSourceSelectivityPredicate)
	}

	return selectivity
}

// whereSearchPredicateSymbols collects the set of symbols referenced by a search-operator
// predicate (equality, regex, comparison, STARTS/ENDS WITH, CONTAINS, IN) within a match's WHERE
// clause. These indicate a filter that can anchor a traversal at the referenced endpoint.
func whereSearchPredicateSymbols(match *cypher.Match) map[string]struct{} {
	symbols := map[string]struct{}{}

	if match == nil || match.Where == nil {
		return symbols
	}

	for _, expression := range match.Where.Expressions {
		addShortestPathSearchPredicateSymbols(symbols, expression)
	}

	return symbols
}

func reversePatternElements(patternPart *cypher.PatternPart) {
	elements := patternPart.PatternElements

	for left, right := 0, len(elements)-1; left < right; left, right = left+1, right-1 {
		elements[left], elements[right] = elements[right], elements[left]
	}

	for _, element := range elements {
		if relationshipPattern, ok := element.AsRelationshipPattern(); ok {
			relationshipPattern.Direction = relationshipPattern.Direction.Reverse()
		}
	}
}
