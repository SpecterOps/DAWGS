package optimize

import (
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/walk"
)

type patternPredicateCollector struct {
	walk.VisitorHandler
	predicates []*cypher.PatternPredicate
}

func (s *patternPredicateCollector) Enter(node cypher.SyntaxNode) {
	if predicate, isPatternPredicate := node.(*cypher.PatternPredicate); isPatternPredicate {
		s.predicates = append(s.predicates, predicate)
	}
}

func (s *patternPredicateCollector) Visit(cypher.SyntaxNode) {}
func (s *patternPredicateCollector) Exit(cypher.SyntaxNode)  {}

func patternPredicatesInQueryPart(queryPart cypher.SyntaxNode) []*cypher.PatternPredicate {
	if queryPart == nil {
		return nil
	}

	collector := &patternPredicateCollector{
		VisitorHandler: walk.NewCancelableErrorHandler(),
	}
	if err := walk.Cypher(queryPart, collector); err != nil {
		return nil
	}

	return collector.predicates
}

func patternPartForPredicate(predicate *cypher.PatternPredicate) *cypher.PatternPart {
	if predicate == nil {
		return nil
	}

	return &cypher.PatternPart{
		PatternElements: predicate.PatternElements,
	}
}
