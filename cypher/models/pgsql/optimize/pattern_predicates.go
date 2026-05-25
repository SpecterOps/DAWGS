package optimize

import (
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/walk"
)

type patternPredicateCollector struct {
	walk.VisitorHandler
	predicates []*cypher.PatternPredicate
}

type indexedPatternPredicate struct {
	ClauseIndex    int
	PredicateIndex int
	Predicate      *cypher.PatternPredicate
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

func indexedPatternPredicatesInQueryPart(queryPart cypher.SyntaxNode) []indexedPatternPredicate {
	var (
		indexedPredicates []indexedPatternPredicate
		seen              = map[*cypher.PatternPredicate]struct{}{}
	)

	appendPredicates := func(clauseIndex int, where *cypher.Where) {
		if where == nil {
			return
		}

		for _, predicate := range patternPredicatesInQueryPart(where) {
			seen[predicate] = struct{}{}
			indexedPredicates = append(indexedPredicates, indexedPatternPredicate{
				ClauseIndex:    clauseIndex,
				PredicateIndex: len(indexedPredicates),
				Predicate:      predicate,
			})
		}
	}

	switch typedQueryPart := queryPart.(type) {
	case *cypher.SinglePartQuery:
		for clauseIndex, readingClause := range typedQueryPart.ReadingClauses {
			if readingClause != nil && readingClause.Match != nil {
				appendPredicates(clauseIndex, readingClause.Match.Where)
			}
		}

	case *cypher.MultiPartQueryPart:
		for clauseIndex, readingClause := range typedQueryPart.ReadingClauses {
			if readingClause != nil && readingClause.Match != nil {
				appendPredicates(clauseIndex, readingClause.Match.Where)
			}
		}
	}

	for _, predicate := range patternPredicatesInQueryPart(queryPart) {
		if _, alreadyIndexed := seen[predicate]; !alreadyIndexed {
			indexedPredicates = append(indexedPredicates, indexedPatternPredicate{
				PredicateIndex: len(indexedPredicates),
				Predicate:      predicate,
			})
		}
	}

	return indexedPredicates
}

func patternPartForPredicate(predicate *cypher.PatternPredicate) *cypher.PatternPart {
	if predicate == nil {
		return nil
	}

	return &cypher.PatternPart{
		PatternElements: predicate.PatternElements,
	}
}
