package translate

import (
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/walk"
)

// referencedIdentifierCollector gathers a conservative set of source-level
// identifiers that may be observed after MATCH translation. Traversal pruning
// uses this set to decide whether a pattern binding, node alias, or edge alias
// must remain projected out of a traversal CTE.
//
// MATCH pattern variables are declarations, not reads. Treating every
// declaration as a read forces path and edge composites to stay alive even when
// the query never references them. The collector therefore ignores variables
// while it is inside a MATCH pattern declaration and records only uses outside
// those declarations.
//
// There are two deliberate conservative cases:
//   - pattern predicates such as `WHERE (s)-[r]->()` are expressions, so their
//     variables are counted as reads;
//   - a variable declared more than once inside MATCH patterns is marked live,
//     because repeated declarations commonly represent reuse across pattern
//     parts or MATCH clauses and may be needed for joins.
type referencedIdentifierCollector struct {
	walk.VisitorHandler

	referencedIdentifiers        *pgsql.IdentifierSet
	matchPatternDeclarationRefs  map[pgsql.Identifier]int
	matchPatternDeclarations     map[*cypher.PatternPart]struct{}
	matchPatternDeclarationDepth int
}

func newReferencedIdentifierCollector() *referencedIdentifierCollector {
	return &referencedIdentifierCollector{
		VisitorHandler:              walk.NewCancelableErrorHandler(),
		referencedIdentifiers:       pgsql.NewIdentifierSet(),
		matchPatternDeclarationRefs: map[pgsql.Identifier]int{},
		matchPatternDeclarations:    map[*cypher.PatternPart]struct{}{},
	}
}

func (s *referencedIdentifierCollector) addVariable(variable *cypher.Variable) {
	if variable != nil {
		s.referencedIdentifiers.Add(pgsql.Identifier(variable.Symbol))
	}
}

func (s *referencedIdentifierCollector) addMatchPatternDeclaration(variable *cypher.Variable) {
	if variable != nil {
		s.matchPatternDeclarationRefs[pgsql.Identifier(variable.Symbol)] += 1
	}
}

func (s *referencedIdentifierCollector) collectRepeatedMatchPatternDeclarations() {
	for identifier, numDeclarations := range s.matchPatternDeclarationRefs {
		if numDeclarations > 1 {
			s.referencedIdentifiers.Add(identifier)
		}
	}
}

func (s *referencedIdentifierCollector) isMatchPatternDeclaration(patternPart *cypher.PatternPart) bool {
	_, isDeclaration := s.matchPatternDeclarations[patternPart]
	return isDeclaration
}

func (s *referencedIdentifierCollector) Enter(node cypher.SyntaxNode) {
	switch typedNode := node.(type) {
	case *cypher.Match:
		for _, patternPart := range typedNode.Pattern {
			s.matchPatternDeclarations[patternPart] = struct{}{}
		}

	case *cypher.PatternPart:
		if s.isMatchPatternDeclaration(typedNode) {
			s.addMatchPatternDeclaration(typedNode.Variable)
			s.matchPatternDeclarationDepth += 1
		} else {
			s.addVariable(typedNode.Variable)
		}

	case *cypher.NodePattern:
		if s.matchPatternDeclarationDepth == 0 {
			s.addVariable(typedNode.Variable)
		} else {
			s.addMatchPatternDeclaration(typedNode.Variable)
		}

	case *cypher.RelationshipPattern:
		if s.matchPatternDeclarationDepth == 0 {
			s.addVariable(typedNode.Variable)
		} else {
			s.addMatchPatternDeclaration(typedNode.Variable)
		}

	case *cypher.Variable:
		s.addVariable(typedNode)
	}
}

func (s *referencedIdentifierCollector) Visit(cypher.SyntaxNode) {}

func (s *referencedIdentifierCollector) Exit(node cypher.SyntaxNode) {
	if patternPart, isPatternPart := node.(*cypher.PatternPart); isPatternPart && s.isMatchPatternDeclaration(patternPart) {
		s.matchPatternDeclarationDepth -= 1
	}
}

// collectReferencedIdentifiers walks a query part before it is translated and
// returns the source identifiers that should be considered live for traversal
// projection pruning. It is intentionally not a full data-flow analysis; it is
// a cheap, conservative guard against pruning values that later query clauses
// can observe.
func collectReferencedIdentifiers(root cypher.SyntaxNode) (*pgsql.IdentifierSet, error) {
	if root == nil {
		return pgsql.NewIdentifierSet(), nil
	}

	collector := newReferencedIdentifierCollector()
	if err := walk.Cypher(root, collector); err != nil {
		return collector.referencedIdentifiers, err
	}

	collector.collectRepeatedMatchPatternDeclarations()
	return collector.referencedIdentifiers, nil
}

func (s *QueryPart) SetReferencedIdentifiers(referencedIdentifiers *pgsql.IdentifierSet) {
	if referencedIdentifiers == nil {
		s.referencedIdentifiers = pgsql.NewIdentifierSet()
	} else {
		s.referencedIdentifiers = referencedIdentifiers
	}
}

func (s *QueryPart) ReferencesSourceIdentifier(identifier pgsql.Identifier) bool {
	if s == nil || s.referencedIdentifiers == nil {
		return false
	}

	return s.referencedIdentifiers.Contains(pgsql.Identifier(cypher.TokenLiteralAsterisk)) || s.referencedIdentifiers.Contains(identifier)
}

func (s *QueryPart) ReferencesBinding(binding *BoundIdentifier) bool {
	if binding == nil {
		return false
	}

	if binding.Alias.Set {
		return s.ReferencesSourceIdentifier(binding.Alias.Value)
	}

	return s.ReferencesSourceIdentifier(binding.Identifier)
}
