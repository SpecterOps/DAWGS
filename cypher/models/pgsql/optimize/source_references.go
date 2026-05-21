package optimize

import (
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/walk"
)

type sourceReferenceCollector struct {
	walk.VisitorHandler

	referencedIdentifiers        map[string]struct{}
	matchPatternDeclarationRefs  map[string]int
	matchPatternDeclarations     map[*cypher.PatternPart]struct{}
	matchPatternDeclarationDepth int
}

func newSourceReferenceCollector() *sourceReferenceCollector {
	return &sourceReferenceCollector{
		VisitorHandler:              walk.NewCancelableErrorHandler(),
		referencedIdentifiers:       map[string]struct{}{},
		matchPatternDeclarationRefs: map[string]int{},
		matchPatternDeclarations:    map[*cypher.PatternPart]struct{}{},
	}
}

func (s *sourceReferenceCollector) addVariable(variable *cypher.Variable) {
	if variable != nil && variable.Symbol != "" {
		s.referencedIdentifiers[variable.Symbol] = struct{}{}
	}
}

func (s *sourceReferenceCollector) addMatchPatternDeclaration(variable *cypher.Variable) {
	if variable != nil && variable.Symbol != "" {
		s.matchPatternDeclarationRefs[variable.Symbol] += 1
	}
}

func (s *sourceReferenceCollector) collectRepeatedMatchPatternDeclarations() {
	for identifier, numDeclarations := range s.matchPatternDeclarationRefs {
		if numDeclarations > 1 {
			s.referencedIdentifiers[identifier] = struct{}{}
		}
	}
}

func (s *sourceReferenceCollector) isMatchPatternDeclaration(patternPart *cypher.PatternPart) bool {
	_, isDeclaration := s.matchPatternDeclarations[patternPart]
	return isDeclaration
}

func (s *sourceReferenceCollector) Enter(node cypher.SyntaxNode) {
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

func (s *sourceReferenceCollector) Visit(cypher.SyntaxNode) {}

func (s *sourceReferenceCollector) Exit(node cypher.SyntaxNode) {
	if patternPart, isPatternPart := node.(*cypher.PatternPart); isPatternPart && s.isMatchPatternDeclaration(patternPart) {
		s.matchPatternDeclarationDepth -= 1
	}
}

func collectReferencedSourceIdentifiers(root cypher.SyntaxNode) (map[string]struct{}, error) {
	if root == nil {
		return map[string]struct{}{}, nil
	}

	collector := newSourceReferenceCollector()
	if err := walk.Cypher(root, collector); err != nil {
		return collector.referencedIdentifiers, err
	}

	collector.collectRepeatedMatchPatternDeclarations()
	return collector.referencedIdentifiers, nil
}

func referencesSourceIdentifier(references map[string]struct{}, symbol string) bool {
	if _, referencesAll := references[cypher.TokenLiteralAsterisk]; referencesAll {
		return true
	}

	if symbol == "" {
		return false
	}

	_, referenced := references[symbol]
	return referenced
}
