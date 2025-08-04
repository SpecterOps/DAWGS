package query

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/specterops/dawgs/graph"
)

func isNodePattern(seen *identifierSet) bool {
	return seen.Contains(Identifiers.node)
}

func isRelationshipPattern(seen *identifierSet) bool {
	var (
		hasStart        = seen.Contains(Identifiers.start)
		hasRelationship = seen.Contains(Identifiers.relationship)
		hasEnd          = seen.Contains(Identifiers.end)
	)

	return hasStart || hasRelationship || hasEnd
}

func prepareNodePattern(match *cypher.Match, seen *identifierSet) error {
	if isRelationshipPattern(seen) {
		return fmt.Errorf("query mixes node and relationship query identifiers")
	}

	match.NewPatternPart().AddPatternElements(&cypher.NodePattern{
		Variable: Identifiers.Node(),
	})

	return nil
}

func prepareRelationshipPattern(match *cypher.Match, seen *identifierSet, relationshipKinds graph.Kinds) error {
	var (
		newPatternPart   = match.NewPatternPart()
		startNodeSeen    = seen.Contains(Identifiers.start)
		relationshipSeen = seen.Contains(Identifiers.relationship)
		endNodeSeen      = seen.Contains(Identifiers.end)
	)

	if startNodeSeen {
		newPatternPart.AddPatternElements(&cypher.NodePattern{
			Variable: Identifiers.Start(),
		})
	} else {
		newPatternPart.AddPatternElements(&cypher.NodePattern{})
	}

	if relationshipSeen {
		newPatternPart.AddPatternElements(&cypher.RelationshipPattern{
			Variable:  Identifiers.Relationship(),
			Direction: graph.DirectionOutbound,
			Kinds:     relationshipKinds,
		})
	} else {
		newPatternPart.AddPatternElements(&cypher.RelationshipPattern{
			Direction: graph.DirectionOutbound,
			Kinds:     relationshipKinds,
		})
	}

	if endNodeSeen {
		newPatternPart.AddPatternElements(&cypher.NodePattern{
			Variable: Identifiers.End(),
		})
	} else {
		newPatternPart.AddPatternElements(&cypher.NodePattern{})
	}

	return nil
}

type identifierSet struct {
	identifiers map[string]struct{}
}

func newIdentifierSet() *identifierSet {
	return &identifierSet{
		identifiers: map[string]struct{}{},
	}
}

func (s *identifierSet) Add(identifier string) {
	s.identifiers[identifier] = struct{}{}
}

func (s *identifierSet) Or(other *identifierSet) {
	for otherIdentifier := range other.identifiers {
		s.identifiers[otherIdentifier] = struct{}{}
	}
}

func (s *identifierSet) Contains(identifier string) bool {
	_, containsIdentifier := s.identifiers[identifier]
	return containsIdentifier
}

func (s *identifierSet) CollectFromExpression(expr cypher.Expression) error {
	if exprIdentifiers, err := extractCypherIdentifiers(expr); err != nil {
		return err
	} else {
		s.Or(exprIdentifiers)
		return nil
	}
}

type identifierExtractor struct {
	walk.Visitor[cypher.SyntaxNode]

	seen *identifierSet

	inDelete bool
	inUpdate bool
	inCreate bool
	inWhere  bool
}

func newIdentifierExtractor() *identifierExtractor {
	return &identifierExtractor{
		Visitor: walk.NewVisitor[cypher.SyntaxNode](),
		seen:    newIdentifierSet(),
	}
}

func (s *identifierExtractor) Enter(node cypher.SyntaxNode) {
	switch typedNode := node.(type) {
	case *cypher.Variable:
		s.seen.Add(typedNode.Symbol)

	case *cypher.NodePattern:
		if typedNode.Variable != nil {
			s.seen.Add(typedNode.Variable.Symbol)
		}

	case *cypher.RelationshipPattern:
		if typedNode.Variable != nil {
			s.seen.Add(typedNode.Variable.Symbol)
		}

	case *cypher.PatternPart:
		if typedNode.Variable != nil {
			s.seen.Add(typedNode.Variable.Symbol)
		}

	case *cypher.ProjectionItem:
		if typedNode.Alias != nil {
			s.seen.Add(typedNode.Alias.Symbol)
		}
	}
}

func extractCypherIdentifiers(expression cypher.Expression) (*identifierSet, error) {
	var (
		identifierExtractorVisitor = newIdentifierExtractor()
		err                        = walk.Cypher(expression, identifierExtractorVisitor)
	)

	return identifierExtractorVisitor.seen, err
}
