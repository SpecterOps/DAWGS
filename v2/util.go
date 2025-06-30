package v2

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models/walk"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
)

func isNodePattern(seen *queryIdentifiers) bool {
	return seen.All().Contains(Identifiers.node)
}

func isRelationshipPattern(seen *queryIdentifiers) bool {
	var (
		allIdentifiers  = seen.All()
		hasStart        = allIdentifiers.Contains(Identifiers.start)
		hasRelationship = allIdentifiers.Contains(Identifiers.relationship)
		hasEnd          = allIdentifiers.Contains(Identifiers.end)
	)

	return hasStart || hasRelationship || hasEnd
}

func prepareNodePattern(match *cypher.Match, seen *queryIdentifiers) error {
	if isRelationshipPattern(seen) {
		return fmt.Errorf("query mixes node and relationship query identifiers")
	}

	if !seen.Created.Contains(Identifiers.node) {
		match.NewPatternPart().AddPatternElements(&cypher.NodePattern{
			Variable: Identifiers.Node(),
		})
	}

	return nil
}

func prepareRelationshipPattern(match *cypher.Match, seen *queryIdentifiers, relationshipKinds graph.Kinds) error {
	var (
		newPatternPart              = match.NewPatternPart()
		startNodeConstrained        = seen.Constrained.Contains(Identifiers.start)
		relationshipConstrained     = seen.Constrained.Contains(Identifiers.relationship)
		relationshipCreated         = seen.Created.Contains(Identifiers.relationship)
		endNodeConstrained          = seen.Constrained.Contains(Identifiers.end)
		relationshipPatternRequired = relationshipConstrained || len(relationshipKinds) > 0
	)

	if relationshipCreated && relationshipPatternRequired {
		return fmt.Errorf("query may not both create and constrain the relationship")
	}

	if startNodeConstrained || relationshipCreated {
		newPatternPart.AddPatternElements(&cypher.NodePattern{
			Variable: Identifiers.Start(),
		})
	} else if relationshipPatternRequired {
		newPatternPart.AddPatternElements(&cypher.NodePattern{})
	}

	if relationshipConstrained {
		newPatternPart.AddPatternElements(&cypher.RelationshipPattern{
			Variable:  Identifiers.Relationship(),
			Direction: graph.DirectionOutbound,
			Kinds:     relationshipKinds,
		})
	} else if relationshipPatternRequired {
		newPatternPart.AddPatternElements(&cypher.RelationshipPattern{
			Direction: graph.DirectionOutbound,
			Kinds:     relationshipKinds,
		})
	}

	if endNodeConstrained || relationshipCreated {
		newPatternPart.AddPatternElements(&cypher.NodePattern{
			Variable: Identifiers.End(),
		})
	} else if relationshipPatternRequired {
		newPatternPart.AddPatternElements(&cypher.NodePattern{})
	}

	return nil
}

func extractExpressionVariable(expression cypher.Expression) *cypher.Variable {
	var variableExpression *cypher.Variable

	switch typedExpression := expression.(type) {
	case *cypher.Variable:
		variableExpression = typedExpression

	case *cypher.NodePattern:
		variableExpression = typedExpression.Variable

	case *cypher.RelationshipPattern:
		variableExpression = typedExpression.Variable

	case *cypher.PatternPart:
		variableExpression = typedExpression.Variable

	case *cypher.ProjectionItem:
		variableExpression = typedExpression.Alias
	}

	return variableExpression
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

type queryIdentifiers struct {
	Created     *identifierSet
	Updated     *identifierSet
	Deleted     *identifierSet
	Constrained *identifierSet
	Projected   *identifierSet
}

func (s *queryIdentifiers) All() *identifierSet {
	allIdentifiers := newIdentifierSet()

	allIdentifiers.Or(s.Created)
	allIdentifiers.Or(s.Updated)
	allIdentifiers.Or(s.Deleted)
	allIdentifiers.Or(s.Constrained)
	allIdentifiers.Or(s.Projected)

	return allIdentifiers
}

func newQueryIdentifiers() *queryIdentifiers {
	return &queryIdentifiers{
		Created:     newIdentifierSet(),
		Updated:     newIdentifierSet(),
		Deleted:     newIdentifierSet(),
		Constrained: newIdentifierSet(),
		Projected:   newIdentifierSet(),
	}
}

type identifierExtractor struct {
	walk.Visitor[cypher.SyntaxNode]

	seen *queryIdentifiers

	inDelete bool
	inUpdate bool
	inCreate bool
}

func newIdentifierExtractor() *identifierExtractor {
	return &identifierExtractor{
		Visitor: walk.NewVisitor[cypher.SyntaxNode](),
		seen:    newQueryIdentifiers(),
	}
}

func (s *identifierExtractor) Enter(node cypher.SyntaxNode) {
	switch node.(type) {
	case *cypher.Delete:
		s.inDelete = true

	case *cypher.Set, *cypher.Remove:
		s.inUpdate = true

	case *cypher.Create:
		s.inCreate = true

	default:
		if variableExpression := extractExpressionVariable(node); variableExpression != nil {
			var identifiers *identifierSet

			if s.inUpdate {
				identifiers = s.seen.Updated
			} else if s.inDelete {
				identifiers = s.seen.Deleted
			} else if s.inCreate {
				identifiers = s.seen.Created
			} else {
				identifiers = s.seen.Constrained
			}

			identifiers.Add(variableExpression.Symbol)
		}
	}
}

func (s *identifierExtractor) Exit(node cypher.SyntaxNode) {
	switch node.(type) {
	case *cypher.Delete:
		s.inDelete = false

	case *cypher.Set, *cypher.Remove:
		s.inUpdate = false

	case *cypher.Create:
		s.inCreate = false
	}
}

func extractQueryIdentifiers(expression cypher.Expression) (*queryIdentifiers, error) {
	var (
		identifierExtractorVisitor = newIdentifierExtractor()
		err                        = walk.Cypher(expression, identifierExtractorVisitor)
	)

	return identifierExtractorVisitor.seen, err
}
