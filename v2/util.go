package v2

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
)

func isNodePattern(seenIdentifiers map[string]struct{}) bool {
	for identifier := range seenIdentifiers {
		switch identifier {
		case Identifiers.node:
			return true
		}
	}

	return false
}

func isRelationshipPattern(seenIdentifiers map[string]struct{}) bool {
	for identifier := range seenIdentifiers {
		switch identifier {
		case Identifiers.start, Identifiers.relationship, Identifiers.end:
			return true
		}
	}

	return false
}

func prepareNodePattern(match *cypher.Match, seenIdentifiers map[string]struct{}) error {
	if isRelationshipPattern(seenIdentifiers) {
		return fmt.Errorf("query mixes node and relationship query identifiers")
	}

	match.NewPatternPart().AddPatternElements(&cypher.NodePattern{
		Variable: Identifiers.Node(),
	})

	return nil
}

func prepareRelationshipPattern(match *cypher.Match, seenIdentifiers map[string]struct{}, relationshipKinds graph.Kinds) error {
	var (
		newPatternPart       = match.NewPatternPart()
		_, startNodeBound    = seenIdentifiers[Identifiers.start]
		_, relationshipBound = seenIdentifiers[Identifiers.relationship]
		_, endNodeBound      = seenIdentifiers[Identifiers.end]
		patternRequired      = relationshipBound || len(relationshipKinds) > 0
	)

	if startNodeBound {
		newPatternPart.AddPatternElements(&cypher.NodePattern{
			Variable: Identifiers.Start(),
		})
	} else if patternRequired {
		newPatternPart.AddPatternElements(&cypher.NodePattern{})
	}

	if relationshipBound {
		newPatternPart.AddPatternElements(&cypher.RelationshipPattern{
			Variable:  Identifiers.Relationship(),
			Direction: graph.DirectionOutbound,
			Kinds:     relationshipKinds,
		})
	} else if patternRequired {
		newPatternPart.AddPatternElements(&cypher.RelationshipPattern{
			Direction: graph.DirectionOutbound,
			Kinds:     relationshipKinds,
		})
	}

	if endNodeBound {
		newPatternPart.AddPatternElements(&cypher.NodePattern{
			Variable: Identifiers.End(),
		})
	} else if patternRequired {
		newPatternPart.AddPatternElements(&cypher.NodePattern{})
	}

	return nil
}
