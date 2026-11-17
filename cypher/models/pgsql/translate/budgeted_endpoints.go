package translate

import (
	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/pgd"
)

const (
	// budgetedRootEndpoints names the materialized source membership relation.
	budgetedRootEndpoints pgsql.Identifier = "asp_root_endpoints"

	// budgetedTerminalEndpoints names the materialized target membership relation.
	budgetedTerminalEndpoints pgsql.Identifier = "asp_terminal_endpoints"

	// budgetedResolvedEndpoints names the admitted source-target pair relation.
	budgetedResolvedEndpoints pgsql.Identifier = "asp_resolved_endpoints"

	// budgetedAdmitted names the result of charging one endpoint record.
	budgetedAdmitted pgsql.Identifier = "admitted"

	// budgetedEndpointPhase identifies membership admission in budget diagnostics.
	budgetedEndpointPhase = "endpoint"

	// budgetedPairPhase identifies resolved-pair admission in budget diagnostics.
	budgetedPairPhase = "pair"

	// budgetedAdmissionStates charges one state for each endpoint or pair record.
	budgetedAdmissionStates int64 = 1

	// budgetedAdmissionBytes charges the fixed representation of an admitted record.
	budgetedAdmissionBytes int64 = 64
)

// budgetedEndpointAdmission charges a fixed-size endpoint/pair record before
// it is stored. Search, predecessor, and enumeration work use the same token.
func budgetedEndpointAdmission(phase string) pgsql.FunctionCall {
	return pgsql.FunctionCall{
		Function: pgsql.FunctionAdmitTraversalWorkV2,
		Parameters: []pgsql.Expression{
			pgsql.NewLiteral(budgetedAdmissionStates, pgsql.Int8),
			pgsql.NewLiteral(budgetedAdmissionBytes, pgsql.Int8),
			pgsql.NewLiteral(phase, pgsql.Text),
		},
	}
}

// budgetedEndpointCTE keeps admission in the SELECT list so it executes only
// after the complete indexed endpoint predicate matched. Materialization
// prevents re-evaluating either the predicate or its charge for each pair.
func budgetedEndpointCTE(name, column pgsql.Identifier, node *BoundIdentifier, predicate pgsql.Expression) pgsql.CommonTableExpression {
	return pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{
			Name:  name,
			Shape: pgsql.NewRecordShape([]pgsql.Identifier{column, budgetedAdmitted}),
		},
		Materialized: &pgsql.Materialized{
			Materialized: true,
		},
		Query: pgsql.Query{
			Body: pgsql.Select{
				Projection: []pgsql.SelectItem{
					&pgsql.AliasedExpression{
						Expression: pgd.EntityID(node.Identifier),
						Alias:      models.OptionalValue(column),
					},
					&pgsql.AliasedExpression{
						Expression: budgetedEndpointAdmission(budgetedEndpointPhase),
						Alias:      models.OptionalValue(budgetedAdmitted),
					},
				},
				From: []pgsql.FromClause{
					{
						Source: expansionNodeTableReference(node.Identifier),
					},
				},
				Where: predicate,
			},
		},
	}
}

// budgetedEndpointPairCTEs resolves zero, one, or many matching nodes on each
// side. The Cartesian product is consumed through a per-row admission guard;
// no LIMIT, uniqueness assumption, or cardinality sentinel can omit a pair.
func (s *ExpansionBuilder) budgetedEndpointPairCTEs() []pgsql.CommonTableExpression {
	var (
		expansion     = s.traversalStep.Expansion
		rootPredicate = expansion.PrimerNodeConstraints
	)

	if expansion.RelationshipKindsSpecified && len(expansion.RelationshipKindIDs) == 0 {
		// The executor's empty kind array means wildcard. A typed pattern with
		// no resolved kinds instead has no matching path, including self-pairs.
		rootPredicate = pgsql.OptionalAnd(rootPredicate, pgsql.NewLiteral(false, pgsql.Boolean))
	}

	var (
		root     = budgetedEndpointCTE(budgetedRootEndpoints, expansionRootID, s.traversalStep.LeftNode, rootPredicate)
		terminal = budgetedEndpointCTE(budgetedTerminalEndpoints, expansionTerminalID, s.traversalStep.RightNode, expansion.TerminalNodeConstraints)
	)

	pairs := pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{
			Name:  budgetedResolvedEndpoints,
			Shape: pgsql.NewRecordShape([]pgsql.Identifier{expansionRootID, expansionTerminalID}),
		},
		Query: pgsql.Query{
			Body: pgsql.Select{
				Projection: []pgsql.SelectItem{
					pgsql.CompoundIdentifier{budgetedRootEndpoints, expansionRootID},
					pgsql.CompoundIdentifier{budgetedTerminalEndpoints, expansionTerminalID},
				},
				From: []pgsql.FromClause{
					{
						Source: pgsql.TableReference{
							Name: budgetedRootEndpoints.AsCompoundIdentifier(),
						},
					},
					{
						Source: pgsql.TableReference{
							Name: budgetedTerminalEndpoints.AsCompoundIdentifier(),
						},
					},
				},
				Where: pgsql.OptionalAnd(
					pgsql.CompoundIdentifier{budgetedRootEndpoints, budgetedAdmitted},
					pgsql.OptionalAnd(pgsql.CompoundIdentifier{budgetedTerminalEndpoints, budgetedAdmitted}, budgetedEndpointAdmission(budgetedPairPhase)),
				),
			},
		},
	}

	return []pgsql.CommonTableExpression{root, terminal, pairs}
}
