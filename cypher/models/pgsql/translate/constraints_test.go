package translate

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/pgd"
	"github.com/stretchr/testify/require"
)

func TestMeasureSelectivity(t *testing.T) {
	selectivity, err := MeasureSelectivity(NewScope(), pgd.Equals(
		pgsql.Identifier("123"),
		pgsql.Identifier("456"),
	))

	require.Nil(t, err)
	require.Equal(t, 30, selectivity)
}

func TestCanExecuteSelectiveBidirectionalSearch(t *testing.T) {
	var (
		lowSelectivity = pgd.Equals(
			pgsql.Identifier("123"),
			pgsql.Identifier("456"),
		)
		idLookup = func(identifier pgsql.Identifier, id int64) pgsql.Expression {
			return pgd.Equals(
				pgsql.CompoundIdentifier{identifier, pgsql.ColumnID},
				pgd.IntLiteral(id),
			)
		}
	)

	t.Run("rejects low selectivity endpoints", func(t *testing.T) {
		step := &TraversalStep{
			Expansion: &Expansion{
				PrimerNodeConstraints:   lowSelectivity,
				TerminalNodeConstraints: lowSelectivity,
			},
		}

		canExecute, err := step.CanExecuteSelectiveBidirectionalSearch(NewScope())

		require.NoError(t, err)
		require.False(t, canExecute)
	})

	t.Run("accepts singleton endpoint pairs", func(t *testing.T) {
		step := &TraversalStep{
			LeftNode: &BoundIdentifier{
				Identifier: pgsql.Identifier("n0"),
			},
			RightNode: &BoundIdentifier{
				Identifier: pgsql.Identifier("n1"),
			},
			Expansion: &Expansion{
				PrimerNodeConstraints:   idLookup(pgsql.Identifier("n0"), 1),
				TerminalNodeConstraints: idLookup(pgsql.Identifier("n1"), 2),
			},
		}

		canExecute, err := step.CanExecuteSelectiveBidirectionalSearch(NewScope())

		require.NoError(t, err)
		require.True(t, canExecute)
	})

	t.Run("rejects non-static id equality endpoints", func(t *testing.T) {
		step := &TraversalStep{
			LeftNode: &BoundIdentifier{
				Identifier: pgsql.Identifier("n0"),
			},
			RightNode: &BoundIdentifier{
				Identifier: pgsql.Identifier("n1"),
			},
			Expansion: &Expansion{
				PrimerNodeConstraints: pgd.Equals(
					pgsql.CompoundIdentifier{pgsql.Identifier("n0"), pgsql.ColumnID},
					pgsql.CompoundIdentifier{pgsql.Identifier("n0"), pgsql.ColumnProperties},
				),
				TerminalNodeConstraints: idLookup(pgsql.Identifier("n1"), 2),
			},
		}

		canExecute, err := step.CanExecuteSelectiveBidirectionalSearch(NewScope())

		require.NoError(t, err)
		require.False(t, canExecute)
	})

	t.Run("rejects singleton endpoints with external constraints", func(t *testing.T) {
		step := &TraversalStep{
			LeftNode: &BoundIdentifier{
				Identifier: pgsql.Identifier("n0"),
			},
			RightNode: &BoundIdentifier{
				Identifier: pgsql.Identifier("n1"),
			},
			Expansion: &Expansion{
				PrimerNodeConstraints: pgd.And(
					idLookup(pgsql.Identifier("n0"), 1),
					pgd.Equals(
						pgsql.CompoundIdentifier{pgsql.Identifier("n0"), pgsql.ColumnProperties},
						pgsql.CompoundIdentifier{pgsql.Identifier("n1"), pgsql.ColumnProperties},
					),
				),
				TerminalNodeConstraints: idLookup(pgsql.Identifier("n1"), 2),
			},
		}

		canExecute, err := step.CanExecuteSelectiveBidirectionalSearch(NewScope())

		require.NoError(t, err)
		require.False(t, canExecute)
	})

	t.Run("accepts materialized endpoint pairs", func(t *testing.T) {
		scope := NewScope()
		_, err := scope.PushFrame()
		require.NoError(t, err)

		frame, err := scope.PushFrame()
		require.NoError(t, err)

		step := &TraversalStep{
			Frame:          frame,
			LeftNodeBound:  true,
			RightNodeBound: true,
			Expansion:      &Expansion{},
		}

		canExecute, err := step.CanExecuteSelectiveBidirectionalSearch(scope)

		require.NoError(t, err)
		require.True(t, canExecute)
	})
}

func TestCanExecutePairAwareBidirectionalSearch(t *testing.T) {
	var (
		scopeWithNodeBindings = func(identifiers ...pgsql.Identifier) *Scope {
			scope := NewScope()
			for _, identifier := range identifiers {
				scope.Define(identifier, pgsql.NodeComposite)
			}

			return scope
		}
		localSelectivePropertyConstraint = func(identifier pgsql.Identifier) pgsql.Expression {
			return pgd.Equals(
				pgd.PropertyLookup(identifier, "name"),
				pgd.TextLiteral("123"),
			)
		}
		localBroadPropertyConstraint = func(identifier pgsql.Identifier) pgsql.Expression {
			return pgd.Equals(
				pgsql.CompoundIdentifier{identifier, pgsql.ColumnProperties},
				pgd.IntLiteral(1),
			)
		}
		localKindConstraint = func(identifier pgsql.Identifier) pgsql.Expression {
			return pgd.And(
				pgd.Equals(
					pgsql.CompoundIdentifier{identifier, pgsql.ColumnKindIDs},
					pgd.IntLiteral(1),
				),
				pgd.Equals(
					pgsql.CompoundIdentifier{identifier, pgsql.ColumnKindIDs},
					pgd.IntLiteral(2),
				),
			)
		}
	)

	t.Run("accepts selective property-backed local endpoint constraints for shortest path", func(t *testing.T) {
		var (
			leftIdentifier  = pgsql.Identifier("n0")
			rightIdentifier = pgsql.Identifier("n1")
			step            = &TraversalStep{
				LeftNode: &BoundIdentifier{
					Identifier: leftIdentifier,
				},
				RightNode: &BoundIdentifier{
					Identifier: rightIdentifier,
				},
				Expansion: &Expansion{
					Options: ExpansionOptions{
						FindShortestPath: true,
					},
					PrimerNodeConstraints:   localSelectivePropertyConstraint(leftIdentifier),
					TerminalNodeConstraints: localSelectivePropertyConstraint(rightIdentifier),
				},
			}
		)

		canExecute, err := step.CanExecutePairAwareBidirectionalSearch(scopeWithNodeBindings(leftIdentifier, rightIdentifier))

		require.NoError(t, err)
		require.True(t, canExecute)
	})

	t.Run("rejects broad non-kind local endpoint constraints for shortest path", func(t *testing.T) {
		var (
			leftIdentifier  = pgsql.Identifier("n0")
			rightIdentifier = pgsql.Identifier("n1")
			step            = &TraversalStep{
				LeftNode: &BoundIdentifier{
					Identifier: leftIdentifier,
				},
				RightNode: &BoundIdentifier{
					Identifier: rightIdentifier,
				},
				Expansion: &Expansion{
					Options: ExpansionOptions{
						FindShortestPath: true,
					},
					PrimerNodeConstraints:   localBroadPropertyConstraint(leftIdentifier),
					TerminalNodeConstraints: localBroadPropertyConstraint(rightIdentifier),
				},
			}
		)

		canExecute, err := step.CanExecutePairAwareBidirectionalSearch(scopeWithNodeBindings(leftIdentifier, rightIdentifier))

		require.NoError(t, err)
		require.False(t, canExecute)
	})

	t.Run("rejects pair-aware search when only one endpoint is selective", func(t *testing.T) {
		var (
			leftIdentifier  = pgsql.Identifier("n0")
			rightIdentifier = pgsql.Identifier("n1")
			step            = &TraversalStep{
				LeftNode: &BoundIdentifier{
					Identifier: leftIdentifier,
				},
				RightNode: &BoundIdentifier{
					Identifier: rightIdentifier,
				},
				Expansion: &Expansion{
					Options: ExpansionOptions{
						FindShortestPath: true,
					},
					PrimerNodeConstraints:   localSelectivePropertyConstraint(leftIdentifier),
					TerminalNodeConstraints: localBroadPropertyConstraint(rightIdentifier),
				},
			}
		)

		canExecute, err := step.CanExecutePairAwareBidirectionalSearch(scopeWithNodeBindings(leftIdentifier, rightIdentifier))

		require.NoError(t, err)
		require.False(t, canExecute)
	})

	t.Run("rejects label-only local endpoint constraints for shortest path", func(t *testing.T) {
		step := &TraversalStep{
			LeftNode: &BoundIdentifier{
				Identifier: pgsql.Identifier("n0"),
			},
			RightNode: &BoundIdentifier{
				Identifier: pgsql.Identifier("n1"),
			},
			Expansion: &Expansion{
				Options: ExpansionOptions{
					FindShortestPath: true,
				},
				PrimerNodeConstraints:   localKindConstraint(pgsql.Identifier("n0")),
				TerminalNodeConstraints: localKindConstraint(pgsql.Identifier("n1")),
			},
		}

		canExecute, err := step.CanExecutePairAwareBidirectionalSearch(NewScope())

		require.NoError(t, err)
		require.False(t, canExecute)
	})

	t.Run("accepts selective property-backed local endpoint constraints for all shortest paths", func(t *testing.T) {
		var (
			leftIdentifier  = pgsql.Identifier("n0")
			rightIdentifier = pgsql.Identifier("n1")
			step            = &TraversalStep{
				LeftNode: &BoundIdentifier{
					Identifier: leftIdentifier,
				},
				RightNode: &BoundIdentifier{
					Identifier: rightIdentifier,
				},
				Expansion: &Expansion{
					Options: ExpansionOptions{
						FindAllShortestPaths: true,
					},
					PrimerNodeConstraints:   localSelectivePropertyConstraint(leftIdentifier),
					TerminalNodeConstraints: localSelectivePropertyConstraint(rightIdentifier),
				},
			}
		)

		canExecute, err := step.CanExecutePairAwareBidirectionalSearch(scopeWithNodeBindings(leftIdentifier, rightIdentifier))

		require.NoError(t, err)
		require.True(t, canExecute)
	})

	t.Run("rejects endpoint constraints that reference the other endpoint", func(t *testing.T) {
		var (
			leftIdentifier  = pgsql.Identifier("n0")
			rightIdentifier = pgsql.Identifier("n1")
			step            = &TraversalStep{
				LeftNode: &BoundIdentifier{
					Identifier: leftIdentifier,
				},
				RightNode: &BoundIdentifier{
					Identifier: rightIdentifier,
				},
				Expansion: &Expansion{
					Options: ExpansionOptions{
						FindShortestPath: true,
					},
					PrimerNodeConstraints: pgd.And(
						localSelectivePropertyConstraint(leftIdentifier),
						pgd.Equals(
							pgsql.CompoundIdentifier{leftIdentifier, pgsql.ColumnKindIDs},
							pgsql.CompoundIdentifier{rightIdentifier, pgsql.ColumnKindIDs},
						),
					),
					TerminalNodeConstraints: localSelectivePropertyConstraint(rightIdentifier),
				},
			}
		)

		canExecute, err := step.CanExecutePairAwareBidirectionalSearch(scopeWithNodeBindings(leftIdentifier, rightIdentifier))

		require.NoError(t, err)
		require.False(t, canExecute)
	})
}

func TestCanMaterializeEndpointPairFilterRequiresPairAwareConstraints(t *testing.T) {
	var (
		leftIdentifier     = pgsql.Identifier("n0")
		rightIdentifier    = pgsql.Identifier("n1")
		kindOnlyConstraint = func(identifier pgsql.Identifier) pgsql.Expression {
			return pgd.Equals(
				pgsql.CompoundIdentifier{identifier, pgsql.ColumnKindIDs},
				pgd.IntLiteral(1),
			)
		}
		propertyConstraint = func(identifier pgsql.Identifier) pgsql.Expression {
			return pgd.Equals(
				pgd.PropertyLookup(identifier, "name"),
				pgd.TextLiteral("target"),
			)
		}
		step = &TraversalStep{
			LeftNode:  &BoundIdentifier{Identifier: leftIdentifier},
			RightNode: &BoundIdentifier{Identifier: rightIdentifier},
		}
	)

	require.False(t, canMaterializeEndpointPairFilterForStep(step, &Expansion{
		PrimerNodeConstraints:   kindOnlyConstraint(leftIdentifier),
		TerminalNodeConstraints: propertyConstraint(rightIdentifier),
	}))
	require.True(t, canMaterializeEndpointPairFilterForStep(step, &Expansion{
		PrimerNodeConstraints:   propertyConstraint(leftIdentifier),
		TerminalNodeConstraints: propertyConstraint(rightIdentifier),
	}))
}
