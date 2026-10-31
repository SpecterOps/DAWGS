package translate

import (
	"errors"
	"fmt"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/pgd"
	"github.com/specterops/dawgs/graph"
)

func newPGKindIDMatcher(scope *Scope, treeTranslator *ExpressionTreeTranslator, binding *BoundIdentifier, kindIDs []int16, isExclusive bool) error {
	kindIDsLiteral := pgsql.NewLiteral(kindIDs, pgsql.Int2Array)

	switch binding.DataType {
	case pgsql.NodeComposite, pgsql.ExpansionRootNode, pgsql.ExpansionTerminalNode:
		treeTranslator.PushOperand(pgd.Column(binding.Identifier, pgsql.ColumnKindIDs))
		treeTranslator.PushOperand(kindIDsLiteral)

		// In an exclusive kind match, if there are no kind IDs to be matched on,
		// the behavior of the contains (`@>`) operator will select all nodes, which drastically differs from
		// the overlap operator's behavior (matches nothing), so preserve the previous behavior.
		//
		// There shouldn't be a case in the Cypher frontend where a kind ID matcher is
		// created without any kind IDs to match on, but `query.Kind`/`query.KindIn` can create those
		// edge cases. We want any existing `query.Kind`/`query.KindIn` usages to match the previous behavior
		// expectations by using overlap/`&&`, which protects from the empty RHS problem.
		if isExclusive && len(kindIDs) > 0 {
			return treeTranslator.CompleteBinaryExpression(scope, pgsql.OperatorPGArrayLHSContainsRHS)
		} else {
			return treeTranslator.CompleteBinaryExpression(scope, pgsql.OperatorPGArrayOverlap)
		}

	case pgsql.EdgeComposite, pgsql.ExpansionEdge:
		// Edge kind checking is a strict equality, so the IsExclusive condition does not apply here.
		treeTranslator.PushOperand(pgsql.CompoundIdentifier{binding.Identifier, pgsql.ColumnKindID})
		treeTranslator.PushOperand(pgsql.NewAnyExpressionHinted(kindIDsLiteral))

		return treeTranslator.CompleteBinaryExpression(scope, pgsql.OperatorEquals)
	}

	return fmt.Errorf("unexpected kind matcher reference data type: %s", binding.DataType)
}

func (s *Translator) translateKindMatcher(kindMatcher *cypher.KindMatcher) error {
	if operand, err := s.treeTranslator.PopOperand(); err != nil {
		return errors.New("expected kind matcher to have one valid operand")
	} else if identifier, isIdentifier := operand.(pgsql.Identifier); !isIdentifier {
		return fmt.Errorf("expected variable for kind matcher reference but found type: %T", operand)
	} else if binding, resolved := s.scope.Lookup(identifier); !resolved {
		return fmt.Errorf("unable to find identifier %s", identifier)
	} else if kindIDs, missing, err := s.kindMapper.MapMatchKinds(kindMatcher.Kinds); err != nil {
		return fmt.Errorf("failed to translate kinds: %w", err)
	} else {
		if missing && kindMatcher.IsExclusive && (binding.DataType == pgsql.NodeComposite || binding.DataType == pgsql.ExpansionRootNode || binding.DataType == pgsql.ExpansionTerminalNode) {
			return newPGKindIDMatcher(s.scope, s.treeTranslator, binding, []int16{}, false)
		}
		return newPGKindIDMatcher(s.scope, s.treeTranslator, binding, kindIDs, kindMatcher.IsExclusive)
	}
}

// nodeKindConstraint requires every requested label. Dropping an unknown label
// would broaden a conjunction, and an empty @> operand would match every node.
func (s *Translator) nodeKindConstraint(alias pgsql.Identifier, kinds graph.Kinds) (pgsql.Expression, error) {
	if len(kinds) == 0 {
		return nil, nil
	}
	ids, missing, err := s.kindMapper.MapMatchKinds(kinds)
	if err != nil {
		return nil, err
	}
	if missing {
		return pgsql.NewLiteral(false, pgsql.Boolean), nil
	}
	return pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{alias, pgsql.ColumnKindIDs}, pgsql.OperatorPGArrayLHSContainsRHS, pgsql.NewLiteral(ids, pgsql.Int2Array)), nil
}
