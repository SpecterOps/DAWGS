package translate

import (
	"errors"
	"fmt"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/pgd"
)

func newPGKindIDMatcher(scope *Scope, treeTranslator *ExpressionTreeTranslator, binding *BoundIdentifier, kindIDs []int16) error {
	kindIDsLiteral := pgsql.NewLiteral(kindIDs, pgsql.Int2Array)

	switch binding.DataType {
	case pgsql.NodeComposite, pgsql.ExpansionRootNode, pgsql.ExpansionTerminalNode:
		treeTranslator.PushOperand(pgd.Column(binding.Identifier, pgsql.ColumnKindIDs))
		treeTranslator.PushOperand(kindIDsLiteral)

		return treeTranslator.CompleteBinaryExpression(scope, pgsql.OperatorPGArrayOverlap)

	case pgsql.EdgeComposite, pgsql.ExpansionEdge:
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
	} else if kindIDs, err := s.kindMapper.MapKinds(kindMatcher.Kinds); err != nil {
		return fmt.Errorf("failed to translate kinds: %w", err)
	} else {
		return newPGKindIDMatcher(s.scope, s.treeTranslator, binding, kindIDs)
	}
}
