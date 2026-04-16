package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
)

func (s *Translator) prepareUnwind(unwind *cypher.Unwind) error {
	cypherIdentifier := pgsql.Identifier(unwind.Variable.Symbol)

	if binding, err := s.scope.DefineNew(pgsql.UnsetDataType); err != nil {
		return err
	} else {
		s.scope.Alias(cypherIdentifier, binding)

		// If there is no current frame (e.g. standalone unwind without a preceding
		// WITH or MATCH), push a new frame to satisfy the scope requirements of
		// Declare and Export.
		if s.scope.CurrentFrame() == nil {
			if frame, err := s.scope.PushFrame(); err != nil {
				return err
			} else {
				s.query.CurrentPart().Frame = frame
			}
		}

		s.scope.Declare(binding.Identifier)
		return nil
	}
}

func (s *Translator) translateUnwind(unwind *cypher.Unwind) error {
	// Pop variable identifier (pushed by *cypher.Variable Enter handler)
	if variableIdentifier, err := s.treeTranslator.PopOperand(); err != nil {
		return err
	} else if arrayExpression, err := s.treeTranslator.PopOperand(); err != nil {
		return err
	} else {
		if err := RewriteFrameBindings(s.scope, arrayExpression); err != nil {
			return err
		}

		if identifier, isIdentifier := variableIdentifier.(pgsql.Identifier); !isIdentifier {
			return fmt.Errorf("expected identifier for unwind variable but got %T", variableIdentifier)
		} else if binding, isBound := s.scope.Lookup(identifier); !isBound {
			return fmt.Errorf("unable to lookup unwind variable binding %s", identifier)
		} else {
			s.query.CurrentPart().AddUnwindClause(UnwindClause{
				Expression: arrayExpression,
				Binding:    binding,
			})

			s.scope.CurrentFrame().Export(binding.Identifier)
		}
	}

	return nil
}
