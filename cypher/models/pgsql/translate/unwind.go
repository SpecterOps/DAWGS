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
		// Declare and Export. The frame is marked Synthetic so that downstream
		// projection logic does not treat it as a real SQL FROM source.
		if s.scope.CurrentFrame() == nil {
			if frame, err := s.scope.PushFrame(); err != nil {
				return err
			} else {
				frame.Synthetic = true
				s.query.CurrentPart().Frame = frame
			}
		}

		s.scope.Declare(binding.Identifier)
		return nil
	}
}

// unwindFromClauses converts a slice of UnwindClause into pgsql.FromClause
// entries suitable for inclusion in a SELECT's FROM list.
func unwindFromClauses(clauses []UnwindClause) []pgsql.FromClause {
	fromClauses := make([]pgsql.FromClause, 0, len(clauses))

	for _, clause := range clauses {
		fromClauses = append(fromClauses, pgsql.FromClause{
			Source: pgsql.AliasedExpression{
				Expression: pgsql.FunctionCall{
					Function:   pgsql.FunctionUnnest,
					Parameters: []pgsql.Expression{clause.Expression},
				},
				Alias: pgsql.AsOptionalIdentifier(clause.Binding.Identifier),
			},
		})
	}

	return fromClauses
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
