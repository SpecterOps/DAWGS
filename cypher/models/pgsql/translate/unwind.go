package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
)

func (s *Translator) prepareUnwindTarget(variable *cypher.Variable) (*BoundIdentifier, bool, error) {
	if _, isUnwindTarget := s.unwindTargets[variable]; !isUnwindTarget {
		return nil, false, nil
	}

	delete(s.unwindTargets, variable)

	binding, err := s.scope.DefineNew(pgsql.UnsetDataType)
	if err != nil {
		return nil, true, err
	}

	s.scope.Alias(pgsql.Identifier(variable.Symbol), binding)
	return binding, true, nil
}

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
	if variableIdentifier, err := s.treeTranslator.PopOperand(); err != nil {
		return err
	} else if arrayExpression, err := s.treeTranslator.PopOperand(); err != nil {
		return err
	} else if err := RewriteFrameBindings(s.scope, arrayExpression); err != nil {
		return err
	} else if identifier, isIdentifier := variableIdentifier.(pgsql.Identifier); !isIdentifier {
		return fmt.Errorf("expected identifier for unwind variable but got %T", variableIdentifier)
	} else if binding, isBound := s.scope.Lookup(identifier); !isBound {
		return fmt.Errorf("unable to lookup unwind variable binding %s", identifier)
	} else {
		if s.scope.CurrentFrame() == nil {
			// A leading UNWIND has no MATCH/CREATE frame to attach to, but
			// later clauses still need a frame that can export the unwind binding.
			frame, err := s.scope.PushFrame()
			if err != nil {
				return err
			}

			frame.Synthetic = true
			s.query.CurrentPart().Frame = frame
		}

		s.scope.Declare(binding.Identifier)
		s.scope.CurrentFrame().Export(binding.Identifier)

		s.query.CurrentPart().AddUnwindClause(UnwindClause{
			Expression: arrayExpression,
			Binding:    binding,
		})
	}

	return nil
}
