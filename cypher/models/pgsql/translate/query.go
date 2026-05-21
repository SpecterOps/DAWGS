package translate

import (
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
)

func (s *Translator) previousValidFrame(partFrame *Frame) (*Frame, bool) {
	if partFrame.Previous == nil {
		return nil, false
	}

	if currentQueryPart := s.query.CurrentPart(); currentQueryPart.Frame != nil && partFrame.Previous.Binding.Identifier == currentQueryPart.Frame.Binding.Identifier {
		// If the part's previous frame matches the query part's frame identifier then it's possible that
		// this current part is a multipart query part. In this case there still may be a valid frame
		// to source references from
		return currentQueryPart.Frame.Previous, currentQueryPart.Frame.Previous != nil
	}

	return partFrame.Previous, true
}

func (s *Translator) buildMultiPartSinglePartQuery(singlePartQuery *cypher.SinglePartQuery, cteChain []pgsql.CommonTableExpression) error {
	// Earlier multipart sections are rendered as CTEs that the final single-part
	// query can read from.
	currentPart := s.query.CurrentPart()
	currentPart.Model.CommonTableExpressions.Expressions = append(cteChain, currentPart.Model.CommonTableExpressions.Expressions...)

	return nil
}

func (s *Translator) buildSinglePartQuery(singlePartQuery *cypher.SinglePartQuery) error {
	if s.query.CurrentPart().HasDeletions() {
		if err := s.buildDeletions(s.scope); err != nil {
			s.SetError(err)
		}
	}

	// If there was no return specified end the CTE chain with a bare select
	if singlePartQuery.Return == nil {
		if literalReturn, err := pgsql.AsLiteral(1); err != nil {
			s.SetError(err)
		} else {
			s.query.CurrentPart().Model.Body = pgsql.Select{
				Projection: []pgsql.SelectItem{literalReturn},
			}
		}
	} else if err := s.buildTailProjection(); err != nil {
		s.SetError(err)
	}

	return nil
}

func (s *Translator) buildMultiPartQuery(singlePartQuery *cypher.SinglePartQuery) error {
	var multipartCTEChain []pgsql.CommonTableExpression

	for _, part := range s.query.Parts[:len(s.query.Parts)-1] {
		// If the part has an empty inner CTE, make sure to remove it otherwise the keyword will still render
		if len(part.Model.CommonTableExpressions.Expressions) == 0 {
			part.Model.CommonTableExpressions = nil
		}

		// Each non-final query part is a pipeline boundary. Wrap its inline
		// projection as a nested CTE so later parts can reference only exported
		// bindings.
		nextCTE := pgsql.CommonTableExpression{
			Query: *part.Model,
		}

		if part.Frame != nil {
			nextCTE.Alias = pgsql.TableAlias{
				Name: part.Frame.Binding.Identifier,
			}
		}

		if inlineSelect, err := s.buildInlineProjection(part); err != nil {
			return err
		} else {
			nextCTE.Query.Body = inlineSelect
		}

		multipartCTEChain = append(multipartCTEChain, nextCTE)
	}

	if err := s.buildMultiPartSinglePartQuery(singlePartQuery, multipartCTEChain); err != nil {
		return err
	}

	s.translation.Statement = *s.query.CurrentPart().Model
	return nil
}

func (s *Translator) translateMultiPartQueryPart() error {
	queryPart := s.query.CurrentPart()

	// Unwind nested frames
	return s.scope.UnwindToFrame(queryPart.Frame)
}

func (s *Translator) prepareSinglePartQueryPart(singlePartQuery *cypher.SinglePartQuery) error {
	newQueryPart := NewQueryPart(len(singlePartQuery.ReadingClauses), len(singlePartQuery.UpdatingClauses))

	if referencedIdentifiers, err := collectReferencedIdentifiers(singlePartQuery); err != nil {
		return err
	} else {
		newQueryPart.SetReferencedIdentifiers(referencedIdentifiers)
	}

	s.query.AddPart(newQueryPart)
	return nil
}

func (s *Translator) prepareMultiPartQueryPart(multiPartQueryPart *cypher.MultiPartQueryPart) error {
	newQueryPart := NewQueryPart(len(multiPartQueryPart.ReadingClauses), len(multiPartQueryPart.UpdatingClauses))

	if referencedIdentifiers, err := collectReferencedIdentifiers(multiPartQueryPart); err != nil {
		return err
	} else {
		newQueryPart.SetReferencedIdentifiers(referencedIdentifiers)
	}

	// All multipart query parts must be wrapped in a nested CTE
	if mpFrame, err := s.scope.PushFrame(); err != nil {
		return err
	} else {
		newQueryPart.Frame = mpFrame
	}

	s.query.AddPart(newQueryPart)
	return nil
}
