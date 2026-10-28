package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
)

func (s *Translator) translateMatch(match *cypher.Match) error {
	currentQueryPart := s.query.CurrentPart()

	// Note the current frame before adding frames from the match
	// so we can use this point for optional match outer joins
	joinFrame := s.scope.CurrentFrame()
	var rowIdentity pgsql.Identifier
	if match.Optional {
		var err error
		joinFrame, rowIdentity, err = s.buildOptionalMatchInput(joinFrame)
		if err != nil {
			return err
		}
	}

	pattern := currentQueryPart.ConsumeCurrentPattern()
	remainingSteps := 0
	for _, part := range pattern.Parts {
		remainingSteps += len(part.TraversalSteps)
	}
	var previousSteps []*TraversalStep
	for _, part := range pattern.Parts {
		remainingSteps -= len(part.TraversalSteps)
		part.previousMatchSteps = previousSteps
		part.hasFollowingMatchTraversal = remainingSteps > 0
		if !part.IsTraversal {
			if err := s.translateNonTraversalPatternPart(part); err != nil {
				return err
			}
		} else {
			if err := s.translateTraversalPatternPart(part, false, !match.Optional); err != nil {
				return err
			}
		}

		// Render this pattern part in the current query part
		if err := s.buildPatternPart(part); err != nil {
			return err
		}

		// Declare the pattern variable in scope if set
		if part.PatternBinding != nil {
			s.scope.Declare(part.PatternBinding.Identifier)
		}
		previousSteps = append(previousSteps, part.TraversalSteps...)
	}

	if err := s.buildPatternPredicates(); err != nil {
		return err
	}
	if err := s.buildPathEdgeIDArrayFutures(); err != nil {
		return err
	}

	if match.Optional {
		return s.translateOptionalMatch(joinFrame, rowIdentity)
	}

	return nil
}

// buildOptionalMatchInput numbers the incoming stream before deriving matches from it.
// The materialized CTE gives both sides of the eventual outer join the same row
// identities, including duplicate values and pending UNWIND rows.
// Without an origin frame, SELECT without FROM supplies the single implicit
// input row for a leading OPTIONAL MATCH, even when the graph is empty.
func (s *Translator) buildOptionalMatchInput(originFrame *Frame) (*Frame, pgsql.Identifier, error) {
	inputFrame, err := s.scope.PushFrame()
	if err != nil {
		return nil, "", err
	}
	identity, err := s.scope.DefineNew(pgsql.Int8)
	if err != nil {
		return nil, "", err
	}

	unwinds := s.query.CurrentPart().ConsumeUnwindClauses()
	unwindIdentifiers := pgsql.NewIdentifierSet()
	for _, unwind := range unwinds {
		unwindIdentifiers.Add(unwind.Binding.Identifier)
	}

	var projection pgsql.Projection
	var inputIdentifiers []pgsql.Identifier
	if originFrame != nil {
		inputIdentifiers = originFrame.Exported.Slice()
	}
	for _, exported := range inputIdentifiers {
		var expression pgsql.Expression = pgsql.CompoundIdentifier{originFrame.Binding.Identifier, exported}
		if unwindIdentifiers.Contains(exported) {
			expression = exported
		}
		projection = append(projection, &pgsql.AliasedExpression{
			Expression: expression,
			Alias:      pgsql.AsOptionalIdentifier(exported),
		})
		binding, found := s.scope.Lookup(exported)
		if !found {
			return nil, "", fmt.Errorf("missing OPTIONAL MATCH input binding: %s", exported)
		}
		binding.MaterializedBy(inputFrame)
	}
	projection = append(projection, &pgsql.AliasedExpression{
		Expression: pgsql.FormattingLiteral("row_number() over ()"),
		Alias:      pgsql.AsOptionalIdentifier(identity.Identifier),
	})
	inputFrame.Export(identity.Identifier)
	identity.MaterializedBy(inputFrame)

	var from []pgsql.FromClause
	if originFrame != nil && !originFrame.Synthetic {
		from = append(from, pgsql.FromClause{Source: pgsql.TableReference{
			Name: pgsql.CompoundIdentifier{originFrame.Binding.Identifier},
		}})
	}
	from = append(from, unwindFromClauses(unwinds)...)
	s.query.CurrentPart().Model.AddCTE(pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: inputFrame.Binding.Identifier},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query:        pgsql.Query{Body: pgsql.Select{Projection: projection, From: from}},
	})
	return inputFrame, identity.Identifier, nil
}

func (s *Translator) translateOptionalMatch(joinFrame *Frame, rowIdentity pgsql.Identifier) error {
	// Building this aggregation step requires pushing another frame onto the scope
	aggrFrame, err := s.scope.PushFrame()
	if err != nil {
		return err
	}

	query, err := s.buildOptionalMatchAggregationStep(aggrFrame, joinFrame, rowIdentity)
	if err != nil {
		return err
	}

	// Attach the aggregation step to the current CTE chain
	s.query.CurrentPart().Model.AddCTE(pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{
			Name: aggrFrame.Binding.Identifier,
		},
		Query: query,
	})

	// For each identifier that is exported by our new frame, update which frame
	// last materialized the identifier, so that references in future and final projections
	// are corrected
	for _, exported := range aggrFrame.Exported.Slice() {
		if boundIdent, exists := s.scope.Lookup(exported); exists {
			boundIdent.MaterializedBy(aggrFrame)
		}
	}

	return nil
}

// buildOptionalMatchAggregationStep merges derived matches back into the numbered
// input stream, preserving one null-extended row for each unmatched input.
func (s *Translator) buildOptionalMatchAggregationStep(aggregationFrame *Frame, originFrame *Frame, rowIdentity pgsql.Identifier) (pgsql.Query, error) {
	optMatchFrame := aggregationFrame.Previous

	if originFrame == nil {
		return pgsql.Query{}, fmt.Errorf("could not get origin frame prior to OPTIONAL MATCH")
	}

	// Match each derived row only to the exact input row that produced it.
	// Equal user-visible values do not establish row identity.
	joinConstraints := pgsql.NewBinaryExpression(
		pgsql.CompoundIdentifier{originFrame.Binding.Identifier, rowIdentity},
		pgsql.OperatorEquals,
		pgsql.CompoundIdentifier{optMatchFrame.Binding.Identifier, rowIdentity},
	)
	// Pattern-predicate futures become available after the match scan is built.
	// Keep those remaining filters on the matched side of the outer join so a
	// rejected match still produces the input's null-extended row.
	remaining, err := s.treeTranslator.ConsumeConstraintsFromVisibleSet(optMatchFrame.Known())
	if err != nil {
		return pgsql.Query{}, err
	}
	if err := RewriteFrameBindings(s.scope, remaining.Expression); err != nil {
		return pgsql.Query{}, err
	}
	optionalConstraints := pgsql.OptionalAnd(joinConstraints, pgsql.OptionalParenthetical(remaining.Expression))

	// Paths can remain expressions over their node/relationship dependencies
	// rather than exported columns. Preserve their visibility for RETURN *.
	aggregationFrame.Visible.MergeSet(optMatchFrame.Visible)

	// The identity is local to this OPTIONAL MATCH. A later optional clause
	// numbers its own input after any fan-out, and DISTINCT/aggregation sees
	// only the Cypher values.
	aggregationFrame.Unexport(rowIdentity)
	aggregationFrame.Visible.Remove(rowIdentity)

	// Construct the projection for this frame. Just take all of the exports for the "origin" frame
	// and optional match frame and re-export them
	// TODO: Does there need to be additional logic for visible/defined bindings, instead of only exports?
	var (
		originIDExclusions = map[string]struct{}{}
		projection         = pgsql.Projection{}
	)

	for _, exported := range originFrame.Exported.Slice() {
		if exported == rowIdentity {
			continue
		}
		projection = append(projection, &pgsql.AliasedExpression{
			Expression: pgsql.CompoundIdentifier{originFrame.Binding.Identifier, exported},
			Alias:      pgsql.AsOptionalIdentifier(exported),
		})
		originIDExclusions[exported.String()] = struct{}{}
		aggregationFrame.Export(exported)
	}
	for _, exported := range optMatchFrame.Exported.Slice() {
		if exported == rowIdentity {
			continue
		}
		// Optional match frame would shadow the origin frame's export with a filtered
		// view of the origin's exports, so make sure not to shadow them
		if _, ok := originIDExclusions[exported.String()]; ok {
			continue
		}

		projection = append(projection, &pgsql.AliasedExpression{
			Expression: pgsql.CompoundIdentifier{optMatchFrame.Binding.Identifier, exported},
			Alias:      pgsql.AsOptionalIdentifier(exported),
		})
		aggregationFrame.Export(exported)
	}

	query := pgsql.Query{
		Body: pgsql.Select{
			// The primary source for the aggregation after an OPTIONAL MATCH should be the "origin" frame
			From: []pgsql.FromClause{{
				Source: pgsql.TableReference{
					Name: pgsql.CompoundIdentifier{originFrame.Binding.Identifier},
				},
				Joins: []pgsql.Join{{
					Table: pgsql.TableReference{
						Name: pgsql.CompoundIdentifier{optMatchFrame.Binding.Identifier},
					},
					JoinOperator: pgsql.JoinOperator{
						JoinType:   pgsql.JoinTypeLeftOuter,
						Constraint: optionalConstraints,
					},
				}},
			}},
			Projection: projection,
		},
	}

	return query, nil
}
