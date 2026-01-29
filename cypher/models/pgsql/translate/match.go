package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
)

func (s *Translator) translateMatch(match *cypher.Match) error {
	currentQueryPart := s.query.CurrentPart()

	for _, part := range currentQueryPart.ConsumeCurrentPattern().Parts {
		if !part.IsTraversal {
			if err := s.translateNonTraversalPatternPart(part); err != nil {
				return err
			}
		} else {
			if err := s.translateTraversalPatternPart(part, false); err != nil {
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
	}

	if err := s.buildPatternPredicates(); err != nil {
		return err
	}

	// If there is no previous frame, then skip translating an `OPTIONAL MATCH`/treat as plain `MATCH`
	if match.Optional && s.scope.CurrentFrame().Previous != nil {
		return s.translateOptionalMatch()
	}

	return nil
}

func (s *Translator) translateOptionalMatch() error {
	// Building this aggregation step requires pushing another frame onto the scope
	aggrFrame, err := s.scope.PushFrame()
	if err != nil {
		return err
	}

	query, err := s.buildOptionalMatchAggregationStep(aggrFrame)
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

// buildOptionalMatchAggregationStep constructs a "merge" frame to insert after an `OPTIONAL MATCH`,
// which requires a subsequent "aggregation" step to collate the optional match to the initial result set.
func (s *Translator) buildOptionalMatchAggregationStep(aggregationFrame *Frame) (pgsql.Query, error) {
	// An "aggregation" frame like this will only be triggered after an OPTIONAL MATCH, which should only
	// take place AFTER `n>=1` previous MATCH expressions. To properly base the aggregation, we need to
	// join to the origin frame (prior to the OPTIONAL MATCH) based on the OPTIONAL MATCH's frame.
	optMatchFrame := aggregationFrame.Previous
	originFrame := optMatchFrame.Previous
	// originFrame could be nil if no previous frame is defined (for ex., leading OPTIONAL MATCH, which is
	// valid but effectively a plain MATCH)
	if originFrame == nil {
		return pgsql.Query{}, fmt.Errorf("could not get origin frame prior to OPTIONAL MATCH")
	}

	// Construct the join condition based on exports from the "origin" frame
	// We expect the OPTIONAL MATCH frame to also export the same, so that becomes
	// our join anchor between the two CTEs
	var joinConstraints pgsql.Expression
	for _, exported := range originFrame.Exported.Slice() {
		joinConstraints = pgsql.OptionalAnd(
			pgsql.NewParenthetical(
				pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{originFrame.Binding.Identifier, exported},
					pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{optMatchFrame.Binding.Identifier, exported},
				),
			),
			joinConstraints,
		)
	}

	// Construct the projection for this frame. Just take all of the exports for the "origin" frame
	// and optional match frame and re-export them
	// TODO: Does there need to be additional logic for visible/defined bindings, instead of only exports?
	originIDExclusions := map[string]struct{}{}
	projection := pgsql.Projection{}
	for _, exported := range originFrame.Exported.Slice() {
		projection = append(projection, &pgsql.AliasedExpression{
			Expression: pgsql.CompoundIdentifier{originFrame.Binding.Identifier, exported},
			Alias:      pgsql.AsOptionalIdentifier(exported),
		})
		originIDExclusions[exported.String()] = struct{}{}
		aggregationFrame.Export(exported)
	}
	for _, exported := range optMatchFrame.Exported.Slice() {
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
						Constraint: joinConstraints,
					},
				}},
			}},
			Projection: projection,
		},
	}

	return query, nil
}
