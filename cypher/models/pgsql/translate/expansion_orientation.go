package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/pgd"
)

const (
	orientationRootID             pgsql.Identifier = "root_id"
	orientationDegreeSample       pgsql.Identifier = "sampled"
	orientationRootRows           pgsql.Identifier = "root_rows"
	orientationSuffixRows         pgsql.Identifier = "suffix_rows"
	orientationBoundaryRows       pgsql.Identifier = "boundary_rows"
	orientationForwardDegreeRows  pgsql.Identifier = "forward_degree_rows"
	orientationReverseDegreeRows  pgsql.Identifier = "reverse_degree_rows"
	orientationProbesComplete     pgsql.Identifier = "probes_complete"
	orientationForwardScore       pgsql.Identifier = "forward_score"
	orientationReverseScore       pgsql.Identifier = "reverse_score"
	orientationUseReverse         pgsql.Identifier = "use_reverse"
	orientationWouldSelectReverse pgsql.Identifier = "would_select_reverse"
	orientationShadowSelected     pgsql.Identifier = "selected"
	orientationArmExecuted        pgsql.Identifier = "executed"
)

// expansionOrientationIdentifiers gives every probe, decision, candidate,
// and fallback relation a stable suffix suitable for plan and telemetry
// attribution.
type expansionOrientationIdentifiers struct {
	rootProbe          pgsql.Identifier
	rootPresence       pgsql.Identifier
	suffixProbe        pgsql.Identifier
	boundaries         pgsql.Identifier
	forwardDegreeProbe pgsql.Identifier
	reverseDegreeProbe pgsql.Identifier
	metrics            pgsql.Identifier
	decision           pgsql.Identifier
	admission          pgsql.Identifier
	shadowForward      pgsql.Identifier
	shadowReverse      pgsql.Identifier
	shadowSelection    pgsql.Identifier
	reverseGate        pgsql.Identifier
	reverseSeed        pgsql.Identifier
	reverseSeedRows    pgsql.Identifier
	executedCandidate  pgsql.Identifier
	executedIncumbent  pgsql.Identifier
	candidateBody      pgsql.Identifier
	incumbentBody      pgsql.Identifier
	reverse            pgsql.Identifier
	states             pgsql.Identifier
	incumbent          pgsql.Identifier
}

func newExpansionOrientationIdentifiers(finalFrame pgsql.Identifier) expansionOrientationIdentifiers {
	prefix := string(finalFrame) + "_orientation_"
	return expansionOrientationIdentifiers{
		rootProbe:          pgsql.Identifier(prefix + "root_probe"),
		rootPresence:       pgsql.Identifier(prefix + "root_presence"),
		suffixProbe:        pgsql.Identifier(prefix + "suffix_probe"),
		boundaries:         pgsql.Identifier(prefix + "boundaries"),
		forwardDegreeProbe: pgsql.Identifier(prefix + "forward_degree_probe"),
		reverseDegreeProbe: pgsql.Identifier(prefix + "reverse_degree_probe"),
		metrics:            pgsql.Identifier(prefix + "metrics"),
		decision:           pgsql.Identifier(prefix + "decision"),
		admission:          pgsql.Identifier(prefix + "admission"),
		shadowForward:      pgsql.Identifier(prefix + "shadow_forward"),
		shadowReverse:      pgsql.Identifier(prefix + "shadow_reverse"),
		shadowSelection:    pgsql.Identifier(prefix + "shadow_selection"),
		reverseGate:        pgsql.Identifier(prefix + "reverse_gate"),
		reverseSeed:        pgsql.Identifier(prefix + "reverse_seed"),
		reverseSeedRows:    pgsql.Identifier(prefix + "reverse_seed_rows"),
		executedCandidate:  pgsql.Identifier(prefix + "executed_candidate"),
		executedIncumbent:  pgsql.Identifier(prefix + "executed_incumbent"),
		candidateBody:      pgsql.Identifier(prefix + "candidate_body"),
		incumbentBody:      pgsql.Identifier(prefix + "incumbent_body"),
		reverse:            pgsql.Identifier(prefix + "reverse"),
		states:             pgsql.Identifier(prefix + "states"),
		incumbent:          pgsql.Identifier(prefix + "incumbent"),
	}
}

// pairwiseRelationshipIDUniqueness excludes every repeated relationship in a
// fixed orientation region. This is intentionally explicit: constraints
// attached while translating the incumbent traversal may be partitioned away
// when the region is rebuilt as an independent seed relation.
func pairwiseRelationshipIDUniqueness(relationships []pgsql.Identifier) pgsql.Expression {
	var constraint pgsql.Expression
	for right := 1; right < len(relationships); right++ {
		for left := 0; left < right; left++ {
			constraint = pgsql.OptionalAnd(
				constraint,
				pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{relationships[right], pgsql.ColumnID},
					pgsql.OperatorNotEquals,
					pgsql.CompoundIdentifier{relationships[left], pgsql.ColumnID},
				),
			)
		}
	}
	return constraint
}

// expansionOrientationReverseDominates mirrors orientation-probe-v1's SQL
// hysteresis rule: reverse evidence must be strictly below 75 percent of
// forward evidence, so equality and ties keep the incumbent.
func expansionOrientationReverseDominates(forwardScore, reverseScore int64) bool {
	return reverseScore*optimize.ExpansionSearchOrientationReverseScoreMultiplier < forwardScore*optimize.ExpansionSearchOrientationForwardScoreMultiplier
}

// boundedProbeOverflow detects the cap+1 sentinel row of a bounded relation.
func boundedProbeOverflow(source pgsql.Identifier, limit int64) pgsql.ExistsExpression {
	return pgsql.ExistsExpression{Subquery: pgsql.Subquery{Query: pgsql.Query{
		Body: pgsql.Select{
			Projection: []pgsql.SelectItem{pgsql.NewLiteral(int64(1), pgsql.Int8)},
			From:       []pgsql.FromClause{tableFrom(source)},
		},
		Offset: pgsql.NewLiteral(limit, pgsql.Int8),
		Limit:  pgsql.NewLiteral(int64(1), pgsql.Int8),
	}}}
}

// boundedTraversalStateProbe materializes a cap+1 view over recursive state.
// Orientation families provide their own state columns and retain their
// existing candidate/fallback semantics around this common admission boundary.
func boundedTraversalStateProbe(
	alias, source pgsql.Identifier,
	columns []pgsql.Identifier,
	limit int64,
	executionMarker ...pgsql.Identifier,
) pgsql.CommonTableExpression {
	projection := make(pgsql.Projection, 0, len(columns))
	for _, column := range columns {
		projection = append(projection, pgsql.CompoundIdentifier{source, column})
	}
	query := pgsql.Query{
		Body: pgsql.Select{
			Projection: projection,
			From:       []pgsql.FromClause{tableFrom(source)},
		},
		Limit: pgsql.NewLiteral(limit+1, pgsql.Int8),
	}
	if len(executionMarker) > 0 && executionMarker[0] != "" {
		marker := executionMarker[0]
		bodyAlias := pgsql.Identifier(string(alias) + "_body")
		body := query.Body.(pgsql.Select)
		body.Where = pgsql.CompoundIdentifier{marker, orientationArmExecuted}
		query.Body = body
		query.Offset = pgsql.NewLiteral(int64(0), pgsql.Int8)

		outerProjection := make(pgsql.Projection, 0, len(columns))
		for _, column := range columns {
			outerProjection = append(outerProjection, &pgsql.AliasedExpression{
				Expression: pgsql.CompoundIdentifier{bodyAlias, column},
				Alias:      models.OptionalValue(column),
			})
		}
		query = pgsql.Query{Body: pgsql.Select{
			Projection: outerProjection,
			From: []pgsql.FromClause{{
				Source: pgsql.TableReference{Name: marker.AsCompoundIdentifier()},
				Joins: []pgsql.Join{{
					Table: pgsql.LateralSubquery{Query: query, Binding: models.OptionalValue(bodyAlias)},
					JoinOperator: pgsql.JoinOperator{
						JoinType:   pgsql.JoinTypeInner,
						Constraint: pgsql.NewLiteral(true, pgsql.Boolean),
					},
				}},
			}},
		}}
	}
	return pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: alias},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query:        query,
	}
}

// boundedAdmissionGates returns exact complementary candidate and incumbent
// gates for independent bounded probes. Empty input admits the candidate and
// suppresses fallback; every ordinary orientation supplies at least one gate.
type boundedProbeLimit struct {
	source pgsql.Identifier
	limit  int64
}

func boundedAdmissionGates(probes ...boundedProbeLimit) (candidate, fallback pgsql.Expression) {
	for _, probe := range probes {
		overflow := boundedProbeOverflow(probe.source, probe.limit)
		candidate = pgsql.OptionalAnd(candidate, pgd.Not(overflow))
		if fallback == nil {
			fallback = overflow
		} else {
			fallback = pgsql.NewBinaryExpression(fallback, pgsql.OperatorOr, overflow)
		}
	}
	if candidate == nil {
		candidate = pgsql.NewLiteral(true, pgsql.Boolean)
	}
	if fallback == nil {
		fallback = pgsql.NewLiteral(false, pgsql.Boolean)
	}
	return candidate, fallback
}

func orientationCount(source pgsql.Identifier) pgsql.Subquery {
	return pgsql.Subquery{Query: pgsql.Query{Body: pgsql.Select{
		Projection: pgsql.Projection{pgsql.FunctionCall{
			Function:   pgsql.FunctionCount,
			Parameters: []pgsql.Expression{pgsql.Wildcard{}},
			CastType:   pgsql.Int8,
		}},
		From: []pgsql.FromClause{tableFrom(source)},
	}}}
}

// buildExpansionOrientationRootProbe materializes duplicate-preserving root
// evidence. It is evidence only; candidate and fallback continue to read the
// exact root relation.
func buildExpansionOrientationRootProbe(rootFrame pgsql.Identifier, root *BoundIdentifier, ids expansionOrientationIdentifiers, cap int64) pgsql.CommonTableExpression {
	return pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: ids.rootProbe},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query: pgsql.Query{
			Body: pgsql.Select{
				Projection: pgsql.Projection{&pgsql.AliasedExpression{
					Expression: projectedNodeIDReference(rootFrame, root),
					Alias:      models.OptionalValue(orientationRootID),
				}},
				From: []pgsql.FromClause{tableFrom(rootFrame)},
			},
			Limit: pgsql.NewLiteral(cap+1, pgsql.Int8),
		},
	}
}

func buildExpansionOrientationRootPresence(ids expansionOrientationIdentifiers) pgsql.CommonTableExpression {
	return pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{Name: ids.rootPresence},
		Query: pgsql.Query{
			Body: pgsql.Select{
				Projection: pgsql.Projection{pgsql.NewLiteral(int64(1), pgsql.Int8)},
				From:       []pgsql.FromClause{tableFrom(ids.rootProbe)},
			},
			Limit: pgsql.NewLiteral(int64(1), pgsql.Int8),
		},
	}
}

// buildExpansionOrientationDegreeProbe materializes one evidence row per typed
// adjacency. Each seed row is retained, so duplicate forward roots contribute
// their real work multiplier while reverse boundaries remain distinct.
func buildExpansionOrientationDegreeProbe(
	alias, seedSource, seedColumn pgsql.Identifier,
	edgeAlias, edgeSeedColumn pgsql.Identifier,
	edgeConstraint pgsql.Expression,
	cap int64,
) pgsql.CommonTableExpression {
	return pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: alias},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query: pgsql.Query{
			Body: pgsql.Select{
				Projection: pgsql.Projection{&pgsql.AliasedExpression{
					Expression: pgsql.NewLiteral(true, pgsql.Boolean),
					Alias:      models.OptionalValue(orientationDegreeSample),
				}},
				From: []pgsql.FromClause{{
					Source: pgsql.TableReference{Name: seedSource.AsCompoundIdentifier()},
					Joins: []pgsql.Join{{
						Table: expansionEdgeTableReference(edgeAlias),
						JoinOperator: pgsql.JoinOperator{
							JoinType: pgsql.JoinTypeInner,
							Constraint: pgsql.NewBinaryExpression(
								pgsql.CompoundIdentifier{edgeAlias, edgeSeedColumn},
								pgsql.OperatorEquals,
								pgsql.CompoundIdentifier{seedSource, seedColumn},
							),
						},
					}},
				}},
				Where: edgeConstraint,
			},
			Limit: pgsql.NewLiteral(cap+1, pgsql.Int8),
		},
	}
}

func buildExpansionOrientationMetrics(ids expansionOrientationIdentifiers, caps optimize.ExpansionSearchProbeCaps) pgsql.CommonTableExpression {
	complete := pgsql.OptionalAnd(
		pgd.Not(boundedProbeOverflow(ids.rootProbe, caps.RootRowLimit)),
		pgd.Not(boundedProbeOverflow(ids.suffixProbe, caps.ReverseSeedRowLimit)),
	)
	complete = pgsql.OptionalAnd(complete, pgd.Not(boundedProbeOverflow(ids.forwardDegreeProbe, caps.DirectionalDegreeRowLimit)))
	complete = pgsql.OptionalAnd(complete, pgd.Not(boundedProbeOverflow(ids.reverseDegreeProbe, caps.DirectionalDegreeRowLimit)))

	return pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: ids.metrics},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query: pgsql.Query{Body: pgsql.Select{Projection: pgsql.Projection{
			&pgsql.AliasedExpression{Expression: orientationCount(ids.rootProbe), Alias: models.OptionalValue(orientationRootRows)},
			&pgsql.AliasedExpression{Expression: orientationCount(ids.suffixProbe), Alias: models.OptionalValue(orientationSuffixRows)},
			&pgsql.AliasedExpression{Expression: orientationCount(ids.boundaries), Alias: models.OptionalValue(orientationBoundaryRows)},
			&pgsql.AliasedExpression{Expression: orientationCount(ids.forwardDegreeProbe), Alias: models.OptionalValue(orientationForwardDegreeRows)},
			&pgsql.AliasedExpression{Expression: orientationCount(ids.reverseDegreeProbe), Alias: models.OptionalValue(orientationReverseDegreeRows)},
			&pgsql.AliasedExpression{Expression: complete, Alias: models.OptionalValue(orientationProbesComplete)},
		}}},
	}
}

func buildExpansionOrientationDecision(ids expansionOrientationIdentifiers) pgsql.CommonTableExpression {
	forwardScore := pgsql.NewBinaryExpression(
		pgsql.CompoundIdentifier{ids.metrics, orientationRootRows},
		pgsql.OperatorAdd,
		pgsql.CompoundIdentifier{ids.metrics, orientationForwardDegreeRows},
	)
	reverseScore := pgsql.NewBinaryExpression(
		pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{ids.metrics, orientationSuffixRows},
			pgsql.OperatorAdd,
			pgsql.CompoundIdentifier{ids.metrics, orientationBoundaryRows},
		),
		pgsql.OperatorAdd,
		pgsql.CompoundIdentifier{ids.metrics, orientationReverseDegreeRows},
	)
	dominates := pgsql.NewBinaryExpression(
		pgsql.NewBinaryExpression(pgsql.NewParenthetical(reverseScore), pgsql.OperatorMultiply, pgsql.NewLiteral(optimize.ExpansionSearchOrientationReverseScoreMultiplier, pgsql.Int8)),
		pgsql.OperatorLessThan,
		pgsql.NewBinaryExpression(pgsql.NewParenthetical(forwardScore), pgsql.OperatorMultiply, pgsql.NewLiteral(optimize.ExpansionSearchOrientationForwardScoreMultiplier, pgsql.Int8)),
	)
	useReverse := pgsql.OptionalAnd(pgsql.CompoundIdentifier{ids.metrics, orientationProbesComplete}, dominates)

	return pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: ids.decision},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query: pgsql.Query{Body: pgsql.Select{
			Projection: pgsql.Projection{
				&pgsql.AliasedExpression{Expression: forwardScore, Alias: models.OptionalValue(orientationForwardScore)},
				&pgsql.AliasedExpression{Expression: reverseScore, Alias: models.OptionalValue(orientationReverseScore)},
				&pgsql.AliasedExpression{Expression: pgsql.CompoundIdentifier{ids.metrics, orientationProbesComplete}, Alias: models.OptionalValue(orientationProbesComplete)},
				&pgsql.AliasedExpression{Expression: useReverse, Alias: models.OptionalValue(orientationUseReverse)},
				&pgsql.AliasedExpression{Expression: useReverse, Alias: models.OptionalValue(orientationWouldSelectReverse)},
			},
			From: []pgsql.FromClause{tableFrom(ids.metrics)},
		}},
	}
}

// buildExpansionOrientationShadowMarkers turns the SQL-visible policy result
// into two mutually exclusive, named plan branches. The final one-row relation
// preserves would_select_reverse without adding a column to the public query
// result. JSON EXPLAIN can therefore attribute the shadow choice while the
// incumbent remains the only executable traversal arm.
func buildExpansionOrientationShadowMarkers(ids expansionOrientationIdentifiers) []pgsql.CommonTableExpression {
	shadowMarker := func(alias pgsql.Identifier, selected bool, predicate pgsql.Expression) pgsql.CommonTableExpression {
		return pgsql.CommonTableExpression{
			Alias:        pgsql.TableAlias{Name: alias},
			Materialized: &pgsql.Materialized{Materialized: true},
			Query: pgsql.Query{Body: pgsql.Select{
				Projection: pgsql.Projection{&pgsql.AliasedExpression{
					Expression: pgsql.NewLiteral(selected, pgsql.Boolean),
					Alias:      models.OptionalValue(orientationShadowSelected),
				}},
				From:  []pgsql.FromClause{tableFrom(ids.decision)},
				Where: predicate,
			}},
		}
	}

	forward := shadowMarker(
		ids.shadowForward,
		false,
		pgd.Not(pgsql.CompoundIdentifier{ids.decision, orientationWouldSelectReverse}),
	)
	reverse := shadowMarker(
		ids.shadowReverse,
		true,
		pgsql.CompoundIdentifier{ids.decision, orientationWouldSelectReverse},
	)
	selection := pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: ids.shadowSelection},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query: pgsql.Query{Body: pgsql.SetOperation{
			Operator: pgsql.OperatorUnion,
			All:      true,
			LOperand: pgsql.Select{
				Projection: pgsql.Projection{&pgsql.AliasedExpression{
					Expression: pgsql.CompoundIdentifier{ids.shadowForward, orientationShadowSelected},
					Alias:      models.OptionalValue(orientationWouldSelectReverse),
				}},
				From: []pgsql.FromClause{tableFrom(ids.shadowForward)},
			},
			ROperand: pgsql.Select{
				Projection: pgsql.Projection{&pgsql.AliasedExpression{
					Expression: pgsql.CompoundIdentifier{ids.shadowReverse, orientationShadowSelected},
					Alias:      models.OptionalValue(orientationWouldSelectReverse),
				}},
				From: []pgsql.FromClause{tableFrom(ids.shadowReverse)},
			},
		}},
	}
	incumbent := pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: ids.executedIncumbent},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query: pgsql.Query{Body: pgsql.Select{
			Projection: pgsql.Projection{&pgsql.AliasedExpression{
				Expression: pgsql.FunctionCall{
					Function: pgsql.Identifier("record_traversal_runtime_attestation_v1"),
					Parameters: []pgsql.Expression{
						pgsql.NewLiteral(string(optimize.ExpansionSearchStepwiseForward), pgsql.Text),
						pgsql.NewLiteral("shadow_incumbent", pgsql.Text),
						pgsql.NewLiteral(false, pgsql.Boolean),
					},
					CastType: pgsql.Boolean,
				},
				Alias: models.OptionalValue(orientationArmExecuted),
			}},
			From: []pgsql.FromClause{tableFrom(ids.shadowSelection)},
		}},
	}

	return []pgsql.CommonTableExpression{forward, reverse, selection, incumbent}
}

// buildExpansionOrientationAdmission materializes the recursive-state
// sentinel once. Both execution markers consume this one decision row so the
// cap+1 state relation is not rescanned independently by each gate and receipt.
func buildExpansionOrientationAdmission(ids expansionOrientationIdentifiers, stateLimit int64) pgsql.CommonTableExpression {
	return pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: ids.admission},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query: pgsql.Query{Body: pgsql.Select{
			Projection: pgsql.Projection{
				&pgsql.AliasedExpression{Expression: pgsql.CompoundIdentifier{ids.decision, orientationUseReverse}, Alias: models.OptionalValue(orientationUseReverse)},
				&pgsql.AliasedExpression{Expression: pgsql.CompoundIdentifier{ids.decision, orientationProbesComplete}, Alias: models.OptionalValue(orientationProbesComplete)},
				&pgsql.AliasedExpression{Expression: boundedProbeOverflow(ids.states, stateLimit), Alias: models.OptionalValue[pgsql.Identifier]("state_overflow")},
			},
			From: []pgsql.FromClause{tableFrom(ids.decision)},
		}},
	}
}

// buildExpansionOrientationExecutionMarkers materializes exactly one named
// marker for the arm admitted by the tournament. Unlike recursive-loop row
// counts, these relations remain unambiguous when a selected arm legitimately
// produces no traversal rows. Candidate admission requires both the policy
// choice and a complete state probe; state overflow selects the incumbent.
func buildExpansionOrientationExecutionMarkers(ids expansionOrientationIdentifiers) []pgsql.CommonTableExpression {
	stateOverflow := pgsql.CompoundIdentifier{ids.admission, pgsql.Identifier("state_overflow")}
	stateAdmitted := pgd.Not(stateOverflow)
	useReverse := pgsql.CompoundIdentifier{ids.admission, orientationUseReverse}
	probeOverflow := pgd.Not(pgsql.CompoundIdentifier{ids.admission, orientationProbesComplete})
	candidateGate := pgsql.OptionalAnd(useReverse, stateAdmitted)
	incumbentGate := pgsql.NewBinaryExpression(pgd.Not(useReverse), pgsql.OperatorOr, stateOverflow)
	fallbackExecuted := pgsql.NewBinaryExpression(probeOverflow, pgsql.OperatorOr, stateOverflow)

	marker := func(alias pgsql.Identifier, gate pgsql.Expression, runtimeIdentity, runtimeBranch string, fallback pgsql.Expression) pgsql.CommonTableExpression {
		return pgsql.CommonTableExpression{
			Alias:        pgsql.TableAlias{Name: alias},
			Materialized: &pgsql.Materialized{Materialized: true},
			Query: pgsql.Query{Body: pgsql.Select{
				Projection: pgsql.Projection{&pgsql.AliasedExpression{
					Expression: pgsql.FunctionCall{
						Function: pgsql.Identifier("record_traversal_runtime_attestation_v1"),
						Parameters: []pgsql.Expression{
							pgsql.NewLiteral(runtimeIdentity, pgsql.Text),
							pgsql.NewLiteral(runtimeBranch, pgsql.Text),
							fallback,
						},
						CastType: pgsql.Boolean,
					},
					Alias: models.OptionalValue(orientationArmExecuted),
				}},
				From:  []pgsql.FromClause{tableFrom(ids.admission)},
				Where: gate,
			}},
		}
	}

	return []pgsql.CommonTableExpression{
		marker(ids.executedCandidate, candidateGate, string(optimize.ExpansionSearchSuffixSeededReverse), "suffix_seeded_reverse", pgsql.NewLiteral(false, pgsql.Boolean)),
		marker(ids.executedIncumbent, incumbentGate, string(optimize.ExpansionSearchStepwiseForward), "exact_forward_incumbent", fallbackExecuted),
	}
}

// buildExpansionOrientationReverseSeed puts the policy marker on the outer
// side of a correlated LATERAL boundary scan. PostgreSQL therefore cannot
// initialize the reverse recursion's seed scan when the policy keeps the
// incumbent; the lateral subquery has no invocation row in that case.
func buildExpansionOrientationReverseSeed(ids expansionOrientationIdentifiers) []pgsql.CommonTableExpression {
	gate := pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: ids.reverseGate},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query: pgsql.Query{Body: pgsql.Select{
			Projection: pgsql.Projection{&pgsql.AliasedExpression{
				Expression: pgsql.NewLiteral(true, pgsql.Boolean),
				Alias:      models.OptionalValue(orientationArmExecuted),
			}},
			From:  []pgsql.FromClause{tableFrom(ids.decision)},
			Where: pgsql.CompoundIdentifier{ids.decision, orientationUseReverse},
		}},
	}
	seedRows := pgsql.Query{
		Body: pgsql.Select{
			Projection: pgsql.Projection{&pgsql.AliasedExpression{
				Expression: pgsql.CompoundIdentifier{ids.boundaries, fixedSuffixBoundaryID},
				Alias:      models.OptionalValue(fixedSuffixBoundaryID),
			}},
			From:  []pgsql.FromClause{tableFrom(ids.boundaries)},
			Where: pgsql.CompoundIdentifier{ids.reverseGate, orientationArmExecuted},
		},
		// OFFSET 0 is a deliberate planner boundary for this correlated gate.
		Offset: pgsql.NewLiteral(int64(0), pgsql.Int8),
	}

	seed := pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: ids.reverseSeed},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query: pgsql.Query{Body: pgsql.Select{
			Projection: pgsql.Projection{&pgsql.AliasedExpression{
				Expression: pgsql.CompoundIdentifier{ids.reverseSeedRows, fixedSuffixBoundaryID},
				Alias:      models.OptionalValue(fixedSuffixBoundaryID),
			}},
			From: []pgsql.FromClause{{
				Source: pgsql.TableReference{Name: ids.reverseGate.AsCompoundIdentifier()},
				Joins: []pgsql.Join{{
					Table: pgsql.LateralSubquery{
						Query:   seedRows,
						Binding: models.OptionalValue(ids.reverseSeedRows),
					},
					JoinOperator: pgsql.JoinOperator{
						JoinType:   pgsql.JoinTypeInner,
						Constraint: pgsql.NewLiteral(true, pgsql.Boolean),
					},
				}},
			}},
		}},
	}
	return []pgsql.CommonTableExpression{gate, seed}
}

// gateQueryBehindMarker makes an execution marker the outer relation of a
// correlated LATERAL query. Merely listing a marker after a materialized CTE
// does not prove PostgreSQL avoids initializing that CTE; this dependency does.
func gateQueryBehindMarker(
	marker, bodyAlias pgsql.Identifier,
	query pgsql.Query,
	exposedProjection pgsql.Projection,
) (pgsql.Select, error) {
	body, ok := query.Body.(pgsql.Select)
	if !ok {
		return pgsql.Select{}, fmt.Errorf("gated orientation body must be a select, found %T", query.Body)
	}
	body.Where = pgsql.OptionalAnd(
		body.Where,
		pgsql.CompoundIdentifier{marker, orientationArmExecuted},
	)
	query.Body = body
	// The correlated reference and OFFSET 0 keep the expensive inner query
	// below the marker-driven LATERAL invocation boundary.
	query.Offset = pgsql.NewLiteral(int64(0), pgsql.Int8)

	projection := make(pgsql.Projection, 0, len(exposedProjection))
	for _, item := range exposedProjection {
		alias, ok := selectItemAlias(item)
		if !ok {
			return pgsql.Select{}, fmt.Errorf("gated orientation projection contains an unaliased item %T", item)
		}
		projection = append(projection, &pgsql.AliasedExpression{
			Expression: pgsql.CompoundIdentifier{bodyAlias, alias},
			Alias:      models.OptionalValue(alias),
		})
	}

	return pgsql.Select{
		Projection: projection,
		From: []pgsql.FromClause{{
			Source: pgsql.TableReference{Name: marker.AsCompoundIdentifier()},
			Joins: []pgsql.Join{{
				Table: pgsql.LateralSubquery{
					Query:   query,
					Binding: models.OptionalValue(bodyAlias),
				},
				JoinOperator: pgsql.JoinOperator{
					JoinType:   pgsql.JoinTypeInner,
					Constraint: pgsql.NewLiteral(true, pgsql.Boolean),
				},
			}},
		}},
	}, nil
}

func expansionOrientationStateProbe(decision optimize.ExpansionSearchStrategyDecision, ids expansionOrientationIdentifiers) pgsql.CommonTableExpression {
	return boundedTraversalStateProbe(ids.states, ids.reverse, []pgsql.Identifier{
		fixedSuffixBoundaryID,
		expansionNextID,
		expansionDepth,
		expansionPath,
	}, decision.Admission.StateLimit, ids.reverseGate)
}
