package translate

import (
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/walk"
)

const (
	// budgetedSelectionMode identifies automatic query-budgeted lowering.
	budgetedSelectionMode = "automatic_budgeted"

	// budgetedEndpointSelectorVersion identifies resolved endpoint admission.
	budgetedEndpointSelectorVersion = "endpoint-resolution-budgeted-v2"

	// budgetedShortestSelectorVersion identifies per-pair compact DAG discovery.
	budgetedShortestSelectorVersion = "asp-resolved-pairs-v2"

	// budgetedEndpointAdmissionFact reports runtime charging of endpoint records.
	budgetedEndpointAdmissionFact = "query_budgeted_endpoint_admission"

	// budgetedResolvedPairsFact reports runtime validation of each endpoint pair.
	budgetedResolvedPairsFact = "resolved_endpoint_pairs"

	// budgetedMaximumDepthFact reports support for open and explicit maxima.
	budgetedMaximumDepthFact = "open_or_explicit_maximum"

	// budgetedDistinctPairFact records the safe replacement for a same-MATCH
	// endpoint-ID inequality's historical correlated-pair classification.
	budgetedDistinctPairFact = "distinct_endpoint_pair_predicate"

	// budgetedMinimumDepth preserves the positive-length compact search contract.
	budgetedMinimumDepth = 1
)

// UsesBudgetedAllShortestPaths reports whether automatic translation can use
// resolved-pair compact discovery. The driver uses the same selector before
// dispatch to establish a stable snapshot without changing the public query.
func UsesBudgetedAllShortestPaths(query *cypher.RegularQuery) bool {
	if query == nil {
		return false
	}

	// Ordinary queries also use this dispatch helper, including translation
	// cache hits. Avoid full optimization unless the AST contains the feature.
	found := false
	if err := walk.Cypher(query, walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, handler walk.VisitorHandler) {
		if part, ok := node.(*cypher.PatternPart); ok && part.AllShortestPathsPattern {
			found = true
			handler.SetDone()
		}
	})); err != nil || !found {
		return false
	}

	if plan, err := optimize.Optimize(query); err != nil {
		return false
	} else if selected, err := selectAutomaticBudgetedAllShortestPaths(&plan, query); err != nil {
		return false
	} else {
		return selected
	}
}

// budgetedReplacesLegacyShortestFact identifies restrictions superseded by
// resolved-pair admission and the query-wide resource budget.
func budgetedReplacesLegacyShortestFact(name string) bool {
	switch name {
	case optimize.ShortestPathEligibilityStaticEndpointIDs, optimize.ShortestPathEligibilityBoundedDepth,
		optimize.ShortestPathEligibilityPhysicalDepth, optimize.ShortestPathEligibilityPathKindState:
		return true

	default:
		return false
	}
}

// selectBudgetedAllShortestPaths adds the automatic V2 architecture after
// existing tool/canary qualification. Historical forced executors retain their
// original static-ID and finite-depth envelope and SQL identity. Selection
// updates the supplied plan's endpoint and shortest-path lowering decisions.
func selectBudgetedAllShortestPaths(plan *optimize.Plan) bool {
	if len(plan.LoweringPlan.ShortestPathExecutor) == 0 {
		return false
	}

	var (
		selected       bool
		pairPredicates = optimize.BudgetedEndpointPairPredicates(plan.Query)
	)

	for index := range plan.LoweringPlan.ShortestPathExecutor {
		decision := &plan.LoweringPlan.ShortestPathExecutor[index]
		if decision.Family != optimize.ShortestPathFamilyAll || decision.MinimumDepth != budgetedMinimumDepth ||
			decision.ObservationMode != optimize.ShortestPathObservationAllPaths ||
			(decision.MaximumDepthSource == optimize.ShortestPathMaximumDepthExplicit && decision.MaximumDepth < budgetedMinimumDepth) {
			continue
		}

		eligible := true
		for _, fact := range decision.Eligibility {
			if !budgetedReplacesLegacyShortestFact(fact.Name) {
				eligible = eligible && fact.Eligible
			}
		}

		if !eligible {
			continue
		}

		endpointIndex := -1
		pairPredicate := pairPredicates[decision.Target]
		for candidateIndex, endpoint := range plan.LoweringPlan.EndpointResolution {
			if endpoint.Target != decision.Target {
				continue
			}

			endpointEligible := true
			for _, fact := range endpoint.EligibilityFacts {
				// The old small-set cardinality cap is analysis metadata. V2
				// admits every endpoint/pair through the query-wide budget.
				if fact.Name != optimize.EndpointResolutionEligibilityStaticCaps &&
					!(fact.Name == optimize.EndpointResolutionEligibilityUncorrelatedPair && pairPredicate == optimize.EndpointPairPredicateDistinctIDs) {
					endpointEligible = endpointEligible && fact.Eligible
				}
			}

			if endpointEligible {
				endpointIndex = candidateIndex
			}

			break
		}

		if endpointIndex < 0 {
			continue
		}

		endpoint := &plan.LoweringPlan.EndpointResolution[endpointIndex]
		endpoint.SelectedPlan = optimize.EndpointResolutionPlanBudgeted
		endpoint.CandidatePlan = optimize.EndpointResolutionPlanBudgeted
		endpoint.FallbackPlan = ""
		endpoint.PlannedCandidates = append(endpoint.PlannedCandidates, optimize.EndpointResolutionPlanBudgeted)
		endpoint.Root.Limit, endpoint.Root.Sentinel = 0, 0
		endpoint.Terminal.Limit, endpoint.Terminal.Sentinel = 0, 0
		endpoint.Caps = optimize.EndpointResolutionCaps{}
		for factIndex := range endpoint.EligibilityFacts {
			switch endpoint.EligibilityFacts[factIndex].Name {
			case optimize.EndpointResolutionEligibilityStaticCaps:
				endpoint.EligibilityFacts[factIndex] = optimize.EndpointResolutionEligibilityFact{
					Name:     budgetedEndpointAdmissionFact,
					Eligible: true,
				}

			case optimize.EndpointResolutionEligibilityUncorrelatedPair:
				if pairPredicate == optimize.EndpointPairPredicateDistinctIDs {
					endpoint.EligibilityFacts[factIndex] = optimize.EndpointResolutionEligibilityFact{
						Name:     budgetedDistinctPairFact,
						Eligible: true,
					}
				}
			}
		}

		if pairPredicate == optimize.EndpointPairPredicateDistinctIDs {
			endpoint.PairClass = ""
			classes := endpoint.PlannedClasses[:0]
			for _, class := range endpoint.PlannedClasses {
				if class != optimize.EndpointResolutionClassCorrelatedPair {
					classes = append(classes, class)
				}
			}

			endpoint.PlannedClasses = classes
		}

		endpoint.SelectionMode = budgetedSelectionMode
		endpoint.SelectorVersion = budgetedEndpointSelectorVersion
		endpoint.StructurallyEligible = true
		endpoint.StaticallyEligible = true
		endpoint.FallbackReason = ""

		decision.PlannedCandidates = append([]optimize.ShortestPathExecutor{optimize.ShortestPathExecutorASPA1DAGV2}, decision.PlannedCandidates...)
		decision.SelectedExecutor = optimize.ShortestPathExecutorASPA1DAGV2
		decision.EndpointPairPredicate = pairPredicate
		decision.Scheduler = decision.SelectedExecutor.Scheduler()
		decision.ExecutionBoundary = decision.SelectedExecutor.ExecutionBoundary()
		decision.SelectionMode = budgetedSelectionMode
		decision.SelectorVersion = budgetedShortestSelectorVersion
		if decision.MaximumDepthSource == optimize.ShortestPathMaximumDepthUnbounded {
			decision.MaximumDepth = 0
		}

		decision.StructurallyEligible = true
		decision.StaticallyEligible = true
		decision.FallbackReason = ""
		decision.FallbackExecutor = ""

		// V2 reads query-wide budgets at runtime. Historical local candidate
		// caps and their trail-search fallback do not describe this executor.
		decision.StateLimit, decision.FrontierLimit, decision.PredecessorLimit = 0, 0, 0
		decision.EnumerationLimit, decision.OutputBytesLimit = 0, 0
		decision.ExperimentalWinner = false
		facts := decision.Eligibility[:0]
		for _, fact := range decision.Eligibility {
			if !budgetedReplacesLegacyShortestFact(fact.Name) {
				facts = append(facts, fact)
			}
		}

		decision.Eligibility = append(facts,
			optimize.ShortestPathEligibilityFact{
				Name:     budgetedResolvedPairsFact,
				Eligible: true,
			},
			optimize.ShortestPathEligibilityFact{
				Name:     budgetedMaximumDepthFact,
				Eligible: true,
			},
		)

		selected = true
	}

	return selected
}
