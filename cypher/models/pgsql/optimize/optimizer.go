package optimize

import "github.com/specterops/dawgs/cypher/models/cypher"

type Rule interface {
	Name() string
	Apply(*Plan) (bool, error)
}

type RuleResult struct {
	Name    string `json:"name"`
	Applied bool   `json:"applied"`
}

type PredicateAttachmentScope string

const (
	PredicateAttachmentScopeBinding PredicateAttachmentScope = "binding"
	PredicateAttachmentScopeRegion  PredicateAttachmentScope = "region"
)

type PredicateAttachment struct {
	QueryPartIndex  int                      `json:"query_part_index"`
	RegionIndex     int                      `json:"region_index"`
	ClauseIndex     int                      `json:"clause_index"`
	ExpressionIndex int                      `json:"expression_index"`
	Scope           PredicateAttachmentScope `json:"scope"`
	BindingSymbols  []string                 `json:"binding_symbols"`
	Dependencies    []string                 `json:"dependencies"`
}

type Plan struct {
	Query                *cypher.RegularQuery
	Analysis             Analysis
	LoweringPlan         LoweringPlan
	Rules                []RuleResult
	PredicateAttachments []PredicateAttachment
}

type Optimizer struct {
	rules []Rule
}

func NewOptimizer(rules ...Rule) Optimizer {
	return Optimizer{
		rules: rules,
	}
}

func DefaultRules() []Rule {
	return []Rule{
		ConservativePatternReorderingRule{},
		PredicateAttachmentRule{},
	}
}

func Optimize(query *cypher.RegularQuery) (Plan, error) {
	return NewOptimizer(DefaultRules()...).Optimize(query)
}

func (s Optimizer) Optimize(query *cypher.RegularQuery) (Plan, error) {
	if query == nil {
		return Plan{}, nil
	}

	plan := Plan{
		Query: cypher.Copy(query),
	}
	plan.Analysis = Analyze(plan.Query)

	for _, rule := range s.rules {
		applied, err := rule.Apply(&plan)
		if err != nil {
			return Plan{}, err
		}

		plan.Rules = append(plan.Rules, RuleResult{
			Name:    rule.Name(),
			Applied: applied,
		})
		plan.Analysis = Analyze(plan.Query)
	}

	if loweringPlan, err := BuildLoweringPlan(plan.Query, plan.Analysis); err != nil {
		return Plan{}, err
	} else {
		plan.LoweringPlan = loweringPlan
	}

	return plan, nil
}

type PredicateAttachmentRule struct{}

func (s PredicateAttachmentRule) Name() string {
	return "PredicateAttachment"
}

func (s PredicateAttachmentRule) Apply(plan *Plan) (bool, error) {
	plan.PredicateAttachments = AttachPredicates(plan.Analysis)
	return len(plan.PredicateAttachments) > 0, nil
}

func AttachPredicates(analysis Analysis) []PredicateAttachment {
	var attachments []PredicateAttachment

	for _, queryPart := range analysis.QueryParts {
		for regionIndex, region := range queryPart.Regions {
			regionBindings := regionBindingSymbols(region)

			for _, predicate := range region.Predicates {
				bindingSymbols := predicateBindingSymbols(predicate, regionBindings)
				scope := PredicateAttachmentScopeRegion

				if len(bindingSymbols) == 1 && len(predicate.Dependencies) == 1 {
					scope = PredicateAttachmentScopeBinding
				}

				attachments = append(attachments, PredicateAttachment{
					QueryPartIndex:  region.QueryPartIndex,
					RegionIndex:     regionIndex,
					ClauseIndex:     predicate.ClauseIndex,
					ExpressionIndex: predicate.ExpressionIndex,
					Scope:           scope,
					BindingSymbols:  copyStrings(bindingSymbols),
					Dependencies:    copyStrings(predicate.Dependencies),
				})
			}
		}
	}

	return attachments
}

func regionBindingSymbols(region Region) map[string]struct{} {
	bindings := map[string]struct{}{}

	for _, binding := range region.Bindings {
		bindings[binding.Symbol] = struct{}{}
	}

	return bindings
}

func predicateBindingSymbols(predicate Predicate, regionBindings map[string]struct{}) []string {
	var bindingSymbols []string

	for _, dependency := range predicate.Dependencies {
		if _, isRegionBinding := regionBindings[dependency]; isRegionBinding {
			bindingSymbols = append(bindingSymbols, dependency)
		}
	}

	return bindingSymbols
}

func copyStrings(values []string) []string {
	if values == nil {
		return nil
	}

	return append([]string(nil), values...)
}
