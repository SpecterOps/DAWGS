package optimize

import "github.com/specterops/dawgs/cypher/models/cypher"

type Rule interface {
	Name() string
	Apply(*Plan) (bool, error)
}

type analysisPreservingRule interface {
	preservesAnalysis() bool
}

// lazyCopyRule promises that Apply does not mutate the input query unless it
// first calls Plan.EnsureMutable. Built-in rules use this to avoid copying a
// query when no rewrite applies. Third-party rules retain the historical safe
// behavior: the optimizer copies before invoking them.
type lazyCopyRule interface {
	usesLazyCopy() bool
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
	queryCopied          bool
}

// EnsureMutable gives a rule an owned query before it changes the AST. It is
// intentionally idempotent so multiple applied rules share one copy.
func (s *Plan) EnsureMutable() *cypher.RegularQuery {
	if s != nil && !s.queryCopied && s.Query != nil {
		s.Query = cypher.Copy(s.Query)
		s.queryCopied = true
	}

	return s.Query
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
		InboundTraversalReversalRule{},
		PredicateAttachmentRule{},
	}
}

func Optimize(query *cypher.RegularQuery) (Plan, error) {
	return NewOptimizer(DefaultRules()...).Optimize(query)
}

// OptimizeBorrowed optimizes query without copying it up front. The returned
// plan aliases query when no rewrite applies; rules must call EnsureMutable
// before modifying the tree. It is intended for callers that already own the
// input tree and can treat the returned plan as read-only.
func OptimizeBorrowed(query *cypher.RegularQuery) (Plan, error) {
	return NewOptimizer(DefaultRules()...).OptimizeBorrowed(query)
}

// Optimize preserves the historical ownership contract: the returned plan
// always owns its query independently of the caller's input.
func (s Optimizer) Optimize(query *cypher.RegularQuery) (Plan, error) {
	if query == nil {
		return Plan{}, nil
	}

	return s.optimize(cypher.Copy(query), true)
}

// OptimizeBorrowed applies the optimizer without copying query until a rule
// needs to mutate it. See OptimizeBorrowed for ownership details.
func (s Optimizer) OptimizeBorrowed(query *cypher.RegularQuery) (Plan, error) {
	return s.optimize(query, false)
}

func (s Optimizer) optimize(query *cypher.RegularQuery, queryCopied bool) (Plan, error) {
	if query == nil {
		return Plan{}, nil
	}

	plan := Plan{
		Query:       query,
		queryCopied: queryCopied,
	}
	plan.Analysis = Analyze(plan.Query)

	for _, rule := range s.rules {
		if lazyRule, usesLazyCopy := rule.(lazyCopyRule); !usesLazyCopy || !lazyRule.usesLazyCopy() {
			plan.EnsureMutable()
		}

		applied, err := rule.Apply(&plan)
		if err != nil {
			return Plan{}, err
		}

		plan.Rules = append(plan.Rules, RuleResult{
			Name:    rule.Name(),
			Applied: applied,
		})

		if applied && !rulePreservesAnalysis(rule) {
			plan.Analysis = Analyze(plan.Query)
		}
	}

	if loweringPlan, err := BuildLoweringPlan(plan.Query, plan.PredicateAttachments); err != nil {
		return Plan{}, err
	} else {
		plan.LoweringPlan = loweringPlan
	}

	return plan, nil
}

func rulePreservesAnalysis(rule Rule) bool {
	preservingRule, preserves := rule.(analysisPreservingRule)
	return preserves && preservingRule.preservesAnalysis()
}

type PredicateAttachmentRule struct{}

func (s PredicateAttachmentRule) Name() string {
	return "PredicateAttachment"
}

func (s PredicateAttachmentRule) preservesAnalysis() bool {
	return true
}

func (s PredicateAttachmentRule) usesLazyCopy() bool {
	return true
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
				var (
					bindingSymbols = predicateBindingSymbols(predicate, regionBindings)
					scope          = PredicateAttachmentScopeRegion
				)

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
