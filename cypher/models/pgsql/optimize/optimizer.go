package optimize

import "github.com/specterops/dawgs/cypher/models/cypher"

type Rule interface {
	Name() string
	Apply(*Plan) error
}

type RuleResult struct {
	Name    string
	Applied bool
}

type Plan struct {
	Query    *cypher.RegularQuery
	Analysis Analysis
	Rules    []RuleResult
}

type Optimizer struct {
	rules []Rule
}

func NewOptimizer(rules ...Rule) Optimizer {
	return Optimizer{
		rules: rules,
	}
}

func Optimize(query *cypher.RegularQuery) (Plan, error) {
	return NewOptimizer().Optimize(query)
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
		if err := rule.Apply(&plan); err != nil {
			return Plan{}, err
		}

		plan.Rules = append(plan.Rules, RuleResult{
			Name:    rule.Name(),
			Applied: true,
		})
		plan.Analysis = Analyze(plan.Query)
	}

	return plan, nil
}
