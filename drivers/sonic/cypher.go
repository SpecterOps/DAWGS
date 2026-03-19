package sonic

import (
	"fmt"

	cypher "github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/specterops/dawgs/graph"
)

// executeCypher takes a parsed Cypher RegularQuery and executes it against the in-memory database.
// It uses a walker-based executor that handles arbitrary Cypher clause structures.
func (db *Database) executeCypher(query *cypher.RegularQuery, parameters map[string]any) (*sonicResult, error) {
	if query.SingleQuery == nil {
		return emptyResult(), nil
	}

	ex := newExecutor(db, parameters)

	if err := walk.Cypher(query, ex); err != nil {
		return nil, err
	}

	if ex.result == nil {
		return emptyResult(), nil
	}

	return ex.result, nil
}

// resolveParameters walks the AST and replaces Parameter nodes with their values.
func resolveParameters(where *cypher.Where, parameters map[string]any) {
	if parameters == nil || where == nil {
		return
	}
	for i, expr := range where.Expressions {
		where.Expressions[i] = resolveParamsInExpr(expr, parameters)
	}
}

func resolveParamsInExpr(expr cypher.Expression, params map[string]any) cypher.Expression {
	switch e := expr.(type) {
	case *cypher.Comparison:
		for _, partial := range e.Partials {
			if param, ok := partial.Right.(*cypher.Parameter); ok {
				if val, exists := params[param.Symbol]; exists {
					param.Value = val
				}
			}
		}
	case *cypher.Conjunction:
		for i, sub := range e.Expressions {
			e.Expressions[i] = resolveParamsInExpr(sub, params)
		}
	case *cypher.Disjunction:
		for i, sub := range e.Expressions {
			e.Expressions[i] = resolveParamsInExpr(sub, params)
		}
	case *cypher.Negation:
		e.Expression = resolveParamsInExpr(e.Expression, params)
	case *cypher.Parenthetical:
		e.Expression = resolveParamsInExpr(e.Expression, params)
	case *cypher.FunctionInvocation:
		for i, arg := range e.Arguments {
			e.Arguments[i] = resolveParamsInExpr(arg, params)
		}
	case *cypher.Parameter:
		if val, exists := params[e.Symbol]; exists {
			e.Value = val
		}
	}
	return expr
}

// --- sonicResult implements graph.Result ---

type sonicResult struct {
	rows    [][]any
	keys    []string
	cursor  int
	current []any
	err     error
}

func emptyResult() *sonicResult {
	return &sonicResult{}
}

func (r *sonicResult) Next() bool {
	if r.cursor >= len(r.rows) {
		return false
	}
	r.current = r.rows[r.cursor]
	r.cursor++
	return true
}

func (r *sonicResult) Keys() []string {
	return r.keys
}

func (r *sonicResult) Values() []any {
	return r.current
}

func (r *sonicResult) Mapper() graph.ValueMapper {
	return graph.NewValueMapper(sonicMapValue)
}

func (r *sonicResult) Scan(targets ...any) error {
	if r.current == nil {
		return fmt.Errorf("sonic: no current row")
	}

	mapper := r.Mapper()
	for i, target := range targets {
		if i >= len(r.current) {
			break
		}
		mapper.Map(r.current[i], target)
	}
	return nil
}

func (r *sonicResult) Error() error {
	return r.err
}

func (r *sonicResult) Close() {
}

// sonicMapValue maps in-memory graph types directly to targets.
func sonicMapValue(value, target any) bool {
	switch t := target.(type) {
	case *graph.Node:
		if n, ok := value.(*graph.Node); ok {
			*t = *n
			return true
		}
	case *graph.Relationship:
		if r, ok := value.(*graph.Relationship); ok {
			*t = *r
			return true
		}
	case *graph.Path:
		if p, ok := value.(*graph.Path); ok {
			*t = *p
			return true
		}
	}
	return false
}
