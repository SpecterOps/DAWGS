package frontend

import (
	"github.com/specterops/dawgs/cypher/parser"
)

// DefaultCypherContext contains filter overrides to prevent cypher specified ops. Allows for customized parse filters through NewContext fn.
func DefaultCypherContext() *Context {
	return NewContext(
		&UpdatingNotAllowedClauseFilter{},
		&UpdatingClauseFilter{},
		&ExplicitProcedureInvocationFilter{},
		&ImplicitProcedureInvocationFilter{},
		&SpecifiedParametersFilter{},
	)
}

type ExplicitProcedureInvocationFilter struct {
	BaseVisitor
}

func (s *ExplicitProcedureInvocationFilter) EnterOC_ExplicitProcedureInvocation(ctx *parser.OC_ExplicitProcedureInvocationContext) {
	s.ctx.AddErrors(ErrProcedureInvocationNotSupported)
}

type ImplicitProcedureInvocationFilter struct {
	BaseVisitor
}

func (s *ImplicitProcedureInvocationFilter) EnterOC_ImplicitProcedureInvocation(ctx *parser.OC_ImplicitProcedureInvocationContext) {
	s.ctx.AddErrors(ErrProcedureInvocationNotSupported)
}

type SpecifiedParametersFilter struct {
	BaseVisitor
}

func (s *SpecifiedParametersFilter) EnterOC_Parameter(ctx *parser.OC_ParameterContext) {
	s.ctx.AddErrors(ErrUserSpecifiedParametersNotSupported)
}

type UpdatingNotAllowedClauseFilter struct {
	BaseVisitor
}

func (s *UpdatingNotAllowedClauseFilter) EnterOC_UpdatingClause(ctx *parser.OC_UpdatingClauseContext) {
	s.ctx.AddErrors(ErrUpdateClauseNotSupported)
}

type UpdatingClauseFilter struct {
	BaseVisitor
}

func (s *UpdatingClauseFilter) EnterOC_UpdatingClause(ctx *parser.OC_UpdatingClauseContext) {
}
