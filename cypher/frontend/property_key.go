package frontend

import (
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/parser"
)

// extractPropertyKeyName decodes a parsed property-key token and records a syntax error when the decoded key is invalid.
func extractPropertyKeyName(ctx *Context, cypherCtx *parser.OC_PropertyKeyNameContext) string {
	name := cypher.UnescapePropertyKeyName(ctx.Exit().(*SymbolicNameOrReservedWordVisitor).Name)
	if err := cypher.ValidatePropertyKeyName(name); err != nil {
		ctx.AddErrors(SyntaxError{
			Line:            cypherCtx.GetStart().GetLine(),
			Column:          cypherCtx.GetStart().GetColumn(),
			OffendingSymbol: cypherCtx.GetText(),
			Message:         err.Error(),
		})
	}

	return name
}
