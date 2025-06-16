package frontend

import (
	"bytes"
	"errors"
	"strings"

	"github.com/specterops/dawgs/cypher/models/cypher"

	"github.com/antlr4-go/antlr/v4"
	"github.com/specterops/dawgs/cypher/parser"
)

func parseCypher(ctx *Context, input string) (*cypher.RegularQuery, error) {
	var (
		queryBuffer     = bytes.NewBufferString(input)
		lexer           = parser.NewCypherLexer(antlr.NewIoStream(queryBuffer))
		tokenStream     = antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
		parserInst      = parser.NewCypherParser(tokenStream)
		parseTreeWalker = antlr.NewParseTreeWalker()
		queryVisitor    = &QueryVisitor{}
	)

	// Set up the lexer and parser to report errors to the context
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(ctx)

	parserInst.RemoveErrorListeners()
	parserInst.AddErrorListener(ctx)

	// Prime the context with the root level visitor
	ctx.Enter(queryVisitor)

	// Hand off to ANTLR
	parseTreeWalker.Walk(ctx, parserInst.OC_Cypher())

	// Collect errors
	return queryVisitor.Query, errors.Join(ctx.Errors...)
}

func ParseCypher(ctx *Context, input string) (*cypher.RegularQuery, error) {
	if formattedInput := strings.TrimSpace(input); len(formattedInput) == 0 {
		return nil, ErrInvalidInput
	} else {
		return parseCypher(ctx, formattedInput)
	}
}
