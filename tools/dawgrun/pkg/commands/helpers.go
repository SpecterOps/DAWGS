package commands

import (
	"fmt"
	"strings"

	"github.com/alecthomas/chroma/v2/quick"
	cypherFrontend "github.com/specterops/dawgs/cypher/frontend"
	cypherModels "github.com/specterops/dawgs/cypher/models/cypher"
)

func parseQueryArray(fields []string) (*cypherModels.RegularQuery, error) {
	cypherCtx := cypherFrontend.DefaultCypherContext()
	return cypherFrontend.ParseCypher(cypherCtx, strings.Join(fields, " "))
}

func indentLines(text string, indentCount int) string {
	builder := new(strings.Builder)
	for line := range strings.Lines(text) {
		fmt.Fprintf(builder, "%s%s", strings.Repeat("\t", indentCount), line)
	}

	return builder.String()
}

func highlightText(text, lexer, style string) (string, error) {
	builder := new(strings.Builder)
	if err := quick.Highlight(builder, text, lexer, "terminal16m", style); err != nil {
		return "", fmt.Errorf("could not highlight source text `%s`: %w", text, err)
	}

	return builder.String(), nil
}
