package texttools

import (
	"fmt"
	"strings"

	"github.com/alecthomas/chroma/v2/quick"
)

func IndentLines(text string, indentCount int) string {
	builder := new(strings.Builder)
	for line := range strings.Lines(text) {
		fmt.Fprintf(builder, "%s%s", strings.Repeat("\t", indentCount), line)
	}

	return builder.String()
}

func HighlightText(text, lexer, style string) (string, error) {
	builder := new(strings.Builder)
	if err := quick.Highlight(builder, text, lexer, "terminal256", style); err != nil {
		return "", fmt.Errorf("could not highlight source text: %w", err)
	}

	return builder.String(), nil
}
