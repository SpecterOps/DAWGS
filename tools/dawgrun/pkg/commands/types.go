package commands

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/openengineer/go-repl"
	"github.com/specterops/dawgs/graph"

	"github.com/specterops/dawgs/tools/dawgrun/pkg/stubs"
)

type (
	CommandFn      func(*CommandContext, []string) error
	CommandContext struct {
		context.Context

		// instance is the running instance of the repl.Repl
		instance *repl.Repl
		// output is a convenience type to make issuing warnings and formatting outputs easier
		output *CommandOutput

		// scope is a singleton instance held by the command manager that holds any persistent state for a command.
		scope *Scope
	}
	CommandDesc struct {
		Fn    CommandFn
		args  []string
		flags *flag.FlagSet
		desc  string
		help  string
		state map[string]any
	}
	CommandOutput struct {
		warnings      []string
		outputBuilder strings.Builder
	}
	// Scope is a grab-bag of miscellaneous objects that store "persistent" scope for commands to cross-reference
	Scope struct {
		connections  map[string]graph.Database
		connKindMaps map[string]stubs.KindMap
	}
)

func NewCommandContext(ctx context.Context, instance *repl.Repl, scope *Scope) *CommandContext {
	return &CommandContext{
		Context:  ctx,
		output:   new(CommandOutput),
		instance: instance,
		scope:    scope,
	}
}

func (cc *CommandContext) warningStyle(text string) string {
	return lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("202")).
		Render(text)
}

func (cc *CommandContext) OutputString() string {
	builder := new(strings.Builder)
	for _, warning := range cc.output.warnings {
		fmt.Fprintf(builder, " * %s\n\n", cc.warningStyle(warning))
	}

	builder.WriteString(cc.output.outputBuilder.String())

	return builder.String()
}

var _ (io.Writer) = (*CommandOutput)(nil)

func (co *CommandOutput) Warn(text string) {
	co.warnings = append(co.warnings, text)
}

func (co *CommandOutput) Warnf(text string, args ...any) {
	co.warnings = append(co.warnings, fmt.Sprintf(text, args...))
}

func (co *CommandOutput) Write(p []byte) (n int, err error) {
	return co.outputBuilder.Write(p)
}

func (co *CommandOutput) WriteIndented(text string, indentCount int) {
	co.outputBuilder.WriteString(indentLines(text, indentCount))
}

func (co *CommandOutput) WriteHighlighted(text, lexer, style string) {
	highlighted, err := highlightText(text, lexer, style)
	if err != nil {
		slog.Error("could not highlight source text; writing non-highlighted text to output",
			slog.String("text", text),
			slog.String("error", err.Error()),
		)
		co.outputBuilder.WriteString(text)
	} else {
		co.outputBuilder.WriteString(highlighted)
	}
}

func NewScope() *Scope {
	return &Scope{
		connections:  make(map[string]graph.Database),
		connKindMaps: make(map[string]stubs.KindMap),
	}
}

func (s *Scope) NumConns() int {
	return len(s.connections)
}
