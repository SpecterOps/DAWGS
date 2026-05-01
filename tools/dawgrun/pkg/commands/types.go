package commands

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/charmbracelet/lipgloss"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/go-repl"

	"github.com/specterops/dawgs/tools/dawgrun/pkg/stubs"
)

type (
	// CommandFn is the executable function for a single REPL command invocation.
	CommandFn func(*CommandContext, []string) error
	// CommandContext carries per-invocation context and shared command state.
	CommandContext struct {
		context.Context

		// instance is the running instance of the repl.Repl
		instance *repl.Repl
		// output is a convenience type to make issuing warnings and formatting outputs easier
		output *CommandOutput

		// scope is a singleton instance held by the command manager that holds any persistent state for a command.
		scope *Scope
	}
	// CommandDesc defines a command's behavior, arguments, and flag lifecycle.
	CommandDesc struct {
		// Fn is the command function to execute
		Fn CommandFn
		// ClearFlagsFn is used to clear a command's flags after execution
		ClearFlagsFn func()
		args         []string
		flags        *flag.FlagSet
		desc         string
		help         string
		state        map[string]any
	}
	// CommandOutput accumulates output text and warnings for a command.
	CommandOutput struct {
		warnings      []string
		outputBuilder strings.Builder
	}
	// Scope holds persistent command state that can be shared across invocations.
	Scope struct {
		mu           sync.RWMutex
		connections  map[string]graph.Database
		connKindMaps map[string]stubs.KindMap
	}
)

// NewCommandContext creates a command context with a fresh output buffer.
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

// OutputString renders warnings followed by command output text.
func (cc *CommandContext) OutputString() string {
	builder := new(strings.Builder)
	for _, warning := range cc.output.warnings {
		fmt.Fprintf(builder, " * %s\n\n", cc.warningStyle(warning))
	}

	builder.WriteString(cc.output.outputBuilder.String())

	return builder.String()
}

var _ (io.Writer) = (*CommandOutput)(nil)

// Warn appends a warning message to the command output.
func (co *CommandOutput) Warn(text string) {
	co.warnings = append(co.warnings, text)
}

// Warnf formats and appends a warning message to the command output.
func (co *CommandOutput) Warnf(text string, args ...any) {
	co.warnings = append(co.warnings, fmt.Sprintf(text, args...))
}

// Write implements io.Writer for CommandOutput.
func (co *CommandOutput) Write(p []byte) (n int, err error) {
	return co.outputBuilder.Write(p)
}

// WriteIndented writes text after applying tab-based indentation per line.
func (co *CommandOutput) WriteIndented(text string, indentCount int) {
	co.outputBuilder.WriteString(indentLines(text, indentCount))
}

// WriteHighlighted writes syntax-highlighted text using the configured style.
func (co *CommandOutput) WriteHighlighted(text, lexer string) {
	style := os.Getenv("DAWGRUN_STYLE")
	if style == "" {
		style = "monokai"
	}

	co.WriteHighlightedWithStyle(text, lexer, style)
}

// WriteHighlightedWithStyle writes syntax-highlighted text with an explicit style.
func (co *CommandOutput) WriteHighlightedWithStyle(text, lexer, style string) {
	highlighted, err := highlightText(text, lexer, style)
	if err != nil {
		co.Warnf("Could not highlight source text: %#v", err)
		co.outputBuilder.WriteString(text)
	} else {
		co.outputBuilder.WriteString(highlighted)
	}
}

// NewScope creates an empty shared scope for command state.
func NewScope() *Scope {
	return &Scope{
		connections:  make(map[string]graph.Database),
		connKindMaps: make(map[string]stubs.KindMap),
	}
}

// GetNumConnections returns the number of tracked database connections.
func (s *Scope) GetNumConnections() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.connections)
}

// AddConnection stores or replaces a named database connection in scope.
func (s *Scope) AddConnection(name string, querier graph.Database) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.connections[name] = querier
}

// DropConnection removes a named connection and any cached kind map.
func (s *Scope) DropConnection(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.connections, name)
	delete(s.connKindMaps, name)
}
