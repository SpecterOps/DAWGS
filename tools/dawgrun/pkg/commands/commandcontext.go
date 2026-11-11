package commands

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/tools/dawgrun/pkg/texttools"
	"github.com/specterops/dawgs/tools/dawgrun/pkg/types"
	"github.com/specterops/go-repl"
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
		// appConfigBaseDir is the root directory for dawgrun user configuration.
		appConfigBaseDir string

		// scope is a singleton instance held by the command manager that holds any persistent state for a command.
		scope *Scope
	}
	// CommandDesc defines a command's behavior, arguments, and flag lifecycle.
	CommandDesc struct {
		// Fn is the command function to execute
		Fn CommandFn
		// ClearFlagsFn is used to clear a command's flags after execution
		ClearFlagsFn func()
		// DisallowRunModes specifies which run modes a command must not be allowed in
		DisallowRunModes []RunMode
		// args is a textual representation of the positional args a command accepts
		args []string
		// flags is the stdlib FlagSet the command parses flags with
		flags *flag.FlagSet
		// desc is a brief description of what the command does
		desc string
		// help is a long description of what the command does
		help string
	}
	// CommandOutput accumulates output text and warnings for a command.
	CommandOutput struct {
		warnings            []string
		outputBuilder       strings.Builder
		styledOutputEnabled bool
	}
)

// NewCommandContext creates a command context with a fresh output buffer.
func NewCommandContext(ctx context.Context, instance *repl.Repl, scope *Scope, appConfigBaseDir string) *CommandContext {
	return &CommandContext{
		Context:          ctx,
		output:           NewCommandOutput(true),
		instance:         instance,
		appConfigBaseDir: appConfigBaseDir,
		scope:            scope,
	}
}

func NewCommandOutput(styledOutputEnabled bool) *CommandOutput {
	return &CommandOutput{
		styledOutputEnabled: styledOutputEnabled,
	}
}

// SetStyledOutputEnabled selects whether command output includes terminal styling.
func (s *CommandContext) SetStyledOutputEnabled(enabled bool) {
	s.output.styledOutputEnabled = enabled
}

// defaultConfigPath resolves the shell's default configuration file location.
func (s *CommandContext) defaultConfigPath() string {
	return types.DefaultConfigPath(s.appConfigBaseDir)
}

// warningStyle renders warning text with terminal styling when styled output is enabled.
func (s *CommandContext) warningStyle(text string) string {
	if !s.output.styledOutputEnabled {
		return text
	}

	return lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("202")).
		Render(text)
}

// OutputString renders warnings followed by command output text.
func (s *CommandContext) OutputString() string {
	builder := new(strings.Builder)
	for _, warning := range s.output.warnings {
		fmt.Fprintf(builder, " * %s\n\n", s.warningStyle(warning))
	}

	builder.WriteString(s.output.outputBuilder.String())

	return builder.String()
}

var _ (io.Writer) = (*CommandOutput)(nil)

// Warn appends a warning message to the command output.
func (s *CommandOutput) Warn(text string) {
	s.warnings = append(s.warnings, text)
}

// Warnf formats and appends a warning message to the command output.
func (s *CommandOutput) Warnf(text string, args ...any) {
	s.warnings = append(s.warnings, fmt.Sprintf(text, args...))
}

// Write implements io.Writer for CommandOutput.
func (s *CommandOutput) Write(p []byte) (n int, err error) {
	return s.outputBuilder.Write(p)
}

// WriteIndented writes text after applying tab-based indentation per line.
func (s *CommandOutput) WriteIndented(text string, indentCount int) {
	s.outputBuilder.WriteString(texttools.IndentLines(text, indentCount))
}

// WriteHighlighted writes syntax-highlighted text using the configured style.
func (s *CommandOutput) WriteHighlighted(text, lexer string) {
	style := os.Getenv("DAWGRUN_STYLE")
	if style == "" {
		style = "monokai"
	}

	s.WriteHighlightedWithStyle(text, lexer, style)
}

// WriteHighlightedWithStyle writes syntax-highlighted text with an explicit style.
func (s *CommandOutput) WriteHighlightedWithStyle(text, lexer, style string) {
	if !s.styledOutputEnabled {
		s.outputBuilder.WriteString(text)
		return
	}

	highlighted, err := texttools.HighlightText(text, lexer, style)
	if err != nil {
		s.Warnf("Could not highlight source text: %#v", err)
		s.outputBuilder.WriteString(text)
	} else {
		s.outputBuilder.WriteString(highlighted)
	}
}

// EnsureConnection checks for a currently open connection or fully opens a lazy-loaded connection for a command to operate on
func (s *CommandContext) EnsureConnection(connName string) (graph.Database, error) {
	if conn, ok := s.scope.GetConnection(connName); ok {
		return conn, nil
	}

	connConfig, ok := s.scope.GetConnectionConfig(connName)
	if !ok {
		return nil, fmt.Errorf("connection %s not found; did you `open` it?", connName)
	}

	if _, err := s.OpenConnection(connName, connConfig.ConnectionString, openConnectionOptions{
		driverName:       connConfig.Driver,
		defaultGraphName: "default",
		quiet:            true,
	}); err != nil {
		return nil, fmt.Errorf("could not open configured connection %s: %w", connName, err)
	}

	conn, ok := s.scope.GetConnection(connName)
	if !ok {
		return nil, fmt.Errorf("configured connection %s did not open", connName)
	}

	return conn, nil
}

// OpenConnection fully opens a database connection on behalf of a command
func (s *CommandContext) OpenConnection(name string, connStr string, options openConnectionOptions) (string, error) {
	querier, driverName, err := s.scope.openDatabase(s, connStr, options)
	if err != nil {
		return "", err
	}

	if existingConn, ok := s.scope.GetConnection(name); ok {
		s.output.Warnf("Discarding previous connection for '%s'", name)
		if err := existingConn.Close(s); err != nil {
			_ = querier.Close(s)
			return "", fmt.Errorf("could not close previous connection '%s' for overwriting: %w", name, err)
		}

		s.scope.DropConnection(name)
	}

	s.scope.AddConnection(name, querier, types.ConnectionConfig{
		Driver:           driverName,
		ConnectionString: connStr,
	})
	if !options.quiet {
		fmt.Fprintf(s.output, "Opened %s connection '%s'\n", driverName, name)
	}

	return driverName, nil
}
