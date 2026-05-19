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
)

// NewCommandContext creates a command context with a fresh output buffer.
func NewCommandContext(ctx context.Context, instance *repl.Repl, scope *Scope, appConfigBaseDir string) *CommandContext {
	return &CommandContext{
		Context:          ctx,
		output:           new(CommandOutput),
		instance:         instance,
		appConfigBaseDir: appConfigBaseDir,
		scope:            scope,
	}
}

func (cc *CommandContext) defaultConfigPath() string {
	return types.DefaultConfigPath(cc.appConfigBaseDir)
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
	co.outputBuilder.WriteString(texttools.IndentLines(text, indentCount))
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
	highlighted, err := texttools.HighlightText(text, lexer, style)
	if err != nil {
		co.Warnf("Could not highlight source text: %#v", err)
		co.outputBuilder.WriteString(text)
	} else {
		co.outputBuilder.WriteString(highlighted)
	}
}

// EnsureConnection checks for a currently open connection or fully opens a lazy-loaded connection for a command to operate on
func (cc *CommandContext) EnsureConnection(connName string) (graph.Database, error) {
	if conn, ok := cc.scope.GetConnection(connName); ok {
		return conn, nil
	}

	connConfig, ok := cc.scope.GetConnectionConfig(connName)
	if !ok {
		return nil, fmt.Errorf("connection %s not found; did you `open` it?", connName)
	}

	if _, err := cc.OpenConnection(connName, connConfig.ConnectionString, openConnectionOptions{
		driverName:       connConfig.Driver,
		defaultGraphName: "default",
		quiet:            true,
	}); err != nil {
		return nil, fmt.Errorf("could not open configured connection %s: %w", connName, err)
	}

	conn, ok := cc.scope.GetConnection(connName)
	if !ok {
		return nil, fmt.Errorf("configured connection %s did not open", connName)
	}

	return conn, nil
}

// OpenConnection fully opens a database connection on behalf of a command
func (cc *CommandContext) OpenConnection(name string, connStr string, options openConnectionOptions) (string, error) {
	querier, driverName, err := cc.scope.openDatabase(cc, connStr, options)
	if err != nil {
		return "", err
	}

	if existingConn, ok := cc.scope.GetConnection(name); ok {
		cc.output.Warnf("Discarding previous connection for '%s'", name)
		if err := existingConn.Close(cc); err != nil {
			_ = querier.Close(cc)
			return "", fmt.Errorf("could not close previous connection '%s' for overwriting: %w", name, err)
		}

		cc.scope.DropConnection(name)
	}

	cc.scope.AddConnection(name, querier, types.ConnectionConfig{
		Driver:           driverName,
		ConnectionString: connStr,
	})
	if !options.quiet {
		fmt.Fprintf(cc.output, "Opened %s connection '%s'\n", driverName, name)
	}

	return driverName, nil
}
