package commands

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func newPlainCommandContext(t *testing.T) *CommandContext {
	t.Helper()

	ctx := NewCommandContext(context.Background(), nil, NewScope(RunModeREPL), t.TempDir())
	ctx.SetStyledOutputEnabled(false)

	return ctx
}

func TestOptimizeCypherCommandRendersOptimizationReport(t *testing.T) {
	ctx := newPlainCommandContext(t)

	require.NoError(t, optimizeCypherCmd().Fn(ctx, []string{
		"MATCH", "p", "=", "(n:Group)-[:MemberOf*0..]->(m)-[:Enroll]->(ca:EnterpriseCA)",
		"WHERE", "ca.name", "=", "'target'",
		"RETURN", "p",
	}))

	output := ctx.OutputString()
	require.Contains(t, output, "Original Query:\n")
	require.Contains(t, output, "Optimized Query:\n")
	require.Contains(t, output, "Optimization rules considered:\n")
	require.Contains(t, output, " - ConservativePatternReordering: not applied\n")
	require.Contains(t, output, " - PredicateAttachment: APPLIED\n")
	require.Contains(t, output, "Analysis:\n")
	require.Contains(t, output, "Lowering Plan:\n")
	require.Contains(t, output, "Predicate Attachments:\n")
	require.NotContains(t, output, "\x1b[")
}

func TestOptimizeCypherCommandReturnsParseErrors(t *testing.T) {
	ctx := newPlainCommandContext(t)

	err := optimizeCypherCmd().Fn(ctx, nil)
	require.ErrorContains(t, err, "error trying to parse query '[]'")
	require.Empty(t, ctx.OutputString())
}

func TestTranslateToPsqlCommandRendersSQLAndOptionalAST(t *testing.T) {
	ctx := newPlainCommandContext(t)

	require.NoError(t, translateToPsqlCmd().Fn(ctx, []string{
		"-dump-pg-ast",
		"MATCH", "(n)",
		"RETURN", "n",
		"LIMIT", "1",
	}))

	output := ctx.OutputString()
	require.Contains(t, output, "TRANSLATOR AST\n\n")
	require.Contains(t, strings.ToUpper(output), "SELECT")
	require.Contains(t, strings.ToUpper(output), "LIMIT")
	require.NotContains(t, output, "\x1b[")
}

func TestOptimizeCypherCommandIsRegistered(t *testing.T) {
	cmd, found := Registry()["optimize-cypher"]
	require.True(t, found)
	require.NotNil(t, cmd.Fn)
	require.Equal(t, []string{"<...query>"}, cmd.args)
	require.Equal(t, "Optimizes a Cypher query", cmd.help)
	require.Contains(t, SortedCommandNames(), "optimize-cypher")
}
