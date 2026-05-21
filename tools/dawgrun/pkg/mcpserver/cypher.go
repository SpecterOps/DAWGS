package mcpserver

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/specterops/dawgs/graph"

	"github.com/specterops/dawgs/tools/dawgrun/pkg/commands"
	"github.com/specterops/dawgs/tools/dawgrun/pkg/texttools"
)

const (
	defaultQueryLimit = 100
	maxQueryLimit     = 1000
)

type QueryCypherInput struct {
	Connection string `json:"connection" jsonschema:"open connection name"`
	Query      string `json:"query" jsonschema:"CySQL/Cypher query to execute"`
	Limit      int    `json:"limit,omitempty" jsonschema:"maximum rows to return; defaults to 100 and caps at 1000"`
}

type QueryCypherOutput struct {
	Columns   []string         `json:"columns"`
	Rows      []map[string]any `json:"rows"`
	RowCount  int              `json:"row_count"`
	Truncated bool             `json:"truncated"`
}

type ExplainPsqlInput struct {
	Connection string `json:"connection" jsonschema:"open PostgreSQL connection name"`
	Query      string `json:"query" jsonschema:"CySQL/Cypher query to explain"`
}

type ExplainPsqlOutput struct {
	SQL  string   `json:"sql"`
	Plan []string `json:"plan"`
}

func (s *Server) queryCypher(ctx context.Context, _ *mcp.CallToolRequest, input QueryCypherInput) (*mcp.CallToolResult, QueryCypherOutput, error) {
	cmdCtx := s.commandContext(ctx)
	conn, err := cmdCtx.EnsureConnection(input.Connection)
	if err != nil {
		return nil, QueryCypherOutput{}, err
	}

	limit := normalizeQueryLimit(input.Limit)
	output := QueryCypherOutput{Rows: []map[string]any{}}
	err = conn.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query(input.Query, nil)
		if err := result.Error(); err != nil {
			return fmt.Errorf("error running cypher query: %w", err)
		}
		defer result.Close()

		for result.Next() {
			values := result.Values()
			if output.Columns == nil {
				output.Columns = texttools.BuildCypherResultColumns(result.Keys(), len(values))
			}

			if len(output.Rows) >= limit {
				output.Truncated = true
				break
			}

			output.Rows = append(output.Rows, texttools.BuildCypherResultJSONRow(output.Columns, values))
		}

		return result.Error()
	})
	if err != nil {
		return nil, QueryCypherOutput{}, err
	}

	if output.Columns == nil {
		output.Columns = []string{}
	}
	output.RowCount = len(output.Rows)

	return nil, output, nil
}

func (s *Server) explainPsql(ctx context.Context, _ *mcp.CallToolRequest, input ExplainPsqlInput) (*mcp.CallToolResult, ExplainPsqlOutput, error) {
	cmdCtx := s.commandContext(ctx)
	conn, err := cmdCtx.EnsureConnection(input.Connection)
	if err != nil {
		return nil, ExplainPsqlOutput{}, err
	}

	translation, err := commands.TranslateCypherToPsql(cmdCtx, input.Query, commands.TranslateCypherOptions{Connection: input.Connection, SQLFormatDistance: 2})
	if err != nil {
		return nil, ExplainPsqlOutput{}, err
	}

	explainSQLQuery := fmt.Sprintf("EXPLAIN %s", translation.SQL)
	output := ExplainPsqlOutput{SQL: explainSQLQuery}
	err = conn.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Raw(explainSQLQuery, nil)
		if err := result.Error(); err != nil {
			return fmt.Errorf("error running raw query: %w", err)
		}
		defer result.Close()

		for result.Next() {
			var value string
			if err := graph.ScanNextResult(result, &value); err != nil {
				return fmt.Errorf("could not scan EXPLAIN row: %w", err)
			}
			output.Plan = append(output.Plan, value)
		}

		return result.Error()
	})
	if err != nil {
		return nil, ExplainPsqlOutput{}, err
	}

	return nil, output, nil
}

func normalizeQueryLimit(limit int) int {
	if limit <= 0 {
		return defaultQueryLimit
	}
	if limit > maxQueryLimit {
		return maxQueryLimit
	}

	return limit
}
