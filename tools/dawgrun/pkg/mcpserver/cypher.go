package mcpserver

import (
	"context"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type QueryCypherInput struct {
	Connection string `json:"connection" jsonschema:"open connection name"`
	Query      string `json:"query" jsonschema:"CySQL/Cypher query to execute"`
}

type QueryCypherOutput struct {
	Output string `json:"output"`
}

type ExplainPsqlInput struct {
	Connection string `json:"connection" jsonschema:"open PostgreSQL connection name"`
	Query      string `json:"query" jsonschema:"CySQL/Cypher query to explain"`
}

type ExplainPsqlOutput struct {
	Output string `json:"output"`
}

func (s *Server) queryCypher(ctx context.Context, _ *mcp.CallToolRequest, input QueryCypherInput) (*mcp.CallToolResult, QueryCypherOutput, error) {
	output, err := s.runCommand(ctx, "query-cypher", []string{"-format", "json", input.Connection, input.Query})
	if err != nil {
		return nil, QueryCypherOutput{}, err
	}

	return nil, QueryCypherOutput{Output: output}, nil
}

func (s *Server) explainPsql(ctx context.Context, _ *mcp.CallToolRequest, input ExplainPsqlInput) (*mcp.CallToolResult, ExplainPsqlOutput, error) {
	output, err := s.runCommand(ctx, "explain-psql", []string{input.Connection, input.Query})
	if err != nil {
		return nil, ExplainPsqlOutput{}, err
	}

	return nil, ExplainPsqlOutput{Output: output}, nil
}
