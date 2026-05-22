package mcpserver

import (
	"context"
	"strconv"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type LoadDBKindsInput struct {
	Connection string `json:"connection" jsonschema:"open PostgreSQL connection name"`
}

type LoadDBKindsOutput struct {
	Output string `json:"output"`
}

type LookupKindInput struct {
	Connection string `json:"connection" jsonschema:"open PostgreSQL connection name"`
	Kind       string `json:"kind" jsonschema:"kind name"`
}

type LookupKindOutput struct {
	Output string `json:"output"`
}

type LookupKindIDInput struct {
	Connection string `json:"connection" jsonschema:"open PostgreSQL connection name"`
	KindID     int16  `json:"kind_id" jsonschema:"kind ID"`
}

type LookupKindIDOutput struct {
	Output string `json:"output"`
}

func (s *Server) loadDBKinds(ctx context.Context, _ *mcp.CallToolRequest, input LoadDBKindsInput) (*mcp.CallToolResult, LoadDBKindsOutput, error) {
	output, err := s.runCommand(ctx, "load-db-kinds", []string{input.Connection})
	if err != nil {
		return nil, LoadDBKindsOutput{}, err
	}

	return nil, LoadDBKindsOutput{Output: output}, nil
}

func (s *Server) lookupKind(ctx context.Context, _ *mcp.CallToolRequest, input LookupKindInput) (*mcp.CallToolResult, LookupKindOutput, error) {
	output, err := s.runCommand(ctx, "lookup-kind", []string{input.Connection, input.Kind})
	if err != nil {
		return nil, LookupKindOutput{}, err
	}

	return nil, LookupKindOutput{Output: output}, nil
}

func (s *Server) lookupKindID(ctx context.Context, _ *mcp.CallToolRequest, input LookupKindIDInput) (*mcp.CallToolResult, LookupKindIDOutput, error) {
	output, err := s.runCommand(ctx, "lookup-kind-id", []string{input.Connection, strconv.FormatInt(int64(input.KindID), 10)})
	if err != nil {
		return nil, LookupKindIDOutput{}, err
	}

	return nil, LookupKindIDOutput{Output: output}, nil
}
