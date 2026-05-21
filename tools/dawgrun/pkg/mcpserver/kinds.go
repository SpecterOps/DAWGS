package mcpserver

import (
	"context"
	"strconv"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/specterops/dawgs/graph"

	"github.com/specterops/dawgs/tools/dawgrun/pkg/commands"
	"github.com/specterops/dawgs/tools/dawgrun/pkg/stubs"
)

type LoadDBKindsInput struct {
	Connection string `json:"connection" jsonschema:"open PostgreSQL connection name"`
}

type LoadDBKindsOutput struct {
	Kinds map[string]string `json:"kinds"`
}

type LookupKindInput struct {
	Connection string `json:"connection" jsonschema:"open PostgreSQL connection name"`
	Kind       string `json:"kind" jsonschema:"kind name"`
}

type LookupKindOutput struct {
	Kind   string `json:"kind"`
	KindID int16  `json:"kind_id"`
}

type LookupKindIDInput struct {
	Connection string `json:"connection" jsonschema:"open PostgreSQL connection name"`
	KindID     int16  `json:"kind_id" jsonschema:"kind ID"`
}

type LookupKindIDOutput struct {
	KindID int16  `json:"kind_id"`
	Kind   string `json:"kind"`
}

func (s *Server) loadDBKinds(ctx context.Context, _ *mcp.CallToolRequest, input LoadDBKindsInput) (*mcp.CallToolResult, LoadDBKindsOutput, error) {
	kindMap, err := commands.LoadKindMap(s.commandContext(ctx), input.Connection)
	if err != nil {
		return nil, LoadDBKindsOutput{}, err
	}

	output := LoadDBKindsOutput{Kinds: make(map[string]string, len(kindMap))}
	for kindID, kind := range kindMap {
		output.Kinds[strconv.FormatInt(int64(kindID), 10)] = kind.String()
	}

	return nil, output, nil
}

func (s *Server) lookupKind(ctx context.Context, _ *mcp.CallToolRequest, input LookupKindInput) (*mcp.CallToolResult, LookupKindOutput, error) {
	kindMap, err := commands.LoadKindMap(s.commandContext(ctx), input.Connection)
	if err != nil {
		return nil, LookupKindOutput{}, err
	}
	kindID, err := stubs.MapperFromKindMap(kindMap).GetIDByKind(graph.StringKind(input.Kind))
	if err != nil {
		return nil, LookupKindOutput{}, err
	}

	return nil, LookupKindOutput{Kind: input.Kind, KindID: kindID}, nil
}

func (s *Server) lookupKindID(ctx context.Context, _ *mcp.CallToolRequest, input LookupKindIDInput) (*mcp.CallToolResult, LookupKindIDOutput, error) {
	kindMap, err := commands.LoadKindMap(s.commandContext(ctx), input.Connection)
	if err != nil {
		return nil, LookupKindIDOutput{}, err
	}
	kind, err := stubs.MapperFromKindMap(kindMap).GetKindByID(input.KindID)
	if err != nil {
		return nil, LookupKindIDOutput{}, err
	}

	return nil, LookupKindIDOutput{KindID: input.KindID, Kind: kind.String()}, nil
}
