package mcpserver

import (
	"context"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type SaveOpenGraphInput struct {
	Connection string `json:"connection" jsonschema:"open connection name"`
	OutputPath string `json:"output_path,omitempty" jsonschema:"optional file path for OpenGraph JSON output"`
}

type SaveOpenGraphOutput struct {
	Output string `json:"output"`
}

type LoadOpenGraphInput struct {
	Connection string `json:"connection" jsonschema:"open connection name"`
	InputPath  string `json:"input_path" jsonschema:"OpenGraph JSON file path to load"`
}

type LoadOpenGraphOutput struct {
	Output string `json:"output"`
}

type CopyOpenGraphInput struct {
	FromConnection string `json:"from_connection" jsonschema:"source open connection name"`
	ToConnection   string `json:"to_connection" jsonschema:"destination open connection name"`
}

type CopyOpenGraphOutput struct {
	Output string `json:"output"`
}

func (s *Server) saveOpenGraph(ctx context.Context, _ *mcp.CallToolRequest, input SaveOpenGraphInput) (*mcp.CallToolResult, SaveOpenGraphOutput, error) {
	args := []string{}
	outputPath := strings.TrimSpace(input.OutputPath)
	if outputPath != "" {
		args = append(args, "-out", outputPath)
	}
	args = append(args, input.Connection)

	output, err := s.runCommand(ctx, "save-opengraph", args)
	if err != nil {
		return nil, SaveOpenGraphOutput{}, err
	}

	return nil, SaveOpenGraphOutput{Output: output}, nil
}

func (s *Server) loadOpenGraph(ctx context.Context, _ *mcp.CallToolRequest, input LoadOpenGraphInput) (*mcp.CallToolResult, LoadOpenGraphOutput, error) {
	if !s.allowWrites {
		return nil, LoadOpenGraphOutput{}, fmt.Errorf("load_opengraph requires --allow-writes")
	}

	output, err := s.runCommand(ctx, "load-opengraph", []string{input.Connection, input.InputPath})
	if err != nil {
		return nil, LoadOpenGraphOutput{}, err
	}

	return nil, LoadOpenGraphOutput{Output: output}, nil
}

func (s *Server) copyOpenGraph(ctx context.Context, _ *mcp.CallToolRequest, input CopyOpenGraphInput) (*mcp.CallToolResult, CopyOpenGraphOutput, error) {
	if !s.allowWrites {
		return nil, CopyOpenGraphOutput{}, fmt.Errorf("copy_opengraph requires --allow-writes")
	}

	output, err := s.runCommand(ctx, "copy-opengraph", []string{input.FromConnection, input.ToConnection})
	if err != nil {
		return nil, CopyOpenGraphOutput{}, err
	}

	return nil, CopyOpenGraphOutput{Output: output}, nil
}
