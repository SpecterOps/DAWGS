package mcpserver

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/specterops/dawgs/opengraph"
)

const maxInlineOpenGraphBytes = 1024 * 1024

type SaveOpenGraphInput struct {
	Connection string `json:"connection" jsonschema:"open connection name"`
	OutputPath string `json:"output_path,omitempty" jsonschema:"optional file path for OpenGraph JSON output"`
}

type SaveOpenGraphOutput struct {
	OutputPath string `json:"output_path,omitempty"`
	Content    string `json:"content,omitempty"`
	Bytes      int    `json:"bytes"`
}

type LoadOpenGraphInput struct {
	Connection string `json:"connection" jsonschema:"open connection name"`
	InputPath  string `json:"input_path" jsonschema:"OpenGraph JSON file path to load"`
}

type LoadOpenGraphOutput struct {
	Nodes int `json:"nodes"`
	Edges int `json:"edges"`
}

type CopyOpenGraphInput struct {
	FromConnection string `json:"from_connection" jsonschema:"source open connection name"`
	ToConnection   string `json:"to_connection" jsonschema:"destination open connection name"`
}

type CopyOpenGraphOutput struct {
	Nodes int `json:"nodes"`
	Edges int `json:"edges"`
}

func (s *Server) saveOpenGraph(ctx context.Context, _ *mcp.CallToolRequest, input SaveOpenGraphInput) (*mcp.CallToolResult, SaveOpenGraphOutput, error) {
	conn, err := s.commandContext(ctx).EnsureConnection(input.Connection)
	if err != nil {
		return nil, SaveOpenGraphOutput{}, err
	}

	outputPath := strings.TrimSpace(input.OutputPath)
	if outputPath != "" {
		outFile, err := os.Create(outputPath)
		if err != nil {
			return nil, SaveOpenGraphOutput{}, fmt.Errorf("could not create output file %s: %w", outputPath, err)
		}
		defer outFile.Close()

		if err := opengraph.Export(ctx, conn, outFile); err != nil {
			return nil, SaveOpenGraphOutput{}, fmt.Errorf("could not export opengraph data: %w", err)
		}

		if fileInfo, err := outFile.Stat(); err == nil {
			return nil, SaveOpenGraphOutput{OutputPath: outputPath, Bytes: int(fileInfo.Size())}, nil
		}

		return nil, SaveOpenGraphOutput{OutputPath: outputPath}, nil
	}

	buffer := new(bytes.Buffer)
	if err := opengraph.Export(ctx, conn, buffer); err != nil {
		return nil, SaveOpenGraphOutput{}, fmt.Errorf("could not export opengraph data: %w", err)
	}
	if buffer.Len() > maxInlineOpenGraphBytes {
		return nil, SaveOpenGraphOutput{}, fmt.Errorf("OpenGraph export is %d bytes, exceeding inline limit %d; provide output_path", buffer.Len(), maxInlineOpenGraphBytes)
	}

	return nil, SaveOpenGraphOutput{Content: buffer.String(), Bytes: buffer.Len()}, nil
}

func (s *Server) loadOpenGraph(ctx context.Context, _ *mcp.CallToolRequest, input LoadOpenGraphInput) (*mcp.CallToolResult, LoadOpenGraphOutput, error) {
	if !s.allowWrites {
		return nil, LoadOpenGraphOutput{}, fmt.Errorf("load_opengraph requires --allow-writes")
	}

	inputFile, err := os.Open(input.InputPath)
	if err != nil {
		return nil, LoadOpenGraphOutput{}, fmt.Errorf("could not open opengraph input file %s: %w", input.InputPath, err)
	}
	defer inputFile.Close()

	conn, err := s.commandContext(ctx).EnsureConnection(input.Connection)
	if err != nil {
		return nil, LoadOpenGraphOutput{}, err
	}
	doc, err := opengraph.ParseDocument(inputFile)
	if err != nil {
		return nil, LoadOpenGraphOutput{}, fmt.Errorf("could not parse opengraph input file %s: %w", input.InputPath, err)
	}
	if _, err := opengraph.WriteGraph(ctx, conn, &doc.Graph); err != nil {
		return nil, LoadOpenGraphOutput{}, fmt.Errorf("could not write opengraph data into connection %s: %w", input.Connection, err)
	}

	return nil, LoadOpenGraphOutput{Nodes: len(doc.Graph.Nodes), Edges: len(doc.Graph.Edges)}, nil
}

func (s *Server) copyOpenGraph(ctx context.Context, _ *mcp.CallToolRequest, input CopyOpenGraphInput) (*mcp.CallToolResult, CopyOpenGraphOutput, error) {
	if !s.allowWrites {
		return nil, CopyOpenGraphOutput{}, fmt.Errorf("copy_opengraph requires --allow-writes")
	}

	if input.FromConnection == input.ToConnection {
		return nil, CopyOpenGraphOutput{}, fmt.Errorf("source and destination connections must differ")
	}

	cmdCtx := s.commandContext(ctx)
	fromConn, err := cmdCtx.EnsureConnection(input.FromConnection)
	if err != nil {
		return nil, CopyOpenGraphOutput{}, err
	}
	toConn, err := cmdCtx.EnsureConnection(input.ToConnection)
	if err != nil {
		return nil, CopyOpenGraphOutput{}, err
	}

	pipeReader, pipeWriter := io.Pipe()
	exportErrCh := make(chan error, 1)
	go func() {
		if err := opengraph.Export(ctx, fromConn, pipeWriter); err != nil {
			_ = pipeWriter.CloseWithError(err)
			exportErrCh <- err
			return
		}

		exportErrCh <- pipeWriter.Close()
	}()

	doc, err := opengraph.ParseDocument(pipeReader)
	if err != nil {
		_ = pipeReader.CloseWithError(err)
	}
	exportErr := <-exportErrCh
	if exportErr != nil {
		return nil, CopyOpenGraphOutput{}, fmt.Errorf("could not export opengraph data from connection %s: %w", input.FromConnection, exportErr)
	}
	if err != nil {
		return nil, CopyOpenGraphOutput{}, fmt.Errorf("could not parse streamed opengraph data from connection %s: %w", input.FromConnection, err)
	}

	if _, err := opengraph.WriteGraph(ctx, toConn, &doc.Graph); err != nil {
		return nil, CopyOpenGraphOutput{}, fmt.Errorf("could not copy opengraph data into connection %s: %w", input.ToConnection, err)
	}

	return nil, CopyOpenGraphOutput{Nodes: len(doc.Graph.Nodes), Edges: len(doc.Graph.Edges)}, nil
}
