package commands

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/specterops/dawgs/opengraph"
)

func saveOpenGraphCmd() CommandDesc {
	flagSet := flag.NewFlagSet("save-opengraph", flag.ContinueOnError)

	outFilePath := ""
	flagSet.StringVar(&outFilePath, "out", "", "Path for writing OpenGraph JSON output (defaults to console output)")

	return CommandDesc{
		args:  []string{"[flags]", "<connection>"},
		help:  "Dumps all data from a connection as OpenGraph JSON",
		desc:  "Writes pretty OpenGraph JSON to the console by default, or to a file when -out is provided.",
		flags: flagSet,

		ClearFlagsFn: func() {
			outFilePath = ""
		},

		Fn: func(ctx *CommandContext, fields []string) error {
			outFilePath = ""
			if err := flagSet.Parse(fields); err != nil {
				return fmt.Errorf("could not parse flags: %w", err)
			}

			fields = flagSet.Args()
			if len(fields) != 1 {
				return fmt.Errorf("invalid usage: save-opengraph [flags] <connection>")
			}

			connName := fields[0]
			conn, ok := ctx.scope.GetConnection(connName)
			if !ok {
				return fmt.Errorf("connection %s not found; did you `open` it?", connName)
			}

			exportBuffer := new(bytes.Buffer)
			if err := opengraph.Export(ctx, conn, exportBuffer); err != nil {
				return fmt.Errorf("could not export opengraph data: %w", err)
			}

			if strings.TrimSpace(outFilePath) == "" {
				ctx.output.WriteHighlighted(exportBuffer.String(), "json")
				return nil
			}

			outFile, err := os.Create(outFilePath)
			if err != nil {
				return fmt.Errorf("could not create output file %s: %w", outFilePath, err)
			}
			defer outFile.Close()

			if _, err := outFile.Write(exportBuffer.Bytes()); err != nil {
				return fmt.Errorf("could not write output file %s: %w", outFilePath, err)
			}

			doc, err := opengraph.ParseDocument(bytes.NewReader(exportBuffer.Bytes()))
			if err != nil {
				return fmt.Errorf("could not parse exported opengraph document: %w", err)
			}

			fmt.Fprintf(ctx.output, "Wrote %d nodes and %d edges to %s\n", len(doc.Graph.Nodes), len(doc.Graph.Edges), outFilePath)
			return nil
		},
	}
}

func loadOpenGraphCmd() CommandDesc {
	return CommandDesc{
		args: []string{"<connection>", "<inputfilename>"},
		help: "Loads an OpenGraph JSON file into a connection",
		desc: "Parses and validates an OpenGraph data file, then writes all nodes and edges to the specified connection.",

		Fn: func(ctx *CommandContext, fields []string) error {
			if len(fields) != 2 {
				return fmt.Errorf("invalid usage: load-opengraph <connection> <inputfilename>")
			}

			connName := fields[0]
			inputFilePath := fields[1]

			conn, ok := ctx.scope.GetConnection(connName)
			if !ok {
				return fmt.Errorf("connection %s not found; did you `open` it?", connName)
			}

			inputFile, err := os.Open(inputFilePath)
			if err != nil {
				return fmt.Errorf("could not open opengraph input file %s: %w", inputFilePath, err)
			}
			defer inputFile.Close()

			doc, err := opengraph.ParseDocument(inputFile)
			if err != nil {
				return fmt.Errorf("could not parse opengraph input file %s: %w", inputFilePath, err)
			}

			if _, err := opengraph.WriteGraph(ctx, conn, &doc.Graph); err != nil {
				return fmt.Errorf("could not write opengraph data into connection %s: %w", connName, err)
			}

			fmt.Fprintf(ctx.output, "Loaded %d nodes and %d edges from %s into connection '%s'\n", len(doc.Graph.Nodes), len(doc.Graph.Edges), inputFilePath, connName)
			return nil
		},
	}
}

func copyOpenGraphCmd() CommandDesc {
	return CommandDesc{
		args: []string{"<from-connection>", "<to-connection>"},
		help: "Copies all graph data from one connection to another",
		desc: "Exports OpenGraph JSON from one active connection and writes the resulting nodes and edges into another active connection.",

		Fn: func(ctx *CommandContext, fields []string) error {
			if len(fields) != 2 {
				return fmt.Errorf("invalid usage: copy-opengraph <from-connection> <to-connection>")
			}

			fromConnName := fields[0]
			toConnName := fields[1]
			if fromConnName == toConnName {
				return fmt.Errorf("source and destination connections must differ")
			}

			fromConn, ok := ctx.scope.GetConnection(fromConnName)
			if !ok {
				return fmt.Errorf("connection %s not found; did you `open` it?", fromConnName)
			}

			toConn, ok := ctx.scope.GetConnection(toConnName)
			if !ok {
				return fmt.Errorf("connection %s not found; did you `open` it?", toConnName)
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
				return fmt.Errorf("could not export opengraph data from connection %s: %w", fromConnName, exportErr)
			}

			if err != nil {
				return fmt.Errorf("could not parse streamed opengraph data from connection %s: %w", fromConnName, err)
			}

			if _, err := opengraph.WriteGraph(ctx, toConn, &doc.Graph); err != nil {
				return fmt.Errorf("could not copy opengraph data into connection %s: %w", toConnName, err)
			}

			fmt.Fprintf(ctx.output, "Copied %d nodes and %d edges from connection '%s' to connection '%s'\n", len(doc.Graph.Nodes), len(doc.Graph.Edges), fromConnName, toConnName)
			return nil
		},
	}
}
