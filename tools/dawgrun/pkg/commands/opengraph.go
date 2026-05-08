package commands

import (
	"flag"
	"fmt"
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
			result, err := ctx.session.SaveOpenGraph(ctx, connName, outFilePath)
			if err != nil {
				return fmt.Errorf("could not export opengraph data: %w", err)
			}

			if result.OutputPath == "" {
				ctx.output.WriteHighlighted(result.JSON, "json")
				return nil
			}

			fmt.Fprintf(ctx.output, "Wrote %d nodes and %d edges to %s\n", result.Nodes, result.Edges, result.OutputPath)
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
			result, err := ctx.session.LoadOpenGraph(ctx, connName, inputFilePath)
			if err != nil {
				return err
			}

			fmt.Fprintf(ctx.output, "Loaded %d nodes and %d edges from %s into connection '%s'\n", result.Nodes, result.Edges, result.InputPath, result.Connection)
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

			result, err := ctx.session.CopyOpenGraph(ctx, fields[0], fields[1])
			if err != nil {
				return err
			}

			fmt.Fprintf(ctx.output, "Copied %d nodes and %d edges from connection '%s' to connection '%s'\n", result.Nodes, result.Edges, result.Source, result.Target)
			return nil
		},
	}
}
