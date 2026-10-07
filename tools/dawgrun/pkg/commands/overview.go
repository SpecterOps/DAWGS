package commands

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/tools/dawgrun/pkg/overview"
)

func renderOverviewCmd() CommandDesc {
	flagSet := flag.NewFlagSet("render-overview", flag.ContinueOnError)
	outFilePath := ""
	flagSet.StringVar(&outFilePath, "out", "", "Path for writing the HTML overview")

	return CommandDesc{
		args:  []string{"-out", "<output.html>", "<connection>"},
		help:  "Writes a top-level interactive graph overview as HTML",
		desc:  "Groups nodes by their complete kind set and relationships by cluster pair, then writes an interactive HTML overview.",
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
			if len(fields) != 1 || strings.TrimSpace(outFilePath) == "" {
				return fmt.Errorf("invalid usage: render-overview -out <output.html> <connection>")
			}

			connectionName := fields[0]
			connection, err := ctx.EnsureConnection(connectionName)
			if err != nil {
				return err
			}

			accumulator := overview.NewAccumulator()
			if err := connection.ReadTransaction(ctx, func(tx graph.Transaction) error {
				if err := tx.Nodes().FetchKinds(func(cursor graph.Cursor[graph.KindsResult]) error {
					for node := range cursor.Chan() {
						accumulator.AddNode(node.ID, node.Kinds)
					}
					return cursor.Error()
				}); err != nil {
					return fmt.Errorf("fetch graph nodes: %w", err)
				}

				if err := tx.Relationships().FetchKinds(func(cursor graph.Cursor[graph.RelationshipKindsResult]) error {
					for relationship := range cursor.Chan() {
						if err := accumulator.AddRelationship(relationship.StartID, relationship.EndID, relationship.Kind); err != nil {
							return err
						}
					}
					return cursor.Error()
				}); err != nil {
					return fmt.Errorf("fetch graph relationships: %w", err)
				}

				return nil
			}); err != nil {
				return fmt.Errorf("build graph overview: %w", err)
			}

			outFile, err := os.Create(outFilePath)
			if err != nil {
				return fmt.Errorf("could not create overview output %s: %w", outFilePath, err)
			}
			defer outFile.Close()

			summary := accumulator.Summary()
			if err := overview.WriteHTML(outFile, summary); err != nil {
				return err
			}

			fmt.Fprintf(ctx.output, "Wrote overview of %d nodes, %d relationships, %d clusters, and %d connections to %s\n", summary.NodeCount, summary.EdgeCount, len(summary.Nodes), len(summary.Edges), outFilePath)
			return nil
		},
	}
}
