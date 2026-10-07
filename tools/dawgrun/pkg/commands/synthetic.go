package commands

import (
	"flag"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/tools/dawgrun/pkg/synthetic"
)

func loadSyntheticCmd() CommandDesc {
	options := synthetic.DefaultOptions()
	clearGraph := false
	flagSet := flag.NewFlagSet("load-synthetic", flag.ContinueOnError)

	flagSet.Float64Var(&options.Scale, "scale", options.Scale, "Scale the reference AD graph size")
	flagSet.Int64Var(&options.Nodes, "nodes", options.Nodes, "Number of nodes (overrides the scaled reference count)")
	flagSet.Int64Var(&options.Edges, "edges", options.Edges, "Number of relationships (overrides the scaled reference count)")
	flagSet.Int64Var(&options.Seed, "seed", options.Seed, "Deterministic graph seed")
	flagSet.IntVar(&options.BatchSize, "batch-size", options.BatchSize, "Number of nodes or relationships per write batch")
	flagSet.BoolVar(&options.IndexStress, "index-stress", options.IndexStress, "Use lower-selectivity indexed string values")
	flagSet.BoolVar(&options.IngestStress, "ingest-stress", options.IngestStress, "Add larger properties and duplicate relationships")
	flagSet.BoolVar(&clearGraph, "clear", clearGraph, "Clear the target graph before loading")

	reset := func() {
		options = synthetic.DefaultOptions()
		clearGraph = false
	}

	return CommandDesc{
		args:  []string{"[flags]", "<connection>"},
		help:  "Loads a deterministic synthetic Active Directory graph",
		desc:  "Generates a production-shaped AD graph directly into a connection; use -index-stress or -ingest-stress for targeted workload shapes.",
		flags: flagSet,

		ClearFlagsFn: reset,

		Fn: func(ctx *CommandContext, fields []string) error {
			reset()
			if err := flagSet.Parse(fields); err != nil {
				return fmt.Errorf("could not parse flags: %w", err)
			}

			fields = flagSet.Args()
			if len(fields) != 1 {
				return fmt.Errorf("invalid usage: load-synthetic [flags] <connection>")
			}

			normalized, err := options.Normalize()
			if err != nil {
				return err
			}

			connName := fields[0]
			conn, err := ctx.EnsureConnection(connName)
			if err != nil {
				return err
			}

			if clearGraph {
				if err := conn.WriteTransaction(ctx, func(tx graph.Transaction) error {
					return tx.Nodes().Delete()
				}); err != nil {
					return fmt.Errorf("clear graph: %w", err)
				}
			}

			startedAt := time.Now()
			stats, err := synthetic.Load(ctx, conn, normalized)
			if err != nil {
				return fmt.Errorf("load synthetic graph: %w", err)
			}

			fmt.Fprintf(ctx.output, "Loaded synthetic AD graph into connection '%s'\n", connName)
			fmt.Fprintf(ctx.output, "Nodes: %d\n", stats.Nodes)
			fmt.Fprintf(ctx.output, "Edges: %d\n", stats.Edges)
			fmt.Fprintf(ctx.output, "Duplicate edges: %d\n", stats.DuplicateEdges)
			fmt.Fprintf(ctx.output, "Node kinds: %s\n", formatSyntheticCounts(stats.NodeKinds))
			fmt.Fprintf(ctx.output, "Edge kinds: %s\n", formatSyntheticCounts(stats.EdgeKinds))
			fmt.Fprintf(ctx.output, "Elapsed: %s\n", time.Since(startedAt).Round(time.Millisecond))

			return nil
		},
	}
}

func formatSyntheticCounts(counts map[string]int64) string {
	keys := slices.Sorted(maps.Keys(counts))
	values := make([]string, 0, len(keys))
	for _, key := range keys {
		values = append(values, fmt.Sprintf("%s=%d", key, counts[key]))
	}

	return strings.Join(values, ", ")
}
