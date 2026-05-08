package commands

import (
	"flag"
	"fmt"
	"strconv"

	"github.com/davecgh/go-spew/spew"

	"github.com/specterops/dawgs/tools/dawgrun/pkg/session"
)

func listConnectionsCmd() CommandDesc {
	return CommandDesc{
		args: []string{},
		help: "Lists currently open named connections",
		desc: "Prints all active connection names for this REPL session.",

		Fn: func(ctx *CommandContext, fields []string) error {
			if len(fields) != 0 {
				return fmt.Errorf("invalid usage: list-connections")
			}

			connections := ctx.session.ListConnections()
			if len(connections) == 0 {
				fmt.Fprintln(ctx.output, "No open connections")
				return nil
			}

			fmt.Fprintf(ctx.output, "Open connections (%d):\n", len(connections))
			for _, connection := range connections {
				fmt.Fprintf(ctx.output, "  %s (%s)\n", connection.Name, connection.Driver)
			}

			return nil
		},
	}
}

func openCmd() CommandDesc {
	flagSet := flag.NewFlagSet("open", flag.ContinueOnError)

	driverOverride := ""
	flagSet.StringVar(&driverOverride, "driver", "", "Driver override: pg or neo4j (default: infer from connection string scheme)")

	return CommandDesc{
		args:  []string{"[flags]", "<name>", "<connection string>"},
		help:  "Connects to a named DAWGS-compatible backend using a connection string.",
		desc:  "Infers backend driver from the connection string scheme (postgres/postgresql => pg, neo4j => neo4j).",
		flags: flagSet,

		ClearFlagsFn: func() {
			driverOverride = ""
		},

		Fn: func(ctx *CommandContext, fields []string) error {
			driverOverride = ""
			if err := flagSet.Parse(fields); err != nil {
				return fmt.Errorf("could not parse flags: %w", err)
			}

			fields = flagSet.Args()
			if len(fields) < 2 {
				return fmt.Errorf("invalid usage: open [flags] <name> <connection str>")
			}

			name := fields[0]
			connStr := fields[1]

			result, err := ctx.session.OpenConnection(ctx, session.OpenConnectionRequest{
				Name:             name,
				ConnectionString: connStr,
				Driver:           driverOverride,
			})
			if err != nil {
				return err
			}
			if result.Replaced {
				ctx.output.Warnf("Discarding previous connection for '%s'", name)
			}

			fmt.Fprintf(ctx.output, "Opened %s connection '%s'\n", result.Connection.Driver, result.Connection.Name)

			return nil
		},
	}
}

func getPGDBKinds() CommandDesc {
	return CommandDesc{
		args: []string{"<connection name>"},
		help: "Loads/shows the kind mapping from the specified DB into the 'active set'",

		Fn: func(ctx *CommandContext, fields []string) error {
			if len(fields) != 1 {
				return fmt.Errorf("invalid usage: load-db-kinds <name>")
			}

			connName := fields[0]
			if kindMap, err := ctx.session.LoadKindMap(ctx, connName); err != nil {
				return fmt.Errorf("could not load kind map: %w", err)
			} else {
				fmt.Fprintf(ctx.output, "Loaded kind map from connection '%s':\n", connName)
				ctx.output.WriteHighlighted(spew.Sdump(kindMap), "golang")
			}

			return nil
		},
	}
}

func lookupKindCmd() CommandDesc {
	return CommandDesc{
		args: []string{"<connection name>", "<kind name>"},
		help: "Looks up a kind from database based on kind name",

		Fn: func(ctx *CommandContext, fields []string) error {
			if len(fields) < 2 {
				return fmt.Errorf("invalid usage: lookup-kind <connection name> <kind name>")
			}

			connName := fields[0]
			kindName := fields[1]

			if result, err := ctx.session.LookupKind(ctx, connName, kindName); err != nil {
				return fmt.Errorf("could not look up kind: %w", err)
			} else {
				fmt.Fprintf(ctx.output, "Kind %s => %d", result["kind_name"], result["kind_id"])
			}

			return nil
		},
	}
}

func lookupKindIDCmd() CommandDesc {
	return CommandDesc{
		args: []string{"<connection name>", "<kind ID>"},
		help: "Looks up a kind from database based on kind ID",

		Fn: func(ctx *CommandContext, fields []string) error {
			if len(fields) < 2 {
				return fmt.Errorf("invalid usage: lookup-kind-id <connection name> <kind ID>")
			}

			connName := fields[0]
			kindIDStr := fields[1]

			kindID, err := strconv.ParseInt(kindIDStr, 10, 16)
			if err != nil {
				return fmt.Errorf("could not parse kind ID as int: %s: %w", kindIDStr, err)
			}

			if result, err := ctx.session.LookupKindID(ctx, connName, int16(kindID)); err != nil {
				return fmt.Errorf("could not look up kind: %w", err)
			} else {
				fmt.Fprintf(ctx.output, "Kind ID %d => %s", result["kind_id"], result["kind_name"])
			}

			return nil
		},
	}
}
