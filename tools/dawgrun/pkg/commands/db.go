package commands

import (
	"flag"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/davecgh/go-spew/spew"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers"
	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"

	"github.com/specterops/dawgs/tools/dawgrun/pkg/stubs"
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

			connNames := ctx.scope.GetConnectionNames()
			if len(connNames) == 0 {
				fmt.Fprintln(ctx.output, "No open connections")
				return nil
			}

			fmt.Fprintf(ctx.output, "Open connections (%d):\n", len(connNames))
			for _, connName := range connNames {
				fmt.Fprintf(ctx.output, "  %s\n", connName)
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

			driverName := ""
			if strings.TrimSpace(driverOverride) != "" {
				driverName = strings.ToLower(strings.TrimSpace(driverOverride))
			} else if detectedDriverName, err := driverFromConnectionString(connStr); err != nil {
				return err
			} else {
				driverName = detectedDriverName
			}

			config := dawgs.Config{
				ConnectionString: connStr,
			}

			openSuccess := false
			switch driverName {
			case pg.DriverName:
				connPool, err := pg.NewPool(drivers.DatabaseConfiguration{
					Connection: connStr,
				})
				if err != nil {
					return fmt.Errorf("error opening connection pool: %w", err)
				}
				defer func() {
					if !openSuccess {
						connPool.Close()
					}
				}()

				config.Pool = connPool
			case neo4j.DriverName:
				// No additional setup required for Neo4j before dawgs.Open.
			default:
				return fmt.Errorf("unsupported driver %q; expected one of: %s, %s", driverName, pg.DriverName, neo4j.DriverName)
			}

			querier, err := dawgs.Open(ctx, driverName, config)
			if err != nil {
				return fmt.Errorf("error opening %s database connection '%s': %w", driverName, connStr, err)
			}

			if existingConn, ok := ctx.scope.GetConnection(name); ok {
				// Warn+close existing connection before overwriting it
				ctx.output.Warnf("Discarding previous connection for '%s'", name)
				if err := existingConn.Close(ctx); err != nil {
					return fmt.Errorf("could not close previous connection '%s' for overwriting: %w", name, err)
				}

				// Wipe out handles and resources for this connection
				ctx.scope.DropConnection(name)
			}

			fmt.Fprintf(ctx.output, "Opened %s connection '%s'\n", driverName, name)
			ctx.scope.AddConnection(name, querier)
			openSuccess = true

			return nil
		},
	}
}

func driverFromConnectionString(connStr string) (string, error) {
	parsedURL, err := url.Parse(connStr)
	if err != nil {
		return "", fmt.Errorf("could not parse connection string: %w", err)
	}

	switch strings.ToLower(parsedURL.Scheme) {
	case "postgres", "postgresql":
		return pg.DriverName, nil
	case neo4j.DriverName:
		return neo4j.DriverName, nil
	default:
		return "", fmt.Errorf("unknown connection string scheme %q; expected postgres/postgresql or neo4j", parsedURL.Scheme)
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
			if kindMap, err := loadKindMap(ctx, connName); err != nil {
				return fmt.Errorf("could not load kind map: %w", err)
			} else {
				ctx.scope.connKindMaps[connName] = kindMap
				fmt.Fprintf(ctx.output, "Loaded kind map from connection '%s':\n", connName)
				ctx.output.WriteHighlighted(spew.Sdump(kindMap), "golang")
			}

			return nil
		},
	}
}

func loadKindMap(ctx *CommandContext, connName string) (stubs.KindMap, error) {
	conn, ok := ctx.scope.GetConnection(connName)
	if !ok {
		return nil, fmt.Errorf("unknown connection %s; did you `open` it?", connName)
	}

	// Force a refresh from the database backend
	if err := conn.RefreshKinds(ctx); err != nil {
		return nil, fmt.Errorf("could not refresh kinds for connection %s: %w", connName, err)
	}

	// Load kinds list
	kinds, err := conn.FetchKinds(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not fetch kinds for connection %s: %w", connName, err)
	}

	// Coerce a pg.Driver out of the conn
	driver, ok := conn.(*pg.Driver)
	if !ok {
		return nil, fmt.Errorf("connection %s is not a 'pg' connection", connName)
	}

	// Map the kinds to their IDs
	kindIds, err := driver.MapKinds(ctx, kinds)
	if err != nil {
		return nil, fmt.Errorf("could not map kinds to IDs: %s", err)
	}

	kindMap := make(stubs.KindMap)
	for idx, kind := range kinds {
		kindMap[kindIds[idx]] = kind
	}

	ctx.scope.connKindMaps[connName] = kindMap

	return kindMap, nil
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

			kindMap, ok := ctx.scope.connKindMaps[connName]
			if !ok {
				// Try to fetch the kind map if the connection is open
				var err error
				kindMap, err = loadKindMap(ctx, connName)
				if err != nil {
					return fmt.Errorf("could not fetch kind map: %w", err)
				}
			}

			mapper := stubs.MapperFromKindMap(kindMap)
			if kindID, err := mapper.GetIDByKind(graph.StringKind(kindName)); err != nil {
				return fmt.Errorf("could not look up kind: %w", err)
			} else {
				fmt.Fprintf(ctx.output, "Kind %s => %d", kindName, kindID)
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

			kindMap, ok := ctx.scope.connKindMaps[connName]
			if !ok {
				// Try to fetch the kind map if the connection is open
				var err error
				kindMap, err = loadKindMap(ctx, connName)
				if err != nil {
					return fmt.Errorf("could not fetch kind map: %w", err)
				}
			}

			mapper := stubs.MapperFromKindMap(kindMap)
			if kind, err := mapper.GetKindByID(int16(kindID)); err != nil {
				return fmt.Errorf("could not look up kind: %w", err)
			} else {
				fmt.Fprintf(ctx.output, "Kind ID %d => %s", kindID, kind)
			}

			return nil
		},
	}
}
