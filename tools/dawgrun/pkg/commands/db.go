package commands

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/davecgh/go-spew/spew"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"

	"github.com/specterops/dawgs/tools/dawgrun/pkg/stubs"
	"github.com/specterops/dawgs/tools/dawgrun/pkg/types"
)

func listConnectionsCmd() CommandDesc {
	return CommandDesc{
		args: []string{},
		help: "Lists open and configured named connections",
		desc: "Prints open connection names and configured connection names that have not been opened yet.",

		Fn: func(ctx *CommandContext, fields []string) error {
			if len(fields) != 0 {
				return fmt.Errorf("invalid usage: list-connections")
			}

			openConnNames := ctx.scope.GetConnectionNames()
			unopenedConnNames := ctx.scope.GetUnopenedConnectionNames()
			if len(openConnNames) == 0 && len(unopenedConnNames) == 0 {
				fmt.Fprintln(ctx.output, "No connections")
				return nil
			}

			if len(openConnNames) > 0 {
				fmt.Fprintf(ctx.output, "Open connections (%d):\n", len(openConnNames))
				for _, connName := range openConnNames {
					fmt.Fprintf(ctx.output, "  %s\n", connName)
				}
			}

			if len(openConnNames) > 0 && len(unopenedConnNames) > 0 {
				fmt.Fprint(ctx.output, "\n")
			}

			if len(unopenedConnNames) > 0 {
				fmt.Fprintf(ctx.output, "Configured connections (%d, unopened):\n", len(unopenedConnNames))
				for _, connName := range unopenedConnNames {
					fmt.Fprintf(ctx.output, "  %s\n", connName)
				}
			}

			return nil
		},
	}
}

func openCmd() CommandDesc {
	flagSet := flag.NewFlagSet("open", flag.ContinueOnError)

	driverOverride := ""
	defaultGraphName := "default"
	initGraphOnFail := false

	flagSet.StringVar(&driverOverride, "driver", "", "Driver override: pg or neo4j (default: infer from connection string scheme)")
	flagSet.StringVar(&defaultGraphName, "default-graph", "default", "Graph that should be referenced in graph operations")
	flagSet.BoolVar(&initGraphOnFail, "init-graph", false, "Whether the specified default graph should be created if opening it fails")

	return CommandDesc{
		args:  []string{"[flags]", "<name>", "<connection string>"},
		help:  "Connects to a named DAWGS-compatible backend using a connection string.",
		desc:  "Infers backend driver from the connection string scheme (postgres/postgresql => pg, neo4j => neo4j).",
		flags: flagSet,

		ClearFlagsFn: func() {
			driverOverride = ""
			defaultGraphName = "default"
			initGraphOnFail = false
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

			if strings.TrimSpace(driverOverride) != "" {
				driverOverride = strings.ToLower(strings.TrimSpace(driverOverride))
			}

			_, err := openConnection(ctx, name, connStr, openConnectionOptions{
				driverName:       driverOverride,
				defaultGraphName: defaultGraphName,
				initGraphOnFail:  initGraphOnFail,
			})
			if err != nil {
				return err
			}

			return nil
		},
	}
}

func openConnection(ctx *CommandContext, name string, connStr string, options openConnectionOptions) (string, error) {
	querier, driverName, err := ctx.scope.openDatabase(ctx, connStr, options)
	if err != nil {
		return "", err
	}
	if existingConn, ok := ctx.scope.GetConnection(name); ok {
		ctx.output.Warnf("Discarding previous connection for '%s'", name)
		if err := existingConn.Close(ctx); err != nil {
			_ = querier.Close(ctx)
			return "", fmt.Errorf("could not close previous connection '%s' for overwriting: %w", name, err)
		}
		ctx.scope.DropConnection(name)
	}

	ctx.scope.AddConnection(name, querier, types.ConnectionConfig{
		Driver:           driverName,
		ConnectionString: connStr,
	})
	if !options.quiet {
		fmt.Fprintf(ctx.output, "Opened %s connection '%s'\n", driverName, name)
	}

	return driverName, nil
}

func openDAWGSDatabase(ctx context.Context, connStr string, options openConnectionOptions) (graph.Database, string, error) {
	driverName := strings.TrimSpace(options.driverName)
	if driverName == "" {
		detectedDriverName, err := driverFromConnectionString(connStr)
		if err != nil {
			return nil, "", err
		}

		driverName = detectedDriverName
	}

	if options.defaultGraphName == "" {
		options.defaultGraphName = "default"
	}

	config := dawgs.Config{
		ConnectionString: connStr,
	}

	poolOwnedByDriver := false
	switch driverName {
	case pg.DriverName:
		connPool, err := pg.NewPool(connStr)
		if err != nil {
			return nil, "", fmt.Errorf("error opening connection pool: %w", err)
		}
		defer func() {
			if !poolOwnedByDriver {
				connPool.Close()
			}
		}()

		config.Pool = connPool
	case neo4j.DriverName:
		// No additional setup required for Neo4j before dawgs.Open.
	default:
		return nil, "", fmt.Errorf("unsupported driver %q; expected one of: %s, %s", driverName, pg.DriverName, neo4j.DriverName)
	}

	querier, err := dawgs.Open(ctx, driverName, config)
	if err != nil {
		return nil, "", fmt.Errorf("error opening %s database connection: %w", driverName, err)
	}
	poolOwnedByDriver = true

	openSuccess := false
	defer func() {
		if !openSuccess {
			_ = querier.Close(ctx)
		}
	}()

	defaultGraph := graph.Graph{Name: options.defaultGraphName}
	if err := querier.SetDefaultGraph(ctx, defaultGraph); err != nil {
		if !options.initGraphOnFail {
			return nil, "", fmt.Errorf("could not set default graph: %w", err)
		}

		graphSchema := graph.Schema{
			Graphs:       []graph.Graph{defaultGraph},
			DefaultGraph: defaultGraph,
		}
		if err := querier.AssertSchema(ctx, graphSchema); err != nil {
			return nil, "", fmt.Errorf("could not initialize graph schema: %w", err)
		}
	}

	openSuccess = true
	return querier, driverName, nil
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
	conn, err := ctx.EnsureConnection(connName)
	if err != nil {
		return nil, err
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
