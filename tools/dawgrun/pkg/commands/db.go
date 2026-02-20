package commands

import (
	"fmt"
	"strconv"

	"github.com/davecgh/go-spew/spew"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	dawgsPg "github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"

	"github.com/specterops/dawgs/tools/dawgrun/pkg/stubs"
)

func openPGDBCmd() CommandDesc {
	return CommandDesc{
		args: []string{"<name>", "<connection string>"},
		help: "Connects to a specified DAWGS-compatible Postgres DB to do graph introspection.",

		Fn: func(ctx *CommandContext, fields []string) error {
			if len(fields) < 2 {
				return fmt.Errorf("invalid usage: open-pg-db <name> <connection str>")
			}

			name := fields[0]
			connStr := fields[1]
			connPool, err := dawgsPg.NewPool(connStr)
			if err != nil {
				return fmt.Errorf("error opening connection pool: %w", err)
			}

			query, err := dawgs.Open(ctx, "pg", dawgs.Config{
				ConnectionString: connStr,
				Pool:             connPool,
			})
			if err != nil {
				return fmt.Errorf("error opening database connection '%s': %w", connStr, err)
			}

			fmt.Fprintf(ctx.output, "Opened connection '%s': %s\n", name, connStr)
			ctx.scope.connections[name] = query

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
			if kindMap, err := loadKindMap(ctx, connName); err != nil {
				return fmt.Errorf("could not load kind map: %w", err)
			} else {
				ctx.scope.connKindMaps[connName] = kindMap
				fmt.Fprintf(ctx.output, "Loaded kind map from connection '%s':\n", connName)
				ctx.output.WriteHighlighted(spew.Sdump(kindMap), "golang", "monokai")
			}

			return nil
		},
	}
}

func loadKindMap(ctx *CommandContext, connName string) (stubs.KindMap, error) {
	conn, ok := ctx.scope.connections[connName]
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
