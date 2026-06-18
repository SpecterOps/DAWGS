package main

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/retriever"
	"github.com/specterops/dawgs/util/size"
)

type databaseConfig struct {
	Driver     string
	Connection string
	Graph      string
}

func openDatabase(ctx context.Context, cfg databaseConfig) (graph.Database, string, error) {
	connection := strings.TrimSpace(cfg.Connection)
	if connection == "" {
		return nil, "", fmt.Errorf("database connection is required; pass -connection or set CONNECTION_STRING")
	}

	driverName := strings.TrimSpace(cfg.Driver)
	if driverName == "" {
		if inferredDriverName, err := driverFromConnectionString(connection); err != nil {
			return nil, "", err
		} else {
			driverName = inferredDriverName
		}
	}

	openConfig := dawgs.Config{
		ConnectionString:      connection,
		GraphQueryMemoryLimit: size.Gibibyte,
	}

	poolOwnedByDriver := false

	switch driverName {
	case pg.DriverName:
		if poolCfg, err := pgxpool.ParseConfig(connection); err != nil {
			return nil, "", fmt.Errorf("parse PostgreSQL pool configuration: %w", err)
		} else if pool, err := pg.NewPool(poolCfg); err != nil {
			return nil, "", fmt.Errorf("open PostgreSQL pool: %w", err)
		} else {
			defer func() {
				if !poolOwnedByDriver {
					pool.Close()
				}
			}()
			openConfig.Pool = pool
		}

	case neo4j.DriverName:
		// No driver-specific setup is required for Neo4j.

	default:
		return nil, "", fmt.Errorf("unsupported driver %q; expected %s or %s", driverName, pg.DriverName, neo4j.DriverName)
	}

	db, err := dawgs.Open(ctx, driverName, openConfig)
	if err != nil {
		return nil, "", fmt.Errorf("open %s database: %w", driverName, err)
	}

	poolOwnedByDriver = true

	openSuccess := false
	defer func() {
		if !openSuccess {
			_ = db.Close(ctx)
		}
	}()

	if graphName := strings.TrimSpace(cfg.Graph); graphName != "" {
		if err := db.SetDefaultGraph(ctx, graph.Graph{
			Name: graphName,
		}); err != nil {
			return nil, "", fmt.Errorf("set graph target %q: %w", graphName, err)
		}
	}

	openSuccess = true

	return db, driverName, nil
}

func driverFromConnectionString(connection string) (string, error) {
	parsedURL, err := url.Parse(connection)
	if err != nil {
		return "", fmt.Errorf("parse connection string: %w", err)
	}

	switch strings.ToLower(parsedURL.Scheme) {
	case "postgres", "postgresql":
		return pg.DriverName, nil
	case neo4j.DriverName, "neo4j+s", "neo4j+ssc":
		return neo4j.DriverName, nil
	default:
		return "", fmt.Errorf("unknown connection string scheme %q; expected postgres/postgresql or neo4j", parsedURL.Scheme)
	}
}

func resolveGraphTargets(ctx context.Context, db graph.Database, driverName string, requested []string, allGraphs bool) ([]retriever.GraphTarget, error) {
	if allGraphs && len(requested) > 0 {
		return nil, fmt.Errorf("-all-graphs cannot be combined with -graph")
	}

	if allGraphs {
		switch driverName {
		case pg.DriverName:
			return discoverPostgresGraphs(ctx, db)
		case neo4j.DriverName:
			return []retriever.GraphTarget{{
				Name: retriever.DefaultGraphName,
			}}, nil
		default:
			return nil, fmt.Errorf("all-graphs is not supported for driver %q", driverName)
		}
	}

	if len(requested) == 0 {
		return []retriever.GraphTarget{{
			Name: retriever.DefaultGraphName,
		}}, nil
	}

	if driverName == neo4j.DriverName && len(requested) > 1 {
		return nil, fmt.Errorf("neo4j supports one retriever graph target because Dawgs graph names are no-ops for that driver")
	}

	targets := make([]retriever.GraphTarget, 0, len(requested))
	seen := map[string]struct{}{}

	for _, name := range requested {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			return nil, fmt.Errorf("graph name cannot be empty")
		}

		if _, ok := seen[trimmed]; ok {
			return nil, fmt.Errorf("duplicate graph target %q", trimmed)
		}

		seen[trimmed] = struct{}{}
		targets = append(targets, retriever.GraphTarget{
			Name: trimmed,
		})
	}

	return targets, nil
}

func discoverPostgresGraphs(ctx context.Context, db graph.Database) ([]retriever.GraphTarget, error) {
	const graphQuery = `
select
  g.name,
  to_regclass('node_' || g.id::text) is not null as has_node_partition,
  to_regclass('edge_' || g.id::text) is not null as has_edge_partition
from graph g
order by g.name`

	var targets []retriever.GraphTarget
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Raw(graphQuery, nil)
		defer result.Close()

		for result.Next() {
			var (
				name             string
				hasNodePartition bool
				hasEdgePartition bool
			)

			if err := result.Scan(&name, &hasNodePartition, &hasEdgePartition); err != nil {
				return err
			}

			if !hasNodePartition || !hasEdgePartition {
				return fmt.Errorf("PostgreSQL graph %q is missing expected node/edge partitions", name)
			}

			targets = append(targets, retriever.GraphTarget{
				Name: name,
			})
		}

		return result.Error()
	}); err != nil {
		return nil, fmt.Errorf("discover PostgreSQL graphs: %w", err)
	}

	if len(targets) == 0 {
		return nil, fmt.Errorf("no PostgreSQL graphs were discovered")
	}

	return targets, nil
}

func graphDirectoryName(name string) string {
	escaped := url.PathEscape(name)
	if escaped == "" {
		return retriever.DefaultGraphName
	}

	return escaped
}
