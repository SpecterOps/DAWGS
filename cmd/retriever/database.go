package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
)

type databaseConfig struct {
	Driver     string
	Connection string
	Graph      string
}

const productDatabaseCloseTimeout = 30 * time.Second

type databaseOpenOperations struct {
	open func(context.Context, string, dawgs.Config) (graph.Database, error)
}

func openDatabase(ctx context.Context, cfg databaseConfig) (graph.Database, string, error) {
	return openDatabaseWith(ctx, cfg, databaseOpenOperations{})
}

func openDatabaseWith(ctx context.Context, cfg databaseConfig, operations databaseOpenOperations) (result graph.Database, driver string, resultErr error) {
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

	open := operations.open
	if open == nil {
		open = dawgs.Open
	}
	db, err := open(ctx, driverName, openConfig)
	if err != nil {
		return nil, "", fmt.Errorf("open %s database: %w", driverName, err)
	}

	poolOwnedByDriver = true

	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, closeProductDatabase(db))
		}
	}()

	if graphName := strings.TrimSpace(cfg.Graph); graphName != "" {
		if err := db.SetDefaultGraph(ctx, graph.Graph{
			Name: graphName,
		}); err != nil {
			return nil, "", fmt.Errorf("set graph target %q: %w", graphName, err)
		}
	}

	return db, driverName, nil
}

func closeProductDatabase(database graph.Database) error {
	if database == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), productDatabaseCloseTimeout)
	defer cancel()
	if err := database.Close(ctx); err != nil {
		return fmt.Errorf("close database: %w", err)
	}
	return nil
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

func resolveGraphNames(ctx context.Context, db graph.Database, driverName string, requested []string, allGraphs bool) ([]string, error) {
	if err := validateGraphSelection(requested, allGraphs); err != nil {
		return nil, err
	}

	if allGraphs {
		switch driverName {
		case pg.DriverName:
			return discoverPostgresGraphs(ctx, db)
		case neo4j.DriverName:
			return []string{defaultGraphName}, nil
		default:
			return nil, fmt.Errorf("all-graphs is not supported for driver %q", driverName)
		}
	}

	if len(requested) == 0 {
		return []string{defaultGraphName}, nil
	}

	if driverName == neo4j.DriverName && len(requested) > 1 {
		return nil, fmt.Errorf("neo4j supports one retriever graph target because Dawgs graph names are no-ops for that driver")
	}

	targets := make([]string, 0, len(requested))
	for _, name := range requested {
		trimmed := strings.TrimSpace(name)
		targets = append(targets, trimmed)
	}

	return targets, nil
}

func validateGraphSelection(requested []string, allGraphs bool) error {
	if allGraphs && len(requested) > 0 {
		return fmt.Errorf("-all-graphs cannot be combined with -graph")
	}
	seen := make(map[string]struct{}, len(requested))
	for _, name := range requested {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" || trimmed == "." || trimmed == ".." ||
			path.Clean(trimmed) != trimmed ||
			strings.ContainsAny(trimmed, "/\\") ||
			strings.ContainsRune(trimmed, '\x00') {
			return fmt.Errorf("graph name %q is not a safe path segment", name)
		}
		if _, found := seen[trimmed]; found {
			return fmt.Errorf("duplicate graph target %q", trimmed)
		}
		seen[trimmed] = struct{}{}
	}
	return nil
}

func discoverPostgresGraphs(ctx context.Context, db graph.Database) ([]string, error) {
	const graphQuery = `
select
  g.name,
  to_regclass('node_' || g.id::text) is not null as has_node_partition,
  to_regclass('edge_' || g.id::text) is not null as has_edge_partition
from graph g
order by g.name`

	var targets []string
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

			targets = append(targets, name)
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
