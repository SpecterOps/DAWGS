package neo4j

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/channels"
)

const (
	// defaultNeo4jTransactionTimeout is set to math.MinInt as this is what the core neo4j library defaults to when
	// left unset. It is recommended that users set this for time-sensitive operations
	defaultNeo4jTransactionTimeout = math.MinInt
)

func newNeo4jDB(_ context.Context, cfg dawgs.Config) (graph.Database, error) {
	if connectionURL, err := url.Parse(cfg.ConnectionString); err != nil {
		return nil, err
	} else if !isNeo4jConnectionScheme(connectionURL.Scheme) {
		return nil, fmt.Errorf("expected connection URL scheme %s for Neo4J but got %s", DriverName, connectionURL.Scheme)
	} else if password, isSet := connectionURL.User.Password(); !isSet {
		return nil, fmt.Errorf("no password provided in connection URL")
	} else if target, err := neo4jConnectionTarget(connectionURL); err != nil {
		return nil, err
	} else if databaseName, err := neo4jConnectionDatabaseName(connectionURL); err != nil {
		return nil, err
	} else {
		if internalDriver, err := neo4j.NewDriver(target, neo4j.BasicAuth(connectionURL.User.Username(), password, "")); err != nil {
			return nil, fmt.Errorf("unable to connect to Neo4J: %w", err)
		} else {
			return &driver{
				driver:                    internalDriver,
				defaultTransactionTimeout: defaultNeo4jTransactionTimeout,
				limiter:                   channels.NewConcurrencyLimiter(DefaultConcurrentConnections),
				writeFlushSize:            DefaultWriteFlushSize,
				batchWriteSize:            DefaultBatchWriteSize,
				graphQueryMemoryLimit:     cfg.GraphQueryMemoryLimit,
				databaseName:              databaseName,
			}, nil
		}
	}
}

func isNeo4jConnectionScheme(scheme string) bool {
	return scheme == DriverName || scheme == "neo4j+s" || scheme == "neo4j+ssc"
}

func neo4jConnectionTarget(connectionURL *url.URL) (string, error) {
	if connectionURL.Host == "" {
		return "", fmt.Errorf("Neo4j connection string host is required")
	}

	return (&url.URL{
		Scheme:   connectionURL.Scheme,
		Host:     connectionURL.Host,
		RawQuery: connectionURL.RawQuery,
	}).String(), nil
}

func neo4jConnectionDatabaseName(connectionURL *url.URL) (string, error) {
	databasePath := strings.Trim(connectionURL.EscapedPath(), "/")
	if databasePath == "" {
		return "", nil
	}

	if strings.Contains(databasePath, "/") {
		return "", fmt.Errorf("Neo4j database path must contain a single database name")
	}

	databaseName, err := url.PathUnescape(databasePath)
	if err != nil {
		return "", fmt.Errorf("parse Neo4j database name: %w", err)
	}
	if strings.Contains(databaseName, "/") {
		return "", fmt.Errorf("Neo4j database path must contain a single database name")
	}

	return databaseName, nil
}

func init() {
	dawgs.Register(DriverName, func(ctx context.Context, cfg dawgs.Config) (graph.Database, error) {
		return newNeo4jDB(ctx, cfg)
	})
}
