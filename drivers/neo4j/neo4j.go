package neo4j

import (
	"context"
	"fmt"
	"math"
	"net/url"

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
	} else if connectionURL.Scheme != DriverName {
		return nil, fmt.Errorf("expected connection URL scheme %s for Neo4J but got %s", DriverName, connectionURL.Scheme)
	} else if password, isSet := connectionURL.User.Password(); !isSet {
		return nil, fmt.Errorf("no password provided in connection URL")
	} else {
		boltURL := fmt.Sprintf("bolt://%s:%s", connectionURL.Hostname(), connectionURL.Port())

		if internalDriver, err := neo4j.NewDriver(boltURL, neo4j.BasicAuth(connectionURL.User.Username(), password, "")); err != nil {
			return nil, fmt.Errorf("unable to connect to Neo4J: %w", err)
		} else {
			return &driver{
				driver:                    internalDriver,
				defaultTransactionTimeout: defaultNeo4jTransactionTimeout,
				limiter:                   channels.NewConcurrencyLimiter(DefaultConcurrentConnections),
				writeFlushSize:            DefaultWriteFlushSize,
				batchWriteSize:            DefaultBatchWriteSize,
				graphQueryMemoryLimit:     cfg.GraphQueryMemoryLimit,
			}, nil
		}
	}
}

func init() {
	dawgs.Register(DriverName, func(ctx context.Context, cfg dawgs.Config) (graph.Database, error) {
		return newNeo4jDB(ctx, cfg)
	})
}
