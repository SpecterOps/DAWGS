package neo4j

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/database"
)

const (
	DefaultNeo4jTransactionTimeout = time.Minute * 15
	DefaultBatchWriteSize          = 20_000
	DefaultWriteFlushSize          = DefaultBatchWriteSize * 5

	// DefaultConcurrentConnections defines the default number of concurrent graph database connections allowed.
	DefaultConcurrentConnections = 50

	Neo4jConnectionScheme = "neo4j"
	DriverName            = "neo4j_v2"
)

func newNeo4jDB(ctx context.Context, cfg dawgs.Config) (database.Instance, error) {
	if connectionURL, err := url.Parse(cfg.ConnectionString); err != nil {
		return nil, err
	} else if connectionURL.Scheme != Neo4jConnectionScheme {
		return nil, fmt.Errorf("expected connection URL scheme %s for Neo4J but got %s", Neo4jConnectionScheme, connectionURL.Scheme)
	} else if password, isSet := connectionURL.User.Password(); !isSet {
		return nil, fmt.Errorf("no password provided in connection URL")
	} else {
		boltURL := fmt.Sprintf("bolt://%s:%s", connectionURL.Hostname(), connectionURL.Port())

		if internalDriver, err := neo4j.NewDriverWithContext(boltURL, neo4j.BasicAuth(connectionURL.User.Username(), password, "")); err != nil {
			return nil, fmt.Errorf("unable to connect to Neo4J: %w", err)
		} else {
			return New(internalDriver, cfg), nil
		}
	}
}

func init() {
	dawgs.Register(DriverName, func(ctx context.Context, cfg dawgs.Config) (database.Instance, error) {
		return newNeo4jDB(ctx, cfg)
	})
}
