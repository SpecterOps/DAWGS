package pool

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strings"
)

type DatabaseConfiguration struct {
	Connection            string `json:"connection"`
	Address               string `json:"addr"`
	Database              string `json:"database"`
	Username              string `json:"username"`
	Secret                string `json:"secret"`
	MaxConcurrentSessions int    `json:"max_concurrent_sessions"`
	EnableRDSIAMAuth      bool   `json:"enable_rds_iam_auth"`
	Endpoint              string `json:"endpoint"`
}

func (s DatabaseConfiguration) PostgreSQLConnectionString() string {
	if s.Connection != "" {
		return s.Connection
	}

	return fmt.Sprintf("postgresql://%s:%s@%s/%s", s.Username, url.QueryEscape(s.Secret), s.Address, s.Database)
}

func (s DatabaseConfiguration) Neo4jConnectionString() string {
	if s.Connection == "" {
		return fmt.Sprintf("neo4j://%s:%s@%s/%s", s.Username, url.QueryEscape(s.Secret), s.Address, s.Database)
	}

	return s.Connection
}

func (s DatabaseConfiguration) LookupEndpoint() string {
	host, port, err := net.SplitHostPort(s.Address)
	if err != nil {
		slog.Warn("Missing port in address. Using default port 5432.", slog.String("err", err.Error()))
		host = s.Address
		port = "5432"
	}

	if hostCName, err := net.DefaultResolver.LookupCNAME(context.TODO(), host); err != nil {
		slog.Warn("Error looking up CNAME for DB host. Using original address.", slog.String("err", err.Error()))
	} else {
		host = hostCName
	}

	// Instance endpoint always returns with a trailing '.'
	return net.JoinHostPort(strings.TrimSuffix(host, "."), port)
}
