package drivers

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strings"

	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
)

type DatabaseConfiguration struct {
	Connection            string `json:"connection"`
	Address               string `json:"addr"`
	Database              string `json:"database"`
	Username              string `json:"username"`
	Secret                string `json:"secret"`
	MaxConcurrentSessions int    `json:"max_concurrent_sessions"`
	EnableRDSIAMAuth      bool   `json:"enable_rds_iam_auth"`
	Endpoint              string
}

func (s DatabaseConfiguration) defaultPostgreSQLConnectionString() string {
	if s.Connection != "" {
		return s.Connection
	}

	return fmt.Sprintf("postgresql://%s:%s@%s/%s", s.Username, url.QueryEscape(s.Secret), s.Address, s.Database)
}

// Looks up CNAME record to get an RDS instance identifier and builds an IAM auth token, returning a connection string
func (s DatabaseConfiguration) RDSIAMAuthConnectionString() string {
	if cfg, err := awsConfig.LoadDefaultConfig(context.TODO()); err != nil {
		slog.Error("AWS Config Loading Error", slog.String("err", err.Error()))
	} else {
		// Must use instance endpoint with IAM auth
		var endpoint string
		if s.Endpoint != "" {
			endpoint = s.Endpoint
		} else {
			endpoint = s.LookupEndpoint()
		}

		slog.Info("Requesting RDS IAM Auth Token")
		if authenticationToken, err := auth.BuildAuthToken(context.TODO(), endpoint, cfg.Region, s.Username, cfg.Credentials); err != nil {
			slog.Error("RDS IAM Auth Token Request Error", slog.String("err", err.Error()))
		} else {
			slog.Info("RDS IAM Auth Token Created")
			return fmt.Sprintf("postgresql://%s:%s@%s/%s", s.Username, url.QueryEscape(authenticationToken), endpoint, s.Database)
		}
	}

	slog.Warn("Failed to create IAM auth token. Falling back to default Postgres connection string")
	return s.defaultPostgreSQLConnectionString()
}

func (s DatabaseConfiguration) PostgreSQLConnectionString() string {
	if s.EnableRDSIAMAuth {
		return s.RDSIAMAuthConnectionString()
	}

	return s.defaultPostgreSQLConnectionString()
}

func (s DatabaseConfiguration) Neo4jConnectionString() string {
	if s.Connection == "" {
		return fmt.Sprintf("neo4j://%s:%s@%s/%s", s.Username, url.QueryEscape(s.Secret), s.Address, s.Database)
	}

	return s.Connection
}

func (s DatabaseConfiguration) LookupEndpoint() string {
	host, port, _ := net.SplitHostPort(s.Address)
	if hostCName, err := net.LookupCNAME(host); err != nil {
		slog.Warn("Error looking up CNAME for DB host. Using original address.", slog.String("err", err.Error()))
		return s.Address
	} else {
		host = hostCName
	}

	// Instance endpoint always returns with a trailing '.'
	return strings.TrimSuffix(host, ".") + ":" + port
}
