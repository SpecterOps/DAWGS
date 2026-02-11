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
}

func (s DatabaseConfiguration) defaultPostgreSQLConnectionString() string {
	if s.Connection != "" {
		return s.Connection
	}

	return fmt.Sprintf("postgresql://%s:%s@%s/%s", s.Username, url.QueryEscape(s.Secret), s.Address, s.Database)
}

func (s DatabaseConfiguration) RDSIAMAuthConnectionString() string {
	slog.Info("Loading RDS Configuration With IAM Auth")

	if cfg, err := awsConfig.LoadDefaultConfig(context.TODO()); err != nil {
		slog.Error("AWS Config Loading Error", slog.String("err", err.Error()))
	} else {
		host := s.Address

		if hostCName, err := net.LookupCNAME(s.Address); err != nil {
			slog.Warn("Error looking up CNAME for DB host. Using original address.", slog.String("err", err.Error()))
		} else {
			host = hostCName
		}

		endpoint := strings.TrimSuffix(host, ".") + ":5432"

		slog.Info("Requesting RDS IAM Auth Token")

		if authenticationToken, err := auth.BuildAuthToken(context.TODO(), endpoint, cfg.Region, s.Username, cfg.Credentials); err != nil {
			slog.Error("RDS IAM Auth Token Request Error", slog.String("err", err.Error()))
		} else {
			slog.Info("RDS IAM Auth Token Created")
			return fmt.Sprintf("postgresql://%s:%s@%s/%s", s.Username, url.QueryEscape(authenticationToken), endpoint, s.Database)
		}
	}

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
		return fmt.Sprintf("neo4j://%s:%s@%s/%s", s.Username, s.Secret, s.Address, s.Database)
	}

	return s.Connection
}
