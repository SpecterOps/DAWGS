package drivers

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
)

type DatabaseConfiguration struct {
	Connection            string `json:"connection"`
	Address               string `json:"addr"`
	Database              string `json:"database"`
	Username              string `json:"username"`
	Secret                string `json:"secret"`
	MaxConcurrentSessions int    `json:"max_concurrent_sessions"`
	IamAuth               bool   `json:"iam_auth"`
}

func (s DatabaseConfiguration) PostgreSQLConnectionString() string {
	if s.IamAuth {
		slog.Info("loading default config for rds auth")
		cfg, err := config.LoadDefaultConfig(context.TODO())
		if err != nil {
			panic("configuration error: " + err.Error())
		}

		cname, err := net.LookupCNAME(s.Address)
		if err != nil {
			fmt.Printf("Error looking up CNAME for %s: %v. Using original address", s.Address, err)
			cname = s.Address
		}

		dbinput := strings.TrimSuffix(cname, ".") + ":5432"
		slog.Info("requesting auth token")
		authenticationToken, err := auth.BuildAuthToken(context.TODO(), dbinput, "us-east-1", s.Username, cfg.Credentials)
		if err != nil {
			panic("failed to create authentication token: " + err.Error())
		}
		slog.Info("auth token successfully created")
		encodedToken := url.QueryEscape(authenticationToken)

		return fmt.Sprintf("postgresql://%s:%s@%s/%s", s.Username, encodedToken, dbinput, s.Database)
	} else if s.Connection != "" {
		return s.Connection
	} else {
		return fmt.Sprintf("postgresql://%s:%s@%s/%s", s.Username, s.Secret, s.Address, s.Database)
	}
}

func (s DatabaseConfiguration) Neo4jConnectionString() string {
	if s.Connection == "" {
		return fmt.Sprintf("neo4j://%s:%s@%s/%s", s.Username, s.Secret, s.Address, s.Database)
	}

	return s.Connection
}
