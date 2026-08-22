// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

// Package databaseguard prevents destructive database workflows from running
// against a target that was not explicitly named as disposable by the
// operator.
package databaseguard

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// AllowDestructiveEnv names the environment variable that must equal "1" before destructive database work is permitted.
	AllowDestructiveEnv = "DAWGS_INTEGRATION_ALLOW_DESTRUCTIVE"

	// DisposableTargetsEnv names the environment variable containing the exact, credential-free targets approved for destructive work.
	DisposableTargetsEnv = "DAWGS_INTEGRATION_DISPOSABLE_TARGETS"

	// allowDestructiveValue is the acknowledgement value required by Validate.
	allowDestructiveValue = "1"
)

// Target returns a credential-free, stable database endpoint identity suitable
// for explicit operator confirmation. The identity is derived from the
// effective driver configuration so endpoint-changing connection parameters
// cannot authorize a different target than the driver will use.
func Target(connection string) (string, error) {
	parsed, err := url.Parse(connection)
	if err != nil {
		return "", fmt.Errorf("invalid database connection string")
	}

	switch strings.ToLower(parsed.Scheme) {
	case "postgres", "postgresql":
		return postgresTarget(connection)
	case "neo4j", "neo4j+s", "neo4j+ssc":
		return neo4jTarget(parsed)
	case "":
		return "", fmt.Errorf("connection string must include a scheme and host")
	default:
		return "", fmt.Errorf("unsupported database connection scheme")
	}
}

// postgresTarget returns the canonical PostgreSQL endpoint and database that the parsed pgx configuration will use.
func postgresTarget(connection string) (string, error) {
	config, err := pgxpool.ParseConfig(connection)
	if err != nil {
		return "", fmt.Errorf("invalid PostgreSQL connection string")
	}

	host := strings.ToLower(strings.TrimSpace(config.ConnConfig.Host))
	port := config.ConnConfig.Port
	if host == "" || port == 0 {
		return "", fmt.Errorf("PostgreSQL connection string must resolve to one host and port")
	}

	for _, fallback := range config.ConnConfig.Fallbacks {
		if !strings.EqualFold(strings.TrimSpace(fallback.Host), host) || fallback.Port != port {
			return "", fmt.Errorf("destructive PostgreSQL connections must resolve to one endpoint")
		}
	}

	database := config.ConnConfig.Database
	if database == "" {
		database = "<default>"
	}

	return "postgresql://" + net.JoinHostPort(host, strconv.FormatUint(uint64(port), 10)) + "/" + url.PathEscape(database), nil
}

// neo4jTarget returns a credential-free Neo4j target with an explicit port and escaped database name.
func neo4jTarget(parsed *url.URL) (string, error) {
	host := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	if host == "" {
		return "", fmt.Errorf("Neo4j connection string must include a host")
	}

	port := uint64(7687)
	if parsedPort := parsed.Port(); parsedPort != "" {
		parsedValue, err := strconv.ParseUint(parsedPort, 10, 16)
		if err != nil || parsedValue == 0 {
			return "", fmt.Errorf("invalid Neo4j connection port")
		}
		port = parsedValue
	}

	database := strings.Trim(parsed.EscapedPath(), "/")
	if database == "" {
		database = "<default>"
	} else if decoded, err := url.PathUnescape(database); err != nil || strings.Contains(decoded, "/") {
		return "", fmt.Errorf("invalid Neo4j database name")
	} else {
		database = url.PathEscape(decoded)
	}

	return strings.ToLower(parsed.Scheme) + "://" + net.JoinHostPort(host, strconv.FormatUint(port, 10)) + "/" + database, nil
}

// Validate requires both an explicit destructive-operation acknowledgement and
// an exact target allowlist match. Errors expose only the sanitized target.
func Validate(connection, acknowledgement, disposableTargets string) error {
	target, err := Target(connection)
	if err != nil {
		return err
	}
	if acknowledgement != allowDestructiveValue {
		return fmt.Errorf("destructive database access to %s is disabled: set %s=%s and include the target in %s", target, AllowDestructiveEnv, allowDestructiveValue, DisposableTargetsEnv)
	}

	targets := splitTargets(disposableTargets)
	if !slices.Contains(targets, target) {
		return fmt.Errorf("destructive database target %s is not confirmed in %s", target, DisposableTargetsEnv)
	}

	return nil
}

// ValidateEnvironment validates a destructive target using the process-wide
// acknowledgement and exact-target allowlist. Destructive entry points should
// call this immediately before opening or mutating a database rather than rely
// on a command wrapper to have performed the check.
func ValidateEnvironment(connection string) error {
	return Validate(
		connection,
		os.Getenv(AllowDestructiveEnv),
		os.Getenv(DisposableTargetsEnv),
	)
}

// splitTargets parses a comma-separated target allowlist, trimming whitespace and discarding empty entries.
func splitTargets(value string) []string {
	var targets []string
	for _, target := range strings.Split(value, ",") {
		if target = strings.TrimSpace(target); target != "" {
			targets = append(targets, target)
		}
	}
	return targets
}
