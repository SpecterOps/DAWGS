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
	"net/url"
	"slices"
	"strings"
)

const (
	AllowDestructiveEnv   = "DAWGS_INTEGRATION_ALLOW_DESTRUCTIVE"
	DisposableTargetsEnv  = "DAWGS_INTEGRATION_DISPOSABLE_TARGETS"
	allowDestructiveValue = "1"
)

// Target returns a credential-free, stable database endpoint identity suitable
// for explicit operator confirmation. Query parameters and fragments are not
// included because they may contain credentials or unstable driver settings.
func Target(connection string) (string, error) {
	parsed, err := url.Parse(connection)
	if err != nil {
		return "", fmt.Errorf("parse connection string: %w", err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("connection string must include a scheme and host")
	}

	database := strings.Trim(parsed.EscapedPath(), "/")
	if database == "" {
		database = "<default>"
	}

	return strings.ToLower(parsed.Scheme) + "://" + strings.ToLower(parsed.Host) + "/" + database, nil
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

func splitTargets(value string) []string {
	var targets []string
	for _, target := range strings.Split(value, ",") {
		if target = strings.TrimSpace(target); target != "" {
			targets = append(targets, target)
		}
	}
	return targets
}
