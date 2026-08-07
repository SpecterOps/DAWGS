// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"

	"github.com/specterops/dawgs/internal/integrationguard"
)

func main() {
	if err := integrationguard.Validate(
		os.Getenv("CONNECTION_STRING"),
		os.Getenv(integrationguard.AllowDestructiveEnv),
		os.Getenv(integrationguard.DisposableTargetsEnv),
	); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
