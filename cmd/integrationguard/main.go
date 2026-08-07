// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"

	"github.com/specterops/dawgs/databaseguard"
)

func main() {
	if err := databaseguard.Validate(
		os.Getenv("CONNECTION_STRING"),
		os.Getenv(databaseguard.AllowDestructiveEnv),
		os.Getenv(databaseguard.DisposableTargetsEnv),
	); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
