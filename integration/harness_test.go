// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"errors"
	"strings"
	"testing"

	"github.com/specterops/dawgs/databaseguard"
)

func TestOpenSessionReturnsConnectionStringError(t *testing.T) {
	t.Setenv(ConnectionStringEnv, "")

	_, err := OpenSession(Options{})
	if !errors.Is(err, errConnectionStringMissing) {
		t.Fatalf("OpenSession() error = %v, want connection string missing error", err)
	}
}

func TestOpenSessionReturnsDriverMismatchError(t *testing.T) {
	t.Setenv(ConnectionStringEnv, "postgresql://user:password@localhost:5432/database")
	t.Setenv(databaseguard.AllowDestructiveEnv, "")
	t.Setenv(databaseguard.DisposableTargetsEnv, "")

	_, err := OpenSession(Options{RequireDriver: "neo4j"})
	if !errors.Is(err, errDriverMismatch) {
		t.Fatalf("OpenSession() error = %v, want driver mismatch error", err)
	}
}

func TestOpenSessionRejectsUnconfirmedDatabase(t *testing.T) {
	for _, testCase := range []struct {
		name            string
		acknowledgement string
		targets         string
		wantEnv         string
	}{
		{
			name:    "missing acknowledgement",
			targets: "postgresql://localhost:5432/database",
			wantEnv: databaseguard.AllowDestructiveEnv,
		},
		{
			name:            "target mismatch",
			acknowledgement: "1",
			targets:         "postgresql://localhost:5432/other",
			wantEnv:         databaseguard.DisposableTargetsEnv,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Setenv(ConnectionStringEnv, "postgresql://user:password@localhost:5432/database")
			t.Setenv(databaseguard.AllowDestructiveEnv, testCase.acknowledgement)
			t.Setenv(databaseguard.DisposableTargetsEnv, testCase.targets)

			session, err := OpenSession(Options{})
			if session != nil {
				t.Fatal("OpenSession() returned a session for an unconfirmed database")
			}
			if err == nil || !strings.Contains(err.Error(), "integration database safety check failed") || !strings.Contains(err.Error(), testCase.wantEnv) {
				t.Fatalf("OpenSession() error = %v, want safety check failure naming %s", err, testCase.wantEnv)
			}
			if strings.Contains(err.Error(), "password") {
				t.Fatalf("OpenSession() error exposed credentials: %v", err)
			}
		})
	}
}
