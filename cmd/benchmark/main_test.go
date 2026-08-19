// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
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

package main

import (
	"testing"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/stretchr/testify/require"
)

// TestPostgresBenchmarkDriverModes verifies that v2 is an explicit benchmark
// mode while retaining PostgreSQL-only explain support.
func TestPostgresBenchmarkDriverModes(t *testing.T) {
	require.True(t, isPostgresBenchmarkDriver(pg.DriverName))
	require.True(t, isPostgresBenchmarkDriver(pgV2BenchmarkDriver))
	require.False(t, isPostgresBenchmarkDriver("neo4j"))
}
