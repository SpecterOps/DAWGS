// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
)

// BuildInlineGuardedShortestDistanceRootV2 emits the selected E1 V2
// architecture. Admission is materialized once before target selection. With
// production's equal caps, total-state admission dominates frontier admission
// and no GROUP BY depth aggregate is rendered. Unequal diagnostic caps retain
// exactly one independent frontier check.
func (s *ExpansionBuilder) BuildInlineGuardedShortestDistanceRootV2() (pgsql.Query, error) {
	return s.buildInlineGuardedShortestDistanceRoot(optimize.ShortestPathExecutorI2GuardedDistanceV2, true)
}
