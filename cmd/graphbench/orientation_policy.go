// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import "github.com/specterops/dawgs/cypher/models/pgsql/optimize"

// isOrientationProbePolicy recognizes immutable orientation selector
// identities without treating an unqualified version as production eligible.
func isOrientationProbePolicy(identity string) bool {
	switch optimize.ExpansionSearchPolicy(identity) {
	case optimize.ExpansionSearchPolicyOrientationProbeV1,
		optimize.ExpansionSearchPolicyOrientationProbeV2:
		return true
	default:
		return false
	}
}
