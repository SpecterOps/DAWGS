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

// isSuffixReverseGuardPolicy recognizes the reverse-first fixed-suffix guard
// without folding it into the topology-scored orientation policy lineage.
func isSuffixReverseGuardPolicy(identity string) bool {
	return optimize.ExpansionSearchPolicy(identity) == optimize.ExpansionSearchPolicySuffixReverseGuardV1
}

// isGuardedExpansionPolicy recognizes same-statement ordinary-expansion
// policies that must expose exactly one candidate or fallback runtime branch.
func isGuardedExpansionPolicy(identity string) bool {
	return isOrientationProbePolicy(identity) || isSuffixReverseGuardPolicy(identity)
}
