package pg

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/stretchr/testify/require"
)

// testTraversalPolicy coordinates PostgreSQL driver behavior for test traversal policy.
func testTraversalPolicy(query string, executor optimize.ShortestPathExecutor, orientation bool) TraversalPolicy {
	candidate := string(executor)
	if orientation {
		candidate = "orientation-probe-v1"
	}
	queryDigest := TraversalPolicyQuerySHA256(query)
	evidence := map[string]map[string]string{}
	for _, role := range []string{"aa", "confirmation", "performance", "resource", "reference_closure", "operational"} {
		evidence[role] = map[string]string{"sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
	}
	boundary := map[bool]string{
		true:  "guarded_dual_arm",
		false: "inline_statement",
	}[orientation]
	selectorVersion := "test-selector-v1"
	caps := map[string]int64{"state_limit": 1000}
	bucket := map[string]any{"query_sha256": []string{queryDigest}, "qualification_split": []string{"training", "holdout"}}
	fallback := ""
	if orientation {
		caps = map[string]int64{
			"root_row_limit":               optimize.ExpansionSearchOrientationRootRowLimit,
			"reverse_seed_row_limit":       optimize.ExpansionSearchOrientationReverseSeedRowLimit,
			"directional_degree_row_limit": optimize.ExpansionSearchOrientationDirectionalDegreeRowLimit,
			"state_limit":                  optimize.ExpansionSearchOrientationStateLimit,
		}
		fallback = string(optimize.ExpansionSearchStepwiseForward)
	}
	if executor == optimize.ShortestPathExecutorASPI1DAG {
		boundary = "guarded_dual_arm"
		caps = map[string]int64{
			"state_limit": 1000, "predecessor_limit": 900, "enumeration_limit": 800, "output_bytes_limit": 70000,
		}
		fallback = string(optimize.ShortestPathExecutorASPA1DAG)
		bucket["direction"] = "outbound"
		bucket["observation_mode"] = "all_paths"
		bucket["minimum_depth"] = 1
		bucket["maximum_depth"] = 4
		bucket["relationship_kind_count"] = 1
		bucket["untyped_relationship"] = false
	}
	if executor == optimize.ShortestPathExecutorI1CanonicalPredecessorWitness {
		selectorVersion = optimize.ShortestPathSelectorStaticV6
		boundary = "guarded_dual_arm"
		caps = map[string]int64{
			"state_limit": 1000, "predecessor_limit": 900, "enumeration_limit": 800, "output_bytes_limit": 70000,
		}
		fallback = string(optimize.ShortestPathExecutorS4CanonicalWitness)
		bucket["direction"] = "inbound"
		bucket["observation_mode"] = "one_path"
		bucket["minimum_depth"] = 1
		bucket["maximum_depth"] = 64
		bucket["relationship_kind_count"] = 1
		bucket["untyped_relationship"] = false
	}
	raw, err := json.Marshal(map[string]any{
		"version": 2, "candidate": candidate, "selector_version": selectorVersion,
		"source_commit": "deadbeef", "source_sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"binary_sha256":      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"corpus_sha256":      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"execution_boundary": boundary,
		"fallback_executor":  fallback,
		"caps":               caps,
		"buckets":            []map[string]any{bucket},
		"evidence":           evidence,
	})
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(raw)
	return TraversalPolicy{
		Generation:                 1,
		PromotionManifestSHA256:    hex.EncodeToString(digest[:]),
		PromotionManifestJSON:      raw,
		QuerySHA256Allowlist:       []string{queryDigest},
		ShortestPathExecutor:       executor,
		EnableExpansionOrientation: orientation,
	}
}

// rewriteTestTraversalPolicyManifest coordinates PostgreSQL driver behavior for rewrite test traversal policy manifest.
func rewriteTestTraversalPolicyManifest(t *testing.T, policy TraversalPolicy, mutate func(*traversalPromotionManifest)) TraversalPolicy {
	t.Helper()

	var manifest traversalPromotionManifest
	require.NoError(t, json.Unmarshal(policy.PromotionManifestJSON, &manifest))
	mutate(&manifest)

	raw, err := json.Marshal(manifest)
	require.NoError(t, err)
	digest := sha256.Sum256(raw)
	policy.PromotionManifestJSON = raw
	policy.PromotionManifestSHA256 = hex.EncodeToString(digest[:])
	return policy
}

// TestTraversalPolicyAuthorizesGuardedInlineASPOnlyWithStableSnapshotAndExactCaps verifies traversal policy authorizes guarded inline asp only with stable snapshot and exact caps behavior.
func TestTraversalPolicyAuthorizesGuardedInlineASPOnlyWithStableSnapshotAndExactCaps(t *testing.T) {
	driver := &Driver{SchemaManager: NewSchemaManager(nil, 0)}
	query := "MATCH p = allShortestPaths((s)-[:MemberOf*1..4]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p"
	policy := testTraversalPolicy(query, optimize.ShortestPathExecutorASPI1DAG, false)
	require.NoError(t, driver.SetTraversalPolicy(policy))

	effective, _ := driver.SchemaManager.effectiveTraversalPolicy(query, pgx.ReadCommitted)
	require.False(t, effective.enabled())
	effective, _ = driver.SchemaManager.effectiveTraversalPolicy(query, pgx.RepeatableRead)
	require.Equal(t, optimize.ShortestPathExecutorASPI1DAG, effective.ShortestPathExecutor)
	options, err := effective.productionOptions(query)
	require.NoError(t, err)
	require.Equal(t, int64(1000), options.ShortestPathCaps.StateLimit)
	require.Equal(t, int64(900), options.ShortestPathCaps.PredecessorLimit)
	require.Equal(t, int64(800), options.ShortestPathCaps.EnumerationLimit)
	require.Equal(t, int64(70000), options.ShortestPathCaps.OutputBytesLimit)
	require.Equal(t, "outbound", options.AuthorizedBucket.Direction)
}

// TestTraversalPolicyInlineASPKillSwitchRequiresNoEvidence verifies traversal policy inline asp kill switch requires no evidence behavior.
func TestTraversalPolicyInlineASPKillSwitchRequiresNoEvidence(t *testing.T) {
	driver := &Driver{SchemaManager: NewSchemaManager(nil, 0)}
	require.NoError(t, driver.SetTraversalPolicy(TraversalPolicy{
		Generation:          9,
		DisableInlineASPDAG: true,
	}))
	effective, identity := driver.SchemaManager.effectiveTraversalPolicy("MATCH (n) RETURN n", pgx.ReadCommitted)
	require.True(t, effective.DisableInlineASPDAG)
	require.Empty(t, effective.ShortestPathExecutor)
	require.Contains(t, identity, "production-policy-")
	options, err := effective.productionOptions("MATCH (n) RETURN n")
	require.NoError(t, err)
	require.Equal(t, "inline-asp-kill-switch-g9", options.SelectorVersion)
}

// TestTraversalPolicyIsAllowlistedSnapshotSafeAndImmediatelyReversible verifies traversal policy is allowlisted snapshot safe and immediately reversible behavior.
func TestTraversalPolicyIsAllowlistedSnapshotSafeAndImmediatelyReversible(t *testing.T) {
	driver := &Driver{SchemaManager: NewSchemaManager(nil, 0)}
	query := "MATCH p = shortestPath((s)<-[:MemberOf*1..64]-(e)) RETURN p"
	policy := testTraversalPolicy(query, optimize.ShortestPathExecutorI1CanonicalPredecessorWitness, false)
	require.NoError(t, driver.SetTraversalPolicy(policy))

	effective, _ := driver.SchemaManager.effectiveTraversalPolicy(query, pgx.ReadCommitted)
	require.False(t, effective.enabled())
	effective, candidateKey := driver.SchemaManager.effectiveTraversalPolicy(query, pgx.RepeatableRead)
	require.True(t, effective.enabled())
	require.Contains(t, candidateKey, "production-policy-")

	effective, _ = driver.SchemaManager.effectiveTraversalPolicy("RETURN 1", pgx.RepeatableRead)
	require.False(t, effective.enabled(), "queries outside the allowlist remain on incumbents")

	require.NoError(t, driver.SetTraversalPolicy(TraversalPolicy{}))
	effective, rollbackKey := driver.SchemaManager.effectiveTraversalPolicy(query, pgx.RepeatableRead)
	require.False(t, effective.enabled())
	require.Equal(t, "production-incumbent-v1", rollbackKey)
	require.NotEqual(t, candidateKey, rollbackKey)
}

// TestTraversalPolicyFailsClosed verifies traversal policy fails closed behavior.
func TestTraversalPolicyFailsClosed(t *testing.T) {
	driver := &Driver{SchemaManager: NewSchemaManager(nil, 0)}
	require.Error(t, driver.SetTraversalPolicy(TraversalPolicy{
		Generation:                 1,
		EnableExpansionOrientation: true,
	}))
	require.Error(t, driver.SetTraversalPolicy(TraversalPolicy{
		Generation:                 1,
		PromotionManifestSHA256:    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		QuerySHA256Allowlist:       []string{"not-a-digest"},
		EnableExpansionOrientation: true,
	}))
	require.Error(t, driver.SetTraversalPolicy(TraversalPolicy{
		Generation:              1,
		PromotionManifestSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		QuerySHA256Allowlist:    []string{TraversalPolicyQuerySHA256("RETURN 1")},
		ShortestPathExecutor:    optimize.ShortestPathExecutorS3Unidirectional,
	}))
	require.Error(t, driver.SetTraversalPolicy(TraversalPolicy{
		Generation:                 1,
		QuerySHA256Allowlist:       []string{TraversalPolicyQuerySHA256("RETURN 1")},
		EnableExpansionOrientation: true,
	}), "an enabled production policy must be traceable to verified evidence")
	require.ErrorContains(t, driver.SetTraversalPolicy(testTraversalPolicy(
		"MATCH p = shortestPath((s)-[*1..4]->(e)) RETURN length(p)",
		optimize.ShortestPathExecutorI1CanonicalDistance,
		false,
	)), "not production-canary eligible")
}

// TestTraversalPolicyCanonicalSPRequiresExactStaticV6Envelope verifies traversal policy canonical sp requires exact static v6 envelope behavior.
func TestTraversalPolicyCanonicalSPRequiresExactStaticV6Envelope(t *testing.T) {
	query := "MATCH p = shortestPath((s)<-[:MemberOf*1..64]-(e)) RETURN p"
	valid := testTraversalPolicy(query, optimize.ShortestPathExecutorI1CanonicalPredecessorWitness, false)
	require.NoError(t, (&Driver{SchemaManager: NewSchemaManager(nil, 0)}).SetTraversalPolicy(valid))

	tests := map[string]struct {
		// mutate retains the mutate while anonymous record is assembled or evaluated.
		mutate func(*traversalPromotionManifest)
		// errorContains retains the error contains while anonymous record is assembled or evaluated.
		errorContains string
	}{
		"selector": {
			mutate:        func(manifest *traversalPromotionManifest) { manifest.SelectorVersion = "sp-static-v5-contained" },
			errorContains: `requires selector "sp-static-v6"`,
		},
		"outbound": {
			mutate:        func(manifest *traversalPromotionManifest) { manifest.Buckets[0].Direction = "outbound" },
			errorContains: "qualified inbound typed single-kind one-path depth 1..64 envelope",
		},
		"shallower maximum": {
			mutate:        func(manifest *traversalPromotionManifest) { manifest.Buckets[0].MaximumDepth = 63 },
			errorContains: "qualified inbound typed single-kind one-path depth 1..64 envelope",
		},
		"multiple kinds": {
			mutate:        func(manifest *traversalPromotionManifest) { manifest.Buckets[0].RelationshipKindCount = 2 },
			errorContains: "qualified inbound typed single-kind one-path depth 1..64 envelope",
		},
		"untyped": {
			mutate: func(manifest *traversalPromotionManifest) {
				manifest.Buckets[0].RelationshipKindCount = 0
				manifest.Buckets[0].UntypedRelationship = true
			},
			errorContains: "qualified inbound typed single-kind one-path depth 1..64 envelope",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			policy := rewriteTestTraversalPolicyManifest(t, valid, test.mutate)
			driver := &Driver{SchemaManager: NewSchemaManager(nil, 0)}
			require.ErrorContains(t, driver.SetTraversalPolicy(policy), test.errorContains)
		})
	}
}

// TestTraversalPolicyQuerySHA256PreservesSemanticWhitespace verifies traversal policy query sha256 preserves semantic whitespace behavior.
func TestTraversalPolicyQuerySHA256PreservesSemanticWhitespace(t *testing.T) {
	require.Equal(t,
		TraversalPolicyQuerySHA256("  MATCH (n) RETURN n  "),
		TraversalPolicyQuerySHA256("MATCH (n) RETURN n"),
	)
	require.NotEqual(t,
		TraversalPolicyQuerySHA256(`RETURN "a  b"`),
		TraversalPolicyQuerySHA256(`RETURN "a b"`),
	)
	require.NotEqual(t,
		TraversalPolicyQuerySHA256("MATCH (`a  b`) RETURN `a  b`"),
		TraversalPolicyQuerySHA256("MATCH (`a b`) RETURN `a b`"),
	)
}

// TestTraversalPolicyAllowsGuardedOrientationWithoutSnapshotUpgrade verifies traversal policy allows guarded orientation without snapshot upgrade behavior.
func TestTraversalPolicyAllowsGuardedOrientationWithoutSnapshotUpgrade(t *testing.T) {
	driver := &Driver{SchemaManager: NewSchemaManager(nil, 0)}
	query := "MATCH (r)-[:Expand*0..16]->()-[:Suffix]->(e) RETURN id(e)"
	policy := testTraversalPolicy(query, "", true)
	policy.Generation = 2
	require.NoError(t, driver.SetTraversalPolicy(policy))
	effective, identity := driver.SchemaManager.effectiveTraversalPolicy(query, pgx.ReadCommitted)
	require.True(t, effective.EnableExpansionOrientation)
	require.Contains(t, identity, "production-policy-")
}

// TestTraversalPolicyGuardedOrientationRequiresExactManifestContract verifies traversal policy guarded orientation requires exact manifest contract behavior.
func TestTraversalPolicyGuardedOrientationRequiresExactManifestContract(t *testing.T) {
	query := "MATCH (r)-[:Expand*0..16]->()-[:Suffix]->(e) RETURN id(e)"
	valid := testTraversalPolicy(query, "", true)
	require.NoError(t, (&Driver{SchemaManager: NewSchemaManager(nil, 0)}).SetTraversalPolicy(valid))

	tests := map[string]struct {
		// mutate retains the mutate while anonymous record is assembled or evaluated.
		mutate func(*traversalPromotionManifest)
		// errorContains retains the error contains while anonymous record is assembled or evaluated.
		errorContains string
	}{
		"candidate": {
			mutate:        func(manifest *traversalPromotionManifest) { manifest.Candidate = "orientation-probe-v2" },
			errorContains: `candidate "orientation-probe-v2" does not authorize "orientation-probe-v1"`,
		},
		"execution boundary": {
			mutate:        func(manifest *traversalPromotionManifest) { manifest.ExecutionBoundary = "inline_statement" },
			errorContains: `execution boundary "inline_statement" does not authorize "guarded_dual_arm"`,
		},
		"missing cap": {
			mutate: func(manifest *traversalPromotionManifest) {
				delete(manifest.Caps, "root_row_limit")
			},
			errorContains: "requires exactly root-row, reverse-seed-row, directional-degree-row, and state caps",
		},
		"extra cap": {
			mutate: func(manifest *traversalPromotionManifest) {
				manifest.Caps["survival_row_limit"] = 1
			},
			errorContains: "requires exactly root-row, reverse-seed-row, directional-degree-row, and state caps",
		},
		"root cap": {
			mutate: func(manifest *traversalPromotionManifest) {
				manifest.Caps["root_row_limit"] = optimize.ExpansionSearchOrientationRootRowLimit + 1
			},
			errorContains: "requires root_row_limit=512",
		},
		"reverse seed cap": {
			mutate: func(manifest *traversalPromotionManifest) {
				manifest.Caps["reverse_seed_row_limit"] = optimize.ExpansionSearchOrientationReverseSeedRowLimit + 1
			},
			errorContains: "requires reverse_seed_row_limit=512",
		},
		"directional degree cap": {
			mutate: func(manifest *traversalPromotionManifest) {
				manifest.Caps["directional_degree_row_limit"] = optimize.ExpansionSearchOrientationDirectionalDegreeRowLimit + 1
			},
			errorContains: "requires directional_degree_row_limit=16384",
		},
		"state cap": {
			mutate: func(manifest *traversalPromotionManifest) {
				manifest.Caps["state_limit"] = optimize.ExpansionSearchOrientationStateLimit + 1
			},
			errorContains: "requires state_limit=4096",
		},
		"fallback": {
			mutate: func(manifest *traversalPromotionManifest) {
				manifest.FallbackExecutor = string(optimize.ExpansionSearchSuffixSeededReverse)
			},
			errorContains: `requires fallback "EXPANSION-STEPWISE-FORWARD"`,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			policy := rewriteTestTraversalPolicyManifest(t, valid, test.mutate)
			driver := &Driver{SchemaManager: NewSchemaManager(nil, 0)}
			require.ErrorContains(t, driver.SetTraversalPolicy(policy), test.errorContains)
		})
	}
}

// TestTraversalPolicyEndpointSeededKillSwitchRequiresNoPromotionEvidence verifies traversal policy endpoint seeded kill switch requires no promotion evidence behavior.
func TestTraversalPolicyEndpointSeededKillSwitchRequiresNoPromotionEvidence(t *testing.T) {
	driver := &Driver{SchemaManager: NewSchemaManager(nil, 0)}
	require.NoError(t, driver.SetTraversalPolicy(TraversalPolicy{
		Generation:                   7,
		DisableEndpointSeededReverse: true,
	}))
	effective, identity := driver.SchemaManager.effectiveTraversalPolicy("MATCH (n) RETURN n", pgx.ReadCommitted)
	require.True(t, effective.DisableEndpointSeededReverse)
	require.Contains(t, identity, "production-policy-")
	options, err := effective.productionOptions("MATCH (n) RETURN n")
	require.NoError(t, err)
	require.Equal(t, "endpoint-seeded-kill-switch-g7", options.SelectorVersion)
}
