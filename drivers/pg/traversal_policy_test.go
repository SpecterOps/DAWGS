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
	boundary := map[bool]string{true: "guarded_dual_arm", false: "inline_statement"}[orientation]
	caps := map[string]int64{"state_limit": 1000}
	bucket := map[string]any{"query_sha256": []string{queryDigest}, "qualification_split": []string{"training", "holdout"}}
	fallback := ""
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
		boundary = "guarded_dual_arm"
		caps = map[string]int64{
			"state_limit": 1000, "predecessor_limit": 900, "enumeration_limit": 800, "output_bytes_limit": 70000,
		}
		fallback = string(optimize.ShortestPathExecutorS4CanonicalWitness)
		bucket["direction"] = "outbound"
		bucket["observation_mode"] = "one_path"
		bucket["minimum_depth"] = 1
		bucket["maximum_depth"] = 4
		bucket["relationship_kind_count"] = 1
		bucket["untyped_relationship"] = false
	}
	raw, err := json.Marshal(map[string]any{
		"version": 2, "candidate": candidate, "selector_version": "test-selector-v1",
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
		Generation: 1, PromotionManifestSHA256: hex.EncodeToString(digest[:]), PromotionManifestJSON: raw,
		QuerySHA256Allowlist: []string{queryDigest}, ShortestPathExecutor: executor, EnableExpansionOrientation: orientation,
	}
}

func TestTraversalPolicyAuthorizesGuardedInlineASPOnlyWithStableSnapshotAndExactCaps(t *testing.T) {
	driver := &Driver{SchemaManager: NewSchemaManager(nil, 0)}
	query := "MATCH p = allShortestPaths((s)-[:MemberOf*1..4]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p"
	policy := testTraversalPolicy(query, optimize.ShortestPathExecutorASPI1DAG, false)
	require.NoError(t, driver.SetTraversalPolicy(policy))

	effective, _ := driver.SchemaManager.effectiveTraversalPolicy(query, pgx.ReadCommitted)
	require.False(t, effective.enabled())
	effective, _ = driver.SchemaManager.effectiveTraversalPolicy(query, pgx.RepeatableRead)
	require.Equal(t, optimize.ShortestPathExecutorASPI1DAG, effective.ShortestPathExecutor)
	options := effective.productionOptions(query)
	require.Equal(t, int64(1000), options.ShortestPathCaps.StateLimit)
	require.Equal(t, int64(900), options.ShortestPathCaps.PredecessorLimit)
	require.Equal(t, int64(800), options.ShortestPathCaps.EnumerationLimit)
	require.Equal(t, int64(70000), options.ShortestPathCaps.OutputBytesLimit)
	require.Equal(t, "outbound", options.AuthorizedBucket.Direction)
}

func TestTraversalPolicyInlineASPKillSwitchRequiresNoEvidence(t *testing.T) {
	driver := &Driver{SchemaManager: NewSchemaManager(nil, 0)}
	require.NoError(t, driver.SetTraversalPolicy(TraversalPolicy{Generation: 9, DisableInlineASPDAG: true}))
	effective, identity := driver.SchemaManager.effectiveTraversalPolicy("MATCH (n) RETURN n", pgx.ReadCommitted)
	require.True(t, effective.DisableInlineASPDAG)
	require.Empty(t, effective.ShortestPathExecutor)
	require.Contains(t, identity, "production-policy-")
	require.Equal(t, "inline-asp-kill-switch-g9", effective.productionOptions("MATCH (n) RETURN n").SelectorVersion)
}

func TestTraversalPolicyIsAllowlistedSnapshotSafeAndImmediatelyReversible(t *testing.T) {
	driver := &Driver{SchemaManager: NewSchemaManager(nil, 0)}
	query := "MATCH p = shortestPath((s)-[*1..4]->(e)) RETURN p"
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

func TestTraversalPolicyFailsClosed(t *testing.T) {
	driver := &Driver{SchemaManager: NewSchemaManager(nil, 0)}
	require.Error(t, driver.SetTraversalPolicy(TraversalPolicy{Generation: 1, EnableExpansionOrientation: true}))
	require.Error(t, driver.SetTraversalPolicy(TraversalPolicy{
		Generation: 1, PromotionManifestSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", QuerySHA256Allowlist: []string{"not-a-digest"}, EnableExpansionOrientation: true,
	}))
	require.Error(t, driver.SetTraversalPolicy(TraversalPolicy{
		Generation: 1, PromotionManifestSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", QuerySHA256Allowlist: []string{TraversalPolicyQuerySHA256("RETURN 1")},
		ShortestPathExecutor: optimize.ShortestPathExecutorS3Unidirectional,
	}))
	require.Error(t, driver.SetTraversalPolicy(TraversalPolicy{
		Generation: 1, QuerySHA256Allowlist: []string{TraversalPolicyQuerySHA256("RETURN 1")}, EnableExpansionOrientation: true,
	}), "an enabled production policy must be traceable to verified evidence")
	require.ErrorContains(t, driver.SetTraversalPolicy(testTraversalPolicy(
		"MATCH p = shortestPath((s)-[*1..4]->(e)) RETURN length(p)",
		optimize.ShortestPathExecutorI1CanonicalDistance,
		false,
	)), "not production-canary eligible")
}

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

func TestTraversalPolicyEndpointSeededKillSwitchRequiresNoPromotionEvidence(t *testing.T) {
	driver := &Driver{SchemaManager: NewSchemaManager(nil, 0)}
	require.NoError(t, driver.SetTraversalPolicy(TraversalPolicy{Generation: 7, DisableEndpointSeededReverse: true}))
	effective, identity := driver.SchemaManager.effectiveTraversalPolicy("MATCH (n) RETURN n", pgx.ReadCommitted)
	require.True(t, effective.DisableEndpointSeededReverse)
	require.Contains(t, identity, "production-policy-")
	require.Equal(t, "endpoint-seeded-kill-switch-g7", effective.productionOptions("MATCH (n) RETURN n").SelectorVersion)
}
