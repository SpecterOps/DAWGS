package pg

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/drivers/pg/model"
	"github.com/specterops/dawgs/graph"
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
		evidence[role] = map[string]string{"path": role + ".json", "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
	}
	boundary := map[bool]string{
		true:  "guarded_dual_arm",
		false: "inline_statement",
	}[orientation]
	selectorVersion := "test-selector-v1"
	caps := map[string]int64{"state_limit": 1000}
	bucket := map[string]any{"name": "qualified-query", "query_sha256": []string{queryDigest}, "qualification_split": []string{"training", "holdout"}}
	fallback := ""
	if orientation {
		selectorVersion = string(optimize.ExpansionSearchPolicyOrientationProbeV1)
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
	if executor == optimize.ShortestPathExecutorI2GuardedDistance {
		selectorVersion = optimize.ShortestPathSelectorStaticV8HiddenFanIn
		boundary = "guarded_dual_arm"
		caps = map[string]int64{
			"state_limit":    optimize.ShortestPathI2QualifiedStateLimit,
			"frontier_limit": optimize.ShortestPathI2QualifiedFrontierLimit,
		}
		fallback = string(optimize.ShortestPathExecutorS4CanonicalDistance)
		bucket["direction"] = "inbound"
		bucket["observation_mode"] = "distance"
		bucket["minimum_depth"] = 1
		bucket["maximum_depth"] = 32
		bucket["relationship_kind_count"] = 1
		bucket["untyped_relationship"] = false
	}
	raw, err := json.Marshal(map[string]any{
		"version": 2, "candidate": candidate, "selector_version": selectorVersion,
		"source_commit": "deadbeef", "source_sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"binary_sha256":                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"corpus_sha256":                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"operational_candidate_sql_sha256": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
		"execution_boundary":               boundary,
		"fallback_executor":                fallback,
		"caps":                             caps,
		"buckets":                          []map[string]any{bucket},
		"evidence":                         evidence,
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

func testTopologyFixedSuffixPolicy(t *testing.T, evidenceQuery string, shape TraversalShape) TraversalPolicy {
	t.Helper()
	manifest := traversalPromotionManifest{
		Version:                       4,
		Candidate:                     string(optimize.ExpansionSearchPolicyTopologyFixedSuffixV1),
		SelectorVersion:               string(optimize.ExpansionSearchPolicyTopologyFixedSuffixV1),
		ExecutionBoundary:             "transaction_retry",
		FallbackExecutor:              string(optimize.ExpansionSearchStepwiseForward),
		SourceCommit:                  "test-topology-fixed-suffix",
		SourceSHA256:                  strings.Repeat("0", sha256.Size*2),
		BinarySHA256:                  strings.Repeat("1", sha256.Size*2),
		CorpusSHA256:                  strings.Repeat("2", sha256.Size*2),
		OperationalCandidateSQLSHA256: strings.Repeat("3", sha256.Size*2),
		TopologyEstimatorVersion:      "topology-fixed-suffix-counts-v1",
		SynopsisSchemaVersion:         "topology-synopsis-schema-v2",
		RouteCacheProtocol:            "topology-selected-routing-v1",
		Caps: map[string]int64{
			"suffix_row_limit":   optimize.ExpansionSearchSuffixReverseGuardSuffixRowLimit,
			"state_limit":        optimize.ExpansionSearchSuffixReverseGuardStateLimit,
			"output_row_limit":   optimize.ExpansionSearchSuffixReverseRetryOutputRowLimit,
			"output_bytes_limit": optimize.ExpansionSearchSuffixReverseRetryOutputBytesLimit,
		},
	}
	bucket := traversalPromotionBucket{
		Name:                   "topology-fixed-suffix",
		QuerySHA256:            []string{TraversalPolicyQuerySHA256(evidenceQuery)},
		QualificationSplit:     []string{"training", "holdout"},
		Direction:              shape.Direction,
		ObservationMode:        shape.ObservationMode,
		MinimumDepth:           shape.MinimumDepth,
		MaximumDepth:           shape.MaximumDepth,
		SuffixLength:           shape.SuffixLength,
		CandidateStrategy:      shape.CandidateStrategy,
		StructuralShapeVersion: shape.Version,
		StructuralFamily:       shape.Family,
		StructuralShapeSHA256:  shape.Fingerprint,
	}
	manifest.Buckets = []traversalPromotionBucket{bucket}
	manifest.Buckets[0].SQLTemplateSHA256 = structuralSQLTemplateSHA256(manifest, manifest.Buckets[0])
	manifest.Evidence = map[string]traversalPromotionEvidence{}
	for _, role := range []string{"aa", "confirmation", "performance", "resource", "reference_closure", "operational"} {
		manifest.Evidence[role] = traversalPromotionEvidence{Path: role + ".json", SHA256: strings.Repeat("4", sha256.Size*2)}
	}
	raw, err := json.Marshal(manifest)
	require.NoError(t, err)
	digest := sha256.Sum256(raw)
	return TraversalPolicy{
		Generation:                1,
		PromotionManifestSHA256:   hex.EncodeToString(digest[:]),
		PromotionManifestJSON:     raw,
		QuerySHA256Allowlist:      []string{bucket.QuerySHA256[0]},
		EnableTopologyFixedSuffix: true,
	}
}

func TestTraversalPolicyV4RequiresRouteOwnedFixedSuffixSelection(t *testing.T) {
	query := `MATCH (root:Root) WHERE root.key = $key MATCH route = (root)-[:Expand*0..16]->()-[:Enter]->(:Middle)-[:Continue]->(:NearTerminal)-[:Complete]->(:Terminal) RETURN route`
	parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
	require.NoError(t, err)
	shape, err := traversalShapeForQuery(parsed)
	require.NoError(t, err)
	require.Equal(t, TraversalFixedSuffixShapeVersion, shape.Version)

	driver := &Driver{SchemaManager: NewSchemaManager(nil, 0)}
	policy := testTopologyFixedSuffixPolicy(t, query, shape)
	require.NoError(t, driver.SetTraversalPolicy(policy))

	ordinary, identity := driver.SchemaManager.effectiveTraversalPolicyForShape(query, shape, pgx.RepeatableRead)
	require.False(t, ordinary.enabled())
	require.Equal(t, "production-incumbent-v1", identity)

	topology, topologyIdentity := driver.SchemaManager.topologyFixedSuffixPolicyForShape(shape, pgx.RepeatableRead)
	require.True(t, topology.enabled())
	require.Contains(t, topologyIdentity, "topology-fixed-suffix-candidate")
	options, err := topology.productionOptionsForShape(query, shape)
	require.NoError(t, err)
	require.True(t, options.EnableTopologyFixedSuffix)
	require.Equal(t, optimize.ExpansionSearchSuffixReverseRetryOutputRowLimit, options.TopologyFixedSuffixCaps.OutputRowLimit)

	invalid := rewriteTestTraversalPolicyManifest(t, policy, func(manifest *traversalPromotionManifest) {
		manifest.RouteCacheProtocol = "unknown"
	})
	require.ErrorContains(t, (&Driver{SchemaManager: NewSchemaManager(nil, 0)}).SetTraversalPolicy(invalid), "route-cache protocol")
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

func TestTraversalPolicyAuthorizesAndRollsBackGuardedDistance(t *testing.T) {
	driver := &Driver{SchemaManager: NewSchemaManager(nil, 0)}
	query := "MATCH p = shortestPath((s)<-[:MemberOf*1..32]-(e)) RETURN length(p)"
	policy := testTraversalPolicy(query, optimize.ShortestPathExecutorI2GuardedDistance, false)
	require.NoError(t, driver.SetTraversalPolicy(policy))

	effective, _ := driver.SchemaManager.effectiveTraversalPolicy(query, pgx.RepeatableRead)
	require.Equal(t, optimize.ShortestPathExecutorI2GuardedDistance, effective.ShortestPathExecutor)
	options, err := effective.productionOptions(query)
	require.NoError(t, err)
	require.Equal(t, optimize.ShortestPathSelectorStaticV8HiddenFanIn, options.SelectorVersion)
	require.Equal(t, optimize.ShortestPathI2QualifiedStateLimit, options.ShortestPathCaps.StateLimit)
	require.Equal(t, optimize.ShortestPathI2QualifiedFrontierLimit, options.ShortestPathCaps.FrontierLimit)

	for name, test := range map[string]struct {
		capName  string
		value    int64
		expected int64
	}{
		"non-qualified state cap": {
			capName: "state_limit", value: 1000, expected: optimize.ShortestPathI2QualifiedStateLimit,
		},
		"non-qualified frontier cap": {
			capName: "frontier_limit", value: 100, expected: optimize.ShortestPathI2QualifiedFrontierLimit,
		},
	} {
		t.Run(name, func(t *testing.T) {
			invalid := rewriteTestTraversalPolicyManifest(t, policy, func(manifest *traversalPromotionManifest) {
				manifest.Caps[test.capName] = test.value
			})
			err := (&Driver{SchemaManager: NewSchemaManager(nil, 0)}).SetTraversalPolicy(invalid)
			require.ErrorContains(t, err, fmt.Sprintf(
				"SP-I2 distance promotion manifest requires %s=%d",
				test.capName,
				test.expected,
			))
		})
	}

	policy.DisableInlineSPDistance = true
	require.NoError(t, driver.SetTraversalPolicy(policy))
	effective, _ = driver.SchemaManager.effectiveTraversalPolicy(query, pgx.RepeatableRead)
	require.Empty(t, effective.ShortestPathExecutor)
	require.True(t, effective.DisableInlineSPDistance)
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

// TestTraversalPolicyRequiresOperationalSQLAnchor verifies production admission
// cannot rely only on the operational report's self-declared fingerprint.
func TestTraversalPolicyRequiresOperationalSQLAnchor(t *testing.T) {
	query := "MATCH p = shortestPath((s)<-[:MemberOf*1..64]-(e)) RETURN p"
	valid := testTraversalPolicy(query, optimize.ShortestPathExecutorI1CanonicalPredecessorWitness, false)

	for name, mutate := range map[string]func(*traversalPromotionManifest){
		"missing": func(manifest *traversalPromotionManifest) { manifest.OperationalCandidateSQLSHA256 = "" },
		"invalid": func(manifest *traversalPromotionManifest) { manifest.OperationalCandidateSQLSHA256 = "NOT-A-DIGEST" },
	} {
		t.Run(name, func(t *testing.T) {
			policy := rewriteTestTraversalPolicyManifest(t, valid, mutate)
			err := (&Driver{SchemaManager: NewSchemaManager(nil, 0)}).SetTraversalPolicy(policy)
			require.ErrorContains(t, err, "operational candidate SQL SHA-256 digest")
		})
	}
}

// TestDecodeTraversalPromotionManifestIsStrict verifies unknown or trailing
// content cannot change a final authorization document without rejection.
func TestDecodeTraversalPromotionManifestIsStrict(t *testing.T) {
	valid := testTraversalPolicy("MATCH (n) RETURN n", optimize.ShortestPathExecutorASPI1DAG, false)
	var document map[string]any
	require.NoError(t, json.Unmarshal(valid.PromotionManifestJSON, &document))
	document["operational_candidate_sql_sha_256"] = document["operational_candidate_sql_sha256"]
	raw, err := json.Marshal(document)
	require.NoError(t, err)
	_, err = decodeTraversalPromotionManifest(raw)
	require.ErrorContains(t, err, "unknown field")

	_, err = decodeTraversalPromotionManifest(append(valid.PromotionManifestJSON, []byte("\n{}")...))
	require.ErrorContains(t, err, "trailing JSON data")

	duplicateTopLevel := strings.Replace(string(valid.PromotionManifestJSON), `"version":2`, `"version":2,"version":2`, 1)
	_, err = decodeTraversalPromotionManifest([]byte(duplicateTopLevel))
	require.ErrorContains(t, err, `duplicate JSON object key "version"`)

	duplicateNested := strings.Replace(string(valid.PromotionManifestJSON), `"qualification_split":`, `"qualification_split":["training","holdout"],"qualification_split":`, 1)
	_, err = decodeTraversalPromotionManifest([]byte(duplicateNested))
	require.ErrorContains(t, err, `duplicate JSON object key "qualification_split"`)
}

func TestTraversalPolicyRequiresExactManifestSets(t *testing.T) {
	query := "MATCH p = shortestPath((s)<-[:MemberOf*1..64]-(e)) RETURN p"
	valid := testTraversalPolicy(query, optimize.ShortestPathExecutorI1CanonicalPredecessorWitness, false)

	tests := map[string]struct {
		mutate func(*traversalPromotionManifest)
		reason string
	}{
		"extra evidence role": {
			mutate: func(manifest *traversalPromotionManifest) {
				manifest.Evidence["invented"] = traversalPromotionEvidence{SHA256: strings.Repeat("a", 64)}
			},
			reason: "exactly the six supported evidence roles",
		},
		"escaping evidence path": {
			mutate: func(manifest *traversalPromotionManifest) {
				reference := manifest.Evidence["aa"]
				reference.Path = "../aa.json"
				manifest.Evidence["aa"] = reference
			},
			reason: "requires a contained relative path",
		},
		"duplicate split": {
			mutate: func(manifest *traversalPromotionManifest) {
				manifest.Buckets[0].QualificationSplit = []string{"training", "training", "holdout"}
			},
			reason: "exactly one training and one holdout qualification split",
		},
		"extra split": {
			mutate: func(manifest *traversalPromotionManifest) {
				manifest.Buckets[0].QualificationSplit = []string{"training", "holdout", "diagnostic"}
			},
			reason: "exactly one training and one holdout qualification split",
		},
		"duplicate query within bucket": {
			mutate: func(manifest *traversalPromotionManifest) {
				manifest.Buckets[0].QuerySHA256 = append(manifest.Buckets[0].QuerySHA256, manifest.Buckets[0].QuerySHA256[0])
			},
			reason: "duplicates query digest",
		},
		"duplicate bucket name": {
			mutate: func(manifest *traversalPromotionManifest) {
				manifest.Buckets = append(manifest.Buckets, manifest.Buckets[0])
			},
			reason: "promotion bucket",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			policy := rewriteTestTraversalPolicyManifest(t, valid, test.mutate)
			err := (&Driver{SchemaManager: NewSchemaManager(nil, 0)}).SetTraversalPolicy(policy)
			require.ErrorContains(t, err, test.reason)
		})
	}

	duplicateAllowlist := valid
	duplicateAllowlist.QuerySHA256Allowlist = append(duplicateAllowlist.QuerySHA256Allowlist, duplicateAllowlist.QuerySHA256Allowlist[0])
	require.ErrorContains(t, (&Driver{SchemaManager: NewSchemaManager(nil, 0)}).SetTraversalPolicy(duplicateAllowlist), "must not contain duplicate digests")
}

// TestValidateTraversalPromotionSQLAnchor verifies the production execution
// path compares exact rendered SQL with the independent manifest digest.
func TestValidateTraversalPromotionSQLAnchor(t *testing.T) {
	sqlQuery := "select 1::int8 as distance"
	digest := sha256.Sum256([]byte(sqlQuery))
	manifest := traversalPromotionManifest{OperationalCandidateSQLSHA256: hex.EncodeToString(digest[:])}
	require.NoError(t, validateTraversalPromotionSQLAnchor(manifest, sqlQuery))
	require.ErrorContains(t, validateTraversalPromotionSQLAnchor(manifest, sqlQuery+" "), "does not match promotion manifest anchor")
	require.NoError(t, validateTraversalPromotionSQLAnchor(traversalPromotionManifest{}, sqlQuery),
		"evidence-free emergency rollback policies have no operational SQL anchor")
}

// TestTraversalPolicyRollbackCompositionIsCandidateSpecific exhaustively
// verifies that each manifest-backed candidate composes only with its dedicated
// emergency control. A matching rollback derives an incumbent-only policy with
// a distinct cache identity and no candidate SQL anchor; unrelated switches
// fail installation and cannot authorize the candidate for another query.
func TestTraversalPolicyRollbackCompositionIsCandidateSpecific(t *testing.T) {
	query := "MATCH p = shortestPath((s)<-[:MemberOf*1..32]-(e)) RETURN length(p)"
	unauthorizedQuery := "RETURN 1"
	candidates := []struct {
		name        string
		executor    optimize.ShortestPathExecutor
		orientation bool
		isolation   pgx.TxIsoLevel
		matching    string
	}{
		{
			name:        "expansion orientation",
			orientation: true,
			isolation:   pgx.ReadCommitted,
			matching:    "expansion orientation",
		},
		{
			name:      "inline all shortest paths",
			executor:  optimize.ShortestPathExecutorASPI1DAG,
			isolation: pgx.RepeatableRead,
			matching:  "inline all shortest paths",
		},
		{
			name:      "inline shortest path witness",
			executor:  optimize.ShortestPathExecutorI1CanonicalPredecessorWitness,
			isolation: pgx.RepeatableRead,
			matching:  "inline shortest path witness",
		},
		{
			name:      "inline shortest path distance",
			executor:  optimize.ShortestPathExecutorI2GuardedDistance,
			isolation: pgx.RepeatableRead,
			matching:  "inline shortest path distance",
		},
	}
	switches := []struct {
		name    string
		disable func(*TraversalPolicy)
		active  func(TraversalPolicy) bool
	}{
		{
			name: "expansion orientation",
			disable: func(policy *TraversalPolicy) {
				policy.DisableExpansionOrientation = true
			},
			active: func(policy TraversalPolicy) bool { return policy.DisableExpansionOrientation },
		},
		{
			name: "endpoint seeded reverse",
			disable: func(policy *TraversalPolicy) {
				policy.DisableEndpointSeededReverse = true
			},
			active: func(policy TraversalPolicy) bool { return policy.DisableEndpointSeededReverse },
		},
		{
			name: "inline all shortest paths",
			disable: func(policy *TraversalPolicy) {
				policy.DisableInlineASPDAG = true
			},
			active: func(policy TraversalPolicy) bool { return policy.DisableInlineASPDAG },
		},
		{
			name: "inline shortest path witness",
			disable: func(policy *TraversalPolicy) {
				policy.DisableInlineSPWitness = true
			},
			active: func(policy TraversalPolicy) bool { return policy.DisableInlineSPWitness },
		},
		{
			name: "inline shortest path distance",
			disable: func(policy *TraversalPolicy) {
				policy.DisableInlineSPDistance = true
			},
			active: func(policy TraversalPolicy) bool { return policy.DisableInlineSPDistance },
		},
	}

	for _, candidate := range candidates {
		for _, rollback := range switches {
			t.Run(candidate.name+"/"+rollback.name, func(t *testing.T) {
				driver := &Driver{SchemaManager: NewSchemaManager(nil, 0)}
				policy := testTraversalPolicy(query, candidate.executor, candidate.orientation)
				require.NoError(t, driver.SetTraversalPolicy(policy))
				activeCandidate, candidateIdentity := driver.SchemaManager.effectiveTraversalPolicy(query, candidate.isolation)
				require.True(t, activeCandidate.manifestCandidateEnabled())

				rollback.disable(&policy)
				err := driver.SetTraversalPolicy(policy)
				if rollback.name != candidate.matching {
					require.ErrorContains(t, err, "single matching emergency rollback switch")
					installed := driver.TraversalPolicy()
					require.True(t, installed.manifestCandidateEnabled(), "rejected policy must not replace the installed candidate")
					effective, identity := driver.SchemaManager.effectiveTraversalPolicy(unauthorizedQuery, candidate.isolation)
					require.False(t, effective.manifestCandidateEnabled(), "an unrelated switch must not bypass query authorization")
					require.Equal(t, "production-incumbent-v1", identity)
					return
				}

				require.NoError(t, err)
				effective, rollbackIdentity := driver.SchemaManager.effectiveTraversalPolicy(query, candidate.isolation)
				require.True(t, rollback.active(effective))
				require.False(t, effective.manifestCandidateEnabled())
				require.NotEqual(t, candidateIdentity, rollbackIdentity)
				require.Empty(t, effective.compiledManifest.OperationalCandidateSQLSHA256)
				require.NoError(t, validateTraversalPromotionSQLAnchor(effective.compiledManifest, "incumbent SQL"))

				unauthorized, unauthorizedIdentity := driver.SchemaManager.effectiveTraversalPolicy(unauthorizedQuery, candidate.isolation)
				require.True(t, rollback.active(unauthorized), "matching emergency rollback remains global")
				require.False(t, unauthorized.manifestCandidateEnabled())
				require.Empty(t, unauthorized.compiledManifest.OperationalCandidateSQLSHA256)
				require.Equal(t, rollbackIdentity, unauthorizedIdentity)

				installed := driver.TraversalPolicy()
				require.True(t, installed.manifestCandidateEnabled())
				require.NotEmpty(t, installed.compiledManifest.OperationalCandidateSQLSHA256,
					"deriving the rollback policy must not mutate the installed manifest")
			})
		}
	}
}

// TestTraversalPolicyRejectsMatchingRollbackWithAdditionalSwitch proves the
// matching exception cannot be broadened by adding any second emergency flag.
func TestTraversalPolicyRejectsMatchingRollbackWithAdditionalSwitch(t *testing.T) {
	query := "MATCH p = shortestPath((s)<-[:MemberOf*1..32]-(e)) RETURN length(p)"
	tests := []struct {
		name        string
		executor    optimize.ShortestPathExecutor
		orientation bool
		disable     func(*TraversalPolicy)
	}{
		{
			name: "expansion orientation", orientation: true,
			disable: func(policy *TraversalPolicy) {
				policy.DisableExpansionOrientation = true
				policy.DisableEndpointSeededReverse = true
			},
		},
		{
			name: "inline all shortest paths", executor: optimize.ShortestPathExecutorASPI1DAG,
			disable: func(policy *TraversalPolicy) {
				policy.DisableInlineASPDAG = true
				policy.DisableEndpointSeededReverse = true
			},
		},
		{
			name: "inline shortest path witness", executor: optimize.ShortestPathExecutorI1CanonicalPredecessorWitness,
			disable: func(policy *TraversalPolicy) {
				policy.DisableInlineSPWitness = true
				policy.DisableEndpointSeededReverse = true
			},
		},
		{
			name: "inline shortest path distance", executor: optimize.ShortestPathExecutorI2GuardedDistance,
			disable: func(policy *TraversalPolicy) {
				policy.DisableInlineSPDistance = true
				policy.DisableEndpointSeededReverse = true
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy := testTraversalPolicy(query, test.executor, test.orientation)
			test.disable(&policy)
			err := (&Driver{SchemaManager: NewSchemaManager(nil, 0)}).SetTraversalPolicy(policy)
			require.ErrorContains(t, err, "single matching emergency rollback switch")
		})
	}
}

// TestTraversalPolicyStandaloneRollbacksRemainGlobalAndUnanchored verifies all
// evidence-free switch-only policies apply independently of query allowlists
// without carrying a manifest candidate or candidate SQL anchor.
func TestTraversalPolicyStandaloneRollbacksRemainGlobalAndUnanchored(t *testing.T) {
	tests := []struct {
		name    string
		disable func(*TraversalPolicy)
		active  func(TraversalPolicy) bool
	}{
		{
			name: "expansion orientation",
			disable: func(policy *TraversalPolicy) {
				policy.DisableExpansionOrientation = true
			},
			active: func(policy TraversalPolicy) bool { return policy.DisableExpansionOrientation },
		},
		{
			name: "endpoint seeded reverse",
			disable: func(policy *TraversalPolicy) {
				policy.DisableEndpointSeededReverse = true
			},
			active: func(policy TraversalPolicy) bool { return policy.DisableEndpointSeededReverse },
		},
		{
			name: "inline all shortest paths",
			disable: func(policy *TraversalPolicy) {
				policy.DisableInlineASPDAG = true
			},
			active: func(policy TraversalPolicy) bool { return policy.DisableInlineASPDAG },
		},
		{
			name: "inline shortest path witness",
			disable: func(policy *TraversalPolicy) {
				policy.DisableInlineSPWitness = true
			},
			active: func(policy TraversalPolicy) bool { return policy.DisableInlineSPWitness },
		},
		{
			name: "inline shortest path distance",
			disable: func(policy *TraversalPolicy) {
				policy.DisableInlineSPDistance = true
			},
			active: func(policy TraversalPolicy) bool { return policy.DisableInlineSPDistance },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			driver := &Driver{SchemaManager: NewSchemaManager(nil, 0)}
			policy := TraversalPolicy{Generation: 42}
			test.disable(&policy)
			require.NoError(t, driver.SetTraversalPolicy(policy))

			effective, identity := driver.SchemaManager.effectiveTraversalPolicy("RETURN 1", pgx.ReadCommitted)
			require.True(t, test.active(effective))
			require.False(t, effective.manifestCandidateEnabled())
			require.Empty(t, effective.compiledManifest.OperationalCandidateSQLSHA256)
			require.Contains(t, identity, "production-policy-")
		})
	}
}

// TestTraversalPolicyStandaloneRollbackRejectsPromotionFields verifies an
// evidence-free switch cannot consume an unverified manifest selector or
// retain irrelevant authorization data in its cache identity.
func TestTraversalPolicyStandaloneRollbackRejectsPromotionFields(t *testing.T) {
	query := "MATCH p = shortestPath((s)<-[:MemberOf*1..32]-(e)) RETURN length(p)"
	candidate := testTraversalPolicy(query, optimize.ShortestPathExecutorI2GuardedDistance, false)
	tests := map[string]func(*TraversalPolicy){
		"manifest digest": func(policy *TraversalPolicy) {
			policy.PromotionManifestSHA256 = strings.Repeat("a", 64)
		},
		"manifest JSON": func(policy *TraversalPolicy) {
			policy.PromotionManifestJSON = json.RawMessage(`{"selector_version":"unverified-selector"}`)
		},
		"query allowlist": func(policy *TraversalPolicy) {
			policy.QuerySHA256Allowlist = []string{TraversalPolicyQuerySHA256(query)}
		},
		"fields copied from candidate": func(policy *TraversalPolicy) {
			policy.PromotionManifestSHA256 = candidate.PromotionManifestSHA256
			policy.PromotionManifestJSON = append(json.RawMessage(nil), candidate.PromotionManifestJSON...)
			policy.QuerySHA256Allowlist = append([]string(nil), candidate.QuerySHA256Allowlist...)
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			policy := TraversalPolicy{Generation: 43, DisableEndpointSeededReverse: true}
			mutate(&policy)
			driver := &Driver{SchemaManager: NewSchemaManager(nil, 0)}
			require.ErrorContains(t, driver.SetTraversalPolicy(policy), "must not carry promotion manifest or query authorization fields")
			require.False(t, driver.TraversalPolicy().enabled(), "rejected standalone evidence must not replace the installed policy")
		})
	}
}

// TestTransactionQueryRejectsPromotionSQLAnchorDrift exercises the production
// parse/translate/format callback and proves drift is returned before Raw can
// reach the database driver.
func TestTransactionQueryRejectsPromotionSQLAnchorDrift(t *testing.T) {
	query := `
		MATCH (root:ExpansionRoot)
		WHERE root.root_key = $root_key
		MATCH path = (root)-[:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal)
		RETURN path
	`
	manager := NewSchemaManager(nil, 0)
	manager.setDefaultGraph(model.Graph{ID: 1, Name: "test"}, graph.Graph{Name: "test"})
	for index, name := range []string{"ExpansionRoot", "Expand", "EnterSuffix", "SuffixHead", "ContinueSuffix", "SuffixMiddle", "CompleteSuffix", "SuffixTerminal"} {
		manager.kindsByID[graph.StringKind(name)] = int16(index + 1)
	}
	policy := testTraversalPolicy(query, "", true)
	require.NoError(t, (&Driver{SchemaManager: manager}).SetTraversalPolicy(policy))

	tx := &transaction{schemaManager: manager, ctx: context.Background(), isolation: pgx.ReadCommitted}
	result := tx.Query(query, map[string]any{"root_key": "root"})
	require.ErrorContains(t, result.Error(), "does not match promotion manifest anchor")
}

// TestTraversalPolicySQLAnchorRequiresOneAuthorizedQuery verifies one scalar
// SQL anchor cannot ambiguously authorize several distinct query statements.
func TestTraversalPolicySQLAnchorRequiresOneAuthorizedQuery(t *testing.T) {
	query := "MATCH p = shortestPath((s)<-[:MemberOf*1..64]-(e)) RETURN p"
	valid := testTraversalPolicy(query, optimize.ShortestPathExecutorI1CanonicalPredecessorWitness, false)
	policy := rewriteTestTraversalPolicyManifest(t, valid, func(manifest *traversalPromotionManifest) {
		manifest.Buckets[0].QuerySHA256 = append(manifest.Buckets[0].QuerySHA256, strings.Repeat("f", 64))
	})
	policy.QuerySHA256Allowlist = append(policy.QuerySHA256Allowlist, strings.Repeat("f", 64))
	err := (&Driver{SchemaManager: NewSchemaManager(nil, 0)}).SetTraversalPolicy(policy)
	require.ErrorContains(t, err, "operational SQL anchor requires exactly one authorized query digest")
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

// TestTraversalPolicyAuthorizesOrientationProbeV2WithoutExecutingV1 verifies
// the manifest selector is carried into production translation options.
func TestTraversalPolicyAuthorizesOrientationProbeV2WithoutExecutingV1(t *testing.T) {
	driver := &Driver{SchemaManager: NewSchemaManager(nil, 0)}
	query := "MATCH (r)-[:Expand*0..16]->()-[:Suffix]->(e) RETURN id(e)"
	policy := testTraversalPolicy(query, "", true)
	policy = rewriteTestTraversalPolicyManifest(t, policy, func(manifest *traversalPromotionManifest) {
		manifest.Candidate = string(optimize.ExpansionSearchPolicyOrientationProbeV2)
		manifest.SelectorVersion = string(optimize.ExpansionSearchPolicyOrientationProbeV2)
	})
	require.NoError(t, driver.SetTraversalPolicy(policy))
	effective, _ := driver.SchemaManager.effectiveTraversalPolicy(query, pgx.ReadCommitted)
	options, err := effective.productionOptions(query)
	require.NoError(t, err)
	require.True(t, options.EnableExpansionOrientation)
	require.Equal(t, optimize.ExpansionSearchPolicyOrientationProbeV2, options.ExpansionOrientationPolicy)
	require.Equal(t, string(optimize.ExpansionSearchPolicyOrientationProbeV2), options.SelectorVersion)
}

// TestTraversalPolicyExpansionOrientationKillSwitchRequiresNoEvidence verifies
// rollback changes the cache identity without requiring promotion evidence.
func TestTraversalPolicyExpansionOrientationKillSwitchRequiresNoEvidence(t *testing.T) {
	driver := &Driver{SchemaManager: NewSchemaManager(nil, 0)}
	require.NoError(t, driver.SetTraversalPolicy(TraversalPolicy{
		Generation:                  10,
		DisableExpansionOrientation: true,
	}))
	effective, identity := driver.SchemaManager.effectiveTraversalPolicy("MATCH (n) RETURN n", pgx.ReadCommitted)
	require.True(t, effective.DisableExpansionOrientation)
	require.Contains(t, identity, "production-policy-")
	options, err := effective.productionOptions("MATCH (n) RETURN n")
	require.NoError(t, err)
	require.False(t, options.EnableExpansionOrientation)
	require.Equal(t, "expansion-orientation-kill-switch-g10", options.SelectorVersion)
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
