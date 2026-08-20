package pg

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/stretchr/testify/require"
)

func TestTraversalShapeUsesOptimizerFactsWithoutIdentifiersOrValues(t *testing.T) {
	first, err := frontend.ParseCypher(frontend.NewContext(), "MATCH p = allShortestPaths((s)-[:Edge*1..4]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p")
	require.NoError(t, err)
	second, err := frontend.ParseCypher(frontend.NewContext(), "MATCH route = allShortestPaths((x)-[:Edge*1..4]->(y)) WHERE id(x) = $left AND id(y) = $right RETURN route")
	require.NoError(t, err)

	firstShape, err := traversalShapeForQuery(first)
	require.NoError(t, err)
	secondShape, err := traversalShapeForQuery(second)
	require.NoError(t, err)
	require.True(t, firstShape.Available())
	require.Equal(t, TraversalShapeVersion, firstShape.Version)
	require.Equal(t, "ASP", firstShape.Family)
	require.Equal(t, "outbound", firstShape.Direction)
	require.Equal(t, "all_paths", firstShape.ObservationMode)
	require.Equal(t, int64(1), firstShape.MinimumDepth)
	require.Equal(t, int64(4), firstShape.MaximumDepth)
	require.Equal(t, 1, firstShape.RelationshipKindCount)
	require.False(t, firstShape.UntypedRelationship)
	require.Equal(t, firstShape.Fingerprint, secondShape.Fingerprint)
}

func TestTraversalPolicyStructuralBucketIsObservationOnlyAndUnambiguous(t *testing.T) {
	query := "MATCH p = allShortestPaths((s)-[:Edge*1..4]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p"
	parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
	require.NoError(t, err)
	shape, err := traversalShapeForQuery(parsed)
	require.NoError(t, err)

	policy := testTraversalPolicy(query, optimize.ShortestPathExecutorASPI1DAG, false)
	manifest, err := decodeTraversalPromotionManifest(policy.PromotionManifestJSON)
	require.NoError(t, err)
	policy.compiledManifest = manifest
	bucket, matched := policy.structuralBucketForShape(shape)
	require.True(t, matched)
	require.Equal(t, "qualified-query", bucket.Name)

	policy.compiledManifest.Buckets = append(policy.compiledManifest.Buckets, bucket)
	policy.compiledManifest.Buckets[1].Name = "ambiguous"
	_, matched = policy.structuralBucketForShape(shape)
	require.False(t, matched)
}

func TestTraversalShapeRejectsMultipleTraversalTargets(t *testing.T) {
	query, err := frontend.ParseCypher(frontend.NewContext(), "MATCH p = shortestPath((a)-[:Edge*1..4]->(b)), q = shortestPath((c)-[:Edge*1..4]->(d)) RETURN p, q")
	require.NoError(t, err)

	shape, err := traversalShapeForQuery(query)
	require.NoError(t, err)
	require.False(t, shape.Available())
}

func TestTraversalPolicyAuthorizesVerifiedStructuralBucket(t *testing.T) {
	query := "MATCH p = allShortestPaths((s)-[:Edge*1..4]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p"
	otherQuery := "MATCH route = allShortestPaths((left)-[:Edge*1..4]->(right)) WHERE id(left) = $a AND id(right) = $b RETURN route"
	parsed, err := frontend.ParseCypher(frontend.NewContext(), otherQuery)
	require.NoError(t, err)
	shape, err := traversalShapeForQuery(parsed)
	require.NoError(t, err)

	policy := testTraversalPolicy(query, optimize.ShortestPathExecutorASPI1DAG, false)
	manifest, err := decodeTraversalPromotionManifest(policy.PromotionManifestJSON)
	require.NoError(t, err)
	manifest.Version = 3
	manifest.Buckets[0].StructuralShapeVersion = shape.Version
	manifest.Buckets[0].StructuralFamily = shape.Family
	manifest.Buckets[0].StructuralShapeSHA256 = shape.Fingerprint
	manifest.Buckets[0].SQLTemplateSHA256 = structuralSQLTemplateSHA256(manifest, manifest.Buckets[0])
	policy = rewriteTestTraversalPolicyManifest(t, policy, func(current *traversalPromotionManifest) {
		*current = manifest
	})

	driver := &Driver{SchemaManager: NewSchemaManager(nil, 0)}
	require.NoError(t, driver.SetTraversalPolicy(policy))
	effective, identity := driver.SchemaManager.effectiveTraversalPolicyForShape(otherQuery, shape, pgx.RepeatableRead)
	require.True(t, effective.enabled())
	require.NotEqual(t, "production-incumbent-v1", identity)
	options, err := effective.productionOptionsForShape(otherQuery, shape)
	require.NoError(t, err)
	require.NotNil(t, options.AuthorizedBucket)
	require.Equal(t, "outbound", options.AuthorizedBucket.Direction)
}
