package pg

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// TestBuildNodeDeleteStatement covers the statement/argument construction for DeleteNodesByKinds, including the guard
// that prevents an unresolved exclusion from widening the delete into an unguarded wipe.
func TestBuildNodeDeleteStatement(t *testing.T) {
	var (
		includeIDs = []int16{1, 2}
		excludeIDs = []int16{9}
	)

	t.Run("no filters deletes all nodes", func(t *testing.T) {
		statement, arguments := buildNodeDeleteStatement(false, nil, nil)
		require.Equal(t, "delete from node", statement)
		require.Empty(t, arguments)
	})

	t.Run("include only", func(t *testing.T) {
		statement, arguments := buildNodeDeleteStatement(true, includeIDs, nil)
		require.Equal(t, "delete from node where kind_ids operator (pg_catalog.&&) $1::int2[]", statement)
		require.Equal(t, []any{includeIDs}, arguments)
	})

	t.Run("exclude only", func(t *testing.T) {
		statement, arguments := buildNodeDeleteStatement(false, nil, excludeIDs)
		require.Equal(t, "delete from node where not (kind_ids operator (pg_catalog.&&) $1::int2[])", statement)
		require.Equal(t, []any{excludeIDs}, arguments)
	})

	t.Run("include and exclude are positionally numbered", func(t *testing.T) {
		statement, arguments := buildNodeDeleteStatement(true, includeIDs, excludeIDs)
		require.Equal(t, "delete from node where kind_ids operator (pg_catalog.&&) $1::int2[] and not (kind_ids operator (pg_catalog.&&) $2::int2[])", statement)
		require.Equal(t, []any{includeIDs, excludeIDs}, arguments)
	})

	t.Run("empty excludeIDs cannot widen the delete", func(t *testing.T) {
		// A requested-but-unresolved exclusion must never emit a not(... && '{}') clause that matches every row.
		statement, arguments := buildNodeDeleteStatement(false, nil, []int16{})
		require.Equal(t, "delete from node", statement)
		require.Empty(t, arguments)

		statement, arguments = buildNodeDeleteStatement(true, includeIDs, []int16{})
		require.Equal(t, "delete from node where kind_ids operator (pg_catalog.&&) $1::int2[]", statement)
		require.Equal(t, []any{includeIDs}, arguments)
	})

	t.Run("include requested with empty IDs is a tolerant no-op predicate", func(t *testing.T) {
		statement, arguments := buildNodeDeleteStatement(true, []int16{}, nil)
		require.Equal(t, "delete from node where kind_ids operator (pg_catalog.&&) $1::int2[]", statement)
		require.Equal(t, []any{[]int16{}}, arguments)
	})
}

// TestResolveKindIDsDefinedFastPath exercises the cache-hit path of resolveKindIDs, which resolves defined kinds
// without touching the database. The cache-miss/refresh and fail-closed exclude paths require a live pool and are
// covered by the integration suite.
func TestResolveKindIDsDefinedFastPath(t *testing.T) {
	ctx := context.Background()

	driver := &Driver{SchemaManager: NewSchemaManager(nil, 0)}

	var (
		userKind  = graph.StringKind("User")
		groupKind = graph.StringKind("Group")
	)
	driver.kindsByID[userKind] = 1
	driver.kindsByID[groupKind] = 2

	t.Run("defined kinds resolve with no missing", func(t *testing.T) {
		ids, missing, err := driver.resolveKindIDs(ctx, graph.Kinds{userKind, groupKind})
		require.NoError(t, err)
		require.Empty(t, missing)
		require.ElementsMatch(t, []int16{1, 2}, ids)
	})

	t.Run("empty kinds short-circuit", func(t *testing.T) {
		ids, missing, err := driver.resolveKindIDs(ctx, nil)
		require.NoError(t, err)
		require.Nil(t, ids)
		require.Nil(t, missing)
	})
}

// TestDeleteRelationshipsByKindsEmptyIsNoop verifies that an empty kinds request returns before acquiring a
// connection, so it is a safe no-op rather than deleting every relationship.
func TestDeleteRelationshipsByKindsEmptyIsNoop(t *testing.T) {
	ctx := context.Background()

	driver := &Driver{SchemaManager: NewSchemaManager(nil, 0)}

	require.NoError(t, driver.DeleteRelationshipsByKinds(ctx, nil))
	require.NoError(t, driver.DeleteRelationshipsByKinds(ctx, graph.Kinds{}))
}
