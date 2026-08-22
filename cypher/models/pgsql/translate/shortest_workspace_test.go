package translate

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestShortestPathWorkspaceFragmentUsesDedicatedTablesAndConstraints verifies isolation and key constraints for each workspace relation.
func TestShortestPathWorkspaceFragmentUsesDedicatedTablesAndConstraints(t *testing.T) {
	fragment := "insert into next_front select * from forward_front " +
		"where not exists (select 1 from forward_visited) " +
		"on conflict on constraint forward_visited_pkey do nothing"

	rewritten := shortestPathWorkspaceFragment(fragment)
	require.Equal(t,
		"insert into pg_temp.bsp_next_front select * from pg_temp.bsp_forward_front "+
			"where not exists (select 1 from pg_temp.bsp_forward_visited) "+
			"on conflict on constraint bsp_forward_visited_pkey do nothing",
		rewritten,
	)
}
