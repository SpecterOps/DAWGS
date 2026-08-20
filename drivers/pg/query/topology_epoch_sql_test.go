package query

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemaDefinesTransactionallyVisibleTraversalEpochs(t *testing.T) {
	require.Contains(t, sqlSchemaUp, "create table if not exists graph_traversal_epoch")
	require.Contains(t, sqlSchemaUp, "create_graph_traversal_epoch")
	require.Contains(t, sqlSchemaUp, "bump_graph_traversal_epoch_new")
	require.Contains(t, sqlSchemaUp, "bump_graph_traversal_epoch_old")
	require.Contains(t, sqlSchemaUp, "bump_all_graph_traversal_epochs")
	require.Contains(t, sqlSchemaUp, "referencing new table as new_rows")
	require.Contains(t, sqlSchemaUp, "referencing old table as old_rows")
	require.Contains(t, sqlSchemaDown, "drop table if exists graph_traversal_epoch")

	for _, table := range []string{"node", "edge"} {
		require.True(t, strings.Contains(sqlSchemaUp, "bump_"+table+"_traversal_epoch_insert"))
		require.True(t, strings.Contains(sqlSchemaUp, "bump_"+table+"_traversal_epoch_delete"))
		require.True(t, strings.Contains(sqlSchemaUp, "bump_"+table+"_traversal_epoch_truncate"))
	}
}
