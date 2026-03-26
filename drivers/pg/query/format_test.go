package query_test

import (
	"strings"
	"testing"

	"github.com/specterops/dawgs/drivers/pg/model"
	query "github.com/specterops/dawgs/drivers/pg/query"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNodeUpdateBatch was created to reproduce a specific scenario where a
// node's identity property is updated around the time that the batch processes
// it.
func TestNodeUpdateBatch(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(t *testing.T) *query.NodeUpdateBatch
		assert func(t *testing.T, updates *query.NodeUpdateBatch)
	}{
		{
			name: "Success: Conflicting key successfully serializes — batch keeps both identities",
			setup: func(t *testing.T) *query.NodeUpdateBatch {
				updates := query.NewNodeUpdateBatch()

				// Add node A to the batch
				firstNode := graph.NewNode(0, graph.NewProperties().Set("objectid", "OID-A"), graph.StringKind("User"))
				_, err := updates.Add(graph.NodeUpdate{
					Node:               firstNode,
					IdentityProperties: []string{"objectid"},
				})
				require.NoError(t, err, "unexpected error occurred while adding first node to batch")

				// The value of the unique key (objectid) is updated AFTER the node
				// has already been added to the batch. 
				// Without a snapshot, this causes both the stored entry
				// (keyed as "OID-A") and the upcoming second entry to serialize with
				// objectid="OID-B" — producing a duplicate key in the INSERT statement.
				firstNode.Properties.Set("objectid", "OID-B")

				// Add node B to the batch
				secondNode := graph.NewNode(0, graph.NewProperties().Set("objectid", "OID-B"), graph.StringKind("User"))
				_, err = updates.Add(graph.NodeUpdate{
					Node:               secondNode,
					IdentityProperties: []string{"objectid"},
				})
				require.NoError(t, err, "unexpected error occurred while adding second node to batch")

				return updates
			},
			assert: func(t *testing.T, updates *query.NodeUpdateBatch) {
				// The snapshot must preserve OID-A so two distinct keys exist.
				assert.Len(t, updates.Updates, 2, "batch must hold two distinct entries")

				// The first entry must retain its original identity property value.
				oidAUpdate, hasOIDA := updates.Updates["OID-A"]
				assert.True(t, hasOIDA, "OID-A entry must still exist in the batch")

				// The stored node must retain its original identity property value.
				storedOIDA, err := oidAUpdate.Node.Properties.Get("objectid").String()
				assert.NoError(t, err, "unexpected error occurred while fetching objectid")
				assert.Equal(t, "OID-A", storedOIDA, "stored node must retain its original identity")

				// The second entry must exist and retain its original identity property value.
				oidBUpdate, hasOIDB := updates.Updates["OID-B"]
				assert.True(t, hasOIDB, "OID-B entry must exist in the batch")

				// The stored node must retain its original identity property value.
				storedOIDB, err := oidBUpdate.Node.Properties.Get("objectid").String()
				assert.NoError(t, err, "unexpected error occurred while fetching objectid")
				assert.Equal(t, "OID-B", storedOIDB)
			},
		},
		{
			name: "Success: Conflicting key successfully serializes — both entries merge into one row",
			setup: func(t *testing.T) *query.NodeUpdateBatch {
				// Add node A to the batch
				firstNode := graph.NewNode(0, graph.NewProperties().Set("objectid", "OID-A"), graph.StringKind("User"))
				queuedUpdates := []graph.NodeUpdate{{
					Node:               firstNode,
					IdentityProperties: []string{"objectid"},
				}}

				// The value of the unique key (objectid) is updated BEFORE the node
				// is added to the batch. 
				// At validation time, Key() reads "OID-B" for both entries,
				// so they naturally collapse into one row. This results in no possible conflict.
				firstNode.Properties.Set("objectid", "OID-B")

				// Add node B to the batch
				secondNode := graph.NewNode(0, graph.NewProperties().Set("objectid", "OID-B"), graph.StringKind("User"))
				queuedUpdates = append(queuedUpdates, graph.NodeUpdate{
					Node:               secondNode,
					IdentityProperties: []string{"objectid"},
				})

				// Validate the batch
				updates, err := query.ValidateNodeUpdateByBatch(queuedUpdates)
				require.NoError(t, err, "setup: ValidateNodeUpdateByBatch failed")

				return updates
			},
			assert: func(t *testing.T, updates *query.NodeUpdateBatch) {
				// Both entries share key "OID-B" at validation time, so they merge.
				assert.Len(t, updates.Updates, 1, "both entries share OID-B so they merge")

				// The merged entry is keyed as "OID-B" and contains the properties of both nodes.
				collapsedUpdate, hasKey := updates.Updates["OID-B"]
				assert.True(t, hasKey, "merged entry must be keyed as OID-B")

				// The collapsed node must retain the original identity property value.
				collapsedObjectID, err := collapsedUpdate.Node.Properties.Get("objectid").String()
				assert.NoError(t, err, "unexpected error occurred while fetching objectid")
				assert.Equal(t, "OID-B", collapsedObjectID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			updates := tt.setup(t)
			tt.assert(t, updates)
		})
	}
}

func generateTestGraphTarget(nodePartitionName string) model.Graph {
	return model.Graph{
		Partitions: model.GraphPartitions{
			Node: model.NewGraphPartition(nodePartitionName),
		},
	}
}

func TestFormatNodesUpdate(t *testing.T) {
	t.Parallel()

	var (
		partitionName = "node_1"
		expected      = strings.Join([]string{
			"update node_1 as n ",
			"set ",
			" kind_ids = uniq(sort(kind_ids - u.deleted_kinds || u.added_kinds)), ",
			" properties = n.properties - u.deleted_properties || u.properties ",
			"from ",
			" (select ",
			"  unnest($1::text[])::int8 as id, ",
			"  unnest($2::text[])::int2[] as added_kinds, ",
			"  unnest($3::text[])::int2[] as deleted_kinds, ",
			"  unnest($4::jsonb[]) as properties, ",
			"  unnest($5::text[])::text[] as deleted_properties) as u ",
			"where n.id = u.id; ",
		}, "")
		result = query.FormatNodesUpdate(generateTestGraphTarget(partitionName))
	)

	assert.Equal(t, expected, result)
}

func TestFormatCreateNodeUpdateStagingTable(t *testing.T) {
	t.Parallel()

	var (
		tableName = "my_staging_table"
		expected  = strings.Join([]string{
			"create temp table if not exists my_staging_table (",
			"id bigint, ",
			"added_kinds text, ",
			"deleted_kinds text, ",
			"properties text, ",
			"deleted_props text",
			") on commit drop;",
		}, "")
		result = query.FormatCreateNodeUpdateStagingTable(tableName)
	)

	assert.Equal(t, expected, result)
}

func TestFormatMergeNodeLargeUpdate(t *testing.T) {
	t.Parallel()

	var (
		partitionName = "node_part_1"
		stagingTable  = "my_staging"
		expected      = strings.Join([]string{
			"merge into node_part_1 as n ",
			"using my_staging as u on n.id = u.id ",
			"when matched then update set ",
			"kind_ids = uniq(sort(n.kind_ids - u.deleted_kinds::int2[] || u.added_kinds::int2[])), ",
			"properties = n.properties - u.deleted_props::text[] || u.properties::jsonb;",
		}, "")
		result = query.FormatMergeNodeLargeUpdate(generateTestGraphTarget(partitionName), stagingTable)
	)

	assert.Equal(t, expected, result)
}

func TestNodeUpdateStagingColumns(t *testing.T) {
	t.Parallel()

	var (
		// Note: order is important
		expected = []string{"id", "added_kinds", "deleted_kinds", "properties", "deleted_props"}
	)

	assert.Equal(t, expected, query.NodeUpdateStagingColumns)
}
