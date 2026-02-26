package query

import (
	"testing"

	"github.com/specterops/dawgs/drivers/pg/model"
	"github.com/stretchr/testify/assert"
)

func Test_FormatNodeMerge(t *testing.T) {
	graphTarget := model.Graph{
		ID:   0,
		Name: "default",
		Partitions: model.GraphPartitions{
			Node: model.NewGraphPartition("node_1"),
			Edge: model.NewGraphPartition("edge_1"),
		},
	}

	const (
		namePropertyAssertion  = "merge into node_1 as n using (select $1 as graph_id, unnest($2::text[])::int2[] as kind_ids, unnest($3::text[])::int2[] as deleted_kind_ids, unnest($4::jsonb[]) as properties) as i on i.properties->>'name' = n.properties->>'name' when matched then update set properties = n.properties || i.properties, kind_ids = uniq(sort(n.kind_ids - i.deleted_kind_ids || i.kind_ids)) when not matched then insert (graph_id, kind_ids, properties) values (i.graph_id, i.kind_ids, i.properties) returning n.id;"
		multipleMatchAssertion = "merge into node_1 as n using (select $1 as graph_id, unnest($2::text[])::int2[] as kind_ids, unnest($3::text[])::int2[] as deleted_kind_ids, unnest($4::jsonb[]) as properties) as i on i.properties->>'name' = n.properties->>'name' and i.properties->>'thing' = n.properties->>'thing' and i.properties->>'other' = n.properties->>'other' when matched then update set properties = n.properties || i.properties, kind_ids = uniq(sort(n.kind_ids - i.deleted_kind_ids || i.kind_ids)) when not matched then insert (graph_id, kind_ids, properties) values (i.graph_id, i.kind_ids, i.properties) returning n.id;"
	)

	assert.Equal(t, namePropertyAssertion, FormatNodeMerge(graphTarget, []string{"name"}))
	assert.Equal(t, multipleMatchAssertion, FormatNodeMerge(graphTarget, []string{"name", "thing", "other"}))
}
