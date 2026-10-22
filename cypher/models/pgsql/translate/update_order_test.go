package translate

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql/format"
	"github.com/stretchr/testify/require"
)

func TestUpdateClauseOrder(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, tc := range []struct {
			name   string
			clause string
			want   string
		}{
			{
				name:   "set then remove",
				clause: "SET n.x = 1 REMOVE n.x",
				want:   "properties = (n1.properties || jsonb_build_object(E'x', 1)::jsonb) - array [E'x']::text[]",
			},
			{
				name:   "remove then set",
				clause: "REMOVE n.x SET n.x = 1",
				want:   "properties = (n1.properties - array [E'x']::text[]) || jsonb_build_object(E'x', 1)::jsonb",
			},
			{
				name:   "alternating property changes",
				clause: "SET n.x = 1 REMOVE n.x SET n.x = 2",
				want:   "properties = ((n1.properties || jsonb_build_object(E'x', 1)::jsonb) - array [E'x']::text[]) || jsonb_build_object(E'x', 2)::jsonb",
			},
			{
				name:   "adjacent sets stay grouped",
				clause: "SET n.x = 1, n.y = 2 SET n.x = 3",
				want:   "properties = n1.properties || jsonb_build_object(E'x', 1, E'y', 2, E'x', 3)::jsonb",
			},
			{
				name:   "adjacent removals stay grouped",
				clause: "REMOVE n.x, n.y REMOVE n.z",
				want:   "properties = n1.properties - array [E'x', E'y', E'z']::text[]",
			},
			{
				name:   "set then remove label",
				clause: "SET n:Group REMOVE n:Group",
				want:   "kind_ids = uniq(sort((n1.kind_ids || array [8]::int2[]) - array [8]::int2[])::int2[])::int2[]",
			},
			{
				name:   "remove then set label",
				clause: "REMOVE n:Group SET n:Group",
				want:   "kind_ids = uniq(sort((n1.kind_ids - array [8]::int2[]) || array [8]::int2[])::int2[])::int2[]",
			},
			{
				name:   "adjacent label operations stay grouped",
				clause: "SET n:Group SET n:User REMOVE n:Group REMOVE n:User",
				want:   "kind_ids = uniq(sort((n1.kind_ids || array [8, 20]::int2[]) - array [8, 20]::int2[])::int2[])::int2[]",
			},
		} {
			t.Run(string(mode)+"/"+tc.name, func(t *testing.T) {
				update, _ := translatedUpdateProjection(t, "MATCH (n) "+tc.clause+" RETURN n", mode)
				require.Len(t, update.Assignments, 1)
				actual, err := format.SyntaxNodeWithBuilder(update.Assignments[0], format.NewOutputBuilder().WithMaterializedParameters(nil))
				require.NoError(t, err)
				require.Equal(t, tc.want, actual.Statement)
			})
		}
	}
}
