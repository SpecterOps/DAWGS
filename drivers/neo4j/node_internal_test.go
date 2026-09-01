package neo4j

import (
	"testing"

	neo4j_core "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// coreNode builds a minimal neo4j driver node with the given ID.
func coreNode(id int64) neo4j_core.Node {
	return neo4j_core.Node{Id: id}
}

// coreRel builds a minimal neo4j driver relationship. StartId and EndId report
// the relationship's stored direction, which may be the reverse of the direction
// the path traverses it.
func coreRel(id, start, end int64, kind string) neo4j_core.Relationship {
	return neo4j_core.Relationship{Id: id, StartId: start, EndId: end, Type: kind}
}

func TestNewPath(t *testing.T) {
	cases := []struct {
		name      string
		path      neo4j_core.Path
		wantNodes []graph.ID
		wantKinds []string
	}{
		{
			name:      "single node, no relationships",
			path:      neo4j_core.Path{Nodes: []neo4j_core.Node{coreNode(1)}},
			wantNodes: []graph.ID{1},
			wantKinds: nil,
		},
		{
			name: "simple one hop a->b",
			path: neo4j_core.Path{
				Nodes:         []neo4j_core.Node{coreNode(1), coreNode(2)},
				Relationships: []neo4j_core.Relationship{coreRel(10, 1, 2, "R")},
			},
			wantNodes: []graph.ID{1, 2},
			wantKinds: []string{"R"},
		},
		{
			name: "self loop a->a repeats the node",
			path: neo4j_core.Path{
				Nodes:         []neo4j_core.Node{coreNode(1)},
				Relationships: []neo4j_core.Relationship{coreRel(10, 1, 1, "R")},
			},
			wantNodes: []graph.ID{1, 1},
			wantKinds: []string{"R"},
		},
		{
			name: "two hop cycle u->v->u repeats the closing node",
			path: neo4j_core.Path{
				Nodes: []neo4j_core.Node{coreNode(1), coreNode(2)},
				Relationships: []neo4j_core.Relationship{
					coreRel(10, 1, 2, "R"),
					coreRel(11, 2, 1, "R"),
				},
			},
			wantNodes: []graph.ID{1, 2, 1},
			wantKinds: []string{"R", "R"},
		},
		{
			name: "three hop cycle d->e->f->d repeats the closing node",
			path: neo4j_core.Path{
				Nodes: []neo4j_core.Node{coreNode(1), coreNode(2), coreNode(3)},
				Relationships: []neo4j_core.Relationship{
					coreRel(10, 1, 2, "R"),
					coreRel(11, 2, 3, "R"),
					coreRel(12, 3, 1, "R"),
				},
			},
			wantNodes: []graph.ID{1, 2, 3, 1},
			wantKinds: []string{"R", "R", "R"},
		},
		{
			name: "inbound edge stored against traversal keeps pattern order",
			// Models match p=(a)<-[:R]-(b): the path is bound a..b (Nodes[0]=a)
			// but the edge is stored b->a, so StartId=2(b), EndId=1(a). The walk
			// must still yield [a, b], following the pool order not stored order.
			path: neo4j_core.Path{
				Nodes:         []neo4j_core.Node{coreNode(1), coreNode(2)},
				Relationships: []neo4j_core.Relationship{coreRel(10, 2, 1, "R")},
			},
			wantNodes: []graph.ID{1, 2},
			wantKinds: []string{"R"},
		},
		{
			name: "mixed direction three hop keeps traversal order",
			// a -> b (stored a->b), then b <- c stored c->b traversed b..c.
			path: neo4j_core.Path{
				Nodes: []neo4j_core.Node{coreNode(1), coreNode(2), coreNode(3)},
				Relationships: []neo4j_core.Relationship{
					coreRel(10, 1, 2, "R"),
					coreRel(11, 3, 2, "S"),
				},
			},
			wantNodes: []graph.ID{1, 2, 3},
			wantKinds: []string{"R", "S"},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			path := newPath(testCase.path)

			require.Len(t, path.Nodes, len(testCase.wantNodes))
			require.Len(t, path.Edges, len(testCase.wantKinds))
			require.Equal(t, len(path.Edges)+1, len(path.Nodes))

			for idx, wantID := range testCase.wantNodes {
				require.NotNil(t, path.Nodes[idx], "node at index %d is nil", idx)
				require.Equal(t, wantID, path.Nodes[idx].ID)
			}

			for idx, wantKind := range testCase.wantKinds {
				require.NotNil(t, path.Edges[idx], "edge at index %d is nil", idx)
				require.NotNil(t, path.Edges[idx].Kind, "edge kind at index %d is nil", idx)
				require.Equal(t, wantKind, path.Edges[idx].Kind.String())
			}
		})
	}
}
