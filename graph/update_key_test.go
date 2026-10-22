package graph_test

import (
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestNodeUpdateKeyIdentity(t *testing.T) {
	makeUpdate := func(kind string, fields []string, values map[string]any) graph.NodeUpdate {
		return graph.NodeUpdate{Node: graph.PrepareNode(graph.AsProperties(values)), IdentityKind: graph.StringKind(kind), IdentityProperties: fields}
	}
	base := makeUpdate("Base", []string{"b", "a"}, map[string]any{"a": "1", "b": "23"})
	baseKey, err := base.Key()
	require.NoError(t, err)
	require.Equal(t, []string{"b", "a"}, base.IdentityProperties, "key generation must not sort caller storage")
	for _, other := range []graph.NodeUpdate{
		makeUpdate("Base", []string{"a", "b"}, map[string]any{"a": "12", "b": "3"}),
		makeUpdate("Other", []string{"a", "b"}, map[string]any{"a": "1", "b": "23"}),
		makeUpdate("Base", []string{"a", "c"}, map[string]any{"a": "1", "c": "23"}),
		makeUpdate("Base", []string{"a", "b"}, map[string]any{"a": int64(1), "b": "23"}),
	} {
		key, err := other.Key()
		require.NoError(t, err)
		require.NotEqual(t, baseKey, key)
	}
	key, err := makeUpdate("Base", []string{"a", "b"}, map[string]any{"a": "1", "b": "23", "ignored": true}).Key()
	require.NoError(t, err)
	require.Equal(t, baseKey, key)
	_, err = makeUpdate("Base", []string{"missing"}, nil).Key()
	require.ErrorIs(t, err, graph.ErrPropertyNotFound)
	_, err = makeUpdate("Base", []string{"a"}, map[string]any{"a": nil}).Key()
	require.ErrorIs(t, err, graph.ErrPropertyNotFound)
	_, err = makeUpdate("Base", []string{"a"}, map[string]any{"a": make(chan int)}).Key()
	require.Error(t, err)
}

func TestRelationshipUpdateKeyBoundaries(t *testing.T) {
	makeUpdate := func(start, property, end string) graph.RelationshipUpdate {
		return graph.RelationshipUpdate{
			Relationship:       graph.NewRelationship(0, 0, 0, graph.AsProperties(map[string]any{"id": property}), graph.StringKind("R")),
			IdentityProperties: []string{"id"},
			Start:              graph.PrepareNode(graph.AsProperties(map[string]any{"id": start})), StartIdentityKind: graph.StringKind("Base"), StartIdentityProperties: []string{"id"},
			End: graph.PrepareNode(graph.AsProperties(map[string]any{"id": end})), EndIdentityKind: graph.StringKind("Base"), EndIdentityProperties: []string{"id"},
		}
	}
	baseKey, err := makeUpdate("1", "2", "34").Key()
	require.NoError(t, err)
	for _, other := range []graph.RelationshipUpdate{makeUpdate("12", "3", "4"), makeUpdate("1", "23", "4"), makeUpdate("34", "2", "1")} {
		key, err := other.Key()
		require.NoError(t, err)
		require.NotEqual(t, baseKey, key)
	}
	bad := makeUpdate("1", "2", "34")
	bad.StartIdentityProperties = []string{"missing"}
	_, err = bad.Key()
	require.ErrorIs(t, err, graph.ErrPropertyNotFound)
	bad = makeUpdate("1", "2", "34")
	bad.EndIdentityProperties = []string{"missing"}
	_, err = bad.Key()
	require.ErrorIs(t, err, graph.ErrPropertyNotFound)
	bad = makeUpdate("1", "2", "34")
	bad.IdentityProperties = []string{"missing"}
	_, err = bad.Key()
	require.ErrorIs(t, err, graph.ErrPropertyNotFound)
}
