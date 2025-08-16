package changestream

// func TestMergeNodeChanges(t *testing.T) {
// 	a1 := NodeChange{
// 		NodeID:             "abc",
// 		changeType:         ChangeTypeAdded,
// 		Properties:         graph.NewProperties().Set("a", 1),
// 		ModifiedProperties: graph.NewProperties().Set("a", 1).MapOrEmpty(),
// 		Deleted:            nil,
// 		Kinds:              graph.Kinds{graph.StringKind("kindA")},
// 	}

// 	a2b3 := NodeChange{
// 		NodeID:             "abc",
// 		changeType:         ChangeTypeModified,
// 		Properties:         graph.NewProperties().SetAll(map[string]any{"a": 2, "b": 3}),
// 		ModifiedProperties: graph.NewProperties().SetAll(map[string]any{"a": 2, "b": 3}).MapOrEmpty(),
// 		Deleted:            nil,
// 		Kinds:              graph.Kinds{graph.StringKind("kindA")},
// 	}

// 	c1 := NodeChange{
// 		NodeID:             "abc",
// 		changeType:         ChangeTypeModified,
// 		Properties:         graph.NewProperties().Set("c", 1),
// 		ModifiedProperties: graph.NewProperties().Set("c", 1).MapOrEmpty(),
// 		Deleted:            []string{"a", "b"},
// 		Kinds:              graph.Kinds{graph.StringKind("kindA")},
// 	}

// 	kindB := NodeChange{
// 		NodeID:             "abc",
// 		changeType:         ChangeTypeModified,
// 		Properties:         nil,
// 		ModifiedProperties: nil,
// 		Deleted:            nil,
// 		Kinds:              graph.Kinds{graph.StringKind("kindB")},
// 	}

// 	changes := []*NodeChange{&a1, &a2b3, &c1, &kindB}
// 	merged, err := mergeNodeChanges(changes)
// 	require.NoError(t, err)

// 	require.Len(t, merged, 1)
// 	m := merged[0]

// 	require.Equal(t, m.Type(), ChangeTypeAdded)
// 	require.Len(t, m.Kinds, 2)
// 	require.Contains(t, m.Properties.MapOrEmpty(), "c")
// 	require.Contains(t, m.Deleted, "a")
// 	require.Contains(t, m.Deleted, "b")

// }

// func TestMergeNodeChanges2(t *testing.T) {
// 	a1b2c3 := NodeChange{
// 		NodeID:             "abc",
// 		changeType:         ChangeTypeAdded,
// 		Properties:         graph.NewProperties().SetAll(map[string]any{"a": 1, "b": 2, "c": 3}),
// 		ModifiedProperties: graph.NewProperties().SetAll(map[string]any{"a": 1, "b": 2, "c": 3}).MapOrEmpty(),
// 		Deleted:            nil,
// 		Kinds:              graph.Kinds{graph.StringKind("kindA")},
// 	}

// 	a2 := NodeChange{
// 		NodeID:             "abc",
// 		changeType:         ChangeTypeModified,
// 		Properties:         graph.NewProperties().SetAll(map[string]any{"a": 2}),
// 		ModifiedProperties: graph.NewProperties().SetAll(map[string]any{"a": 2}).MapOrEmpty(),
// 		Deleted:            []string{"b", "c"},
// 		Kinds:              graph.Kinds{graph.StringKind("kindA")},
// 	}

// 	d4 := NodeChange{
// 		NodeID:             "abc",
// 		changeType:         ChangeTypeModified,
// 		Properties:         graph.NewProperties().Set("d", 4),
// 		ModifiedProperties: graph.NewProperties().Set("d", 4).MapOrEmpty(),
// 		Deleted:            []string{"a"},
// 		Kinds:              graph.Kinds{graph.StringKind("kindA")},
// 	}

// 	changes := []*NodeChange{&a1b2c3, &a2, &d4}
// 	merged, err := mergeNodeChanges(changes)
// 	require.NoError(t, err)

// 	require.Len(t, merged, 1)
// 	m := merged[0]

// 	require.Equal(t, m.Type(), ChangeTypeAdded)
// 	require.Len(t, m.Kinds, 1)
// 	require.Len(t, m.Deleted, 0) // cancelled out
// 	require.Len(t, m.ModifiedProperties, 1)
// 	require.Contains(t, m.Properties.MapOrEmpty(), "d")

// }
