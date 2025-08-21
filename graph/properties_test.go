package graph_test

import (
	"strconv"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewProperties(t *testing.T) {
	properties := graph.NewProperties()

	require.Equal(t, 24, int(properties.SizeOf()))
	require.Equal(t, 0, properties.Len())
	require.Equal(t, nil, properties.Get("test").Any())
	require.Equal(t, map[string]any{}, properties.ModifiedProperties())
	require.Equal(t, []string(nil), properties.DeletedProperties())

	properties.Delete("test")
	require.False(t, properties.Exists("not found"))
	require.Equal(t, "default", properties.GetOrDefault("not found", "default").Any())

	// Set values to trigger allocation of the storage map
	properties.Set("test", "test")
	properties.Set("value", "test")

	require.Equal(t, 440, int(properties.SizeOf()))

	properties.Delete("value")

	require.Equal(t, 296, int(properties.SizeOf()))

	require.False(t, properties.Exists("not found"))
	require.Equal(t, 1, properties.Len())
	require.Equal(t, "default", properties.GetOrDefault("not found", "default").Any())
	require.Equal(t, "test", properties.GetOrDefault("test", "default").Any())
	require.Equal(t, "test", properties.Get("test").Any())

	properties.Delete("test")
	properties.Set("test", "other")

	require.Equal(t, map[string]any{"test": "other"}, properties.ModifiedProperties())

	_, markedForDeletion := properties.Deleted["test"]
	require.False(t, markedForDeletion)
	require.Equal(t, []string{"value"}, properties.DeletedProperties())
}

func TestSizeOfProperties(t *testing.T) {
	properties := graph.NewProperties()

	require.Equal(t, 24, int(properties.SizeOf()))

	// Set an initial value to force allocation
	properties.Set("initial", 0)
	require.Equal(t, 256, int(properties.SizeOf()))

	// Further allocation past 8 forces a new bucket to be allocated for the backing maps in the properties struct
	for iter := 0; iter < 8; iter++ {
		properties.Set("test"+strconv.Itoa(iter), iter)
	}

	require.Equal(t, 1600, int(properties.SizeOf()))

}

func TestGetWithFallbackProperties(t *testing.T) {
	properties := graph.NewProperties()

	// Set an initial value to force allocation
	properties.SetAll(map[string]any{"sugar": "Sugar", "spice": "Spice", "everythingNice": "Everything Nice"})

	t.Run("returns property when exists", func(t *testing.T) {
		value, err := properties.GetWithFallback("sugar", "").String()
		require.Nil(t, err)
		require.Equal(t, "Sugar", value)
	})

	t.Run("returns fallback property when original property doesn't exists", func(t *testing.T) {
		value, err := properties.GetWithFallback("missing", "", "spice").String()
		require.Nil(t, err)
		require.Equal(t, "Spice", value)
	})

	t.Run("returns default property when neither fallback nor property exists", func(t *testing.T) {
		value, err := properties.GetWithFallback("", "ChemicalX", "").String()
		require.Nil(t, err)
		require.Equal(t, "ChemicalX", value)
	})
}

func TestPropertiesKey(t *testing.T) {
	// sorted and ignores "z"
	props := graph.NewProperties()
	props.SetAll(map[string]any{
		"z": 1,
		"a": 2,
		"m": 3,
	})
	ignored := map[string]struct{}{
		"z": {},
	}

	got := props.Keys(ignored)
	want := []string{"a", "m"} // sorted, excludes "z"

	assert.Equal(t, want, got)

	// sorted and ignores all
	props = graph.NewProperties()
	props.SetAll(map[string]any{
		"a": 2,
	})
	ignored = map[string]struct{}{
		"a": {},
	}

	assert.Empty(t, props.Keys(ignored))

	// sorted and empty
	props = graph.NewProperties()
	assert.Empty(t, props.Keys(nil))
}

func TestPropertiesHash(t *testing.T) {
	t.Run("Hash is deterministic", func(t *testing.T) {
		props := graph.NewProperties().SetAll(map[string]any{
			"foo": "bar",
			"baz": 42,
		})
		h1, err1 := props.Hash(nil)
		h2, err2 := props.Hash(nil)

		require.NoError(t, err1)
		require.NoError(t, err2)
		assert.Equal(t, h1, h2)
	})

	t.Run("order of keys doesn't matter", func(t *testing.T) {
		a := graph.NewProperties().SetAll(map[string]any{"x": 1, "y": 2})
		b := graph.NewProperties().SetAll(map[string]any{"y": 2, "x": 1})

		h1, _ := a.Hash(nil)
		h2, _ := b.Hash(nil)

		assert.Equal(t, h1, h2)
	})

	t.Run("ignored keys", func(t *testing.T) {
		props := graph.NewProperties().SetAll(map[string]any{
			"keep": "yes",
			"drop": "no",
		})
		ignored := map[string]struct{}{"drop": {}}

		h1, _ := props.Hash(nil)
		h2, _ := props.Hash(ignored)

		assert.NotEqual(t, h1, h2)
	})
}
