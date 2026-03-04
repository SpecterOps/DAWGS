package pgsql_test

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/assert"
)

func TestDeletedPropertiesToString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func() *graph.Properties
		check func(t *testing.T, result string)
	}{
		{
			name: "no deleted properties returns empty braces",
			setup: func() *graph.Properties {
				return graph.NewProperties()
			},
			check: func(t *testing.T, result string) {
				assert.Equal(t, "{}", result)
			},
		},
		{
			name: "single deleted property is wrapped in braces",
			setup: func() *graph.Properties {
				props := graph.NewProperties()
				props.Set("mykey", "myvalue")
				props.Delete("mykey")
				return props
			},
			check: func(t *testing.T, result string) {
				assert.Equal(t, "{mykey}", result)
			},
		},
		{
			name: "multiple deleted properties are all present in output",
			setup: func() *graph.Properties {
				props := graph.NewProperties()
				props.Set("alpha", 1)
				props.Set("beta", 2)
				props.Delete("alpha")
				props.Delete("beta")
				return props
			},
			check: func(t *testing.T, result string) {
				// Map iteration order is non-deterministic; accept either ordering.
				assert.True(t, result == "{alpha,beta}" || result == "{beta,alpha}",
					"unexpected result: %s", result)
			},
		},
		{
			name: "non-deleted properties are not included",
			setup: func() *graph.Properties {
				props := graph.NewProperties()
				props.Set("active", "yes")
				props.Set("removed", "no")
				props.Delete("removed")
				return props
			},
			check: func(t *testing.T, result string) {
				assert.Equal(t, "{removed}", result)
				assert.NotContains(t, result, "active")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			props := tc.setup()
			result := pgsql.DeletedPropertiesToString(props)
			tc.check(t, result)
		})
	}
}
