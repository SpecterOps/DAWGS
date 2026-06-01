package graph_test

import (
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/assert"
)

func TestOptimizeOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts []graph.OptimizeOption
		want []graph.TargetStorage
	}{
		{
			name: "no options yields nil Targets",
			opts: nil,
			want: nil,
		},
		{
			name: "OptimizeTargets with no args leaves Targets nil",
			opts: []graph.OptimizeOption{graph.OptimizeTargets()},
			want: nil,
		},
		{
			name: "single call preserves order",
			opts: []graph.OptimizeOption{graph.OptimizeTargets(graph.Nodes, graph.Relationships)},
			want: []graph.TargetStorage{graph.Nodes, graph.Relationships},
		},
		{
			name: "repeated calls accumulate",
			opts: []graph.OptimizeOption{
				graph.OptimizeTargets(graph.Nodes),
				graph.OptimizeTargets(graph.Relationships),
			},
			want: []graph.TargetStorage{graph.Nodes, graph.Relationships},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var cfg graph.OptimizeConfig
			for _, o := range tc.opts {
				o(&cfg)
			}
			assert.Equal(t, tc.want, cfg.Targets)
		})
	}
}
