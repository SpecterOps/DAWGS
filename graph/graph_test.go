package graph_test

import (
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/assert"
)

func TestWithBatchSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		batchSize    int
		expectedSize int
	}{
		{
			name:         "sets batch size to provided value",
			batchSize:    500,
			expectedSize: 500,
		},
		{
			name:         "sets batch size to zero",
			batchSize:    0,
			expectedSize: 0,
		},
		{
			name:         "sets batch size to large value",
			batchSize:    1_000_000,
			expectedSize: 1_000_000,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			config := &graph.BatchConfig{}
			opt := graph.WithBatchSize(tc.batchSize)
			opt(config)
			assert.Equal(t, tc.expectedSize, config.BatchSize)
		})
	}
}
