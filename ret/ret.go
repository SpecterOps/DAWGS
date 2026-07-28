package ret

import (
	"context"

	"github.com/specterops/dawgs/graph"
)

const (
	DefaultShardSize        = 100_000
	DefaultBatchSize        = 10_000
	DefaultProgressInterval = 250_000
)

type DumpResult struct {
}

type DumpConfig struct {
}

func Dump(ctx context.Context, db graph.Database, config DumpConfig) (DumpResult, error) {
	faucet := newGraphFaucet(db, graph.Graph{Name: "default"}, DefaultBatchSize)
	transformer := newTransformer(scrubber{})

	for {
		nodes, err := faucet.NextNodeBatch(ctx)
		if err != nil {
			return DumpResult{}, err
		}

		if len(nodes) == 0 {
			break
		}

		_, err = transformer.TransformNodes(nodes)
		if err != nil {
			return DumpResult{}, err
		}

	}

	return DumpResult{}, nil
}
