package ret

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/specterops/dawgs/ret/collection"
	"github.com/specterops/dawgs/ret/observe"
	"github.com/stretchr/testify/require"
)

func TestVerifyCollectionFullyVerifiesArtifactsAndReturnsOnlyAggregateCounts(t *testing.T) {
	// Break caught: forwarding the internal manifest instead of the public
	// aggregate result, or skipping concrete artifact verification.
	root := writeLoadCollection(t, []string{"second", "first"}, map[string]*dumpTestGraph{
		"second": {
			nodes:         dumpTestNodes(1, 2),
			relationships: dumpTestRelationships(10, 1, 2, "LINKED"),
		},
		"first": {nodes: dumpTestNodes(20)},
	}, true, true)
	var events []observe.Event

	result, err := VerifyCollection(context.Background(), VerifyCollectionConfig{
		Directory: root,
		Observer: observe.ObserverFunc(func(_ context.Context, event observe.Event) {
			events = append(events, event)
		}),
	})

	require.NoError(t, err)
	require.Equal(t, VerifyCollectionResult{GraphCount: 2, NodeCount: 3, RelationshipCount: 1}, result)
	require.IsType(t, observe.OperationStarted{}, events[0])
	require.IsType(t, observe.OperationCompleted{}, events[len(events)-1])
	started := events[0].(observe.OperationStarted)
	completed := events[len(events)-1].(observe.OperationCompleted)
	require.Equal(t, "verify_collection", started.Operation)
	require.Equal(t, "verify_collection", completed.Operation)
	require.NoError(t, completed.Err)
	var verified int
	for _, event := range events {
		if _, ok := event.(observe.ArtifactVerified); ok {
			verified++
		}
	}
	require.Equal(t, 6, verified)
}

func TestVerifyCollectionCorruptParquetReturnsInvalidCollection(t *testing.T) {
	// Break caught: accidentally switching the full verification facade to the
	// JSONL-only load preflight and ignoring corrupt configured Parquet.
	root := writeLoadCollection(t, []string{"asset"}, map[string]*dumpTestGraph{
		"asset": {nodes: dumpTestNodes(1)},
	}, true, true)
	manifest, err := collection.Read(root)
	require.NoError(t, err)
	parquetArtifact := manifest.Graphs[0].NodeShards[0].Parquet
	require.NoError(t, os.WriteFile(filepath.Join(root, filepath.FromSlash(parquetArtifact.Path)), []byte("corrupt"), 0o600))
	var events []observe.Event

	result, err := VerifyCollection(context.Background(), VerifyCollectionConfig{
		Directory: root,
		Observer: observe.ObserverFunc(func(_ context.Context, event observe.Event) {
			events = append(events, event)
		}),
	})

	require.ErrorIs(t, err, ErrInvalidCollection)
	require.Zero(t, result)
	completed, ok := events[len(events)-1].(observe.OperationCompleted)
	require.True(t, ok)
	require.ErrorIs(t, completed.Err, ErrInvalidCollection)
}

func TestVerifyCollectionRejectsInvalidConfigWithinObserverLifecycle(t *testing.T) {
	// Break caught: calling collection verification with an empty root or
	// omitting the terminal typed event on configuration failure.
	var events []observe.Event

	_, err := VerifyCollection(context.Background(), VerifyCollectionConfig{
		Observer: observe.ObserverFunc(func(_ context.Context, event observe.Event) {
			events = append(events, event)
		}),
	})

	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Len(t, events, 2)
	require.IsType(t, observe.OperationStarted{}, events[0])
	completed, ok := events[1].(observe.OperationCompleted)
	require.True(t, ok)
	require.ErrorIs(t, completed.Err, ErrInvalidConfig)
}

func TestVerifyCollectionCancellationOnFinalArtifactSuppressesSuccess(t *testing.T) {
	// Break caught: aggregating and returning success after the synchronous
	// observer cancels while receiving the final verified artifact.
	root := writeLoadCollection(t, []string{"asset"}, map[string]*dumpTestGraph{
		"asset": {nodes: dumpTestNodes(1)},
	}, true, true)
	ctx, cancel := context.WithCancel(context.Background())
	var events []observe.Event

	result, err := VerifyCollection(ctx, VerifyCollectionConfig{
		Directory: root,
		Observer: observe.ObserverFunc(func(_ context.Context, event observe.Event) {
			events = append(events, event)
			if value, ok := event.(observe.ArtifactVerified); ok && value.Format == "Parquet" {
				cancel()
			}
		}),
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, result)
	requireSingleCanceledTerminalEvent(t, events)
}
