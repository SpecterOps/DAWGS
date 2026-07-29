package ret

import (
	"context"
	"fmt"
	"time"

	"github.com/specterops/dawgs/ret/collection"
	"github.com/specterops/dawgs/ret/observe"
)

const verifyCollectionOperationName = "verify_collection"

// VerifyCollection validates every configured artifact in a collection and
// returns aggregate entity counts.
func VerifyCollection(
	ctx context.Context,
	config VerifyCollectionConfig,
) (result VerifyCollectionResult, resultErr error) {
	started := time.Now()
	observe.Emit(ctx, config.Observer, observe.OperationStarted{Operation: verifyCollectionOperationName})
	defer func() {
		observe.Emit(ctx, config.Observer, observe.OperationCompleted{
			Operation: verifyCollectionOperationName,
			Duration:  time.Since(started),
			Err:       resultErr,
		})
	}()

	if err := ctx.Err(); err != nil {
		return VerifyCollectionResult{}, fmt.Errorf("verify collection: %w", err)
	}
	if err := config.Validate(); err != nil {
		return VerifyCollectionResult{}, err
	}
	verification, err := collection.Verify(ctx, config.Directory, config.Observer)
	if err != nil {
		return VerifyCollectionResult{}, fmt.Errorf("%w: %w", ErrInvalidCollection, err)
	}
	if err := ctx.Err(); err != nil {
		return VerifyCollectionResult{}, fmt.Errorf("verify collection completion: %w", err)
	}
	result.GraphCount = len(verification.Manifest.Graphs)
	for _, graphEntry := range verification.Manifest.Graphs {
		result.NodeCount += graphEntry.NodeCount
		result.RelationshipCount += graphEntry.RelationshipCount
	}
	if err := ctx.Err(); err != nil {
		return VerifyCollectionResult{}, fmt.Errorf("verify collection completion: %w", err)
	}
	return result, nil
}
