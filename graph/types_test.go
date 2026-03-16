package graph_test

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/specterops/dawgs/cardinality"
	"github.com/specterops/dawgs/graph"
)

func generateKinds(numKinds int) graph.Kinds {
	var kinds graph.Kinds

	for kindIdx := range numKinds {
		kinds = kinds.Add(graph.StringKind("Kind" + strconv.Itoa(kindIdx+1)))
	}

	return kinds
}

func Test_ThreadSafeKindBitmap_ConcurrentAccess(t *testing.T) {
	var (
		instance = graph.NewThreadSafeKindBitmap()
		scratch  = cardinality.NewBitmap64()
		kinds    = generateKinds(1_00)
		workerWG = &sync.WaitGroup{}
	)

	ctx, done := context.WithCancel(context.Background())
	defer done()

	workerWG.Add(1)

	go func() {
		defer workerWG.Done()

		iteration := 0

		for {
			for range 1_000 {
				scratch.Add(rand.Uint64() % 1_000_000)
			}

			kind := kinds[iteration%len(kinds)]
			iteration += 1

			instance.Or(kind, scratch)
			scratch.Clear()

			select {
			case <-ctx.Done():
				return

			default:
			}
		}
	}()

	for range 4 {
		workerWG.Add(1)

		go func() {
			defer workerWG.Done()

			for {
				for i := range 10_000 {
					kind := kinds[i%len(kinds)]

					for range 100_000 {
						instance.Cardinality(kind)

						select {
						case <-ctx.Done():
							return

						default:
						}
					}
				}
			}
		}()
	}

	time.Sleep(time.Millisecond * 250)
	done()

	workerWG.Wait()
}
