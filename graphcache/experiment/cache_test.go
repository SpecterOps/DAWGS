package experiment

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestAssociativeCache_AddNode(t *testing.T) {
	go func() {
		http.ListenAndServe(":2112", nil)
	}()

	//cache, err := NewVoidCache("/tmp/test.db", "objectid")
	//require.NoError(t, err)

	cache := NewPackMap()
	kinds := graph.Kinds{
		graph.StringKind("base"),
		graph.StringKind("next"),
	}

	var hashes [][]byte
	const times = 100_000_000

	for idx := 0; idx < times; idx++ {
		if idx > 0 && idx%1_000_000 == 0 {
			fmt.Printf("Inserted %d\n", idx)
		}

		entityHash, err := cache.PutNode("1234-"+strconv.Itoa(idx), &graph.Node{
			ID:    graph.ID(idx),
			Kinds: kinds,
			Properties: graph.AsProperties(map[string]any{
				"objectid": "12345-12345-" + strconv.Itoa(idx),
				"other":    strconv.Itoa(idx),
			}),
		})

		require.NoError(t, err)
		hashes = append(hashes, entityHash)
	}

	require.NoError(t, cache.Compact())

	fmt.Printf("Memory prepped, validating %d buckets\n", len(cache.allocatedBuckets))

	cache.GetEntity( "1234-"+strconv.Itoa(1))

	then := time.Now()

	for idx := 0; idx < times; idx++ {
		hash, err := cache.GetEntity( "1234-"+strconv.Itoa(idx))

		require.NoError(t, err)
		require.NotEmpty(t, hash, "Missing hash for id %s", "1234-"+strconv.Itoa(idx))
		require.Equalf(t, hashes[idx], hash, "Hash mismatch for idx %v+ != %v+", hashes[idx], hash)

		if idx > 0 && idx %10_000 == 0 {
			fmt.Printf("Validated %d - %2.2f ms per-op\n", idx, float64(time.Since(then).Milliseconds())/float64(idx))
		}
	}

	fmt.Printf("Validated in %.2f seconds\n", time.Since(then).Seconds())

	//hash, err := cache.Get("anything")
	//
	//require.NoError(t, err)
	//require.Empty(t, hash)

	//hash, err := cache.Get("1234-999")
	//
	//require.NoError(t, err)
	//require.NotEmpty(t, hash)
	//
	//then := time.Now()
	//
	//fmt.Println()
	//
	//for idx := times - 1; idx >= 0; idx-- {
	//	hash, err := cache.Get("1234-" + strconv.Itoa(idx))
	//
	//	require.NoError(t, err)
	//	require.NotEmpty(t, hash)
	//	require.Len(t, hash, 8)
	//
	//	if idx > 0 && idx%1000 == 0 {
	//		elapsed := time.Since(then).Milliseconds()
	//		fmt.Printf("Elapsed time for %d lookups: %d ms - average ms per-op: %.2f ms\n", idx, elapsed, float64(elapsed)/float64(times-idx))
	//	}
	//}
	//
	//elapsed := time.Since(then).Milliseconds()
	//fmt.Printf("Elapsed time for %d lookups: %d ms - average ms per-op: %d ms\n", times, elapsed, elapsed/times)
	//
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, os.Interrupt)

	select {
	case <-sigC:
	}
}
