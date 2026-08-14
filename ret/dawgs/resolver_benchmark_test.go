package dawgs_test

import (
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ret/dawgs"
)

var (
	resolverBenchmarkID    graph.ID
	resolverBenchmarkFound bool
)

func BenchmarkResolverOperations(b *testing.B) {
	b.Run("Numeric", func(b *testing.B) {
		resolver := dawgs.NewResolver(1)
		resolver.Put("184467", graph.ID(42))

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			resolverBenchmarkID, resolverBenchmarkFound = resolver.Resolve("184467")
		}
	})

	b.Run("String", func(b *testing.B) {
		resolver := dawgs.NewResolver(1)
		resolver.Put("node-alpha", graph.ID(42))

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			resolverBenchmarkID, resolverBenchmarkFound = resolver.Resolve("node-alpha")
		}
	})
}
