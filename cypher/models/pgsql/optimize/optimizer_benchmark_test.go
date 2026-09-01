package optimize

import (
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
)

const trivialOptimizerBenchmarkQuery = `MATCH (n) RETURN n`

func BenchmarkOptimizeTrivialQuery(b *testing.B) {
	benchmarkOptimizeQuery(b, trivialOptimizerBenchmarkQuery)
}

func BenchmarkOptimizeADCSQuery(b *testing.B) {
	benchmarkOptimizeQuery(b, adcsQuery)
}

func benchmarkOptimizeQuery(b *testing.B, query string) {
	b.Helper()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), query)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		if _, err := Optimize(regularQuery); err != nil {
			b.Fatal(err)
		}
	}
}
