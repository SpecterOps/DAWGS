package neo4j

import (
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func mapTestCase[T, V any](t *testing.T, source T, expected V) {
	var (
		value  V
		mapper = NewValueMapper()
	)

	require.True(t, mapper.Map(source, &value))
	require.Equalf(t, expected, value, "Mapping case for type %T to %T failed. Value is: %v", source, &value, value)
}

func Test_mapValue(t *testing.T) {
	var (
		utcNow         = time.Now().UTC()
		anyStringSlice = []any{"a", "b", "c"}
		stringSlice    = []string{"a", "b", "c"}
		kindSlice      = []graph.Kind{graph.StringKind("a"), graph.StringKind("b"), graph.StringKind("c")}
		kinds          = graph.Kinds{graph.StringKind("a"), graph.StringKind("b"), graph.StringKind("c")}
	)

	mapTestCase[uint, uint](t, 0, 0)
	mapTestCase[uint8, uint8](t, 0, 0)
	mapTestCase[uint16, uint16](t, 0, 0)
	mapTestCase[uint32, uint32](t, 0, 0)
	mapTestCase[uint64, uint64](t, 0, 0)

	mapTestCase(t, 0, 0) // Inferred int
	mapTestCase[int8, int8](t, 0, 0)
	mapTestCase[int16, int16](t, 0, 0)
	mapTestCase[int32, int32](t, 0, 0)
	mapTestCase[int64, int64](t, 0, 0)
	mapTestCase[int64, graph.ID](t, 0, 0)

	mapTestCase[float32, float32](t, 1.5, 1.5)
	mapTestCase(t, 1.5, 1.5) // Inferred float64

	mapTestCase(t, true, true)
	mapTestCase(t, "test", "test")

	mapTestCase(t, utcNow, utcNow)
	mapTestCase(t, utcNow.Format(time.RFC3339Nano), utcNow)
	mapTestCase(t, utcNow.Unix(), time.Unix(utcNow.Unix(), 0))
	mapTestCase(t, dbtype.Time(utcNow), utcNow)
	mapTestCase(t, dbtype.LocalTime(utcNow), utcNow)
	mapTestCase(t, dbtype.Date(utcNow), utcNow)
	mapTestCase(t, dbtype.LocalDateTime(utcNow), utcNow)

	mapTestCase(t, anyStringSlice, stringSlice)
	mapTestCase(t, anyStringSlice, kindSlice)
	mapTestCase(t, anyStringSlice, kinds)
}
