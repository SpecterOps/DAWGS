package pg

import (
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/stretchr/testify/require"
)

const (
	// testNodeCompositeOID is the synthetic scalar node OID registered by codec unit tests.
	testNodeCompositeOID uint32 = 91_001

	// testNodeCompositeArrayOID is the synthetic node-array OID registered by codec unit tests.
	testNodeCompositeArrayOID uint32 = 91_002

	// testEdgeCompositeOID is the synthetic scalar edge OID registered by codec unit tests.
	testEdgeCompositeOID uint32 = 91_003

	// testEdgeCompositeArrayOID is the synthetic edge-array OID registered by codec unit tests.
	testEdgeCompositeArrayOID uint32 = 91_004

	// testPathCompositeOID is the synthetic path OID registered by codec unit tests.
	testPathCompositeOID uint32 = 91_005
)

// compositeCodecTestTypes provides a controllable test double for PostgreSQL composite decoding; graph values round-trip through pgx without losing identity or properties.
type compositeCodecTestTypes struct {
	// node is the registered scalar node type.
	node *pgtype.Type

	// nodeArray is the registered node-array type.
	nodeArray *pgtype.Type

	// edge is the registered scalar edge type.
	edge *pgtype.Type

	// edgeArray is the registered edge-array type.
	edgeArray *pgtype.Type

	// path is the registered scalar path type.
	path *pgtype.Type
}

// requirePGType returns the type registered for oid and fails the test when the registration is absent.
func requirePGType(t testing.TB, typeMap *pgtype.Map, oid uint32) *pgtype.Type {
	t.Helper()

	dataType, typeOK := typeMap.TypeForOID(oid)
	require.True(t, typeOK, "expected PostgreSQL type OID %d", oid)

	return dataType
}

// newCompositeCodecTestMap registers synthetic node, edge, and path definitions, optionally installing owned codecs.
func newCompositeCodecTestMap(t testing.TB, owned bool) (*pgtype.Map, compositeCodecTestTypes) {
	t.Helper()

	typeMap := pgtype.NewMap()
	types := compositeCodecTestTypes{}
	types.node = &pgtype.Type{
		Name: pgsql.NodeComposite.String(),
		OID:  testNodeCompositeOID,
		Codec: &pgtype.CompositeCodec{
			Fields: []pgtype.CompositeCodecField{
				{
					Name: "id",
					Type: requirePGType(t, typeMap, pgtype.Int8OID),
				},
				{
					Name: "kind_ids",
					Type: requirePGType(t, typeMap, pgtype.Int2ArrayOID),
				},
				{
					Name: "properties",
					Type: requirePGType(t, typeMap, pgtype.JSONBOID),
				},
			},
		},
	}
	if owned {
		require.NoError(t, installOwnedCompositeCodec(pgsql.NodeComposite, types.node))
	}
	typeMap.RegisterType(types.node)

	types.nodeArray = &pgtype.Type{
		Name: pgsql.NodeCompositeArray.String(),
		OID:  testNodeCompositeArrayOID,
		Codec: &pgtype.ArrayCodec{
			ElementType: types.node,
		},
	}
	if owned {
		require.NoError(t, installOwnedCompositeCodec(pgsql.NodeCompositeArray, types.nodeArray))
	}
	typeMap.RegisterType(types.nodeArray)

	types.edge = &pgtype.Type{
		Name: pgsql.EdgeComposite.String(),
		OID:  testEdgeCompositeOID,
		Codec: &pgtype.CompositeCodec{
			Fields: []pgtype.CompositeCodecField{
				{
					Name: "id",
					Type: requirePGType(t, typeMap, pgtype.Int8OID),
				},
				{
					Name: "start_id",
					Type: requirePGType(t, typeMap, pgtype.Int8OID),
				},
				{
					Name: "end_id",
					Type: requirePGType(t, typeMap, pgtype.Int8OID),
				},
				{
					Name: "kind_id",
					Type: requirePGType(t, typeMap, pgtype.Int2OID),
				},
				{
					Name: "properties",
					Type: requirePGType(t, typeMap, pgtype.JSONBOID),
				},
			},
		},
	}
	if owned {
		require.NoError(t, installOwnedCompositeCodec(pgsql.EdgeComposite, types.edge))
	}
	typeMap.RegisterType(types.edge)

	types.edgeArray = &pgtype.Type{
		Name: pgsql.EdgeCompositeArray.String(),
		OID:  testEdgeCompositeArrayOID,
		Codec: &pgtype.ArrayCodec{
			ElementType: types.edge,
		},
	}
	if owned {
		require.NoError(t, installOwnedCompositeCodec(pgsql.EdgeCompositeArray, types.edgeArray))
	}
	typeMap.RegisterType(types.edgeArray)

	types.path = &pgtype.Type{
		Name: pgsql.PathComposite.String(),
		OID:  testPathCompositeOID,
		Codec: &pgtype.CompositeCodec{
			Fields: []pgtype.CompositeCodecField{
				{
					Name: "nodes",
					Type: types.nodeArray,
				},
				{
					Name: "edges",
					Type: types.edgeArray,
				},
			},
		},
	}
	if owned {
		require.NoError(t, installOwnedCompositeCodec(pgsql.PathComposite, types.path))
	}
	typeMap.RegisterType(types.path)

	return typeMap, types
}

// testNodeComposite returns a representative node value with the requested ID.
func testNodeComposite(id int64) nodeComposite {
	return nodeComposite{
		ID:         id,
		KindIDs:    []int16{1, 2},
		Properties: map[string]any{"id": float64(id), "name": "node"},
	}
}

// testEdgeComposite returns a representative edge value with the requested identity and endpoints.
func testEdgeComposite(id, startID, endID int64) edgeComposite {
	return edgeComposite{
		ID:         id,
		StartID:    startID,
		EndID:      endID,
		KindID:     3,
		Properties: map[string]any{"id": float64(id), "name": "edge"},
	}
}

// TestOwnedCompositeCodecDecodeValue verifies scalar node, edge, and path values decode into owned concrete types.
func TestOwnedCompositeCodecDecodeValue(t *testing.T) {
	typeMap, types := newCompositeCodecTestMap(t, true)
	expectedNode := testNodeComposite(101)
	expectedEdge := testEdgeComposite(201, 101, 102)
	expectedPath := pathComposite{
		Nodes: []nodeComposite{expectedNode, testNodeComposite(102)},
		Edges: []edgeComposite{expectedEdge},
	}

	for _, testCase := range []struct {
		// name identifies the composite type and wire-format subtest.
		name string

		// format selects the pgx encoding format.
		format int16

		// dataType supplies the composite codec under test.
		dataType *pgtype.Type

		// value is the concrete composite expected after decoding.
		value any
	}{
		{
			name:     "node/binary",
			format:   pgtype.BinaryFormatCode,
			dataType: types.node,
			value:    expectedNode,
		},
		{
			name:     "node/text",
			format:   pgtype.TextFormatCode,
			dataType: types.node,
			value:    expectedNode,
		},
		{
			name:     "edge/binary",
			format:   pgtype.BinaryFormatCode,
			dataType: types.edge,
			value:    expectedEdge,
		},
		{
			name:     "edge/text",
			format:   pgtype.TextFormatCode,
			dataType: types.edge,
			value:    expectedEdge,
		},
		{
			name:     "path/binary",
			format:   pgtype.BinaryFormatCode,
			dataType: types.path,
			value:    expectedPath,
		},
		{
			name:     "path/text",
			format:   pgtype.TextFormatCode,
			dataType: types.path,
			value:    expectedPath,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			src, err := typeMap.Encode(testCase.dataType.OID, testCase.format, testCase.value, nil)
			require.NoError(t, err)

			decoded, err := testCase.dataType.Codec.DecodeValue(typeMap, testCase.dataType.OID, testCase.format, src)
			require.NoError(t, err)
			require.IsType(t, testCase.value, decoded)
			require.Equal(t, testCase.value, decoded)

			// pgx may reuse its receive buffer after Rows.Values returns. None of
			// the concrete composite's slices, strings, or maps may alias it.
			clear(src)
			require.Equal(t, testCase.value, decoded)
		})
	}
}

// TestOwnedCompositeCodecPreservesExplicitScanAndNull verifies explicit scan targets and null composites keep pgx semantics.
func TestOwnedCompositeCodecPreservesExplicitScanAndNull(t *testing.T) {
	typeMap, types := newCompositeCodecTestMap(t, true)
	expected := testNodeComposite(101)

	for _, format := range []int16{pgtype.BinaryFormatCode, pgtype.TextFormatCode} {
		src, err := typeMap.Encode(types.node.OID, format, expected, nil)
		require.NoError(t, err)

		var decoded nodeComposite
		require.NoError(t, typeMap.Scan(types.node.OID, format, src, &decoded))
		require.Equal(t, expected, decoded)

		nullValue, err := types.node.Codec.DecodeValue(typeMap, types.node.OID, format, nil)
		require.NoError(t, err)
		require.Nil(t, nullValue)
	}
}

// TestOwnedCompositeCodecFallsBackForNullInternalFields verifies nullable internal fields retain pgx's lossless map representation.
func TestOwnedCompositeCodecFallsBackForNullInternalFields(t *testing.T) {
	typeMap, types := newCompositeCodecTestMap(t, true)
	value := pgtype.CompositeFields{nil, []int16{1, 2}, map[string]any{"name": "nullable"}}

	for _, format := range []int16{pgtype.BinaryFormatCode, pgtype.TextFormatCode} {
		src, err := typeMap.Encode(types.node.OID, format, value, nil)
		require.NoError(t, err)

		decoded, err := types.node.Codec.DecodeValue(typeMap, types.node.OID, format, src)
		require.NoError(t, err)
		require.Equal(t, map[string]any{
			"id":         nil,
			"kind_ids":   []any{int16(1), int16(2)},
			"properties": map[string]any{"name": "nullable"},
		}, decoded)

		arraySource := []pgtype.CompositeFields{value}
		src, err = typeMap.Encode(types.nodeArray.OID, format, arraySource, nil)
		require.NoError(t, err)

		decoded, err = types.nodeArray.Codec.DecodeValue(typeMap, types.nodeArray.OID, format, src)
		require.NoError(t, err)
		require.Equal(t, []any{map[string]any{
			"id":         nil,
			"kind_ids":   []any{int16(1), int16(2)},
			"properties": map[string]any{"name": "nullable"},
		}}, decoded)
	}
}

// TestOwnedCompositeCodecSupportsArrays verifies non-null composite arrays decode directly into typed slices.
func TestOwnedCompositeCodecSupportsArrays(t *testing.T) {
	typeMap, types := newCompositeCodecTestMap(t, true)
	first := testNodeComposite(101)
	second := testNodeComposite(102)
	expectedNodes := []nodeComposite{first, second}
	expectedEdges := []edgeComposite{
		testEdgeComposite(201, 101, 102),
		testEdgeComposite(202, 102, 103),
	}

	for _, format := range []int16{pgtype.BinaryFormatCode, pgtype.TextFormatCode} {
		src, err := typeMap.Encode(types.nodeArray.OID, format, expectedNodes, nil)
		require.NoError(t, err)

		decoded, err := types.nodeArray.Codec.DecodeValue(typeMap, types.nodeArray.OID, format, src)
		require.NoError(t, err)
		require.Equal(t, expectedNodes, decoded)

		var typedValues []nodeComposite
		require.NoError(t, typeMap.Scan(types.nodeArray.OID, format, src, &typedValues))
		require.Equal(t, expectedNodes, typedValues)

		src, err = typeMap.Encode(types.edgeArray.OID, format, expectedEdges, nil)
		require.NoError(t, err)

		decoded, err = types.edgeArray.Codec.DecodeValue(typeMap, types.edgeArray.OID, format, src)
		require.NoError(t, err)
		require.Equal(t, expectedEdges, decoded)

		var typedEdges []edgeComposite
		require.NoError(t, typeMap.Scan(types.edgeArray.OID, format, src, &typedEdges))
		require.Equal(t, expectedEdges, typedEdges)
	}
}

// TestOwnedCompositeCodecArrayPreservesNullElements verifies arrays containing null composites retain a nullable representation.
func TestOwnedCompositeCodecArrayPreservesNullElements(t *testing.T) {
	typeMap, types := newCompositeCodecTestMap(t, true)
	first := testNodeComposite(101)
	values := []*nodeComposite{&first, nil}

	for _, format := range []int16{pgtype.BinaryFormatCode, pgtype.TextFormatCode} {
		src, err := typeMap.Encode(types.nodeArray.OID, format, values, nil)
		require.NoError(t, err)

		decoded, err := types.nodeArray.Codec.DecodeValue(typeMap, types.nodeArray.OID, format, src)
		require.NoError(t, err)
		require.Equal(t, []any{first, nil}, decoded)
	}
}

// TestInstallOwnedCompositeCodec verifies supported definitions are wrapped and incompatible definitions are rejected.
func TestInstallOwnedCompositeCodec(t *testing.T) {
	for _, testCase := range []struct {
		// dataType identifies the supported composite definition to install.
		dataType pgsql.DataType

		// value selects the concrete owned codec type expected for dataType.
		value any
	}{
		{
			dataType: pgsql.NodeComposite,
			value:    nodeComposite{},
		},
		{
			dataType: pgsql.EdgeComposite,
			value:    edgeComposite{},
		},
		{
			dataType: pgsql.PathComposite,
			value:    pathComposite{},
		},
	} {
		t.Run(testCase.dataType.String(), func(t *testing.T) {
			definition := &pgtype.Type{
				Name:  testCase.dataType.String(),
				OID:   testNodeCompositeOID,
				Codec: &pgtype.CompositeCodec{},
			}

			require.NoError(t, installOwnedCompositeCodec(testCase.dataType, definition))
			require.NotEqual(t, reflect.TypeOf(&pgtype.CompositeCodec{}), reflect.TypeOf(definition.Codec))
		})
	}

	arrayDefinition := &pgtype.Type{Codec: &pgtype.ArrayCodec{}}
	require.NoError(t, installOwnedCompositeCodec(pgsql.NodeCompositeArray, arrayDefinition))
	require.IsType(t, &ownedCompositeArrayCodec[nodeComposite]{}, arrayDefinition.Codec)

	invalidDefinition := &pgtype.Type{Codec: pgtype.TextCodec{}}
	require.ErrorContains(t, installOwnedCompositeCodec(pgsql.NodeComposite, invalidDefinition), "*pgtype.CompositeCodec")
}

// compositeCodecBenchmarkSink retains decoded values so benchmark work cannot be optimized away.
var compositeCodecBenchmarkSink any

// benchmarkCompositeDecodeValue repeatedly decodes one encoded value through the selected codec implementation.
func benchmarkCompositeDecodeValue(
	b *testing.B,
	owned bool,
	dataType func(compositeCodecTestTypes) *pgtype.Type,
	value any,
) {
	b.Helper()

	typeMap, types := newCompositeCodecTestMap(b, owned)
	selectedType := dataType(types)
	src, err := typeMap.Encode(selectedType.OID, pgtype.BinaryFormatCode, value, nil)
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		decoded, err := selectedType.Codec.DecodeValue(typeMap, selectedType.OID, pgtype.BinaryFormatCode, src)
		if err != nil {
			b.Fatal(err)
		}
		compositeCodecBenchmarkSink = decoded
	}
}

// BenchmarkNodeCompositeDecodeValue compares scalar node decoding through stock and owned codecs.
func BenchmarkNodeCompositeDecodeValue(b *testing.B) {
	value := testNodeComposite(101)
	for _, testCase := range []struct {
		// name identifies whether the benchmark uses stock or owned decoding.
		name string

		// owned enables the owned composite codec when true.
		owned bool
	}{
		{
			name:  "map",
			owned: false,
		},
		{
			name:  "owned",
			owned: true,
		},
	} {
		b.Run(testCase.name, func(b *testing.B) {
			benchmarkCompositeDecodeValue(b, testCase.owned, func(types compositeCodecTestTypes) *pgtype.Type {
				return types.node
			}, value)
		})
	}
}

// BenchmarkNodeCompositeArrayDecodeValue compares node-array decoding through stock and owned codecs.
func BenchmarkNodeCompositeArrayDecodeValue(b *testing.B) {
	values := make([]nodeComposite, 128)
	for idx := range values {
		values[idx] = testNodeComposite(int64(idx + 1))
	}

	for _, testCase := range []struct {
		// name identifies whether the benchmark uses stock or owned decoding.
		name string

		// owned enables the owned composite codec when true.
		owned bool
	}{
		{
			name:  "map",
			owned: false,
		},
		{
			name:  "owned",
			owned: true,
		},
	} {
		b.Run(testCase.name, func(b *testing.B) {
			benchmarkCompositeDecodeValue(b, testCase.owned, func(types compositeCodecTestTypes) *pgtype.Type {
				return types.nodeArray
			}, values)
		})
	}
}

// BenchmarkPathCompositeDecodeValue compares path decoding through stock and owned codecs.
func BenchmarkPathCompositeDecodeValue(b *testing.B) {
	value := pathComposite{
		Nodes: make([]nodeComposite, 32),
		Edges: make([]edgeComposite, 31),
	}
	for idx := range value.Nodes {
		value.Nodes[idx] = testNodeComposite(int64(idx + 1))
	}
	for idx := range value.Edges {
		value.Edges[idx] = testEdgeComposite(int64(idx+1), int64(idx+1), int64(idx+2))
	}

	for _, testCase := range []struct {
		// name identifies whether the benchmark uses stock or owned decoding.
		name string

		// owned enables the owned composite codec when true.
		owned bool
	}{
		{
			name:  "map",
			owned: false,
		},
		{
			name:  "owned",
			owned: true,
		},
	} {
		b.Run(testCase.name, func(b *testing.B) {
			benchmarkCompositeDecodeValue(b, testCase.owned, func(types compositeCodecTestTypes) *pgtype.Type {
				return types.path
			}, value)
		})
	}
}
