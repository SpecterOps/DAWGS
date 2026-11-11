//go:build manual_integration

package integration

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/stretchr/testify/require"
)

const (
	// pathBatchStudyEnv enables the optional path materialization timing study.
	pathBatchStudyEnv = "PATH_BATCH_STUDY"

	// pathBatchStudyEnabledValue selects explicit timing runs instead of ordinary correctness checks.
	pathBatchStudyEnabledValue = "1"

	// pathBatchReportDirectoryEnv overrides the default directory for study artifacts.
	pathBatchReportDirectoryEnv = "PATH_BATCH_REPORT_DIR"

	// pathBatchStudyWarmupRounds primes prepared queries before measured observations.
	pathBatchStudyWarmupRounds = 6

	// pathBatchStudySampleRounds records the warm observations used by each median.
	pathBatchStudySampleRounds = 5

	// pathBatchMemorySamplingInterval sets the client heap sampler's observation frequency.
	pathBatchMemorySamplingInterval = time.Millisecond

	// pathBatchNodeKind identifies nodes created by the path materialization experiment.
	pathBatchNodeKind = "PathBatchNode"

	// pathBatchEdgeKindA identifies the first relationship type used by the experiment's mixed paths.
	pathBatchEdgeKindA = "PathBatchA"

	// pathBatchEdgeKindB identifies the second relationship type used by the experiment's mixed paths.
	pathBatchEdgeKindB = "PathBatchB"
)

// pathBatchSharedFixture converges every layer onto width nodes, with both
// relationship kinds between each adjacent pair. Exactly (2*width)^3 paths of
// depth three therefore reuse a small set of complete node and edge records.
func pathBatchSharedFixture(width, propertyBytes int) (*opengraph.Graph, int) {
	fixture := &opengraph.Graph{}
	pathBatchAddNode(fixture, "root", propertyBytes)
	previous := []string{"root"}
	for layer := 1; layer <= 3; layer++ {
		var next []string
		for index := range width {
			id := fmt.Sprintf("layer-%d-%d", layer, index)
			pathBatchAddNode(fixture, id, propertyBytes)
			next = append(next, id)
			for _, start := range previous {
				for _, kind := range []string{pathBatchEdgeKindA, pathBatchEdgeKindB} {
					pathBatchAddEdge(fixture, start, id, kind, propertyBytes)
				}
			}
		}
		previous = next
	}
	branches := 2 * width
	return fixture, branches * branches * branches
}

// pathBatchLowOverlapFixture has independent three-edge chains. Its paths
// share only the root, allowing the same result cardinality and path depth as
// the shared fixture while requiring nearly every entity to be fetched.
func pathBatchLowOverlapFixture(pathCount, propertyBytes int) (*opengraph.Graph, int) {
	fixture := &opengraph.Graph{}
	pathBatchAddNode(fixture, "root", propertyBytes)
	for chain := range pathCount {
		previous := "root"
		for layer := 1; layer <= 3; layer++ {
			id := fmt.Sprintf("chain-%d-%d", chain, layer)
			pathBatchAddNode(fixture, id, propertyBytes)
			kind := pathBatchEdgeKindA
			if (chain+layer)%2 == 0 {
				kind = pathBatchEdgeKindB
			}
			pathBatchAddEdge(fixture, previous, id, kind, propertyBytes)
			previous = id
		}
	}
	return fixture, pathCount
}

// pathBatchAddNode appends a fixture node with the experiment's node kind and supplied properties.
func pathBatchAddNode(fixture *opengraph.Graph, id string, propertyBytes int) {
	fixture.Nodes = append(fixture.Nodes, opengraph.Node{
		ID:         id,
		Kinds:      []string{pathBatchNodeKind},
		Properties: pathBatchProperties("node/"+id, len(fixture.Nodes), propertyBytes),
	})
}

// pathBatchAddEdge appends a typed fixture relationship with the supplied endpoints and properties.
func pathBatchAddEdge(fixture *opengraph.Graph, start, end, kind string, propertyBytes int) {
	identity := fmt.Sprintf("edge/%d/%s/%s/%s", len(fixture.Edges), start, end, kind)
	fixture.Edges = append(fixture.Edges, opengraph.Edge{
		StartID:    start,
		EndID:      end,
		Kind:       kind,
		Properties: pathBatchProperties(identity, len(fixture.Edges), propertyBytes),
	})
}

// pathBatchProperties builds the deterministic property payload used by one fixture entity.
func pathBatchProperties(identity string, ordinal, propertyBytes int) map[string]any {
	properties := map[string]any{
		"name": identity, "ordinal": int64(ordinal), "flag": ordinal%2 == 0,
		"large_exact": int64(9007199254740993) + int64(ordinal),
		"tags":        []string{identity, "path-batch", strconv.Itoa(ordinal % 7)},
	}
	// Absence varies by entity so a hydration bug cannot pass by returning an
	// identical constant property bag for all records.
	if ordinal%3 != 0 {
		properties["sometimes"] = fmt.Sprintf("present-%d", ordinal)
	}
	if propertyBytes > 0 {
		properties["payload"] = pathBatchPayload(identity, propertyBytes)
	}
	return properties
}

// pathBatchPayload generates entity-specific deterministic text that avoids compressible repeated filler.
//
// Hash-derived base64 text is deterministic, differs for every entity, and
// avoids the misleading TOAST compression obtained from repeated filler text.
func pathBatchPayload(identity string, size int) string {
	var payload strings.Builder
	payload.Grow(size + 44)
	for block := 0; payload.Len() < size; block++ {
		digest := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", identity, block)))
		payload.WriteString(base64.RawStdEncoding.EncodeToString(digest[:]))
	}
	return payload.String()[:size]
}

// pathBatchCanonicalValue retains integer precision, concrete numeric
// types, nulls, and ordered arrays. It never round-trips properties through
// float64, unlike marshaling followed by unmarshaling ordinary JSON.
type pathBatchCanonicalValue struct {
	// Type retains the property's concrete type for exact-value comparisons.
	Type string `json:"type"`
	// Value contains the normalized value without collapsing distinct numeric representations.
	Value any `json:"value"`
}

// pathBatchCanonicalMapEntry retains a map key together with its type-preserving property value.
type pathBatchCanonicalMapEntry struct {
	// Key retains the property map key paired with its normalized value.
	Key string `json:"key"`
	// Value contains the property's type-preserving normalized representation.
	Value pathBatchCanonicalValue `json:"value"`
}

// pathBatchCanonicalNode captures a path node's identity, kinds and complete normalized properties.
type pathBatchCanonicalNode struct {
	// ID retains the node identity at its path position.
	ID graph.ID `json:"id"`
	// Kinds contains sorted kind names for order-independent label comparisons.
	Kinds []string `json:"kinds"`
	// Properties contains all normalized node properties.
	Properties pathBatchCanonicalValue `json:"properties"`
}

// pathBatchCanonicalEdge captures a path relationship's identity, direction, type and complete properties.
type pathBatchCanonicalEdge struct {
	// ID retains the relationship identity at its path position.
	ID graph.ID `json:"id"`
	// StartID retains the relationship's directed start endpoint.
	StartID graph.ID `json:"start_id"`
	// EndID retains the relationship's directed end endpoint.
	EndID graph.ID `json:"end_id"`
	// Kind retains the relationship type name.
	Kind string `json:"kind"`
	// Properties contains all normalized relationship properties.
	Properties pathBatchCanonicalValue `json:"properties"`
}

// pathBatchCanonicalPath preserves ordered complete node and relationship values for exact comparisons.
type pathBatchCanonicalPath struct {
	// Nodes retains complete node values in path order, including nil positions.
	Nodes []*pathBatchCanonicalNode `json:"nodes"`
	// Edges retains complete relationship values in path order, including nil positions.
	Edges []*pathBatchCanonicalEdge `json:"edges"`
}

// TestPathBatchCompleteValueCanonicalization checks that the equality oracle detects incorrect path hydration and property types.
//
// The experiment's equality oracle must reject missing or incorrectly hydrated
// values even when every path retains the same identity sequence.
func TestPathBatchCompleteValueCanonicalization(t *testing.T) {
	newPath := func() graph.Path {
		return graph.Path{
			Nodes: []*graph.Node{
				graph.NewNode(1, graph.AsProperties(map[string]any{"exact": int64(9007199254740993), "flag": true,
					"tags": []string{"one", "two"}}), graph.StringKind("A"), graph.StringKind("B")),
				graph.NewNode(2, graph.AsProperties(map[string]any{"name": "second"}), graph.StringKind("A")),
			},
			Edges: []*graph.Relationship{graph.NewRelationship(3, 1, 2,
				graph.AsProperties(map[string]any{"weight": int64(4)}), graph.StringKind("Edge"))},
		}
	}
	want := pathBatchFullValue(t, newPath())
	for _, test := range []struct {
		// name identifies the complete-value corruption applied to the path fixture.
		name string
		// mutate changes one path value to verify that equality rejects incorrect hydration.
		mutate func(*graph.Path)
	}{
		{
			name:   "integer_precision",
			mutate: func(path *graph.Path) { path.Nodes[0].Properties.Map["exact"] = int64(9007199254740992) },
		},
		{
			name:   "integer_type",
			mutate: func(path *graph.Path) { path.Nodes[0].Properties.Map["exact"] = uint64(9007199254740993) },
		},
		{
			name:   "property_absence",
			mutate: func(path *graph.Path) { delete(path.Nodes[0].Properties.Map, "flag") },
		},
		{
			name:   "property_null",
			mutate: func(path *graph.Path) { path.Nodes[0].Properties.Map["absent"] = nil },
		},
		{
			name:   "array_order",
			mutate: func(path *graph.Path) { path.Nodes[0].Properties.Map["tags"] = []string{"two", "one"} },
		},
		{
			name:   "node_order",
			mutate: func(path *graph.Path) { path.Nodes[0], path.Nodes[1] = path.Nodes[1], path.Nodes[0] },
		},
		{
			name:   "node_kind",
			mutate: func(path *graph.Path) { path.Nodes[0].Kinds = graph.Kinds{graph.StringKind("A")} },
		},
		{
			name:   "edge_start",
			mutate: func(path *graph.Path) { path.Edges[0].StartID = 2 },
		},
		{
			name:   "edge_end",
			mutate: func(path *graph.Path) { path.Edges[0].EndID = 1 },
		},
		{
			name:   "edge_kind",
			mutate: func(path *graph.Path) { path.Edges[0].Kind = graph.StringKind("Other") },
		},
		{
			name:   "edge_property",
			mutate: func(path *graph.Path) { path.Edges[0].Properties.Map["weight"] = int64(5) },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := newPath()
			test.mutate(&path)
			require.NotEqual(t, want, pathBatchFullValue(t, path))
		})
	}
	path := newPath()
	path.Nodes[0].Kinds[0], path.Nodes[0].Kinds[1] = path.Nodes[0].Kinds[1], path.Nodes[0].Kinds[0]
	require.Equal(t, want, pathBatchFullValue(t, &path), "kind sets are unordered")
	require.Equal(t, "null", pathBatchFullValue(t, (*graph.Path)(nil)))
	require.NotEqual(t, "null", pathBatchFullValue(t, graph.Path{}), "null differs from an empty path")
}

// pathBatchFullValues returns a sorted multiset: duplicate result occurrences
// remain duplicate signatures, while result row order is immaterial. Each row
// must project one path or a null path.
func pathBatchFullValues(t *testing.T, result graph.Result) []string {
	t.Helper()
	defer result.Close()
	values := []string{}
	for result.Next() {
		require.Len(t, result.Values(), 1)
		raw := result.Values()[0]
		if pathBatchNull(raw) {
			values = append(values, pathBatchFullValue(t, nil))
			continue
		}
		var path graph.Path
		require.True(t, result.Mapper().Map(raw, &path), "cannot map %T to path", raw)
		values = append(values, pathBatchFullValue(t, path))
	}
	require.NoError(t, result.Error())
	sort.Strings(values)
	return values
}

// pathBatchFullValue includes complete ordered entity values, rather than
// checking only IDs. Kind order is canonicalized because labels form a set.
func pathBatchFullValue(t *testing.T, value any) string {
	t.Helper()
	if pathBatchNull(value) {
		return "null"
	}
	var path graph.Path
	switch typed := value.(type) {
	case graph.Path:
		path = typed
	case *graph.Path:
		path = *typed
	default:
		t.Fatalf("expected path or null, received %T", value)
	}
	canonical := pathBatchCanonicalPath{
		Nodes: make([]*pathBatchCanonicalNode, len(path.Nodes)),
		Edges: make([]*pathBatchCanonicalEdge, len(path.Edges)),
	}
	for index, node := range path.Nodes {
		if node == nil {
			continue
		}
		kinds := node.Kinds.Strings()
		sort.Strings(kinds)
		canonical.Nodes[index] = &pathBatchCanonicalNode{
			ID:         node.ID,
			Kinds:      kinds,
			Properties: pathBatchCanonicalProperty(t, reflect.ValueOf(node.Properties.MapOrEmpty())),
		}
	}
	for index, edge := range path.Edges {
		if edge == nil {
			continue
		}
		var kind string
		if edge.Kind != nil {
			kind = edge.Kind.String()
		}
		canonical.Edges[index] = &pathBatchCanonicalEdge{
			ID:         edge.ID,
			StartID:    edge.StartID,
			EndID:      edge.EndID,
			Kind:       kind,
			Properties: pathBatchCanonicalProperty(t, reflect.ValueOf(edge.Properties.MapOrEmpty())),
		}
	}
	encoded, err := json.Marshal(canonical)
	require.NoError(t, err)
	return string(encoded)
}

// pathBatchNull reports whether a reflected value represents an absent or typed-nil value.
func pathBatchNull(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	return (reflected.Kind() == reflect.Pointer || reflected.Kind() == reflect.Interface) && reflected.IsNil()
}

// pathBatchCanonicalProperty normalizes a property into a type-preserving value for complete-result comparisons.
func pathBatchCanonicalProperty(t *testing.T, value reflect.Value) pathBatchCanonicalValue {
	t.Helper()
	if !value.IsValid() {
		return pathBatchCanonicalValue{
			Type: "null",
		}
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			return pathBatchCanonicalValue{
				Type: "null",
			}
		}
		return pathBatchCanonicalProperty(t, value.Elem())
	}
	canonical := pathBatchCanonicalValue{
		Type: value.Type().String(),
	}
	if timestamp, ok := value.Interface().(time.Time); ok {
		canonical.Value = timestamp.Format(time.RFC3339Nano)
		return canonical
	}
	switch value.Kind() {
	case reflect.Bool:
		canonical.Value = value.Bool()
	case reflect.String:
		canonical.Value = value.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		canonical.Value = strconv.FormatInt(value.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		canonical.Value = strconv.FormatUint(value.Uint(), 10)
	case reflect.Float32:
		canonical.Value = strconv.FormatUint(uint64(math.Float32bits(float32(value.Float()))), 16)
	case reflect.Float64:
		canonical.Value = strconv.FormatUint(math.Float64bits(value.Float()), 16)
	case reflect.Map:
		require.Equal(t, reflect.String, value.Type().Key().Kind(), "property map keys must be strings")
		if value.IsNil() {
			break
		}
		keys := value.MapKeys()
		sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
		entries := make([]pathBatchCanonicalMapEntry, 0, len(keys))
		for _, key := range keys {
			entries = append(entries, pathBatchCanonicalMapEntry{
				Key:   key.String(),
				Value: pathBatchCanonicalProperty(t, value.MapIndex(key)),
			})
		}
		canonical.Value = entries
	case reflect.Array, reflect.Slice:
		if value.Kind() == reflect.Slice && value.IsNil() {
			break
		}
		items := make([]pathBatchCanonicalValue, value.Len())
		for index := range items {
			items[index] = pathBatchCanonicalProperty(t, value.Index(index))
		}
		canonical.Value = items
	case reflect.Pointer:
		if !value.IsNil() {
			canonical.Value = pathBatchCanonicalProperty(t, value.Elem())
		}
	default:
		t.Fatalf("unsupported complete-value property type %s", value.Type())
	}
	return canonical
}
