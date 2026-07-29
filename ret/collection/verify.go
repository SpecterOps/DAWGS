package collection

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/specterops/dawgs/ret/entity"
	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/metrics"
	"github.com/specterops/dawgs/ret/observe"
	"github.com/specterops/dawgs/ret/parquet"
)

type Verification struct {
	Manifest Manifest
	Graphs   []GraphVerification
}

type GraphVerification struct {
	Name              string
	NodeCount         int64
	RelationshipCount int64
}

// Verify validates every configured concrete artifact in a collection.
func Verify(ctx context.Context, root string, observer observe.Observer) (Verification, error) {
	manifest, err := readManifestWithoutSymlinks(root)
	if err != nil {
		return Verification{}, err
	}
	if err := preflightArtifactPaths(root, manifest, false); err != nil {
		return Verification{}, err
	}

	result := Verification{Manifest: manifest, Graphs: make([]GraphVerification, 0, len(manifest.Graphs))}
	for _, graph := range manifest.Graphs {
		if err := ctx.Err(); err != nil {
			return Verification{}, fmt.Errorf("verify collection: %w", err)
		}
		graphResult, err := verifyGraph(ctx, root, graph, observer)
		if err != nil {
			return Verification{}, err
		}
		result.Graphs = append(result.Graphs, graphResult)
	}
	return result, nil
}

// VerifyJSONLForLoad validates only JSONL artifacts and requires one for every
// logical shard. Parquet paths and bytes are deliberately not inspected.
func VerifyJSONLForLoad(ctx context.Context, root string, observer observe.Observer) (Verification, error) {
	manifest, err := readManifestWithoutSymlinks(root)
	if err != nil {
		return Verification{}, err
	}
	if err := preflightArtifactPaths(root, manifest, true); err != nil {
		return Verification{}, err
	}

	result := Verification{Manifest: manifest, Graphs: make([]GraphVerification, 0, len(manifest.Graphs))}
	for _, graph := range manifest.Graphs {
		if err := ctx.Err(); err != nil {
			return Verification{}, fmt.Errorf("verify JSONL collection for load: %w", err)
		}
		graphResult, err := verifyJSONLGraph(ctx, root, graph, observer)
		if err != nil {
			return Verification{}, err
		}
		result.Graphs = append(result.Graphs, graphResult)
	}
	return result, nil
}

// ReplayGraph visits JSONL nodes in shard order, followed by JSONL
// relationships in shard order. Callers first run VerifyJSONLForLoad.
func ReplayGraph(
	ctx context.Context,
	root string,
	graph Graph,
	visitNode func(entity.Node) error,
	visitRelationship func(entity.Relationship) error,
) error {
	for _, shard := range graph.NodeShards {
		if shard.JSONL == nil {
			return fmt.Errorf("replay graph %q node shard %d requires JSONL output", graph.Name, shard.Index)
		}
		if err := inspectNonSymlinkArtifact(root, shard.JSONL.Path); err != nil {
			return fmt.Errorf("replay graph %q JSONL node shard %d path: %w", graph.Name, shard.Index, err)
		}
	}
	for _, shard := range graph.RelationshipShards {
		if shard.JSONL == nil {
			return fmt.Errorf("replay graph %q relationship shard %d requires JSONL output", graph.Name, shard.Index)
		}
		if err := inspectNonSymlinkArtifact(root, shard.JSONL.Path); err != nil {
			return fmt.Errorf("replay graph %q JSONL relationship shard %d path: %w", graph.Name, shard.Index, err)
		}
	}

	for _, shard := range graph.NodeShards {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("replay graph %q nodes: %w", graph.Name, err)
		}
		err := jsonl.ReadNodes(root, *shard.JSONL, func(node entity.Node) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if visitNode != nil {
				return visitNode(node)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("replay graph %q JSONL node shard %d: %w", graph.Name, shard.Index, err)
		}
	}
	for _, shard := range graph.RelationshipShards {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("replay graph %q relationships: %w", graph.Name, err)
		}
		err := jsonl.ReadRelationships(root, *shard.JSONL, func(relationship entity.Relationship) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if visitRelationship != nil {
				return visitRelationship(relationship)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("replay graph %q JSONL relationship shard %d: %w", graph.Name, shard.Index, err)
		}
	}
	return nil
}

func verifyGraph(ctx context.Context, root string, graph Graph, observer observe.Observer) (GraphVerification, error) {
	builder := metrics.NewBuilder()
	catalog := newKindCatalog()
	parquetRelationshipIDs := make(map[string]struct{})

	for _, shard := range graph.NodeShards {
		var jsonNodes []entity.Node
		if shard.JSONL != nil {
			err := jsonl.ReadNodes(root, *shard.JSONL, func(node entity.Node) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := builder.ObserveNode(node); err != nil {
					return err
				}
				catalog.observeNode(node)
				jsonNodes = append(jsonNodes, node)
				return nil
			})
			if err != nil {
				return GraphVerification{}, fmt.Errorf("verify graph %q JSONL node shard %d: %w", graph.Name, shard.Index, err)
			}
			emitJSONLNodeArtifactVerified(ctx, observer, graph.Name, *shard.JSONL)
		}

		if shard.Parquet != nil {
			parquetRow := 0
			err := parquet.ReadNodes(root, *shard.Parquet, func(node entity.Node) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				parquetRow++
				if shard.JSONL != nil {
					if parquetRow > len(jsonNodes) {
						return fmt.Errorf("dual node row %d has no JSONL row", parquetRow)
					}
					if err := compareNodes(jsonNodes[parquetRow-1], node); err != nil {
						return fmt.Errorf("dual node row %d differs: %w", parquetRow, err)
					}
					return nil
				}
				if err := builder.ObserveNode(node); err != nil {
					return err
				}
				catalog.observeNode(node)
				return nil
			})
			if err != nil {
				return GraphVerification{}, fmt.Errorf("verify graph %q Parquet node shard %d: %w", graph.Name, shard.Index, err)
			}
			if shard.JSONL != nil && parquetRow != len(jsonNodes) {
				return GraphVerification{}, fmt.Errorf(
					"verify graph %q Parquet node shard %d: dual row count differs: JSONL %d, Parquet %d",
					graph.Name, shard.Index, len(jsonNodes), parquetRow,
				)
			}
			emitParquetNodeArtifactVerified(ctx, observer, graph.Name, *shard.Parquet)
		}
	}

	for _, shard := range graph.RelationshipShards {
		var jsonRelationships []entity.Relationship
		if shard.JSONL != nil {
			err := jsonl.ReadRelationships(root, *shard.JSONL, func(relationship entity.Relationship) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := builder.ObserveRelationship(relationship); err != nil {
					return err
				}
				catalog.observeRelationship(relationship)
				jsonRelationships = append(jsonRelationships, relationship)
				return nil
			})
			if err != nil {
				return GraphVerification{}, fmt.Errorf("verify graph %q JSONL relationship shard %d: %w", graph.Name, shard.Index, err)
			}
			emitJSONLRelationshipArtifactVerified(ctx, observer, graph.Name, *shard.JSONL)
		}

		if shard.Parquet != nil {
			parquetRow := 0
			err := parquet.ReadRelationships(root, *shard.Parquet, func(relationship entity.Relationship) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				parquetRow++
				if relationship.SourceID == "" {
					return fmt.Errorf("Parquet relationship row %d source ID is empty", parquetRow)
				}
				if _, found := parquetRelationshipIDs[relationship.SourceID]; found {
					return fmt.Errorf("Parquet relationship source ID %q is duplicate within graph", relationship.SourceID)
				}
				parquetRelationshipIDs[relationship.SourceID] = struct{}{}

				if shard.JSONL != nil {
					if parquetRow > len(jsonRelationships) {
						return fmt.Errorf("dual relationship row %d has no JSONL row", parquetRow)
					}
					if err := compareRelationships(jsonRelationships[parquetRow-1], relationship); err != nil {
						return fmt.Errorf("dual relationship row %d differs: %w", parquetRow, err)
					}
					return nil
				}
				if err := builder.ObserveRelationship(relationship); err != nil {
					return err
				}
				catalog.observeRelationship(relationship)
				return nil
			})
			if err != nil {
				return GraphVerification{}, fmt.Errorf("verify graph %q Parquet relationship shard %d: %w", graph.Name, shard.Index, err)
			}
			if shard.JSONL != nil && parquetRow != len(jsonRelationships) {
				return GraphVerification{}, fmt.Errorf(
					"verify graph %q Parquet relationship shard %d: dual row count differs: JSONL %d, Parquet %d",
					graph.Name, shard.Index, len(jsonRelationships), parquetRow,
				)
			}
			emitParquetRelationshipArtifactVerified(ctx, observer, graph.Name, *shard.Parquet)
		}
	}

	return finalizeGraphVerification(graph, builder, catalog.values)
}

func verifyJSONLGraph(ctx context.Context, root string, graph Graph, observer observe.Observer) (GraphVerification, error) {
	builder := metrics.NewBuilder()
	catalog := newKindCatalog()

	for _, shard := range graph.NodeShards {
		err := jsonl.ReadNodes(root, *shard.JSONL, func(node entity.Node) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := builder.ObserveNode(node); err != nil {
				return err
			}
			catalog.observeNode(node)
			return nil
		})
		if err != nil {
			return GraphVerification{}, fmt.Errorf("verify graph %q JSONL node shard %d for load: %w", graph.Name, shard.Index, err)
		}
		emitJSONLNodeArtifactVerified(ctx, observer, graph.Name, *shard.JSONL)
	}

	for _, shard := range graph.RelationshipShards {
		err := jsonl.ReadRelationships(root, *shard.JSONL, func(relationship entity.Relationship) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := builder.ObserveRelationship(relationship); err != nil {
				return err
			}
			catalog.observeRelationship(relationship)
			return nil
		})
		if err != nil {
			return GraphVerification{}, fmt.Errorf("verify graph %q JSONL relationship shard %d for load: %w", graph.Name, shard.Index, err)
		}
		emitJSONLRelationshipArtifactVerified(ctx, observer, graph.Name, *shard.JSONL)
	}

	return finalizeGraphVerification(graph, builder, catalog.values)
}

func finalizeGraphVerification(graph Graph, builder *metrics.Builder, catalog []string) (GraphVerification, error) {
	if !slices.Equal(graph.KindCatalog, catalog) {
		return GraphVerification{}, fmt.Errorf(
			"verify graph %q kind catalog differs: expected %v, actual %v",
			graph.Name, graph.KindCatalog, catalog,
		)
	}
	actualMetrics := builder.Finalize()
	if err := metrics.Compare(graph.Metrics, actualMetrics); err != nil {
		return GraphVerification{}, fmt.Errorf("verify graph %q metrics: %w", graph.Name, err)
	}
	if graph.NodeCount != actualMetrics.NodeCount {
		return GraphVerification{}, fmt.Errorf(
			"verify graph %q node count differs: expected %d, actual %d",
			graph.Name, graph.NodeCount, actualMetrics.NodeCount,
		)
	}
	if graph.RelationshipCount != actualMetrics.RelationshipCount {
		return GraphVerification{}, fmt.Errorf(
			"verify graph %q relationship count differs: expected %d, actual %d",
			graph.Name, graph.RelationshipCount, actualMetrics.RelationshipCount,
		)
	}
	return GraphVerification{
		Name:              graph.Name,
		NodeCount:         actualMetrics.NodeCount,
		RelationshipCount: actualMetrics.RelationshipCount,
	}, nil
}

type kindCatalog struct {
	seen   map[string]struct{}
	values []string
}

func newKindCatalog() *kindCatalog {
	return &kindCatalog{seen: make(map[string]struct{})}
}

func (s *kindCatalog) observeNode(node entity.Node) {
	for _, kind := range node.Kinds {
		s.add(kind)
	}
}

func (s *kindCatalog) observeRelationship(relationship entity.Relationship) {
	s.add(relationship.Kind)
}

func (s *kindCatalog) add(kind string) {
	if _, found := s.seen[kind]; found {
		return
	}
	s.seen[kind] = struct{}{}
	s.values = append(s.values, kind)
}

func compareNodes(jsonNode, parquetNode entity.Node) error {
	if jsonNode.SourceID != parquetNode.SourceID {
		return fmt.Errorf("source ID: JSONL %q, Parquet %q", jsonNode.SourceID, parquetNode.SourceID)
	}
	if !slices.Equal(jsonNode.Kinds, parquetNode.Kinds) {
		return fmt.Errorf("kinds: JSONL %v, Parquet %v", jsonNode.Kinds, parquetNode.Kinds)
	}
	if err := compareProperties(jsonNode.Properties, parquetNode.Properties); err != nil {
		return fmt.Errorf("properties: %w", err)
	}
	return nil
}

func compareRelationships(jsonRelationship, parquetRelationship entity.Relationship) error {
	if jsonRelationship.StartID != parquetRelationship.StartID {
		return fmt.Errorf("start ID: JSONL %q, Parquet %q", jsonRelationship.StartID, parquetRelationship.StartID)
	}
	if jsonRelationship.EndID != parquetRelationship.EndID {
		return fmt.Errorf("end ID: JSONL %q, Parquet %q", jsonRelationship.EndID, parquetRelationship.EndID)
	}
	if jsonRelationship.Kind != parquetRelationship.Kind {
		return fmt.Errorf("kind: JSONL %q, Parquet %q", jsonRelationship.Kind, parquetRelationship.Kind)
	}
	if err := compareProperties(jsonRelationship.Properties, parquetRelationship.Properties); err != nil {
		return fmt.Errorf("properties: %w", err)
	}
	return nil
}

func compareProperties(jsonProperties, parquetProperties map[string]any) error {
	jsonCanonical, err := canonicalJSONValue(jsonProperties)
	if err != nil {
		return fmt.Errorf("JSONL value is not JSON-compatible: %w", err)
	}
	parquetCanonical, err := canonicalJSONValue(parquetProperties)
	if err != nil {
		return fmt.Errorf("Parquet value is not JSON-compatible: %w", err)
	}
	if !bytes.Equal(jsonCanonical, parquetCanonical) {
		return fmt.Errorf("JSONL and Parquet values differ")
	}
	return nil
}

func canonicalJSONValue(value any) ([]byte, error) {
	var output bytes.Buffer
	if err := appendCanonicalJSONValue(&output, reflect.ValueOf(value), "$"); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func appendCanonicalJSONValue(output *bytes.Buffer, value reflect.Value, path string) error {
	if !value.IsValid() {
		output.WriteByte('z')
		return nil
	}
	for value.Kind() == reflect.Interface {
		if value.IsNil() {
			output.WriteByte('z')
			return nil
		}
		value = value.Elem()
	}

	if value.CanInterface() {
		if number, ok := value.Interface().(json.Number); ok {
			canonical, err := canonicalDecimal(number.String())
			if err != nil {
				return fmt.Errorf("%s has invalid JSON number %q: %w", path, number, err)
			}
			writeLengthPrefixed(output, 'n', canonical)
			return nil
		}
	}

	switch value.Kind() {
	case reflect.Bool:
		if value.Bool() {
			output.WriteString("b1")
		} else {
			output.WriteString("b0")
		}
		return nil
	case reflect.String:
		writeLengthPrefixed(output, 's', value.String())
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		canonical, err := canonicalDecimal(strconv.FormatInt(value.Int(), 10))
		if err != nil {
			return fmt.Errorf("%s has invalid integer: %w", path, err)
		}
		writeLengthPrefixed(output, 'n', canonical)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		canonical, err := canonicalDecimal(strconv.FormatUint(value.Uint(), 10))
		if err != nil {
			return fmt.Errorf("%s has invalid unsigned integer: %w", path, err)
		}
		writeLengthPrefixed(output, 'n', canonical)
		return nil
	case reflect.Float32, reflect.Float64:
		number := value.Float()
		if math.IsNaN(number) || math.IsInf(number, 0) {
			return fmt.Errorf("%s has non-finite number", path)
		}
		canonical, err := canonicalDecimal(strconv.FormatFloat(number, 'g', -1, value.Type().Bits()))
		if err != nil {
			return fmt.Errorf("%s has invalid floating-point number: %w", path, err)
		}
		writeLengthPrefixed(output, 'n', canonical)
		return nil
	case reflect.Map:
		if value.IsNil() {
			output.WriteByte('z')
			return nil
		}
		if value.Type().Key().Kind() != reflect.String {
			return fmt.Errorf("%s has object key type %s, want string", path, value.Type().Key())
		}
		keys := value.MapKeys()
		sort.Slice(keys, func(left, right int) bool {
			return keys[left].String() < keys[right].String()
		})
		output.WriteByte('{')
		for _, key := range keys {
			writeLengthPrefixed(output, 'k', key.String())
			if err := appendCanonicalJSONValue(output, value.MapIndex(key), path+"."+key.String()); err != nil {
				return err
			}
		}
		output.WriteByte('}')
		return nil
	case reflect.Slice:
		if value.IsNil() {
			output.WriteByte('z')
			return nil
		}
		if value.Type().Elem().Kind() == reflect.Uint8 {
			return fmt.Errorf("%s has binary value, which JSON cannot represent", path)
		}
		fallthrough
	case reflect.Array:
		output.WriteByte('[')
		for index := 0; index < value.Len(); index++ {
			if err := appendCanonicalJSONValue(output, value.Index(index), fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
		}
		output.WriteByte(']')
		return nil
	default:
		return fmt.Errorf("%s has unsupported type %s", path, value.Type())
	}
}

func canonicalDecimal(value string) (string, error) {
	if value == "" {
		return "", fmt.Errorf("empty number")
	}
	sign := ""
	if value[0] == '-' || value[0] == '+' {
		if value[0] == '-' {
			sign = "-"
		}
		value = value[1:]
		if value == "" {
			return "", fmt.Errorf("missing digits")
		}
	}

	exponent := new(big.Int)
	if separator := strings.IndexAny(value, "eE"); separator >= 0 {
		if strings.IndexAny(value[separator+1:], "eE") >= 0 {
			return "", fmt.Errorf("multiple exponents")
		}
		exponentText := value[separator+1:]
		if exponentText == "" {
			return "", fmt.Errorf("missing exponent")
		}
		if _, ok := exponent.SetString(exponentText, 10); !ok {
			return "", fmt.Errorf("invalid exponent")
		}
		value = value[:separator]
	}

	whole, fraction, found := strings.Cut(value, ".")
	if found && strings.Contains(fraction, ".") {
		return "", fmt.Errorf("multiple decimal points")
	}
	if whole == "" && fraction == "" {
		return "", fmt.Errorf("missing digits")
	}
	if whole == "" {
		whole = "0"
	}
	if !decimalDigits(whole) || !decimalDigits(fraction) {
		return "", fmt.Errorf("invalid decimal digits")
	}

	digits := strings.TrimLeft(whole+fraction, "0")
	if digits == "" {
		return "0e0", nil
	}
	exponent.Sub(exponent, big.NewInt(int64(len(fraction))))
	trailing := len(digits) - len(strings.TrimRight(digits, "0"))
	if trailing != 0 {
		digits = digits[:len(digits)-trailing]
		exponent.Add(exponent, big.NewInt(int64(trailing)))
	}
	return sign + digits + "e" + exponent.String(), nil
}

func decimalDigits(value string) bool {
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	return true
}

func writeLengthPrefixed(output *bytes.Buffer, kind byte, value string) {
	output.WriteByte(kind)
	output.WriteString(strconv.Itoa(len(value)))
	output.WriteByte(':')
	output.WriteString(value)
}

func readManifestWithoutSymlinks(root string) (Manifest, error) {
	if err := inspectNonSymlinkArtifact(root, ManifestName); err != nil {
		return Manifest{}, fmt.Errorf("verify collection manifest path: %w", err)
	}
	manifest, err := Read(root)
	if err != nil {
		return Manifest{}, fmt.Errorf("verify collection manifest: %w", err)
	}
	return manifest, nil
}

func preflightArtifactPaths(root string, manifest Manifest, jsonlOnly bool) error {
	for _, graph := range manifest.Graphs {
		for _, shard := range graph.NodeShards {
			if jsonlOnly && shard.JSONL == nil {
				return fmt.Errorf("graph %q node shard %d requires JSONL output", graph.Name, shard.Index)
			}
			if shard.JSONL != nil {
				if err := inspectNonSymlinkArtifact(root, shard.JSONL.Path); err != nil {
					return fmt.Errorf("verify graph %q JSONL node shard %d path: %w", graph.Name, shard.Index, err)
				}
			}
			if !jsonlOnly && shard.Parquet != nil {
				if err := inspectNonSymlinkArtifact(root, shard.Parquet.Path); err != nil {
					return fmt.Errorf("verify graph %q Parquet node shard %d path: %w", graph.Name, shard.Index, err)
				}
			}
		}
		for _, shard := range graph.RelationshipShards {
			if jsonlOnly && shard.JSONL == nil {
				return fmt.Errorf("graph %q relationship shard %d requires JSONL output", graph.Name, shard.Index)
			}
			if shard.JSONL != nil {
				if err := inspectNonSymlinkArtifact(root, shard.JSONL.Path); err != nil {
					return fmt.Errorf("verify graph %q JSONL relationship shard %d path: %w", graph.Name, shard.Index, err)
				}
			}
			if !jsonlOnly && shard.Parquet != nil {
				if err := inspectNonSymlinkArtifact(root, shard.Parquet.Path); err != nil {
					return fmt.Errorf("verify graph %q Parquet relationship shard %d path: %w", graph.Name, shard.Index, err)
				}
			}
		}
	}
	return nil
}

// inspectNonSymlinkArtifact is a portable local-filesystem best effort. It
// checks every collection-relative path component before a reader opens the
// file, but pathname replacement cannot be made atomic on every supported OS.
func inspectNonSymlinkArtifact(root, relative string) error {
	inspectionRoot := root
	if inspectionRoot == "" {
		inspectionRoot = "."
	}
	rootInfo, err := os.Lstat(inspectionRoot)
	if err != nil {
		return fmt.Errorf("inspect collection root: %w", err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return fmt.Errorf("collection root is not a non-symlink directory: %q", root)
	}
	if _, err := SafeJoin(inspectionRoot, relative); err != nil {
		return err
	}

	components := strings.Split(filepath.FromSlash(relative), string(filepath.Separator))
	current := inspectionRoot
	for index, component := range components {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			return fmt.Errorf("inspect artifact path component %q: %w", component, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("artifact path contains symlink component %q", component)
		}
		if index < len(components)-1 {
			if !info.IsDir() {
				return fmt.Errorf("artifact path component %q is not a directory", component)
			}
			continue
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("artifact path is not a regular file: %q", relative)
		}
	}
	return nil
}

func emitJSONLNodeArtifactVerified(ctx context.Context, observer observe.Observer, graph string, artifact jsonl.NodeArtifact) {
	observe.Emit(ctx, observer, observe.ArtifactVerified{
		Graph: graph, EntityType: "node", Format: "JSONL", Path: artifact.Path,
		Count: artifact.Count, Bytes: artifact.StoredBytes,
	})
}

func emitJSONLRelationshipArtifactVerified(
	ctx context.Context,
	observer observe.Observer,
	graph string,
	artifact jsonl.RelationshipArtifact,
) {
	observe.Emit(ctx, observer, observe.ArtifactVerified{
		Graph: graph, EntityType: "relationship", Format: "JSONL", Path: artifact.Path,
		Count: artifact.Count, Bytes: artifact.StoredBytes,
	})
}

func emitParquetNodeArtifactVerified(ctx context.Context, observer observe.Observer, graph string, artifact parquet.NodeArtifact) {
	observe.Emit(ctx, observer, observe.ArtifactVerified{
		Graph: graph, EntityType: "node", Format: "Parquet", Path: artifact.Path,
		Count: artifact.Count, Bytes: artifact.StoredBytes,
	})
}

func emitParquetRelationshipArtifactVerified(
	ctx context.Context,
	observer observe.Observer,
	graph string,
	artifact parquet.RelationshipArtifact,
) {
	observe.Emit(ctx, observer, observe.ArtifactVerified{
		Graph: graph, EntityType: "relationship", Format: "Parquet", Path: artifact.Path,
		Count: artifact.Count, Bytes: artifact.StoredBytes,
	})
}
