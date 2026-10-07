package synthetic

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
)

const (
	ReferenceNodeCount = int64(15106)
	ReferenceEdgeCount = int64(876117)
	DefaultSeed        = int64(8047)
	DefaultBatchSize   = 10000
)

var referenceEdgesPerNode = float64(ReferenceEdgeCount) / float64(ReferenceNodeCount)

// Options controls the shape and size of a synthetic Active Directory graph.
// Nodes and Edges override the reference dataset proportion when they are set.
type Options struct {
	Nodes        int64
	Edges        int64
	Scale        float64
	Seed         int64
	BatchSize    int
	IndexStress  bool
	IngestStress bool
}

// Stats describes the graph written by Load.
type Stats struct {
	Nodes          int64
	Edges          int64
	DuplicateEdges int64
	NodeKinds      map[string]int64
	EdgeKinds      map[string]int64
}

// Build returns an in-memory OpenGraph document for small fixtures and tests.
// Load should be used for large graphs so the complete document is not held in
// memory.
func Build(options Options) (opengraph.Graph, Stats, error) {
	options, err := options.Normalize()
	if err != nil {
		return opengraph.Graph{}, Stats{}, err
	}

	generator := newGenerator(options)
	stats := newStats(options)
	document := opengraph.Graph{
		Nodes: make([]opengraph.Node, 0, int(options.Nodes)),
		Edges: make([]opengraph.Edge, 0, int(options.Edges)),
	}

	for index := int64(0); index < options.Nodes; index++ {
		kind := generator.kindFor(index)
		document.Nodes = append(document.Nodes, opengraph.Node{
			ID:         nodeID(index),
			Kinds:      []string{kind.String()},
			Properties: generator.nodeProperties(index, kind),
		})
	}

	for index := int64(0); index < options.Edges; index++ {
		nextEdge := generator.edge(index)
		document.Edges = append(document.Edges, opengraph.Edge{
			StartID:    nodeID(nextEdge.start),
			EndID:      nodeID(nextEdge.end),
			Kind:       nextEdge.kind,
			Properties: nextEdge.properties,
		})
		stats.EdgeKinds[nextEdge.kind]++
		if nextEdge.duplicate {
			stats.DuplicateEdges++
		}
	}
	stats.Edges = options.Edges

	return document, stats, opengraph.Validate(opengraph.Document{Graph: document})
}

// DefaultOptions returns a reference-sized, deterministic AD graph profile.
func DefaultOptions() Options {
	return Options{
		Scale:     1,
		Seed:      DefaultSeed,
		BatchSize: DefaultBatchSize,
	}
}

// Normalize applies reference scaling and validates options before a database
// connection is opened.
func (s Options) Normalize() (Options, error) {
	if s.Scale == 0 {
		s.Scale = 1
	}
	if s.Scale < 0 || math.IsNaN(s.Scale) || math.IsInf(s.Scale, 0) {
		return Options{}, fmt.Errorf("scale must be a finite value greater than zero")
	}
	if s.Nodes < 0 {
		return Options{}, fmt.Errorf("nodes must not be negative")
	}
	if s.Edges < 0 {
		return Options{}, fmt.Errorf("edges must not be negative")
	}
	if s.BatchSize < 0 {
		return Options{}, fmt.Errorf("batch size must not be negative")
	}

	if s.Nodes == 0 {
		s.Nodes = scaledCount(ReferenceNodeCount, s.Scale)
	}
	if s.Edges == 0 {
		if s.Nodes != scaledCount(ReferenceNodeCount, s.Scale) {
			s.Edges = scaledCountFloat(s.Nodes, referenceEdgesPerNode)
		} else {
			s.Edges = scaledCount(ReferenceEdgeCount, s.Scale)
		}
	}
	if s.BatchSize == 0 {
		s.BatchSize = DefaultBatchSize
	}
	maxInt := int64(^uint(0) >> 1)
	if s.Nodes > maxInt || s.Edges > maxInt {
		return Options{}, fmt.Errorf("nodes and edges must fit in the platform int size")
	}
	if s.Nodes < 1 {
		return Options{}, fmt.Errorf("nodes must be greater than zero")
	}

	return s, nil
}

// Load generates and writes a synthetic AD graph to db. The generated node
// IDs are retained only long enough to connect the subsequent edge batches.
func Load(ctx context.Context, db graph.Database, options Options) (Stats, error) {
	options, err := options.Normalize()
	if err != nil {
		return Stats{}, err
	}

	generator := newGenerator(options)
	stats := newStats(options)
	createdIDs := make([]graph.ID, int(options.Nodes))

	if err := db.BatchOperation(ctx, func(batch graph.Batch) error {
		creator, ok := batch.(graph.NodeBatchCreator)
		if !ok {
			return fmt.Errorf("database does not support bulk node creation")
		}

		for start := int64(0); start < options.Nodes; start += int64(options.BatchSize) {
			if err := ctx.Err(); err != nil {
				return err
			}

			end := minInt64(start+int64(options.BatchSize), options.Nodes)
			nodes := make([]*graph.Node, end-start)
			for index := start; index < end; index++ {
				nodes[index-start] = generator.node(index)
			}

			ids, err := creator.CreateNodes(nodes)
			if err != nil {
				return err
			}
			copy(createdIDs[start:end], ids)
		}

		return nil
	}, graph.WithBatchSize(options.BatchSize)); err != nil {
		return Stats{}, fmt.Errorf("create synthetic nodes: %w", err)
	}

	if err := db.BatchOperation(ctx, func(batch graph.Batch) error {
		for index := int64(0); index < options.Edges; index++ {
			if index%int64(options.BatchSize) == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}

			nextEdge := generator.edge(index)
			if err := batch.CreateRelationshipByIDs(
				createdIDs[nextEdge.start],
				createdIDs[nextEdge.end],
				graph.StringKind(nextEdge.kind),
				graph.AsProperties(nextEdge.properties),
			); err != nil {
				return err
			}

			stats.Edges++
			stats.EdgeKinds[nextEdge.kind]++
			if nextEdge.duplicate {
				stats.DuplicateEdges++
			}
		}

		return nil
	}, graph.WithBatchSize(options.BatchSize)); err != nil {
		return Stats{}, fmt.Errorf("create synthetic relationships: %w", err)
	}

	return stats, nil
}

type nodeKind int

const (
	domainKind nodeKind = iota
	ouKind
	gpoKind
	groupKind
	computerKind
	userKind
)

func (s nodeKind) String() string {
	switch s {
	case domainKind:
		return "Domain"
	case ouKind:
		return "OU"
	case gpoKind:
		return "GPO"
	case groupKind:
		return "Group"
	case computerKind:
		return "Computer"
	case userKind:
		return "User"
	default:
		return ""
	}
}

type nodeRange struct {
	kind  nodeKind
	start int64
	count int64
}

func (s nodeRange) end() int64 {
	return s.start + s.count
}

func (s nodeRange) pick(value int64) (int64, bool) {
	if s.count == 0 {
		return 0, false
	}

	return s.start + positiveMod(value, s.count), true
}

type generator struct {
	options Options
	ranges  map[nodeKind]nodeRange
}

func newGenerator(options Options) generator {
	return generator{
		options: options,
		ranges:  allocateRanges(options.Nodes),
	}
}

func allocateRanges(nodes int64) map[nodeKind]nodeRange {
	counts := map[nodeKind]int64{
		domainKind:   maxInt64(1, nodes/100000),
		ouKind:       maxInt64(1, nodes/1000),
		gpoKind:      maxInt64(1, nodes/5000),
		groupKind:    maxInt64(1, nodes*15/100),
		computerKind: maxInt64(1, nodes*30/100),
	}

	remaining := nodes
	for _, kind := range []nodeKind{domainKind, ouKind, gpoKind, groupKind, computerKind} {
		if counts[kind] >= remaining {
			counts[kind] = maxInt64(0, remaining-1)
		}
		remaining -= counts[kind]
	}
	counts[userKind] = remaining

	ranges := make(map[nodeKind]nodeRange, len(counts))
	var start int64
	for _, kind := range []nodeKind{domainKind, ouKind, gpoKind, groupKind, computerKind, userKind} {
		ranges[kind] = nodeRange{kind: kind, start: start, count: counts[kind]}
		start += counts[kind]
	}

	return ranges
}

func (s generator) node(index int64) *graph.Node {
	kind := s.kindFor(index)
	return &graph.Node{
		Kinds:      graph.Kinds{graph.StringKind(kind.String())},
		Properties: graph.AsProperties(s.nodeProperties(index, kind)),
	}
}

func nodeID(index int64) string {
	return fmt.Sprintf("synthetic-node-%012d", index)
}

func (s generator) kindFor(index int64) nodeKind {
	for _, kind := range []nodeKind{domainKind, ouKind, gpoKind, groupKind, computerKind, userKind} {
		nextRange := s.ranges[kind]
		if index >= nextRange.start && index < nextRange.end() {
			return kind
		}
	}

	return userKind
}

func (s generator) nodeProperties(index int64, kind nodeKind) map[string]any {
	name := fmt.Sprintf("%s-%08d", strings.ToLower(kind.String()), index)
	if s.options.IndexStress {
		name = fmt.Sprintf("%s-%02d", strings.ToLower(kind.String()), index%8)
	}

	domain := fmt.Sprintf("corp-%04d.example.com", index%maxInt64(1, s.ranges[domainKind].count))
	objectID := fmt.Sprintf("S-1-5-21-%d-%d", positiveMod(s.options.Seed, 1000000000), index+1000)

	properties := map[string]any{
		"name":              name,
		"objectid":          objectID,
		"distinguishedname": fmt.Sprintf("CN=%s,DC=example,DC=com", name),
		"domain":            domain,
	}

	switch kind {
	case domainKind:
		properties["name"] = domain
		properties["distinguishedname"] = "DC=example,DC=com"
	case ouKind:
		properties["name"] = fmt.Sprintf("OU-%04d", index)
	case gpoKind:
		properties["displayname"] = fmt.Sprintf("Default Domain Policy %04d", index)
	case groupKind:
		properties["samaccountname"] = name
		properties["highvalue"] = index%100 == 0
	case computerKind:
		properties["samaccountname"] = name + "$"
		properties["operatingsystem"] = []string{"WINDOWS 10", "WINDOWS 11", "WINDOWS SERVER 2022"}[index%3]
	case userKind:
		properties["samaccountname"] = name
		properties["enabled"] = index%17 != 0
		properties["hasspn"] = index%23 == 0
		properties["email"] = name + "@example.com"
	}

	if s.options.IngestStress && index%10 == 0 {
		properties["description"] = strings.Repeat("synthetic-ingest-payload ", 16)
	}

	return properties
}

type generatedEdge struct {
	start      int64
	end        int64
	kind       string
	properties map[string]any
	duplicate  bool
}

func (s generator) edge(index int64) generatedEdge {
	if s.options.IngestStress {
		stressStart := s.options.Edges * 4 / 5
		if index >= stressStart {
			duplicate := s.productionEdge(index - stressStart)
			duplicate.duplicate = true
			return duplicate
		}
	}

	return s.productionEdge(index)
}

func (s generator) productionEdge(index int64) generatedEdge {
	if index < s.options.Nodes {
		return s.containmentEdge(index)
	}

	groupRange := s.ranges[groupKind]
	if groupRange.count > 0 && index%3 != 0 {
		if source, ok := s.pickSource(index); ok {
			if target, targetOK := groupRange.pick(index * 17); targetOK {
				if source == target && s.options.Nodes > 1 {
					target = (target + 1) % s.options.Nodes
				}
				return generatedEdge{start: source, end: target, kind: "MemberOf"}
			}
		}
	}

	selector := mix(uint64(s.options.Seed), uint64(index)) % 6
	var (
		source   int64
		target   int64
		sourceOK bool
		targetOK bool
		edgeKind string
	)
	switch selector {
	case 0:
		source, sourceOK = s.pickSource(index * 31)
		target, targetOK = s.pickTarget(index * 47)
		edgeKind = "GenericAll"
	case 1:
		source, sourceOK = s.pickSource(index * 31)
		target, targetOK = s.pickTarget(index * 47)
		edgeKind = "GenericWrite"
	case 2:
		source, sourceOK = s.pickKinds(index*31, userKind, groupKind)
		target, targetOK = s.pickTarget(index * 47)
		edgeKind = "Owns"
	case 3:
		source, sourceOK = s.pickKinds(index*31, userKind, groupKind)
		target, targetOK = s.ranges[computerKind].pick(index * 47)
		edgeKind = "AdminTo"
	case 4:
		source, sourceOK = s.pickKinds(index*31, userKind, groupKind)
		target, targetOK = s.ranges[computerKind].pick(index * 47)
		edgeKind = "CanRDP"
	default:
		source, sourceOK = s.ranges[computerKind].pick(index * 31)
		target, targetOK = s.ranges[userKind].pick(index * 47)
		edgeKind = "HasSession"
	}

	if !sourceOK || !targetOK {
		return generatedEdge{start: 0, end: minInt64(1, s.options.Nodes-1), kind: "MemberOf"}
	}
	if source == target && s.options.Nodes > 1 {
		target = (target + 1) % s.options.Nodes
	}

	return generatedEdge{
		start: source,
		end:   target,
		kind:  edgeKind,
	}
}

func (s generator) containmentEdge(index int64) generatedEdge {
	kind := s.kindFor(index)
	if kind == domainKind {
		if target, ok := s.ranges[ouKind].pick(index); ok {
			return generatedEdge{start: index, end: target, kind: "Contains"}
		}
	}

	if kind == ouKind {
		if parent, ok := s.ranges[domainKind].pick(index * 13); ok && parent != index {
			return generatedEdge{start: parent, end: index, kind: "Contains"}
		}
	}

	if parent, ok := s.ranges[ouKind].pick(index * 13); ok && kind != ouKind {
		return generatedEdge{start: parent, end: index, kind: "Contains"}
	}
	if parent, ok := s.ranges[domainKind].pick(index * 13); ok && index != parent {
		return generatedEdge{start: parent, end: index, kind: "Contains"}
	}

	parent := (index + 1) % s.options.Nodes
	return generatedEdge{start: parent, end: index, kind: "Contains"}
}

func (s generator) pickSource(value int64) (int64, bool) {
	for _, kind := range []nodeKind{userKind, groupKind, computerKind} {
		if next, ok := s.ranges[kind].pick(value); ok {
			return next, true
		}
	}

	return 0, false
}

func (s generator) pickTarget(value int64) (int64, bool) {
	return s.pickKinds(value, groupKind, computerKind, userKind, domainKind)
}

func (s generator) pickKinds(value int64, kinds ...nodeKind) (int64, bool) {
	for _, kind := range kinds {
		if next, ok := s.ranges[kind].pick(value); ok {
			return next, true
		}
	}

	return 0, false
}

func newStats(options Options) Stats {
	stats := Stats{
		Nodes:     options.Nodes,
		NodeKinds: make(map[string]int64),
		EdgeKinds: make(map[string]int64),
	}
	generator := newGenerator(options)

	for _, kind := range []nodeKind{domainKind, ouKind, gpoKind, groupKind, computerKind, userKind} {
		stats.NodeKinds[kind.String()] = generator.ranges[kind].count
	}

	return stats
}

func scaledCount(value int64, scale float64) int64 {
	return scaledCountFloat(value, scale)
}

func scaledCountFloat(value int64, scale float64) int64 {
	count := int64(math.Round(float64(value) * scale))
	if count < 1 {
		return 1
	}

	return count
}

func minInt64(left, right int64) int64 {
	if left < right {
		return left
	}

	return right
}

func maxInt64(left, right int64) int64 {
	if left > right {
		return left
	}

	return right
}

func positiveMod(value, modulus int64) int64 {
	if modulus <= 0 {
		return 0
	}

	result := value % modulus
	if result < 0 {
		return result + modulus
	}

	return result
}

func mix(seed, value uint64) uint64 {
	value += uint64(seed) + 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}
