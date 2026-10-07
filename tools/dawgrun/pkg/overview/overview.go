// Package overview builds a compact, browser-rendered view of a graph.
package overview

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"strings"

	"github.com/specterops/dawgs/graph"
)

const unlabeledCluster = "Unlabeled"

// Accumulator groups graph elements into a top-level overview.
type Accumulator struct {
	nodeClusters map[graph.ID]string
	clusters     map[string]int64
	edges        map[edgeKey]map[string]int64
	nodeCount    int64
	edgeCount    int64
}

type edgeKey struct {
	source string
	target string
}

// Summary is the complete data payload consumed by the generated HTML page.
type Summary struct {
	Nodes      []Cluster    `json:"nodes"`
	Edges      []Connection `json:"edges"`
	NodeCount  int64        `json:"node_count"`
	EdgeCount  int64        `json:"edge_count"`
	MaxCluster int64        `json:"max_cluster"`
	MaxEdge    int64        `json:"max_edge"`
}

// Cluster is a group of nodes sharing the same complete set of kinds.
type Cluster struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	Count int64  `json:"count"`
	Size  int    `json:"size"`
}

// Connection is an aggregated directed relationship between two clusters.
type Connection struct {
	ID     string      `json:"id"`
	Source string      `json:"source"`
	Target string      `json:"target"`
	Count  int64       `json:"count"`
	Width  int         `json:"width"`
	Kinds  []KindCount `json:"kinds"`
}

// KindCount is the number of relationships with one relationship kind.
type KindCount struct {
	Kind  string `json:"kind"`
	Count int64  `json:"count"`
}

// NewAccumulator creates an empty graph overview accumulator.
func NewAccumulator() *Accumulator {
	return &Accumulator{
		nodeClusters: make(map[graph.ID]string),
		clusters:     make(map[string]int64),
		edges:        make(map[edgeKey]map[string]int64),
	}
}

// AddNode adds a node to the cluster corresponding to all of its kinds.
func (s *Accumulator) AddNode(id graph.ID, kinds graph.Kinds) {
	cluster := clusterLabel(kinds)
	s.nodeClusters[id] = cluster
	s.clusters[cluster]++
	s.nodeCount++
}

// AddRelationship records a directed relationship between previously added nodes.
func (s *Accumulator) AddRelationship(startID, endID graph.ID, kind graph.Kind) error {
	source, found := s.nodeClusters[startID]
	if !found {
		return fmt.Errorf("relationship start node %d was not included in the overview", startID)
	}

	target, found := s.nodeClusters[endID]
	if !found {
		return fmt.Errorf("relationship end node %d was not included in the overview", endID)
	}

	key := edgeKey{source: source, target: target}
	if s.edges[key] == nil {
		s.edges[key] = make(map[string]int64)
	}

	s.edges[key][kind.String()]++
	s.edgeCount++
	return nil
}

// Summary returns a deterministic data model for the overview page.
func (s *Accumulator) Summary() Summary {
	clusterNames := slices.Sorted(maps.Keys(s.clusters))
	clusterIDs := make(map[string]string, len(clusterNames))
	summary := Summary{
		Nodes:     make([]Cluster, 0, len(clusterNames)),
		NodeCount: s.nodeCount,
		EdgeCount: s.edgeCount,
	}

	for index, name := range clusterNames {
		count := s.clusters[name]
		clusterID := fmt.Sprintf("cluster-%d", index+1)
		clusterIDs[name] = clusterID
		summary.Nodes = append(summary.Nodes, Cluster{
			ID:    clusterID,
			Label: name,
			Count: count,
		})
		summary.MaxCluster = max(summary.MaxCluster, count)
	}

	edgeKeys := make([]edgeKey, 0, len(s.edges))
	for key := range s.edges {
		edgeKeys = append(edgeKeys, key)
	}
	slices.SortFunc(edgeKeys, func(left, right edgeKey) int {
		if left.source != right.source {
			return strings.Compare(left.source, right.source)
		}
		return strings.Compare(left.target, right.target)
	})

	for index, key := range edgeKeys {
		kinds := s.edges[key]
		kindNames := slices.Sorted(maps.Keys(kinds))
		connection := Connection{
			ID:     fmt.Sprintf("edge-%d", index+1),
			Source: clusterIDs[key.source],
			Target: clusterIDs[key.target],
			Kinds:  make([]KindCount, 0, len(kindNames)),
		}
		for _, kind := range kindNames {
			count := kinds[kind]
			connection.Kinds = append(connection.Kinds, KindCount{Kind: kind, Count: count})
			connection.Count += count
		}
		summary.MaxEdge = max(summary.MaxEdge, connection.Count)
		summary.Edges = append(summary.Edges, connection)
	}

	for index := range summary.Nodes {
		summary.Nodes[index].Size = scaledSize(summary.Nodes[index].Count, summary.MaxCluster, 38, 88)
	}
	for index := range summary.Edges {
		summary.Edges[index].Width = scaledSize(summary.Edges[index].Count, summary.MaxEdge, 2, 12)
	}

	return summary
}

// WriteHTML writes an interactive overview page. Cytoscape.js is loaded from a
// pinned CDN URL when the page is opened.
func WriteHTML(w io.Writer, summary Summary) error {
	payload, err := json.Marshal(summary)
	if err != nil {
		return fmt.Errorf("encode overview data: %w", err)
	}

	page := strings.Replace(htmlTemplate, "{{ overview }}", string(payload), 1)
	if _, err := io.WriteString(w, page); err != nil {
		return fmt.Errorf("write overview HTML: %w", err)
	}

	return nil
}

func clusterLabel(kinds graph.Kinds) string {
	labels := sortedStrings(kinds.Strings())
	labels = slices.Compact(labels)
	if len(labels) == 0 {
		return unlabeledCluster
	}
	return strings.Join(labels, " + ")
}

func scaledSize(count, maximum int64, minimum, maximumSize int) int {
	if maximum <= 1 || count <= 1 {
		return minimum
	}
	progress := math.Log(float64(count)) / math.Log(float64(maximum))
	return minimum + int(math.Round(progress*float64(maximumSize-minimum)))
}

func sortedStrings(values []string) []string {
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	return sorted
}

const htmlTemplate = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>DAWGS Graph Overview</title>
  <script src="https://unpkg.com/cytoscape@3.33.1/dist/cytoscape.min.js"></script>
  <style>
    :root { color-scheme: dark; font-family: ui-sans-serif, system-ui, sans-serif; background: #10151f; color: #ecf1f8; }
    body { margin: 0; min-height: 100vh; display: grid; grid-template-columns: minmax(0, 1fr) 300px; }
    #cy { min-height: 100vh; background: radial-gradient(circle at top, #1c2a3d, #10151f 58%); }
    aside { border-left: 1px solid #304055; background: #141c29; padding: 24px; }
    h1 { font-size: 1.2rem; margin: 0 0 8px; } h2 { font-size: .85rem; letter-spacing: .08em; text-transform: uppercase; color: #9fb1c9; margin: 28px 0 8px; }
    p { color: #c3cfde; line-height: 1.5; } #stats { font-variant-numeric: tabular-nums; } #details { white-space: pre-wrap; overflow-wrap: anywhere; }
    .hint { font-size: .85rem; color: #8fa3bd; } .error { color: #ff9c9c; }
    @media (max-width: 760px) { body { grid-template-columns: 1fr; grid-template-rows: minmax(65vh, 1fr) auto; } #cy { min-height: 65vh; } aside { border-left: 0; border-top: 1px solid #304055; } }
  </style>
</head>
<body>
  <main id="cy"></main>
  <aside>
    <h1>Graph Overview</h1>
    <p id="stats"></p>
    <p class="hint">Each node represents all graph nodes with the same complete set of labels. Select a cluster or connection for details.</p>
    <h2>Selection</h2>
    <p id="details">Nothing selected.</p>
  </aside>
  <script>
    const overview = {{ overview }};
    const stats = document.getElementById('stats');
    const details = document.getElementById('details');
    stats.textContent = overview.node_count.toLocaleString() + ' nodes in ' + overview.nodes.length.toLocaleString() + ' clusters; ' + overview.edge_count.toLocaleString() + ' relationships across ' + overview.edges.length.toLocaleString() + ' connections.';
    if (typeof cytoscape !== 'function') {
      details.className = 'error';
      details.textContent = 'Cytoscape.js could not be loaded. Connect to the internet and reopen this file.';
    } else {
      const elements = [
        ...overview.nodes.map(node => ({ data: node })),
        ...overview.edges.map(edge => ({ data: edge }))
      ];
      const cy = cytoscape({
        container: document.getElementById('cy'), elements,
        style: [
          { selector: 'node', style: { 'background-color': '#53a8ff', 'border-width': 2, 'border-color': '#b5dbff', 'width': 'data(size)', 'height': 'data(size)', 'label': 'data(label)', 'color': '#f5f8fc', 'font-size': 12, 'text-wrap': 'wrap', 'text-max-width': 120, 'text-valign': 'center', 'text-halign': 'center' } },
          { selector: 'edge', style: { 'width': 'data(width)', 'line-color': '#7890ad', 'target-arrow-color': '#7890ad', 'target-arrow-shape': 'triangle', 'curve-style': 'bezier', 'opacity': .8 } },
          { selector: ':selected', style: { 'background-color': '#f1b75b', 'line-color': '#f1b75b', 'target-arrow-color': '#f1b75b', 'border-color': '#ffe0a6', 'opacity': 1 } }
        ],
        layout: { name: 'cose', animate: false, padding: 60, nodeRepulsion: 900000, idealEdgeLength: 180, gravity: .3 }
      });
      cy.on('tap', 'node', event => {
        const node = event.target.data();
        details.textContent = 'Cluster: ' + node.label + '\nNodes: ' + node.count.toLocaleString();
      });
      cy.on('tap', 'edge', event => {
        const edge = event.target.data();
        const source = cy.getElementById(edge.source).data('label');
        const target = cy.getElementById(edge.target).data('label');
        const kinds = edge.kinds.map(kind => kind.kind + ': ' + kind.count.toLocaleString()).join('\n');
        details.textContent = source + ' -> ' + target + '\nRelationships: ' + edge.count.toLocaleString() + '\n\nBy kind:\n' + kinds;
      });
      cy.on('tap', event => { if (event.target === cy) { details.textContent = 'Nothing selected.'; } });
    }
  </script>
</body>
</html>
`
