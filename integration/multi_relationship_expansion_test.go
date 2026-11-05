//go:build manual_integration

package integration

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/stretchr/testify/require"
)

var multiExpansionDomains = [][2]string{
	{"Okta_MemberOf", "Okta_HasRoleAssignment"},
	{"MemberOf", "AdminTo"},
}

// The layered fixture converges many paths on the same endpoints. Comparing
// complete node/edge identities preserves multiplicity, unlike DISTINCT nodes.
func TestMultiRelationshipExpansionEquivalence(t *testing.T) {
	session := openMultiExpansionSession(t)
	for _, kinds := range multiExpansionDomains {
		for _, inbound := range []bool{false, true} {
			fixture := multiExpansionFixture(kinds, 2, 0)
			if inbound {
				for index := range fixture.Edges {
					edge := &fixture.Edges[index]
					edge.StartID, edge.EndID = edge.EndID, edge.StartID
				}
			}
			for _, depth := range []int{2, 3} {
				t.Run(fmt.Sprintf("%s/inbound=%t/depth=%d", kinds[0], inbound, depth), func(t *testing.T) {
					require.NoError(t, session.WithRollbackFixture(t, fixture, true, func(tx graph.Transaction, ids opengraph.IDMap) error {
						params := map[string]any{"root": ids["root"].Int64()}
						query := multiExpansionQuery([]string{fmt.Sprintf("%s|%s*1..%d", kinds[0], kinds[1], depth)}, inbound, "p")
						combined := multiExpansionPaths(t, tx.Query(query, params))
						var components []string
						for _, sequence := range multiExpansionSequences(kinds, depth) {
							components = append(components, multiExpansionPaths(t, tx.Query(multiExpansionQuery(sequence, inbound, "p"), params))...)
						}
						sort.Strings(components)
						require.Equal(t, components, combined)
						// Width two gives four outgoing choices at every hop.
						require.Len(t, combined, multiExpansionCount(4, depth))
						var homogeneous []string
						for _, kind := range kinds {
							homogeneous = append(homogeneous, multiExpansionPaths(t, tx.Query(multiExpansionQuery([]string{fmt.Sprintf("%s*1..%d", kind, depth)}, inbound, "p"), params))...)
						}
						require.Len(t, homogeneous, 2*multiExpansionCount(2, depth))
						require.Greater(t, len(combined), len(homogeneous), "single-type queries omit mixed paths")
						return nil
					}))
				})
			}
		}
	}
}

// Timing is diagnostic: exact cardinalities enforce the traversal contract,
// while balanced warm samples compare equivalent work without a flaky deadline.
func TestMultiRelationshipExpansionFanout(t *testing.T) {
	session := openMultiExpansionSession(t)
	for _, kinds := range multiExpansionDomains {
		for _, width := range []int{2, 8} {
			fixture := multiExpansionFixture(kinds, width, 256)
			for _, depth := range []int{2, 3} {
				for _, projection := range []string{"count(*)", "p"} {
					t.Run(fmt.Sprintf("%s/width=%d/depth=%d/%s", kinds[0], width, depth, projection), func(t *testing.T) {
						require.NoError(t, session.WithRollbackFixture(t, fixture, true, func(tx graph.Transaction, ids opengraph.IDMap) error {
							params := map[string]any{"root": ids["root"].Int64()}
							combined := []string{multiExpansionQuery([]string{fmt.Sprintf("%s|%s*1..%d", kinds[0], kinds[1], depth)}, false, projection)}
							var components, homogeneous []string
							for _, sequence := range multiExpansionSequences(kinds, depth) {
								components = append(components, multiExpansionQuery(sequence, false, projection))
							}
							for _, kind := range kinds {
								homogeneous = append(homogeneous, multiExpansionQuery([]string{fmt.Sprintf("%s*1..%d", kind, depth)}, false, projection))
							}
							arms := [][]string{combined, components, homogeneous}
							var samples [3][]time.Duration
							for round := range 6 {
								for offset := range arms {
									arm := (round + offset) % len(arms)
									started := time.Now()
									var count int64
									for _, query := range arms[arm] {
										if projection == "p" {
											count += int64(len(multiExpansionPaths(t, tx.Query(query, params))))
										} else {
											count += multiExpansionScalar(t, tx.Query(query, params))
										}
									}
									elapsed := time.Since(started)
									want := multiExpansionCount(2*width, depth)
									if arm == 2 {
										want = 2 * multiExpansionCount(width, depth)
									}
									require.Equal(t, int64(want), count)
									if round > 0 {
										samples[arm] = append(samples[arm], elapsed)
									}
								}
							}
							for _, sample := range samples {
								sort.Slice(sample, func(i, j int) bool { return sample[i] < sample[j] })
							}
							t.Logf("%s: %d paths; warm median combined=%s, %d exhaustive components=%s; two single-type queries=%s (%d paths)", session.Driver, multiExpansionCount(2*width, depth), samples[0][2], len(components), samples[1][2], samples[2][2], 2*multiExpansionCount(width, depth))
							return nil
						}))
					})
				}
			}
		}
	}
}

func openMultiExpansionSession(t *testing.T) *Session {
	t.Helper()
	kinds := graph.Kinds{graph.StringKind("MultiExpansionOther")}
	for _, domain := range multiExpansionDomains {
		kinds = kinds.Add(graph.StringsToKinds(domain[:])...)
	}
	return Open(t, Options{SkipIfNoConnection: true, CleanupMode: CloseOnly,
		ExtraNodeKinds: graph.Kinds{graph.StringKind("MultiExpansionNode")}, ExtraEdgeKinds: kinds})
}

// Four layers make depth-three over-expansion observable. Wrong-type first and
// later hops, reverse edges, and a disconnected component are deliberate decoys.
func multiExpansionFixture(kinds [2]string, width, disconnected int) *opengraph.Graph {
	fixture := &opengraph.Graph{}
	addNode := func(id string) {
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{ID: id, Kinds: []string{"MultiExpansionNode"}, Properties: map[string]any{"name": id}})
	}
	addEdge := func(start, end, kind string) {
		fixture.Edges = append(fixture.Edges, opengraph.Edge{StartID: start, EndID: end, Kind: kind})
	}
	addNode("root")
	previous := []string{"root"}
	for layer := 1; layer <= 4; layer++ {
		var next []string
		for index := range width {
			id := fmt.Sprintf("layer-%d-%d", layer, index)
			addNode(id)
			next = append(next, id)
			for _, start := range previous {
				for _, kind := range kinds {
					addEdge(start, id, kind)
				}
			}
		}
		previous = next
	}
	for _, id := range []string{"wrong-first", "wrong-next", "reverse"} {
		addNode(id)
	}
	addEdge("root", "wrong-first", "MultiExpansionOther")
	addEdge("wrong-first", "layer-2-0", kinds[0])
	addEdge("layer-1-0", "wrong-next", "MultiExpansionOther")
	addEdge("wrong-next", "layer-3-0", kinds[1])
	addEdge("reverse", "root", kinds[0])
	for index := range disconnected {
		id := fmt.Sprintf("disconnected-%d", index)
		addNode(id)
		if index > 0 {
			for _, kind := range kinds {
				addEdge(fmt.Sprintf("disconnected-%d", index-1), id, kind)
			}
		}
	}
	return fixture
}

func multiExpansionQuery(sequence []string, inbound bool, projection string) string {
	pattern := "(s:MultiExpansionNode)"
	for _, kind := range sequence {
		if inbound {
			pattern += "<-[:" + kind + "]-()"
		} else {
			pattern += "-[:" + kind + "]->()"
		}
	}
	return "MATCH p = " + pattern + " WHERE id(s) = $root RETURN " + projection
}

// Enumerate every type word, rather than treating the two homogeneous
// expansions as an equivalent decomposition of the relationship disjunction.
func multiExpansionSequences(kinds [2]string, depth int) [][]string {
	var sequences [][]string
	frontier := [][]string{{}}
	for range depth {
		var next [][]string
		for _, prefix := range frontier {
			for _, kind := range kinds {
				sequence := append(append([]string{}, prefix...), kind)
				next = append(next, sequence)
				sequences = append(sequences, sequence)
			}
		}
		frontier = next
	}
	return sequences
}

func multiExpansionCount(branches, depth int) int {
	count, paths := 0, 1
	for range depth {
		paths *= branches
		count += paths
	}
	return count
}

func multiExpansionPaths(t *testing.T, result graph.Result) []string {
	t.Helper()
	defer result.Close()
	paths := []string{}
	for result.Next() {
		var path graph.Path
		require.NoError(t, result.Scan(&path))
		var signature strings.Builder
		for index, node := range path.Nodes {
			fmt.Fprintf(&signature, "n%d/", node.ID)
			if index < len(path.Edges) {
				fmt.Fprintf(&signature, "e%d/", path.Edges[index].ID)
			}
		}
		paths = append(paths, signature.String())
	}
	require.NoError(t, result.Error())
	sort.Strings(paths)
	return paths
}

func multiExpansionScalar(t *testing.T, result graph.Result) int64 {
	t.Helper()
	defer result.Close()
	require.True(t, result.Next(), "%v", result.Error())
	var count int64
	require.NoError(t, result.Scan(&count))
	require.False(t, result.Next())
	require.NoError(t, result.Error())
	return count
}
