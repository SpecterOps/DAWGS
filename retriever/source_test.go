package retriever

import (
	"context"
	"reflect"
	"testing"

	"github.com/specterops/dawgs/graph"
)

func TestDatabaseGraphSourceCreatesFreshCappedFaucets(t *testing.T) {
	database := newScriptedDumpDatabase(map[string]*scriptedDumpGraph{
		"source": {
			nodeCount: 2,
			nodes: []*graph.Node{
				graph.NewNode(3, nil),
				graph.NewNode(1, nil),
				graph.NewNode(2, nil),
			},
			ignoreNodeLimit: true,
		},
	})
	source := newDatabaseGraphSource(database)
	target := graph.Graph{Name: "source"}

	snapshot, err := source.Inventory(context.Background(), target)
	if err != nil {
		t.Fatalf("inventory: %v", err)
	}
	if snapshot.NodeCount != 2 || snapshot.EdgeCount != 0 {
		t.Fatalf("inventory = %+v", snapshot)
	}

	for run := range 2 {
		var ids []graph.ID
		processed, err := source.Nodes(target, snapshot.NodeCount, 10).Run(context.Background(), func(nodes []*graph.Node) error {
			for _, node := range nodes {
				ids = append(ids, node.ID)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("run %d: %v", run+1, err)
		}
		if processed != 2 || !reflect.DeepEqual(ids, []graph.ID{1, 2}) {
			t.Fatalf("run %d processed=%d ids=%v", run+1, processed, ids)
		}
	}
}

func TestDumpWithScriptedGraphSource(t *testing.T) {
	source := &scriptedGraphSource{
		snapshot: graphEntitySnapshot{NodeCount: 2, EdgeCount: 1},
		nodeBatches: [][]*graph.Node{{
			graph.NewNode(1, graph.AsProperties(map[string]any{"name": "alice"}), graph.StringKind("User")),
			graph.NewNode(2, nil, graph.StringKind("Group")),
		}},
		edgeBatches: [][]*graph.Relationship{{
			graph.NewRelationship(10, 1, 2, nil, graph.StringKind("MemberOf")),
		}},
	}

	result, err := runDump(context.Background(), source, "scripted-source", []GraphTarget{{Name: "source"}}, DumpOptions{
		OutputDir:   t.TempDir(),
		Scrub:       ScrubFull,
		Salt:        "source-test",
		Compression: CompressionGzip,
		ZstdLevel:   DefaultZstdLevel,
		ShardSize:   10,
		BatchSize:   10,
	}, dumpOverrides{})
	if err != nil {
		t.Fatalf("dump: %v", err)
	}

	if result.NodeCount != 2 || result.EdgeCount != 1 {
		t.Fatalf("dump counts: nodes=%d edges=%d", result.NodeCount, result.EdgeCount)
	}
	if source.inventoryCalls != 1 || source.nodeFaucets != 2 || source.edgeFaucets != 1 {
		t.Fatalf("source calls: inventory=%d node_faucets=%d edge_faucets=%d", source.inventoryCalls, source.nodeFaucets, source.edgeFaucets)
	}
	if len(result.Manifest.Graphs) != 1 || len(result.Manifest.Graphs[0].Files) != 2 {
		t.Fatalf("manifest graph = %+v", result.Manifest.Graphs)
	}
}

type scriptedGraphSource struct {
	snapshot       graphEntitySnapshot
	nodeBatches    [][]*graph.Node
	edgeBatches    [][]*graph.Relationship
	inventoryCalls int
	nodeFaucets    int
	edgeFaucets    int
}

func (s *scriptedGraphSource) Inventory(context.Context, graph.Graph) (graphEntitySnapshot, error) {
	s.inventoryCalls++
	return s.snapshot, nil
}

func (s *scriptedGraphSource) Nodes(graph.Graph, int64, int) faucet[*graph.Node] {
	s.nodeFaucets++
	return scriptedFaucet[*graph.Node]{batches: s.nodeBatches, total: s.snapshot.NodeCount}
}

func (s *scriptedGraphSource) Edges(graph.Graph, int64, int) faucet[*graph.Relationship] {
	s.edgeFaucets++
	return scriptedFaucet[*graph.Relationship]{batches: s.edgeBatches, total: s.snapshot.EdgeCount}
}

type scriptedFaucet[T any] struct {
	batches [][]T
	total   int64
}

func (s scriptedFaucet[T]) Run(ctx context.Context, handle entityBatchHandler[T]) (int64, error) {
	var processed int64
	for _, batch := range s.batches {
		if err := ctx.Err(); err != nil {
			return processed, err
		}

		remaining := s.total - processed
		if remaining <= 0 {
			break
		}
		if int64(len(batch)) > remaining {
			batch = batch[:int(remaining)]
		}

		if err := handle(batch); err != nil {
			return processed, err
		}
		processed += int64(len(batch))
	}

	return processed, nil
}
