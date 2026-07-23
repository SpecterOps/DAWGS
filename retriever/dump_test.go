package retriever

import "testing"

func TestJSONLFragmentPath(t *testing.T) {
	nodePath, err := jsonlFragmentPath("graph/name", PhaseNodes, 7, CompressionZstd)
	if err != nil {
		t.Fatalf("node fragment path: %v", err)
	}
	if nodePath != "graphs/graph%2Fname/nodes-000007.jsonl.zst" {
		t.Fatalf("unexpected node fragment path %q", nodePath)
	}

	edgePath, err := jsonlFragmentPath("default", PhaseEdges, 3, CompressionGzip)
	if err != nil {
		t.Fatalf("edge fragment path: %v", err)
	}
	if edgePath != "graphs/default/edges-000003.jsonl.gz" {
		t.Fatalf("unexpected edge fragment path %q", edgePath)
	}

	if _, err := jsonlFragmentPath("default", Phase("bad"), 1, CompressionGzip); err == nil {
		t.Fatalf("expected unsupported Phase error")
	}
	if _, err := jsonlFragmentPath("default", PhaseNodes, 0, CompressionGzip); err == nil {
		t.Fatalf("expected invalid shard number error")
	}
}

func TestKindAndActionHelpers(t *testing.T) {
	kinds := map[string]struct{}{}
	addKindsToSet(kinds, []string{"User", "", "Computer", "User"})
	if got := stringsFromKindSet(kinds); len(got) != 2 || got[0] != "Computer" || got[1] != "User" {
		t.Fatalf("unexpected kinds: %v", got)
	}

	target := map[string]int{"preserve": 1}
	addActionCounts(target, map[string]int{"preserve": 2, "redact": 3})
	if target["preserve"] != 3 || target["redact"] != 3 {
		t.Fatalf("unexpected action counts: %+v", target)
	}

	clone := cloneActionCounts(target)
	clone["preserve"] = 100
	if target["preserve"] == 100 {
		t.Fatalf("expected clone to be independent")
	}

	total := fileTotal([]FileManifest{
		{
			Count: 2,
		},
		{
			Count: 3,
		},
	})
	if total != 5 {
		t.Fatalf("file total = %d", total)
	}
}
