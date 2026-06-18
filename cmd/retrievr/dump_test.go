package main

import (
	"path/filepath"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

func TestFragmentPath(t *testing.T) {
	nodePath, err := fragmentPath("graph/name", phaseNodes, 7, compressionZstd)
	if err != nil {
		t.Fatalf("node fragment path: %v", err)
	}
	if nodePath != "graphs/graph%2Fname/nodes-000007.ogfrag.zst" {
		t.Fatalf("unexpected node fragment path %q", nodePath)
	}

	edgePath, err := fragmentPath("default", phaseEdges, 3, compressionGzip)
	if err != nil {
		t.Fatalf("edge fragment path: %v", err)
	}
	if edgePath != "graphs/default/edges-000003.ogfrag.gz" {
		t.Fatalf("unexpected edge fragment path %q", edgePath)
	}

	if _, err := fragmentPath("default", phase("bad"), 1, compressionGzip); err == nil {
		t.Fatalf("expected unsupported phase error")
	}
	if _, err := fragmentPath("default", phaseNodes, 0, compressionGzip); err == nil {
		t.Fatalf("expected invalid shard number error")
	}
}

func TestWriteFragmentMetadata(t *testing.T) {
	options := dumpOptions{
		OutputDir:   t.TempDir(),
		Compression: compressionGzip,
		ZstdLevel:   defaultZstdLevel,
	}

	fileEntry, err := writeNodeFragment(options.OutputDir, "default", 1, options, []fragmentNode{{
		ID:         "1",
		Kinds:      []string{"User"},
		Properties: map[string]any{"name": "alice"},
	}}, map[string]int{"pseudonymize": 1})
	if err != nil {
		t.Fatalf("write node fragment: %v", err)
	}
	if fileEntry.Phase != phaseNodes || fileEntry.Path != "graphs/default/nodes-000001.ogfrag.gz" || fileEntry.Count != 1 {
		t.Fatalf("unexpected node file manifest: %+v", fileEntry)
	}
	if fileEntry.ActionCounts["pseudonymize"] != 1 {
		t.Fatalf("missing action count: %+v", fileEntry.ActionCounts)
	}
	if _, err := readManifest(filepath.Join(options.OutputDir, "graphs")); err == nil {
		t.Fatalf("fragment write should not create manifest")
	}

	edgeEntry, err := writeEdgeFragment(options.OutputDir, "default", 2, options, []fragmentEdge{{
		StartID: "1",
		EndID:   "2",
		Kind:    "AdminTo",
	}}, nil)
	if err != nil {
		t.Fatalf("write edge fragment: %v", err)
	}
	if edgeEntry.Phase != phaseEdges || edgeEntry.Path != "graphs/default/edges-000002.ogfrag.gz" || edgeEntry.Count != 1 {
		t.Fatalf("unexpected edge file manifest: %+v", edgeEntry)
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

	total := fileTotal([]fileManifest{
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

func TestEntityIDScanCriteria(t *testing.T) {
	if criteria := entityIDScanCriteria(query.NodeID(), 0, false, 0, false); criteria != nil {
		t.Fatalf("expected no criteria without bounds")
	}

	cases := []struct {
		name       string
		afterID    graph.ID
		hasAfterID bool
		maxID      graph.ID
		hasMaxID   bool
	}{
		{
			name:       "after",
			afterID:    1,
			hasAfterID: true,
		},
		{
			name:     "max",
			maxID:    10,
			hasMaxID: true,
		},
		{
			name:       "range",
			afterID:    1,
			hasAfterID: true,
			maxID:      10,
			hasMaxID:   true,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if criteria := entityIDScanCriteria(query.NodeID(), testCase.afterID, testCase.hasAfterID, testCase.maxID, testCase.hasMaxID); criteria == nil {
				t.Fatalf("expected criteria")
			}
		})
	}
}
