package retriever

import (
	"path/filepath"
	"testing"
)

func TestManifestValidateRejectsUnsupportedFormat(t *testing.T) {
	for _, format := range []string{
		"future",
		"retriever-opengraph-collection-v1",
		"retrievr-opengraph-collection-v1",
	} {
		t.Run(format, func(t *testing.T) {
			value := newValidTestManifest(0)
			value.Format = format

			if err := value.validate(); err == nil {
				t.Fatalf("expected unsupported format error")
			}
		})
	}
}

func TestPublicManifestHelpers(t *testing.T) {
	value := newValidTestManifest(0)
	if err := value.Validate(); err != nil {
		t.Fatalf("validate manifest through public method: %v", err)
	}

	if !IsSupportedManifestFormat(manifestFormat) {
		t.Fatalf("expected public format helper to accept current format")
	}

	if IsSupportedManifestFormat("retriever-opengraph-collection-v1") || IsSupportedManifestFormat("retrievr-opengraph-collection-v1") || IsSupportedManifestFormat("future") {
		t.Fatalf("expected unsupported format to be rejected")
	}

	graphValue := GraphSchemaFromMetadata(GraphSchemaMetadata{
		Name:      "default",
		NodeKinds: []string{"User"},
		EdgeKinds: []string{"MemberOf"},
	})
	if graphValue.Name != "default" || len(graphValue.Nodes) != 1 || graphValue.Nodes[0].String() != "User" {
		t.Fatalf("unexpected graph schema conversion: %+v", graphValue)
	}
}

func TestManifestValidateRequiresNodeFilesBeforeEdgeFiles(t *testing.T) {
	value := newValidTestManifest(1)
	value.Graphs = []GraphManifest{{
		Name:      "default",
		NodeCount: 1,
		EdgeCount: 1,
		Files: []FileManifest{
			{
				Phase:  PhaseEdges,
				Path:   "graphs/default/edges-000001.jsonl.gz",
				Count:  1,
				SHA256: "abc",
			},
			{
				Phase:  PhaseNodes,
				Path:   "graphs/default/nodes-000001.jsonl.gz",
				Count:  1,
				SHA256: "def",
			},
		},
	}}

	if err := value.validate(); err == nil {
		t.Fatalf("expected Phase ordering error")
	}
}

func TestManifestValidateRejectsMalformedGraphEntries(t *testing.T) {
	validFile := FileManifest{
		Phase:           PhaseNodes,
		Path:            "graphs/default/nodes-000001.jsonl.gz",
		Count:           1,
		CompressedBytes: 10,
		SHA256:          "abc",
	}

	cases := map[string]func(Manifest) Manifest{
		"graph count mismatch": func(value Manifest) Manifest {
			value.Source.GraphCount = 2
			return value
		},
		"duplicate graph": func(value Manifest) Manifest {
			value.Source.GraphCount = 2
			value.Graphs = append(value.Graphs, value.Graphs[0])
			return value
		},
		"empty graph name": func(value Manifest) Manifest {
			value.Graphs[0].Name = ""
			return value
		},
		"unsupported Phase": func(value Manifest) Manifest {
			value.Graphs[0].Files[0].Phase = Phase("bad")
			return value
		},
		"missing path": func(value Manifest) Manifest {
			value.Graphs[0].Files[0].Path = ""
			return value
		},
		"missing checksum": func(value Manifest) Manifest {
			value.Graphs[0].Files[0].SHA256 = ""
			return value
		},
		"negative count": func(value Manifest) Manifest {
			value.Graphs[0].Files[0].Count = -1
			return value
		},
		"negative bytes": func(value Manifest) Manifest {
			value.Graphs[0].Files[0].CompressedBytes = -1
			return value
		},
		"node count mismatch": func(value Manifest) Manifest {
			value.Graphs[0].NodeCount = 2
			return value
		},
		"edge count mismatch": func(value Manifest) Manifest {
			value.Graphs[0].EdgeCount = 1
			return value
		},
		"unsupported codec": func(value Manifest) Manifest {
			value.Compression = CompressionCodec("zip")
			return value
		},
		"unsupported scrub mode": func(value Manifest) Manifest {
			value.Scrub.Mode = ScrubMode("partial")
			return value
		},
		"unsupported id strategy": func(value Manifest) Manifest {
			value.IDStrategy = "other"
			return value
		},
	}

	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			value := newValidTestManifest(1)
			value.Graphs = []GraphManifest{{
				Name:      "default",
				NodeCount: 1,
				Files:     []FileManifest{validFile},
			}}

			if err := mutate(value).validate(); err == nil {
				t.Fatalf("expected validation error")
			}
		})
	}
}

func TestWriteReadManifest(t *testing.T) {
	dir := t.TempDir()
	value := newValidTestManifest(1)
	value.Schema.Graphs = []GraphSchemaMetadata{{
		Name:      "default",
		NodeKinds: []string{"User"},
		EdgeKinds: []string{"AdminTo"},
	}}
	value.Graphs = []GraphManifest{{
		Name:      "default",
		NodeCount: 0,
		EdgeCount: 0,
	}}

	if err := writeManifest(dir, value); err != nil {
		t.Fatalf("write Manifest: %v", err)
	}

	if _, err := readManifest(dir); err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	if _, err := readManifest(filepath.Join(dir, "missing")); err == nil {
		t.Fatalf("expected missing Manifest error")
	}
}

func TestManifestValidateAcceptsMetrics(t *testing.T) {
	value := newValidTestManifest(1)
	metrics := buildFingerprintFixture(t, []FragmentNode{{
		ID:    "a",
		Kinds: []string{"User"},
	}}, nil)
	value.Graphs = []GraphManifest{{
		Name:      "default",
		NodeCount: 1,
		EdgeCount: 0,
		Files: []FileManifest{{
			Phase:           PhaseNodes,
			Path:            "graphs/default/nodes-000001.jsonl.gz",
			Count:           1,
			CompressedBytes: 1,
			SHA256:          "abc",
		}},
	}}
	value.Metrics = &MetricsManifest{
		Version: metricsVersion,
		Graphs:  []GraphMetrics{metrics},
	}

	if err := value.validate(); err != nil {
		t.Fatalf("validate Manifest metrics: %v", err)
	}

	value.Metrics.Graphs[0].Fingerprint = "sha256:bad"

	if err := value.validate(); err == nil {
		t.Fatalf("expected metrics fingerprint validation error")
	}
}

func newValidTestManifest(graphCount int) Manifest {
	return newManifest("pg", CompressionGzip, DefaultZstdLevel, ScrubMetadata{
		Mode:             ScrubNone,
		NodeActionCounts: map[string]int{},
		EdgeActionCounts: map[string]int{},
	}, graphCount)
}
