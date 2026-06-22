package main

import (
	"path/filepath"
	"testing"
)

func TestManifestValidateRejectsUnsupportedFormat(t *testing.T) {
	value := newValidTestManifest(0)
	value.Format = "future"

	if err := value.validate(); err == nil {
		t.Fatalf("expected unsupported format error")
	}
}

func TestManifestValidateAcceptsLegacyFormat(t *testing.T) {
	value := newValidTestManifest(0)
	value.Format = legacyManifestFormat

	if err := value.validate(); err != nil {
		t.Fatalf("validate legacy manifest format: %v", err)
	}
}

func TestManifestValidateRequiresNodeFilesBeforeEdgeFiles(t *testing.T) {
	value := newValidTestManifest(1)
	value.Graphs = []graphManifest{{
		Name:      "default",
		NodeCount: 1,
		EdgeCount: 1,
		Files: []fileManifest{
			{
				Phase:  phaseEdges,
				Path:   "graphs/default/edges-000001.ogfrag.gz",
				Count:  1,
				SHA256: "abc",
			},
			{
				Phase:  phaseNodes,
				Path:   "graphs/default/nodes-000001.ogfrag.gz",
				Count:  1,
				SHA256: "def",
			},
		},
	}}

	if err := value.validate(); err == nil {
		t.Fatalf("expected phase ordering error")
	}
}

func TestManifestValidateRejectsMalformedGraphEntries(t *testing.T) {
	validFile := fileManifest{
		Phase:           phaseNodes,
		Path:            "graphs/default/nodes-000001.ogfrag.gz",
		Count:           1,
		CompressedBytes: 10,
		SHA256:          "abc",
	}

	cases := map[string]func(manifest) manifest{
		"graph count mismatch": func(value manifest) manifest {
			value.Source.GraphCount = 2
			return value
		},
		"duplicate graph": func(value manifest) manifest {
			value.Source.GraphCount = 2
			value.Graphs = append(value.Graphs, value.Graphs[0])
			return value
		},
		"empty graph name": func(value manifest) manifest {
			value.Graphs[0].Name = ""
			return value
		},
		"unsupported phase": func(value manifest) manifest {
			value.Graphs[0].Files[0].Phase = phase("bad")
			return value
		},
		"missing path": func(value manifest) manifest {
			value.Graphs[0].Files[0].Path = ""
			return value
		},
		"missing checksum": func(value manifest) manifest {
			value.Graphs[0].Files[0].SHA256 = ""
			return value
		},
		"negative count": func(value manifest) manifest {
			value.Graphs[0].Files[0].Count = -1
			return value
		},
		"negative bytes": func(value manifest) manifest {
			value.Graphs[0].Files[0].CompressedBytes = -1
			return value
		},
		"node count mismatch": func(value manifest) manifest {
			value.Graphs[0].NodeCount = 2
			return value
		},
		"edge count mismatch": func(value manifest) manifest {
			value.Graphs[0].EdgeCount = 1
			return value
		},
		"unsupported codec": func(value manifest) manifest {
			value.Compression = compressionCodec("zip")
			return value
		},
		"unsupported id strategy": func(value manifest) manifest {
			value.IDStrategy = "other"
			return value
		},
	}

	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			value := newValidTestManifest(1)
			value.Graphs = []graphManifest{{
				Name:      "default",
				NodeCount: 1,
				Files:     []fileManifest{validFile},
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
	value.Schema.Graphs = []graphSchemaMetadata{{
		Name:      "default",
		NodeKinds: []string{"User"},
		EdgeKinds: []string{"AdminTo"},
	}}
	value.Graphs = []graphManifest{{
		Name:      "default",
		NodeCount: 0,
		EdgeCount: 0,
	}}

	if err := writeManifest(dir, value); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if _, err := readManifest(dir); err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if _, err := readManifest(filepath.Join(dir, "missing")); err == nil {
		t.Fatalf("expected missing manifest error")
	}
}

func TestManifestValidateAcceptsMetrics(t *testing.T) {
	value := newValidTestManifest(1)
	metrics := buildFingerprintFixture(t, []fragmentNode{{
		ID:    "a",
		Kinds: []string{"User"},
	}}, nil)
	value.Graphs = []graphManifest{{
		Name:      "default",
		NodeCount: 1,
		EdgeCount: 0,
		Files: []fileManifest{{
			Phase:           phaseNodes,
			Path:            "graphs/default/nodes-000001.ogfrag.gz",
			Count:           1,
			CompressedBytes: 1,
			SHA256:          "abc",
		}},
	}}
	value.Metrics = &metricsManifest{
		Version: metricsVersion,
		Graphs:  []graphMetrics{metrics},
	}

	if err := value.validate(); err != nil {
		t.Fatalf("validate manifest metrics: %v", err)
	}

	value.Metrics.Graphs[0].Fingerprint = "sha256:bad"
	if err := value.validate(); err == nil {
		t.Fatalf("expected metrics fingerprint validation error")
	}
}

func newValidTestManifest(graphCount int) manifest {
	return newManifest("pg", compressionGzip, defaultZstdLevel, scrubMetadata{
		Mode:             scrubNone,
		NodeActionCounts: map[string]int{},
		EdgeActionCounts: map[string]int{},
	}, graphCount)
}
