package metrics

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAnalyzeQualityReportsIntegrationSignals(t *testing.T) {
	sourceRoot := t.TempDir()
	writeTestFile(t, filepath.Join(sourceRoot, "integration", "testdata", "cases", "nodes.json"), `{
  "dataset": "base",
  "cases": [
    {"name": "duplicate", "cypher": "match (n) return n", "assert": "no_error"},
    {"name": "duplicate", "cypher": "match (n) return n", "assert": {"row_count": 1}}
  ]
}`)
	writeTestFile(t, filepath.Join(sourceRoot, "integration", "testdata", "templates", "metamorphic.json"), `{
  "families": [
    {
      "name": "template family",
      "fixture": {"nodes": [], "edges": []},
      "template": "{{query}}",
      "variants": [
        {"name": "variant", "vars": {"query": "match (n) return n"}, "assert": {"row_count": 0}}
      ]
    }
  ],
  "metamorphic": [
    {
      "name": "metamorphic family",
      "fixture": {"nodes": [], "edges": []},
      "compare": "node_ids",
      "queries": [
        {"name": "base", "cypher": "match (n) return n"},
        {"name": "equivalent", "cypher": "match (m) return m"}
      ]
    }
  ]
}`)
	writeTestFile(t, filepath.Join(sourceRoot, "cypher", "models", "pgsql", "test", "translation_cases", "nodes.sql"), "-- case: sample\nselect 1;\n")

	report := AnalyzeQuality(sourceRoot, QualityOptions{})

	if report.SemanticDrift.CoreCases != 2 {
		t.Fatalf("core cases = %d, want 2", report.SemanticDrift.CoreCases)
	}
	if report.SemanticDrift.TemplateVariants != 1 {
		t.Fatalf("template variants = %d, want 1", report.SemanticDrift.TemplateVariants)
	}
	if report.SemanticDrift.MetamorphicFamilies != 1 || report.SemanticDrift.MetamorphicQueries != 2 {
		t.Fatalf("metamorphic counts = %d/%d, want 1/2", report.SemanticDrift.MetamorphicFamilies, report.SemanticDrift.MetamorphicQueries)
	}
	if report.Invariants.LowOracle != 1 {
		t.Fatalf("low oracle assertions = %d, want 1", report.Invariants.LowOracle)
	}
	assertQualityFinding(t, report.Invariants.Findings, "case_duplicate_name")
	if report.Fuzz.Status != QualityStatusPending {
		t.Fatalf("fuzz status = %s, want pending", report.Fuzz.Status)
	}
}

func TestAnalyzeQualityComparesBackendResults(t *testing.T) {
	sourceRoot := t.TempDir()
	pgResult := filepath.Join(sourceRoot, "pg.json")
	neo4jResult := filepath.Join(sourceRoot, "neo4j.json")

	writeTestFile(t, pgResult, strings.Join([]string{
		`{"Action":"pass","Package":"github.com/specterops/dawgs/integration","Test":"TestCypher/nodes"}`,
		`{"Action":"pass","Package":"github.com/specterops/dawgs/integration","Test":"TestCypher/edges"}`,
	}, "\n"))
	writeTestFile(t, neo4jResult, strings.Join([]string{
		`{"Action":"pass","Package":"github.com/specterops/dawgs/integration","Test":"TestCypher/nodes"}`,
		`{"Action":"skip","Package":"github.com/specterops/dawgs/integration","Test":"TestCypher/edges"}`,
	}, "\n"))

	report := AnalyzeQuality(sourceRoot, QualityOptions{
		BackendResults: []NamedPath{
			{Name: "pg", Path: pgResult},
			{Name: "neo4j", Path: neo4jResult},
		},
	})

	if report.BackendEquivalence.Status != QualityStatusWatch {
		t.Fatalf("backend status = %s, want watch", report.BackendEquivalence.Status)
	}
	if report.BackendEquivalence.Mismatches != 1 {
		t.Fatalf("backend mismatches = %d, want 1", report.BackendEquivalence.Mismatches)
	}
	assertQualityFinding(t, report.BackendEquivalence.Findings, "backend_status_mismatch")
}

func TestAnalyzeQualityComparesBenchmarkDrift(t *testing.T) {
	sourceRoot := t.TempDir()
	current := filepath.Join(sourceRoot, "current.json")
	baseline := filepath.Join(sourceRoot, "baseline.json")

	writeTestFile(t, baseline, `{
  "driver": "pg",
  "results": [
    {"section": "Traversal", "dataset": "base", "label": "depth 1", "stats": {"median": 100, "p95": 200, "max": 300}}
  ]
}`)
	writeTestFile(t, current, `{
  "driver": "pg",
  "results": [
    {"section": "Traversal", "dataset": "base", "label": "depth 1", "stats": {"median": 140, "p95": 190, "max": 300}}
  ]
}`)

	report := AnalyzeQuality(sourceRoot, QualityOptions{
		BenchmarkReportPath:         current,
		BenchmarkBaselinePath:       baseline,
		BenchmarkRegressionFraction: 0.20,
	})

	if report.BenchmarkDrift.Status != QualityStatusWatch {
		t.Fatalf("benchmark status = %s, want watch", report.BenchmarkDrift.Status)
	}
	if len(report.BenchmarkDrift.Regressions) != 1 {
		t.Fatalf("benchmark regressions = %d, want 1", len(report.BenchmarkDrift.Regressions))
	}
	if report.BenchmarkDrift.Regressions[0].Metric != "median" {
		t.Fatalf("regression metric = %s, want median", report.BenchmarkDrift.Regressions[0].Metric)
	}
}

func TestWriteQualityTextIncludesFindings(t *testing.T) {
	report := QualityReport{
		SourceRoot: "/work/dawgs",
		Summary: QualitySummary{
			Status:         QualityStatusWatch,
			FindingCount:   1,
			WatchSignals:   1,
			PendingSignals: 1,
		},
		Invariants: InvariantReport{
			Status: QualityStatusWatch,
			Findings: []QualityFinding{{
				Signal:   "invariants",
				Severity: "high",
				ID:       "case_duplicate_name",
				Message:  "duplicate case name",
				File:     "integration/testdata/cases/nodes.json",
			}},
		},
	}

	var output bytes.Buffer
	if err := WriteQualityText(&output, report); err != nil {
		t.Fatalf("write quality text: %v", err)
	}

	text := output.String()
	for _, expected := range []string{"Quality report", "case_duplicate_name", "duplicate case name"} {
		if !strings.Contains(text, expected) {
			t.Fatalf("quality text missing %q:\n%s", expected, text)
		}
	}
}

func assertQualityFinding(t *testing.T, findings []QualityFinding, id string) {
	t.Helper()

	for _, finding := range findings {
		if finding.ID == id {
			return
		}
	}

	t.Fatalf("missing quality finding %q in %#v", id, findings)
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
