package metrics

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAnalyzeCalculatesCRAPFromFunctionCoverage(t *testing.T) {
	sourceRoot := t.TempDir()
	sourcePath := filepath.Join(sourceRoot, "sample.go")
	coverPath := filepath.Join(sourceRoot, "coverage.out")

	source := `package sample

func Branchy(ok bool) int {
	if ok {
		return 1
	}
	return 0
}

func Uncovered() int {
	return 2
}

//gocyclo:ignore
func Ignored(ok bool) int {
	if ok {
		return 3
	}
	return 4
}
`
	coverage := `mode: atomic
sample.go:4.2,4.8 1 1
sample.go:5.3,5.11 1 0
sample.go:7.2,7.10 1 1
sample.go:11.2,11.10 1 0
`

	if err := os.WriteFile(sourcePath, []byte(source), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if err := os.WriteFile(coverPath, []byte(coverage), 0o644); err != nil {
		t.Fatalf("write coverage: %v", err)
	}

	report, err := Analyze(Options{
		SourceRoot:   sourceRoot,
		CoverProfile: coverPath,
	})
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}

	if report.FunctionCount != 2 {
		t.Fatalf("function count = %d, want 2", report.FunctionCount)
	}

	records := recordsByFunction(report)
	branchy := records["Branchy"]
	if branchy.Function == "" {
		t.Fatal("missing Branchy record")
	}
	if branchy.Complexity != 2 {
		t.Fatalf("Branchy complexity = %d, want 2", branchy.Complexity)
	}
	if branchy.Statements != 3 || branchy.CoveredStatements != 2 {
		t.Fatalf("Branchy coverage statements = %d/%d, want 2/3", branchy.CoveredStatements, branchy.Statements)
	}
	if !approximately(branchy.Coverage, 2.0/3.0) {
		t.Fatalf("Branchy coverage = %f, want %f", branchy.Coverage, 2.0/3.0)
	}
	if !approximately(branchy.CRAP, 2.0+4.0*math.Pow(1.0/3.0, 3)) {
		t.Fatalf("Branchy CRAP = %f", branchy.CRAP)
	}

	uncovered := records["Uncovered"]
	if uncovered.Function == "" {
		t.Fatal("missing Uncovered record")
	}
	if uncovered.Coverage != 0 {
		t.Fatalf("Uncovered coverage = %f, want 0", uncovered.Coverage)
	}
	if uncovered.CRAP != 2 {
		t.Fatalf("Uncovered CRAP = %f, want 2", uncovered.CRAP)
	}
	if _, found := records["Ignored"]; found {
		t.Fatal("gocyclo ignored function should not be reported")
	}
}

func TestWriteTextAppliesTopAndCRAPThreshold(t *testing.T) {
	report := Report{
		CoverProfile:      "coverage.out",
		FunctionCount:     3,
		AverageComplexity: 2,
		AverageCRAP:       8,
		Records: []FunctionMetric{
			{Package: "sample", Function: "Highest", File: "sample.go", Line: 1, Column: 1, Complexity: 5, CRAP: 20},
			{Package: "sample", Function: "Middle", File: "sample.go", Line: 5, Column: 1, Complexity: 3, CRAP: 10},
			{Package: "sample", Function: "Lowest", File: "sample.go", Line: 9, Column: 1, Complexity: 1, CRAP: 2},
		},
	}

	var output bytes.Buffer
	if err := WriteText(&output, report, TextOptions{Top: 1, CRAPOver: 5}); err != nil {
		t.Fatalf("write text: %v", err)
	}

	text := output.String()
	if !strings.Contains(text, "Highest") {
		t.Fatalf("text report missing Highest:\n%s", text)
	}
	if strings.Contains(text, "Middle") || strings.Contains(text, "Lowest") {
		t.Fatalf("text report did not apply top/threshold filters:\n%s", text)
	}
}

func TestWriteHTMLProducesStandaloneReport(t *testing.T) {
	report := Report{
		SourceRoot:        "/work/dawgs",
		CoverProfile:      ".coverage/unit.out",
		FunctionCount:     3,
		AverageComplexity: 3,
		AverageCRAP:       12,
		Records: []FunctionMetric{
			{
				Package:           "sample",
				Function:          "Highest",
				File:              "sample.go",
				Line:              1,
				Column:            1,
				Complexity:        5,
				Statements:        10,
				CoveredStatements: 3,
				Coverage:          0.3,
				CRAP:              20,
			},
			{
				Package:           "sample",
				Function:          "Middle",
				File:              "sample.go",
				Line:              5,
				Column:            1,
				Complexity:        3,
				Statements:        8,
				CoveredStatements: 8,
				Coverage:          1,
				CRAP:              3,
			},
			{
				Package:           "other",
				Function:          "Complex",
				File:              "other.go",
				Line:              9,
				Column:            1,
				Complexity:        9,
				Statements:        6,
				CoveredStatements: 0,
				Coverage:          0,
				CRAP:              90,
			},
		},
	}

	var output bytes.Buffer
	if err := WriteHTML(&output, report, HTMLReportOptions{Top: 2, CRAPOver: 10, CycloOver: 4}); err != nil {
		t.Fatalf("write HTML: %v", err)
	}

	html := output.String()
	for _, expected := range []string{
		"<!doctype html>",
		"<style>",
		"<script>",
		"DAWGS Metrics Report",
		"sample.Highest",
		"other.Complex",
		"CRAP = complexity",
	} {
		if !strings.Contains(html, expected) {
			t.Fatalf("HTML report missing %q:\n%s", expected, html)
		}
	}

	for _, unexpected := range []string{"http://", "https://", " src=", " href="} {
		if strings.Contains(html, unexpected) {
			t.Fatalf("HTML report contains external reference marker %q", unexpected)
		}
	}
}

func recordsByFunction(report Report) map[string]FunctionMetric {
	records := make(map[string]FunctionMetric, len(report.Records))
	for _, record := range report.Records {
		records[record.Function] = record
	}

	return records
}

func approximately(actual, expected float64) bool {
	return math.Abs(actual-expected) < 0.000001
}
