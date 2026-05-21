package metrics

import (
	"bufio"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

const (
	QualityStatusPass    = "pass"
	QualityStatusWatch   = "watch"
	QualityStatusPending = "pending"
)

type NamedPath struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

type QualityOptions struct {
	BackendResults              []NamedPath
	BenchmarkReportPath         string
	BenchmarkBaselinePath       string
	BenchmarkRegressionFraction float64
	FuzzReportPath              string
	MutationReportPath          string
}

type QualityReport struct {
	SourceRoot         string                   `json:"source_root"`
	Summary            QualitySummary           `json:"summary"`
	SemanticDrift      SemanticDriftReport      `json:"semantic_drift"`
	BackendEquivalence BackendEquivalenceReport `json:"backend_equivalence"`
	Invariants         InvariantReport          `json:"invariants"`
	Fuzz               FuzzReport               `json:"fuzz"`
	Mutation           MutationReport           `json:"mutation"`
	BenchmarkDrift     BenchmarkDriftReport     `json:"benchmark_drift"`
}

type QualitySummary struct {
	Status           string `json:"status"`
	PassSignals      int    `json:"pass_signals"`
	WatchSignals     int    `json:"watch_signals"`
	PendingSignals   int    `json:"pending_signals"`
	FindingCount     int    `json:"finding_count"`
	BlockingFindings int    `json:"blocking_findings"`
}

type QualityFinding struct {
	Signal   string `json:"signal"`
	Severity string `json:"severity"`
	ID       string `json:"id"`
	Message  string `json:"message"`
	File     string `json:"file,omitempty"`
	Detail   string `json:"detail,omitempty"`
}

type SemanticDriftReport struct {
	Status                    string           `json:"status"`
	CoreCaseFiles             int              `json:"core_case_files"`
	CoreCases                 int              `json:"core_cases"`
	TemplateFiles             int              `json:"template_files"`
	TemplateFamilies          int              `json:"template_families"`
	TemplateVariants          int              `json:"template_variants"`
	MetamorphicFamilies       int              `json:"metamorphic_families"`
	MetamorphicQueries        int              `json:"metamorphic_queries"`
	TranslationCaseFiles      int              `json:"translation_case_files"`
	GeneratedArtifactChanges  int              `json:"generated_artifact_changes"`
	ChangedGeneratedArtifacts []string         `json:"changed_generated_artifacts,omitempty"`
	Findings                  []QualityFinding `json:"findings,omitempty"`
}

type BackendEquivalenceReport struct {
	Status       string                `json:"status"`
	ResultFiles  []NamedPath           `json:"result_files,omitempty"`
	Drivers      []BackendDriverResult `json:"drivers,omitempty"`
	CommonTests  int                   `json:"common_tests"`
	MissingTests int                   `json:"missing_tests"`
	Mismatches   int                   `json:"mismatches"`
	FailOnAll    int                   `json:"fail_on_all"`
	SkipOnAll    int                   `json:"skip_on_all"`
	Findings     []QualityFinding      `json:"findings,omitempty"`
}

type BackendDriverResult struct {
	Name  string `json:"name"`
	Path  string `json:"path"`
	Pass  int    `json:"pass"`
	Fail  int    `json:"fail"`
	Skip  int    `json:"skip"`
	Tests int    `json:"tests"`
}

type InvariantReport struct {
	Status     string           `json:"status"`
	Files      int              `json:"files"`
	Checked    int              `json:"checked"`
	Violations int              `json:"violations"`
	LowOracle  int              `json:"low_oracle_assertions"`
	ByID       []InvariantCount `json:"by_id,omitempty"`
	Findings   []QualityFinding `json:"findings,omitempty"`
}

type InvariantCount struct {
	ID    string `json:"id"`
	Count int    `json:"count"`
}

type FuzzReport struct {
	Status       string           `json:"status"`
	Targets      []FuzzTarget     `json:"targets,omitempty"`
	TargetCount  int              `json:"target_count"`
	CorpusFiles  int              `json:"corpus_files"`
	ResultPath   string           `json:"result_path,omitempty"`
	CrashCount   int              `json:"crash_count"`
	TimeoutCount int              `json:"timeout_count"`
	Findings     []QualityFinding `json:"findings,omitempty"`
}

type FuzzTarget struct {
	Package     string `json:"package"`
	Name        string `json:"name"`
	File        string `json:"file"`
	Line        int    `json:"line"`
	CorpusFiles int    `json:"corpus_files"`
}

type MutationReport struct {
	Status        string           `json:"status"`
	ReportPath    string           `json:"report_path,omitempty"`
	Mutants       int              `json:"mutants"`
	Killed        int              `json:"killed"`
	Survived      int              `json:"survived"`
	TimedOut      int              `json:"timed_out"`
	CompileErrors int              `json:"compile_errors"`
	Score         float64          `json:"score"`
	Findings      []QualityFinding `json:"findings,omitempty"`
}

type BenchmarkDriftReport struct {
	Status              string                `json:"status"`
	ReportPath          string                `json:"report_path,omitempty"`
	BaselinePath        string                `json:"baseline_path,omitempty"`
	RegressionThreshold float64               `json:"regression_threshold"`
	Results             int                   `json:"results"`
	BaselineResults     int                   `json:"baseline_results"`
	Regressions         []BenchmarkRegression `json:"regressions,omitempty"`
	Improvements        []BenchmarkRegression `json:"improvements,omitempty"`
	Findings            []QualityFinding      `json:"findings,omitempty"`
}

type BenchmarkRegression struct {
	Key           string  `json:"key"`
	Metric        string  `json:"metric"`
	BaselineNanos int64   `json:"baseline_nanos"`
	CurrentNanos  int64   `json:"current_nanos"`
	DeltaFraction float64 `json:"delta_fraction"`
}

type qualityCaseFile struct {
	Dataset string            `json:"dataset"`
	Cases   []qualityTestCase `json:"cases"`
}

type qualityTestCase struct {
	Name    string          `json:"name"`
	Cypher  string          `json:"cypher"`
	Assert  json.RawMessage `json:"assert"`
	Fixture json.RawMessage `json:"fixture,omitempty"`
}

type qualityTemplateFile struct {
	Families    []qualityTemplateFamily    `json:"families,omitempty"`
	Metamorphic []qualityMetamorphicFamily `json:"metamorphic,omitempty"`
}

type qualityTemplateFamily struct {
	Name     string                   `json:"name"`
	Fixture  json.RawMessage          `json:"fixture"`
	Template string                   `json:"template"`
	Variants []qualityTemplateVariant `json:"variants"`
}

type qualityTemplateVariant struct {
	Name   string            `json:"name"`
	Vars   map[string]string `json:"vars,omitempty"`
	Assert json.RawMessage   `json:"assert"`
}

type qualityMetamorphicFamily struct {
	Name    string                    `json:"name"`
	Fixture json.RawMessage           `json:"fixture"`
	Compare any                       `json:"compare"`
	Queries []qualityMetamorphicQuery `json:"queries"`
}

type qualityMetamorphicQuery struct {
	Name   string `json:"name"`
	Cypher string `json:"cypher"`
}

func AnalyzeQuality(sourceRoot string, options QualityOptions) QualityReport {
	report := QualityReport{
		SourceRoot: filepath.ToSlash(sourceRoot),
	}

	report.SemanticDrift = analyzeSemanticDrift(sourceRoot)
	report.BackendEquivalence = analyzeBackendEquivalence(options.BackendResults)
	report.Invariants = analyzeInvariants(sourceRoot)
	report.Fuzz = analyzeFuzz(sourceRoot, options.FuzzReportPath)
	report.Mutation = analyzeMutation(options.MutationReportPath)
	report.BenchmarkDrift = analyzeBenchmarkDrift(options)
	report.Summary = summarizeQuality(report)

	return report
}

func WriteQualityJSON(output io.Writer, report QualityReport) error {
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

func WriteQualityText(output io.Writer, report QualityReport) error {
	if _, err := fmt.Fprintf(output, "Quality report for %s\n", report.SourceRoot); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(
		output,
		"status: %s, findings: %d, watch signals: %d, pending signals: %d\n\n",
		report.Summary.Status,
		report.Summary.FindingCount,
		report.Summary.WatchSignals,
		report.Summary.PendingSignals,
	); err != nil {
		return err
	}

	rows := []struct {
		name   string
		status string
	}{
		{"semantic_drift", report.SemanticDrift.Status},
		{"backend_equivalence", report.BackendEquivalence.Status},
		{"invariants", report.Invariants.Status},
		{"fuzz", report.Fuzz.Status},
		{"mutation", report.Mutation.Status},
		{"benchmark_drift", report.BenchmarkDrift.Status},
	}

	if _, err := fmt.Fprintln(output, "SIGNAL                 STATUS"); err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := fmt.Fprintf(output, "%-22s %s\n", row.name, row.status); err != nil {
			return err
		}
	}

	findings := qualityFindings(report)
	if len(findings) == 0 {
		_, err := fmt.Fprintln(output, "\nNo quality findings.")
		return err
	}

	if _, err := fmt.Fprintln(output, "\nSEV      SIGNAL                 ID                                LOCATION"); err != nil {
		return err
	}
	for _, finding := range findings {
		location := finding.File
		if location == "" {
			location = "-"
		}
		if _, err := fmt.Fprintf(output, "%-8s %-22s %-33s %s\n", finding.Severity, finding.Signal, finding.ID, location); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(output, "  %s\n", finding.Message); err != nil {
			return err
		}
	}

	return nil
}

func analyzeSemanticDrift(sourceRoot string) SemanticDriftReport {
	report := SemanticDriftReport{Status: QualityStatusPass}

	caseFiles, caseCount, caseFindings := countCoreCases(sourceRoot)
	report.CoreCaseFiles = caseFiles
	report.CoreCases = caseCount
	report.Findings = append(report.Findings, caseFindings...)

	templateCounts, templateFindings := countTemplates(sourceRoot)
	report.TemplateFiles = templateCounts.files
	report.TemplateFamilies = templateCounts.families
	report.TemplateVariants = templateCounts.variants
	report.MetamorphicFamilies = templateCounts.metamorphicFamilies
	report.MetamorphicQueries = templateCounts.metamorphicQueries
	report.Findings = append(report.Findings, templateFindings...)

	report.TranslationCaseFiles = countFilesWithExtension(filepath.Join(sourceRoot, "cypher", "models", "pgsql", "test", "translation_cases"), ".sql")

	changedFiles, gitFinding := changedGeneratedArtifacts(sourceRoot)
	report.ChangedGeneratedArtifacts = changedFiles
	report.GeneratedArtifactChanges = len(changedFiles)
	if gitFinding != nil {
		report.Findings = append(report.Findings, *gitFinding)
	}
	for _, path := range changedFiles {
		report.Findings = append(report.Findings, QualityFinding{
			Signal:   "semantic_drift",
			Severity: "watch",
			ID:       "generated_artifact_changed",
			Message:  "Generated or fixture-backed artifact has uncommitted changes.",
			File:     path,
		})
	}

	report.Status = statusFromFindings(report.Findings)
	return report
}

func countCoreCases(sourceRoot string) (int, int, []QualityFinding) {
	var files int
	var cases int
	var findings []QualityFinding

	for _, path := range jsonFiles(filepath.Join(sourceRoot, "integration", "testdata", "cases")) {
		files++
		var doc qualityCaseFile
		if err := readJSON(path, &doc); err != nil {
			findings = append(findings, fileFinding("semantic_drift", "high", "case_file_invalid_json", path, err.Error()))
			continue
		}

		cases += len(doc.Cases)
	}

	return files, cases, findings
}

type templateCount struct {
	files               int
	families            int
	variants            int
	metamorphicFamilies int
	metamorphicQueries  int
}

func countTemplates(sourceRoot string) (templateCount, []QualityFinding) {
	var counts templateCount
	var findings []QualityFinding

	for _, path := range jsonFiles(filepath.Join(sourceRoot, "integration", "testdata", "templates")) {
		counts.files++
		var doc qualityTemplateFile
		if err := readJSON(path, &doc); err != nil {
			findings = append(findings, fileFinding("semantic_drift", "high", "template_file_invalid_json", path, err.Error()))
			continue
		}

		counts.families += len(doc.Families)
		counts.metamorphicFamilies += len(doc.Metamorphic)
		for _, family := range doc.Families {
			counts.variants += len(family.Variants)
		}
		for _, family := range doc.Metamorphic {
			counts.metamorphicQueries += len(family.Queries)
		}
	}

	return counts, findings
}

func changedGeneratedArtifacts(sourceRoot string) ([]string, *QualityFinding) {
	args := []string{
		"-C", sourceRoot,
		"status", "--porcelain", "--",
		"integration/testdata/cases",
		"integration/testdata/templates",
		"cypher/test/cases",
		"cypher/models/pgsql/test/translation_cases",
		"cypher/parser",
	}

	output, err := exec.Command("git", args...).Output()
	if err != nil {
		finding := QualityFinding{
			Signal:   "semantic_drift",
			Severity: "info",
			ID:       "git_status_unavailable",
			Message:  "Unable to inspect git status for generated artifact drift.",
			Detail:   err.Error(),
		}
		return nil, &finding
	}

	var changed []string
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) < 4 {
			continue
		}

		path := strings.TrimSpace(line[3:])
		if strings.Contains(path, " -> ") {
			parts := strings.Split(path, " -> ")
			path = strings.TrimSpace(parts[len(parts)-1])
		}
		if path != "" {
			changed = append(changed, filepath.ToSlash(path))
		}
	}
	sort.Strings(changed)

	return changed, nil
}

func analyzeBackendEquivalence(results []NamedPath) BackendEquivalenceReport {
	report := BackendEquivalenceReport{
		Status:      QualityStatusPending,
		ResultFiles: append([]NamedPath(nil), results...),
	}

	if len(results) < 2 {
		report.Findings = append(report.Findings, QualityFinding{
			Signal:   "backend_equivalence",
			Severity: "info",
			ID:       "backend_results_missing",
			Message:  "Provide at least two go test -json result files to compare backend behavior.",
		})
		return report
	}

	driverTests := make(map[string]map[string]string, len(results))
	for _, result := range results {
		tests, summary, findings := parseBackendTestResult(result)
		report.Drivers = append(report.Drivers, summary)
		report.Findings = append(report.Findings, findings...)
		if len(tests) > 0 {
			driverTests[result.Name] = tests
		}
	}

	if len(driverTests) < 2 {
		report.Status = QualityStatusPending
		return report
	}

	testKeys := map[string]struct{}{}
	for _, tests := range driverTests {
		for key := range tests {
			testKeys[key] = struct{}{}
		}
	}

	var sortedKeys []string
	for key := range testKeys {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)

	for _, key := range sortedKeys {
		statuses := map[string]string{}
		missing := false
		for driverName, tests := range driverTests {
			status, found := tests[key]
			if !found {
				missing = true
				status = "missing"
			}
			statuses[driverName] = status
		}

		if missing {
			report.MissingTests++
			report.Findings = append(report.Findings, QualityFinding{
				Signal:   "backend_equivalence",
				Severity: "watch",
				ID:       "backend_test_missing",
				Message:  "Backend test result is present for one driver but missing for another.",
				Detail:   key + " " + formatStatusMap(statuses),
			})
			continue
		}

		report.CommonTests++
		if allStatuses(statuses, "fail") {
			report.FailOnAll++
			continue
		}
		if allStatuses(statuses, "skip") {
			report.SkipOnAll++
			continue
		}
		if !sameStatuses(statuses) {
			report.Mismatches++
			report.Findings = append(report.Findings, QualityFinding{
				Signal:   "backend_equivalence",
				Severity: "high",
				ID:       "backend_status_mismatch",
				Message:  "Shared backend test has different outcomes across drivers.",
				Detail:   key + " " + formatStatusMap(statuses),
			})
		}
	}

	report.Status = statusFromFindings(report.Findings)
	return report
}

type goTestEvent struct {
	Action  string  `json:"Action"`
	Package string  `json:"Package"`
	Test    string  `json:"Test"`
	Output  string  `json:"Output"`
	Elapsed float64 `json:"Elapsed"`
}

func parseBackendTestResult(result NamedPath) (map[string]string, BackendDriverResult, []QualityFinding) {
	summary := BackendDriverResult{Name: result.Name, Path: result.Path}
	findings := []QualityFinding{}
	tests := map[string]string{}

	file, err := os.Open(result.Path)
	if err != nil {
		findings = append(findings, QualityFinding{
			Signal:   "backend_equivalence",
			Severity: "info",
			ID:       "backend_result_unreadable",
			Message:  "Unable to read backend go test -json result file.",
			File:     result.Path,
			Detail:   err.Error(),
		})
		return tests, summary, findings
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var event goTestEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			continue
		}
		if event.Test == "" {
			continue
		}
		if event.Action != "pass" && event.Action != "fail" && event.Action != "skip" {
			continue
		}

		key := event.Package + " " + event.Test
		tests[key] = event.Action
	}
	if err := scanner.Err(); err != nil {
		findings = append(findings, QualityFinding{
			Signal:   "backend_equivalence",
			Severity: "info",
			ID:       "backend_result_read_error",
			Message:  "Unable to read complete backend go test -json result file.",
			File:     result.Path,
			Detail:   err.Error(),
		})
	}

	for _, status := range tests {
		switch status {
		case "pass":
			summary.Pass++
		case "fail":
			summary.Fail++
		case "skip":
			summary.Skip++
		}
	}
	summary.Tests = len(tests)

	return tests, summary, findings
}

func analyzeInvariants(sourceRoot string) InvariantReport {
	report := InvariantReport{Status: QualityStatusPass}

	for _, path := range jsonFiles(filepath.Join(sourceRoot, "integration", "testdata", "cases")) {
		report.Files++
		var raw any
		if err := readJSON(path, &raw); err != nil {
			report.Findings = append(report.Findings, fileFinding("invariants", "high", "case_file_invalid_json", path, err.Error()))
			continue
		}
		report.Findings = append(report.Findings, forbiddenKeyFindings(path, raw)...)

		var doc qualityCaseFile
		if err := readJSON(path, &doc); err != nil {
			continue
		}
		report.Checked += len(doc.Cases)
		report.Findings = append(report.Findings, validateCases(path, doc, &report)...)
	}

	for _, path := range jsonFiles(filepath.Join(sourceRoot, "integration", "testdata", "templates")) {
		report.Files++
		var raw any
		if err := readJSON(path, &raw); err != nil {
			report.Findings = append(report.Findings, fileFinding("invariants", "high", "template_file_invalid_json", path, err.Error()))
			continue
		}
		report.Findings = append(report.Findings, forbiddenKeyFindings(path, raw)...)

		var doc qualityTemplateFile
		if err := readJSON(path, &doc); err != nil {
			continue
		}
		for _, family := range doc.Families {
			report.Checked += len(family.Variants)
		}
		for _, family := range doc.Metamorphic {
			report.Checked += len(family.Queries)
		}
		report.Findings = append(report.Findings, validateTemplateFile(path, doc, &report)...)
	}

	report.Violations = countFindingsAtLeast(report.Findings, "watch")
	report.ByID = countsByFindingID(report.Findings)
	report.Status = statusFromFindings(report.Findings)
	return report
}

func validateCases(path string, doc qualityCaseFile, report *InvariantReport) []QualityFinding {
	var findings []QualityFinding
	seen := map[string]struct{}{}

	for _, testCase := range doc.Cases {
		contextName := testCase.Name
		if contextName == "" {
			contextName = "<unnamed>"
		}

		if testCase.Name == "" {
			findings = append(findings, fileFinding("invariants", "high", "case_missing_name", path, "case entry is missing name"))
		} else if _, duplicate := seen[testCase.Name]; duplicate {
			findings = append(findings, fileFinding("invariants", "high", "case_duplicate_name", path, "duplicate case name "+testCase.Name))
		}
		seen[testCase.Name] = struct{}{}

		if strings.TrimSpace(testCase.Cypher) == "" {
			findings = append(findings, fileFinding("invariants", "high", "case_missing_cypher", path, contextName+" has no cypher query"))
		}

		assertFindings, lowOracle := validateAssertion(path, contextName, testCase.Assert)
		findings = append(findings, assertFindings...)
		if lowOracle {
			report.LowOracle++
		}
	}

	return findings
}

func validateTemplateFile(path string, doc qualityTemplateFile, report *InvariantReport) []QualityFinding {
	var findings []QualityFinding
	familyNames := map[string]struct{}{}

	for _, family := range doc.Families {
		if family.Name == "" {
			findings = append(findings, fileFinding("invariants", "high", "template_family_missing_name", path, "template family is missing name"))
		} else if _, duplicate := familyNames[family.Name]; duplicate {
			findings = append(findings, fileFinding("invariants", "high", "template_family_duplicate_name", path, "duplicate template family name "+family.Name))
		}
		familyNames[family.Name] = struct{}{}

		if len(family.Fixture) == 0 {
			findings = append(findings, fileFinding("invariants", "high", "template_family_missing_fixture", path, family.Name+" has no inline fixture"))
		}
		if strings.TrimSpace(family.Template) == "" {
			findings = append(findings, fileFinding("invariants", "high", "template_family_missing_template", path, family.Name+" has no template"))
		}

		placeholders := placeholderNames(family.Template)
		variantNames := map[string]struct{}{}
		for _, variant := range family.Variants {
			contextName := family.Name + "/" + variant.Name
			if variant.Name == "" {
				findings = append(findings, fileFinding("invariants", "high", "template_variant_missing_name", path, family.Name+" has a variant without a name"))
			} else if _, duplicate := variantNames[variant.Name]; duplicate {
				findings = append(findings, fileFinding("invariants", "high", "template_variant_duplicate_name", path, "duplicate variant name "+contextName))
			}
			variantNames[variant.Name] = struct{}{}

			for _, placeholder := range placeholders {
				if _, found := variant.Vars[placeholder]; !found {
					findings = append(findings, fileFinding("invariants", "high", "template_variant_missing_placeholder", path, contextName+" does not define "+placeholder))
				}
			}

			assertFindings, lowOracle := validateAssertion(path, contextName, variant.Assert)
			findings = append(findings, assertFindings...)
			if lowOracle {
				report.LowOracle++
			}
		}
	}

	for _, family := range doc.Metamorphic {
		if family.Name == "" {
			findings = append(findings, fileFinding("invariants", "high", "metamorphic_family_missing_name", path, "metamorphic family is missing name"))
		}
		if len(family.Fixture) == 0 {
			findings = append(findings, fileFinding("invariants", "high", "metamorphic_family_missing_fixture", path, family.Name+" has no inline fixture"))
		}
		if !hasComparisonMode(family.Compare) {
			findings = append(findings, fileFinding("invariants", "high", "metamorphic_family_missing_compare", path, family.Name+" has no comparison mode"))
		}
		if len(family.Queries) < 2 {
			findings = append(findings, fileFinding("invariants", "high", "metamorphic_family_too_few_queries", path, family.Name+" has fewer than two queries"))
		}

		queryNames := map[string]struct{}{}
		for _, query := range family.Queries {
			contextName := family.Name + "/" + query.Name
			if query.Name == "" {
				findings = append(findings, fileFinding("invariants", "high", "metamorphic_query_missing_name", path, family.Name+" has a query without a name"))
			} else if _, duplicate := queryNames[query.Name]; duplicate {
				findings = append(findings, fileFinding("invariants", "high", "metamorphic_query_duplicate_name", path, "duplicate metamorphic query name "+contextName))
			}
			queryNames[query.Name] = struct{}{}

			if strings.TrimSpace(query.Cypher) == "" {
				findings = append(findings, fileFinding("invariants", "high", "metamorphic_query_missing_cypher", path, contextName+" has no cypher query"))
			}
		}
	}

	return findings
}

func analyzeFuzz(sourceRoot, resultPath string) FuzzReport {
	report := FuzzReport{
		Status:     QualityStatusPending,
		ResultPath: resultPath,
	}

	targets, findings := discoverFuzzTargets(sourceRoot)
	report.Targets = targets
	report.TargetCount = len(targets)
	report.Findings = append(report.Findings, findings...)
	for _, target := range targets {
		report.CorpusFiles += target.CorpusFiles
	}

	if len(targets) == 0 {
		report.Findings = append(report.Findings, QualityFinding{
			Signal:   "fuzz",
			Severity: "info",
			ID:       "fuzz_targets_missing",
			Message:  "No Go fuzz targets were discovered.",
		})
	}

	if resultPath != "" {
		crashes, timeouts, parseFindings := parseFuzzResult(resultPath)
		report.CrashCount = crashes
		report.TimeoutCount = timeouts
		report.Findings = append(report.Findings, parseFindings...)
	}

	if len(targets) > 0 {
		report.Status = QualityStatusPass
	}
	if report.CrashCount > 0 || report.TimeoutCount > 0 || hasFindingAtLeast(report.Findings, "watch") {
		report.Status = QualityStatusWatch
	}

	return report
}

func discoverFuzzTargets(sourceRoot string) ([]FuzzTarget, []QualityFinding) {
	var targets []FuzzTarget
	var findings []QualityFinding

	err := filepath.WalkDir(sourceRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if shouldSkipDirectory(sourceRoot, path, entry.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}

		relativePath, err := relativeSlashPath(sourceRoot, path)
		if err != nil {
			return err
		}

		fileSet := token.NewFileSet()
		parsedFile, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			findings = append(findings, fileFinding("fuzz", "info", "fuzz_file_parse_error", relativePath, err.Error()))
			return nil
		}

		for _, declaration := range parsedFile.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || !strings.HasPrefix(function.Name.Name, "Fuzz") {
				continue
			}

			position := fileSet.Position(function.Pos())
			target := FuzzTarget{
				Package:     parsedFile.Name.Name,
				Name:        function.Name.Name,
				File:        relativePath,
				Line:        position.Line,
				CorpusFiles: countCorpusFiles(filepath.Join(filepath.Dir(path), "testdata", "fuzz", function.Name.Name)),
			}
			targets = append(targets, target)
		}

		return nil
	})
	if err != nil {
		findings = append(findings, QualityFinding{
			Signal:   "fuzz",
			Severity: "info",
			ID:       "fuzz_scan_error",
			Message:  "Unable to scan source tree for fuzz targets.",
			Detail:   err.Error(),
		})
	}

	sort.SliceStable(targets, func(leftIndex, rightIndex int) bool {
		left := targets[leftIndex]
		right := targets[rightIndex]
		if left.File != right.File {
			return left.File < right.File
		}
		return left.Name < right.Name
	})

	return targets, findings
}

func parseFuzzResult(path string) (int, int, []QualityFinding) {
	file, err := os.Open(path)
	if err != nil {
		return 0, 0, []QualityFinding{{
			Signal:   "fuzz",
			Severity: "info",
			ID:       "fuzz_result_unreadable",
			Message:  "Unable to read fuzz go test -json result file.",
			File:     path,
			Detail:   err.Error(),
		}}
	}
	defer file.Close()

	var crashes int
	var timeouts int
	var findings []QualityFinding
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var event goTestEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			continue
		}
		if event.Test == "" || !strings.HasPrefix(event.Test, "Fuzz") {
			continue
		}
		if event.Action == "fail" {
			crashes++
			findings = append(findings, QualityFinding{
				Signal:   "fuzz",
				Severity: "high",
				ID:       "fuzz_target_failed",
				Message:  "Fuzz target failed.",
				Detail:   event.Package + " " + event.Test,
			})
		}
		if strings.Contains(strings.ToLower(event.Output), "timeout") {
			timeouts++
		}
	}

	return crashes, timeouts, findings
}

type mutationInputReport struct {
	Mutants       int                    `json:"mutants"`
	Total         int                    `json:"total"`
	Killed        int                    `json:"killed"`
	Survived      int                    `json:"survived"`
	TimedOut      int                    `json:"timed_out"`
	CompileErrors int                    `json:"compile_errors"`
	Score         float64                `json:"score"`
	Findings      []mutationInputFinding `json:"findings"`
}

type mutationInputFinding struct {
	Package  string `json:"package"`
	File     string `json:"file"`
	Line     int    `json:"line"`
	Operator string `json:"operator"`
	Detail   string `json:"detail"`
}

func analyzeMutation(path string) MutationReport {
	report := MutationReport{
		Status:     QualityStatusPending,
		ReportPath: path,
	}

	if path == "" {
		report.Findings = append(report.Findings, QualityFinding{
			Signal:   "mutation",
			Severity: "info",
			ID:       "mutation_report_missing",
			Message:  "Provide a mutation report JSON file to score mutation testing strength.",
		})
		return report
	}

	var input mutationInputReport
	if err := readJSON(path, &input); err != nil {
		report.Findings = append(report.Findings, fileFinding("mutation", "info", "mutation_report_unreadable", path, err.Error()))
		return report
	}

	report.Mutants = input.Mutants
	if report.Mutants == 0 {
		report.Mutants = input.Total
	}
	report.Killed = input.Killed
	report.Survived = input.Survived
	report.TimedOut = input.TimedOut
	report.CompileErrors = input.CompileErrors
	report.Score = input.Score
	if report.Score == 0 && report.Mutants > 0 {
		report.Score = float64(report.Killed) / float64(report.Mutants)
	}

	for _, finding := range input.Findings {
		file := finding.File
		if finding.Line > 0 {
			file = fmt.Sprintf("%s:%d", file, finding.Line)
		}
		message := "Surviving mutation"
		if finding.Operator != "" {
			message += " for " + finding.Operator
		}
		report.Findings = append(report.Findings, QualityFinding{
			Signal:   "mutation",
			Severity: "watch",
			ID:       "mutation_survived",
			Message:  message,
			File:     file,
			Detail:   strings.TrimSpace(finding.Package + " " + finding.Detail),
		})
	}

	if report.Survived > 0 {
		report.Findings = append(report.Findings, QualityFinding{
			Signal:   "mutation",
			Severity: "watch",
			ID:       "mutation_survivors",
			Message:  fmt.Sprintf("%d mutation candidates survived the test suite.", report.Survived),
		})
	}
	if report.TimedOut > 0 || report.CompileErrors > 0 {
		report.Findings = append(report.Findings, QualityFinding{
			Signal:   "mutation",
			Severity: "info",
			ID:       "mutation_incomplete",
			Message:  "Mutation run had timed-out or compile-error candidates.",
		})
	}

	report.Status = statusFromFindings(report.Findings)
	return report
}

type benchmarkInputReport struct {
	Driver  string                 `json:"driver"`
	Results []benchmarkInputResult `json:"results"`
}

type benchmarkInputResult struct {
	Section string              `json:"section"`
	Label   string              `json:"label"`
	Dataset string              `json:"dataset"`
	Stats   benchmarkInputStats `json:"stats"`
}

type benchmarkInputStats struct {
	Median int64 `json:"median"`
	P95    int64 `json:"p95"`
	Max    int64 `json:"max"`
}

func analyzeBenchmarkDrift(options QualityOptions) BenchmarkDriftReport {
	threshold := options.BenchmarkRegressionFraction
	if threshold <= 0 {
		threshold = 0.20
	}

	report := BenchmarkDriftReport{
		Status:              QualityStatusPending,
		ReportPath:          options.BenchmarkReportPath,
		BaselinePath:        options.BenchmarkBaselinePath,
		RegressionThreshold: threshold,
	}

	if options.BenchmarkReportPath == "" || options.BenchmarkBaselinePath == "" {
		report.Findings = append(report.Findings, QualityFinding{
			Signal:   "benchmark_drift",
			Severity: "info",
			ID:       "benchmark_inputs_missing",
			Message:  "Provide benchmark report and baseline JSON files to compare performance drift.",
		})
		return report
	}

	current, err := readBenchmarkReport(options.BenchmarkReportPath)
	if err != nil {
		report.Findings = append(report.Findings, fileFinding("benchmark_drift", "info", "benchmark_report_unreadable", options.BenchmarkReportPath, err.Error()))
		return report
	}
	baseline, err := readBenchmarkReport(options.BenchmarkBaselinePath)
	if err != nil {
		report.Findings = append(report.Findings, fileFinding("benchmark_drift", "info", "benchmark_baseline_unreadable", options.BenchmarkBaselinePath, err.Error()))
		return report
	}

	currentByKey := benchmarkResultsByKey(current)
	baselineByKey := benchmarkResultsByKey(baseline)
	report.Results = len(currentByKey)
	report.BaselineResults = len(baselineByKey)

	var keys []string
	for key := range currentByKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		currentResult := currentByKey[key]
		baselineResult, found := baselineByKey[key]
		if !found {
			report.Findings = append(report.Findings, QualityFinding{
				Signal:   "benchmark_drift",
				Severity: "info",
				ID:       "benchmark_baseline_missing_result",
				Message:  "Benchmark result has no matching baseline.",
				Detail:   key,
			})
			continue
		}

		report.compareBenchmarkMetric(key, "median", baselineResult.Stats.Median, currentResult.Stats.Median)
		report.compareBenchmarkMetric(key, "p95", baselineResult.Stats.P95, currentResult.Stats.P95)
	}

	if len(report.Regressions) > 0 {
		report.Findings = append(report.Findings, QualityFinding{
			Signal:   "benchmark_drift",
			Severity: "watch",
			ID:       "benchmark_regressions",
			Message:  fmt.Sprintf("%d benchmark metrics regressed beyond %.0f%%.", len(report.Regressions), threshold*100),
		})
	}

	report.Status = statusFromFindings(report.Findings)
	return report
}

func (s *BenchmarkDriftReport) compareBenchmarkMetric(key, metric string, baseline, current int64) {
	if baseline <= 0 || current <= 0 {
		return
	}

	delta := (float64(current) - float64(baseline)) / float64(baseline)
	record := BenchmarkRegression{
		Key:           key,
		Metric:        metric,
		BaselineNanos: baseline,
		CurrentNanos:  current,
		DeltaFraction: delta,
	}
	if delta > s.RegressionThreshold {
		s.Regressions = append(s.Regressions, record)
	} else if delta < -s.RegressionThreshold {
		s.Improvements = append(s.Improvements, record)
	}
}

func readBenchmarkReport(path string) (benchmarkInputReport, error) {
	var report benchmarkInputReport
	if err := readJSON(path, &report); err != nil {
		return benchmarkInputReport{}, err
	}

	return report, nil
}

func benchmarkResultsByKey(report benchmarkInputReport) map[string]benchmarkInputResult {
	results := make(map[string]benchmarkInputResult, len(report.Results))
	for _, result := range report.Results {
		key := strings.Join([]string{report.Driver, result.Dataset, result.Section, result.Label}, "/")
		results[key] = result
	}

	return results
}

func summarizeQuality(report QualityReport) QualitySummary {
	summary := QualitySummary{}
	statuses := []string{
		report.SemanticDrift.Status,
		report.BackendEquivalence.Status,
		report.Invariants.Status,
		report.Fuzz.Status,
		report.Mutation.Status,
		report.BenchmarkDrift.Status,
	}

	for _, status := range statuses {
		switch status {
		case QualityStatusWatch:
			summary.WatchSignals++
		case QualityStatusPending:
			summary.PendingSignals++
		default:
			summary.PassSignals++
		}
	}

	findings := qualityFindings(report)
	summary.FindingCount = len(findings)
	summary.BlockingFindings = countFindingsAtLeast(findings, "high")

	switch {
	case summary.WatchSignals > 0 || summary.BlockingFindings > 0:
		summary.Status = QualityStatusWatch
	case summary.PendingSignals > 0:
		summary.Status = QualityStatusPending
	default:
		summary.Status = QualityStatusPass
	}

	return summary
}

func qualityFindings(report QualityReport) []QualityFinding {
	var findings []QualityFinding
	findings = append(findings, report.SemanticDrift.Findings...)
	findings = append(findings, report.BackendEquivalence.Findings...)
	findings = append(findings, report.Invariants.Findings...)
	findings = append(findings, report.Fuzz.Findings...)
	findings = append(findings, report.Mutation.Findings...)
	findings = append(findings, report.BenchmarkDrift.Findings...)

	sort.SliceStable(findings, func(leftIndex, rightIndex int) bool {
		left := findings[leftIndex]
		right := findings[rightIndex]
		if severityRank(left.Severity) != severityRank(right.Severity) {
			return severityRank(left.Severity) > severityRank(right.Severity)
		}
		if left.Signal != right.Signal {
			return left.Signal < right.Signal
		}
		if left.ID != right.ID {
			return left.ID < right.ID
		}
		return left.File < right.File
	})

	return findings
}

func statusFromFindings(findings []QualityFinding) string {
	if hasFindingAtLeast(findings, "watch") {
		return QualityStatusWatch
	}
	if len(findings) > 0 {
		return QualityStatusPending
	}
	return QualityStatusPass
}

func hasFindingAtLeast(findings []QualityFinding, minimum string) bool {
	return countFindingsAtLeast(findings, minimum) > 0
}

func countFindingsAtLeast(findings []QualityFinding, minimum string) int {
	minimumRank := severityRank(minimum)
	var count int
	for _, finding := range findings {
		if severityRank(finding.Severity) >= minimumRank {
			count++
		}
	}

	return count
}

func severityRank(severity string) int {
	switch severity {
	case "critical":
		return 4
	case "high":
		return 3
	case "watch":
		return 2
	case "info":
		return 1
	default:
		return 0
	}
}

func countsByFindingID(findings []QualityFinding) []InvariantCount {
	counts := map[string]int{}
	for _, finding := range findings {
		counts[finding.ID]++
	}

	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	result := make([]InvariantCount, 0, len(keys))
	for _, key := range keys {
		result = append(result, InvariantCount{ID: key, Count: counts[key]})
	}

	return result
}

func fileFinding(signal, severity, id, path, message string) QualityFinding {
	return QualityFinding{
		Signal:   signal,
		Severity: severity,
		ID:       id,
		Message:  message,
		File:     filepath.ToSlash(path),
	}
}

func readJSON(path string, value any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, value); err != nil {
		return err
	}

	return nil
}

func jsonFiles(root string) []string {
	var paths []string
	_ = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	sort.Strings(paths)

	return paths
}

func countFilesWithExtension(root, extension string) int {
	var count int
	_ = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(path) != extension {
			return nil
		}
		count++
		return nil
	})

	return count
}

func countCorpusFiles(root string) int {
	var count int
	_ = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		count++
		return nil
	})

	return count
}

var forbiddenQualityKeys = map[string]struct{}{
	"assert_by_driver": {},
	"skip_drivers":     {},
}

func forbiddenKeyFindings(path string, value any) []QualityFinding {
	var findings []QualityFinding
	walkForbiddenKeys(value, func(key string) {
		findings = append(findings, fileFinding(
			"invariants",
			"high",
			"core_backend_specific_key",
			path,
			fmt.Sprintf("core integration suite uses backend-specific key %q", key),
		))
	})

	return findings
}

func walkForbiddenKeys(value any, onForbidden func(string)) {
	switch typedValue := value.(type) {
	case map[string]any:
		for key, child := range typedValue {
			if _, forbidden := forbiddenQualityKeys[key]; forbidden {
				onForbidden(key)
			}
			walkForbiddenKeys(child, onForbidden)
		}
	case []any:
		for _, child := range typedValue {
			walkForbiddenKeys(child, onForbidden)
		}
	}
}

var allowedStringAssertions = map[string]struct{}{
	"empty":       {},
	"no_error":    {},
	"non_empty":   {},
	"query_error": {},
}

var lowOracleStringAssertions = map[string]struct{}{
	"empty":       {},
	"no_error":    {},
	"non_empty":   {},
	"query_error": {},
}

var allowedObjectAssertions = map[string]struct{}{
	"at_least_int":             {},
	"contains_edge":            {},
	"contains_node_with_prop":  {},
	"contains_node_with_props": {},
	"exact_int":                {},
	"node_id_set":              {},
	"node_ids":                 {},
	"node_list_ids":            {},
	"ordered_node_ids":         {},
	"ordered_row_values":       {},
	"ordered_scalar_values":    {},
	"path_edge_kinds":          {},
	"path_lengths":             {},
	"path_node_ids":            {},
	"relationship_list_kinds":  {},
	"row_count":                {},
	"row_values":               {},
	"scalar_values":            {},
}

func validateAssertion(path, contextName string, raw json.RawMessage) ([]QualityFinding, bool) {
	if len(raw) == 0 {
		return []QualityFinding{fileFinding("invariants", "high", "assertion_missing", path, contextName+" has no assertion")}, false
	}

	var stringAssertion string
	if err := json.Unmarshal(raw, &stringAssertion); err == nil {
		if _, allowed := allowedStringAssertions[stringAssertion]; !allowed {
			return []QualityFinding{fileFinding("invariants", "high", "assertion_unknown_string", path, contextName+" uses unknown assertion "+stringAssertion)}, false
		}

		_, lowOracle := lowOracleStringAssertions[stringAssertion]
		return nil, lowOracle
	}

	var objectAssertion map[string]json.RawMessage
	if err := json.Unmarshal(raw, &objectAssertion); err != nil {
		return []QualityFinding{fileFinding("invariants", "high", "assertion_invalid", path, contextName+" has invalid assertion JSON")}, false
	}
	if len(objectAssertion) == 0 {
		return []QualityFinding{fileFinding("invariants", "high", "assertion_empty", path, contextName+" has an empty assertion object")}, false
	}

	var findings []QualityFinding
	for key := range objectAssertion {
		if _, allowed := allowedObjectAssertions[key]; !allowed {
			findings = append(findings, fileFinding("invariants", "high", "assertion_unknown_object_key", path, contextName+" uses unknown assertion key "+key))
		}
	}

	return findings, false
}

func placeholderNames(template string) []string {
	seen := map[string]struct{}{}
	for {
		start := strings.Index(template, "{{")
		if start == -1 {
			break
		}
		template = template[start+2:]
		end := strings.Index(template, "}}")
		if end == -1 {
			break
		}
		name := strings.TrimSpace(template[:end])
		if name != "" {
			seen[name] = struct{}{}
		}
		template = template[end+2:]
	}

	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)

	return names
}

func hasComparisonMode(compare any) bool {
	switch typedValue := compare.(type) {
	case string:
		return strings.TrimSpace(typedValue) != ""
	case []any:
		return len(typedValue) > 0
	case []string:
		return len(typedValue) > 0
	default:
		return typedValue != nil
	}
}

func allStatuses(statuses map[string]string, want string) bool {
	for _, status := range statuses {
		if status != want {
			return false
		}
	}

	return len(statuses) > 0
}

func sameStatuses(statuses map[string]string) bool {
	var previous string
	for _, status := range statuses {
		if previous == "" {
			previous = status
			continue
		}
		if status != previous {
			return false
		}
	}

	return true
}

func formatStatusMap(statuses map[string]string) string {
	keys := make([]string, 0, len(statuses))
	for key := range statuses {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+statuses[key])
	}

	return strings.Join(parts, ",")
}
