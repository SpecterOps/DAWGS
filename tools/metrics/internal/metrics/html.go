package metrics

import (
	"embed"
	"fmt"
	"html/template"
	"io"
	"sort"
)

// HTMLReportOptions controls record limits and thresholds in the standalone HTML report.
type HTMLReportOptions struct {
	// Top limits ranked records; WriteHTML defaults zero to 20 and negative values leave them unlimited.
	Top int
	// CRAPOver filters the ranked CRAP table to scores above this threshold.
	CRAPOver float64
	// CycloOver filters the complexity table to functions above this threshold.
	CycloOver int
}

// htmlReportData holds precomputed metrics and quality summaries consumed by the HTML template.
type htmlReportData struct {
	// Report retains source metadata and the full analyzed report.
	Report Report
	// Records contains an independent slice of the analyzed function records.
	Records []FunctionMetric
	// TopCRAP contains the filtered, ranked CRAP records.
	TopCRAP []FunctionMetric
	// TopComplexity contains the filtered, ranked complexity records.
	TopComplexity []FunctionMetric
	// QualitySignals contains summaries of each quality evidence source.
	QualitySignals []qualitySignalSummary
	// QualityFindings contains the highest-priority findings within the configured limit.
	QualityFindings []QualityFinding
	// SeverityCounts counts function records in each CRAP severity band.
	SeverityCounts severityCounts
	// TotalStatements counts instrumented statements across all records.
	TotalStatements int
	// CoveredStatements counts statements reached by tests across all records.
	CoveredStatements int
	// AverageCoverage is CoveredStatements divided by TotalStatements, or zero for an empty report.
	AverageCoverage float64
	// CRAPThreshold is the selected CRAP filter shown to readers.
	CRAPThreshold float64
	// ComplexityThreshold is the selected complexity filter shown to readers.
	ComplexityThreshold int
	// TopCount is the configured limit for ranked table entries.
	TopCount int
	// ReportSummaryComment explains the CRAP score formula used by the report.
	ReportSummaryComment string
}

// severityCounts counts functions by their CRAP score band.
type severityCounts struct {
	// Critical counts functions with CRAP scores of at least 100.
	Critical int
	// High counts functions with CRAP scores from 60 up to 100.
	High int
	// Watch counts functions with CRAP scores from 30 up to 60.
	Watch int
	// Ok counts functions with CRAP scores below 30.
	Ok int
}

// qualitySignalSummary provides presentation text for one quality evidence source.
type qualitySignalSummary struct {
	// Name is the human-readable evidence source name.
	Name string
	// Status is the quality status used to select the visual state.
	Status string
	// Summary is the short result shown in the signal card.
	Summary string
	// Detail explains the counts underlying that result.
	Detail string
}

// WriteHTML renders a standalone escaped report to output using the embedded template.
// Concurrent calls may share report data if callers do not mutate it; each call needs its own writer.
func WriteHTML(output io.Writer, report Report, options HTMLReportOptions) error {
	if options.Top == 0 {
		options.Top = 20
	}

	data := newHTMLReportData(report, options)
	if err := htmlReportTemplate.Execute(output, data); err != nil {
		return fmt.Errorf("execute HTML report template: %w", err)
	}

	return nil
}

// newHTMLReportData computes statement coverage and ranked tables without changing the input records.
func newHTMLReportData(report Report, options HTMLReportOptions) htmlReportData {
	records := make([]FunctionMetric, len(report.Records))
	copy(records, report.Records)

	var (
		totalStatements   int
		coveredStatements int
		counts            severityCounts
	)
	for _, record := range records {
		totalStatements += record.Statements
		coveredStatements += record.CoveredStatements

		switch severityClass(record) {
		case "critical":
			counts.Critical++
		case "high":
			counts.High++
		case "watch":
			counts.Watch++
		default:
			counts.Ok++
		}
	}

	var averageCoverage float64
	if totalStatements > 0 {
		averageCoverage = float64(coveredStatements) / float64(totalStatements)
	}

	return htmlReportData{
		Report:               report,
		Records:              records,
		TopCRAP:              topCRAPRecords(records, options),
		TopComplexity:        topComplexityRecords(records, options),
		QualitySignals:       qualitySignals(report.Quality),
		QualityFindings:      topQualityFindings(report.Quality, options.Top),
		SeverityCounts:       counts,
		TotalStatements:      totalStatements,
		CoveredStatements:    coveredStatements,
		AverageCoverage:      averageCoverage,
		CRAPThreshold:        options.CRAPOver,
		ComplexityThreshold:  options.CycloOver,
		TopCount:             options.Top,
		ReportSummaryComment: "CRAP = complexity^2 * (1 - coverage)^3 + complexity",
	}
}

// htmlTemplateFuncs registers formatting and classification helpers used by the owned HTML template.
func htmlTemplateFuncs() template.FuncMap {
	return template.FuncMap{
		"coverageWidth": func(coverage float64) template.CSS {
			return template.CSS(fmt.Sprintf("width: %.2f%%", coverage*100))
		},
		"fullName": func(record FunctionMetric) string {
			return record.Package + "." + record.Function
		},
		"location": func(record FunctionMetric) string {
			return fmt.Sprintf("%s:%d:%d", record.File, record.Line, record.Column)
		},
		"percent": func(value float64) string {
			return fmt.Sprintf("%.2f", value*100)
		},
		"score": func(value float64) string {
			return fmt.Sprintf("%.2f", value)
		},
		"severityClass": severityClass,
		"severityLabel": severityLabel,
		"statements": func(record FunctionMetric) string {
			return fmt.Sprintf("%d/%d", record.CoveredStatements, record.Statements)
		},
		"qualityStatusClass":   qualityStatusClass,
		"qualityStatusLabel":   qualityStatusLabel,
		"qualitySeverityClass": qualitySeverityClass,
	}
}

// topCRAPRecords filters and ranks an independent record slice by CRAP score.
func topCRAPRecords(records []FunctionMetric, options HTMLReportOptions) []FunctionMetric {
	filteredRecords := filterRecords(records, TextOptions{
		Top:      options.Top,
		CRAPOver: options.CRAPOver,
	})

	return filteredRecords
}

// topComplexityRecords filters and ranks a new record slice by complexity, CRAP, file, and name.
func topComplexityRecords(records []FunctionMetric, options HTMLReportOptions) []FunctionMetric {
	filteredRecords := make([]FunctionMetric, 0, len(records))
	for _, record := range records {
		if options.CycloOver > 0 && record.Complexity <= options.CycloOver {
			continue
		}

		filteredRecords = append(filteredRecords, record)
	}

	sort.SliceStable(filteredRecords, func(leftIndex, rightIndex int) bool {
		left := filteredRecords[leftIndex]
		right := filteredRecords[rightIndex]

		if left.Complexity != right.Complexity {
			return left.Complexity > right.Complexity
		}
		if left.CRAP != right.CRAP {
			return left.CRAP > right.CRAP
		}
		if left.File != right.File {
			return left.File < right.File
		}

		return left.Function < right.Function
	})

	if options.Top > 0 && len(filteredRecords) > options.Top {
		return filteredRecords[:options.Top]
	}

	return filteredRecords
}

// severityClass assigns a CSS severity band from a function's CRAP score.
func severityClass(record FunctionMetric) string {
	switch {
	case record.CRAP >= 100:
		return "critical"
	case record.CRAP >= 60:
		return "high"
	case record.CRAP >= 30:
		return "watch"
	default:
		return "ok"
	}
}

// severityLabel converts the function's CRAP severity band to a display label.
func severityLabel(record FunctionMetric) string {
	switch severityClass(record) {
	case "critical":
		return "Critical"
	case "high":
		return "High"
	case "watch":
		return "Watch"
	default:
		return "OK"
	}
}

// qualitySignals summarizes the six quality evidence sources in report display order.
func qualitySignals(report QualityReport) []qualitySignalSummary {
	return []qualitySignalSummary{
		{
			Name:    "Semantic Drift",
			Status:  report.SemanticDrift.Status,
			Summary: fmt.Sprintf("%d artifact changes", report.SemanticDrift.GeneratedArtifactChanges),
			Detail:  fmt.Sprintf("%d cases, %d template variants, %d metamorphic families", report.SemanticDrift.CoreCases, report.SemanticDrift.TemplateVariants, report.SemanticDrift.MetamorphicFamilies),
		},
		{
			Name:    "Backend Equivalence",
			Status:  report.BackendEquivalence.Status,
			Summary: fmt.Sprintf("%d mismatches", report.BackendEquivalence.Mismatches),
			Detail:  fmt.Sprintf("%d common tests, %d missing tests", report.BackendEquivalence.CommonTests, report.BackendEquivalence.MissingTests),
		},
		{
			Name:    "Invariants",
			Status:  report.Invariants.Status,
			Summary: fmt.Sprintf("%d violations", report.Invariants.Violations),
			Detail:  fmt.Sprintf("%d checked, %d low-oracle assertions", report.Invariants.Checked, report.Invariants.LowOracle),
		},
		{
			Name:    "Fuzz Health",
			Status:  report.Fuzz.Status,
			Summary: fmt.Sprintf("%d targets", report.Fuzz.TargetCount),
			Detail:  fmt.Sprintf("%d corpus files, %d crashes, %d timeouts", report.Fuzz.CorpusFiles, report.Fuzz.CrashCount, report.Fuzz.TimeoutCount),
		},
		{
			Name:    "Mutation Score",
			Status:  report.Mutation.Status,
			Summary: fmt.Sprintf("%.2f%% score", report.Mutation.Score*100),
			Detail:  fmt.Sprintf("%d killed, %d survived, %d timed out", report.Mutation.Killed, report.Mutation.Survived, report.Mutation.TimedOut),
		},
		{
			Name:    "Benchmark Drift",
			Status:  report.BenchmarkDrift.Status,
			Summary: fmt.Sprintf("%d regressions", len(report.BenchmarkDrift.Regressions)),
			Detail:  fmt.Sprintf("%d results, %d improvements", report.BenchmarkDrift.Results, len(report.BenchmarkDrift.Improvements)),
		},
	}
}

// topQualityFindings selects the leading ranked findings, leaving nonpositive limits unbounded.
func topQualityFindings(report QualityReport, top int) []QualityFinding {
	findings := qualityFindings(report)
	if top > 0 && len(findings) > top {
		return findings[:top]
	}

	return findings
}

// qualityStatusClass maps evidence pass/watch states to CSS and treats other states as pending.
func qualityStatusClass(status string) string {
	switch status {
	case QualityStatusPass:
		return "ok"
	case QualityStatusWatch:
		return "watch"
	default:
		return "pending"
	}
}

// qualityStatusLabel maps evidence pass/watch states to labels and treats other states as pending.
func qualityStatusLabel(status string) string {
	switch status {
	case QualityStatusPass:
		return "Pass"
	case QualityStatusWatch:
		return "Watch"
	case QualityStatusPending:
		return "Pending"
	default:
		return "Pending"
	}
}

// qualitySeverityClass maps finding severity to CSS, treating unknown severities as pending.
func qualitySeverityClass(finding QualityFinding) string {
	switch finding.Severity {
	case "critical":
		return "critical"
	case "high":
		return "high"
	case "watch":
		return "watch"
	default:
		return "pending"
	}
}

// htmlReportAssets contains the standalone metrics report template.
//
//go:embed templates/metrics-report.html
var htmlReportAssets embed.FS

// htmlReportTemplate shares the immutable template parsed during package initialization.
var htmlReportTemplate = template.Must(template.New("metrics-report.html").Funcs(htmlTemplateFuncs()).ParseFS(htmlReportAssets, "templates/metrics-report.html"))
