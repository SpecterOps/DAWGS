package metrics

import (
	"fmt"
	"html/template"
	"io"
	"sort"
)

type HTMLReportOptions struct {
	Top       int
	CRAPOver  float64
	CycloOver int
}

type htmlReportData struct {
	Report               Report
	Records              []FunctionMetric
	TopCRAP              []FunctionMetric
	TopComplexity        []FunctionMetric
	QualitySignals       []qualitySignalSummary
	QualityFindings      []QualityFinding
	SeverityCounts       severityCounts
	TotalStatements      int
	CoveredStatements    int
	AverageCoverage      float64
	CRAPThreshold        float64
	ComplexityThreshold  int
	TopCount             int
	ReportSummaryComment string
}

type severityCounts struct {
	Critical int
	High     int
	Watch    int
	Ok       int
}

type qualitySignalSummary struct {
	Name    string
	Status  string
	Summary string
	Detail  string
}

func WriteHTML(output io.Writer, report Report, options HTMLReportOptions) error {
	if options.Top == 0 {
		options.Top = 20
	}

	data := newHTMLReportData(report, options)
	reportTemplate, err := template.New("metrics-report").Funcs(htmlTemplateFuncs()).Parse(htmlReportTemplate)
	if err != nil {
		return fmt.Errorf("parse HTML report template: %w", err)
	}

	if err := reportTemplate.Execute(output, data); err != nil {
		return fmt.Errorf("execute HTML report template: %w", err)
	}

	return nil
}

func newHTMLReportData(report Report, options HTMLReportOptions) htmlReportData {
	records := make([]FunctionMetric, len(report.Records))
	copy(records, report.Records)

	var totalStatements int
	var coveredStatements int
	var counts severityCounts
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

func topCRAPRecords(records []FunctionMetric, options HTMLReportOptions) []FunctionMetric {
	filteredRecords := filterRecords(records, TextOptions{
		Top:      options.Top,
		CRAPOver: options.CRAPOver,
	})

	return filteredRecords
}

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

func topQualityFindings(report QualityReport, top int) []QualityFinding {
	findings := qualityFindings(report)
	if top > 0 && len(findings) > top {
		return findings[:top]
	}

	return findings
}

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

const htmlReportTemplate = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>DAWGS Metrics Report</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f7f8fb;
      --panel: #ffffff;
      --ink: #17202a;
      --muted: #5e6b78;
      --line: #d9e0e8;
      --accent: #2457c5;
      --accent-soft: #e9efff;
      --critical: #a3262a;
      --critical-soft: #ffe7e8;
      --high: #b45413;
      --high-soft: #fff0dd;
      --watch: #756112;
      --watch-soft: #fff7c9;
      --ok: #1f7a4d;
      --ok-soft: #dff5e9;
      --pending: #54606d;
      --pending-soft: #eef1f4;
      --shadow: 0 8px 24px rgba(25, 36, 51, 0.08);
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-size: 14px;
      line-height: 1.45;
    }

    header {
      background: #101820;
      color: #fff;
      padding: 28px 32px;
    }

    header h1 {
      margin: 0;
      font-size: 28px;
      font-weight: 700;
      letter-spacing: 0;
    }

    header p {
      margin: 8px 0 0;
      color: #cbd4df;
      max-width: 980px;
    }

    main {
      width: min(1500px, calc(100vw - 48px));
      margin: 24px auto 40px;
    }

    .summary-grid {
      display: grid;
      grid-template-columns: repeat(6, minmax(150px, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }

    .metric {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
      min-height: 108px;
      padding: 16px;
    }

    .metric span {
      color: var(--muted);
      display: block;
      font-size: 12px;
      font-weight: 700;
      margin-bottom: 8px;
      text-transform: uppercase;
    }

    .metric strong {
      display: block;
      font-size: 28px;
      font-weight: 750;
      line-height: 1.1;
    }

    .metric small {
      color: var(--muted);
      display: block;
      margin-top: 8px;
    }

    .metric strong.status-text {
      font-size: 24px;
    }

    .status-text.ok {
      color: var(--ok);
    }

    .status-text.watch {
      color: var(--watch);
    }

    .status-text.pending {
      color: var(--pending);
    }

    .band {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
      margin-bottom: 18px;
      overflow: hidden;
    }

    .band-header {
      align-items: center;
      border-bottom: 1px solid var(--line);
      display: flex;
      gap: 14px;
      justify-content: space-between;
      padding: 14px 16px;
    }

    .band-header h2 {
      font-size: 16px;
      line-height: 1.2;
      margin: 0;
    }

    .band-header p {
      color: var(--muted);
      margin: 4px 0 0;
    }

    .severity-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(120px, 1fr));
      gap: 0;
    }

    .severity {
      border-right: 1px solid var(--line);
      padding: 14px 16px;
    }

    .severity:last-child {
      border-right: 0;
    }

    .severity b {
      display: block;
      font-size: 22px;
      line-height: 1.1;
    }

    .severity span {
      color: var(--muted);
      display: block;
      margin-top: 4px;
    }

    .severity.critical b {
      color: var(--critical);
    }

    .severity.high b {
      color: var(--high);
    }

    .severity.watch b {
      color: var(--watch);
    }

    .severity.ok b {
      color: var(--ok);
    }

    .two-column {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 18px;
      margin-bottom: 18px;
    }

    .table-wrap {
      overflow: auto;
      width: 100%;
    }

    table {
      border-collapse: collapse;
      min-width: 100%;
      width: 100%;
    }

    th,
    td {
      border-bottom: 1px solid var(--line);
      padding: 10px 12px;
      text-align: left;
      vertical-align: middle;
      white-space: nowrap;
    }

    th {
      background: #f1f4f8;
      color: #34404c;
      font-size: 12px;
      font-weight: 750;
      position: sticky;
      text-transform: uppercase;
      top: 0;
      z-index: 1;
    }

    tr:hover td {
      background: #f9fbff;
    }

    .numeric {
      font-variant-numeric: tabular-nums;
      text-align: right;
    }

    .function-name {
      font-family: ui-monospace, SFMono-Regular, Consolas, "Liberation Mono", monospace;
      max-width: 440px;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .location {
      color: var(--muted);
      font-family: ui-monospace, SFMono-Regular, Consolas, "Liberation Mono", monospace;
      max-width: 360px;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .pill {
      border-radius: 999px;
      display: inline-flex;
      font-size: 12px;
      font-weight: 750;
      line-height: 1;
      padding: 6px 8px;
    }

    .pill.critical {
      background: var(--critical-soft);
      color: var(--critical);
    }

    .pill.high {
      background: var(--high-soft);
      color: var(--high);
    }

    .pill.watch {
      background: var(--watch-soft);
      color: var(--watch);
    }

    .pill.ok {
      background: var(--ok-soft);
      color: var(--ok);
    }

    .pill.pending {
      background: var(--pending-soft);
      color: var(--pending);
    }

    .coverage {
      align-items: center;
      display: grid;
      gap: 8px;
      grid-template-columns: 60px 120px;
      justify-content: end;
    }

    .bar {
      background: #e7ebf0;
      border-radius: 999px;
      height: 8px;
      overflow: hidden;
      width: 120px;
    }

    .bar-fill {
      background: linear-gradient(90deg, #b84b3f, #e4a039, #2e8f63);
      display: block;
      height: 100%;
    }

    .toolbar {
      align-items: end;
      display: grid;
      gap: 12px;
      grid-template-columns: minmax(240px, 1fr) 130px 130px 150px auto;
      padding: 14px 16px;
    }

    label {
      color: var(--muted);
      display: grid;
      font-size: 12px;
      font-weight: 700;
      gap: 5px;
      text-transform: uppercase;
    }

    input {
      background: #fff;
      border: 1px solid var(--line);
      border-radius: 6px;
      color: var(--ink);
      font: inherit;
      height: 36px;
      min-width: 0;
      padding: 7px 9px;
      text-transform: none;
    }

    .checkbox {
      align-items: center;
      display: flex;
      gap: 8px;
      height: 36px;
      text-transform: none;
    }

    .checkbox input {
      height: 16px;
      width: 16px;
    }

    button {
      background: var(--accent);
      border: 1px solid var(--accent);
      border-radius: 6px;
      color: #fff;
      cursor: pointer;
      font: inherit;
      font-weight: 700;
      height: 36px;
      padding: 0 12px;
    }

    button.secondary {
      background: #fff;
      color: var(--accent);
    }

    .muted {
      color: var(--muted);
    }

    .empty {
      color: var(--muted);
      padding: 18px 16px;
    }

    @media (max-width: 1100px) {
      .summary-grid,
      .two-column {
        grid-template-columns: 1fr 1fr;
      }

      .toolbar {
        grid-template-columns: 1fr 1fr;
      }
    }

    @media (max-width: 720px) {
      header {
        padding: 22px 18px;
      }

      main {
        width: calc(100vw - 24px);
      }

      .summary-grid,
      .severity-grid,
      .two-column,
      .toolbar {
        grid-template-columns: 1fr;
      }

      .severity {
        border-bottom: 1px solid var(--line);
        border-right: 0;
      }

      .severity:last-child {
        border-bottom: 0;
      }
    }
  </style>
</head>
<body>
  <header>
    <h1>DAWGS Metrics Report</h1>
    <p>{{.ReportSummaryComment}}. Coverage is Go statement coverage from {{.Report.CoverProfile}}. Quality signals summarize drift, backend equivalence, invariants, fuzzing, mutation, and benchmark drift.</p>
  </header>

  <main>
    <section class="summary-grid" aria-label="Summary metrics">
      <div class="metric">
        <span>Functions</span>
        <strong>{{.Report.FunctionCount}}</strong>
        <small>Production Go functions analyzed</small>
      </div>
      <div class="metric">
        <span>Average CRAP</span>
        <strong>{{score .Report.AverageCRAP}}</strong>
        <small>Sorted by highest risk first</small>
      </div>
      <div class="metric">
        <span>Average Complexity</span>
        <strong>{{score .Report.AverageComplexity}}</strong>
        <small>Cyclomatic complexity</small>
      </div>
      <div class="metric">
        <span>Statement Coverage</span>
        <strong>{{percent .AverageCoverage}}%</strong>
        <small>{{.CoveredStatements}} / {{.TotalStatements}} statements</small>
      </div>
      <div class="metric">
        <span>Thresholds</span>
        <strong>{{score .CRAPThreshold}}</strong>
        <small>CRAP over threshold, cyclo over {{.ComplexityThreshold}}</small>
      </div>
      <div class="metric">
        <span>Quality Status</span>
        <strong class="status-text {{qualityStatusClass .Report.Quality.Summary.Status}}">{{qualityStatusLabel .Report.Quality.Summary.Status}}</strong>
        <small>{{.Report.Quality.Summary.WatchSignals}} watch, {{.Report.Quality.Summary.PendingSignals}} pending signals</small>
      </div>
    </section>

    <section class="band" aria-label="Severity distribution">
      <div class="band-header">
        <div>
          <h2>Severity Distribution</h2>
          <p>Severity is derived from CRAP score: critical >= 100, high >= 60, watch >= 30.</p>
        </div>
      </div>
      <div class="severity-grid">
        <div class="severity critical"><b>{{.SeverityCounts.Critical}}</b><span>Critical</span></div>
        <div class="severity high"><b>{{.SeverityCounts.High}}</b><span>High</span></div>
        <div class="severity watch"><b>{{.SeverityCounts.Watch}}</b><span>Watch</span></div>
        <div class="severity ok"><b>{{.SeverityCounts.Ok}}</b><span>OK</span></div>
      </div>
    </section>

    <section class="band" aria-label="Quality signals">
      <div class="band-header">
        <div>
          <h2>Quality Signals</h2>
          <p>Report-only safety and correctness signals beyond CRAP and cyclomatic complexity.</p>
        </div>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Signal</th>
              <th>Status</th>
              <th>Summary</th>
              <th>Detail</th>
            </tr>
          </thead>
          <tbody>
            {{range .QualitySignals}}
            <tr>
              <td>{{.Name}}</td>
              <td><span class="pill {{qualityStatusClass .Status}}">{{qualityStatusLabel .Status}}</span></td>
              <td>{{.Summary}}</td>
              <td class="muted">{{.Detail}}</td>
            </tr>
            {{end}}
          </tbody>
        </table>
      </div>
    </section>

    <section class="band" aria-label="Quality findings">
      <div class="band-header">
        <div>
          <h2>Quality Findings</h2>
          <p>Top {{.TopCount}} findings from quality signals, sorted by severity.</p>
        </div>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Severity</th>
              <th>Signal</th>
              <th>ID</th>
              <th>Message</th>
              <th>Location</th>
            </tr>
          </thead>
          <tbody>
            {{range .QualityFindings}}
            <tr>
              <td><span class="pill {{qualitySeverityClass .}}">{{.Severity}}</span></td>
              <td>{{.Signal}}</td>
              <td class="function-name" title="{{.ID}}">{{.ID}}</td>
              <td>{{.Message}}</td>
              <td class="location" title="{{.File}}">{{.File}}</td>
            </tr>
            {{else}}
            <tr><td colspan="5" class="empty">No quality findings.</td></tr>
            {{end}}
          </tbody>
        </table>
      </div>
    </section>

    <section class="two-column">
      <div class="band">
        <div class="band-header">
          <div>
            <h2>Top CRAP Scores</h2>
            <p>Top {{.TopCount}} functions above the configured CRAP threshold.</p>
          </div>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Severity</th>
                <th class="numeric">CRAP</th>
                <th class="numeric">Cyclo</th>
                <th class="numeric">Cov</th>
                <th>Function</th>
              </tr>
            </thead>
            <tbody>
              {{range .TopCRAP}}
              <tr>
                <td><span class="pill {{severityClass .}}">{{severityLabel .}}</span></td>
                <td class="numeric">{{score .CRAP}}</td>
                <td class="numeric">{{.Complexity}}</td>
                <td class="numeric">{{percent .Coverage}}%</td>
                <td class="function-name" title="{{fullName .}}">{{fullName .}}</td>
              </tr>
              {{else}}
              <tr><td colspan="5" class="empty">No functions exceeded the configured CRAP threshold.</td></tr>
              {{end}}
            </tbody>
          </table>
        </div>
      </div>

      <div class="band">
        <div class="band-header">
          <div>
            <h2>Top Cyclomatic Complexity</h2>
            <p>Top {{.TopCount}} functions above the configured complexity threshold.</p>
          </div>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th class="numeric">Cyclo</th>
                <th class="numeric">CRAP</th>
                <th class="numeric">Cov</th>
                <th>Function</th>
              </tr>
            </thead>
            <tbody>
              {{range .TopComplexity}}
              <tr>
                <td class="numeric">{{.Complexity}}</td>
                <td class="numeric">{{score .CRAP}}</td>
                <td class="numeric">{{percent .Coverage}}%</td>
                <td class="function-name" title="{{fullName .}}">{{fullName .}}</td>
              </tr>
              {{else}}
              <tr><td colspan="4" class="empty">No functions exceeded the configured complexity threshold.</td></tr>
              {{end}}
            </tbody>
          </table>
        </div>
      </div>
    </section>

    <section class="band">
      <div class="band-header">
        <div>
          <h2>All Functions</h2>
          <p><span id="visible-count">{{len .Records}}</span> visible of {{len .Records}} functions.</p>
        </div>
        <button class="secondary" id="reset" type="button">Reset Filters</button>
      </div>

      <div class="toolbar">
        <label>
          Search
          <input id="search" type="search" placeholder="package, function, or file">
        </label>
        <label>
          Min CRAP
          <input id="min-crap" type="number" min="0" step="1" value="0">
        </label>
        <label>
          Min Cyclo
          <input id="min-cyclo" type="number" min="0" step="1" value="0">
        </label>
        <label class="checkbox">
          <input id="hide-covered" type="checkbox">
          Hide 100% covered
        </label>
        <button id="sort-crap" type="button">Sort By CRAP</button>
      </div>

      <div class="table-wrap">
        <table id="records">
          <thead>
            <tr>
              <th>Severity</th>
              <th class="numeric"><button class="secondary" type="button" data-sort="crap">CRAP</button></th>
              <th class="numeric"><button class="secondary" type="button" data-sort="complexity">Cyclo</button></th>
              <th class="numeric"><button class="secondary" type="button" data-sort="coverage">Coverage</button></th>
              <th class="numeric">Statements</th>
              <th>Function</th>
              <th>Location</th>
            </tr>
          </thead>
          <tbody>
            {{range .Records}}
            <tr data-metric-row data-search="{{fullName .}} {{location .}}" data-crap="{{score .CRAP}}" data-complexity="{{.Complexity}}" data-coverage="{{percent .Coverage}}">
              <td><span class="pill {{severityClass .}}">{{severityLabel .}}</span></td>
              <td class="numeric">{{score .CRAP}}</td>
              <td class="numeric">{{.Complexity}}</td>
              <td class="numeric">
                <div class="coverage">
                  <span>{{percent .Coverage}}%</span>
                  <span class="bar"><span class="bar-fill" style="{{coverageWidth .Coverage}}"></span></span>
                </div>
              </td>
              <td class="numeric">{{statements .}}</td>
              <td class="function-name" title="{{fullName .}}">{{fullName .}}</td>
              <td class="location" title="{{location .}}">{{location .}}</td>
            </tr>
            {{end}}
          </tbody>
        </table>
      </div>
    </section>
  </main>

  <script>
    (function () {
      var rows = Array.prototype.slice.call(document.querySelectorAll("[data-metric-row]"));
      var tbody = document.querySelector("#records tbody");
      var search = document.querySelector("#search");
      var minCrap = document.querySelector("#min-crap");
      var minCyclo = document.querySelector("#min-cyclo");
      var hideCovered = document.querySelector("#hide-covered");
      var visibleCount = document.querySelector("#visible-count");
      var reset = document.querySelector("#reset");
      var descending = { crap: true, complexity: true, coverage: false };

      function numberValue(row, key) {
        return Number(row.getAttribute("data-" + key)) || 0;
      }

      function applyFilters() {
        var query = search.value.trim().toLowerCase();
        var crapFloor = Number(minCrap.value) || 0;
        var cycloFloor = Number(minCyclo.value) || 0;
        var visible = 0;

        rows.forEach(function (row) {
          var text = row.getAttribute("data-search").toLowerCase();
          var matches = (!query || text.indexOf(query) !== -1) &&
            numberValue(row, "crap") >= crapFloor &&
            numberValue(row, "complexity") >= cycloFloor &&
            (!hideCovered.checked || numberValue(row, "coverage") < 100);

          row.hidden = !matches;
          if (matches) {
            visible += 1;
          }
        });

        visibleCount.textContent = String(visible);
      }

      function sortRows(key) {
        var multiplier = descending[key] ? -1 : 1;
        rows.sort(function (left, right) {
          var delta = numberValue(left, key) - numberValue(right, key);
          if (delta === 0) {
            return left.getAttribute("data-search").localeCompare(right.getAttribute("data-search"));
          }
          return delta * multiplier;
        });

        rows.forEach(function (row) {
          tbody.appendChild(row);
        });
        descending[key] = !descending[key];
        applyFilters();
      }

      [search, minCrap, minCyclo, hideCovered].forEach(function (control) {
        control.addEventListener("input", applyFilters);
        control.addEventListener("change", applyFilters);
      });

      document.querySelector("#sort-crap").addEventListener("click", function () {
        sortRows("crap");
      });

      Array.prototype.forEach.call(document.querySelectorAll("[data-sort]"), function (button) {
        button.addEventListener("click", function () {
          sortRows(button.getAttribute("data-sort"));
        });
      });

      reset.addEventListener("click", function () {
        search.value = "";
        minCrap.value = "0";
        minCyclo.value = "0";
        hideCovered.checked = false;
        applyFilters();
      });

      applyFilters();
    }());
  </script>
</body>
</html>
`
