// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0
package bdd

import (
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	bddReportDir      = "../.coverage"
	bddJSONReportPath = bddReportDir + "/opencypher-tck-results.json"
	bddHTMLReportPath = bddReportDir + "/opencypher-tck-report.html"
)

type cucumberFeature struct {
	URI      string             `json:"uri"`
	Name     string             `json:"name"`
	Elements []cucumberScenario `json:"elements"`
}

type cucumberScenario struct {
	Name  string         `json:"name"`
	Tags  []cucumberTag  `json:"tags"`
	Steps []cucumberStep `json:"steps"`
}

type cucumberTag struct {
	Name string `json:"name"`
}

type cucumberStep struct {
	Name   string         `json:"name"`
	Result cucumberResult `json:"result"`
}

type cucumberResult struct {
	Status string `json:"status"`
	Error  string `json:"error_message"`
}

type scenarioFailure struct {
	Feature string
	Name    string
	Error   string
}

type scenarioResult struct {
	Name     string
	Status   string
	Error    string
	Enforced bool
}

type featureResult struct {
	Name      string
	Total     int
	Passed    int
	Failed    int
	Scenarios []scenarioResult
}

type folderResult struct {
	Name     string
	Total    int
	Passed   int
	Failed   int
	Features []featureResult
}

type bddReport struct {
	GeneratedAt    string
	Total          int
	Passed         int
	Failed         int
	Enforced       int
	EnforcedFailed int
	Folders        []folderResult
}

var bddReportTemplate = template.Must(template.New("bdd-report").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Cypher Technology Compatibility Kit Report</title>
<style>body{font:16px system-ui,sans-serif;margin:2rem;color:#202124}table{border-collapse:collapse;width:100%;max-width:1000px}th,td{border:1px solid #dadce0;padding:.6rem;text-align:left}th{background:#f1f3f4}.pass{color:#137333}.fail{color:#c5221f}details{margin-top:.75rem}code{white-space:pre-wrap}</style>
</head><body><h1>Cypher Technology Compatibility Kit Report</h1><p>Generated {{.GeneratedAt}}</p>
<p><strong>Total:</strong> {{.Total}} &nbsp; <span class="pass"><strong>Passed:</strong> {{.Passed}}</span> &nbsp; <span class="fail"><strong>Failed:</strong> {{.Failed}}</span></p>
<p><strong>Enforced gate:</strong> {{.Enforced}} scenarios, {{.EnforcedFailed}} failed. Scenarios tagged <code>@report-only</code> are compatibility reports and do not fail the gate.</p>
{{range .Folders}}<details><summary><strong>{{.Name}}</strong> — {{.Total}} scenarios, <span class="pass">{{.Passed}} passed</span>, <span class="fail">{{.Failed}} failed</span></summary>
{{range .Features}}<details><summary>{{.Name}} — {{.Total}} scenarios, <span class="pass">{{.Passed}} passed</span>, <span class="fail">{{.Failed}} failed</span></summary><ul>
{{range .Scenarios}}<li class="{{if eq .Status "passed"}}pass{{else}}fail{{end}}"><strong>{{.Status}}</strong> — {{.Name}} ({{if .Enforced}}enforced{{else}}report-only{{end}}){{if .Error}}<br><code>{{.Error}}</code>{{end}}</li>
{{end}}</ul></details>
{{end}}</details>
{{end}}</body></html>`))

func writeBDDHTMLReport(inputPath, outputPath string) (report bddReport, err error) {
	data, err := os.ReadFile(inputPath)
	if err != nil {
		return bddReport{}, fmt.Errorf("read Cucumber report: %w", err)
	}
	var features []cucumberFeature
	if err := json.Unmarshal(data, &features); err != nil {
		return bddReport{}, fmt.Errorf("parse Cucumber report: %w", err)
	}
	report = aggregateBDDReport(features)
	report.GeneratedAt = time.Now().UTC().Format(time.RFC3339)
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return bddReport{}, fmt.Errorf("create report directory: %w", err)
	}
	file, err := os.Create(outputPath)
	if err != nil {
		return bddReport{}, fmt.Errorf("create HTML report: %w", err)
	}
	defer func() {
		if errClose := file.Close(); errClose != nil && err == nil {
			err = fmt.Errorf("close HTML report: %w", errClose)
		}
	}()
	if err := bddReportTemplate.Execute(file, report); err != nil {
		return bddReport{}, fmt.Errorf("render HTML report: %w", err)
	}
	return report, nil
}

func aggregateBDDReport(features []cucumberFeature) bddReport {
	byFolder := map[string]*folderResult{}
	report := bddReport{}
	for _, feature := range features {
		folder, ok := topLevelFolderFromURI(feature.URI)
		if !ok {
			// Unknown feature locations still participate in the gate.
			folder = "unclassified"
		}
		folderReport := byFolder[folder]
		if folderReport == nil {
			folderReport = &folderResult{Name: folder}
			byFolder[folder] = folderReport
		}
		featureResult := featureResult{Name: featureNameFromURI(feature.URI, folder)}
		for _, scenario := range feature.Elements {
			enforced := true
			for _, tag := range scenario.Tags {
				if tag.Name == "@report-only" {
					enforced = false
				}
			}
			if enforced {
				report.Enforced++
			}
			featureResult.Total++
			folderReport.Total++
			status := "passed"
			failure := firstScenarioFailure(scenario)
			if failure == "" {
				featureResult.Passed++
				folderReport.Passed++
			} else {
				status = "failed"
				featureResult.Failed++
				folderReport.Failed++
				if enforced {
					report.EnforcedFailed++
				}
			}
			featureResult.Scenarios = append(featureResult.Scenarios, scenarioResult{Name: scenario.Name, Status: status, Error: failure, Enforced: enforced})
		}
		folderReport.Features = append(folderReport.Features, featureResult)
	}
	folders := make([]folderResult, 0, len(byFolder))
	for _, folder := range byFolder {
		sort.Slice(folder.Features, func(i, j int) bool { return folder.Features[i].Name < folder.Features[j].Name })
		folders = append(folders, *folder)
	}
	sort.Slice(folders, func(i, j int) bool { return folders[i].Name < folders[j].Name })
	report.Folders = folders
	for _, folder := range folders {
		report.Total += folder.Total
		report.Passed += folder.Passed
		report.Failed += folder.Failed
	}
	return report
}

func checkBDDGate(report bddReport, suiteStatus int) error {
	if suiteStatus != 0 && suiteStatus != 1 {
		return fmt.Errorf("BDD runner failed with status %d", suiteStatus)
	}
	if report.Total == 0 || report.Enforced == 0 {
		return fmt.Errorf("BDD report contains no enforced scenarios")
	}
	if report.EnforcedFailed > 0 {
		return fmt.Errorf("BDD gate failed: %d of %d enforced scenarios failed", report.EnforcedFailed, report.Enforced)
	}
	if (suiteStatus == 0) != (report.Failed == 0) {
		return fmt.Errorf("BDD runner status %d disagrees with report failure count %d", suiteStatus, report.Failed)
	}
	return nil
}

func topLevelFolderFromURI(uri string) (string, bool) {
	parts := strings.Split(filepath.ToSlash(uri), "/")
	for i, part := range parts {
		if strings.ToLower(part) == "features" && i+1 < len(parts) && parts[i+1] != "" {
			return parts[i+1], true
		}
	}
	return "", false
}

func featureNameFromURI(uri, folder string) string {
	parts := strings.Split(filepath.ToSlash(uri), "/")
	for i, part := range parts {
		if part == folder && i+1 < len(parts) {
			return strings.Join(parts[i+1:], "/")
		}
	}
	// capture any features outside of tck and bdd folder
	return filepath.Base(uri)
}

func firstScenarioFailure(scenario cucumberScenario) string {
	if len(scenario.Steps) == 0 {
		return "scenario has no steps"
	}
	for _, step := range scenario.Steps {
		if strings.ToLower(step.Result.Status) != "passed" {
			if step.Result.Error != "" {
				return step.Result.Error
			}
			if step.Result.Status == "" {
				return "step has no result status"
			}
			return step.Result.Status
		}
	}
	return ""
}
