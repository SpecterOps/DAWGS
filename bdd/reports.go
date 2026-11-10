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
	"embed"
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
	// bddReportDir is the shared output directory for compatibility artifacts.
	bddReportDir = "../.coverage"
	// bddJSONReportPath is the Cucumber JSON output consumed by aggregation.
	bddJSONReportPath = bddReportDir + "/opencypher-tck-results.json"
	// bddHTMLReportPath is the standalone compatibility summary output.
	bddHTMLReportPath = bddReportDir + "/opencypher-tck-report.html"
)

// cucumberFeature decodes one feature and its scenarios from the Cucumber JSON report.
type cucumberFeature struct {
	// URI locates the feature for folder grouping.
	URI string `json:"uri"`
	// Name is the feature title supplied by Cucumber.
	Name string `json:"name"`
	// Elements contains the scenarios executed for this feature.
	Elements []cucumberScenario `json:"elements"`
}

// cucumberScenario decodes scenario tags and ordered step outcomes.
type cucumberScenario struct {
	// Name is the scenario title displayed in the report.
	Name string `json:"name"`
	// Tags determines whether the scenario is enforced or report-only.
	Tags []cucumberTag `json:"tags"`
	// Steps contains the executed step outcomes in scenario order.
	Steps []cucumberStep `json:"steps"`
}

// cucumberTag identifies a Cucumber scenario tag.
type cucumberTag struct {
	// Name includes the tag's leading @ marker.
	Name string `json:"name"`
}

// cucumberStep associates an executed step with its result.
type cucumberStep struct {
	// Name is the step text supplied by Cucumber.
	Name string `json:"name"`
	// Result contains the execution status and optional failure detail.
	Result cucumberResult `json:"result"`
}

// cucumberResult decodes the status and failure text of a step.
type cucumberResult struct {
	// Status is the Cucumber execution outcome; only passed counts as successful.
	Status string `json:"status"`
	// Error contains failure detail when supplied by the runner.
	Error string `json:"error_message"`
}

// scenarioFailure identifies a failed scenario for gate diagnostics.
type scenarioFailure struct {
	// Feature identifies the feature containing the failed scenario.
	Feature string
	// Name identifies the failed scenario within the feature.
	Name string
	// Error contains the first failing step's detail or status.
	Error string
}

// scenarioResult holds the rendered status and enforcement decision for one scenario.
type scenarioResult struct {
	// Name is the displayed scenario title.
	Name string
	// Status is passed only when every step succeeded.
	Status string
	// Error contains the first failing step's detail, or is empty on success.
	Error string
	// Enforced is false for scenarios tagged @report-only.
	Enforced bool
}

// featureResult aggregates scenario counts and details within a feature.
type featureResult struct {
	// Name is the feature path relative to its report folder.
	Name string
	// Total counts all scenarios, including report-only scenarios.
	Total int
	// Passed counts scenarios whose steps all passed.
	Passed int
	// Failed counts scenarios with a missing or unsuccessful step.
	Failed int
	// Scenarios contains scenario details in input order.
	Scenarios []scenarioResult
}

// folderResult groups feature reports under their top-level feature folder.
type folderResult struct {
	// Name is the grouping folder or unclassified for unknown locations.
	Name string
	// Total counts all scenarios across the folder's features.
	Total int
	// Passed counts successful scenarios across the folder.
	Passed int
	// Failed counts unsuccessful scenarios across the folder.
	Failed int
	// Features contains feature summaries sorted by name.
	Features []featureResult
}

// bddReport contains compatibility totals and the subset that controls the enforcement gate.
type bddReport struct {
	// GeneratedAt is the UTC report generation time formatted as RFC3339.
	GeneratedAt string
	// Total counts all scenarios, including report-only scenarios.
	Total int
	// Passed counts all successful scenarios.
	Passed int
	// Failed counts all unsuccessful scenarios.
	Failed int
	// Enforced counts scenarios without the @report-only tag.
	Enforced int
	// EnforcedFailed counts failures among enforced scenarios.
	EnforcedFailed int
	// Folders contains folder summaries sorted by name.
	Folders []folderResult
}

// bddReportAssets contains the standalone compatibility report template.
//
//go:embed templates/bdd-report.html
var bddReportAssets embed.FS

// bddReportTemplate is parsed at initialization so an invalid embedded report fails early.
var bddReportTemplate = template.Must(template.New("bdd-report.html").ParseFS(bddReportAssets, "templates/bdd-report.html"))

// writeBDDHTMLReport decodes Cucumber results and writes an escaped standalone report.
// A close failure is returned when rendering itself succeeded.
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

// aggregateBDDReport counts all scenario outcomes while tracking enforced failures separately.
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

// checkBDDGate rejects empty reports, enforced failures, and runner failures not explained by report-only scenarios.
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

// topLevelFolderFromURI extracts the first folder below a features path segment.
func topLevelFolderFromURI(uri string) (string, bool) {
	parts := strings.Split(filepath.ToSlash(uri), "/")
	for i, part := range parts {
		if strings.ToLower(part) == "features" && i+1 < len(parts) && parts[i+1] != "" {
			return parts[i+1], true
		}
	}
	return "", false
}

// featureNameFromURI displays the feature path below its grouping folder, falling back to the basename.
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

// firstScenarioFailure returns the first unsuccessful step detail or status, treating an empty scenario as a failure.
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
