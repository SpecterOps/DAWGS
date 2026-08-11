// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// resourceGateVersion identifies the serialized schema revision for resource gate.
const resourceGateVersion = 1

// ResourceGateReport reports whether production and reference plan resources remain within their allowed envelopes.
type ResourceGateReport struct {
	// Version identifies the serialized schema revision.
	Version int `json:"version"`
	// Passed reports whether every required gate condition succeeded.
	Passed bool `json:"passed"`
	// Cases contains resource-envelope decisions for each evaluated production or reference executor.
	Cases []ResourceGateCase `json:"cases"`
}

// ResourceGateCase attributes resource-gate failures to one production or reference executor architecture.
type ResourceGateCase struct {
	// Dataset identifies the fixture dataset.
	Dataset string `json:"dataset"`
	// Name identifies the case or record within its dataset.
	Name string `json:"name"`
	// Reference identifies the reference arm evaluated by the resource gate.
	Reference string `json:"reference,omitempty"`
	// Tier identifies the resource envelope applied to the case.
	Tier string `json:"tier"`
	// Architecture identifies the executor architecture.
	Architecture string `json:"architecture,omitempty"`
	// FallbackArchitecture identifies the executor architecture used after fallback.
	FallbackArchitecture string `json:"fallback_architecture,omitempty"`
	// Passed reports whether every required gate condition succeeded.
	Passed bool `json:"passed"`
	// Reasons lists explanations for the reported disposition.
	Reasons []string `json:"reasons,omitempty"`
}

// createResourceGateReport evaluates production and reference plan metrics against resource ceilings and writes the report.
func createResourceGateReport(artifact, output string) (bool, error) {
	records, err := readJSONLFile(artifact)
	if err != nil {
		return false, err
	}
	report := ResourceGateReport{
		Version: resourceGateVersion,
		Passed:  true,
	}
	for _, record := range records {
		if record.ExecutionMode != ModePostgresSQL || record.Shape.FixtureTier == "stress" {
			continue
		}
		gateCase := ResourceGateCase{
			Dataset: record.Dataset,
			Name:    record.Name,
			Tier:    record.Shape.FixtureTier,
			Passed:  true,
		}
		if gateCase.Tier == "" {
			gateCase.Tier = "legacy"
		}
		gateCase.Architecture = appliedPostgresArchitecture(record)
		portableCandidate := gateCase.Architecture != "" && gateCase.Architecture != "SP-S0"
		workspaceCandidate := gateCase.Architecture == "ASP-A1-DAG" || gateCase.Architecture == "SP-S4-C-D" || gateCase.Architecture == "SP-S4-C-WE+MAT-M0"
		if gateCase.Architecture == "SP-S0-DIRECT" {
			if loops, found, err := postgresPlanFunctionLoops(record.PostgresPlanJSON, "bidirectional_sp_harness"); err != nil {
				gateCase.Reasons = append(gateCase.Reasons, "direct preflight fallback attribution failed: "+err.Error())
			} else if !found {
				gateCase.Reasons = append(gateCase.Reasons, "direct preflight fallback plan node is missing")
			} else if loops > 0 {
				portableCandidate = false
				gateCase.FallbackArchitecture = "SP-S0"
			}
		}
		if record.Status != StatusOK {
			gateCase.Reasons = append(gateCase.Reasons, "record status is "+record.Status)
		}
		if record.PostgresMetrics == nil {
			gateCase.Reasons = append(gateCase.Reasons, "structured PostgreSQL plan metrics are missing")
		} else if workspaceCandidate {
			appendWorkspaceResourceReasons(&gateCase, record.PostgresMetrics)
		} else if portableCandidate {
			appendPortableResourceReasons(&gateCase, record.PostgresMetrics)
		}
		gateCase.Passed = len(gateCase.Reasons) == 0
		if !gateCase.Passed {
			report.Passed = false
		}
		report.Cases = append(report.Cases, gateCase)
		for _, reference := range record.PostgresReferences {
			if !reference.FullComparator || reference.Architecture == "" {
				continue
			}
			referenceCase := ResourceGateCase{
				Dataset:      record.Dataset,
				Name:         record.Name,
				Reference:    reference.Name,
				Tier:         gateCase.Tier,
				Architecture: reference.Architecture,
				Passed:       true,
			}
			if reference.PostgresMetrics == nil {
				referenceCase.Reasons = append(referenceCase.Reasons, "structured PostgreSQL reference plan metrics are missing")
			} else if reference.Architecture != "SP-S0" {
				appendPortableResourceReasons(&referenceCase, reference.PostgresMetrics)
			}
			referenceCase.Passed = len(referenceCase.Reasons) == 0
			if !referenceCase.Passed {
				report.Passed = false
			}
			report.Cases = append(report.Cases, referenceCase)
		}
	}
	if len(report.Cases) == 0 {
		return false, fmt.Errorf("resource artifact contains no non-stress PostgreSQL cases")
	}
	sort.Slice(report.Cases, func(i, j int) bool {
		if report.Cases[i].Dataset != report.Cases[j].Dataset {
			return report.Cases[i].Dataset < report.Cases[j].Dataset
		}
		if report.Cases[i].Name != report.Cases[j].Name {
			return report.Cases[i].Name < report.Cases[j].Name
		}
		return report.Cases[i].Reference < report.Cases[j].Reference
	})

	var raw []byte
	if raw, err = json.MarshalIndent(report, "", "  "); err != nil {
		return false, err
	}
	if output == "" {
		_, err = os.Stdout.Write(append(raw, '\n'))
	} else {
		err = os.WriteFile(output, append(raw, '\n'), 0o644)
	}
	if err != nil {
		return false, err
	}

	return report.Passed, nil
}

// appendWorkspaceResourceReasons adds failures for excessive executor or session workspace usage.
func appendWorkspaceResourceReasons(gateCase *ResourceGateCase, metrics *PostgresPlanMetrics) {
	if metrics.Buffers.TempRead != 0 || metrics.Buffers.TempWritten != 0 {
		gateCase.Reasons = append(gateCase.Reasons, "compact workspace candidate spilled to executor temporary storage")
	}
	if metrics.WALRecords != 0 || metrics.WALBytes != 0 {
		gateCase.Reasons = append(gateCase.Reasons, "non-mutating compact workspace candidate emitted WAL")
	}
}

// appendPortableResourceReasons adds failures for spill, loops, or cardinality evidence that violates portable limits.
func appendPortableResourceReasons(gateCase *ResourceGateCase, metrics *PostgresPlanMetrics) {
	buffers := metrics.Buffers
	if buffers.TempRead != 0 || buffers.TempWritten != 0 {
		gateCase.Reasons = append(gateCase.Reasons, "portable candidate used temporary buffers")
	}
	if buffers.LocalHit != 0 || buffers.LocalRead != 0 || buffers.LocalDirtied != 0 || buffers.LocalWritten != 0 {
		gateCase.Reasons = append(gateCase.Reasons, "portable candidate used local workspace")
	}
	if metrics.WALRecords != 0 || metrics.WALBytes != 0 {
		gateCase.Reasons = append(gateCase.Reasons, "non-mutating portable candidate emitted WAL")
	}
}

// postgresPlanFunctionLoops sums actual loops for PostgreSQL plan nodes invoking the named function.
func postgresPlanFunctionLoops(raw json.RawMessage, function string) (int64, bool, error) {
	if len(raw) == 0 {
		return 0, false, nil
	}
	var document []map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		return 0, false, err
	}
	if len(document) == 0 {
		return 0, false, nil
	}
	root, ok := document[0]["Plan"].(map[string]any)
	if !ok {
		return 0, false, nil
	}
	var loops int64
	found := false
	var walk func(map[string]any)
	walk = func(node map[string]any) {
		alias, _ := node["Alias"].(string)
		functionName, _ := node["Function Name"].(string)
		if alias == function || functionName == function {
			found = true
			if actualLoops, ok := node["Actual Loops"].(float64); ok {
				loops += int64(actualLoops)
			}
		}
		children, _ := node["Plans"].([]any)
		for _, child := range children {
			if childNode, ok := child.(map[string]any); ok {
				walk(childNode)
			}
		}
	}
	walk(root)
	return loops, found, nil
}

// appliedPostgresArchitecture returns the effective PostgreSQL executor architecture, including fallback attribution.
func appliedPostgresArchitecture(record CaseResult) string {
	if record.Optimization == nil {
		return ""
	}
	for _, outcome := range record.Optimization.TargetOutcomes {
		if outcome.Family == "SP" || outcome.Family == "ASP" || outcome.Family == "fixed_suffix_expansion" || outcome.Family == "fixed_prefix_terminal_expansion" {
			if outcome.Applied != "" {
				return outcome.Applied
			}
			return outcome.Selected
		}
	}
	return ""
}
