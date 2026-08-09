// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

const resourceGateVersion = 1

type ResourceGateReport struct {
	Version int                `json:"version"`
	Passed  bool               `json:"passed"`
	Cases   []ResourceGateCase `json:"cases"`
}

type ResourceGateCase struct {
	Dataset              string   `json:"dataset"`
	Name                 string   `json:"name"`
	Reference            string   `json:"reference,omitempty"`
	Tier                 string   `json:"tier"`
	Architecture         string   `json:"architecture,omitempty"`
	FallbackArchitecture string   `json:"fallback_architecture,omitempty"`
	Passed               bool     `json:"passed"`
	Reasons              []string `json:"reasons,omitempty"`
}

func createResourceGateReport(artifact, output string) (bool, error) {
	records, err := readJSONLFile(artifact)
	if err != nil {
		return false, err
	}
	report := ResourceGateReport{Version: resourceGateVersion, Passed: true}
	for _, record := range records {
		if record.ExecutionMode != ModePostgresSQL || record.Shape.FixtureTier == "stress" {
			continue
		}
		gateCase := ResourceGateCase{Dataset: record.Dataset, Name: record.Name, Tier: record.Shape.FixtureTier, Passed: true}
		if gateCase.Tier == "" {
			gateCase.Tier = "legacy"
		}
		gateCase.Architecture = appliedShortestArchitecture(record)
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
		if workspaceCandidate && record.PostgresMetrics != nil {
			appendWorkspaceResourceReasons(&gateCase, record.PostgresMetrics)
		} else if portableCandidate && record.PostgresMetrics != nil {
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
				Dataset: record.Dataset, Name: record.Name, Reference: reference.Name,
				Tier: gateCase.Tier, Architecture: reference.Architecture, Passed: true,
			}
			if reference.Architecture != "SP-S0" && reference.PostgresMetrics != nil {
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
	return report.Passed, err
}

func appendWorkspaceResourceReasons(gateCase *ResourceGateCase, metrics *PostgresPlanMetrics) {
	if metrics.Buffers.TempRead != 0 || metrics.Buffers.TempWritten != 0 {
		gateCase.Reasons = append(gateCase.Reasons, "compact workspace candidate spilled to executor temporary storage")
	}
	if metrics.WALRecords != 0 || metrics.WALBytes != 0 {
		gateCase.Reasons = append(gateCase.Reasons, "non-mutating compact workspace candidate emitted WAL")
	}
}

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

func appliedShortestArchitecture(record CaseResult) string {
	if record.Optimization == nil {
		return ""
	}
	for _, outcome := range record.Optimization.TargetOutcomes {
		if outcome.Family == "SP" || outcome.Family == "ASP" {
			if outcome.Applied != "" {
				return outcome.Applied
			}
			return outcome.Selected
		}
	}
	return ""
}
