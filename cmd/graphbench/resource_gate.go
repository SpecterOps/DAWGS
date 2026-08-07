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
	Dataset      string   `json:"dataset"`
	Name         string   `json:"name"`
	Tier         string   `json:"tier"`
	Architecture string   `json:"architecture,omitempty"`
	Passed       bool     `json:"passed"`
	Reasons      []string `json:"reasons,omitempty"`
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
		if record.Status != StatusOK {
			gateCase.Reasons = append(gateCase.Reasons, "record status is "+record.Status)
		}
		if portableCandidate && record.PostgresMetrics != nil {
			buffers := record.PostgresMetrics.Buffers
			if buffers.TempRead != 0 || buffers.TempWritten != 0 {
				gateCase.Reasons = append(gateCase.Reasons, "portable candidate used temporary buffers")
			}
			if buffers.LocalHit != 0 || buffers.LocalRead != 0 || buffers.LocalDirtied != 0 || buffers.LocalWritten != 0 {
				gateCase.Reasons = append(gateCase.Reasons, "portable candidate used local workspace")
			}
			if record.PostgresMetrics.WALRecords != 0 || record.PostgresMetrics.WALBytes != 0 {
				gateCase.Reasons = append(gateCase.Reasons, "read-only portable candidate emitted WAL")
			}
		}
		gateCase.Passed = len(gateCase.Reasons) == 0
		if !gateCase.Passed {
			report.Passed = false
		}
		report.Cases = append(report.Cases, gateCase)
	}
	if len(report.Cases) == 0 {
		return false, fmt.Errorf("resource artifact contains no non-stress PostgreSQL cases")
	}
	sort.Slice(report.Cases, func(i, j int) bool {
		if report.Cases[i].Dataset != report.Cases[j].Dataset {
			return report.Cases[i].Dataset < report.Cases[j].Dataset
		}
		return report.Cases[i].Name < report.Cases[j].Name
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

func appliedShortestArchitecture(record CaseResult) string {
	if record.Optimization == nil {
		return ""
	}
	for _, outcome := range record.Optimization.TargetOutcomes {
		if outcome.Family == "SP" {
			if outcome.Applied != "" {
				return outcome.Applied
			}
			return outcome.Selected
		}
	}
	return ""
}
