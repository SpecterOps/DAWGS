// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"sort"
	"time"
)

const (
	spI2TailQualificationSchemaV2 = "sp-i2-tail-qualification-v2"
	spI2TailFreezeSchemaV2        = "sp-i2-tail-freeze-v2"
	spI2TailAASchemaV2            = "sp-i2-tail-aa-v2"
)

// SPI2TailQualificationV2 is the generation-specific report schema. It is
// intentionally separate from the archived V1 reporter.
type SPI2TailQualificationV2 struct {
	Schema                    string                        `json:"schema"`
	Generation                string                        `json:"generation"`
	ProtocolDeclarationSHA256 string                        `json:"protocol_declaration_sha256"`
	Executor                  string                        `json:"executor"`
	Policy                    string                        `json:"policy"`
	Selector                  string                        `json:"selector"`
	Baseline                  string                        `json:"baseline"`
	StatisticalImplementation string                        `json:"statistical_implementation"`
	Confidence                float64                       `json:"confidence"`
	BootstrapReplicates       int                           `json:"bootstrap_replicates"`
	Rounds                    int                           `json:"rounds"`
	SamplesPerRound           int                           `json:"samples_per_round"`
	MultiplicityRule          string                        `json:"multiplicity_rule"`
	Cases                     []SPI2TailQualificationCaseV2 `json:"cases"`
	Passed                    bool                          `json:"passed"`
}

// SPI2TailQualificationCaseV2 contains signs explicitly: P95Change is
// candidate minus baseline, while MedianSaving is baseline minus candidate.
type SPI2TailQualificationCaseV2 struct {
	Dataset             string           `json:"dataset"`
	Name                string           `json:"name"`
	Role                string           `json:"role"`
	MedianRatio         RatioInterval    `json:"median_ratio"`
	MedianSaving        DurationInterval `json:"median_saving"`
	P95Ratio            RatioInterval    `json:"p95_ratio"`
	P95Change           DurationInterval `json:"p95_change"`
	WorstMedianOverhead time.Duration    `json:"worst_median_overhead"`
	WorstP95Overhead    time.Duration    `json:"worst_p95_overhead"`
	SemanticPassed      bool             `json:"semantic_passed"`
	ReceiptPassed       bool             `json:"receipt_passed"`
	ResourcePassed      bool             `json:"resource_passed"`
	SchedulePassed      bool             `json:"schedule_passed"`
	Passed              bool             `json:"passed"`
	Reasons             []string         `json:"reasons,omitempty"`
}

// SPI2TailCaseInputV2 supplies already validated native samples and the
// conjunctive non-timing gates. Artifact readers must establish chronology and
// identity before constructing this value.
type SPI2TailCaseInputV2 struct {
	Dataset        string
	Name           string
	Role           string
	Baseline       roundSamples
	Candidate      roundSamples
	SemanticPassed bool
	ReceiptPassed  bool
	ResourcePassed bool
	SchedulePassed bool
}

func buildSPI2TailQualificationV2(protocol spI2ProtocolV2, protocolSHA256 string, inputs []SPI2TailCaseInputV2) (SPI2TailQualificationV2, error) {
	if err := validateSPI2ProtocolV2(protocol); err != nil {
		return SPI2TailQualificationV2{}, err
	}
	if !lowercaseSHA256(protocolSHA256) {
		return SPI2TailQualificationV2{}, fmt.Errorf("SP-I2 V2 report requires the exact protocol declaration SHA-256")
	}
	if len(inputs) == 0 {
		return SPI2TailQualificationV2{}, fmt.Errorf("SP-I2 V2 report requires at least one declared case")
	}
	sort.Slice(inputs, func(left, right int) bool {
		if inputs[left].Dataset == inputs[right].Dataset {
			return inputs[left].Name < inputs[right].Name
		}
		return inputs[left].Dataset < inputs[right].Dataset
	})
	report := SPI2TailQualificationV2{
		Schema:                    spI2TailQualificationSchemaV2,
		Generation:                protocol.Generation,
		ProtocolDeclarationSHA256: protocolSHA256,
		Executor:                  protocol.Identities.Executor,
		Policy:                    protocol.Identities.Policy,
		Selector:                  protocol.Identities.Selector,
		Baseline:                  protocol.Identities.FallbackExecutor,
		StatisticalImplementation: protocol.Identities.StatisticalImplementation,
		Confidence:                protocol.Design.ConfidenceLevel,
		BootstrapReplicates:       protocol.Design.BootstrapReplicates,
		Rounds:                    protocol.Design.Rounds,
		SamplesPerRound:           protocol.Design.TimedSamplesPerRound,
		MultiplicityRule:          protocol.MultiplicityRule,
		Passed:                    true,
	}
	seen := map[string]struct{}{}
	for _, input := range inputs {
		key := input.Dataset + "\x00" + input.Name
		if input.Dataset == "" || input.Name == "" {
			return SPI2TailQualificationV2{}, fmt.Errorf("SP-I2 V2 case identity is incomplete")
		}
		if _, duplicate := seen[key]; duplicate {
			return SPI2TailQualificationV2{}, fmt.Errorf("SP-I2 V2 case %s/%s is duplicated", input.Dataset, input.Name)
		}
		seen[key] = struct{}{}
		if input.Role != "adverse_control" && input.Role != "efficacy_target" {
			return SPI2TailQualificationV2{}, fmt.Errorf("SP-I2 V2 case %s/%s has invalid preregistered role %q", input.Dataset, input.Name, input.Role)
		}
		rounds, err := validateSPI2HierarchicalInputs(input.Baseline, input.Candidate, 0.95, protocol.Design.ConfidenceLevel, protocol.Design.BootstrapReplicates)
		if err != nil {
			return SPI2TailQualificationV2{}, fmt.Errorf("SP-I2 V2 case %s/%s: %w", input.Dataset, input.Name, err)
		}
		if len(rounds) != protocol.Design.Rounds {
			return SPI2TailQualificationV2{}, fmt.Errorf("SP-I2 V2 case %s/%s requires exactly %d rounds", input.Dataset, input.Name, protocol.Design.Rounds)
		}
		for _, round := range rounds {
			if len(input.Baseline[round]) != protocol.Design.TimedSamplesPerRound {
				return SPI2TailQualificationV2{}, fmt.Errorf("SP-I2 V2 case %s/%s round %d requires exactly %d samples per arm", input.Dataset, input.Name, round, protocol.Design.TimedSamplesPerRound)
			}
		}
		medianRatio, medianSaving, err := bootstrapSPI2RoundMedianV2(input.Baseline, input.Candidate, input.Dataset, input.Name, "median", protocol.Design.ConfidenceLevel, protocol.Design.BootstrapReplicates)
		if err != nil {
			return SPI2TailQualificationV2{}, err
		}
		p95, err := bootstrapSPI2HierarchicalTailV2(input.Baseline, input.Candidate, input.Dataset, input.Name, "p95", 0.95, protocol.Design.ConfidenceLevel, protocol.Design.BootstrapReplicates)
		if err != nil {
			return SPI2TailQualificationV2{}, err
		}
		entry := SPI2TailQualificationCaseV2{
			Dataset: input.Dataset, Name: input.Name, Role: input.Role,
			MedianRatio: medianRatio, MedianSaving: medianSaving, P95Ratio: p95.Ratio, P95Change: p95.Change,
			WorstMedianOverhead: -medianSaving.Lower, WorstP95Overhead: p95.Change.Upper,
			SemanticPassed: input.SemanticPassed, ReceiptPassed: input.ReceiptPassed,
			ResourcePassed: input.ResourcePassed, SchedulePassed: input.SchedulePassed, Passed: true,
		}
		if input.Role == "efficacy_target" {
			if medianRatio.Upper > protocol.Gates.TargetMedianRatioUpper && medianSaving.Lower < time.Duration(protocol.Gates.TargetMedianSavingLowerUS)*time.Microsecond {
				entry.Reasons = append(entry.Reasons, "median materiality gate failed")
			}
		} else {
			if medianRatio.Upper > protocol.Gates.ControlMedianRatioUpper && entry.WorstMedianOverhead > time.Duration(protocol.Gates.ControlMedianOverheadUpperUS)*time.Microsecond {
				entry.Reasons = append(entry.Reasons, "adverse-control median containment gate failed")
			}
			if entry.WorstP95Overhead > time.Duration(protocol.Gates.ControlP95OverheadUpperUS)*time.Microsecond {
				entry.Reasons = append(entry.Reasons, "adverse-control absolute p95 containment gate failed")
			}
		}
		if p95.Ratio.Upper > protocol.Gates.P95RatioUpper {
			entry.Reasons = append(entry.Reasons, "relative p95 containment gate failed")
		}
		if !entry.SemanticPassed || !entry.ReceiptPassed || !entry.ResourcePassed || !entry.SchedulePassed {
			entry.Reasons = append(entry.Reasons, "one or more non-timing gates failed")
		}
		entry.Passed = len(entry.Reasons) == 0
		report.Passed = report.Passed && entry.Passed
		report.Cases = append(report.Cases, entry)
	}
	return report, nil
}

// SPI2TailFreezeV2 binds discovery to the exact protocol and raw native
// artifacts. Creation is permitted only from a passing V2 report.
type SPI2TailFreezeV2 struct {
	Schema                    string            `json:"schema"`
	Generation                string            `json:"generation"`
	ProtocolDeclarationSHA256 string            `json:"protocol_declaration_sha256"`
	QualificationReportSHA256 string            `json:"qualification_report_sha256"`
	RawArtifactSHA256         map[string]string `json:"raw_artifact_sha256"`
}
