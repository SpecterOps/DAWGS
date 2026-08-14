// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"slices"
)

const (
	spI2V2TrainingTag = "sp-i2-distance-v2-training"
	spI2V2HoldoutTag  = "sp-i2-distance-v2-holdout"

	spI2V2TrainingCorpusSHA256      = "b57e77369d9686123a847e24fd4037d7ee4bc4c9b3d6f73b67eca7a956b0493b"
	spI2V2HoldoutCorpusSHA256       = "16a4f8663cdc1c537c99a85571931c3e7d3f9f71500cb1311375aa9930f8c201"
	spI2V2FullCorpusSHA256          = "f057a779bd1587ff08596459de42ef51f7befcc0365a9bf86f894a77e0e06d0e"
	spI2V2TrainingDeclarationSHA256 = "5d704f62c70fea909565ae0541d8a74a925c6cc14587a49a9a6422d5aa077133"
	spI2V2HoldoutDeclarationSHA256  = "009101538c650a213e807189bd45dede5ab6785dd5ac9e93dc3f1ad328b3fcfa"
	spI2V2FullDeclarationSHA256     = "1721f48e724b227e0bf4d9a1e03b0471f10fc23ef7402e47536b664af6b96a69"
	spI2V2TrainingResolvedSHA256    = "75802b0d76034fac1b2c144c125069f8b971180997540b6b2bf46b89523fbacc"
	spI2V2HoldoutResolvedSHA256     = "d08d149fcaf29e91750fde1e1eae1f3b2f2a6819608a073558ee4d6f13d82ce9"
	spI2V2FullResolvedSHA256        = "fa1abc601d60d295add2095c0ff343c47d013165fe23b9a09eeca429254318c2"
)

type spI2V2FormalCase struct {
	dataset string
	name    string
	split   string
	role    string
}

var spI2V2FormalCases = []spI2V2FormalCase{
	{"generated_shortest_paths_v2_d1_o0_r4_fo0_fi0_l0_k0_t0_w0_x1_p0_c0_s0", "GSP-I2-V2-TRAIN-direct-acyclic-shallow", "training", "adverse_control"},
	{"generated_shortest_paths_v2_d7_o0_r0_fo0_fi0_l0_k0_t0_w0_x7_p0_c1_s0", "GSP-I2-V2-TRAIN-direct-cycle-control", "training", "adverse_control"},
	{"generated_shortest_paths_v2_d5_o0_r19_fo0_fi11_l3_k0_t0_w0_x5_p0_c1_s0", "GSP-I2-V2-TRAIN-D02-post-target-cycle", "training", "adverse_control"},
	{"generated_shortest_paths_v2_d4_o0_r37_fo0_fi73_l2_k0_t0_w0_x4_p0_c0_s0", "GSP-I2-V2-TRAIN-D02-hidden-intermediate-fanin", "training", "efficacy_target"},
	{"generated_shortest_paths_v2_d3_o0_r83_fo0_fi41_l2_k0_t0_w0_x3_p0_c0_s0", "GSP-I2-V2-TRAIN-D03-hidden-root-fanin", "training", "efficacy_target"},
	{"generated_shortest_paths_v2_d8_o0_r149_fo0_fi79_l4_k0_t0_w0_x8_p0_c0_s0", "GSP-I2-V2-TRAIN-D08-mixed-fanin", "training", "efficacy_target"},
	{"generated_shortest_paths_v2_d16_o0_r263_fo0_fi521_l8_k0_t0_w0_x16_p0_c0_s0", "GSP-I2-V2-TRAIN-D16-high-fanin", "training", "efficacy_target"},
	{"generated_shortest_paths_v2_d16_o0_r271_fo0_fi527_l8_k0_t0_w0_x17_p0_c1_s0", "GSP-I2-V2-TRAIN-D16-disconnected-cyclic-exhaustion", "training", "efficacy_target"},
	{"generated_shortest_paths_v2_d1_o0_r17_fo0_fi0_l0_k3_t2_w0_x1_p0_c1_s0", "GSP-I2-V2-HOLDOUT-direct-parallel-asymmetric-cycle", "holdout", "adverse_control"},
	{"generated_shortest_paths_v2_d6_o0_r43_fo0_fi29_l3_k0_t0_w0_x6_p0_c1_s1", "GSP-I2-V2-HOLDOUT-D02-longer-competing-cycle", "holdout", "adverse_control"},
	{"generated_shortest_paths_v2_d9_o0_r173_fo0_fi97_l4_k0_t0_w0_x9_p0_c0_s1", "GSP-I2-V2-HOLDOUT-D03-irrelevant-high-fanout", "holdout", "efficacy_target"},
	{"generated_shortest_paths_v2_d11_o0_r197_fo0_fi211_l6_k0_t0_w0_x11_p0_c0_s0", "GSP-I2-V2-HOLDOUT-D11-medium-fanin", "holdout", "efficacy_target"},
	{"generated_shortest_paths_v2_d23_o0_r307_fo0_fi601_l12_k0_t0_w0_x23_p0_c0_s0", "GSP-I2-V2-HOLDOUT-D23-deep-fanin", "holdout", "efficacy_target"},
	{"generated_shortest_paths_v2_d27_o0_r313_fo0_fi607_l14_k0_t0_w0_x29_p0_c1_s1", "GSP-I2-V2-HOLDOUT-D27-disconnected-cycles", "holdout", "efficacy_target"},
}

type spI2V2FormalCohort struct {
	trainingKeys map[performanceKey]string
	holdoutKeys  map[performanceKey]string
}

func canonicalSPI2V2FormalCohort() (spI2V2FormalCohort, error) {
	cohort := spI2V2FormalCohort{trainingKeys: map[performanceKey]string{}, holdoutKeys: map[performanceKey]string{}}
	for _, declaration := range spI2V2FormalCases {
		key := performanceKey{dataset: declaration.dataset, name: declaration.name, backend: ModePostgresSQL}
		if declaration.role != "adverse_control" && declaration.role != "efficacy_target" {
			return spI2V2FormalCohort{}, fmt.Errorf("SP-I2 V2 formal case %s has invalid role", declaration.name)
		}
		switch declaration.split {
		case "training":
			if _, duplicate := cohort.trainingKeys[key]; duplicate {
				return spI2V2FormalCohort{}, fmt.Errorf("SP-I2 V2 formal training case %s is duplicated", declaration.name)
			}
			cohort.trainingKeys[key] = declaration.role
		case "holdout":
			if _, duplicate := cohort.holdoutKeys[key]; duplicate {
				return spI2V2FormalCohort{}, fmt.Errorf("SP-I2 V2 formal holdout case %s is duplicated", declaration.name)
			}
			cohort.holdoutKeys[key] = declaration.role
		default:
			return spI2V2FormalCohort{}, fmt.Errorf("SP-I2 V2 formal case %s has invalid split", declaration.name)
		}
	}
	if len(cohort.trainingKeys) != 8 || len(cohort.holdoutKeys) != 6 {
		return spI2V2FormalCohort{}, fmt.Errorf("SP-I2 V2 formal cohort requires exactly eight training and six holdout cases")
	}
	return cohort, nil
}

func spI2V2FormalProtocolSelection(selectors CorpusSelectors) bool {
	if slices.Contains(selectors.Tags, spI2V2TrainingTag) || slices.Contains(selectors.Tags, spI2V2HoldoutTag) {
		return true
	}
	for _, selected := range selectors.Cases {
		for _, declaration := range spI2V2FormalCases {
			if selected == declaration.name {
				return true
			}
		}
	}
	return false
}

func spI2V2FormalHoldoutSelected(selectors CorpusSelectors) bool {
	if slices.Contains(selectors.Tags, spI2V2HoldoutTag) {
		return true
	}
	for _, selected := range selectors.Cases {
		for _, declaration := range spI2V2FormalCases {
			if declaration.split == "holdout" && selected == declaration.name {
				return true
			}
		}
	}
	return false
}

func selectedCorpusContainsSPI2V2FormalHoldout(corpus ScaleCorpus) bool {
	cohort, err := canonicalSPI2V2FormalCohort()
	if err != nil {
		return true
	}
	for _, testCase := range corpus.Cases {
		if _, found := cohort.holdoutKeys[performanceKey{dataset: testCase.Dataset, name: testCase.Name, backend: ModePostgresSQL}]; found {
			return true
		}
	}
	return false
}
