// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
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

package testutil

import (
	"errors"
	"fmt"
	"strings"

	"github.com/specterops/dawgs/opengraph"
)

const ShortestPathScaleV2Dataset = ShortestPathScaleDataset + "_v2"

type ShortestPathScaleV2Config struct {
	Depth                    int
	ForwardRootFanOut        int
	ReverseRootFanIn         int
	IntermediateFanOut       int
	IntermediateReverseFanIn int
	FanInLevel               int
	ParallelKindCount        int
	ParallelTargetCount      int
	DiamondWidth             int
	DisconnectedWidth        int
	PropertyPayloadSize      int
	AddCycle                 bool
	AddSelfLoop              bool
}

func ValidateShortestPathScaleV2Config(config ShortestPathScaleV2Config) error {
	values := []int{
		config.Depth, config.ForwardRootFanOut, config.ReverseRootFanIn,
		config.IntermediateFanOut, config.IntermediateReverseFanIn,
		config.FanInLevel, config.ParallelKindCount, config.ParallelTargetCount,
		config.DiamondWidth, config.DisconnectedWidth, config.PropertyPayloadSize,
	}
	for _, value := range values {
		if value < 0 {
			return errors.New("shortest-path v2 configuration values must not be negative")
		}
	}
	if config.Depth > 64 {
		return errors.New("shortest-path v2 depth must not exceed 64")
	}
	if config.IntermediateFanOut == 0 && config.IntermediateReverseFanIn == 0 {
		if config.FanInLevel != 0 {
			return errors.New("shortest-path v2 fan-in level must be zero without intermediate fanout or fan-in")
		}
	} else if config.FanInLevel < 1 || config.FanInLevel >= config.Depth {
		return errors.New("shortest-path v2 fan-in level must identify an intermediate path level")
	}
	if (config.ParallelKindCount == 0) != (config.ParallelTargetCount == 0) {
		return errors.New("shortest-path v2 parallel kind and target counts must both be zero or both be positive")
	}
	return nil
}

// NewShortestPathScaleV2Fixture builds independent deterministic anchors for
// a primary path, hidden fan-in/fan-out, parallel kinds, diamonds, cycles,
// self-loops, and disconnected exhaustion. Every relationship has a stable
// logical_key so backend physical IDs are never required for path comparison.
func NewShortestPathScaleV2Fixture(config ShortestPathScaleV2Config) *opengraph.Graph {
	if err := ValidateShortestPathScaleV2Config(config); err != nil {
		panic(err)
	}

	payload := strings.Repeat("x", config.PropertyPayloadSize)
	fixture := &opengraph.Graph{}
	addNode := func(id string, properties map[string]any) {
		if properties == nil {
			properties = map[string]any{}
		}
		if payload != "" {
			properties["payload"] = payload
		}
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{
			ID:         id,
			Kinds:      []string{"ShortestNode"},
			Properties: properties,
		})
	}
	addEdge := func(start, end, kind, key string) {
		properties := map[string]any{"logical_key": key}
		if payload != "" {
			properties["payload"] = payload
		}
		fixture.Edges = append(fixture.Edges, opengraph.Edge{
			StartID:    start,
			EndID:      end,
			Kind:       kind,
			Properties: properties,
		})
	}

	addNode("sp-v2-start", map[string]any{"role": "start", "level": 0})
	addNode("sp-v2-end", map[string]any{"role": "end", "level": config.Depth})
	pathNodes := []string{"sp-v2-start"}
	for level := 1; level < config.Depth; level++ {
		id := fmt.Sprintf("sp-v2-linear-%02d", level)
		addNode(id, map[string]any{"role": "path", "level": level})
		pathNodes = append(pathNodes, id)
	}
	if config.Depth > 0 {
		pathNodes = append(pathNodes, "sp-v2-end")
		for level := 1; level < len(pathNodes); level++ {
			addEdge(pathNodes[level-1], pathNodes[level], "Traverse", fmt.Sprintf("primary-%02d", level))
		}
	}
	inboundPathNodes := []string{"sp-v2-inbound-end"}
	addNode("sp-v2-inbound-end", map[string]any{"role": "inbound_terminal", "level": config.Depth})
	addNode("sp-v2-inbound-root", map[string]any{"role": "inbound_root", "level": 0})
	for level := config.Depth - 1; level >= 1; level-- {
		id := fmt.Sprintf("sp-v2-inbound-linear-%02d", level)
		addNode(id, map[string]any{"role": "inbound_path", "level": level})
		inboundPathNodes = append(inboundPathNodes, id)
	}
	if config.Depth > 0 {
		inboundPathNodes = append(inboundPathNodes, "sp-v2-inbound-root")
		for level := 1; level < len(inboundPathNodes); level++ {
			addEdge(inboundPathNodes[level-1], inboundPathNodes[level], "Traverse", fmt.Sprintf("inbound-primary-%02d", level))
		}
	}

	for idx := range config.ForwardRootFanOut {
		id := fmt.Sprintf("sp-v2-root-out-%06d", idx)
		addNode(id, map[string]any{"role": "root_forward_dead_end"})
		addEdge("sp-v2-start", id, "Traverse", fmt.Sprintf("root-out-%06d", idx))
	}
	for idx := range config.ReverseRootFanIn {
		id := fmt.Sprintf("sp-v2-root-in-%06d", idx)
		addNode(id, map[string]any{"role": "root_reverse_dead_end"})
		addEdge(id, "sp-v2-inbound-root", "Traverse", fmt.Sprintf("root-in-%06d", idx))
	}
	if config.FanInLevel > 0 {
		boundary := pathNodes[config.FanInLevel]
		for idx := range config.IntermediateFanOut {
			id := fmt.Sprintf("sp-v2-level-%02d-out-%06d", config.FanInLevel, idx)
			addNode(id, map[string]any{"role": "intermediate_forward_dead_end", "level": config.FanInLevel + 1})
			addEdge(boundary, id, "Traverse", fmt.Sprintf("level-%02d-out-%06d", config.FanInLevel, idx))
		}
		for idx := range config.IntermediateReverseFanIn {
			id := fmt.Sprintf("sp-v2-level-%02d-in-%06d", config.FanInLevel, idx)
			addNode(id, map[string]any{"role": "intermediate_reverse_dead_end", "level": config.FanInLevel - 1})
			inboundBoundary := fmt.Sprintf("sp-v2-inbound-linear-%02d", config.FanInLevel)
			addEdge(id, inboundBoundary, "Traverse", fmt.Sprintf("level-%02d-in-%06d", config.FanInLevel, idx))
		}
	}

	if config.ParallelKindCount > 0 {
		addNode("sp-v2-parallel-start", map[string]any{"role": "parallel_start"})
		for target := range config.ParallelTargetCount {
			targetID := fmt.Sprintf("sp-v2-parallel-target-%06d", target)
			addNode(targetID, map[string]any{"role": "parallel_target"})
			for kind := range config.ParallelKindCount {
				addEdge("sp-v2-parallel-start", targetID, fmt.Sprintf("ParallelKind%02d", kind), fmt.Sprintf("parallel-k%02d-t%06d", kind, target))
			}
		}
	}

	if config.DiamondWidth > 0 {
		addNode("sp-v2-diamond-start", map[string]any{"role": "diamond_start"})
		addNode("sp-v2-diamond-end", map[string]any{"role": "diamond_end"})
		for idx := range config.DiamondWidth {
			middle := fmt.Sprintf("sp-v2-diamond-%06d", idx)
			addNode(middle, map[string]any{"role": "diamond_middle"})
			addEdge("sp-v2-diamond-start", middle, "DiamondTraverse", fmt.Sprintf("diamond-%06d-a", idx))
			addEdge(middle, "sp-v2-diamond-end", "DiamondTraverse", fmt.Sprintf("diamond-%06d-b", idx))
		}
	}

	addNode("sp-v2-disconnected-start", map[string]any{"role": "disconnected_start"})
	addNode("sp-v2-disconnected-end", map[string]any{"role": "disconnected_end"})
	previous := "sp-v2-disconnected-start"
	for idx := range config.DisconnectedWidth {
		next := fmt.Sprintf("sp-v2-disconnected-%06d", idx)
		addNode(next, map[string]any{"role": "disconnected_state"})
		addEdge(previous, next, "Traverse", fmt.Sprintf("disconnected-%06d", idx))
		previous = next
	}
	if config.AddCycle {
		addNode("sp-v2-cycle-a", map[string]any{"role": "cycle"})
		addNode("sp-v2-cycle-b", map[string]any{"role": "cycle"})
		addEdge("sp-v2-start", "sp-v2-cycle-a", "Traverse", "cycle-entry")
		addEdge("sp-v2-cycle-a", "sp-v2-cycle-b", "Traverse", "cycle-a-b")
		addEdge("sp-v2-cycle-b", "sp-v2-cycle-a", "Traverse", "cycle-b-a")
	}
	if config.AddSelfLoop {
		addNode("sp-v2-self-loop", map[string]any{"role": "self_loop"})
		addEdge("sp-v2-start", "sp-v2-self-loop", "Traverse", "self-loop-entry")
		addEdge("sp-v2-self-loop", "sp-v2-self-loop", "Traverse", "self-loop")
	}

	return fixture
}
