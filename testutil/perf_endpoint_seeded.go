package testutil

import (
	"fmt"
	"strings"

	"github.com/specterops/dawgs/opengraph"
)

// EndpointSeededExpansionScaleDataset identifies the generated endpoint-seeded
// expansion fixture.
const EndpointSeededExpansionScaleDataset = "generated_endpoint_seeded_expansion_v1"

// EndpointSeededExpansionScaleConfig controls the endpoint populations and
// traversal lanes emitted by NewEndpointSeededExpansionScaleFixture.
type EndpointSeededExpansionScaleConfig struct {
	// Depth sets the number of MemberOf hops in each lane.
	Depth int

	// MatchingEndpoints sets the number of terminal groups whose object IDs
	// satisfy the benchmark predicate.
	MatchingEndpoints int

	// OtherEndpoints sets the number of terminal groups that do not satisfy the
	// benchmark predicate.
	OtherEndpoints int

	// MatchingEligibleLanes sets the number of session-backed lanes ending at a
	// matching endpoint.
	MatchingEligibleLanes int

	// OtherEligibleLanes sets the number of session-backed lanes ending at a
	// nonmatching endpoint.
	OtherEligibleLanes int

	// MatchingIneligibleLanes sets the number of lanes without a session edge
	// that nevertheless end at a matching endpoint.
	MatchingIneligibleLanes int

	// ParallelEdges sets the number of MemberOf edges emitted per lane hop.
	ParallelEdges int

	// AddCycle adds a reverse MemberOf edge near the middle of every lane.
	AddCycle bool

	// PropertyPayloadSize sets the length of synthetic payload properties.
	PropertyPayloadSize int
}

// ValidateEndpointSeededExpansionScaleConfig rejects fixture configurations
// that cannot produce a valid or uniquely keyed endpoint-seeded graph.
func ValidateEndpointSeededExpansionScaleConfig(config EndpointSeededExpansionScaleConfig) error {
	if config.Depth < 1 || config.Depth > 64 {
		return fmt.Errorf("depth must be between 1 and 64")
	}
	if config.MatchingEndpoints < 1 || config.OtherEndpoints < 0 || config.MatchingEligibleLanes < 1 || config.OtherEligibleLanes < 0 || config.MatchingIneligibleLanes < 0 {
		return fmt.Errorf("endpoint and lane counts are invalid")
	}
	if config.ParallelEdges != 1 {
		return fmt.Errorf("parallel edges must be exactly one because DAWGS graph storage uniquely keys edges by start, end, kind, and graph")
	}
	if config.PropertyPayloadSize < 0 {
		return fmt.Errorf("property payload size must not be negative")
	}
	return nil
}

// NewEndpointSeededExpansionScaleFixture creates terminal-selective expansion
// lanes with independently controlled productive and unproductive reverse work.
func NewEndpointSeededExpansionScaleFixture(config EndpointSeededExpansionScaleConfig) *opengraph.Graph {
	if ValidateEndpointSeededExpansionScaleConfig(config) != nil {
		return nil
	}
	payload := strings.Repeat("x", config.PropertyPayloadSize)
	fixture := &opengraph.Graph{}
	for idx := range config.MatchingEndpoints {
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{
			ID:    fmt.Sprintf("ese-match-%03d", idx),
			Kinds: []string{"Group"},
			Properties: map[string]any{
				"objectid": fmt.Sprintf("S-1-5-21-%03d-512", idx),
				"payload":  payload,
			},
		})
	}
	for idx := range config.OtherEndpoints {
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{
			ID:    fmt.Sprintf("ese-other-%03d", idx),
			Kinds: []string{"Group"},
			Properties: map[string]any{
				"objectid": fmt.Sprintf("S-1-5-21-%03d-513", idx),
				"payload":  payload,
			},
		})
	}

	addLane := func(class string, lane int, endpoint string, eligible bool) {
		user := fmt.Sprintf("ese-%s-user-%04d", class, lane)
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{
			ID:         user,
			Kinds:      []string{"User"},
			Properties: map[string]any{"payload": payload},
		})
		if eligible {
			computer := fmt.Sprintf("ese-%s-computer-%04d", class, lane)
			fixture.Nodes = append(fixture.Nodes, opengraph.Node{
				ID:         computer,
				Kinds:      []string{"Computer"},
				Properties: map[string]any{"payload": payload},
			})
			fixture.Edges = append(fixture.Edges, opengraph.Edge{
				StartID:    computer,
				EndID:      user,
				Kind:       "HasSession",
				Properties: map[string]any{"logical_key": computer + "-session"},
			})
		}
		previous := user
		for level := 1; level <= config.Depth; level++ {
			next := endpoint
			if level < config.Depth {
				next = fmt.Sprintf("ese-%s-lane-%04d-level-%02d", class, lane, level)
				fixture.Nodes = append(fixture.Nodes, opengraph.Node{
					ID:         next,
					Kinds:      []string{"Group"},
					Properties: map[string]any{"payload": payload},
				})
			}
			for parallel := range config.ParallelEdges {
				fixture.Edges = append(fixture.Edges, opengraph.Edge{
					StartID: previous,
					EndID:   next,
					Kind:    "MemberOf",
					Properties: map[string]any{
						"logical_key": fmt.Sprintf("%s-%04d-%02d-%02d", class, lane, level, parallel),
					},
				})
			}
			if config.AddCycle && level == max(1, config.Depth/2) && previous != user {
				fixture.Edges = append(fixture.Edges, opengraph.Edge{
					StartID: next,
					EndID:   previous,
					Kind:    "MemberOf",
					Properties: map[string]any{
						"logical_key": fmt.Sprintf("%s-%04d-cycle", class, lane),
					},
				})
			}
			previous = next
		}
	}

	for lane := range config.MatchingEligibleLanes {
		addLane("matching", lane, fmt.Sprintf("ese-match-%03d", lane%config.MatchingEndpoints), true)
	}
	for lane := range config.OtherEligibleLanes {
		endpoint := "ese-other-000"
		if config.OtherEndpoints > 0 {
			endpoint = fmt.Sprintf("ese-other-%03d", lane%config.OtherEndpoints)
		} else {
			fixture.Nodes = append(fixture.Nodes, opengraph.Node{
				ID:    endpoint,
				Kinds: []string{"Group"},
				Properties: map[string]any{
					"objectid": "S-1-5-21-513",
					"payload":  payload,
				},
			})
		}
		addLane("other", lane, endpoint, true)
	}
	for lane := range config.MatchingIneligibleLanes {
		addLane("ineligible", lane, fmt.Sprintf("ese-match-%03d", lane%config.MatchingEndpoints), false)
	}
	return fixture
}
