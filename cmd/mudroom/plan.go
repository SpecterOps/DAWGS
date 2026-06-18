package main

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/specterops/dawgs/opengraph"
)

type PlanEntity string

const (
	PlanEntityMetadata     PlanEntity = "metadata"
	PlanEntityNode         PlanEntity = "node"
	PlanEntityRelationship PlanEntity = "relationship"
)

type PlanEntry struct {
	Entity  PlanEntity     `json:"entity"`
	Key     string         `json:"key"`
	Action  PropertyAction `json:"action"`
	Count   int            `json:"count"`
	Shapes  []string       `json:"shapes,omitempty"`
	Reasons []string       `json:"reasons,omitempty"`
}

type Plan struct {
	GeneratedAt       time.Time   `json:"generated_at"`
	Mode              ScrubMode   `json:"mode"`
	RulesVersion      string      `json:"rules_version"`
	NodeCount         int         `json:"node_count"`
	RelationshipCount int         `json:"relationship_count"`
	Entries           []PlanEntry `json:"entries"`
}

type planAccumulator struct {
	entries map[string]PlanEntry
}

func NewPlanner(cfg Config) (Scrubber, error) {
	if err := cfg.Validate(); err != nil {
		return Scrubber{}, err
	}
	if ScrubMode(cfg.Scrub.Mode) != ModeShapeOnly {
		return Scrubber{}, fmt.Errorf("scrub mode %q is recognized but not implemented yet", cfg.Scrub.Mode)
	}

	classifier, err := cfg.Classifier.Compile()
	if err != nil {
		return Scrubber{}, err
	}

	cfg.Scrub.FakeDomain = strings.Trim(strings.ToLower(strings.TrimSpace(cfg.Scrub.FakeDomain)), ".")
	cfg.Scrub.RedactionMarker = strings.TrimSpace(cfg.Scrub.RedactionMarker)

	return Scrubber{
		config:     cfg.Scrub,
		classifier: classifier,
	}, nil
}

func (s Scrubber) PlanOpenGraph(reader io.Reader) (Plan, error) {
	document, err := DecodeOpenGraphDocument(reader)
	if err != nil {
		return Plan{}, err
	}

	return s.PlanOpenGraphDocument(document), nil
}

func (s Scrubber) WritePlan(reader io.Reader, writer io.Writer) (Plan, error) {
	plan, err := s.PlanOpenGraph(reader)
	if err != nil {
		return Plan{}, err
	}

	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(plan); err != nil {
		return Plan{}, fmt.Errorf("encode scrub plan: %w", err)
	}

	return plan, nil
}

func (s Scrubber) PlanOpenGraphDocument(document OpenGraphDocument) Plan {
	accumulator := planAccumulator{entries: map[string]PlanEntry{}}
	accumulator.addProperties(PlanEntityMetadata, document.Metadata, s)

	if document.Graph != nil {
		accumulator.addNodes(document.Graph.Nodes, s)
		accumulator.addEdges(document.Graph.Edges, s)
		return newPlan(len(document.Graph.Nodes), len(document.Graph.Edges), accumulator.sorted())
	}

	accumulator.addNodes(document.Nodes, s)
	accumulator.addEdges(document.Edges, s)
	return newPlan(len(document.Nodes), len(document.Edges), accumulator.sorted())
}

func PlanManifest(plan Plan, saltProvided bool) Manifest {
	manifest := Manifest{
		GeneratedAt:         time.Now().UTC(),
		Mode:                plan.Mode,
		RulesVersion:        plan.RulesVersion,
		SaltProvided:        saltProvided,
		NodeCount:           plan.NodeCount,
		RelationshipCount:   plan.RelationshipCount,
		NodeActions:         map[PropertyAction]int{},
		RelationshipActions: map[PropertyAction]int{},
		TopologyPreserved:   true,
		Notes: []string{
			"Dry-run only; no graph output was written.",
			"Node IDs would be preserved.",
			"Relationship endpoints would be preserved.",
			"Node and relationship kinds would be preserved.",
		},
	}

	for _, entry := range plan.Entries {
		switch entry.Entity {
		case PlanEntityNode:
			manifest.NodeActions[entry.Action] += entry.Count
		case PlanEntityRelationship:
			manifest.RelationshipActions[entry.Action] += entry.Count
		}
	}

	return manifest
}

func newPlan(nodeCount, relationshipCount int, entries []PlanEntry) Plan {
	return Plan{
		GeneratedAt:       time.Now().UTC(),
		Mode:              ModeShapeOnly,
		RulesVersion:      RulesVersion,
		NodeCount:         nodeCount,
		RelationshipCount: relationshipCount,
		Entries:           entries,
	}
}

func (s planAccumulator) addNodes(nodes []opengraph.Node, scrubber Scrubber) {
	for _, node := range nodes {
		s.addProperties(PlanEntityNode, node.Properties, scrubber)
	}
}

func (s planAccumulator) addEdges(edges []opengraph.Edge, scrubber Scrubber) {
	for _, edge := range edges {
		s.addProperties(PlanEntityRelationship, edge.Properties, scrubber)
	}
}

func (s planAccumulator) addProperties(entity PlanEntity, properties map[string]any, scrubber Scrubber) {
	for key, value := range properties {
		s.add(entity, scrubber.PlanProperty(key, value))
	}
}

func (s planAccumulator) add(entity PlanEntity, propertyPlan PropertyPlan) {
	shapes := append([]string(nil), propertyPlan.Shapes...)
	reasons := append([]string(nil), propertyPlan.Reasons...)
	sort.Strings(shapes)
	sort.Strings(reasons)

	accumulatorKey := string(entity) + "\x00" + propertyPlan.Key + "\x00" + string(propertyPlan.Action) + "\x00" + strings.Join(shapes, ",") + "\x00" + strings.Join(reasons, ",")
	entry := s.entries[accumulatorKey]
	if entry.Count == 0 {
		entry = PlanEntry{
			Entity:  entity,
			Key:     propertyPlan.Key,
			Action:  propertyPlan.Action,
			Shapes:  shapes,
			Reasons: reasons,
		}
	}
	entry.Count += 1
	s.entries[accumulatorKey] = entry
}

func (s planAccumulator) sorted() []PlanEntry {
	entries := make([]PlanEntry, 0, len(s.entries))
	for _, entry := range s.entries {
		entries = append(entries, entry)
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Entity == entries[j].Entity {
			if entries[i].Key == entries[j].Key {
				return entries[i].Action < entries[j].Action
			}
			return entries[i].Key < entries[j].Key
		}
		return entries[i].Entity < entries[j].Entity
	})

	return entries
}
