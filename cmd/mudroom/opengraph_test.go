package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/specterops/dawgs/opengraph"
)

func TestScrubOpenGraphDocumentPreservesShapeAndScrubsProperties(t *testing.T) {
	scrubber := testScrubber(t, "unit-test-salt")
	document := OpenGraphDocument{
		Metadata: map[string]any{
			"tenantid": "01234567-89ab-cdef-0123-456789abcdef",
		},
		Graph: &OpenGraphGraph{
			Nodes: []opengraph.Node{{
				ID:    "node-1",
				Kinds: []string{"User"},
				Properties: map[string]any{
					"userprincipalname":      "alice@corp.example",
					"operatingsystemversion": "10.0.19044",
					"description":            "Example Corp workstation owner",
				},
			}},
			Edges: []opengraph.Edge{{
				StartID: "node-1",
				EndID:   "node-2",
				Kind:    "AdminTo",
				Properties: map[string]any{
					"serviceprincipalname": "MSSQLSvc/sql.corp.example:1433",
				},
			}},
		},
	}

	scrubbed, manifest := scrubber.ScrubOpenGraphDocument(document)

	if scrubbed.Graph.Nodes[0].ID != "node-1" {
		t.Fatalf("expected node ID to be preserved, got %q", scrubbed.Graph.Nodes[0].ID)
	}
	if scrubbed.Graph.Nodes[0].Kinds[0] != "User" {
		t.Fatalf("expected node kind to be preserved, got %v", scrubbed.Graph.Nodes[0].Kinds)
	}
	if scrubbed.Graph.Edges[0].StartID != "node-1" || scrubbed.Graph.Edges[0].EndID != "node-2" {
		t.Fatalf("expected edge endpoints to be preserved, got %+v", scrubbed.Graph.Edges[0])
	}
	if scrubbed.Graph.Edges[0].Kind != "AdminTo" {
		t.Fatalf("expected edge kind to be preserved, got %q", scrubbed.Graph.Edges[0].Kind)
	}

	nodeProperties := scrubbed.Graph.Nodes[0].Properties
	if nodeProperties["userprincipalname"] == "alice@corp.example" {
		t.Fatalf("expected UPN to be scrubbed: %+v", nodeProperties)
	}
	if nodeProperties["operatingsystemversion"] != "10.0.19044" {
		t.Fatalf("expected OS version to be preserved: %+v", nodeProperties)
	}
	if nodeProperties["description"] != "[REDACTED]" {
		t.Fatalf("expected description to be redacted: %+v", nodeProperties)
	}
	if scrubbed.Graph.Edges[0].Properties["serviceprincipalname"] == "MSSQLSvc/sql.corp.example:1433" {
		t.Fatalf("expected relationship property to be scrubbed: %+v", scrubbed.Graph.Edges[0].Properties)
	}
	if scrubbed.Metadata["tenantid"] == "01234567-89ab-cdef-0123-456789abcdef" {
		t.Fatalf("expected metadata property to be scrubbed: %+v", scrubbed.Metadata)
	}

	if !manifest.TopologyPreserved || manifest.NodeCount != 1 || manifest.RelationshipCount != 1 {
		t.Fatalf("unexpected manifest shape claims: %+v", manifest)
	}
	if manifest.NodeActions[ActionPseudonymize] != 1 || manifest.NodeActions[ActionPreserve] != 1 || manifest.NodeActions[ActionRedact] != 1 {
		t.Fatalf("unexpected node action counts: %+v", manifest.NodeActions)
	}
	if manifest.RelationshipActions[ActionPseudonymize] != 1 {
		t.Fatalf("unexpected relationship action counts: %+v", manifest.RelationshipActions)
	}
}

func TestScrubOpenGraphDocumentPreservesDomainSIDAssociation(t *testing.T) {
	scrubber := testScrubber(t, "unit-test-salt")
	sourceDomainSID := "S-1-5-21-1003820064-1082945404-302993405"
	document := OpenGraphDocument{
		Graph: &OpenGraphGraph{
			Nodes: []opengraph.Node{
				{
					ID:    "domain-1",
					Kinds: []string{"Base", "Domain"},
					Properties: map[string]any{
						"name":     "CORP.EXAMPLE",
						"objectid": sourceDomainSID,
					},
				},
				{
					ID:    "user-1",
					Kinds: []string{"Base", "User"},
					Properties: map[string]any{
						"name":      "alice@corp.example",
						"domainsid": sourceDomainSID,
					},
				},
			},
		},
	}

	scrubbed, _ := scrubber.ScrubOpenGraphDocument(document)

	domainObjectID := scrubbed.Graph.Nodes[0].Properties["objectid"]
	if domainObjectID == sourceDomainSID {
		t.Fatal("expected domain objectid to be scrubbed")
	}
	if scrubbed.Graph.Nodes[1].Properties["domainsid"] != domainObjectID {
		t.Fatalf("expected domainsid to match domain objectid: domain=%q user=%q", domainObjectID, scrubbed.Graph.Nodes[1].Properties["domainsid"])
	}
}

func TestPlanAndManifestOmitRawValues(t *testing.T) {
	planner := testPlanner(t)
	input := `{"graph":{"nodes":[{"id":"node-1","kinds":["User"],"properties":{"userprincipalname":"secret-user@example.com"}}]}}`
	var output bytes.Buffer

	plan, err := planner.WritePlan(strings.NewReader(input), &output)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "secret-user@example.com") {
		t.Fatalf("plan leaked raw value: %s", output.String())
	}
	manifest := PlanManifest(plan, false)
	contents, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(contents), "secret-user@example.com") {
		t.Fatalf("manifest leaked raw value: %s", contents)
	}
}

func TestValidateOpenGraphDocumentsAcceptsShapePreservingScrub(t *testing.T) {
	original := OpenGraphDocument{
		Graph: &OpenGraphGraph{
			Nodes: []opengraph.Node{{
				ID:         "node-1",
				Kinds:      []string{"User"},
				Properties: map[string]any{"userprincipalname": "alice@corp.example"},
			}},
			Edges: []opengraph.Edge{{
				StartID:    "node-1",
				EndID:      "node-2",
				Kind:       "AdminTo",
				Properties: map[string]any{"note": "Example Corp"},
			}},
		},
	}
	scrubbed := original
	scrubbed.Graph.Nodes[0].Properties = map[string]any{"userprincipalname": "user-123@example.invalid"}
	scrubbed.Graph.Edges[0].Properties = map[string]any{"note": "[REDACTED]"}

	result := ValidateOpenGraphDocuments(original, scrubbed)
	if !result.Valid {
		t.Fatalf("expected valid shape, got %+v", result)
	}
}

func TestMudroomScrubCommandWritesScrubbedOutputAndManifest(t *testing.T) {
	tempDir := t.TempDir()
	inputPath := filepath.Join(tempDir, "graph.json")
	outputPath := filepath.Join(tempDir, "scrubbed.json")
	manifestPath := filepath.Join(tempDir, "manifest.json")
	input := `{"graph":{"nodes":[{"id":"node-1","kinds":["User"],"properties":{"userprincipalname":"secret-user@example.com"}}],"edges":[]}}`
	if err := os.WriteFile(inputPath, []byte(input), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	runtime := newCommandRuntime(&stdout, &stderr)
	if err := runtime.run(t.Context(), []string{
		"scrub",
		"-input", inputPath,
		"-output", outputPath,
		"-manifest", manifestPath,
		"-salt", "unit-test-salt",
	}); err != nil {
		t.Fatalf("scrub command failed: %v stderr=%s", err, stderr.String())
	}

	outputBytes, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(outputBytes), "secret-user@example.com") {
		t.Fatalf("scrubbed output leaked raw value: %s", outputBytes)
	}
	if !strings.Contains(stdout.String(), "scrubbed 1 nodes and 0 relationships") {
		t.Fatalf("unexpected stdout: %s", stdout.String())
	}

	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(manifestBytes), "unit-test-salt") || strings.Contains(string(manifestBytes), "secret-user@example.com") {
		t.Fatalf("manifest leaked sensitive value: %s", manifestBytes)
	}
}

func testPlanner(t *testing.T) Scrubber {
	t.Helper()

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatal(err)
	}

	planner, err := NewPlanner(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return planner
}
