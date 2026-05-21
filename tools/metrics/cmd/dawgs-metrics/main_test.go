package main

import "testing"

func TestNamedPathFlagsParsesRepeatedBackendResults(t *testing.T) {
	var flags namedPathFlags
	if err := flags.Set("pg=.coverage/pg.json"); err != nil {
		t.Fatalf("set pg result: %v", err)
	}
	if err := flags.Set("neo4j=.coverage/neo4j.json"); err != nil {
		t.Fatalf("set neo4j result: %v", err)
	}

	if len(flags.values) != 2 {
		t.Fatalf("values = %d, want 2", len(flags.values))
	}
	if flags.values[0].Name != "pg" || flags.values[0].Path != ".coverage/pg.json" {
		t.Fatalf("first value = %#v", flags.values[0])
	}
	if flags.values[1].Name != "neo4j" || flags.values[1].Path != ".coverage/neo4j.json" {
		t.Fatalf("second value = %#v", flags.values[1])
	}
}

func TestNamedPathFlagsRejectsInvalidInput(t *testing.T) {
	var flags namedPathFlags
	if err := flags.Set("missing-separator"); err == nil {
		t.Fatal("expected invalid backend result input to fail")
	}
}
