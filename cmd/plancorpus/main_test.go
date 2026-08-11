package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// closeErrorWriter wraps an in-memory buffer and injects a Close error for output tests.
type closeErrorWriter struct {
	// Buffer captures bytes written before the injected Close failure.
	bytes.Buffer

	// err is returned after serialization attempts to close the destination.
	err error
}

// Close returns the injected failure used to verify output finalization errors.
func (s *closeErrorWriter) Close() error {
	return s.err
}

// TestCaptureSpecs verifies that backend-specific connection flags override the generic URI and produce PostgreSQL then Neo4j capture specs.
func TestCaptureSpecs(t *testing.T) {
	specs, err := captureSpecs(commandConfig{
		Connection:      "neo4j://neo4j:password@localhost:7687",
		PGConnection:    "postgres://postgres:password@localhost/db",
		Neo4jConnection: "neo4j://neo4j:override@localhost:7687",
	})
	require.NoError(t, err)
	require.Equal(t, []captureSpec{{
		DriverName: "pg",
		Connection: "postgres://postgres:password@localhost/db",
	}, {
		DriverName: "neo4j",
		Connection: "neo4j://neo4j:override@localhost:7687",
	}}, specs)
}

// TestCaptureSpecsRequiresConnection verifies that capture cannot proceed when no generic or backend-specific connection URI is supplied.
func TestCaptureSpecsRequiresConnection(t *testing.T) {
	_, err := captureSpecs(commandConfig{})
	require.ErrorContains(t, err, "no connection string supplied")
}

// TestWritePlanRecordsWritesJSONLines verifies the stable JSON Lines schema, including source query identity and default metadata.
func TestWritePlanRecordsWritesJSONLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.jsonl")

	err := writePlanRecords(path, []PlanRecord{{
		Driver: "pg",
		Source: "cases/example.json",
		Name:   "example",
		Cypher: "MATCH (n) RETURN n",
	}})
	require.NoError(t, err)

	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"driver": "pg",
		"source": "cases/example.json",
		"name": "example",
		"cypher": "MATCH (n) RETURN n",
		"metadata": {
			"dawgs_version": ""
		}
	}`, string(bytes.TrimSpace(contents)))
}

// TestWritePlanRecordsToReturnsCloseError verifies that destination close failures retain the output path in their diagnostic.
func TestWritePlanRecordsToReturnsCloseError(t *testing.T) {
	writer := &closeErrorWriter{err: errors.New("close failed")}

	err := writePlanRecordsTo(writer, "records.jsonl", nil)

	require.ErrorContains(t, err, "close records.jsonl")
	require.ErrorContains(t, err, "close failed")
}

// TestWritePlanRecordsToClosesAfterEncodeError verifies that encoding and close failures are joined so cleanup is attempted without losing the primary serialization error.
func TestWritePlanRecordsToClosesAfterEncodeError(t *testing.T) {
	writer := &closeErrorWriter{err: errors.New("close failed")}

	err := writePlanRecordsTo(writer, "records.jsonl", []PlanRecord{{
		Driver: "pg",
		Name:   "bad params",
		Params: map[string]any{"bad": make(chan int)},
	}})

	require.ErrorContains(t, err, "write records.jsonl")
	require.ErrorContains(t, err, "unsupported type")
	require.ErrorContains(t, err, "close records.jsonl")
	require.ErrorContains(t, err, "close failed")
}

// TestDriverFromConnectionString verifies PostgreSQL and all supported Neo4j routing schemes and rejects an unrelated database protocol.
func TestDriverFromConnectionString(t *testing.T) {
	driverName, err := driverFromConnectionString("postgresql://postgres:password@localhost/db")
	require.NoError(t, err)
	require.Equal(t, "pg", driverName)

	for _, connStr := range []string{
		"neo4j://neo4j:password@localhost:7687",
		"neo4j+s://neo4j:password@localhost:7687",
		"neo4j+ssc://neo4j:password@localhost:7687",
	} {
		driverName, err = driverFromConnectionString(connStr)
		require.NoError(t, err)
		require.Equal(t, "neo4j", driverName)
	}

	_, err = driverFromConnectionString("mysql://localhost")
	require.ErrorContains(t, err, "unknown connection string scheme")
}

// TestParseNeo4jPlanDriverConfigPreservesURI verifies credentials extraction while preserving routing security, host, query, and an optional single database name.
func TestParseNeo4jPlanDriverConfigPreservesURI(t *testing.T) {
	testCases := []struct {
		// name identifies the routing form in subtest diagnostics.
		name string

		// connStr is the credential-bearing URI accepted by the parser.
		connStr string

		// expectedTarget is the credential-free driver URI after database-path extraction.
		expectedTarget string

		// expectedDatabase is the optional database parsed from the sole path segment.
		expectedDatabase string
	}{{
		name:             "plain routing",
		connStr:          "neo4j://neo4j:password@localhost:7687",
		expectedTarget:   "neo4j://localhost:7687",
		expectedDatabase: "",
	}, {
		name:             "secure routing",
		connStr:          "neo4j+s://neo4j:password@cluster.example:7687",
		expectedTarget:   "neo4j+s://cluster.example:7687",
		expectedDatabase: "",
	}, {
		name:             "self signed routing with database and query",
		connStr:          "neo4j+ssc://neo4j:password@cluster.example:7687/analytics?policy=fast",
		expectedTarget:   "neo4j+ssc://cluster.example:7687?policy=fast",
		expectedDatabase: "analytics",
	}}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cfg, err := parseNeo4jPlanDriverConfig(testCase.connStr)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedTarget, cfg.Target)
			require.Equal(t, "neo4j", cfg.Username)
			require.Equal(t, "password", cfg.Password)
			require.Equal(t, testCase.expectedDatabase, cfg.DatabaseName)
		})
	}
}

// TestParseNeo4jPlanDriverConfigRejectsNestedDatabasePath verifies that literal and percent-encoded nested paths cannot masquerade as one Neo4j database name.
func TestParseNeo4jPlanDriverConfigRejectsNestedDatabasePath(t *testing.T) {
	for _, connStr := range []string{
		"neo4j://neo4j:password@localhost:7687/db/extra",
		"neo4j://neo4j:password@localhost:7687/db%2Fextra",
	} {
		_, err := parseNeo4jPlanDriverConfig(connStr)
		require.ErrorContains(t, err, "single database name")
	}
}
