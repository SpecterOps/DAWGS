package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type closeErrorWriter struct {
	bytes.Buffer
	err error
}

func (s *closeErrorWriter) Close() error {
	return s.err
}

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

func TestCaptureSpecsRequiresConnection(t *testing.T) {
	_, err := captureSpecs(commandConfig{})
	require.ErrorContains(t, err, "no connection string supplied")
}

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
		"cypher": "MATCH (n) RETURN n"
	}`, string(bytes.TrimSpace(contents)))
}

func TestWritePlanRecordsToReturnsCloseError(t *testing.T) {
	writer := &closeErrorWriter{err: errors.New("close failed")}

	err := writePlanRecordsTo(writer, "records.jsonl", nil)

	require.ErrorContains(t, err, "close records.jsonl")
	require.ErrorContains(t, err, "close failed")
}

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

func TestParseNeo4jPlanDriverConfigPreservesURI(t *testing.T) {
	testCases := []struct {
		name             string
		connStr          string
		expectedTarget   string
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

func TestParseNeo4jPlanDriverConfigRejectsNestedDatabasePath(t *testing.T) {
	for _, connStr := range []string{
		"neo4j://neo4j:password@localhost:7687/db/extra",
		"neo4j://neo4j:password@localhost:7687/db%2Fextra",
	} {
		_, err := parseNeo4jPlanDriverConfig(connStr)
		require.ErrorContains(t, err, "single database name")
	}
}
