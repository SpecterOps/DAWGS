package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
	_, err := parseNeo4jPlanDriverConfig("neo4j://neo4j:password@localhost:7687/db/extra")
	require.ErrorContains(t, err, "single database name")
}
