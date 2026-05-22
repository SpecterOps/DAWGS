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

	driverName, err = driverFromConnectionString("neo4j://neo4j:password@localhost:7687")
	require.NoError(t, err)
	require.Equal(t, "neo4j", driverName)

	_, err = driverFromConnectionString("mysql://localhost")
	require.ErrorContains(t, err, "unknown connection string scheme")
}
