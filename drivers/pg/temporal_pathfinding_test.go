//go:build manual_integration

package pg_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

// logPath pretty-prints a path for readable test output.
func logPath(t *testing.T, label string, path graph.Path) {
	t.Helper()

	var parts []string
	path.Walk(func(start, end *graph.Node, rel *graph.Relationship) bool {
		if len(parts) == 0 {
			startName, _ := start.Properties.Get("name").String()
			parts = append(parts, fmt.Sprintf("(%s:%s)", startName, strings.Join(start.Kinds.Strings(), ":")))
		}
		endName, _ := end.Properties.Get("name").String()
		parts = append(parts, fmt.Sprintf("-[%s]-> (%s:%s)", rel.Kind.String(), endName, strings.Join(end.Kinds.Strings(), ":")))
		return true
	})

	t.Logf("[%s] Path (len=%d): %s", label, len(path.Edges), strings.Join(parts, " "))
}

// TestTemporalAttackPathAppears — "The Intern Gets Promoted"
//
// jsmith is a regular user. A chain exists from a privileged group through a domain controller
// to a DA account, but jsmith isn't connected to it. After the snapshot, jsmith gets added to
// the privileged group — completing the attack path.
func TestTemporalAttackPathAppears(t *testing.T) {
	pgDriver, cleanup := setupTestDriver(t)
	defer cleanup()

	ctx := context.Background()

	var jsmithID, regularUsersID, domainAdminsID, dc01ID, dasvcID graph.ID

	// Phase 1: Create nodes and the partial chain (no link from jsmith to Domain Admins)
	require.NoError(t, pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		jsmith, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "jsmith"}), kindUser)
		if err != nil {
			return err
		}
		jsmithID = jsmith.ID

		regularUsers, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "Regular Users"}), kindGroup)
		if err != nil {
			return err
		}
		regularUsersID = regularUsers.ID

		domainAdmins, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "Domain Admins Group"}), kindGroup)
		if err != nil {
			return err
		}
		domainAdminsID = domainAdmins.ID

		dc01, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "DC01"}), kindComputer)
		if err != nil {
			return err
		}
		dc01ID = dc01.ID

		dasvc, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "DA-SVC"}), kindDomainAdmin)
		if err != nil {
			return err
		}
		dasvcID = dasvc.ID

		// jsmith -> Regular Users (harmless)
		if _, err := tx.CreateRelationshipByIDs(jsmithID, regularUsersID, kindMemberOf, graph.NewProperties()); err != nil {
			return err
		}
		// Domain Admins Group -> DC01
		if _, err := tx.CreateRelationshipByIDs(domainAdminsID, dc01ID, kindAdminTo, graph.NewProperties()); err != nil {
			return err
		}
		// DC01 -> DA-SVC
		if _, err := tx.CreateRelationshipByIDs(dc01ID, dasvcID, kindHasSession, graph.NewProperties()); err != nil {
			return err
		}

		return nil
	}))

	// Snapshot: no path from jsmith to DA-SVC at this point
	snapshotTime := time.Now()
	time.Sleep(50 * time.Millisecond)

	// Phase 2: The accident — jsmith added to Domain Admins Group
	require.NoError(t, pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateRelationshipByIDs(jsmithID, domainAdminsID, kindMemberOf, graph.NewProperties())
		return err
	}))

	// Current state: shortest path jsmith -> DA-SVC should exist (3 hops)
	require.NoError(t, pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		var found bool
		err := tx.Relationships().Filter(
			query.And(
				query.Equals(query.StartID(), jsmithID),
				query.Equals(query.EndID(), dasvcID),
			),
		).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
			for path := range cursor.Chan() {
				found = true
				require.Equal(t, 3, len(path.Edges), "attack path should be 3 hops")
				logPath(t, "CURRENT", path)
			}
			return cursor.Error()
		})
		require.NoError(t, err)
		require.True(t, found, "current state should have attack path from jsmith to DA-SVC")
		return nil
	}))

	// Historical state: no path from jsmith to DA-SVC
	require.NoError(t, pgDriver.AsOfReadTransaction(ctx, snapshotTime, func(tx graph.Transaction) error {
		var found bool
		err := tx.Relationships().Filter(
			query.And(
				query.Equals(query.StartID(), jsmithID),
				query.Equals(query.EndID(), dasvcID),
			),
		).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
			for path := range cursor.Chan() {
				found = true
				logPath(t, "HISTORICAL (unexpected!)", path)
			}
			return cursor.Error()
		})
		require.NoError(t, err)
		require.False(t, found, "historical state should NOT have attack path from jsmith to DA-SVC")
		return nil
	}))

	t.Log("PASS: Attack path appeared after snapshot — historical state correctly shows no path")
}

// TestTemporalAttackPathRemediated — "The Breach That Was Fixed"
//
// A full attack path exists through a compromised session. Security ops kill the session
// (delete the HasSession edge). Current state: path gone. Historical state: path still
// visible — proving the exposure window.
func TestTemporalAttackPathRemediated(t *testing.T) {
	pgDriver, cleanup := setupTestDriver(t)
	defer cleanup()

	ctx := context.Background()

	var compromisedUserID, itAdminsID, fileSrvID, daAdminID graph.ID
	var hasSessionRelID graph.ID

	// Phase 1: Full attack path exists
	require.NoError(t, pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		compromisedUser, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "compromised_user"}), kindUser)
		if err != nil {
			return err
		}
		compromisedUserID = compromisedUser.ID

		itAdmins, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "IT Admins"}), kindGroup)
		if err != nil {
			return err
		}
		itAdminsID = itAdmins.ID

		fileSrv, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "FILE-SRV-01"}), kindComputer)
		if err != nil {
			return err
		}
		fileSrvID = fileSrv.ID

		daAdmin, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "DA-ADMIN"}), kindDomainAdmin)
		if err != nil {
			return err
		}
		daAdminID = daAdmin.ID

		// compromised_user -> IT Admins
		if _, err := tx.CreateRelationshipByIDs(compromisedUserID, itAdminsID, kindMemberOf, graph.NewProperties()); err != nil {
			return err
		}
		// IT Admins -> FILE-SRV-01
		if _, err := tx.CreateRelationshipByIDs(itAdminsID, fileSrvID, kindAdminTo, graph.NewProperties()); err != nil {
			return err
		}
		// FILE-SRV-01 -> DA-ADMIN (the compromised session)
		hasSessionRel, err := tx.CreateRelationshipByIDs(fileSrvID, daAdminID, kindHasSession, graph.NewProperties())
		if err != nil {
			return err
		}
		hasSessionRelID = hasSessionRel.ID

		return nil
	}))

	// Verify the full attack path exists before remediation
	require.NoError(t, pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		var found bool
		err := tx.Relationships().Filter(
			query.And(
				query.Equals(query.StartID(), compromisedUserID),
				query.Equals(query.EndID(), daAdminID),
			),
		).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
			for path := range cursor.Chan() {
				found = true
				require.Equal(t, 3, len(path.Edges), "full attack path should be 3 hops")
				logPath(t, "BEFORE REMEDIATION", path)
			}
			return cursor.Error()
		})
		require.NoError(t, err)
		require.True(t, found, "attack path should exist before remediation")
		return nil
	}))

	// Snapshot: path exists at this point
	snapshotTime := time.Now()
	time.Sleep(50 * time.Millisecond)

	// Phase 2: Security ops kill the session — delete via BatchOperation for CTE logging
	require.NoError(t, pgDriver.BatchOperation(ctx, func(batch graph.Batch) error {
		return batch.DeleteRelationship(hasSessionRelID)
	}))

	// Current state: no path (remediated)
	require.NoError(t, pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		var found bool
		err := tx.Relationships().Filter(
			query.And(
				query.Equals(query.StartID(), compromisedUserID),
				query.Equals(query.EndID(), daAdminID),
			),
		).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
			for path := range cursor.Chan() {
				found = true
				logPath(t, "CURRENT (unexpected!)", path)
			}
			return cursor.Error()
		})
		require.NoError(t, err)
		require.False(t, found, "current state should NOT have attack path after remediation")
		return nil
	}))

	// Historical state: path still visible — proving the exposure window
	require.NoError(t, pgDriver.AsOfReadTransaction(ctx, snapshotTime, func(tx graph.Transaction) error {
		var found bool
		err := tx.Relationships().Filter(
			query.And(
				query.Equals(query.StartID(), compromisedUserID),
				query.Equals(query.EndID(), daAdminID),
			),
		).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
			for path := range cursor.Chan() {
				found = true
				require.Equal(t, 3, len(path.Edges), "historical attack path should be 3 hops")
				logPath(t, "HISTORICAL", path)
			}
			return cursor.Error()
		})
		require.NoError(t, err)
		require.True(t, found, "historical state should still show the attack path (exposure window)")
		return nil
	}))

	t.Log("PASS: Attack path remediated — historical state correctly shows the exposure window")
}

// TestTemporalAttackPathAtScale — "Enterprise AD Topology"
//
// 10k-user org with realistic group/computer/DA topology. One planted attack path gap is
// completed after the snapshot. Times shortest path queries on both current and historical state.
func TestTemporalAttackPathAtScale(t *testing.T) {
	pgDriver, cleanup := setupTestDriver(t)
	defer cleanup()

	ctx := context.Background()

	const (
		numUsers     = 10000
		numGroups    = 200
		numComputers = 500
		numDAs       = 5
	)

	// Create all nodes in a single batch
	userIDs := make([]graph.ID, numUsers)
	groupIDs := make([]graph.ID, numGroups)
	computerIDs := make([]graph.ID, numComputers)
	daIDs := make([]graph.ID, numDAs)

	setupStart := time.Now()

	require.NoError(t, pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for i := 0; i < numUsers; i++ {
			n, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": fmt.Sprintf("user-%d", i)}), kindUser)
			if err != nil {
				return err
			}
			userIDs[i] = n.ID
		}
		for i := 0; i < numGroups; i++ {
			n, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": fmt.Sprintf("group-%d", i)}), kindGroup)
			if err != nil {
				return err
			}
			groupIDs[i] = n.ID
		}
		for i := 0; i < numComputers; i++ {
			n, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": fmt.Sprintf("computer-%d", i)}), kindComputer)
			if err != nil {
				return err
			}
			computerIDs[i] = n.ID
		}
		for i := 0; i < numDAs; i++ {
			n, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": fmt.Sprintf("da-%d", i)}), kindDomainAdmin)
			if err != nil {
				return err
			}
			daIDs[i] = n.ID
		}
		return nil
	}))

	t.Logf("Nodes created in %v", time.Since(setupStart))

	// Create edges via batch for performance
	edgeStart := time.Now()
	require.NoError(t, pgDriver.BatchOperation(ctx, func(batch graph.Batch) error {
		// Random MemberOf: each user joins 1-3 groups (skip user-0 — planted path)
		for i := 1; i < numUsers; i++ {
			groupCount := 1 + (i % 3)
			for g := 0; g < groupCount; g++ {
				groupIdx := (i*7 + g*13) % numGroups // deterministic pseudo-random
				if err := batch.CreateRelationshipByIDs(userIDs[i], groupIDs[groupIdx], kindMemberOf, graph.NewProperties()); err != nil {
					return err
				}
			}
		}

		// AdminTo: each group admins 1-4 computers
		for i := 0; i < numGroups; i++ {
			compCount := 1 + (i % 4)
			for c := 0; c < compCount; c++ {
				compIdx := (i*11 + c*17) % numComputers
				if err := batch.CreateRelationshipByIDs(groupIDs[i], computerIDs[compIdx], kindAdminTo, graph.NewProperties()); err != nil {
					return err
				}
			}
		}

		// HasSession: each DA has sessions on 5-10 computers
		for i := 0; i < numDAs; i++ {
			sessCount := 5 + (i % 6)
			for s := 0; s < sessCount; s++ {
				compIdx := (i*23 + s*31) % numComputers
				if err := batch.CreateRelationshipByIDs(computerIDs[compIdx], daIDs[i], kindHasSession, graph.NewProperties()); err != nil {
					return err
				}
			}
		}

		// Planted chain: group-0 -> computer-0 -> da-0 (guaranteed path segment)
		if err := batch.CreateRelationshipByIDs(groupIDs[0], computerIDs[0], kindAdminTo, graph.NewProperties()); err != nil {
			return err
		}
		if err := batch.CreateRelationshipByIDs(computerIDs[0], daIDs[0], kindHasSession, graph.NewProperties()); err != nil {
			return err
		}

		return nil
	}))

	t.Logf("Edges created in %v", time.Since(edgeStart))

	// Count edges
	require.NoError(t, pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		count, err := tx.Relationships().Count()
		require.NoError(t, err)
		t.Logf("Total edges: %d", count)
		return nil
	}))

	// Snapshot: user-0 is NOT yet connected to group-0
	snapshotTime := time.Now()
	time.Sleep(50 * time.Millisecond)

	// Phase 2: user-0 -> group-0 (completes the attack path)
	require.NoError(t, pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateRelationshipByIDs(userIDs[0], groupIDs[0], kindMemberOf, graph.NewProperties())
		return err
	}))

	t.Logf("Total setup: %v", time.Since(setupStart))

	// Benchmark: current state shortest path
	const runs = 5
	var currentTotal time.Duration
	var currentPathLen int

	for i := 0; i < runs; i++ {
		start := time.Now()
		require.NoError(t, pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
			return tx.Relationships().Filter(
				query.And(
					query.Equals(query.StartID(), userIDs[0]),
					query.Equals(query.EndID(), daIDs[0]),
				),
			).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
				for path := range cursor.Chan() {
					currentPathLen = len(path.Edges)
					if i == 0 {
						logPath(t, "CURRENT", path)
					}
				}
				return cursor.Error()
			})
		}))
		currentTotal += time.Since(start)
	}

	// Benchmark: historical state shortest path
	var historicalTotal time.Duration
	var historicalFound bool

	for i := 0; i < runs; i++ {
		start := time.Now()
		require.NoError(t, pgDriver.AsOfReadTransaction(ctx, snapshotTime, func(tx graph.Transaction) error {
			return tx.Relationships().Filter(
				query.And(
					query.Equals(query.StartID(), userIDs[0]),
					query.Equals(query.EndID(), daIDs[0]),
				),
			).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
				for path := range cursor.Chan() {
					historicalFound = true
					if i == 0 {
						logPath(t, "HISTORICAL (unexpected!)", path)
					}
				}
				return cursor.Error()
			})
		}))
		historicalTotal += time.Since(start)
	}

	require.Greater(t, currentPathLen, 0, "current state should find a path from user-0 to da-0")
	require.False(t, historicalFound, "historical state should NOT find a path (user-0 wasn't connected to group-0)")

	currentAvg := currentTotal / time.Duration(runs)
	historicalAvg := historicalTotal / time.Duration(runs)

	t.Logf("Current path length: %d hops", currentPathLen)
	t.Logf("Current shortest path avg (%d runs):    %v", runs, currentAvg)
	t.Logf("Historical shortest path avg (%d runs):  %v", runs, historicalAvg)

	if currentAvg > 0 {
		t.Logf("Historical/Current overhead ratio: %.2fx", float64(historicalAvg)/float64(currentAvg))
	}
}
