package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/drivers/pg/changestream"
	"github.com/specterops/dawgs/graph"
)

var (
	nodeKinds = graph.Kinds{graph.StringKind("NK1")}
	edgeKinds = graph.Kinds{graph.StringKind("EK2")}
)

func schema() graph.Schema {
	defaultGraph := graph.Graph{
		Name:  "default",
		Nodes: nodeKinds,
		Edges: edgeKinds,
		NodeConstraints: []graph.Constraint{{
			Field: "objectid",
			Type:  graph.BTreeIndex,
		}},
	}

	return graph.Schema{
		Graphs:       []graph.Graph{defaultGraph},
		DefaultGraph: defaultGraph,
	}
}

func setupHarness() (*changestream.Changelog, chan changestream.Notification, context.Context, context.CancelFunc) {
	const pgConnStr = "user=bhe password=weneedbetterpasswords dbname=bhe host=localhost port=55432"

	var (
		ctx, done = context.WithCancel(context.Background())
	)

	notificationC := make(chan changestream.Notification)

	if pool, err := pg.NewPool(pgConnStr); err != nil {
		fmt.Printf("failed to connect to database: %v\n", err)
		os.Exit(1)
	} else if dawgsDB, err := dawgs.Open(ctx, pg.DriverName, dawgs.Config{
		ConnectionString: pgConnStr,
		Pool:             pool,
	}); err != nil {
		fmt.Printf("Failed to open database connection: %v\n", err)
		os.Exit(1)
	} else {
		// Attempt to truncate but don't care about the error
		dawgsDB.Run(
			ctx,
			`
						do $$ declare
							r record;
						begin
							for r in (select tablename from pg_tables where schemaname = 'public') loop
								execute 'drop table if exists ' || quote_ident(r.tablename) || ' cascade';
							end loop;
						end $$;
		`, nil)

		if err := dawgsDB.AssertSchema(ctx, schema()); err != nil {
			fmt.Printf("Failed to validate schema: %v\n", err)
			os.Exit(1)
		} else if schemaManager := pg.NewSchemaManager(pool); err != nil {
			// this is dumb but whatevs
		} else if changelog, err := changestream.NewChangelogDaemon(ctx, pool, schemaManager, 1000, notificationC); err != nil {
			fmt.Printf("Failed to create daemon: %v\n", err)
			os.Exit(1)
		} else {
			return changelog, notificationC, ctx, done
		}
	}

	return nil, nil, ctx, done
}

func main() {
	changelog, notifications, ctx, done := setupHarness()
	// Graceful shutdown on SIGINT/SIGTERM
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigC
		slog.Info("Received shutdown signal")
		done()
	}()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	lastNodeChangeID := 0

	// Run the applier loop in a goroutine like we would in bloodhound.entrypoint.go
	// could gate this with a feature flag.
	go func() {
		defer wg.Done()

		err := changestream.RunNodeApplierLoop(
			ctx,
			notifications,
			changelog,
			1, // default graph id is 1 yeah?
			int64(lastNodeChangeID),
		)

		if err != nil {
			slog.Error("Node applier loop exited with error", slog.Any("error", err))
		} else {
			slog.Info("Node applier loop stopped cleanly")
		}
	}()

	if err := test(ctx, changelog); err != nil {
		fmt.Printf("test_100 failed: %v\n", err)
		os.Exit(1)
	}

	wg.Wait()
	slog.Info("Shutdown complete")
}

func test(ctx context.Context, changelog *changestream.Changelog) error {
	numNodes := 1000
	for idx := 0; idx < numNodes; idx++ {
		var (
			nodeObjectID   = strconv.Itoa(idx)
			nodeProperties = graph.NewProperties()
		)

		nodeProperties.Set("objectid", nodeObjectID)
		nodeProperties.Set("node_index", idx)

		proposedChange := changestream.NewNodeChange(
			nodeObjectID,
			nodeKinds,
			nodeProperties,
		)

		if err := changelog.ResolveNodeChange(ctx, proposedChange); err != nil {
			fmt.Println("blahh")
		} else if proposedChange.ShouldSubmit() {
			changelog.Submit(ctx, proposedChange)
		}
	}

	for idx := 0; idx < numNodes; idx++ {
		var (
			nodeObjectID   = strconv.Itoa(idx)
			nodeProperties = graph.NewProperties()
		)

		nodeProperties.Set("objectid", nodeObjectID)
		nodeProperties.Set("node_index", idx)
		nodeProperties.Set("new_value", 41240-idx) // this property counts as a change

		proposedChange := changestream.NewNodeChange(
			nodeObjectID,
			nodeKinds,
			nodeProperties,
		)

		if err := changelog.ResolveNodeChange(ctx, proposedChange); err != nil {
			fmt.Println("blahh")
		} else if proposedChange.ShouldSubmit() {
			changelog.Submit(ctx, proposedChange)
		}
	}

	slog.Info("Done with test")

	return nil
}

// test_100 will produce 3 hunnit records in the changelog,
// which when repalyed, create and make modifications to 100 nodes numbered 0..99
// as written every node should have 2 props: a:1 and objectid
func test_100(ctx context.Context, changelog *changestream.Changelog) error {
	changes := []*changestream.NodeChange{}

	// 100 additions
	for idx := range 100 {
		nodeID := strconv.Itoa(idx)
		properties := graph.NewProperties().Set("hello", "world")
		change := changestream.NewNodeChange(nodeID, nodeKinds, properties)
		changes = append(changes, change)
	}

	// 100 no-ops, because these are the same as before
	for idx := range 100 {
		nodeID := strconv.Itoa(idx)
		properties := graph.NewProperties().Set("hello", "world")
		change := changestream.NewNodeChange(nodeID, nodeKinds, properties)
		changes = append(changes, change)
	}

	// 100 updates, change hello prop
	for idx := range 100 {
		nodeID := strconv.Itoa(idx)
		properties := graph.NewProperties().Set("hello", "world_updated")
		change := changestream.NewNodeChange(nodeID, nodeKinds, properties)
		changes = append(changes, change)
	}

	// 100 deletes
	for idx := range 100 {
		nodeID := strconv.Itoa(idx)
		properties := graph.NewProperties().Set("a", 1)
		change := changestream.NewNodeChange(nodeID, nodeKinds, properties)
		changes = append(changes, change)
	}

	// numSkipped := 0
	for _, change := range changes {
		if err := changelog.ResolveNodeChange(ctx, change); err != nil {
			// fmt.Println("blahh")
		} else if change.ShouldSubmit() {
			// fmt.Println("shouldsubmit")
			changelog.Submit(ctx, change)
		} else {
			// numSkipped++
		}
	}
	// fmt.Println("numskipped; ", numSkipped)

	return nil
}
