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
	"time"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/drivers/pg/changelog"
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

func setupHarness() (*changelog.Changelog, graph.Database, context.Context, context.CancelFunc) {
	const pgConnStr = "user=bhe password=weneedbetterpasswords dbname=bhe host=localhost port=55432"

	var (
		ctx, done = context.WithCancel(context.Background())
	)

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
		} else {
			changelog := changelog.NewChangelog(pool, schemaManager, 1000)
			return changelog, dawgsDB, ctx, done
		}
	}

	return nil, nil, ctx, done
}

func main() {
	log, dawgsDB, ctx, done := setupHarness()
	log.Start(ctx)

	// Graceful shutdown on SIGINT/SIGTERM
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		<-sigC
		slog.Info("Received shutdown signal")
		done()
		wg.Done()
		os.Exit(0)
	}()

	if err := test(ctx, log, dawgsDB); err != nil {
		fmt.Printf("test_100 failed: %v\n", err)
		os.Exit(1)
	}

	wg.Wait()

	slog.Info("Shutdown complete")
}

func test(ctx context.Context, log *changelog.Changelog, db graph.Database) error {
	numNodes := 100_000

	db.BatchOperation(ctx, func(batch graph.Batch) error {
		start := time.Now()
		defer func() {
			slog.Info("batch 1 finished",
				"duration", time.Since(start))
		}()

		slog.Info("batch 1 starting", "timestamp", start)

		for idx := 0; idx < numNodes; idx++ {
			var (
				nodeObjectID   = strconv.Itoa(idx)
				nodeProperties = graph.NewProperties()
			)

			nodeProperties.Set("objectid", nodeObjectID)
			nodeProperties.Set("node_index", idx)
			nodeProperties.Set("lastseen", start)

			proposedChange := changelog.NewNodeChange(
				nodeObjectID,
				nodeKinds,
				nodeProperties,
			)

			if shouldSubmit, err := log.ResolveChange(ctx, proposedChange); err != nil {
				fmt.Println("blahh")
			} else if shouldSubmit {
				batch.UpdateNodeBy(graph.NodeUpdate{
					Node:               graph.PrepareNode(proposedChange.Properties, proposedChange.Kinds...),
					IdentityProperties: []string{"objectid"},
				})
			} else { // we only submit to the log for reconciliation
				log.Submit(ctx, proposedChange)
			}
		}
		return nil
	})

	log.FlushStats()

	db.BatchOperation(ctx, func(batch graph.Batch) error {
		start := time.Now()
		defer func() {
			slog.Info("batch 2 finished",
				"duration", time.Since(start))
		}()

		slog.Info("batch 2 starting", "timestamp", start)

		for idx := 0; idx < numNodes; idx++ {
			var (
				nodeObjectID   = strconv.Itoa(idx)
				nodeProperties = graph.NewProperties()
			)

			nodeProperties.Set("objectid", nodeObjectID)
			nodeProperties.Set("node_index", idx)
			nodeProperties.Set("lastseen", start) // timestamp will be different than original batch, but its an ignored prop so should get cache hits still

			proposedChange := changelog.NewNodeChange(
				nodeObjectID,
				nodeKinds,
				nodeProperties,
			)

			if shouldSubmit, err := log.ResolveChange(ctx, proposedChange); err != nil {
				fmt.Println("blahh")
			} else if shouldSubmit {
				batch.UpdateNodeBy(graph.NodeUpdate{
					Node:               graph.PrepareNode(proposedChange.Properties, proposedChange.Kinds...),
					IdentityProperties: []string{"objectid"},
				})
			} else { // we only submit to the log for reconciliation
				log.Submit(ctx, proposedChange)
			}
		}

		return nil
	})

	log.FlushStats()

	db.BatchOperation(ctx, func(batch graph.Batch) error {
		start := time.Now()
		defer func() {
			slog.Info("batch 3 finished",
				"duration", time.Since(start))
		}()

		slog.Info("batch 3 starting", "timestamp", start)

		for idx := 0; idx < numNodes; idx++ {
			var (
				startObjID = strconv.Itoa(idx)
				endObjID   = strconv.Itoa(idx + 1)
				edgeProps  = graph.NewProperties()
			)

			edgeProps.Set("startID", startObjID)
			edgeProps.Set("endID", endObjID)
			edgeProps.Set("lastseen", start)

			proposedChange := changelog.NewEdgeChange(
				startObjID,
				endObjID,
				edgeKinds[0],
				edgeProps,
			)

			if shouldSubmit, err := log.ResolveChange(ctx, proposedChange); err != nil {
				fmt.Println("blahh")
			} else if shouldSubmit {
				update := graph.RelationshipUpdate{
					Start:                   graph.PrepareNode(graph.NewProperties().SetAll(map[string]any{"objectid": startObjID, "lastseen": start}), nodeKinds...),
					StartIdentityProperties: []string{"objectid"},
					// StartIdentityKind:       sourceKind,
					End: graph.PrepareNode(graph.NewProperties().SetAll(map[string]any{"objectid": endObjID, "lastseen": start}), nodeKinds...),
					// EndIdentityKind:       sourceKind,
					EndIdentityProperties: []string{"objectid"},
					Relationship:          graph.PrepareRelationship(edgeProps, edgeKinds[0]),
				}
				batch.UpdateRelationshipBy(update)
			} else { // we only submit to the log for reconciliation
				log.Submit(ctx, proposedChange)
			}
		}
		return nil
	})

	log.FlushStats()

	db.BatchOperation(ctx, func(batch graph.Batch) error {
		start := time.Now()
		defer func() {
			slog.Info("batch 4 finished",
				"duration", time.Since(start))
		}()

		slog.Info("batch 4 starting", "timestamp", start)

		for idx := 0; idx < numNodes; idx++ {
			var (
				startObjID = strconv.Itoa(idx)
				endObjID   = strconv.Itoa(idx + 1)
				edgeProps  = graph.NewProperties()
			)

			edgeProps.Set("startID", startObjID)
			edgeProps.Set("endID", endObjID)
			// edgeProps.Set("idx", idx)
			edgeProps.Set("lastseen", start)

			proposedChange := changelog.NewEdgeChange(
				startObjID,
				endObjID,
				edgeKinds[0],
				edgeProps,
			)

			if shouldSubmit, err := log.ResolveChange(ctx, proposedChange); err != nil {
				fmt.Println("blahh")
			} else if shouldSubmit {
				update := graph.RelationshipUpdate{
					Start:                   graph.PrepareNode(graph.NewProperties().SetAll(map[string]any{"objectid": startObjID, "lastseen": start}), nodeKinds...),
					StartIdentityProperties: []string{"objectid"},
					// StartIdentityKind:       sourceKind,
					End: graph.PrepareNode(graph.NewProperties().SetAll(map[string]any{"objectid": endObjID, "lastseen": start}), nodeKinds...),
					// EndIdentityKind:       sourceKind,
					EndIdentityProperties: []string{"objectid"},
					Relationship:          graph.PrepareRelationship(edgeProps, edgeKinds[0]),
				}
				batch.UpdateRelationshipBy(update)
			} else { // we only submit to the log for reconciliation
				log.Submit(ctx, proposedChange)
			}
		}
		return nil
	})
	log.FlushStats()

	slog.Info("Done with test")

	return nil
}
