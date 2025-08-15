package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
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

func setupHarness() (*changestream.Changelog, *pg.SchemaManager, *pgxpool.Pool, chan changestream.Notification) {
	const pgConnStr = "user=bhe password=weneedbetterpasswords dbname=bhe host=localhost port=55432"

	var (
		ctx, _ = context.WithCancel(context.Background())
	)

	// defer done()

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
			return changelog, schemaManager, pool, notificationC
		}
	}

	return nil, nil, nil, nil
}

type encoder struct {
	buffer *bytes.Buffer
}

func (s *encoder) Encode(values []int16) string {
	s.buffer.Reset()
	s.buffer.WriteRune('{')

	for idx, value := range values {
		if idx > 0 {
			s.buffer.WriteRune(',')
		}

		s.buffer.WriteString(strconv.Itoa(int(value)))
	}

	s.buffer.WriteRune('}')
	return s.buffer.String()
}

func main() {
	changelog, schemaManager, pool, notifications := setupHarness()
	ctx, _ := context.WithCancel(context.Background())

	// Ingest routine
	wg := &sync.WaitGroup{}

	go func() {
		var (
			encoder = encoder{
				buffer: &bytes.Buffer{},
			}
			lastNodeChangeID int64 = 0
		)

		// Ingest routine
		wg.Add(1)

		defer wg.Done()

		for notification := range notifications {
			switch notification.Type {
			case changestream.NotificationNode:
				slog.Info("received notification to update nodes to changelog revision",
					slog.Int64("rev_id", lastNodeChangeID),
					slog.Int64("next_rev_id", notification.RevisionID))

				if err := changelog.ReplayNodeChanges(ctx, lastNodeChangeID, func(change changestream.NodeChange) {

					change.ModifiedProperties["objectid"] = change.NodeID

					if propertyJSON, err := json.Marshal(change.ModifiedProperties); err != nil {
						slog.ErrorContext(ctx, "failed to marshal change properties", slog.Any("error", err))
					} else if kindIDs, err := schemaManager.MapKinds(ctx, change.Kinds); err != nil {
						slog.ErrorContext(ctx, "failed to map kinds", slog.Any("error", err))
					} else {
						switch change.Type() {
						case changestream.ChangeTypeAdded:
							// defaultGraph.ID === 1??
							if _, err := pool.Exec(ctx, changestream.InsertNodeFromChangeSQL, 1, encoder.Encode(kindIDs), propertyJSON); err != nil {
								slog.ErrorContext(ctx, "failed to insert node to graph", slog.Any("error", err))
							}

						case changestream.ChangeTypeModified:
							if _, err := pool.Exec(ctx, changestream.UpdateNodeFromChangeSQL, change.NodeID, encoder.Encode(kindIDs), propertyJSON, change.Deleted); err != nil {
								slog.ErrorContext(ctx, "failed to update node to graph", slog.Any("error", err))
							}

						case changestream.ChangeTypeRemoved:
							slog.Error("not implemented")
						}
					}
				}); err != nil {
					slog.ErrorContext(ctx, "failed to replay node changes", slog.Any("error", err))
				}

				lastNodeChangeID = notification.RevisionID

			case changestream.NotificationEdge:
				slog.Info("received notification to update edges to changelog revision", slog.Int64("rev_id", notification.RevisionID))
				//lastEdgeChangeID = nextNotification.RevisionID
			}
		}

		slog.Info("notification channel closed")
	}()

	if err := test_100(ctx, changelog); err != nil {
		fmt.Printf("Test failed: %v\n", err)
		os.Exit(1)
	}

	slog.Info("Waiting for dawgs to finish")
	wg.Wait()
}

func test(ctx context.Context, changelog *changestream.Changelog) error {
	// propose 3 chnages for `123`
	proposedChange1 := changestream.NewNodeChange(
		"123",
		nodeKinds,
		graph.NewProperties().Set("a", 1),
	)
	proposedChange2 := changestream.NewNodeChange(
		"123",
		nodeKinds,
		graph.NewProperties().SetAll(map[string]any{"a": 1, "b": 2}),
	)
	proposedChange3 := changestream.NewNodeChange(
		"123",
		nodeKinds,
		graph.NewProperties().SetAll(map[string]any{"a": 2, "b": 3}),
	)

	changes := []*changestream.NodeChange{proposedChange1, proposedChange2, proposedChange3}

	for _, change := range changes {
		if err := changelog.ResolveNodeChange(ctx, change); err != nil {
			fmt.Println("blahh")
		} else if change.ShouldSubmit() {
			fmt.Println("shouldsubmit")
			changelog.Submit(ctx, change)
		}
	}

	return nil
}

// test_100 will produce 3 hunnit records in the changelog, which when repalyed, create and make modifications to 100 nodes numbered 0..99
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

	numSkipped := 0
	for _, change := range changes {
		if err := changelog.ResolveNodeChange(ctx, change); err != nil {
			fmt.Println("blahh")
		} else if change.ShouldSubmit() {
			fmt.Println("shouldsubmit")
			changelog.Submit(ctx, change)
		} else {
			numSkipped++
		}
	}
	fmt.Println("numskipped; ", numSkipped)

	return nil
}
