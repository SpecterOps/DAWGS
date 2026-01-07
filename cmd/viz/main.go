package main

import (
	"context"
	"fmt"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/container"
	"github.com/specterops/dawgs/database/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

func main() {
	ctx, done := context.WithCancel(context.Background())
	defer done()

	dbInst, err := dawgs.Open(ctx, pg.DriverName, dawgs.Config{
		ConnectionString: "user=postgres dbname=bhe password=bhe4eva host=localhost",
	})

	if err != nil {
		panic(err)
	}

	metaKinds := graph.Kinds{
		graph.StringKind("Meta"),
		graph.StringKind("MetaDetail"),
	}

	kindFilter := query.And(
		query.Not(query.Start().Kinds().HasOneOf(metaKinds)),
		query.Not(query.End().Kinds().HasOneOf(metaKinds)),
	)

	if digraph, err := container.FetchTSDB(ctx, dbInst, kindFilter); err != nil {
		panic(err)
	} else {
		fmt.Printf("Loaded %d nodes\n", digraph.Triplestore.NumNodes())

		// algo.CalculateKatzCentrality(digraph, 0.01, 1, 0.01, 1000)
	}
}
