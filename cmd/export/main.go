package main

import (
	"context"
	"fmt"
	"os"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/util/size"
)

func main() {
	connStr := os.Getenv("PGCONN")
	if connStr == "" {
		connStr = "postgresql://bloodhound:bloodhoundcommunityedition@localhost:5432/bloodhound"
	}

	pool, err := pg.NewPool(connStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	db := pg.NewDriver(size.Gibibyte, pool)

	outFile := "graph_export.json"
	if len(os.Args) > 1 {
		outFile = os.Args[1]
	}

	f, err := os.Create(outFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	if err := opengraph.Export(context.Background(), db, f); err != nil {
		fmt.Fprintf(os.Stderr, "export failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "exported graph to %s\n", outFile)
}
