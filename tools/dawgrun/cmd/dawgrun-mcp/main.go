package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/davecgh/go-spew/spew"

	"github.com/specterops/dawgs/tools/dawgrun/pkg/mcpserver"
)

func main() {
	spew.Config.DisablePointerAddresses = true

	var allowWrites bool
	flag.BoolVar(&allowWrites, "allow-writes", false, "enable write-capable DAWGS tools")
	flag.Parse()

	server := mcpserver.New(os.Stdin, os.Stdout, mcpserver.Options{
		AllowWrites: allowWrites,
	})
	if err := server.Serve(); err != nil {
		fmt.Fprintf(os.Stderr, "dawgrun-mcp failed: %v\n", err)
		os.Exit(1)
	}
}
