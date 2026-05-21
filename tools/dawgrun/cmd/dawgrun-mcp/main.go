package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/davecgh/go-spew/spew"

	"github.com/specterops/dawgs/tools/dawgrun/pkg/commands"
	"github.com/specterops/dawgs/tools/dawgrun/pkg/mcpserver"
)

func main() {
	spew.Config.DisablePointerAddresses = true

	var allowWrites bool
	flag.BoolVar(&allowWrites, "allow-writes", false, "enable write-capable DAWGS tools")
	flag.Parse()

	scope := commands.NewScope(commands.RunModeCLI)
	server := mcpserver.NewWithScope(scope, mcpserver.Options{AllowWrites: allowWrites})
	if err := server.Run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "dawgrun-mcp failed: %v\n", err)
		os.Exit(1)
	}
}
