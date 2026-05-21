// Package commands holds all of the repl commands along with infrastructure types and helpers
package commands

import (
	"maps"
	"slices"
)

var cmdRegistry map[string]CommandDesc = map[string]CommandDesc{
	"copy-opengraph":   copyOpenGraphCmd(),
	"exit":             quitCmd(),
	"explain-psql":     explainAsPsqlCmd(),
	"list-connections": listConnectionsCmd(),
	"load-connections": loadConnectionsCmd(),
	"load-opengraph":   loadOpenGraphCmd(),
	"load-db-kinds":    getPGDBKinds(),
	"lookup-kind":      lookupKindCmd(),
	"lookup-kind-id":   lookupKindIDCmd(),
	"open":             openCmd(),
	"parse":            parseCmd(),
	"query-cypher":     queryCypherCmd(),
	"quit":             quitCmd(),
	"runtime-trace":    runtimeTraceCmd(),
	"save-connections": saveConnectionsCmd(),
	"save-opengraph":   saveOpenGraphCmd(),
	"translate-psql":   translateToPsqlCmd(),
}

func init() {
	cmdRegistry["help"] = helpCmd()
}

func Registry() map[string]CommandDesc {
	return cmdRegistry
}

func SortedCommandNames() []string {
	return slices.Sorted(maps.Keys(cmdRegistry))
}
