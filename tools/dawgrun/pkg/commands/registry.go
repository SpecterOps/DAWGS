// Package commands holds all of the repl commands along with infrastructure types and helpers
package commands

var cmdRegistry map[string]CommandDesc = map[string]CommandDesc{
	"exit":           quitCmd(),
	"explain-psql":   explainAsPsqlCmd(),
	"load-db-kinds":  getPGDBKinds(),
	"lookup-kind":    lookupKindCmd(),
	"lookup-kind-id": lookupKindIDCmd(),
	"open-pg-db":     openPGDBCmd(),
	"parse":          parseCmd(),
	"quit":           quitCmd(),
	"runtime-trace":  runtimeTraceCmd(),
	"translate-psql": translateToPsqlCmd(),
}

func init() {
	cmdRegistry["help"] = helpCmd()
}

func Registry() map[string]CommandDesc {
	return cmdRegistry
}
