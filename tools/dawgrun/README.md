```
                            .--~~,__
               :-....,-------`~~'._.'
                `-,,,  ,_      ;'~U'
                 _,-' ,'`-__; '--.
                (_/'~~      ''''(;

               ,---.            ,-.-.      _,---.
  _,..---._  .--.'  \  ,-..-.-./  \==\ _.='.'-,  \
/==/,   -  \ \==\-/\ \ |, \=/\=|- |==|/==.'-     /
|==|   _   _\/==/-|_\ ||- |/ |/ , /==/==/ -   .-'
|==|  .=.   |\==\,   - \\, ,     _|==|==|_   /_,-.
|==|,|   | -|/==/ -   ,|| -  -  , |==|==|  , \_.' )
|==|  '='   /==/-  /\ - \\  ,  - /==/\==\-  ,    (
|==|-,   _`/\==\ _.\=\.-'|-  /\ /==/  /==/ _  ,  /
`-.`.____.'  `--`        `--`  `--`   `--`------'
                                 .-._
         .-.,.---.  .--.-. .-.-./==/ \  .-._
        /==/  `   \/==/ -|/=/  ||==|, \/ /, /
       |==|-, .=., |==| ,||=| -||==|-  \|  |
       |==|   '='  /==|- | =/  ||==| ,  | -|
       |==|- ,   .'|==|,  \/ - ||==| -   _ |
       |==|_  . ,'.|==|-   ,   /|==|  /\ , |
       /==/  /\ ,  )==/ , _  .' /==/, | |- |
       `--`-`--`--'`--`..---'   `--`./  `--`
```

`dawgrun` is a work-in-progress developer tool for interacting with
`DAWGS` and the data structures it produces.

It currently runs as a REPL for introspecting a `DAWGS`-compatible
Postgres graph, parsing and translating Cypher queries, and executing
queries against a live connection.

## Building

From a `DAWGS` checkout:

    go tool dawgrun

With a customized `DAWGS` clone, for testing features, version differences, etc:

    cd tools/dawgrun
    just build-with-dawgs path/to/DAWGS

To switch the build back to mainline:

    cd tools/dawgrun
    just build-with-upstream

## Running

You are dropped into a prompt:

    dawgrun >

At any time, run `help` to list commands or `help <command>` for
detailed usage, flag defaults, and description.

## Commands

The REPL supports command-name completion with `Tab`; ambiguous matches render a transient popover list near the prompt and can be dismissed with `Esc`.

Available commands:

```
    exit                            Quit
    explain-psql                    Explains a translated query over an active PG connection
    help                            This help message, but also more detailed help for individual commands
    load-db-kinds                   Loads/shows the kind mapping from the specified DB into the 'active set'
    lookup-kind                     Looks up a kind from database based on kind name
    lookup-kind-id                  Looks up a kind from database based on kind ID
    open-pg-db                      Connects to a specified DAWGS-compatible Postgres DB to do graph introspection.
    parse                           Parses and dumps a Cypher query to AST form.
    query-cypher                    Executes a Cypher query and renders table or JSON output
    quit                            Quit
    runtime-trace                   Manage runtime tracing
    translate-psql                  Parses a query and converts it to the underlying PostgreSQL query
```

### Connections and kind maps

Most commands that touch a database take a connection _name_ as their
first argument. Names are assigned when you open the connection and
are reused for the remainder of the session. The bottom-right status
widget shows the current number of open connections.

A "kind map" is the mapping between a graph's kind names (e.g.
`User`, `Group`) and the numeric IDs they are stored under in
Postgres. Commands that need to translate between the two
(`lookup-kind`, `lookup-kind-id`, and the `-conn` mode of
`translate-psql`) will lazily fetch a kind map from the database the
first time they need it; `load-db-kinds` forces an immediate refresh
and dumps the result.

## Examples

### Open a Postgres connection

    dawgrun > open-pg-db local "postgres://dawgs:dawgs@localhost:5432/dawgs?sslmode=disable"
    Opened connection 'local': postgres://dawgs:dawgs@localhost:5432/dawgs?sslmode=disable

The first argument (`local`) is the name other commands will refer
to; the second is any DAWGS-compatible Postgres connection string.

### Inspect kinds

Load and dump the kind mapping from a connection:

    dawgrun > load-db-kinds local

Resolve a kind name to its numeric ID:

    dawgrun > lookup-kind local User
    Kind User => 3

…or go the other direction:

    dawgrun > lookup-kind-id local 3
    Kind ID 3 => User

### Parse a Cypher query to AST

    dawgrun > parse "match (n:User) where n.name = 'alice' return n"

The REPL highlights the dumped AST as Go source.

> **Quoting note:** the REPL splits input with shell-style rules
> (via `shlex`), so double quotes are consumed by the line parser
> before the Cypher parser ever sees them. Cypher string literals
> must use single quotes, and queries that contain them are easiest
> to pass as a single double-quoted argument, e.g.
> `"match (n) where n.name = 'alice' return n"`.

### Translate Cypher to PostgreSQL

Without a connection (kinds remain symbolic):

    dawgrun > translate-psql match (n:User) return n limit 10

With a connection, so that kind names are resolved to the IDs in the
target database:

    dawgrun > translate-psql -conn local match (n:User) return n limit 10

To also see the translator's internal SQL AST alongside the formatted
query:

    dawgrun > translate-psql -conn local -dump-pg-ast match (n:User) return n limit 10

### Ask Postgres to EXPLAIN a translated query

Runs the translation, prepends `EXPLAIN`, and dispatches it over the
named connection:

    dawgrun > explain-psql local "match (n:User) where n.name = 'alice' return n"

### Execute a Cypher query

Default `table` output:

    dawgrun > query-cypher local match (n:User) return n.name, n.objectid limit 5

`json` output, useful for piping into other tooling:

    dawgrun > query-cypher -format json local match (n:User) return n.name, n.objectid limit 5

An empty result set renders as `(0 rows)` in table mode and `[]` in
JSON mode.

### Runtime tracing

Capture a Go runtime trace for a subsequent command or block of work.
The trace file defaults to `trace.out` in the current directory:

    dawgrun > runtime-trace start
    dawgrun > query-cypher local match (n) return count(n)
    dawgrun > runtime-trace stop

Open the resulting trace with `go tool trace trace.out`.

## History

The REPL persists command history to
`$XDG_CONFIG_HOME/dawgrun/history.txt` (or the platform equivalent),
capped at 1000 lines, so recent commands are available via the
arrow keys across sessions.

## Styling

Syntax highlighting style defaults to `monokai`, but can be configured via
the `DAWGRUN_STYLE` environment variable. 
Any styles in [Chroma](https://github.com/alecthomas/chroma/tree/master/styles) are available for use as a syntax highlighting style. 
