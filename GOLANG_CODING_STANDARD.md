# Go Coding Standard

This standard captures the preferred Go style used in `dawgs`. Follow `gofmt`
first, then apply the conventions below when choosing code shape.

## Receiver Names

Use `s` as the receiver name for methods, regardless of the concrete receiver
type. The goal is to remove per-type receiver-name churn and reduce cognitive
load while reading method bodies.

```go
// Start begins serving requests.
func (s *Server) Start() error {
	go s.loop()
	return nil
}

// Validate reports whether the configuration is supported.
func (s Config) Validate() error {
	if s.Firewall.Backend != "nftables" {
		return fmt.Errorf("unsupported firewall backend %q", s.Firewall.Backend)
	}

	return nil
}
```

Avoid type-derived receiver names such as `srv`, `cfg`, `db`, or `wm` unless a
local collision makes `s` unusable.

## Identifier Names

Choose names that communicate ownership, purpose, and transformation. Avoid
shadowing Go predeclared identifiers such as `copy`, `len`, `new`, or `error`,
even though the language permits it. Shadowing makes later expressions harder
to distinguish from built-in operations and increases the risk of accidental
misuse during refactoring.

Name derived values for what they represent rather than how they happened to be
created. For example, prefer `clientCopy`, `boundedClient`, or
`normalizedIssuer` over generic names such as `copy`, `value`, or `result` when
the more specific meaning is known.

```go
// withBoundedTransport copies client and applies the required bounded transport.
func withBoundedTransport(client *http.Client) *http.Client {
	boundedClient := *client
	boundedClient.Transport = boundedTransport{base: client.Transport}

	return &boundedClient
}
```

## Parameter Reassignment

Do not reassign function or method parameter variables. Parameters name the
inputs supplied by the caller; keeping those bindings stable makes later uses
unambiguous and prevents normalization or defaulting from silently changing
what the original name represents.

Bind transformed or validated values to a new local with a descriptive name:

```go
func Open(path string) error {
	cleanPath := filepath.Clean(path)
	if cleanPath == "." {
		return errors.New("path is required")
	}

	return openCleanPath(cleanPath)
}
```

This rule applies to direct assignment, compound assignment, and using a
parameter as the destination of an operation that replaces its value. Mutating
an object referenced by a parameter is a separate API-design choice and should
be explicit in that function's contract.

## Vertical Spacing

Use single blank lines inside functions and closures to separate logical
paragraphs. The driver packages are the reference style: setup, validation,
resource acquisition, side-effecting work, mutation, and final return each get
their own visual space when they are independent steps.

Keep validation blocks at the top of a function compact when they are a single
gate into the rest of the operation.

```go
if len(kindIDs) == 0 {
	return graph.Kinds{}, true
}

if mappedKinds, err := kindMapper.MapKindIDs(ctx, kindIDs); err == nil {
	return mappedKinds, true
}

return nil, false
```

Separate a multi-line call from the next independent operation. This includes
logging, progress emission, database calls, and other side-effecting calls.

```go
slog.Info("retriever load node phase started",
	slog.String("graph", graphEntry.Name),
	slog.Int64("node_count", graphEntry.NodeCount),
)
progress.emit(ProgressEvent{
	Operation: OperationLoad,
	Message:   "retriever load node phase started",
	Graph:     graphEntry.Name,
})

nodeMap, nodeCount, err := loadGraphNodes(ctx, db, graphEntry)
if err != nil {
	return 0, 0, err
}
```

Within loops and callbacks, give each filter, transformation, mutation, and
checkpoint its own paragraph when the block does more than a couple of trivial
statements.

```go
for _, node := range nodes {
	kinds := node.Kinds.Strings()
	sort.Strings(kinds)
	addKindsToSet(nodeKinds, kinds)

	properties := node.Properties.MapOrEmpty()
	if activeScrubber != nil {
		properties = activeScrubber.scrubProperties(properties)
	}

	item := FragmentNode{
		ID:         node.ID.String(),
		Kinds:      kinds,
		Properties: properties,
	}

	items = append(items, item)
	if len(items) >= shardSize {
		if err := flush(); err != nil {
			return err
		}
	}
}
```

Do not add blank lines between statements that are one tight operation, such as
creating a value and immediately returning it, assigning a field and returning
the receiver, or a short `if`/`else if` chain that represents one decision.

## Error Handling and Scope

Prefer initializer-backed `if` statements for operations that can fail. Handle
the error at the point it is raised, and keep successful values scoped to the
branch that uses them.

```go
if cfg, err := server.ReadConfiguration(cfgPath); err != nil {
	log.Fatalf("Error reading config: %v", err)
} else if dbInst, err := db.NewDatabase(cfg.DBPath); err != nil {
	log.Fatalf("Error opening database: %v", err)
} else {
	// cfg and dbInst are only available where they are valid.
}
```

Use `else if` chains when each later step depends on the successful output of
the previous step. This keeps the happy path close to the failure path and
prevents partially initialized variables from leaking into wider scopes.

```go
if content, err := os.ReadFile(path); err != nil {
	return cfg, err
} else if err := toml.Unmarshal(content, &cfg); err != nil {
	return cfg, err
} else {
	cfg = cfg.withDefaults()
	return cfg, nil
}
```

When an error requires a special-case branch, keep that branch nested at the
error site.

```go
if record, err := s.db.GetHostRecord(match.IPAddress); err != nil {
	if !errors.Is(err, db.ErrNotFound) {
		return err
	}

	// Create the missing record here.
} else {
	// Update the existing record here.
}
```

Use plain early returns when there is no useful success value to scope or when
the initializer chain would make the code harder to follow.

## Return Values

Reserve multiple return values for a result and its status, such as `(value,
error)` or `(value, found)`. When an operation produces multiple coequal values,
define a named result struct instead of returning an ad hoc tuple. The named
fields document the relationship between the values and allow the result to
evolve without expanding every call site's assignment list.

```go
// OpenServices constructs the services that share one checked client.
func OpenServices(ctx context.Context) (Services, error) {
	// Construction omitted.
}
```

Avoid signatures such as `(Reader, Writer, Closer, error)`. Group `Reader`,
`Writer`, and `Closer` in a `Services` result and return `(Services, error)`.

## Variable Grouping

Group related local variables aggressively with `var` blocks. This is preferred
when multiple locals establish the state for the same operation, especially when
some values are initialized and others are intentionally zero-valued.

```go
var (
	records HostRecords

	txn    = s.db.NewTransaction(false)
	iter   = txn.NewIterator(badger.DefaultIteratorOptions)
	prefix = []byte(recordKeyPrefix)
)
```

Use blank lines inside the group to separate conceptual clusters. Prefer a
single grouped declaration over several adjacent `var` statements.

For short-lived values used immediately, `:=` is still appropriate.

```go
line := scanner.Text()
```

## Logical Spacing

Use blank lines inside functions to separate logical phases. This is guidance,
not a hard formatting rule: prefer readability over mechanically inserting
empty lines.

Common phase boundaries include setup before control flow, guard checks before
work, resource acquisition before deferred cleanup and use, and mutation before
the final return.

```go
transaction, err := s.renderFirewallTransaction(state)
if err != nil {
	return err
}

log.Infof("Applying nftables transaction. Banned %d hosts.", state.Count())
if output, err := s.runNftCommand(s.nftPath, transaction.Payload, "-j", "-f", "-"); err != nil {
	return fmt.Errorf("backend apply failed running %s: %s: %w", s.nftPath, output, err)
}

s.firewallState = state
return nil
```

Within loops, use blank lines to make each filtering or transformation step
stand on its own.

```go
for _, record := range hostRecords {
	if !now.Before(record.NextRefresh) {
		continue
	}

	ipAddress := net.ParseIP(record.IPAddress)
	if ipAddress == nil || ipAddress.To4() == nil {
		continue
	}

	if staticCIDRAllows(cfg.StaticAllowCIDRs, ipAddress) {
		continue
	}

	state.BannedIPv4[record.IPAddress] = record
}
```

In `select` statements, separate cases with blank lines when each case performs
distinct work.

```go
select {
case <-refreshTicker.C:
	s.update()

case <-s.updateC:
	s.update()

case <-s.joiner.StopC:
	return
}
```

Avoid splitting tightly coupled statements when the second line is the immediate
effect of the first.

## Documentation Comments

Every function and method declaration and every struct or interface definition
must have a semantically relevant Go doc comment, whether it is exported or
unexported. Document every struct field and interface member individually,
including embedded fields and embedded interface elements. Follow Go doc form
by starting each comment with the declared identifier when applicable.

Comments must explain the declaration's purpose, meaning, behavior, or contract.
Merely restating the identifier without adding useful information does not
satisfy this requirement.

```go
// RecordStore retrieves and persists records.
type RecordStore interface {
	// Find returns the record identified by key.
	Find(ctx context.Context, key string) (Record, error)

	// Save persists record and returns any write failure.
	Save(ctx context.Context, record Record) error
}
```

## Function Ordering

Prefer ordering functions in the same file so dependencies appear before the
functions that call them, when that makes the file read naturally. This is
guidance, not a hard formatting rule: public entry points, framework
conventions, and established local ordering may take precedence.

## Struct Definitions

Write struct type definitions across multiple lines, with one field per line.
Align naturally with `gofmt`; do not compress structs onto one line.

```go
// FirewallConfig controls how the firewall backend manages bans.
type FirewallConfig struct {
	// Backend selects the firewall implementation.
	Backend string `toml:"backend"`

	// Table identifies the firewall table managed by the backend.
	Table string `toml:"table"`

	// BanSet identifies the set containing banned addresses.
	BanSet string `toml:"ban_set"`

	// Family selects the address family managed by the backend.
	Family string `toml:"family"`

	// DryRunSummaryOnly limits dry-run output to a summary.
	DryRunSummaryOnly bool `toml:"dry_run_summary_only"`
}
```

Use field names in struct literals, especially for exported types, config
types, tests, and any literal with more than one field.

```go
return Server{
	cfg:           cfg,
	db:            dbInst,
	firewallState: NewFirewallState(),
	updateC:       updateC,
	joiner:        NewJoinChannelPair(),
	nftPath:       nftPath,
	nftBackend:    NewNftablesBackend(cfg.Firewall),
	runNftCommand: defaultNftCommandRunner,
}, nil
```

Prefer multi-line keyed literals even when a literal is currently small, because
they remain stable as fields are added and make diffs easier to read.

```go
freshWatch := db.File{
	Path:   event.Name,
	Offset: 0,
}
```

## Structured Data and Wire Formats

Represent stable structured data with named types. HTTP responses, persisted
records, protocol messages, configuration objects, and other contract-bearing
shapes must not use `map[string]any` or an anonymous collection merely to avoid
declaring a type. Named fields make the schema reviewable, preserve compile-time
checking, and keep producers and consumers synchronized as the contract evolves.

Document the type and every field according to the documentation rules above.
Use JSON tags and typed optional fields, including pointers or `omitempty` when
appropriate, to express wire-format behavior explicitly.

```go
// StatusResponse describes the public status returned by the service.
type StatusResponse struct {
	// Status identifies the current bounded service state.
	Status string `json:"status"`

	// CompletedAt identifies when the terminal state was reached, when known.
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}
```

Maps remain appropriate when keys are genuinely dynamic and are themselves
part of the data model. They are not a substitute for a known record shape.

## Shared Validation

Centralize validation rules that represent the same invariant in more than one
place. Identifier syntax, ASCII and control-character checks, canonical HTTPS
URLs, DNS hostnames, bounded text, and fixed allowed-value checks are examples
of security-sensitive rules that must not acquire package-specific variations.

The package that owns an invariant should expose a narrowly named validation
function when other packages need the same rule. Consumers must use that
function instead of copying its conditional expression. If no existing domain
package can own the invariant without introducing an import cycle, place it in
a small dependency-neutral validation package with a deliberately limited API.

Shared validators must define their complete contract, including normalization,
length units, accepted character set, canonical form, and whether an empty value
is valid. Callers must not silently repair values before validation unless the
contract explicitly calls for normalization.

```go
// ValidIdentifier reports whether value is a canonical bounded identifier.
func ValidIdentifier(value string) bool {
	return identifierPattern.MatchString(value)
}
```

## Semantic Constants and Domain Vocabulary

Use well-named constants for literals that carry domain, protocol, security, or
operational meaning. This includes routes, header names, content types, claim
names, algorithms, environment variables, flag names, audit vocabulary, state
values, delimiters, character boundaries, and size or count limits. Repeating a
literal is a strong signal that it needs a name, but a single occurrence also
needs a constant when its meaning is contractual or would otherwise require a
comment to explain it.

Define related vocabulary once in the package that owns the domain contract.
Producers, validators, persistence code, and presentation code must refer to the
same definitions so accepted values cannot drift from emitted values. Export a
constant only when another package legitimately consumes that package's
contract; do not export it merely to avoid deciding which package owns the
concept.

Give constants domain-specific types when doing so prevents unrelated values
from being mixed. Keep implementation-only constants unexported and scoped to
the narrowest package that owns them.

```go
// AuditResult identifies a bounded terminal audit outcome.
type AuditResult string

const (
	// AuditResultSuccess indicates that the audited operation completed.
	AuditResultSuccess AuditResult = "success"

	// AuditResultRejected indicates that policy rejected the audited operation.
	AuditResultRejected AuditResult = "rejected"
)
```

Package-level constants remain subject to the individual documentation and
group-spacing requirements below.

## Static Assets

Keep substantial HTML, templates, schemas, and other static assets in dedicated
files rather than inline raw-string literals in Go source. Embed repository-owned
assets with `embed.FS` and load them through APIs such as `template.ParseFS`.
This preserves syntax-aware editing, keeps Go control flow readable, and makes
asset changes independently reviewable.

```go
// assets contains the server-owned templates compiled into the binary.
//
//go:embed templates/*.html
var assets embed.FS

// loginTemplate renders the server-owned login page.
var loginTemplate = template.Must(template.ParseFS(assets, "templates/login.html"))
```

Embedded assets must remain server-owned, bounded, and parsed during startup or
package initialization so malformed content fails before request handling.

## SQL Organization

Define nontrivial SQL statements as documented constants rather than embedding
raw queries directly in database calls. Group related queries in a cohesive
constant block or a domain-focused query file such as `identity_queries.go`.
Name each query for the operation it performs, not for incidental table names.

Database methods should make validation, transaction boundaries, control flow,
and result handling visible while referring to SQL through those operation
names. Parameters must continue to be passed separately through the database
API; constructing SQL by concatenating caller-controlled values is prohibited.

```go
const (
	// selectPrincipalForUpdateQuery locks one principal for an authority decision.
	selectPrincipalForUpdateQuery = `
		SELECT principal_id, enabled
		FROM principals
		WHERE principal_id = $1
		FOR UPDATE`
)
```

## Practical Defaults

Keep code direct and local. Favor readable control flow over abstractions that
only hide one or two calls.

Extract a focused helper when a repeated multi-step sequence establishes one
behavioral contract, such as route registration or a shared parse-and-validate
workflow. The helper name and signature should explain the invariant it owns.
Do not extract wrappers that merely rename a single obvious operation or conceal
important control flow without eliminating meaningful duplication.

Return zero values explicitly on error for multi-return functions.

```go
if watcher, err := fsnotify.NewWatcher(); err != nil {
	return FileWatcher{}, err
} else {
	return FileWatcher{
		cfg:     cfg,
		watcher: watcher,
	}, nil
}
```

Defer cleanup immediately after acquiring a resource.

```go
fin, err := os.Open(fileWatch.Path)
if err != nil {
	return uniqueBanHosts, uniqueAllowHosts
}
defer fin.Close()
```

Use package-level grouped `const` and `var` declarations for related values.
Every package-level (global) `var` and `const` entity must have a semantically
relevant Go doc comment, whether it is exported or unexported. Document each
member of a grouped declaration individually. Comments on function-local `var`
declarations are optional and left to the author's discretion.

```go
// ErrNotFound indicates that the requested record does not exist.
var ErrNotFound = errors.New("not found")

const (
	// fileWatchKey formats a file watch key.
	fileWatchKey KeyFormat = "file_watch.%s"

	// hostRecordKey formats a host record key.
	hostRecordKey KeyFormat = "hosts.%s"

	// hostRecordKeyPrefix identifies the host record key namespace.
	hostRecordKeyPrefix KeyFormat = "hosts."
)
```

In grouped `var` and `const` declarations, treat each leading comment and the
member definition it documents as one unit. Separate that unit from the next
comment and member definition with exactly one blank line. The final member
definition may instead be followed directly by the closing `)`.

```go
var (
	// expansionRootFilter identifies the recursive traversal root filter.
	expansionRootFilter = pgsql.Identifier("traversal_root_filter")

	// expansionTerminalFilter identifies the recursive traversal terminal filter.
	expansionTerminalFilter = pgsql.Identifier("traversal_terminal_filter")

	// expansionPairFilter identifies the recursive traversal pair filter.
	expansionPairFilter = pgsql.Identifier("traversal_pair_filter")
)
```
