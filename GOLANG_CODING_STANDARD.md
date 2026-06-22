# Go Coding Standard

This standard captures the preferred Go style used in `gtfo`. Follow `gofmt`
first, then apply the conventions below when choosing code shape.

## Receiver Names

Use `s` as the receiver name for methods, regardless of the concrete receiver
type. The goal is to remove per-type receiver-name churn and reduce cognitive
load while reading method bodies.

```go
func (s *Server) Start() error {
	go s.loop()
	return nil
}

func (s Config) Validate() error {
	if s.Firewall.Backend != "nftables" {
		return fmt.Errorf("unsupported firewall backend %q", s.Firewall.Backend)
	}

	return nil
}
```

Avoid type-derived receiver names such as `srv`, `cfg`, `db`, or `wm` unless a
local collision makes `s` unusable.

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

## Function Ordering

Prefer ordering functions in the same file so dependencies appear before the
functions that call them, when that makes the file read naturally. This is
guidance, not a hard formatting rule: public entry points, framework
conventions, and established local ordering may take precedence.

## Struct Definitions

Write struct type definitions across multiple lines, with one field per line.
Align naturally with `gofmt`; do not compress structs onto one line.

```go
type FirewallConfig struct {
	Backend           string `toml:"backend"`
	Table             string `toml:"table"`
	BanSet            string `toml:"ban_set"`
	Family            string `toml:"family"`
	DryRunSummaryOnly bool   `toml:"dry_run_summary_only"`
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

## Practical Defaults

Keep code direct and local. Favor readable control flow over abstractions that
only hide one or two calls.

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

```go
const (
	ErrNotFound = errors.New("not found")

	fileWatchKey        KeyFormat = "file_watch.%s"
	hostRecordKey       KeyFormat = "hosts.%s"
	hostRecordKeyPrefix KeyFormat = "hosts."
)
```
