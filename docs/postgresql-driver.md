# PostgreSQL driver

Connection pools, kind metadata, schema upgrades, and storage operations for the PostgreSQL backend.

[Documentation index](README.md) · [Project overview](../README.md)

## Graph selection and schema assertion

PostgreSQL `WithGraph` selects an existing graph without creating partitions or
reconciling indexes/constraints. Missing graphs return an error. Explicit schema
assertion creates or reconciles every declared graph, including secondary graphs
and previously cached graphs. Kind registration and mapping inside transactions
use the connection already owned by that transaction. New kind IDs become shared
metadata only after commit, so rollback does not leave cached nonexistent IDs.

## Compilation caches

The PostgreSQL driver bounds repeated work with immutable 256-entry Cypher AST and SQL translation caches. The shared AST
cache is sharded to avoid serializing unrelated query shapes while preserving same-query AST reuse. Translation
entries are keyed by normalized query text, graph ID, a collision-safe parameter-name/type shape, and the effective
versioned traversal-policy identity; they retain SQL
and parameter-source mappings, never request values or defaults, and fail closed when a required source value is absent.
Cached query text is released by LRU eviction or driver close, and diagnostics expose aggregate counters without query
text.

## PostgreSQL connection-local translation cache

The PostgreSQL driver uses connection-local SQL translation caches. Pool construction owns the required connection
lifecycle hooks; callers continue to hand the resulting `*pgxpool.Pool` to `dawgs.Open`:

```go
poolConfig, err := pgxpool.ParseConfig(connectionString)
if err != nil {
	return err
}

pool, err := pg.NewPool(poolConfig)
if err != nil {
	return err
}
database, err := dawgs.Open(ctx, pg.DriverName, dawgs.Config{Pool: pool})
if err != nil {
	return err
}
defer database.Close(ctx)
```

`pg.DefaultRuntimeConfig()` uses 64 retained translation entries per live physical PostgreSQL connection and a 5-50
connection pool. Pass `pg.RuntimeConfig{TranslationCacheEntries: 0}` to `pg.NewPoolWithRuntimeConfig` to disable
connection-local retention, or configure exact bounds with `pg.RuntimeConfig{TranslationCacheEntries: 64,
SharedShortestPathTemplateEntries: 128, Pool: &pg.PoolConfig{MinConnections: 0, MaxConnections: 4}}`. The shared
shortest-path tier retains only immutable SQL templates and fresh bindings are always negotiated per execution; set its
capacity to zero to disable it. Negative cache capacity, negative minimums, zero maximums, and inverted limits are
rejected. The theoretical aggregate entry bound is live physical connections multiplied by this capacity, not a global
bound.

A cache remains with its physical `*pgx.Conn` across pool lease release and reacquisition, and is removed when that
connection closes or the driver closes. `TranslationCacheStats` reports opaque diagnostic connection IDs and aggregate,
query-text-free counters only. Cached entries retain immutable SQL and parameter-source metadata; every hit binds the
current caller values. The driver keeps the parse cache driver-wide, caches neither results nor routing decisions, and advances
its generation after successful schema assertion and kind refresh. Stable-snapshot traversal workspaces are marked ready
only for the current physical connection and schema generation, then reinitialized after reset, closure, or generation
change. For out-of-band schema changes that affect types or generated SQL, reset or recreate the pool before continuing.

PostgreSQL MATCH treats unregistered node labels and relationship types as empty matches without registering them.
Known relationship-type alternatives remain eligible; label conjunctions, negation and OPTIONAL MATCH retain their
normal semantics. Lookup, cancellation and database failures still return errors. Plans that depend on missing kinds
bypass translation retention so later registration is visible; CREATE retains its kind-registration behavior.

PostgreSQL kind metadata uses immutable shared snapshots and small private transaction
maps. Attaching to an existing graph loads the catalog before exposing the first
transaction; concurrent first readers share that load. Schema assertion creates
storage before servicing pending metadata refreshes, including after a failed refresh.
Warm fluent, Cypher and Raw graph reads use cached mappings without catalog
queries or copying the dictionary. Name misses look up only the missing names on
the owned connection; unknown reads never register kinds or retain negative results.
Registrations become shared after their actual commit, including successful
autocommit registration statements used by batches.

An unknown result kind ID keeps the mapping error and signals a full reload at a
later idle query boundary, including before an explicit transaction starts. Result
decoding performs no catalog SQL and does not retry the failed query. Concurrent
reload requests are coalesced; there is no timer, polling or default refresh cadence.
External kind additions become visible through name misses or this result-miss
signal. After externally deleting kind metadata or resetting its catalog, commit
those changes and call `driver.RefreshKinds(ctx)` on every affected driver instance
before continuing. Raw SQL catalog writes are opaque to the driver; cached positive
entries cannot detect their own deletion, so there is no automatic freshness bound.
Explicit refresh replaces both mapping directions and invalidates both translation
layers; automatic reloads also invalidate translations when mappings disappear or
change. `GetKindIDsByKind` continues to return an independently mutable copy.

READ COMMITTED transactions and batch flushes adopt refreshed mappings before
binding kind IDs. After a destructive refresh, repeatable-read and serializable
transactions read the catalog once through their own transaction and retain that
private view. This establishes PostgreSQL's snapshot if no data statement has run
yet, or preserves the catalog visible to an existing snapshot, so mappings match
the rows being read. Unchanged warm queries need no catalog read. Private
registrations remain visible to their owning transaction and cannot republish an
obsolete catalog view.

`TranslationCacheStats().SQLGeneration` reports `KindLookup`, `KindRefresh` and
`KindRegistration` counts and durations for actual metadata work. Their `Dispatch`
field includes reading and closing metadata results. Ordinary cached metadata
checks emit no timing sample and read no clock. See the
[P1 performance and validation results](../perf_regressions.md#p1-implementation-results)
for regression coverage, measurements and their limits.

Closed pools are collectable after callers release them.
The pool/runtime registry and runtime's pool reference are weak, so retaining or reusing `Pool.Config()` does not
retain closed original pools. `Driver.Close` still releases its state synchronously.

After schema assertion, applications may opt in to pre-prepare selected hot PostgreSQL statements without executing them:

```go
if err := database.WarmStatements(ctx, "select 1"); err != nil {
	return err
}
```

Warm-up touches each currently idle physical connection and uses pgx's normal `CacheStatement` identity, so the first
regular execution adopts the prepared server statement rather than creating a second one. The driver records only SHA-256
statement identities in its lifecycle state; `TranslationCacheStats` includes aggregate workspace and warm-up counters.
For a persistent, explicitly selected warm set that also applies to new physical connections, use
`database.SetStatementWarmupPolicy(ctx, statements...)`; passing no statements clears the future warm set. Pools supplied
to `pg.NewDriver` must have been created by `pg.NewPool` or `pg.NewPoolWithRuntimeConfig` so those lifecycle hooks are in
place.

## Driver-wide compilation cache

The PostgreSQL driver has a bounded, driver-wide Cypher compilation cache for both text Cypher and programmatic graph
queries. Its default capacity is 256 entries; callers that need a different capacity can use
`pg.NewDriverWithOptions`. Builder identities include a bounded typed AST digest, so ASTs that render the same Cypher
text cannot reuse one another's SQL. Runtime parameter values are rebound on each hit; unsupported fingerprint shapes
compile without cache reuse. The process-wide `pg.SetOptimizedTranslation` switch selects baseline translation for
newly started compilations across every PostgreSQL driver in the process. See
[PostgreSQL translation](postgresql_translation.md#translation-cache) for the cache contract and rollback controls.

## Storage maintenance

The PostgreSQL driver requires PostgreSQL 16 or newer; PostgreSQL 18 is recommended. `OptimizeStorage` performs
storage maintenance on the `node` and `edge` tables in the following order:

1. Drops INVALID btree indexes on `node` and `edge` partitions whose names end in `_ccnew[N]` or `_ccold[N]`. These are
   left behind when a `REINDEX CONCURRENTLY` is interrupted. Drops use `DROP INDEX CONCURRENTLY IF EXISTS`.
2. Runs `VACUUM (ANALYZE) node, edge`, which covers every partition.
3. Rebuilds every valid btree index on `node` and `edge` partitions with `REINDEX INDEX CONCURRENTLY`, smallest first,
   so that an early cancellation still completes as many rebuilds as possible. GIN indexes are not rebuilt; `VACUUM`
   already merges their pending lists.

Concurrent drops and rebuilds take `SHARE UPDATE EXCLUSIVE` locks, which do not block reads or writes. A failure on one
statement does not stop the remaining work; every failure is returned together as a joined error. Context cancellation
stops the pass before the next statement and is included in the returned error. A cancelled rebuild may leave an
INVALID `_ccnew` index behind, which the next pass drops in step 1. The pass applies no thresholds and has no time
limit; callers are responsible for scheduling it, for example behind a cooldown.

## Indexes and upgrades

PostgreSQL translates exact string property equality with a JSON string type guard and `properties ->>` extraction, so
indexes created on expressions such as `properties ->> 'objectid'` and `properties ->> 'name'` can be used for selective
anchors without matching JSON booleans or numbers. The PostgreSQL edge schema uses four baseline B-tree indexes per
partition: the primary key, a unique `(start_id, kind_id, end_id, graph_id) INCLUDE (id)` index that also covers outbound
traversal, an `(end_id, kind_id) INCLUDE (id, start_id)` inbound traversal index, and a narrow `(kind_id)` index for typed
counts and deletes. Avoiding heap reads during traversal requires topology-only reads and all-visible heap pages;
Cypher relationship counts that validate endpoint existence may still fetch edge rows for endpoint IDs.

Schema assertion replaces the historical edge uniqueness constraints and removes the superseded covering indexes on
existing partitions. This first upgrade takes table locks and rebuilds the affected indexes; schedule it for a suitable
maintenance window on large databases. Subsequent assertions preserve the replacement indexes. See
[indexing notes](postgresql_translation.md#indexing-notes) for the layout and regression coverage.

PostgreSQL property index regression coverage is hard-failing under the `manual_integration` tag. The synthetic plan
test translates Cypher to PgSQL, disables sequential scans for the `EXPLAIN`, and requires explicit node property
indexes to appear in the JSON plan.
