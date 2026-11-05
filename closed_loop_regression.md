# User Role Assignments regression: findings and targeted iteration plan

## Context and limits

- Reported PostgreSQL processing times: production **32 minutes**, local **72 minutes**, a **2.25×** slowdown.
- The user confirmed equivalent datasets and existing post-processed edges; the dataset is unavailable for this investigation.
- Local DAWGS revision reviewed: `74727f20`, with existing working-tree changes.
- Code and microbenchmark comparison baseline: `v0.10.0`. The production DAWGS revision remains unknown.
- This investigation identifies measurable overhead and candidate amplifiers. It does not yet explain the full 40-minute difference.

## Vetted findings

### Fingerprinting is the strongest measured regression

The new structural fingerprint runs before the builder compilation-cache lookup,
including on cache hits. It measured approximately **13 µs per builder query**
against `v0.10.0`.

The exploratory database-free benchmarks measured:

| Query shape | `v0.10.0` cached | Reviewed checkout cached |
| --- | ---: | ---: |
| Group members | 21.4 µs | 33.2 µs |
| Role members | 14.9 µs | 29.5 µs |
| Tenant roles | 18.2 µs | 32.2 µs |

These benchmarks include query building, preparation, and compilation-cache
lookup/binding. They exclude database execution and scanning. The baseline has
one sample per benchmark, while the checkout has three. The two formatting
passes already existed on `v0.10.0`; fingerprinting is the new preparation cost
in that comparison.

The measured cached-path ratios are below the full operation's 2.25× slowdown.
Attribution requires actual query counts, CPU/GC measurements, and measurements
of the omitted work.

Relevant code: [compiler.go](drivers/pg/compiler.go),
[builder_cache_identity.go](drivers/pg/builder_cache_identity.go).

### Cache reuse is only partially validated

Changing IDs produced one miss followed by 99 hits for each of four isolated
query shapes with preloaded kinds. This validates those shapes under those
conditions. It does not establish cache behavior across actual transaction
state, invalidation, contention, or mixed-workload eviction.

Removing `AZHasRole` produced 100 misses, 100 bypasses, and no insertions, even
though the query could still match `AZMemberOf`. That diagnostic uses an
in-memory mapper: it proves translation-cache exclusion, not database catalog
I/O. The actual transaction mapper does query the catalog again for absent
kinds. Normal BHE/BHCE schema initialization registers these kinds, making this
a conditional amplifier.

Relevant code: [diagnostics](drivers/pg/azure_role_cache_diagnostic_test.go),
[transaction_kinds.go](drivers/pg/transaction_kinds.go),
[translation_cache.go](drivers/pg/translation_cache.go).

### Net row-hydration performance remains unknown

The exact-number JSON decoder subroutine is slower than standard JSON decoding,
but newer composite decoding has offsetting improvements. Additional isolated
benchmarks showed owned-node decoding faster than stock-map decoding, with
exact-number JSON enabled in both arms:

| Scalar node decoding | Time | Allocations |
| --- | ---: | ---: |
| Stock map composite + exact JSON | 3.27–3.42 µs | 45 |
| Owned composite + exact JSON | 2.26–2.27 µs | 29 |

Neither comparison measures the complete old/new row-hydration path. Preserve
exact-number correctness when evaluating improvements.

Relevant code: [json_codec.go](drivers/pg/json_codec.go),
[composite_codec.go](drivers/pg/composite_codec.go),
[result.go](drivers/pg/result.go).

### Traversal amplification is established but preexisting

`ops/traversal.go` is unchanged from `v0.10.0`. Per-group queries, per-node leaf
probes, and repeated traversal work amplify driver overhead, but do not
independently establish a new DAWGS regression.

Writer-trigger overhead remains an unmeasured hypothesis. The relationship
batch path inserts/upserts into parent `edge`, so the graph-epoch statement
triggers execute. Their update to one row per graph creates a contention point
for concurrent same-graph writes. Normal batch flushes autocommit individually.

Relevant code: [traversal.go](ops/traversal.go),
[batch.go](drivers/pg/batch.go),
[statements.go](drivers/pg/statements.go),
[schema_up.sql](drivers/pg/query/sql/schema_up.sql).

### Existing observability misses relevant work

Two confirmed gaps can conceal recompilation or preparation costs:

- Builder queries in `liveQuery.runRegularQuery` do not emit the existing
  SQL-generation profiles.
- `compileWithMapper` calls `buildUncached` for transaction-state bypasses
  without incrementing the bypass counter.

Consequently, apparently healthy cache statistics can coexist with substantial
unaccounted compilation. Builder queries use `CompilationCacheStats()`;
`TranslationCacheStats()` reports the separate connection-local Cypher cache.

Relevant code: [query.go](drivers/pg/query.go),
[compiler.go](drivers/pg/compiler.go),
[driver.go](drivers/pg/driver.go),
[sql_generation_profile.go](drivers/pg/sql_generation_profile.go).

## Prioritized next steps

### 1. P0 — Restore normal PostgreSQL package testing

Resolve the `pgxmock`/`pgx` interface incompatibility. Ordinary targeted tests
currently fail to compile because the pinned mock implementation lacks methods
required by the selected pgx version, including `Rows.TypeMap` and
`BatchResults.Tx`.

Keep the explicit-file diagnostic script as exploratory tooling. The normal
unit suite and race checks need to run before performance changes can be
trusted.

**Acceptance:** normal PostgreSQL package unit tests and race checks compile
and pass without excluding the other driver tests.

### 2. P0 — Instrument the actual builder path

Extend the existing collector to measure:

- Builder construction, preparation, and fingerprinting.
- Traversal-budget AST inspection and kind servicing.
- Cache lookup/binding, actual translation, and SQL rendering.
- Dispatch, row consumption/decode, and rows returned.
- Pool-acquisition wait and batch-flush duration.

Add actual compilation counts and bounded bypass reasons, including missing
kinds, private transaction metadata, invalid identity, disabled caching, and
generation changes. Capture phase deltas rather than relying only on lifetime
totals. Use sampled detailed timings and bounded labels.

Define overlapping timings explicitly. The existing text-query `Cache` timer
includes translation and formatting, so those fields cannot simply be summed.
Dispatch ends at obtaining a row stream; it does not account for complete result
consumption. Keep consumer/backpressure time distinguishable from decode work.

**Acceptance:** every actual compilation is accounted for, and instrumentation
overhead is measured with collection disabled and enabled.

### 3. P0 — Build a reproducible synthetic PostgreSQL workload

Exercise `tx.Relationships()` and `ops` through the real driver lifecycle,
without introducing a BHE dependency. Include tenant enumeration, principal
enumeration, role lookups, membership traversals, tracker reads, and relationship
writes.

Vary empty groups, fan-out, overlapping memberships, role count, and property
size. Separate initial edge creation, unchanged reruns, and partial churn.
Record query counts, hydrated rows, mutation counts, CPU, allocations/GC, and
blocking.

Use repeated paired runs with the same fixture, toolchain, PostgreSQL settings,
CPU allocation, and pool configuration. Record exact code/dependency provenance
and restore equivalent database state between runs. Keep `v0.10.0` explicitly
provisional until the production revision is known.

**Acceptance:** identical results across revisions, reproducible scaling, and
enough timing coverage to locate the unexplained cost.

### 4. P1 — Extend cache tests to real transactions

Add Azure-shaped builder workloads to existing kind/cache tests. Cover warm
autocommit and explicit transactions, private/uncommitted kinds, missing-kind
registration, destructive catalog refresh, varying parameter values/list
lengths, and cache pressure.

Assert warm/cold bindings and results agree, ordinary warm queries perform no
catalog reads, and bypass telemetry matches actual builds. Missing-kind bypass
protects correctness; retaining those translations requires a sound
invalidation design.

**Acceptance:** real transaction-path tests establish reuse and necessary
bypasses, with matching results and complete telemetry.

### 5. P1 — Isolate database execution and write costs

Compare generated SQL and `EXPLAIN (ANALYZE, BUFFERS)` across revisions using
equivalent fixtures and statistics. Benchmark batch inserts, conflict updates,
and deletes with one writer and concurrent writers.

The epoch triggers update one row per graph, creating a concrete contention
point for concurrent same-graph writes. Measure lock waits and flush latency
before redesigning them. Compare same-graph and separate-graph writers and
representative batch sizes.

**Acceptance:** distinguish client compilation, database plans/execution, and
write-lock costs while preserving transactional epoch correctness.

### 6. P1 — Optimize fingerprinting with correctness protection

Once instrumentation establishes its workload contribution, prototype a typed
AST encoder or combine encoding with an existing traversal. Include the
additional traversal-budget AST walk, which the current microbenchmark omits.

Preserve structural distinctions, parameter rebinding, numeric types,
unsupported-kind handling, and AST ownership. Require repeated paired
benchmarks plus an improvement in the synthetic workload.

**Acceptance:** lower measured builder CPU/allocation and end-to-end workload
time, with unchanged results and existing cache-identity correctness tests
passing.

### 7. P2 — Investigate complete hydration and traversal improvements

Benchmark the complete old/new composite-decoding and graph-scanning paths
across realistic property sizes. Preserve exact-number correctness.

Pursue DAWGS traversal batching only where query counts justify it.
Path-dependent filtering makes global visited-node deduplication unsafe without
diamond, cycle, alternate-path, and ordering tests. BHE bitmap hoisting and the
boolean/string predicate correction should remain separately tracked upstream
work. The string-type guard is present in both compared `v0.10.0` and checkout
SQL, so it is not an established regression between those revisions.

**Acceptance:** measured workload improvements with equivalent traversal
semantics and exact hydrated values.
