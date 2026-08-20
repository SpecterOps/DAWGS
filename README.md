# DAWGS

Database Abstraction Wrapper for Graph Schemas

![A Corgi Treat](logo_small.png)

DAWGS provides tools and query helpers for running property graphs on vanilla PostgreSQL without extra database
plugins. It exposes a backend abstraction for graph queries, with current backend support for PostgreSQL and Neo4j.
The query interface is built around openCypher, including a PostgreSQL SQL translator for environments that do not
support Cypher natively.

The PostgreSQL driver bounds repeated work with immutable 256-entry Cypher AST and SQL translation caches. The shared AST
cache is sharded to avoid serializing unrelated query shapes while preserving same-query AST reuse. Translation
entries are keyed by normalized query text, graph ID, a collision-safe parameter-name/type shape, and the effective
versioned traversal-policy identity; they retain SQL
and parameter-source mappings, never request values or defaults, and fail closed when a required source value is absent.
Cached query text is released by LRU eviction or driver close, and diagnostics expose aggregate counters without query
text.

### Opt-in PostgreSQL v2 translation cache

`drivers/pg/v2` is an explicit Go API for evaluating connection-local SQL translation caches. It is not registered with
`dawgs.Open`, adds no connection-string scheme, and does not alter the established `pg` driver. Construct the pool and
driver directly:

```go
poolConfig, err := pgxpool.ParseConfig(connectionString)
if err != nil {
	return err
}

pool, err := pgv2.NewDefaultPool(ctx, poolConfig)
if err != nil {
	return err
}
database := pgv2.NewDriver(0, pool)
defer database.Close(ctx)
```

`pgv2.DefaultConfig()` uses 64 retained translation entries per live physical PostgreSQL connection and a 5-50 connection
pool. Pass `pgv2.Config{TranslationCacheEntries: 0}` to `pgv2.NewPool` to disable connection-local retention, or configure an exact bounded
pool with `pgv2.Config{TranslationCacheEntries: 64, SharedShortestPathTemplateEntries: 128, Pool: &pgv2.PoolConfig{MinConnections: 0, MaxConnections: 4}}`. The shared shortest-path tier retains only immutable SQL templates and fresh bindings are always negotiated per execution; set its capacity to zero to disable it. Negative cache capacity, negative minimums,
zero maximums, and inverted limits are rejected. The theoretical aggregate entry bound is live physical connections
multiplied by this capacity, not a global bound.

A cache remains with its physical `*pgx.Conn` across pool lease release and reacquisition, and is removed when that
connection closes or the driver closes. `TranslationCacheStats` reports opaque diagnostic connection IDs and aggregate,
query-text-free counters only. Cached entries retain immutable SQL and parameter-source metadata; every hit binds the
current caller values. V2 keeps the parse cache driver-wide, caches neither results nor routing decisions, and advances
its generation after successful schema assertion and kind refresh. Stable-snapshot traversal workspaces are marked ready
only for the current physical connection and schema generation, then reinitialized after reset, closure, or generation
change. For out-of-band schema changes that affect types or generated SQL, reset or recreate the pool before continuing.

After schema assertion, applications may opt in to pre-prepare selected hot PostgreSQL statements without executing them:

```go
if err := database.WarmStatements(ctx, "select 1"); err != nil {
	return err
}
```

Warm-up touches each currently idle physical connection and uses pgx's normal `CacheStatement` identity, so the first
regular execution adopts the prepared server statement rather than creating a second one. V2 records only SHA-256
statement identities in its lifecycle state; `TranslationCacheStats` includes aggregate workspace and warm-up counters.

## Quick Start

Build the repository:

```bash
make build
```

Run unit tests:

```bash
make test
```

Run integration tests when a backend is available:

```bash
export CONNECTION_STRING="postgresql://dawgs:weneedbetterpasswords@localhost:65432/dawgs"
export DAWGS_INTEGRATION_ALLOW_DESTRUCTIVE=1
export DAWGS_INTEGRATION_DISPOSABLE_TARGETS="postgresql://localhost:65432/dawgs"
make test_integration
```

Integration suites and fixture-loading GraphBench runs delete graph data. The
acknowledgement and credential-free target allowlist above are both required;
an absent or mismatched target is rejected before testing. Existing-graph
GraphBench runs reject mutating cases and do not require destructive
acknowledgement. PostgreSQL sessions remain read-write so temporary traversal
workspaces use the same reset strategy as production.

Use this module from another Go project:

```bash
go get github.com/specterops/dawgs
```

`make quality_backend` captures PostgreSQL and Neo4j integration results for backend equivalence comparison. It requires
`PG_CONNECTION_STRING` and `NEO4J_CONNECTION_STRING`. `make quality_bench` writes benchmark markdown and JSON captures
for later baseline comparison. Benchmark drift comparison can be performed by `make quality` through `tools/metrics` when
`BENCHMARK_REPORT` and `BENCHMARK_BASELINE` are provided.

Run the package benchmark suite with:

```bash
make test_bench
```

The direct-write regression benchmark is integration-scoped because it
measures real driver batch APIs. It reloads or clears its fixture outside the
timed region and validates post-state after every iteration:

```bash
DAWGS_INTEGRATION_ALLOW_DESTRUCTIVE=1 \
DAWGS_INTEGRATION_DISPOSABLE_TARGETS="postgresql://localhost:65432/dawgs" \
  CONNECTION_STRING="postgresql://dawgs:weneedbetterpasswords@localhost:65432/dawgs" \
  go test -tags manual_integration ./integration -run '^$' \
  -bench BenchmarkMutationSafeDirectWrites -benchtime=1x
```

Use `cmd/benchdiff` to compare benchmarks between two committed refs without changing the active worktree:

```bash
go run ./cmd/benchdiff -base main -target HEAD -kind unit
```

For integration benchmark comparisons, provide the same `CONNECTION_STRING` used by integration tests:

```bash
export CONNECTION_STRING="postgresql://dawgs:weneedbetterpasswords@localhost:65432/dawgs"
go run ./cmd/benchdiff -base main -target HEAD -kind all -driver pg -fail-regression 10%
```

The harness writes raw outputs and a Markdown report under `.bench/runs/` by default. The report begins with comparison
findings, includes the raw `benchstat` output for each benchmark suite, and ends with a table of all captured benchmark
numbers.

The integration benchmark runner includes committed `base`,
`fixed_suffix_expansion_fanout`, and `traversal_shapes` datasets by default.
The traversal shape suite checks expected result counts for chain, fanout, bounded cycle, disconnected,
edge-kind-selective, and multi-path shortest-path scenarios before recording timings.

`make plan_corpus` captures plan diagnostics for the shared Cypher integration corpus. It accepts either
`CONNECTION_STRING` for one backend or `PG_CONNECTION_STRING` and `NEO4J_CONNECTION_STRING` for both backends, then
writes JSONL captures and markdown/JSON summaries under `.coverage/`. Captures record the DAWGS source version, which
can be overridden with a command flag when needed. Because it reloads fixtures, it also requires
`DAWGS_INTEGRATION_ALLOW_DESTRUCTIVE=1` and every selected credential-free target in
`DAWGS_INTEGRATION_DISPOSABLE_TARGETS`.

`go run ./cmd/graphbench` captures runtime diagnostics for the scale corpus under `benchmark/testdata/scale`. The
implemented execution modes are `postgres_sql` and `neo4j`; `local_traversal` is an explicit, non-gating
`not_implemented` diagnostic placeholder, and AGE is reference-design input only. The command can emit JSONL records
plus Markdown and JSON summaries, and can compare current timings against a previous JSONL baseline. Mutating scale
cases must declare a `write_scenario`; each warm-up and timed iteration runs in a rollback transaction and verifies
matched, affected, and post-state cardinality.
Read timings retain every raw warm sample and are bracketed by untimed exact-row
multiset checks. PostgreSQL datasets are vacuumed and analyzed after loading and
before measured reads. Fixture reloads truncate the active relationship and node
partitions together, and
PostgreSQL captures fail before timing unless the active partitions' physical
node and edge counts exactly match the declared fixture; active child-partition
sizes are retained with each fixture. Node-ID expectations and recorded paths use stable
fixture identities rather than backend-assigned IDs, while preserving duplicate
rows and path order.
For sanitized production-like data, `-existing-graph` uses a versioned
logical-key anchor manifest and bypasses every schema/load/clear/vacuum path.
It rejects mutation cases, verifies before/after cardinalities, redacts anchor
values, and supports atomic checkpoints, resume, progress JSONL, and explicitly
labeled adaptive discovery. See `cmd/graphbench/README.md` for the fixed
confirmation and timeout-class workflows.
The executable gate uses the complete corpus/backend declaration instead of the
intersection of successful records, treats Neo4j only as an exact-result and
informational latency oracle, and supports predeclared materiality thresholds.
`make perf_aa` derives host-fingerprinted p50/p95 measurement resolution from
order-balanced repeated A/A captures. Complete normal/envelope performance
gates require that checksummed per-case evidence and use minimum 5%/100us
floors; stress timing remains diagnostic. Exact case/dataset/category/tag selectors create diagnostic-only
artifacts that the complete gate refuses; configured warmups and matched
arm/block/run metadata support isolated confirmation. The GraphBench CLI accepts
repeated `-aa-artifact` inputs so two immutable append-series arms can be
validated without an external merge. `make perf_confirm`
reports paired absolute and relative p50/p95 changes with optional block/reload
A/A floors. Capture bundles can retain the source patch, untracked sources,
module state, binary, manifest, raw records, and checksums. Opt-in pool
concurrency blocks and PostgreSQL component/full-query
references are documented in `cmd/graphbench/README.md`. Path-observed
singleton captures include exact benchmark-only M0/M1 materializer arms with a
shared search boundary; they do not enable an experimental production executor.
Generated fixed-suffix expansion captures provide selectable exact root-reuse,
late-hydration, factored-suffix forward, suffix-seeded reverse, and
backward-viability forward arms plus versioned fixtures with independent
suffix-density and reverse-fan-in controls. V3 fixtures additionally encode
matching-root multiplicity and independent relationship-distinct cycle and
self-loop controls at the productive boundary. The optimizer reports a typed
expansion-search decision. Repository-native
`EXPANSION-SUFFIX-SEEDED-REVERSE` is an exact qualification-only implementation.
When a complete path is observed, that arm carries ordered node IDs alongside
ordered edge IDs and hydrates both arrays directly in the translated statement;
endpoint-only forms keep the smaller edge-only reverse state. The guarded
candidate uses the same materializer while its exact forward fallback retains
the generic path helper.
Production selection remains on the stepwise incumbent because query shape and
available metadata do not provide hard suffix-density or reverse-state bounds.
The now-terminal, tool-only `orientation-probe-v2` experiment uses
`F2 = root_rows + maximum_depth * forward_degree_rows` and
`R2 = suffix_rows + boundary_rows + reverse_degree_rows`, selecting reverse
only when every bounded probe is complete and `4 * R2 < 3 * F2`. Its frozen v3
corpus contains eight selector-training cases and four evaluation holdouts whose
timings remain unopened. Its frozen historical protocol required matched
`shadow`, `incumbent`, `reverse`, and `guarded` artifacts captured under
Repeatable Read with traversal telemetry. Degree evidence is represented as a
scalar count over the same cap+1-limited adjacency stream, avoiding tuple
materialization without changing the immutable score or fail-closed overflow
rule. A promotable discovery would have needed a clean-tree report and freeze
from the exact eight training cases before confirmation could open the complete
eight-training/four-holdout cohort. That path is now archival only: v2 must not
be recaptured or advanced, and its holdout timings remain unopened. Per-case A/A
evidence also binds the PostgreSQL timing environment, including transaction
isolation, and the exact validated fixture. See
[GraphBench](cmd/graphbench/README.md) for the frozen protocol.
No v2 qualification benchmark has passed. The driver/runtime seam recognizes
exact-query manifests that name either the v1 or v2 selector identity so their
diagnostic and guarded statements remain reproducible. The schema-v2 final
verifier rejects legacy v1 evidence because it cannot bind the required source,
corpus, and frozen cohort; it also terminally rejects v2 because that immutable
policy generation failed its training overhead gate. A future attempt requires
a new policy generation. Neither selector is enabled, and default production
behavior remains unchanged.
The latest five-round, eight-case v2 training prequalification produced exact,
receipt-complete evidence but failed selected-arm overhead on every case: the
guarded statement remained roughly 156-396 microseconds above its selected
exact arm against the immutable 100-microsecond gate. A reduced-gating
prototype also failed a fresh matched capture and was reverted. V2 is therefore
a rejected selector generation rather than a pending clean-tree promotion; any
next attempt must use a new policy identity and qualification freeze.
The subsequent bounded experiment was the independent, tool-only
`suffix-reverse-guard-v1` policy. It enrolls only complete-path fixed-suffix
queries, performs no topology or degree probes, caps suffix payload and reverse
state at 512 rows each, and dispatches either exact suffix-seeded reverse or the
unchanged exact forward traversal in one Repeatable Read statement. Its
six-round training feasibility gate is deliberately not a qualification or
production seam; endpoint-only queries, mutations, protected holdouts, and the
zero-value production translator remain outside the experiment. The gate also
requires process timestamps to prove the declared physical doubled-Williams
order and an artifact-bound schema-v4 A/A chronology proof. The first capture
predates that enforcement and is invalid. A chronology-compliant recapture then
failed the immutable guard-overhead bound on both training cases, so this
generation is terminally stopped before holdout, manifest, driver-policy, or
automatic-selector work.
For the distinct one-fixed-prefix plus selective-terminal-expansion shape,
production uses guarded `EXPANSION-ENDPOINT-SEEDED-REVERSE`: 32 endpoint and
4096 reverse-state caps select either the reverse candidate or an exact
same-statement forward fallback without exposing partial candidate rows.

The next fixed-suffix generation is available only to GraphBench through
`-postgres-expansion-suffix-reverse-retry`. It executes a reverse-only bounded
statement, buffers all candidate rows, and retries the exact forward incumbent
after a savepoint rollback in the same Repeatable Read transaction. Its
statement contains no topology probes or inactive forward body. The frozen
development contract and stop gate are documented in
[Suffix reverse transaction retry v1](docs/experiments/suffix_reverse_retry_v1.md).
It is not a production selector.

PostgreSQL recursive shortest-path execution includes contained S3/S4
singleton selection, a guarded canonical inline witness canary, and an
all-shortest predecessor-DAG executor, with exact same-statement fallback,
reusable session-local workspace-v2 state, late hydration, event-chain runtime
receipts, and
a parameter-shape-aware translation cache. The implementation and its
qualification boundaries are documented in
[Recursive-descent cost controls](docs/recursive_descent_cost_controls.md).

New inline SP and ordinary-orientation lowerings remain default-off. Canonical
SP-I1 authorization now uses selector `sp-static-v6` and accepts only the
qualified inbound, typed, single-kind, one-path `min=1`/`max=64` bucket; the
automatic `sp-static-v5-contained` S3/S4 choices are unchanged. The
guarded distance identity `SP-I2-C-D` uses selector `sp-static-v8-hidden-fanin`
for exact inbound typed single-kind distance buckets, reverse-physical ID-only
search, the preregistered production-form `state_limit=100000` and
`frontier_limit=100000` guards, and exact S4 fallback. Those caps are immutable
protocol inputs, not qualified values: the dirty-tree rehearsal stopped before
creating a discovery report or freeze, its cycle-control point estimates missed
the frozen bounds, and no protected holdout was opened. Syntax-open
singleton shortest paths now use the documented effective maximum depth 15 and
report `policy_default` depth provenance under `sp-static-v7-contained`.
The PostgreSQL driver's `SetTraversalPolicy` API can expose one eligible
candidate to an explicit normalized-query SHA-256 allowlist under a nonzero
generation.
Activation requires the exact promotion manifest, including its measured
execution boundary, independently frozen operational candidate SQL SHA-256,
and evidence digests. The SQL anchor is derived in a non-promotional preflight,
then added to the provisional manifest before formal evidence is recaptured.
Manifest schema v2 also requires every evidence report to repeat the exact
candidate, selector, source, binary,
corpus, cap, bucket, and query-cohort identity; a digest-shaped string alone is
not authorization. The verifier strictly decodes candidate-specific evidence,
recomputes the bound native A/A digest and performance decisions, requires the
reference workload digest to match the exact PostgreSQL A/A workload per cohort
case, and closes every performance receipt against the complete set of resource
case-round receipts. Confirmation and performance expose typed evidence rather
than raw benchmark samples, so final verification can recompute their declared
decisions but cannot independently replay every bootstrap draw; closing that
reproducibility gap requires a producer-schema revision. The operational gate
likewise validates an assembled 32-record native input but does not yet provide
a standalone capture producer. Evidence roles,
bucket names, query identities, and the canonical training/holdout split are
closed sets; duplicate JSON keys, duplicate allowlist entries, extra roles, and
filesystem-symlink escapes fail closed. The legacy
`orientation-probe-v1` report lacks the source/corpus/cohort identity needed by
this closure and therefore cannot authorize promotion. The structurally richer
v2 report remains readable, but final authorization rejects that terminal
generation because its immutable training overhead gate failed. B1/B2 and legacy
unguarded `SP-I1-C-D` remain tooling-only. Endpoint-seeded reverse,
inline ASP, inline canonical witness, and guarded inline distance each have an
evidence-free emergency disable switch. If a manifest-backed candidate carries
a rollback switch, it may carry exactly one and it must match: orientation with
`DisableExpansionOrientation`, ASP-I1 with `DisableInlineASPDAG`, SP-I1 witness
with `DisableInlineSPWitness`, or SP-I2 with `DisableInlineSPDistance`. An
unrelated or second switch is rejected. `DisableEndpointSeededReverse` is
standalone-only; every standalone rollback must omit a candidate and leave
`promotion_manifest_sha256`, `promotion_manifest_json`, and
`query_sha256_allowlist` empty. A matching rollback preserves the installed
manifest and candidate anchor but derives an incumbent-only effective policy
under a new cache generation. Resetting the policy to its zero value immediately
returns all queries to incumbent cache identities. This is a reversible canary
seam, not evidence that a candidate is qualified for broad production use.

The PostgreSQL scale-plan gate runs as part of `make test_all` when
`CONNECTION_STRING` selects PostgreSQL. It executes every required Cypher scale
representative with `EXPLAIN ANALYZE`, enforces declared result or mutation
cardinality, and checks stable mutation-target and anchored edge-index
invariants. Run it directly with:

```bash
DAWGS_INTEGRATION_ALLOW_DESTRUCTIVE=1 \
DAWGS_INTEGRATION_DISPOSABLE_TARGETS="postgresql://localhost:65432/dawgs" \
  CONNECTION_STRING="postgresql://dawgs:weneedbetterpasswords@localhost:65432/dawgs" \
  go test -tags manual_integration ./cmd/graphbench \
  -run 'Test(PostgreSQLScalePlanInvariants|ScaleCorpusRequiredRepresentativesDeclareCardinality)' \
  -count=1
```

Runtime and plan captures are intentionally generated under the ignored
`.coverage/` directory. Keep them as reviewed environment-specific artifacts;
use the stable `GFSE-*`, `REC-*`, `TRUST-*`, `PRUNE-*`, `HOP-*`, `SCAN-*`, and
`LOOKUP-*` IDs to compare captures with their semantic fixtures and manifest
entries.

The SP-I2 distance V2 study is terminally stopped before formal timing. Its
frozen 20,000-run prospective calibration, reconstructed from the clean V1
trace, could not support the fixed A/A and qualification design at the required
power. V2 therefore remains production-off and must not proceed to A/A,
holdout, confirmation, or promotion; see
[`cmd/graphbench/README.md`](cmd/graphbench/README.md) for the reproducible
verification command. In plain terms, comparing the same implementation with
itself still produced uncertainty of roughly `-5.4%` to `+6.3%` and
`-116us` to `+133us`, wider than the allowed plus-or-minus 5% and 100us. The
fixed study recognized the target and control outcomes only about 48% and 51%
of the time, respectively, instead of the required 90% reliability.

`go run ./cmd/retriever` dumps and loads live Dawgs graph databases as
manifest-based collections of compressed JSONL fragments. It supports
PostgreSQL and Neo4j, uncompressed, gzip, and zstd fragments, bounded keyset
scans, resumable dump checkpoints, checksum validation before load, optional
deterministic property scrubbing, and a read-throughput benchmark mode. It can
also package dumps as single HPKE/ML-KEM encrypted TAR archives.
See [cmd/retriever/README.md](cmd/retriever/README.md) for dump, encrypted
archive, load, scrubbed dump, metrics verification, and benchmark examples.
The same import/export functionality is available to library consumers from
`github.com/specterops/dawgs/retriever`; callers provide an already-open
`graph.Database`, and archive helpers support both path-based and stream-based
APIs. The package exposes CLI-matching default option constructors, structured
progress callbacks, manifest/metrics helpers, HPKE key envelope reader/writer
helpers, and typed errors for validation, compatibility, checksum, metrics, and
count mismatches.

PostgreSQL translates exact string property equality with a JSON string type guard and `properties ->>` extraction, so
indexes created on expressions such as `properties ->> 'objectid'` and `properties ->> 'name'` can be used for selective
anchors without matching JSON booleans or numbers. Simple relationship count fast paths depend on the schema's
`kind_id`-first edge index for efficient typed counts.

PostgreSQL property index regression coverage is hard-failing under the `manual_integration` tag. The synthetic plan
test translates Cypher to PgSQL, disables sequential scans for the `EXPLAIN`, and requires explicit node property
indexes to appear in the JSON plan:

For local development against a checkout, use a Go module replacement in the consuming project:

```go
replace github.com/specterops/dawgs => /path/to/dawgs
```

## Documentation

- [Development workflow](docs/development.md): build, test, integration, metrics, quality, and corpus-capture commands.
- [Cypher library](cypher/README.md): parser generation and Cypher package overview.
- [PostgreSQL translation](docs/postgresql_translation.md): PostgreSQL translator behavior, optimizer lowerings, indexing notes, and validation expectations.
- [CySQL traversal performance priorities](docs/cysql_traversal_priorities.md): source-grounded roadmap for orientation, SP/ASP, probes, statistics, telemetry, and qualification.
- [Traversal priority implementation status](docs/experiments/traversal_priority_implementation_status_v1.md): implemented candidate identities, fail-closed gates, and current no-promotion disposition.
- [Remaining traversal outlier delivery](docs/experiments/remaining_outlier_delivery_v1.md): SP-I2, fixed-suffix, qualification, promotion-closure, and rollback handoff.
- [PostgreSQL versus Neo4j performance summary](report_summary.md): current outlier measurements and evidence boundaries.
- [Plan corpus capture](cmd/plancorpus/README.md): shared integration corpus plan diagnostics.
- [Graph benchmark capture](cmd/graphbench/README.md): runtime diagnostics for scale scenarios.
- [Integration corpus](integration/testdata/README.md): fixture, mutation post-state, and typed-parameter schema.
- [BloodHound regression coverage manifest](regression_coverage_manifest.md): per-query-form layer status and existing primitive links.
- [BloodHound source-parity workflow](docs/regression_source_parity.md): dormant-form activation rules and repeatable BHE/BHCE source audits.
- [Cypher syntax support](cypher/Cypher%20Syntax%20Support.md): supported Cypher behavior and semantic notes.

## Repository Map

- `cypher/`: parser, Cypher AST, walkers, and backend translation models.
- `drivers/`: database driver implementations.
- `integration/`: backend-equivalent integration suites and fixtures.
- `cmd/`: command-line tools for capture, export, and diagnostics.
- `tools/`: developer tools such as `dawgrun` and metrics reporting.
