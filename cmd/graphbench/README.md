# GraphBench

`graphbench` runs the scale benchmark corpus under `benchmark/testdata/scale`.
It is meant for runtime gap accounting: query duration, returned row counts,
PostgreSQL plan details, Neo4j plan operators, fallback reasons, and comparison
summaries.

The current execution modes are:

- `postgres_sql`: runs DAWGS' PostgreSQL SQL translation against a PostgreSQL database.
- `local_traversal`: records explicit `not_implemented` placeholders until the local traversal executor lands.
- `neo4j`: runs the same corpus against Neo4j through the DAWGS Neo4j backend.

Apache AGE is not an execution mode in this harness yet. AGE behavior can be
captured in corpus `reference_design` notes so DAWGS can use it as design input
without treating it as a direct benchmark comparison.

## Inputs

The command loads cases from `benchmark/testdata/scale` by default and imports
the fixture datasets from `integration/testdata`.

Corpus parameters support fixture IDs through `node_params` and
`node_list_params`. Tagged datetime values are decoded to `time.Time`, avoiding
lexical string comparisons in temporal cases. Mutating cases require an
explicit `write_scenario`; the runner checks matched and affected counts plus
post-state queries and rolls back warm-up, timed iterations, and PostgreSQL
plan capture.

Read cases that return node IDs can declare `expected.id_rows` using fixture
node names. GraphBench reverse-maps backend-assigned IDs through the complete
dataset ID map and compares the rows as a multiset, preserving duplicates.

Connection strings can be supplied as flags or environment variables:

- PostgreSQL: `-pg-connection`, `PG_CONNECTION_STRING`, `-connection`, or `CONNECTION_STRING`.
- Neo4j: `-neo4j-connection`, `NEO4J_CONNECTION_STRING`, `-connection`, or `CONNECTION_STRING`.

Every output record includes the DAWGS source version plus source commit,
dirty-worktree hash (including untracked files), binary hash, sanitized invocation,
Go/OS/CPU/kernel/cgroup data, run UUID, arm/block/order timestamps, pool settings,
and declared memory ceilings. Use
`-dawgs-version` to override the auto-detected DAWGS version.

GraphBench clears and reloads fixtures. A non-blocking local lock at
`.coverage/graphbench.lock` prevents overlapping processes; override it with
`-destructive-lock`. Runners on different hosts must use distinct disposable
databases because a filesystem lock cannot coordinate across machines.

## Examples

Run only PostgreSQL SQL translation:

```bash
go run ./cmd/graphbench \
  -modes postgres_sql \
  -pg-connection "$PG_CONNECTION_STRING" \
  -jsonl-output .coverage/graphbench-postgres.jsonl \
  -summary .coverage/graphbench-postgres.md \
  -summary-json .coverage/graphbench-postgres.json
```

Capture PostgreSQL, local traversal placeholders, and Neo4j in one report:

```bash
go run ./cmd/graphbench \
  -modes postgres_sql,local_traversal,neo4j \
  -pg-connection "$PG_CONNECTION_STRING" \
  -neo4j-connection "$NEO4J_CONNECTION_STRING" \
  -jsonl-output .coverage/graphbench.jsonl \
  -summary .coverage/graphbench.md \
  -summary-json .coverage/graphbench.json
```

Compare a run against a previous JSONL capture:

```bash
go run ./cmd/graphbench \
  -modes postgres_sql,neo4j \
  -pg-connection "$PG_CONNECTION_STRING" \
  -neo4j-connection "$NEO4J_CONNECTION_STRING" \
  -baseline .coverage/graphbench-baseline.jsonl \
  -jsonl-output .coverage/graphbench.jsonl \
  -summary .coverage/graphbench.md
```

Capture independent rounds with 30-50 warm observations each. The PostgreSQL
runner resets its one-connection pool before every case, records the first
query execution as `cold`, and keeps connection establishment outside that
sample. Use a distinct `-round` value for every independently reloaded run:
Even-numbered rounds reverse the requested backend order to alternate which
backend runs first.

```bash
go run ./cmd/graphbench \
  -round 1 \
  -iterations 30 \
  -modes postgres_sql,neo4j \
  -pg-connection "$PG_CONNECTION_STRING" \
  -neo4j-connection "$NEO4J_CONNECTION_STRING" \
  -jsonl-output .coverage/graphbench-round-1.jsonl
```

Concatenate the JSONL rounds for each version, then run the executable
confidence gate:

```bash
make perf_gate \
  PERF_BASELINE=.coverage/graphbench-baseline.jsonl \
  PERF_CANDIDATE=.coverage/graphbench-candidate.jsonl
```

The versioned gate report includes artifact SHA-256 checksums, seeded 95%
bootstrap intervals over matched round medians, stratified p95 intervals once
each side has at least 150 samples, and the 20% comparable-corpus regression
gate. The version-controlled corpus `candidate_modes` declarations are the
required-key/status manifest: a missing or non-`ok` PostgreSQL record fails
instead of disappearing through intersection-only comparison. Neo4j records
must be present and `ok`, but Neo4j latency is informational and never fails a
CySQL performance gate. Fewer than five matched PostgreSQL rounds or
insufficient p95 samples is incomplete and fails.

Predeclare cases expected to improve with `PERF_TARGETS` (or
`-gate-targets`). A target passes materiality when its median-ratio upper bound
is at most `0.95` or its median-saving lower bound is at least `100us`; both
defaults are configurable. Calculate host-specific A/A resolution from a
baseline artifact with:

```bash
make perf_aa PERF_AA_ARTIFACT=.coverage/graphbench-aa.jsonl
```

The report splits alternating samples within every independent round, reports
p50/p95 ratio and absolute resolution, and keeps p99 diagnostic until each arm
has at least 10,000 samples.

### Targeted matched diagnostics

`-cases` accepts exact, unambiguous case names. `-datasets`, `-categories`, and
`-tags` add exact selectors; values within one selector are alternatives and
different selector dimensions are intersected. Unknown, duplicate, ambiguous,
or empty selections fail before a fixture is changed. Filtered captures are
marked `diagnostic_only`, record both the requested and resolved selection and
the omitted declaration count, and are refused by the ordinary complete gate.
Use `-diagnostic-gate` only to compare two artifacts with the same resolved
subset checksum.

Configured `-warmup-iterations` run outside the recorded samples. The cold
diagnostic, exact preflight/postflight observations, and fixture reload/analyze
contract remain separate. A matched arm records `-arm`, `-arm-order`, `-block`,
`-round`, and a shared `-run-uuid`:

```bash
go build -trimpath -o .coverage/confirm/bin/graphbench ./cmd/graphbench
.coverage/confirm/bin/graphbench \
  -modes postgres_sql \
  -cases 'LOOKUP-05_repeated_case_insensitive_prefix,GSP-D02-F016_distance' \
  -warmup-iterations 20 -iterations 50 -pool-size 1 \
  -arm candidate -arm-order 1 -block 1 -round 1 -run-uuid "$RUN_UUID" \
  -pg-connection "$PG_CONNECTION_STRING" \
  -bundle-dir .coverage/confirm/candidate-round-1 \
  -jsonl-output .coverage/confirm/candidate-round-1.jsonl
```

`-bundle-dir` retains the tracked patch, checksummed copies of untracked files,
`go.mod`/`go.sum`, the running executable, the selected corpus declaration, raw
JSONL, a sanitized manifest, and bundle checksums. It never records connection
strings or arbitrary environment variables.

Compare matched arms, optionally applying the worse block/reload A/A report:

```bash
make perf_confirm \
  PERF_LEFT=.coverage/confirm/predecessor.jsonl \
  PERF_RIGHT=.coverage/confirm/candidate.jsonl \
  PERF_CONFIRM_AA=.coverage/confirm/block-aa.json \
  PERF_CASES='LOOKUP-05_repeated_case_insensitive_prefix,GSP-D02-F016_distance'
```

The report emits paired relative and absolute p50/p95 intervals. It classifies
fresh p95 evidence as confirmed, cleared/non-inferior, inconclusive, or a
fingerprint mismatch using a minimum 5%/0.10 ms noise floor. Comparing two
captures of the same executable produces a `block_reload_aa` report; alternate
arm order across independently reloaded rounds.

## Concurrency and PostgreSQL references

Serial behavior remains the default (`-pool-size 1`). An opt-in concurrency
smoke retains a physical pool and records pool wait, transaction setup,
execute/decode/drain time, backend PID, cold/warm session classification, wall
time, and QPS:

```bash
go run ./cmd/graphbench \
  -modes postgres_sql \
  -pg-connection "$PG_CONNECTION_STRING" \
  -pool-size 8 \
  -concurrency 1,8,16 \
  -iterations 30 \
  -session-memory-ceiling-bytes 67108864 \
  -pool-memory-ceiling-bytes 536870912 \
  -jsonl-output .coverage/graphbench-concurrency.jsonl
```

`-postgres-references` additionally captures an identical-SQL raw-pgx boundary
(pool wait, transaction, bind/prepare, first row, remaining decode, drain, and
allocations), a raw prepared round-trip, the C1 prepared round-trip,
endpoint validation, minimum graph-access ID floor, raw ordered-ID search,
path hydration from precomputed ordered edge IDs, and complete hand-written
PostgreSQL references for the active shortest-path and ADCS targets. The main
case record is the seventh, translated-CySQL rung. Component floors need not
match the full query's row count; complete references do. It also records
compile-stage timings and allocations. JSON/Markdown summaries include a
versioned exclusive-boundary cost table and its unexplained residual. The waterfall marks its translation
interval as overlapping optimization, so those fields must not be summed as an
additive attribution.

Supported generated singleton-shortest cases also run two additive comparators:
`s3_unidirectional_trail_cte` (legacy name
`complete_reference_s1_array_cte`) and `s3_bidirectional_trail_cte` (legacy name
`candidate_s2_bidirectional_cte`). New reference records declare a schema
version, architecture, implementation/state/observation shape, and semantic
validation level. Full comparators are checked against untimed exact public
observations rather than row count alone. Distance S3-U uses node/depth frontier
state with no path or predecessor arrays. Historical readers preserve the old
labels in `legacy_name` while mapping them to S3-U/S3-B. These remain
benchmark-only; S3-B is not evidence for the compact S2 architecture.

The optimizer also emits a typed `ShortestPathExecutorDecision` for every
shortest traversal. It records structural eligibility facts, observation mode,
depth bound, selected/fallback executor, and a stable fallback code. Until a
reconstructible live S0-S3 tournament satisfies the C2Q resource and semantic
gates, the selected executor remains `incumbent_workspace` and otherwise
eligible singleton forms report `tournament_unqualified`; this diagnostic does
not silently activate benchmark SQL in production.

## Outputs

JSONL output contains one `CaseResult` record per case and execution mode.
Markdown and JSON summaries aggregate mode status counts, per-case timings, row
counts, fallback reasons, and baseline regressions or improvements when a
baseline capture is supplied.

Each timing record retains the unsorted cold and warm latency samples with round,
iteration, case, dataset, backend, and connection/session fields so confidence
interval and regression tooling does not have to reconstruct observations from
summary percentiles. Read cases run untimed preflight and postflight queries
and compare their complete row multisets, including duplicate rows, around the
timed block. For declared `id_rows`, `path_set`, and scalar results, recorded
`observed_rows` use stable fixture identities, retain relationship order,
kinds, and properties, and reject relationship reuse within a path. GraphBench
compares those stable result kinds across backends. Other result kinds still
receive per-backend preflight/postflight checks, but are not compared across
backends because they may contain backend-generated relationship IDs.
PostgreSQL fixture loads are followed by `VACUUM (ANALYZE)` through
the pool; a maintenance failure aborts the benchmark.
The PostgreSQL runner defaults to a one-connection pool and records
`pg_backend_pid()` as the serial sample connection identifier. Concurrency
blocks record the physical PID of every direct pool acquisition so per-session
cold state and pool queuing remain visible.

Write records additionally report matched and affected counts and each
post-state observation. The recorded duration covers the mutation query; setup,
verification, and rollback are outside that duration.

PostgreSQL records include translated SQL and its fingerprint, server settings,
fixture checksum/cardinalities, and `EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)`
shared/local/temp metrics plus `EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS,
FORMAT JSON)` for reads. Neo4j records include plan operator names
when an `EXPLAIN` plan can be captured.

## PostgreSQL scale-plan correctness gate

The PostgreSQL-only `TestPostgreSQLScalePlanInvariants` test loads the same
scale corpus and fixture as the command. It executes all required Cypher scale
representatives, requires their declared cardinalities and mutation post-state,
and verifies that the captured plan came from `EXPLAIN ANALYZE`. Stable
assertions cover relationship/node mutation targets, branch-local logical
structure, temporal filtering, and anchored edge-index orientation. The test
uses rollback isolation for writes and runs automatically under
`make test_all` when `CONNECTION_STRING` selects PostgreSQL.

Run only the scale-plan gate with:

```bash
CONNECTION_STRING="$PG_CONNECTION_STRING" \
  go test -tags manual_integration ./cmd/graphbench \
  -run 'Test(PostgreSQLScalePlanInvariants|ScaleCorpusRequiredRepresentativesDeclareCardinality)' \
  -count=1
```

The non-integration cardinality test also guarantees that every required stable
query-form ID remains represented in the scale corpus and declares an expected
read or write cardinality.
