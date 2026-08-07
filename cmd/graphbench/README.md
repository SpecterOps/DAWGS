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
case record remains the translated-CySQL boundary rather than a fixed ordinal
among the additive references. Component floors need not match the full query's
row count; complete references do. It also records
compile-stage timings and allocations. JSON/Markdown summaries include a
versioned exclusive-boundary cost table and its unexplained residual. The waterfall marks its translation
interval as overlapping optimization, so those fields must not be summed as an
additive attribution.

Use `-postgres-reference-arms` to run only named tournament arms; it implies
`-postgres-references` and rejects unknown or duplicate names. Generated ADCS
cases expose `current_forward_ordered_ids`, `a1a_root_reuse_*`,
`a1b_late_hydration_*`, `a2_factored_suffix_forward_*`,
`a3_suffix_seeded_reverse_*`, and `a4_viability_forward_*` boundaries. Complete
arms are exact-multiset checked against the public CySQL observation. Ordered-ID
arms retain relationship IDs for trail uniqueness. When exactly five arms are
selected, rounds follow the fixed ten-sequence carryover-balanced schedule from
`perf_cont_3.md`; other arm counts retain the historical alternating order.

`-postgres-force-shortest-executor SP-S3-U-D` is a qualification-only seam for
eligible bounded singleton distance cases. It executes the repository-native
recursive AST directly, using compact `(next_id, depth)` state when both
endpoints are ID-only and retaining `(root_id, next_id, depth)` otherwise. It
reports the exact forced/applied target and rejects path-observed or otherwise
ineligible cases. It does not enable the executor in the public query API.

`-postgres-force-shortest-executor SP-S3-U-E+MAT-M0` is the corresponding
qualification-only seam for eligible one-path observations. It emits
repository-native `(next_id, depth, edge_ids)` recursive state and hydrates the
ordered path directly from direction-specific edge endpoints. Distance-only,
directionless, correlated, optional, mutation, and other ineligible forms keep
the incumbent unless explicitly rejected by the tool request. Automatic path
dispatch remains disabled.

`-postgres-force-expansion-search ADCS-A3` is the qualification-only seam for
eligible directed, bounded variable expansions followed by the exact
three-relationship ADCS suffix. It emits the repository-native suffix-seeded
reverse recursive AST, preserves relationship-trail uniqueness and exact suffix
multiplicity, and supports endpoint-ID and complete-path observations. The
request fails closed when the target is structurally ineligible or translation
does not record A3 as applied. It is mutually exclusive with forced shortest
execution. Automatic A3 dispatch remains disabled because query shape does not
bound suffix density or reverse fan-in.

Independent benchmark rounds can be accumulated with `-append-jsonl`. The
append path must be supplied with `-jsonl-output`; GraphBench rejects mismatched
run UUIDs, arms, binary/diff identities, and duplicate case rounds before
writing. This is the intended input shape for paired confirmation and the
round-stratified performance gate.

Use `-reference-closure-artifact` with a capture containing the translated
raw-pgx boundary and one exact PostgreSQL full-comparator arm to generate a
seeded production/reference closure report. The report requires 10-20 matched
rounds, at least 20 untimed warmups and 50 measured samples per side in every
round, and exact public observations. It passes when the production/reference
median-ratio upper bound is at most 1.10 or the absolute median-gap interval is
within the greater of the case's within-session A/A resolution and
`-materiality-absolute` (100 microseconds by default). The report derives and
records A/A resolution independently for the production and reference raw
boundaries by splitting alternating samples within each round. Single selected
reference captures run production first in odd rounds and the reference first
in even rounds; the order is recorded on both boundaries and enforced by the
reporter:

```bash
go run ./cmd/graphbench \
  -reference-closure-artifact .coverage/shortest-reference.jsonl \
  -reference-closure-arm s3_unidirectional_trail_cte \
  -reference-closure-output .coverage/shortest-reference-gate.json \
  -confidence-level 0.975 \
  -seed 1
```

ADCS JSON plans are retained in both text and structured forms. Structured
metrics include per-node planned/actual rows, loops, width, timing, buffers,
relation/index identity, recursive rows, access-direction probe counts, and
hydration lookup loops. Derived fields state their provenance and do not present
fixture-derived per-depth counts as PostgreSQL measurements.

Supported generated singleton-shortest cases also run two additive comparators:
`s3_unidirectional_trail_cte` (legacy name
`complete_reference_s1_array_cte`) and `s3_bidirectional_trail_cte` (legacy name
`candidate_s2_bidirectional_cte`). New reference records declare a schema
version, architecture, implementation/state/observation shape, and semantic
validation level, raw-pgx timing boundary, normalized SQL fingerprint, and any
explicit A/A alias. A requested arm that is unavailable for a case fails the
run, and distinct architecture IDs with identical normalized SQL fail unless
the alias is declared. Full comparators are checked against untimed exact public
observations rather than row count alone. Distance S3-U uses node/depth frontier
state with no path or predecessor arrays. Historical readers preserve the old
labels in `legacy_name` while mapping them to `SP-S3-U-NE`/`SP-S3-B`. These remain
benchmark-only; S3-B is not evidence for the compact S2 architecture.

Distance-only generated cases also expose `s1_array_bfs_distance`, a genuine
typed PL/pgSQL SP-S1 prototype. It keeps frontier and visited node IDs in
bounded arrays, records a fixed 100,000-node state ceiling, and restarts the
exact S3-U distance reference in the same statement on overflow. It is a
benchmark arm only and is never selected by production translation.

Capture S3-U-D and SP-S1 together with 20 warmups and 50 observations, then
produce their seeded, order-balanced matched comparison with:

```bash
go run ./cmd/graphbench \
  -reference-pair-artifact .coverage/shortest-alternatives.jsonl \
  -reference-pair-baseline s3_unidirectional_trail_cte \
  -reference-pair-candidate s1_array_bfs_distance \
  -reference-pair-output .coverage/shortest-alternatives.json \
  -confidence-level 0.975 \
  -seed 1
```

The default confirmation pair reporter requires 10-20 independent rounds, 20
warmups, 50 samples per arm per round, and distinct recorded measurement order.
`-reference-pair-protocol discovery` produces an explicitly labeled exploratory
report from 5-20 rounds, five warmups, and ten samples per arm; it cannot be
mistaken for confirmation evidence because the protocol and requirements are
written into the report. The reporter accepts two exact public-observation
comparators, two exact ordered-ID comparators, or two hydration-only arms
independently validated from the same precomputed exact path inputs; mixed
boundaries are rejected. ADCS ordered-ID candidates are checked against the
canonical A0 node/edge-ID arrays before their timing is retained. Reports show
candidate/baseline median and p95 ratios, absolute median change, and
within-session A/A resolution without turning architecture selection into a
post-hoc pass threshold.

Path-observed singleton cases additionally capture benchmark-only M0 and M1
materializer arms. Whole-query comparison uses each architecture's minimal
state: `SP-S3-U-E+MAT-M0` carries edge IDs only and derives node order from the
directed edge endpoints, while `SP-S3-U-NE+MAT-M1` carries node and edge IDs and
hydrates both streams independently by ordinality. Outbound and inbound M0 use
distinct implementation identities. Separate
hydration-only arms use precomputed IDs so search cost stays outside the timed
materializer boundary. These arms are exact-result checked but do not change
production path rendering. Odd benchmark rounds execute references in declared
order and even rounds reverse that order, balancing which M0/M1 arm runs first
across the required independently reloaded rounds.

Every PostgreSQL dataset reload truncates the active relationship and node
partitions together. Other backends delete relationships before nodes. PostgreSQL then checks
the physical row counts in the active `node_<graph>` and `edge_<graph>`
partitions against the fixture declaration before vacuuming or measuring. A
stale/orphan row therefore fails the run instead of silently contaminating scan
and count cases. Fixture records also retain active child-partition sizes rather
than the zero-sized partitioned-parent relations.

```bash
go run ./cmd/graphbench \
  -modes postgres_sql -postgres-references \
  -cases 'GSP-D01-F001_path,GSP-D02-F016_path,GSP-D04-F128_path,GSP-D08-F001_path_inbound,GSP-D16-F016_path,GSP-D32-F512_path,GSP-D64-F1000_path' \
  -warmup-iterations 20 -iterations 50 -pool-size 1 \
  -pg-connection "$PG_CONNECTION_STRING" \
  -jsonl-output .coverage/materializer-round-1.jsonl
```

The optimizer also emits a typed `ShortestPathExecutorDecision` for every
shortest traversal. It records a machine-readable structural-eligibility result,
SP family and planned candidate identities, observation mode, minimum/maximum
depth, selected/fallback executor, selector version/mode, limits, and stable
fallback code. These fields are also copied into each exact target outcome.
Call count and read-only status are statement-wide, including shortest calls or
mutations separated by `WITH`. Selector `sp-static-v2` chooses `SP-S3-U-D` for
qualified distance observations and `SP-S3-U-E+MAT-M0` for qualified one-path
observations. Qualification requires one directed three-element shortest-path
traversal, a supported bounded depth, one static ID equality per endpoint, no
relationship variable or predicate, no path predicate, one uncorrelated
endpoint pair, one statement-wide shortest call, and a read-only statement.
Every other shape retains `SP-S0` and its specific fallback code.

Ordinary variable expansions with fixed continuations similarly emit a typed
`ExpansionSearchStrategyDecision`. It records suffix bounds, logical direction,
observation mode, depth bounds, structural facts, selection mode, and stable
fallback codes. It also reports the ADCS family, planned candidate set, selector
version, limits, and distinct correlated-suffix/cross-region fallback reasons.
A2/A4 SQL remains reference-only. A3 additionally has a repository-native
forced emitter for qualification, but it is not selected by the public query
API. Until a bounded selector and exact same-snapshot overflow fallback pass
the required tournament, structurally eligible forms select
`ADCS-INCUMBENT-STEPWISE` with `tournament_unqualified`.

## Outputs

JSONL output contains one `CaseResult` record per case and execution mode.
Markdown and JSON summaries aggregate mode status counts, per-case timings, row
counts, fallback reasons, and baseline regressions or improvements when a
baseline capture is supplied.

PostgreSQL case records also include aggregate query-text-free parse-cache
counters. Optimization diagnostics retain target-specific selected, applied,
and skipped identities; compile-time records do not claim a runtime branch.

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
