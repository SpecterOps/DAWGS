# Ordinary path materialization batching v1

Date: 2026-11-05

Status: diagnostic feasibility study; test-only candidate; production unchanged.

[Experiment index](README.md) · [Reproduction commands](../testing.md#path-materialization-experiment) · [Traversal follow-up queue](../cysql_traversal_priorities.md#2026-11-05-follow-up-ordinary-path-materialization) · [Recorded measurements](path_batch_materialization_v1_results.json)

## Finding and disposition

Batching entity retrieval and assembling paths in the outer SQL statement can
substantially reduce ordinary full-path query time. The benefit varies with
topology and output width. Whole-result batching delays the first row and can
spill even when the caller requests `LIMIT 1`. The next implementation experiment
should use bounded hydration batches and apply proven selection limits before
hydration.

This study does not authorize a production switch. It establishes a follow-up
candidate and correctness tests. The existing
[`ordered_edge_ids_to_path`](../../drivers/pg/query/sql/schema_up.sql) materializer
remains the ordinary-expansion control. The earlier
[ASP A1 inline hydration result](asp_a1_inline_hydration_v1.md) concerns a
different lowering and remains a separate disposition.

The original report concerned `[:A|B*1..2]` versus component queries. That pattern
permits `A`, `B`, `AA`, `AB`, `BA`, and `BB`; two homogeneous expansions omit the
mixed paths. Bounds through three hops permit fourteen sequences. The new
[multi-relationship regressions](../../integration/multi_relationship_expansion_test.go)
compare the exhaustive component set. PostgreSQL plan checks admit the expected
recursive rows in the tested fixtures, including disconnected and wrong-kind
decoys. These observations do not prove that physical scans inspect no rejected
edges, and do not reproduce the user's production 15-second latency or the
unexplained error. Those symptoms still require the original parameterized
queries, representative data, and server error details.

## Implementation under test

The experimental implementation is entirely in integration-test code:

- [Candidate SQL and eligibility checks](../../integration/pgsql_path_batch_candidate_test.go).
- [Complete-value corpus comparisons](../../integration/pgsql_path_batch_corpus_test.go).
- [Limited and ordered page comparisons](../../integration/pgsql_path_batch_pages_test.go).
- [Fixtures and complete-value oracle](../../integration/pgsql_path_batch_fixtures_test.go).
- [PostgreSQL measurements](../../integration/pgsql_path_batch_benchmark_test.go).
- [Neo4j reference measurements](../../integration/neo4j_path_batch_reference_test.go).

Each compact input occurrence has a root composite, ordered edge IDs,
per-occurrence known-node composites, and a null flag. A synthetic occurrence ID
preserves bag multiplicity. The batch deduplicates graph-scoped persistent lookup
keys, fetches node and relationship records, and assembles ordered arrays.
Relationship occurrence rows carry IDs and endpoints. Known-node values retain
their first-matching-entry precedence within each path. Directed paths can use
endpoint ordering directly; the generic form walks ordered adjacency for mixed
directions and repeated nodes.

The performance arms are:

| Arm | Meaning |
| --- | --- |
| `current_full` | Existing translated `RETURN p`, fully consumed. |
| `batch_full` | Batch all compact result occurrences, then return complete paths. |
| `current_limit1` | Existing translated `RETURN p LIMIT 1`. |
| `batch_outer_limit1` | Build the global batch with a final outer `LIMIT 1`. |
| `batch_page_limit1` | Select one compact path before batching, then return one complete path. |

The early limit is proven only for the supported final projection shapes.
Ordering, grouping, distinctness, filters consuming hydrated values, earlier
path consumers, and mutation visibility require separate proofs. The opt-in
ordered-page tests preserve a carried rank for a deterministic edge-ID order;
they do not establish arbitrary path sorting support.

## Capture method and limits

PostgreSQL 18.6 and Neo4j 4.4.44 Community ran on the same local test host,
sequentially. The PostgreSQL settings were `work_mem=4MB`,
`hash_mem_multiplier=2`, `plan_cache_mode=auto`, and `jit=on`. The Go runtime was
`go1.27.3-X:nodwarf5`, with `GOMAXPROCS=20`. The retained environment timestamp is
2026-11-05 19:37:12 UTC.

The synthetic fixtures use the same exact three-hop query and result size per
topology. Wide entities add distinct, hash-derived 2,048-byte payloads to avoid
misleading compression from repeated filler. Narrow entities still contain
typed properties, including integers above 2^53, booleans, arrays, and absent
keys.

| Fixture | Paths | Nodes | Relationships | Reuse |
| --- | ---: | ---: | ---: | --- |
| Shared width 4 | 512 | 13 | 72 | Shared prefixes and convergence across three layers; two relationship types per adjacent pair. |
| Independent chains | 512 | 1,537 | 1,536 | Only the root is shared between paths. |
| Shared width 8 | 4,096 | 25 | 272 | Larger shared-prefix/convergent case. |

Each strategy has six warmup cycles followed by five measured cycles, with arm
order rotated within each backend run. Warmups are retained. PostgreSQL uses an
isolated graph; its empty partitions are vacuumed before each scenario and
analyzed after fixture loading. Fixture writes roll back. Setup, statistics,
complete-value assertions, memory sampling, and EXPLAIN replays are outside
latency samples.

Time to first row starts before `Raw` and ends after the first successful
`Next` and `Scan` into `graph.Path`. Total time includes complete consumption
and close. PostgreSQL's existing GraphBench waterfall starts its `FirstRow`
stage after `Query` returns; with cached statements, pgx can wait for a DataRow
inside `Query`. That residual stage alone is not the end-to-end first-row
measurement used here. Pool acquisition, fixture loading, and Cypher compilation
are outside this study's timing boundary.

This is a modified-working-tree discovery run based on commit `74727f20`.
There was no host A/A calibration, confidence interval, frozen holdout,
backend-order-balanced confirmation, clean-source binary-bound capture, or
concurrency/cancellation qualification. Five samples describe these fixtures;
they do not establish a production latency threshold. Initial runs with shorter
warmup or without partition cleanup are not the canonical numbers below.

## Latency results

Warm median milliseconds, including decoding and path mapping:

| Graph | Paths | Extra payload per entity | Current PG total | Batch PG total | Neo4j total | PG first row, current → batch |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Shared prefixes/convergence | 512 | Narrow | 25.4 | 25.0 | 5.0 | 0.60 → 6.00 |
| Independent chains | 512 | Narrow | 176.4 | 22.0 | 7.3 | 3.05 → 8.45 |
| Shared prefixes/convergence | 512 | 2,048 bytes | 157.0 | 59.1 | 11.6 | 0.96 → 16.67 |
| Independent chains | 512 | 2,048 bytes | 156.1 | 57.2 | 11.0 | 1.96 → 24.75 |
| Shared prefixes/convergence | 4,096 | Narrow | 1,184.7 | 192.5 | 36.7 | 2.33 → 56.94 |

The narrow 512-path shared case is effectively unchanged end to end. The larger
shared graph improves by approximately 6.2x, while still taking about 5.2x the
Neo4j time. Improvement on independent chains shows that removing per-path SQL
assembly overhead matters even when persistent entity reuse is low. The study
does not separately quantify every contributor to the remaining backend gap.

First decoded row for `LIMIT 1`, in milliseconds:

| Graph | Payload | Current PG | Batch, outer limit | Compact input limited first | Neo4j |
| --- | ---: | ---: | ---: | ---: | ---: |
| Shared, 512 paths | Narrow | 0.68 | 3.84 | 0.50 | 0.36 |
| Independent chains, 512 paths | Narrow | 1.81 | 8.00 | 1.55 | 0.39 |
| Shared, 512 paths | 2,048 bytes | 2.15 | 9.90 | 1.16 | 0.50 |
| Independent chains, 512 paths | 2,048 bytes | 3.79 | 26.36 | 3.38 | 0.43 |
| Shared, 4,096 paths | Narrow | 2.53 | 39.40 | 1.45 | 0.28 |

The 4,096-path plan counters explain the limit behavior:

| PG arm | Recursive frontier rows | Compact paths entering batch | Batched relationship records | Batched node records |
| --- | ---: | ---: | ---: | ---: |
| Current full | 4,368 | — | — | — |
| Batch full | 4,368 | 4,096 | 272 | 25 |
| Current `LIMIT 1` | 273 | — | — | — |
| Batch, outer `LIMIT 1` | 4,368 | 4,096 | 272 | 25 |
| Compact input limited first | 273 | 1 | 3 | 4 |

The current helper's internal hydration counts are unavailable in these plans,
so dashes are not zeroes. Early selection matches the current limited query's
frontier count and reduces hydration. Breadth-first expansion still processes
prefixes before the first depth-three result.

## Memory and spill

Memory and EXPLAIN are separate, single diagnostic replays per arm/settings
combination. Client allocation is `TotalAlloc` delta while streaming and
discarding mapped paths. Extra live Go heap is sampled every millisecond above
the pre-query baseline, including other activity in the test process; it may
miss the actual peak.

| Wide 512-path fixture | Client allocation current → batch (MiB) | Sampled extra heap current → batch (MiB) | Largest visible batch executor node (kB) | Batch temp writes at 4MB / 64kB `work_mem` (MiB) |
| --- | ---: | ---: | ---: | ---: |
| Shared prefixes/convergence | 41.74 → 41.68 | 4.37 → 4.56 | 4,628 | 4.59 / 68.16 |
| Independent chains | 41.52 → 41.41 | 11.59 → 12.18 | 7,042 | 6.79 / 95.71 |

Current-query temporary writes at 64kB were 0.09 and 0.15 MiB, respectively.
The wide global batches retained their default spill volume even with an outer
`LIMIT 1`. The narrow 4,096-path global batch wrote 4.27 MiB at default settings;
its input-limited arm wrote no temporary blocks at those settings.

Returned paths repeat entity values, so client allocation and decoding remain
substantial. The candidate still repeats root/known-node composites per path
and builds full output arrays. Keeping relationship occurrences narrow has not
removed every wide intermediate.

PostgreSQL EXPLAIN memory fields cover planning and selected visible executor
nodes. The maximum visible node is not whole-query peak memory, and helper
internals are partly hidden. Root temporary-block counts include descendants
and are not summed across plan nodes. The supplied role cannot read backend
memory contexts or request backend memory logging; complete server peak memory
was not measured. Missing internal fields must remain unavailable, not zero.

## Correctness coverage and exclusions

The inventory spans 2,450 query identities, including 302 directly returning a
bound path. It includes ordinary cases, rendered template variants, and
metamorphic queries. Complete values preserve ordered nodes and relationships,
IDs, labels, types, endpoints, exact property types and integers, nulls, and
duplicate occurrences. Labels are compared as sets.

| Comparison | Enabled checks / path occurrences | Disabled checks / path occurrences | Total checks / occurrences |
| --- | ---: | ---: | ---: |
| Single-column corpus | 202 / 401 | 218 / 413 | 420 / 814 |
| Isolated multi-column path values | 49 / 53 | 49 / 53 | 98 / 106 |
| Limited corpus pages | 27 / 41 | 30 / 43 | 57 / 84 |
| Total | 278 / 495 | 297 / 509 | 575 / 1,004 |

The multi-column supplement covers 45 queries per mode and validates their path
columns against the original queries; other columns are outside this comparison.
The page supplement has 54 exact nontruncating multiset comparisons and three
truncating membership/count comparisons. Unordered pages may choose different
valid rows; their complete-value multiplicity must fit the unlimited result.
Six additional deterministic `LIMIT 1`, offset, and empty-page tests compare
ordered results exactly. Those six are outside the corpus totals above.

Coverage is 274 distinct query identities with optimization enabled, 293 with
optimization disabled, and 293/302 in at least one mode. These are query counts;
575 counts path-column comparisons across modes. Unsupported cases never count
as candidate successes. The baseline inventory's eligibility flag alone covers
only 218 distinct queries: the 45 multi-column and 30 limited-query supplements
produce the final total of 293. Page records marked `excluded` are omitted.

The nine remaining query identities are preserved verbatim in the
[recorded summary](path_batch_materialization_v1_results.json):

- Five already assembled path consumers: `multipart_inline`, `optimizer_inline`,
  `pattern_binding_inline`, and the relevant `lowering_regression_shapes` and
  `pattern_shapes` templates.
- Two mutations: named-path CREATE in `create_inline` and relationship-property
  CREATE in `create_shapes`.
- Two intentional shortest-path errors: same-endpoint cases in `shortest_bound`
  and `shortest_paths`.

Additional semantic tests cover graph-ID collisions, directed fast hints,
incoming and mixed directions, fixed undirected hops, cycles, self-loops,
repeated nodes, zero-length and optional-null paths, duplicate input rows, and
first-known-node override precedence. PostgreSQL variable-length undirected
expansion remains outside the existing translator's supported envelope.

## Action queue

These are proposed follow-up tasks, all open. They do not change the historical
P0–P6 priority order or enable a selector.

| ID | Follow-up | Completion evidence |
| --- | --- | --- |
| H1 | Implement a bounded hydration prototype with both path-row and byte caps. Preserve eligible `LIMIT`/`OFFSET` before hydration and ordering through a carried rank. Start with simple read-only final path projections. | Complete-value and page tests pass in both modes. A `LIMIT 1` plan admits one compact path and hydrates only its entities. Record first-row, full-drain, and early-close latency across the existing topology/width matrix and declared batch sizes. |
| H2 | Reduce remaining wide intermediates: root/known-node repetition, avoidable persistent fetches, and full-path sorting/grouping. Preserve graph-scoped identity and per-occurrence first-known-node precedence. | Raw plans show bounded buffered bytes, measured spill and memory fit a declared budget, and exact properties/multiplicity remain unchanged. A global deduplication of carried values requires proof that those values are interchangeable. |
| H3 | Add an end-to-end first-decoded-row metric to GraphBench's PostgreSQL waterfall, starting before `Query`. Retain stage timings so preparation and decode remain attributable. | Tests cover cached statements and a delayed first row. The metric includes waits inside `Query`; the existing post-Query `FirstRow` stage is labeled as residual. |
| H4 | Resolve the five earlier path-consumer cases and two mutations, or retain an explicit incumbent route. Preserve the two expected errors. Add required source-case, template, SQL-shape, and mutation coverage for any production translator change. | Mode-specific coverage records identify which path materializer actually ran. Existing path consumers, nulls, pagination, mutation visibility, and error contracts remain exact; no fallback execution is counted as candidate equivalence. |
| H5 | Qualify streaming and resource behavior before production integration. Keep one statement snapshot; a multi-query hydration design needs an explicit isolation/snapshot contract. Add cancellation, early-close, rollback, pool-reuse, and cap-overflow tests. | Clean-source, binary-bound captures satisfy the existing performance/resource gates, including host A/A, paired confirmation/p95, declared holdouts, attributable server memory, spill budgets, and safe fallback before row exposure. |
| H6 | Reproduce the original latency/error report on representative sanitized data with the exact query and parameter shape. Separate compact search, hydration, serialization/decoding, and error handling. | Exhaustive component comparisons preserve all mixed-type paths; matched plans and server errors explain the slow or failed query. Query parameters and sensitive endpoint IDs remain out of durable records. |

H1 and H3 can proceed independently. H2 informs H1's resource envelope; H4 and
H5 are required before broad production adoption. H6 validates how well the
synthetic findings explain the original report. No numeric promotion threshold
is chosen from these five-sample discovery timings; use the existing
[qualification contract](../cysql_traversal_priorities.md#performance-and-resource-gates).

## Evidence, validation, and reproduction

The [credential-free JSON summary](path_batch_materialization_v1_results.json)
retains all 35 arm medians and sample ranges, client and plan-resource metrics,
mode-specific coverage totals, exact remaining query names, backend/settings,
and SHA-256 references to the local raw artifacts. Source hashes in that file
describe the documentation-time working tree; source evolved during the study,
so those hashes do not bind a measured executable. There is no source/binary
qualification bundle or production selector ID for this test-only candidate.
The record identity is `path-batch-materialization-v1`.

Local ignored evidence remains under `.coverage/path-batch-study/`: `results.json`,
`neo4j-results.json`, `environment.json`, `corpus-inventory.json`,
`corpus-pages.json`, physical-layout records, emitted SQL, and raw EXPLAIN plans.
The durable summary preserves decision data when that directory is removed.
Reruns should use a fresh output directory to preserve the recorded hashes.

| Validation | Recorded result | Local log |
| --- | --- | --- |
| Full-value corpus, semantics, oracle, eligibility with `-race` | Pass, 36.144 s | `.coverage/path-batch-correctness-race.log` |
| Limited corpus, ordered pages, eligibility with `-race` | Pass, 9.407 s | `.coverage/path-batch-pages-race.log` |
| PostgreSQL five-fixture performance study | Pass, 33.638 s | `.coverage/path-batch-performance.log` |
| Neo4j five-fixture reference | Pass, 2.728 s | `.coverage/path-batch-neo4j.log` |
| Shared narrow fixture with client-memory sampler under `-race` | Pass, 8.277 s | `.coverage/path-batch-sampler-race.log` |
| PostgreSQL translation golden check | Pass | `.coverage/path-batch-translation-check.log` |
| Broader unit/full-suite build | Blocked by existing `pgxmock/v5` 5.1.0 incompatibility with pgx interfaces: missing `BatchResults.Tx` and `Rows.TypeMap` | `.coverage/path-batch-driver-build.log` |

Race-run timings are validation results and are excluded from latency tables.
No concurrency/cancellation or complete backend-memory qualification is implied
by those passes. Full backend suites should be rerun separately after resolving
the mock dependency mismatch. See [testing](../testing.md#path-materialization-experiment)
for the exact opt-in commands and database selection workflow.
