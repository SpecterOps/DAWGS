# P5 native adjacency extension feasibility v1

Date: 2026-08-19

Status: prospectively frozen, non-candidate plan. No extension implementation,
Cypher executor, selector, normal-schema dependency, protected timing, or
production support claim is authorized by this record.

## Decision

The P5 successor is one PostgreSQL-native, read-only adjacency-scan primitive
over the existing `public.edge` partitions and their covering indexes. Its
identity is `P5-NATIVE-ADJACENCY-SCAN-V1`; the extension package identity is
`dawgs_p5_native_adjacency_v1`.

This is a distinct architecture from the terminal adjacency materialization.
That relation made raw lookup faster, but nearly doubled storage, doubled or
tripled structural-write WAL, and caused two-to-four orders of magnitude
destructive-write latency regressions. The successor therefore maintains no
copy of graph data. It also adds no synopsis, mutation epoch, trigger, hook,
background worker, shared-memory state, or application-side service.

A server-local extension is the selected lane because it can execute inside
the invoking PostgreSQL backend and transaction snapshot while reusing the
existing physical adjacency indexes. The other untested architecture classes
still have an earlier unresolved prerequisite: a topology synopsis needs a
mutation/publication/cache-epoch contract, and an application-side service
cannot currently preserve the driver's Repeatable Read snapshot and exact
fallback boundary. Selecting the extension does not authorize either of them.

The complete frozen contract is
[`p5_native_adjacency_extension_feasibility_v1.json`](../../benchmark/testdata/scale/protocols/p5_native_adjacency_extension_feasibility_v1.json).

## Frozen implementation boundary

The feasibility implementation may add one opt-in PGXS package written in C,
one extension-owned schema, and one read-only diagnostic function:

```text
p5_native_feasibility.p5_native_adjacency_scan_v1(
  graph_id int4,
  anchor_id int8,
  edge_kind_ids int2[],
  inbound bool
) -> (
  edge_ids int8[],
  next_node_ids int8[],
  kind_ids int2[],
  scanned_index_tuples int8,
  heap_fetches int8,
  returned_rows int4,
  overflow bool,
  complete bool
)
```

The function is installed in the extension-owned `p5_native_feasibility`
schema. Each invocation resolves the exact `public.edge` child whose partition
bound equals `graph_id`, validates its inherited outbound or inbound covering
index, and performs one native index scan under `GetActiveSnapshot()`. It
constrains the physical `start_id` or `end_id` anchor, filters
wildcard/single/multiple kinds, reads `edge_id` and the opposite endpoint from
the covering tuple when MVCC visibility permits, reports every required heap
fetch, and stops at the immutable 4,096-row cap plus one sentinel. The SQL
function is `STABLE`, `STRICT`, security-invoker, and parallel-restricted.
It always returns exactly one row, including empty arrays and final counters
for a missing anchor, so zero-result work cannot disappear from telemetry.

Timed native and direct-SQL arms each return one row containing parallel arrays
and do not add an `ORDER BY`; the direct arm uses the equivalent bounded query
and aggregate. The capture runner canonicalizes copied edge/next/kind triples
by `(kind_id, edge_id)` only after timing before comparing the complete bounded
multisets. The function is not a Cypher result source and does not implement
traversal, path uniqueness, hydration, a selector, or fallback.

The extension must not use `shared_preload_libraries`, install hooks or
workers, retain backend-global graph state, depend on an unsafe `search_path`,
or receive a dependency from the normal DAWGS schema. Normal `CreateSchema`,
translation, driver startup, and `cypherTranslationCacheKey` remain unchanged.

## Gate 0: matched-major delivery

One clean source archive must produce separate PostgreSQL 17 and PostgreSQL 18
artifacts. Each artifact is built by the matching major's `pg_config` and
server headers, with warnings treated as errors, and may be loaded only into
that major. The initial feasibility platform is explicitly Linux/amd64; this
is not a general production support matrix.

For each major, the capture must bind the control file, versioned installation
SQL, PGXS Makefile, C sources, unstripped build identity, stripped installed
library, compiler, PostgreSQL headers, server version, image identity, and
checksums. The lifecycle is build, isolated installation, `CREATE EXTENSION`,
exact smoke probe, `DROP EXTENSION` without `CASCADE`, reinstall, and a second
exact probe.

The planning inventory exposes the first concrete blocker rather than hiding
it: the live capture server is PostgreSQL 17.10, the locally available PGXS is
18.4, and CI currently runs only a PostgreSQL 18 service. No feasibility
capture may begin until a matched PostgreSQL 17 build-and-load environment and
the PostgreSQL 18 lane both exist. Cross-major loading is a terminal failure,
not a workaround.

## Gate 1: exact read and snapshot behavior

The open read fixture is `testutil.NewHopScaleFixture` at fanouts
32/33/128/512/513/1,000/4,096/4,097. It covers inbound and outbound physical
directions, wildcard/single/multiple kinds, missing anchors, self-loops,
cross-kind parallel edges, graph isolation, and the exact cap/cap-plus-one
boundary. No new generated corpus and no existing protected declaration may be
used.

Every native result must byte-match the canonical bounded direct-SQL multiset
on both server majors. The function must reject a graph ID whose exact child
partition or matching inherited index cannot be proven. Missing, hidden, or
contradictory native-scan, returned-row, heap-fetch, sentinel, partition, index,
or direction evidence fails closed. A concurrent-writer test holds the direct
and native probes on the same connection under Repeatable Read and proves they
retain the pre-writer snapshot through transaction end.

Cancellation must interrupt a high-degree invocation promptly. After rollback,
the pool must reacquire the same backend PID and execute an exact probe.
Graph create/reload/drop isolation and extension drop/reinstall symmetry are
also mandatory.

## Gate 2: zero mutation and storage coupling

`testutil.NewDirectWriteScaleFixture` at 1, 1,000, and 2,000 targets supplies
the existing create, conflict/upsert, property-only update, relationship
delete, node-delete cascade, graph-clear/reload, and graph-drop roster. The
extension stays idle during these operations.

Source inspection and catalog evidence must show zero graph relations, graph
indexes, mutation triggers/hooks, background workers, shared-memory bytes, and
graph-proportional storage. Read probes must emit zero WAL and use zero
temporary bytes. No WAL may be attributable to the extension during mutation;
installed-versus-absent statement WAL must remain within the frozen 1.00 ratio,
and mutation latency within 1.05. The stripped per-major shared library is
capped at 1 MiB. Catalog storage is limited to the extension row, its owned
schema, and its single function and is reported separately from graph storage.

Any graph-proportional object or mutation-path dependency terminally stops this
identity. It may not be justified by a faster read result.

## Gate 3: raw-read headroom

Each server major gets its own order-balanced direct-SQL/direct-SQL A/A
artifact and eight doubled-Williams SQL/native blocks. Every arm/case/block has
five warmups and twenty timed observations with pool size one under Repeatable
Read. Reports bind physical chronology, a clean source archive, protocol,
fixtures, extension sources, per-major binary, capture runner, and sanitized
environment. Confidence is 97.5% and caps cannot be overridden.

For every normal or envelope case, the native p50 must remain within either
1.10x or 100 microseconds of direct SQL, and p95 within either 1.20x or 200
microseconds after applying that host's A/A floor. Attributed shared-buffer
work may not exceed 1.10x. On each server major, at least one non-overflowing
typed or wildcard case with fanout at least 1,000 must improve p50 by at least
5% or 100 microseconds. Exactness and operational gates take precedence over
latency.

These measurements answer only whether matched-major packaging and the
backend-local covering-index boundary have enough headroom for further research.
They are not Cypher performance or promotion evidence.

## Stop and next authorization

Failure of either server-major lifecycle, any exactness or snapshot check, any
zero-state/resource requirement, or the raw-read gate terminally stops
`P5-NATIVE-ADJACENCY-SCAN-V1`. Its matrix, cap, fixtures, schedule, and limits
must not be weakened or retuned. Another attempt requires a distinct
architecture and protocol identity.

A complete pass authorizes only two separately frozen artifacts: a tool-only
native traversal candidate roster and a prospective power study calibrated
from these open traces. It still does not authorize Cypher translation,
automatic selection, a required database extension, normal schema changes,
protected access, production packaging, or a support claim.

The next executable slice is therefore the matched-major delivery harness and
the single diagnostic function, followed by lifecycle/exactness tests. Raw
timing starts only after both PostgreSQL 17 and 18 lanes pass those untimed
gates.
