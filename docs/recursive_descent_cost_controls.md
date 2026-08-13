# Recursive-descent cost controls

Date: 2026-08-09

This implementation addresses the six recursive-descent findings from the PostgreSQL/Neo4j delta review. It changes
the PostgreSQL execution architecture; it does not claim that the cross-backend latency gap is closed until the same
corpus is recaptured against both supplied backends.

The next-phase orientation, SP/ASP, topology-evidence, and qualification work is
sequenced in the [CySQL traversal performance priorities](cysql_traversal_priorities.md),
with current candidate and promotion status recorded in
[the implementation status](experiments/traversal_priority_implementation_status_v1.md).

| Finding | Implemented control |
|---|---|
| 1. `allShortestPaths` retained too much trail state | `ASP-A1-DAG` performs minimum-layer discovery, stores all relationship-distinct predecessors only for minimum layers, then enumerates the predecessor DAG. |
| 2. Small depths paid recursive setup cost | Both production functions have exact one-hop and two-hop SQL arms before workspace allocation. |
| 3. Breadth-first levels churned temporary catalog objects | Session-local workspace v2 is created once per connection and reset once per invocation. A1 and S4 derive each frontier from depth-tagged seen state and share one candidate relation instead of swapping or repeatedly truncating frontier tables. |
| 4. Singleton shortest paths needed a bounded compact search | `SP-S4-C-D` and `SP-S4-C-WE+MAT-M0` use canonical ID-only BFS state, a 100,000-state default ceiling, and exact same-statement fallback. |
| 5. Recursive rows hydrated entities too early | New executors carry node/relationship IDs and perform one ordered path hydration after search. |
| 6. Repeated compilation and unstable recursive estimates added overhead | Functions declare `COST`/`ROWS` and set `recursive_worktable_factor`; the driver has a bounded, coalescing, parameter-shape-aware translation cache. |

Terminal-selective ordinary expansions also have a guarded reverse lowering. The optimizer only selects it for one
fixed directed prefix hop followed by a terminal directed expansion (`*1..64`) with one relationship kind and a local
terminal ID/property search. The statement probes 33 endpoints and 4097 reverse states: up to 32/4096 uses the reverse
candidate, while either sentinel activates the exact forward incumbent in the same snapshot. Candidate output is
gated until both probes finish, so overflow and cancellation cannot leak partial results.

## Selection boundaries

`asp-static-v1` selects `ASP-A1-DAG` only for one read-only, non-optional, directed `allShortestPaths` traversal with one
static ID equality per endpoint, minimum depth one, no path/relationship predicate, and no observed relationship value.
An open maximum uses depth 15. Minimum-depth-zero, self-endpoint, directionless, correlated, mutation, and predicate
shapes retain the incumbent exact executor.

`sp-static-v5-contained` retains `SP-S3-U-D` for qualified distance work, with
`SP-S4-C-D` for deep physical-inbound distance searches. Already-qualified,
bounded, directed, single-kind one-path witnesses use `SP-S3-U-E+MAT-M0`;
deep inbound and multi-kind or untyped witnesses retain
`SP-S4-C-WE+MAT-M0`. This containment avoids paying the S4 workspace boundary
where the relationship-trail executor is the better incumbent. S4 checks a
cap+1 state ceiling before emitting any row and records its exact
`SP-S3-U-E+MAT-M0` fallback in the same statement and snapshot.

`SP-I1-C-WE+MAT-M0` is a separate default-off canonical-predecessor canary for
the directed singleton one-path envelope. Its guarded inline statement uses
four cap+1 gates, hydrates only after admission, and falls back through S4. A
state overflow can therefore produce the auditable event chain
`SP-I1-C-WE+MAT-M0 -> SP-S4-C-WE+MAT-M0 -> SP-S3-U-E+MAT-M0` without exposing
rows from an abandoned arm. Stable isolation, an exact manifest bucket, and
positive immutable caps are mandatory; `DisableInlineSPWitness` is the
evidence-free rollback switch.

`ASP-I1-U-DAG+MAT-M0` is also available through the production policy as a
default-off exact-query canary. It is limited to a singleton directed endpoint
pair, `allShortestPaths`, minimum depth one, and an explicit maximum no greater
than 64. Exact one- and two-hop targets are resolved before recursive
discovery. The typed recursive statement bounds distance discovery,
same-minimum-depth predecessor retention, all intermediate enumeration states,
and output bytes with immutable cap+1 sentinels. It exposes candidate and
fallback markers only after every guard is known; any overflow selects exact
`ASP-A1-DAG` before public output. Its canary requires a stable transaction
snapshot and a manifest whose topology bucket matches the optimized target.
Runtime receipts use schema v2 and retain the complete ordered branch-event
chain rather than overwriting nested fallback evidence. The automatic
`asp-static-v1` choice remains A1 until clean confirmation,
holdout, resource, and reference-closure evidence authorizes broader rollout.

`EXPANSION-SUFFIX-SEEDED-REVERSE` remains tool-only. Existing evidence showed a
fixed-suffix expansion topology crossover that query shape alone does not safely
bound, so this work does not activate the strategy in production. The rejected
bounded-fallback and continuation experiments are retained only as historical
decision records under `docs/experiments`.

## Qualification contract

GraphBench recognizes `ASP-A1-DAG`, `ASP-I1-U-DAG+MAT-M0`, `SP-S4-C-D`, and `SP-S4-C-WE+MAT-M0` as applied architectures. Their resource gate
allows the declared local workspace but rejects executor temporary-file reads/writes and WAL for non-mutating queries.
Use the generated depth/fanout corpus, exact path observations, planner modes, concurrency, cancellation/session reuse,
and matched PostgreSQL/Neo4j delta report before treating the implementation as performance-qualified.
