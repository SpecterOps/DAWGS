# Traversal priority implementation status v1

Date: 2026-08-13

Status: canonical-I1 qualified; SP-I2 unqualified; suffix guard rejected; production unchanged

This record separates repository implementation from empirical promotion for
[`cysql_traversal_priorities.md`](../cysql_traversal_priorities.md). The
candidate algorithms, exact fallbacks, diagnostic surfaces, qualification
corpora, and fail-closed gates are repository code. This change does not claim
new latency results and does not fabricate a clean M0 capture from a modified
working tree. Consequently, no new automatic suffix, SP, ASP, endpoint,
predicate, or `ExpandInto` selector is enabled.

## Immutable identities

| Concern | Implemented identity |
| --- | --- |
| Ordinary orientation policies | `orientation-probe-v1`, `orientation-probe-v2` (v2 rejected) |
| Ordinary incumbent | `EXPANSION-STEPWISE-FORWARD` |
| Fixed-suffix candidate | `EXPANSION-SUFFIX-SEEDED-REVERSE` |
| Static fixed-suffix guard | `suffix-reverse-guard-v1` (terminally rejected) |
| Existing endpoint candidate | `EXPANSION-ENDPOINT-SEEDED-REVERSE` |
| SP strict node alternation | `SP-B1-C-ALT-NODE-D`, `SP-B1-C-ALT-NODE-WE+MAT-M0` |
| SP smaller current level | `SP-B2-C-MIN-LEVEL-D`, `SP-B2-C-MIN-LEVEL-WE+MAT-M0` |
| ASP strict node alternation | `ASP-B1-DAG-ALT-NODE` |
| ASP smaller current level | `ASP-B2-DAG-MIN-LEVEL` |
| SP/ASP production controls | `SP-S3-U-D`, `SP-S3-U-E+MAT-M0`, `SP-S4-C-D`, `SP-S4-C-WE+MAT-M0`, `ASP-A1-DAG`, `SP-S0` |
| Inline production canaries | `SP-I1-C-WE+MAT-M0`, `SP-I2-C-D`, `ASP-I1-U-DAG+MAT-M0` |
| Inline tool-only executors | `SP-I1-C-D`, `SP-I1-U-E+MAT-M0` |
| Bounded endpoint analysis | `endpoint-resolution-v1` |
| Traversal predicate analysis | `traversal-predicate-v1` |
| Fixed one-hop study | `expand-into-study-v1` |

## Milestone disposition

| Milestone | Repository implementation | Promotion disposition |
| --- | --- | --- |
| M0 | Capture bundle v3 binds source state, patch and untracked payloads, dependency files, executable, the complete sorted corpus declaration and identity, evidence checksums, and sanitized environment metadata. Its independent verifier reconstructs and validates the bundled source and corpus fingerprints. Host-bound A/A schema v4 requires two explicitly executed, order-balanced arms plus artifact-bound physical chronology; frozen training/holdout declarations are enforced. | A fresh clean-source capture is still required. A dirty diagnostic bundle cannot qualify promotion. |
| M1 | Traversal telemetry v2 separates summary identity from untimed diagnostic replay and carries per-field provenance/completeness. PostgreSQL diagnostics fail closed for hidden function work and give guarded SP-I1, SP-I2, and ASP-I1 distinct typed counter families. Neo4j reads use `PROFILE`, preserve ordered children and actual metrics, and explicitly mark opaque SP/ASP internals. Plan-delta v2 uses union pairing and semantic stages. Resource gate v5 enforces attribution, caps, measured memory, spill/WAL policy, fallback, hydration, and inactive-arm work. | Missing, hidden, contradictory, or unattributable counters fail qualification; they are never treated as zero. |
| M2 | The common typed orientation decision records planned/emitted policies, candidates, caps, admission, and fallback separately. Guarded and shadow fixed-suffix statements use bounded root/suffix/directional-degree probes, cap+1 sentinels, strict 3/4 hysteresis, bounded reverse state, and exact forward fallback. The independent static suffix guard removes topology probes, caps suffix/state rows at 512, and marker-gates exact reverse and forward executors. | Production fixed-suffix translation remains the exact forward incumbent. Orientation v2 and `suffix-reverse-guard-v1` both failed their immutable training overhead gates and are terminally rejected. The already-shipped endpoint family retains its established 32/33 endpoint and 4096/4097 state guards. |
| M3 | Compact B1/B2 SP functions retain ID-only two-sided frontier/seen/predecessor state, exact 0/1/2-hop controls, typed schedulers, lower-bound termination, deterministic minimum witnesses, late hydration, invocation-local diagnostics, and exact S4 fallback on cap overflow. GraphBench exposes four full-comparator reference arms on a carryover-balanced three-arm schedule. `SP-I1-C-WE+MAT-M0` has guarded predecessor/witness execution. `SP-I2-C-D` adds reverse-physical hidden-fan-in distance discovery with state/frontier gates, exact S4 fallback, branch receipts, exact-bucket manifest authorization, diagnostic attribution, and an evidence-free rollback switch. Syntax-open singleton SP uses the existing depth-15 policy with explicit provenance. | B1/B2 and legacy unguarded I1 executors remain forceable/reference candidates only. `sp-static-v6` and `sp-static-v8-hidden-fanin` are default-off exact-query canary selectors. SP-I2's dirty training rehearsal won its five targets but missed the frozen cycle-control bounds; the clean-source barrier prevented a freeze and kept all holdouts unopened. |
| M4 | Promotion manifest v2 strictly decodes candidate-specific confirmation and performance evidence and accepts exactly six roles, one query digest, unique buckets, and the canonical training/holdout split. A/A, resource, and reference-closure wrappers embed exact native producer bytes; cross-role validation closes the confirmation/performance/resource candidate artifact, exact performance/resource round and receipt-chain sets, and each reference workload digest against exactly one PostgreSQL A/A workload. Operational gate v2 embeds its canonical 32-record input and digest so final verification recomputes the matrix, cancellation, snapshot, isolation, overflow, and disposition; duplicate input keys fail closed. The independently frozen candidate SQL digest is checked before execution. Orientation v1 remains readable but is rejected for schema insufficiency; v2 remains readable but is terminally rejected because its immutable training overhead gate failed. Default-off driver policies partition the cache by generation and expose evidence-free switches for orientation, endpoint reverse, SP witness, SP distance, and ASP DAG. | The clean `6d56a609` canonical-I1 confirmation passed all four training and three holdout cases with zero fallback and resource-gate v5 passing all 70 case-round records. Automatic production remains unchanged pending exact production-statement, reference-closure, operational, and final manifest evidence. |
| M5 | B1/B2 ASP functions retain all same-minimum-depth predecessors on each side, select one deterministic completed meeting cut, saturate pre-enumeration counts, stage unique ordered edge arrays, and enforce separate discovery, predecessor, enumeration, and output-byte sentinels before exact A1 fallback. Full-multiset references and stress/cap cases are included. `ASP-I1-U-DAG+MAT-M0` has a typed inline emitter, exact bounded one/two-hop preflights, four cap+1 guards, exact A1 same-statement fallback, event-chain runtime receipts, inactive-arm evidence, exact-query manifest buckets, a kill switch, and live driver-policy/isolation/cache/rollback coverage. | B1/B2 ASP remain forceable/reference candidates. `ASP-A1-DAG` remains the automatic production choice. I1 is a default-off, stable-snapshot, exact-query canary; broader activation still requires clean evidence. |
| M6 | Optimizer diagnostics conservatively classify bounded endpoint sources and traversal predicate locality without changing execution. A property name alone is never considered a uniqueness proof; parameterized and literal small sets use the 32/33 contract. Fixed one-hop translation has an optimizer-independent exact dual-bound fallback, recognizes carried and node-valued `UNWIND` endpoints, and preserves directionless self-loops in unbound, single-bound, and dual-bound forms. The corpus and three exact PostgreSQL study arms cover pair join, lower-degree scan, pair reuse, both logical directions, wildcard/multi-kind edges, missing pairs, duplicates, and self-loops. Confirmation now requires material improvement, p95 containment, and one stable winner across separate training and holdout partitions. | Endpoint/predicate broadening remains analysis-only until the SP/ASP candidates it would feed qualify. The `ExpandInto` report is a study and cannot activate a policy. |
| M7 | The versioned topology-synopsis ADR records schema, mutation, refresh, staleness, cache-key, graph-lifecycle, and rollout requirements. | Deferred. Runtime probes remain authoritative; no synopsis schema or cache dependency is introduced. |

## Qualification invariants

Release-eligible evidence must satisfy all of the following:

- complete declared corpus coverage, with diagnostic selections unable to pass;
- checksummed host-matched A/A evidence and balanced rounds at 97.5% confidence;
- a target p50 improvement clearing 5% or 100 microseconds and contained p95;
- independent, nonempty training and frozen-holdout passes for every concrete
  prioritized candidate family;
- exact stable observations and SP witness validity or complete ASP/ordinary
  result multisets as appropriate;
- complete required search and hydration telemetry with measured, attributable
  resource use;
- at-most-once probes, zero work in unselected arms, and an exact, single,
  declared fallback before output;
- cancellation, rollback, session reuse, pool isolation, and schema-down
  symmetry.

Stress cases are correctness/resource diagnostics. Their timing cannot tune or
promote a selector, and a stress fallback is accepted only where the case and
candidate declare that exact fallback.

## Evidence still required for promotion

Promotion is a later evidence-producing change. It must start from a clean
source checkout and publish credential-free checksums for the baseline and
candidate binaries, corpus declaration, source revision, database versions,
host A/A report, matched plans, discovery, confirmation, frozen holdout,
resource, reference-closure, cancellation/concurrency, and bundle-verification
reports. A passing report then enables only the named runtime-recognizable
topology and observation buckets; all other shapes retain their incumbents.
