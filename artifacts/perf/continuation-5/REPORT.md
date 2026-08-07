# Performance continuation 5 baseline

Date: 2026-08-07

This directory is the checksum-bound baseline for `perf_cont_5.md`. The raw
live-v2 artifacts remain in their original directory and are referenced by
path and SHA-256 in `manifest.json`; they are not rewritten or duplicated.

The entering live-v2 run is discovery and qualification evidence. It proves
that graph cardinalities were unchanged, but it does not claim an
identity-equivalent Neo4j comparison. It narrows the qualified production
envelope for deep physical-inbound searches and multi-kind singleton path
state.

The frozen containment policy is `sp-static-v3`, with stable fallback reasons
`deep_inbound_unqualified` and
`non_single_kind_path_state_unqualified`. Normal, envelope, and stress tier
definitions are frozen in the manifest before candidate measurement.

Credentials, connection strings, endpoint IDs, and raw sensitive properties
are not part of this bundle.

## Repository implementation disposition

The repository increment implements the safety boundary and the platform
needed to collect the remaining evidence:

- `sp-static-v3` is the production selector. It records direction, physical
  expansion, named-kind count, wildcard state, topology class, structural
  eligibility, and static eligibility. Deep physical-inbound searches use
  `deep_inbound_unqualified`; wildcard/multi-kind one-path state uses
  `non_single_kind_path_state_unqualified`. Structural reasons retain
  precedence and forced S3 remains qualification-only.
- Deterministic shortest fixture v2 supports hidden fan-in, mirrored fan-out,
  parallel kinds/targets, diamonds, disconnected exhaustion, payload, cycles,
  and self-loops. Strict names, logical relationship keys, checksums, exact
  topology expectations, physical cardinality, and normal/stress corpus cases
  are tested without changing legacy fixtures.
- GraphBench has an existing-graph PostgreSQL mode that bypasses schema
  assertion, clear/load, and vacuum; rejects mutations before runner creation;
  resolves versioned logical-key anchors; verifies before/after counts; hashes
  sensitive observations and identifiers; and supports progress, atomic
  checkpoint/resume, predeclared timeout classes, and adaptive-discovery
  labeling. Adaptive artifacts are refused by the complete release gate.
- PostgreSQL reference tournaments include `SP-S4-C-D`,
  `SP-S4-C-WE+MAT-M0`, and `ASP-A1-DAG` exact full-comparator prototypes.
  Plan metrics expose frontier, witness, meeting, and hydration rows, and the
  independent resource gate rejects normal/envelope portable-candidate spill,
  local workspace, and read-only WAL.
- The singleton tie policy promises one valid minimum relationship-unique
  trail, not a PostgreSQL physical edge-ID order. `allShortestPaths` retains
  exact relationship-distinct multiplicity.

`make test_all` passes independently against PostgreSQL and Neo4j, including
the race-enabled unit suite and the serialized integration suite. The supplied
PostgreSQL `localhost` endpoint resolved to an unavailable IPv6 listener, so
the successful run used the same database over its reachable IPv4 loopback
address. `make format` could not run because the sandbox lacks `goimports`;
every touched Go file was formatted with `gofmt` and compiled by both backend
suites.

## Generated live validation

GraphBench ran the fixed `normal-tier` corpus against both live backends with
three timed iterations and one fixed warmup. All 42 PostgreSQL records and all
43 Neo4j records completed with `ok` status. The shortest fixture v2 subset
contributed six successful records on each backend and verified these v3
decisions on PostgreSQL:

- outbound distance: `SP-S3-U-D`;
- deep physical-inbound distance and path: `SP-S0` with
  `deep_inbound_unqualified`;
- multi-kind distance: `SP-S3-U-D`;
- multi-kind singleton path: `SP-S0` with
  `non_single_kind_path_state_unqualified`; and
- diamond all-shortest: independent `SP-S0` handling with exact two-path
  multiplicity.

The independent resource report passes. The descriptive backend-delta report
records observation equality where the public observation is deterministic;
backend-native IDs and permitted singleton tie choices remain descriptive and
are not PostgreSQL release gates. The durable artifacts are:

- `generated-normal-live.jsonl` (`sha256:6c1aef91370f6551e177ff7312f0030210f1cc338fda8d4d15b7d56429b819e1`);
- `generated-normal-resources.json` (`sha256:7c7d0e5c22c2d34343f07e95f85c739b750109cf6548b4caab469dfaa9ce3301`); and
- `generated-normal-backend-delta.json` (`sha256:2bf3fe94e17cc50c2deaab2edf2b248ccda8d6bccbff3016c30cb4eede639af9`).

The artifacts contain no connection strings or supplied credentials.

## Restored real-world live-v2 rerun

The preserved 147-case harness was rerun after the original graph was restored
and its exact cardinalities verified. The matched result is recorded in
`REAL_WORLD_DELTA.md` and changes the release disposition: `sp-static-v3`
recovers hidden-fan-in D64 latency by roughly 70-80x, but blanket inbound
containment regresses cheap direct-inbound cases by 3-12x, multi-kind path
fallback regresses K7/D2 by 63%, and two formerly successful cases time out.

The strict read-only run also proves that `SP-S0` cannot initialize its
temporary workspace while `default_transaction_read_only=on`; 22 contained
cases failed on temporary `DROP TABLE`. A separately guarded `pg_temp` rerun
provides diagnostic performance numbers but does not convert that safety-path
failure into a release pass. N1 and N9 therefore have failed live
qualification dispositions.

## Evidence-gated work still open

This report does not claim Plan 5 complete without sanitized-data
qualification. PostgreSQL and Neo4j integration connections were validated,
but no identity-equivalent sanitized graph or anchor manifest was supplied.
Consequently:

- N1 generated integration, live normal-tier, and race validation passes on
  both backends, but restored sanitized-graph containment/regret qualification
  fails as documented in `REAL_WORLD_DELTA.md`;
- N3/N4 S4 prototypes are not activated and native bidirectional feasibility
  remains open;
- N5 runtime overflow remains closed to production, with `StateLimit` zero;
- N6 `ASP-A1-DAG` remains tool-only;
- N7 exact count architecture is not triggered because no product latency and
  write-cost objective was supplied, so `COUNT-C0` remains selected; hydration
  tail attribution awaits live sampling;
- N8 identity-equivalent generated-fixture observations were validated, but
  sanitized real-data Neo4j evidence is absent; ADCS remains closed because
  the live-v2 graph has no complete `TrustedForNTAuth` suffix; and
- N9 PostgreSQL/Neo4j `make test_all` and the fixed generated normal-tier live
  corpus pass, while PostgreSQL real-data release qualification fails. Neo4j
  same-data comparison, cancellation, soak, and cumulative release reports
  remain open.

These are evidence and product-input dependencies, not silently waived gates.
