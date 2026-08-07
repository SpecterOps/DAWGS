# L3A ADCS discovery status

Date: 2026-08-07

Status: native A3 qualification complete. A2 and A4 are closed; A3 is retained
as a qualification-only native emitter. Automatic selection is closed for this
continuation because the confirmation matrix proves a data-dependent crossover
and no bounded selector passed the L4 gates.

## Implemented foundation

- Optimizer diagnostics classify structurally eligible ADCS targets and list
  `ADCS-A0`, corrected `ADCS-A2`, `ADCS-A3`, and `ADCS-A4` candidates while
  selecting the incumbent stepwise strategy.
- Corrected PostgreSQL reference arms expose ordered-ID and complete public
  observation boundaries for A0/A2/A3/A4.
- Generated fixtures cover endpoint and path observations, depth and fanout,
  sparse/half/all suffix density, 4 KiB payload, zero reachable boundaries,
  disconnected boundaries, and high reverse fan-in.

The repository-native A3 emitter rewrites the incumbent expansion and fixed
suffix frames into root-presence, exact suffix-bag, distinct boundary-seed,
reverse-recursive trail-state, and exact suffix-rejoin CTEs. Recursive paths
prepend relationship IDs, reject repeated expansion edges with `ALL(path)`,
exclude suffix-edge overlap, and retain graph-scoped node-existence checks. The
tool-only forcing contract accepts only a structurally eligible, bound-root A3
target and fails closed unless translation records that A3 was actually
emitted. Automatic ADCS selection remains off.

## Live reference smoke

The complete 16-case generated ADCS corpus ran all eight selected reference
arms under PostgreSQL `auto`, `force_custom_plan`, and `force_generic_plan`.
All 16 top-level records and all 128 reference arms completed exactly in each
mode, for 384 exact reference executions overall.

The one-sample smoke reproduces the intended crossover diagnostic:

| Plan mode | D16/F1000 A0 | D16/F1000 A3 | High reverse fan-in A0 | High reverse fan-in A3 |
|---|---:|---:|---:|---:|
| `auto` | 33.941 ms | 12.639 ms | 6.401 ms | 14.487 ms |
| `force_custom_plan` | 31.889 ms | 12.811 ms | 4.456 ms | 15.080 ms |
| `force_generic_plan` | 51.099 ms | 6.588 ms | 1.777 ms | 3.595 ms |

These timings are diagnostic and cannot select an architecture or plan mode.
They show why the formal tournament must keep sparse and high-reverse-fan-in
tiers paired and must attribute emitter and planner policy independently.

## Matched primary discovery

Five independently reloaded rounds with five warmups and ten measured samples
per arm were captured for the four frozen crossover cases. Reports use the
explicit `discovery` protocol and 97.5% intervals. Ordered-ID arms now retain
timing only after their exact node-ID, endpoint-ID, and edge-ID arrays match the
canonical A0 observation.

Under normal `auto` planning, the ordered-ID median-ratio upper bounds were:

| Candidate | D16 sparse endpoint | D16 sparse path | Zero reachable | Reverse fan-in 1,000 |
|---|---:|---:|---:|---:|
| A2 | 3.968 | 4.371 | 2.971 | 2.914 |
| A3 | 0.469 | 0.547 | 1.408 | 3.490 |
| A4 | 0.494 | 0.516 | 1.389 | 2.989 |

Complete-result comparisons preserve the same crossover. A3's ratio upper
bounds were 0.703/0.464 on sparse endpoint/path and 2.395/3.599 on zero-result
and reverse-fan-in controls.

Disposition:

- A2 is Pareto-dominated on every primary crossover case and is closed.
- A4 has no stable auto-plan tier where it improves on the A3 decision while
  meeting control gates, so it is closed for forced-emitter work.
- A3 materially wins the sparse D16/F1000 tier and advances as the only forced
  AST candidate. It may not run unconditionally because it materially regresses
  zero-result and high-reverse-fan-in controls.
- Forced generic planning changes several winners but still regresses the
  reverse-fan-in control. No global or driver-level plan-mode change advances;
  production remains on PostgreSQL `auto`.

## Artifact checksums

| Artifact | SHA-256 |
|---|---|
| `postgres-l3a-reference-smoke-v1.jsonl` | `5af3903a6399c58ad1c7eb255855c2030831152593cb3732d7b73a7545182a4e` |
| `postgres-l3a-reference-force_custom_plan-smoke-v1.jsonl` | `822e597270829968a6a5429beff0bf2439e7c6613c67fc537455b5d66c433c70` |
| `postgres-l3a-reference-force_generic_plan-smoke-v1.jsonl` | `b1196970f41246355cb4b9063b3271a890b03f28b11e7d50464c8275325ae4e0` |
| `postgres-l3a-discovery-auto-ordered-v3.jsonl` | `35364c9a9d6107b53cc4cec0c5a3c9a6cdc437b68761e03ec14c89962130217c` |
| `postgres-l3a-discovery-custom-ordered-v3.jsonl` | `98a803888995cc128baa2802dfb5f7919463af73f09282bfcd61e1ed0480cb2b` |
| `postgres-l3a-discovery-generic-ordered-v3.jsonl` | `39a63d3dc67a4503d30810d4541bded9ff4cbe9e8b4153a297602a77653c537d` |
| `postgres-l3a-auto-ordered-a3_suffix_seeded_reverse_ordered_ids-report-v3.json` | `bd1e75a3071ddec36ea8a55bdd87dc9c26e5db48419131367bb4ca62a5646873` |
| `postgres-l3a-auto-complete-a3_suffix_seeded_reverse_complete-report-v1.json` | `d5189543d51e73116673e8b5cb7d994d4d739a9af9ae3c722b7d11f03028dbde` |

## Native qualification

The full 16-case generated ADCS corpus passed exact native A3 execution for
endpoint and path observations. A ten-round, 20-warmup/50-measurement closure
against `a3_suffix_seeded_reverse_complete` passed all four primary cases; the
worst median-ratio upper bound was 0.201105.

The ten-round incumbent/native confirmation materially favored A3 on sparse
endpoint, sparse path, and zero-result cases. The high-reverse-fan-in control
regressed: its p50 ratio upper bound was 1.951273 with a positive median-change
lower bound of 0.605740 ms. The sparse endpoint/path p50 ratio upper bounds were
0.032737 and 0.047611. This is diagnostic evidence because the architectures
intentionally have different SQL and plan fingerprints.

Live PostgreSQL tests additionally prove positive recursive work, no local or
temporary buffers and no read-only WAL, exact execution at concurrency 1/2/4
with a two-connection pool, cancellation with SQLSTATE `57014`, rollback,
same-backend-PID reuse, and an exact successful rerun.

## Final L4 disposition

Suffix density and reverse fan-in are data properties, not statically bounded
query facts. An unconditional A3 selector would violate the high-reverse-fan-in
control. No bounded runtime probe with same-snapshot overflow fallback has
passed the threshold, regret, overhead, cancellation, and resource gates.
Following the plan's explicit failure rule, automatic A3 selection is closed
and production retains exact forward stepwise lowering. A3 remains available
only through the fail-closed GraphBench/tool seam for future selector research.

## Native artifact checksums

| Artifact | SHA-256 |
|---|---|
| `postgres-a3-native-semantic-v1.jsonl` | `a05cd4189c07dc4df992430b7d6de2e3ea7463c8d60d1fe73556077f36cb0c1c` |
| `postgres-a3-native-reference-closure-v1.jsonl` | `755fcf22df2baf515cedd207b7362f97ae6fee3d1c0bc98da79e15b761d278bd` |
| `postgres-a3-native-reference-gate-v1.json` | `e1ad47d3e8c2cf0d7163132dbe45c9d9a75e98a4e7287689f4aa3c0c51c2ec0c` |
| `postgres-a3-confirm-incumbent-v1.jsonl` | `8ac00f07f5c84782ef917eadb189f14f1e5f4a82f49dcae4a4c76be8fdd5459c` |
| `postgres-a3-confirm-candidate-v1.jsonl` | `516f001ca569d8f4b887a157dea6eed374a3752f561badfcb5c06edf4769de28` |
| `postgres-a3-confirm-report-v1.json` | `ed215cbd9a864584ca15039fa118f95779a102bd75ac7c279711bf8964c7aca3` |
