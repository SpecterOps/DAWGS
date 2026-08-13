# Inline all-shortest-path predecessor DAG v1

Date: 2026-08-12

Status: implemented as a default-off production canary; automatic selection
withheld pending clean qualification evidence

`ASP-I1-U-DAG+MAT-M0` is the typed, inline PostgreSQL comparator for qualified
`allShortestPaths` queries. It is intentionally distinct from the stored
helper implementation `ASP-A1-DAG` so benchmark arms and production receipts
identify the executable code path rather than only the algorithm family.

## Correctness and resource boundary

The emitter accepts one read-only, non-optional, directed endpoint pair with
static singleton endpoint IDs, minimum depth one, and a bounded maximum depth
from 1 through 64. It discovers minimum node distances, retains every
relationship-distinct predecessor at that minimum layer, and enumerates the
predecessor DAG into ordered relationship-ID arrays. Existing outer
translation performs path hydration.

The production emitter resolves exact one- and two-hop targets first. These
bounded preflight rows participate in the enumeration cap+1 gate, and recursive
distance discovery runs only when no early target exists.

Every recursive producer is consumed through a materialized cap+1 relation.
Separate immutable limits cover discovered states, predecessor rows, all
intermediate enumeration states, and serialized output bytes. The guarded
decision is complete before either public-output arm opens. A cap overflow
selects exact `ASP-A1-DAG` in the same statement and stable snapshot; candidate
rows cannot mix with fallback rows.

Materialized candidate and fallback markers provide singular plan evidence.
The runtime attestation receipt schema v2 records an ordered event chain. A
non-nested I1 execution records one of:

- `inline_predecessor_dag` with runtime identity `ASP-I1-U-DAG+MAT-M0`;
- `inline_no_path` with runtime identity `ASP-I1-U-DAG+MAT-M0`;
- `exact_a1_fallback` with runtime identity `ASP-A1-DAG`.

GraphBench replays distance, predecessor, enumeration, output, marker, and
branch-row counters. Qualification fails when attribution is absent,
contradictory, over cap, or shows rows from the inactive output arm.

## Production policy

The driver can select I1 only under Repeatable Read or Serializable isolation.
The verified schema-v2 promotion manifest must name the candidate and exact A1 fallback,
use `guarded_dual_arm`, declare all four positive caps, and authorize the exact
normalized-query SHA plus direction, all-path observation, depth,
relationship-kind count, and typed/untyped bucket. Every evidence report must
repeat that complete authorization identity. Query allowlisting and the
policy generation partition the translation cache. Read Committed, unmatched
queries, and the zero policy retain the incumbent. `DisableInlineASPDAG`
provides an evidence-free immediate rollback switch.

Tool forcing remains available for controlled comparison but does not broaden
the structural envelope. B1/B2 shortest and ASP experiments remain tool-only;
the production allowlist is centralized on the implemented inline families.

## Qualification sequence

1. Capture balanced A/A and A1-versus-I1 runs from a clean source tree.
2. Require exact full path-multiset parity on training, frozen holdout, and
   diagnostic cases, including inbound, disconnected, parallel-kind,
   early-target, diamond, cycle, and self-loop topologies.
3. Pass confirmation materiality/p95, selector-regret, resource,
   reference-closure, cancellation, concurrency, and session-isolation gates.
4. Generate a checksummed manifest for only the independently passing query
   and topology buckets, then canary at stable isolation.
5. Expand allowlisted buckets only with new clean evidence. Keep A1 automatic
   and retain the kill switch until post-canary production telemetry closes.

No result from a dirty diagnostic tree is promotion evidence, and this
implementation does not change the automatic `asp-static-v1` selector.
