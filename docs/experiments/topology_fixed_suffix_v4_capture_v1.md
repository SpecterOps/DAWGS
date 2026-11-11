# Topology fixed-suffix v4 capture procedure

Status: executable qualification procedure; no promotion manifest is granted by this document.

This procedure produces the six evidence roles required to authorize the
default-off `topology-fixed-suffix-v1` candidate. It must begin from a clean,
committed source tree. An untracked file, generated benchmark output, or a
locally edited manifest makes the capture diagnostic-only.

## Preconditions

- Use a disposable PostgreSQL target and the repository destructive-test
  guard variables.
- Build one `-trimpath` GraphBench binary into the capture directory and use
  that exact binary for every arm.
- Freeze the selected training declarations before running any holdout work.
- Use pool size one, one run UUID, Repeatable Read, and a fresh synopsis for
  every fixture load.
- Keep the candidate disabled until `go run ./cmd/graphbench
  -promotion-manifest <manifest>` verifies all six bound reports.

The v4 route is intentionally observable only after the incumbent has run for
the same query and parameter values inside one active stable-snapshot
transaction. Every selected candidate record must therefore include an
incumbent-first sample and a same-transaction candidate-hit sample.

## Required matrix

Capture both endpoint-ID and complete-path fixed-suffix observations for:

1. sparse reachable targets;
2. no-path and disconnected controls;
3. high reverse fan-in and dense-suffix controls;
4. output-row and output-byte overflow;
5. missing, stale, incompatible, and refreshed synopses;
6. mutation, savepoint rollback, cancellation, and pool-reuse boundaries.

Every candidate record must have exact public observations, a complete
candidate/fallback receipt chain, no leaked candidate output on overflow, and
attributable plan/resource telemetry. Every selector failure remains an
incumbent record with its typed reason.

## Capture order

1. Capture an order-balanced PostgreSQL A/A artifact.
2. Capture incumbent and v4 candidate training rounds with the frozen corpus.
3. Produce the training discovery report and freeze it before opening holdout.
4. Capture the sealed holdout rounds without changing the binary, host,
   corpus, schema, policy caps, estimator, or synopsis version.
5. Produce performance, resource, reference-closure, and operational reports
   from the exact bound artifacts.
6. Assemble the manifest only after every report passes, bind each report to
   that manifest, and run the independent manifest verifier.

The relevant GraphBench reports are the standard `-aa-output`, performance
gate, `-resource-output`, `-reference-closure-output`, and operational gate
outputs. The manifest verifier requires exactly these roles: `aa`,
`confirmation`, `performance`, `resource`, `reference_closure`, and
`operational`.

## Admission and disposition

The candidate must improve p50 by at least 5 percent or 100 microseconds, keep
p95 at or below 1.05 times the incumbent, and keep selector overhead at or
below 1.10 times or 100 microseconds of the selected exact arm. Refresh cost,
WAL, storage, mutation amplification, and cache capacity are release inputs,
not optional diagnostics.

If any required record, receipt, chronology proof, or gate is missing or
fails, write a terminal rejection record for that evidence generation. Do not
retune the v4 estimator, thresholds, caps, or first-use behavior. A new design
requires a new selector version and a separately frozen procedure.
