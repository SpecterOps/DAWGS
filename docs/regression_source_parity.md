# BloodHound Regression Source Parity

This workflow keeps the stable query-form manifest synchronized with reviewed
BloodHound Enterprise (BHE) and BloodHound Community Edition (BHCE) source
snapshots. It records query shapes only; DAWGS must not import application
business logic or reproduce complete BloodHound traversal behavior.

## Dormant tier

`FUTURE-01` is the outbound tenant reconciliation form:

```cypher
MATCH (s:AZEntity)-[r:K]->()
WHERE s.tenantid IN $tenant_ids
DELETE r
```

At BHE commit `c9f61530f45b`, its callers in
`lib/go/daemons/datapipe/ingest.go` are inside the block labeled "Disabled for
now". The compiled `ReconcileOutboundKindsForTenants` helper does not by itself
make the form production-active.

Keep `FUTURE-01` in the dormant section of
`regression_coverage_manifest.md`. Do not add it to
`integration/testdata/cases`, `integration/testdata/templates`, or
`benchmark/testdata/scale/cases` while the caller remains disabled. Unit gates
in `cmd/plancorpus` and `cmd/graphbench` reject every `FUTURE-*` ID from those
active corpora.

When a reviewed source snapshot enables the caller:

1. Record the enabling entry point and source commit before changing the tier.
2. Move the manifest row from dormant to active and update the corpus gates in
   the same change.
3. Add the exact outbound builder composition and the `PG`, `IT`, `PC`, and
   `SC` layers required by `regression_plan.md`.
4. Cover empty, single-item, 1,000-item, boundary, and stress tenant lists;
   include direction, kind, tenant, endpoint, and missing/null decoys.
5. Use exact mutation post-state and rollback/reset isolation. Reuse the
   `REC-04` matrix, but do not reuse its inbound query as proof of outbound
   orientation.
6. Capture the PostgreSQL plan/runtime baseline with the same source metadata.

## Audit procedure

Set source roots to reviewed, immutable checkouts. These sources are audit
inputs and are not copied into DAWGS:

```bash
export BHE_ROOT=/path/to/bhe
export BHCE_ROOT=/path/to/bhce
git -C "$BHE_ROOT" rev-parse HEAD
git -C "$BHCE_ROOT" rev-parse HEAD
git rev-parse HEAD
```

Start with a broad call-site inventory. This intentionally includes helpers and
commented code; activity is classified during the trace step:

```bash
rg -n --glob '*.go' \
  '\b(Filterf?|Query|First|Count|Fetch[A-Za-z0-9_]*|Create[A-Za-z0-9_]*|Delete[A-Za-z0-9_]*|Update[A-Za-z0-9_]*|BatchOperation)\b' \
  "$BHE_ROOT" "$BHCE_ROOT"
```

For each candidate:

1. Trace the helper to an active reconciliation, post-processing, or changelog
   entry point. Label helper-only, test-only, and commented-out forms.
2. Normalize active forms by anchor, pattern, direction, relationship kinds,
   predicates, projection, cardinality, mutation target, and execution path.
3. Map the tuple to an existing stable ID or add a new manifest row and source
   link. A new operator, grouping, direction, anchor, projection, or mutation
   target requires a distinct ID.
4. Treat stepwise traversal evidence as standalone `HOP-*` shapes only. Never
   add a test that sequences the application traversal.
5. Recheck projection independently from predicates, and recheck relationship
   kind-list and ID-list cardinalities after schema-set changes.
6. Apply the required coverage layers from `regression_plan.md`, then run both
   backend suites and refresh PostgreSQL plan/scale captures when applicable.

## Audit record template

Append one record per reviewed source update:

```markdown
### YYYY-MM-DD source parity audit

- BHE commit: `<commit>`
- BHCE commit: `<commit>`
- DAWGS commit/worktree: `<commit and worktree state>`
- Active entry points reviewed: `<paths>`
- Existing IDs confirmed: `<IDs>`
- IDs added or changed: `<IDs or none>`
- Dormant/helper-only forms: `<IDs and source evidence>`
- Projection/cardinality changes: `<details or none>`
- Validation and captures: `<commands and artifact paths>`
```

## Seed audit record

### 2026-08-04 source parity audit

- BHE commit: `c9f61530f45b`
- BHCE commit: `74dd3daa58a8`
- DAWGS baseline: `v0.6.0-13-g6638cc2`; implementation worktree based on
  `8c5fba7` with the Phase 8 parity-gate changes
- Active IDs: the `LOGIC-*`, `REC-*`, `TRUST-*`, `PRUNE-*`, `HOP-*`,
  `SCAN-*`, `LOOKUP-*`, and `WRITE-*` rows in
  `regression_coverage_manifest.md`
- Dormant forms: `FUTURE-01`; both reviewed callers remain in the disabled
  Azure reconciliation block
- Validation: PostgreSQL and Neo4j `make test_all`; Phase 7 PostgreSQL plan and
  scale captures under `.coverage/`
