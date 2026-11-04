# Quoted kind remediation

Completed 2026-11-03; implementation recorded in commit `618c5601`.
This report retains the implementation contract, regression evidence, and
acceptance results from the completed quoted-kind remediation.

[Review index](README.md) · [Query semantics](../query-semantics.md)

## Contract and implementation

Cypher source tokens are decoded once into semantic kind names at the frontend
boundary. Node patterns, relationship patterns, label predicates, and SET/REMOVE
labels use the same extraction helper. The symbolic-name visitor still returns
source text to other consumers. Kind APIs, catalog lookup, caches, SQL compilation,
and unknown-kind handling retain their existing contracts.

The Cypher emitter escapes names in joined kind lists, direct `graph.Kind`
expressions, and `KindMatcher` predicates. Literal surrounding backticks remain
part of a raw name. Property-key lexical routines are reused without applying
property-key validation to kinds. Empty escaped kind tokens still parse; this
repair introduces no empty-name execution policy or backend capability.

## Regression evidence

- Codec and grammar round trips cover ordinary and mixed-case ASCII, reserved
  schema words, spaces, punctuation, leading digits, Unicode spelling, embedded,
  multiple and surrounding literal backticks, query-looking punctuation, and the
  existing empty-token parser behavior.
- All four frontend consumers have ordinary quoted and unusual-name controls;
  CREATE and parser-only MERGE coverage use the repaired pattern visitors.
- Formatter tests cover all three kind emission branches, direct ASTs, both
  literal-stripping modes, semantic parse/emit/parse equality, and stable output.
- Translator tests trace lookup and CREATE registration in both optimizer modes,
  compare bare and quoted SQL while delimiter-bearing decoys exist, and retain
  genuine unknown-kind tests.
- Fluent builders and both parameter-map and temporal Neo4j rewrites preserve raw
  kind identity and values.
- The shared `quoted_kind_shapes` corpus has 61 variants, including predicates,
  negation, alternatives, direction, expansion, shortest paths, optional nulls,
  unknown kinds, property/parameter/alias controls, mutations with and without
  RETURN, exact surviving fixture records, creation counts and independent reads.
  PostgreSQL executes it in both optimizer modes. Shared assertions have no
  backend-specific skips or alternative answers.
- PostgreSQL driver controls exercise repeated quoted reads, quoted/bare
  alternation, both text compilation entry points, absence without catalog
  insertion, registration becoming visible, and distinct literal-backtick names.

Final regression tests fail against the original parser/emitter source using a
Go source overlay; the ordinary controls remain passing. Evidence and full
command logs are in the workspace's
`.test-results/quoted-kinds-20261103/` directory. No credentials are included in
this document.

## Validation completed 2026-11-03

The quoted-kind acceptance contract passed. Broader BHE accuracy remains qualified
by the independent findings below. Validation results describe the recorded
2026-11-03 source and environment.

| Validation | Result |
| --- | --- |
| Focused frontend, codec, formatter, translator, builder and driver tests | Passed |
| Final regressions against original parser/emitter source overlay | Reproduced failures; ordinary controls passed |
| Formatting and `git diff --check` | Passed; implementation hashes unchanged during full validation |
| `make test_update`, repeated | Passed; all 50 corpus/SQL files unchanged on the final repeat |
| `make test` with race detection | Passed; overall coverage 67.6%, matching the saved pre-change report |
| PostgreSQL `make test_all` | Passed on isolated PostgreSQL 18.6 with `pg_stat_statements` preloaded and installed |
| Neo4j `make test_all` | Passed on the supplied Neo4j 4.4.44 Community server |
| Enforced BDD scenarios | 17 passed on each backend |
| Existing report-only BDD scenarios | 18 failures retained on each backend; these do not establish conformance |
| Fresh SQL-versus-DAWGS checks on the BHE database | 88/88 passed, including all original 81 and seven added predicate/alternative controls |
| BHE production analysis | Three successful cycles; all six pipeline stages succeeded in each cycle; zero logged errors |

The shared 61-variant corpus runs through the standard runner on both backends and
through the explicit PostgreSQL optimizer-enabled/disabled wrapper. Other-backend
tests retain their existing scheme guards. Assertions and coverage were not
weakened. The added SQL goldens use the actual known kind IDs, including distinct
literal-backtick names, rather than accepting false predicates for known kinds.

The original quoted `Base` and `AZContains` checks now agree with fresh SQL and
bare-name controls: 931 nodes and 24,004 relationships respectively. Read-only
unknown-kind probes left zero catalog rows for their absent name.

### Production analysis and timing

The runner uses the existing root configuration and local `bhe/go.work` modules.
DAWGS schema initialization ran before the accuracy probes and timed cycles, as
normal BHE startup does. Feature flags, multi-tier analysis (enabled), and the
2 GiB graph-query memory limit were retained. Compilation, probes, snapshots,
and final oracles are outside analysis timing.

| Cycle | Recorded prior run | Remediated run | Change |
| --- | ---: | ---: | ---: |
| 1 | 170.617 s | 175.834 s | +3.1% |
| 2 | 125.044 s | 127.320 s | +1.8% |
| 3 | 153.353 s | 170.568 s | +11.2% |
| Median | 153.353 s | 170.568 s | +11.2% |

Mean elapsed time increased 5.5%. The observed ranges substantially overlap,
but the higher median is retained as a performance qualification. Investigation
localized extra time mainly to tier analysis: its per-cycle totals increased
by 16.3, 3.7 and 16.5 seconds. Azure post-processing decreased by 10.0 and 2.2
seconds in the first two cycles and increased by 9.8 seconds in the third.
All stage measurements are archived. Post-run maintenance statistics were also
captured; they show metadata-table maintenance around the measured interval but
do not establish a cause. Source inspection and identical semantic outputs do
not establish performance causality. No numerical regression gate was specified,
and these three operational measurements do not prove performance equivalence.
They are not cold-cache or previous-engine benchmarks. Sampled runner high-water
memory was 344.3 MiB; PostgreSQL memory is excluded.

### Semantic results and independent findings

All three snapshots match the previous final snapshot for semantic main nodes
and relationships, graph cardinalities, findings, prioritization, data quality,
risk counts and configuration. Main graph: 42,420 nodes / 5,958,127 stored
relationships; meta graph: 1,743 nodes / 522 relationships.

Only raw relationship fingerprints change between snapshots. Semantic fingerprint
queries remove exactly `firstseen`, `lastseen` and `lastcollected` properties.
Generated meta IDs are excluded only from meta edge fingerprint comparison;
cardinalities remain compared. The latest-risk query selects the newest record
per environment/tier/finding within the hour ending at the latest risk record.
No broader timestamp or data normalization was applied.

Fresh source-permission SQL oracles confirm all 1,468,572 relationships across
five Azure derived types, with zero missing or unexpected pairs. Independent
findings remain visible:

- The BHE `AZExecuteCommand` tenant-loop early return still omits 1,320 mandatory
  relationships; eight loaded tenants have no devices. No BHE source was changed.
- Direct SQL confirms 130 missing selector seeds and 121 missing principals.
  Each cycle retains 151 missing-seed warnings and 121 warnings in each of the
  two principal-warning categories.
- Six pre-existing relationships have both endpoints zero. Pattern-matching
  comparisons join valid endpoints: 5,958,121 relationships. There is no increase
  in orphan storage and no missing kind reference.

Passing quoted-kind acceptance does not establish full BHE accuracy sign-off;
those independent BHE/data findings still require separate remediation.

### Connection qualification and evidence

The supplied PostgreSQL `bhe` role connects to the requested database but lacks
`SELECT` on its public graph tables. BHE analysis, the 88 accuracy checks, and
final read-only SQL oracles use the existing production configuration required
by the plan: the `postgres` role on the same local `bhe` database, port 5432.
The failed direct-role oracle attempt is preserved. No grants, ownership or
credentials were changed. Destructive PostgreSQL suites used a separate server
on port 55433, which was stopped after testing. The supplied Neo4j server was
used for its destructive full suite with the user's disposable-data authorization.

Source revisions (plus this working-tree change): DAWGS `579ab32522803d793de51294caf0235cb902270c`,
BHE `3ce6e7b4f0d9610a0dabc12e96c6045891fbe8fd`, and BHCE
`f2adef540eb9c21df0c375aded8dc38802e00543`.
BHE/BHCE working-tree status is unchanged by this task. Module build information,
source hashes, exact commands, failed-before/final tests, regeneration hashes,
full backend logs, separate BDD JSON/HTML reports, 88 probes, cycle logs,
resources, snapshots and SQL oracles are archived in
`.test-results/quoted-kinds-20261103/` at the workspace root. See the
[structured validation record](../../../.test-results/quoted-kinds-20261103/validation.json).

## Boundaries

There is no schema migration, historical kind cleanup, catalog normalization,
optimizer change, or new PostgreSQL clause support. Variable and parameter
identifier normalization remains separate O09 work. The BHE Azure
`AZExecuteCommand` tenant-loop early return and existing missing selector/principal
references are independent findings and remain outside this change.
