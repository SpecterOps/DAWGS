---
bh-rfc: unlisted
title: "dawgrun: Playground for DAWGS"
authors: |
    [Sean Johnson](sjohnson@specterops.io)
status: DRAFT
created: 2026-01-29
audiences: |
    DAWGS maintainers and contributors
    Engineers in adjacent repositories who author or debug DAWGS-backed queries
---

# dawgrun: Playground for DAWGS

## 1. Overview

`dawgrun` is a developer tool for exploring, debugging, and validating DAWGS behavior from a single interactive surface.

Today, `dawgrun` operates as a REPL focused on PostgreSQL-backed DAWGS workflows. It supports opening DAWGS-compatible database connections, inspecting kind mappings, parsing CySQL/Cypher queries, translating those queries to PostgreSQL, running translated query plans with `EXPLAIN`, and executing Cypher directly.

This RFC formalizes the current state of the tool and proposes an incremental roadmap that keeps `dawgrun` intentionally broad in mission: a long-lived playground for DAWGS debugging and experimentation.

## 2. Motivation & Goals

DAWGS development currently depends on a mix of tests, ad hoc scripts, and one-off debugging approaches. Those methods are useful but fragmented, and they do not provide a single place to inspect parser output, translation behavior, backend kind mapping, and live query execution together.

`dawgrun` is meant to close that gap by providing a practical, team-owned workflow tool that can be used while authoring features, investigating regressions, and workshopping query behavior across repositories that depend on DAWGS.

- **Unify Debugging Workflows** - Provide one tool that centralizes query parsing, translation inspection, kind resolution, and execution against live backends.
- **Shorten Iteration Loops** - Reduce friction between changing DAWGS code and validating behavior against real-world query/data scenarios.
- **Improve Translation Visibility** - Make AST and generated SQL output easy to inspect, compare, and discuss during development.
- **Support Real Backends** - Enable debugging against actual DAWGS-compatible data sources rather than only synthetic unit-test fixtures.
- **Remain Extensible** - Preserve room for deeper introspection capabilities as DAWGS evolves.

## 3. Considerations

### 3.1 Impact on Existing Systems

`dawgrun` is an additive developer tool and does not change runtime behavior of DAWGS libraries in production paths.

The tool integrates at DAWGS API boundaries and should continue to do so. New features SHOULD prefer composition over invasive changes to core DAWGS packages unless a clear library-level improvement is justified independently.

### 3.2 Security & Compliance

`dawgrun` targets engineering and test workflows, but it still operates on live data connections. Usage SHOULD assume standard safeguards for credentials, access controls, and environment boundaries.

At minimum:

- Connection strings MUST be handled as sensitive values.
- Query output MAY include sensitive graph attributes and SHOULD be used accordingly.
- Features that increase automation or scripting SHOULD avoid encouraging unsafe credential handling patterns.

### 3.3 Drawbacks & Alternatives

`dawgrun` currently uses a modified fork of `github.com/openengineer/go-repl` and carries that maintenance burden in-tree. This provides needed control, but it also means REPL behavior is partly owned by this project.

Primary trade-offs:

- A REPL-first interface is efficient for iterative debugging, but less direct for batch automation.
- A broad mission can drift into unstructured feature sprawl without explicit scope guardrails.
- Maintaining tooling inside this repository creates ongoing upkeep cost.

Alternatives include continuing with ad hoc scripts or embedding isolated diagnostics in each dependent repository. Those options lower central tool complexity, but they duplicate effort and weaken shared debugging conventions.

### 3.4 Audience

The primary audience is engineers actively developing DAWGS.

Secondary audiences include engineers in BHCE/BHE and other adjacent codebases who need to workshop or debug DAWGS-backed query behavior before committing changes to application-level implementations.

### 3.5 Adoption Model

`dawgrun` is an as-needed engineering tool, not a formal user-facing product feature.

Adoption is intentionally lightweight:

- No coordinated organization-wide rollout is required.
- Capabilities are expected to evolve incrementally as development needs arise.
- Changes SHOULD still be documented clearly so workflows remain discoverable for current and future contributors.

## 4. Current State

This section documents the implemented baseline at the time of writing.

### 4.1 Runtime Model

`dawgrun` currently runs as an interactive REPL with command parsing, persistent session history, and command-name completion.

- Prompt: `dawgrun >`
- History file: `$XDG_CONFIG_HOME/dawgrun/history.txt` (or platform equivalent)
- Status widget: active connection count
- Completion: command-name matching with candidate popover

### 4.2 Implemented Command Surface

The current command set includes:

- `copy-opengraph` - copy a full graph from one named connection to another
- `open` - open a named DAWGS-compatible connection (PostgreSQL or Neo4j)
- `list-connections` - list currently open named backend connections
- `save-opengraph` - export a connection's full graph as OpenGraph JSON
- `load-opengraph` - load OpenGraph JSON into a named connection
- `load-db-kinds` - refresh and print kind mappings for a connection
- `lookup-kind` / `lookup-kind-id` - resolve kind names and IDs
- `parse` - parse CySQL/Cypher into AST and dump highlighted output
- `translate-psql` - translate CySQL/Cypher to PostgreSQL SQL
- `explain-psql` - translate, prepend `EXPLAIN`, and execute against PostgreSQL
- `query-cypher` - execute query over active connection with table/JSON output
- `runtime-trace` - start/stop Go runtime tracing to a file
- `help`, `exit`, `quit`

### 4.3 Query and Translation Workflows

Current query workflows support:

- Parsing query input into Cypher AST structures for inspection.
- Translating Cypher into PostgreSQL statements.
- Optional dump of translator SQL AST (`translate-psql -dump-pg-ast`).
- Optional kind-aware translation against a named connection (`translate-psql -conn <name>`).
- `EXPLAIN` execution against translated SQL for planner visibility.

### 4.4 Data and Kind Inspection

Named connections and lazily loaded kind maps provide the basis for cross-command introspection.

- Kind maps are fetched from the live backend and cached in command scope.
- Name/ID lookup helpers support debugging mismatches in query translation and execution.
- Backend connection opening is driver-agnostic via connection-string scheme detection.

### 4.5 Known Constraints

- Kind ID lookup commands are PostgreSQL-specific because they depend on numeric kind IDs.
- REPL input uses shell-style tokenization, which affects quoting behavior for Cypher string literals.
- The tool is interactive-first; scriptability exists only in limited form through command composition and shell invocation patterns.

## 5. Details of the Proposal

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in [RFC 2119](https://www.ietf.org/rfc/rfc2119.txt).

This proposal keeps the broad long-term goal intact while defining concrete near-term improvements.

### 5.1 Data Inspection

`dawgrun` MUST continue to support interactive inspection against live graph data and kind metadata.

Requirements:

- The tool MUST preserve straightforward connection management for at least PostgreSQL.
- The tool SHOULD preserve backend-agnostic `open` behavior for additional DAWGS drivers.
- Kind inspection SHOULD remain available as first-class commands and SHOULD support both name-to-ID and ID-to-name workflows.
- Refresh semantics for kind mappings MUST remain explicit and predictable.

Implementation direction:

- Evolve command surface from backend-specific naming toward backend-agnostic patterns where practical.
- Keep connection-opening semantics explicit and discoverable through command help.

### 5.2 Query Tooling

`dawgrun` MUST provide robust tools for understanding query lifecycle transformations.

Requirements:

- Users MUST be able to parse and inspect CySQL/Cypher AST output.
- Users MUST be able to translate CySQL/Cypher into PostgreSQL queries with and without live kind mapping.
- Users SHOULD be able to inspect both final SQL and intermediate translator AST artifacts.
- Users SHOULD be able to validate planner behavior through `EXPLAIN` on translated output.
- Query execution output MUST remain consumable both by humans (table mode) and tooling (JSON mode).

Implementation direction:

- Preserve current `parse`, `translate-psql`, `explain-psql`, and `query-cypher` workflows as the stable baseline.
- Add workflow shortcuts only when they map to repeatable debugging use-cases and do not obscure underlying behavior.

### 5.3 Developer Tooling

`dawgrun` SHOULD continue evolving into a generalized DAWGS debugging workbench.

Target capabilities:

- Support manually stepping query translation paths for deeper debugging visibility.
- Support interactive construction and execution of DAWGS graph-query components.
- Support generation helpers for translation test-case authoring.
- Improve automation friendliness for repeated local workflows (for example, repeatable startup state and command scripts).

Guardrails for expansion:

- New capabilities MUST correspond to concrete DAWGS engineering workflows.
- REPL ergonomics and clarity SHOULD be favored over highly specialized one-off behavior.
- Additions SHOULD document whether they are implemented, experimental, or roadmap-only.

## 6. Scope Boundaries

`dawgrun` is intentionally broad in mission, but not unbounded in purpose.

In scope:

- Any developer-facing capability that materially improves DAWGS debugging, query workshopping, or translation validation.
- Features that help engineers observe behavior across parser, translator, kind mapping, and backend execution layers.

Out of scope:

- End-user product UX concerns unrelated to DAWGS engineering workflows.
- Replacing formal test suites, CI validation, or benchmark infrastructure.
- Adding features that do not map to recurring debugging or development needs.

## 7. Success Criteria

This RFC is successful when `dawgrun` is treated as the default developer entry point for interactive DAWGS debugging by its primary audience.

Practical indicators include:

- Engineers can move from query idea to parser/translator/execute feedback in a single session.
- Kind mapping and translation issues can be reproduced and inspected without custom one-off scripts.
- New DAWGS contributors can discover and use common debugging workflows through command help and documentation.
- Expansion work remains coherent with the tool's mission rather than fragmenting into unrelated utilities.

## 8. Open Questions

- Should `open` support additional secure Neo4j URI schemes beyond `neo4j://` by default?
- Which automation model is most valuable first: startup config profiles, scripted command batches, or both?
- What level of translation-step introspection is useful by default versus too noisy for routine workflows?
- Should roadmap features be tagged by maturity level directly in help output to clarify supported versus experimental behavior?
