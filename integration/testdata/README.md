# Integration Corpus

Files under `cases/` execute one Cypher query per case. Files under `templates/`
share a fixture and query template across variants. Fixture-backed cases run in
a write transaction that is always rolled back.

Mutation cases use `assert: "no_error"` (or another primary result assertion)
and one or more `post_assertions`. The primary mutation result is fully drained
and checked before post-state queries run in the same transaction. Each
post-state entry contains `cypher`, optional `params`, and `assert`.

The assertion vocabulary includes exact fixture-backed state checks:

- `node_id_set` for exact surviving node IDs;
- `node_records` for exact node IDs, kinds, and complete property maps;
- `relationship_triples` for exact directed start/end/kind triples;
- `relationship_records` for exact triples and complete property maps;
- `exact_int` and `row_count` for counts.

Every new reconciliation or post-processing mutation fixture must contain a
positive match and applicable decoys for direction, kind, property, fixture ID,
missing/null property state, and relationship property. Reuse
`NewReconciliationFixture`, `FixtureNames`, and `FixtureKinds` from the
`integration` package for deterministic Go integration cases and large
cardinality lists.

Tagged datetime parameters decode to `time.Time`:

```json
{
  "params": {
    "threshold": {
      "$type": "datetime",
      "value": "2026-01-02T03:04:05Z"
    }
  }
}
```

Raw Cypher may instead use an explicit conversion such as
`datetime($threshold)`. Legacy query-builder cases must pass `time.Time`
directly.

Fixture-backed cases and template variants can bind database IDs without
hard-coding them. `node_params` maps a query parameter to one fixture node ID;
`node_list_params` maps a parameter to an ordered list of fixture node IDs:

```json
{
  "node_params": {"start_id": "start"},
  "node_list_params": {"end_ids": ["end-a", "end-b"]}
}
```

The integration runner and `cmd/plancorpus` resolve these fields after loading
the fixture, so semantic execution and plan capture use the same ID-anchored
query shape.
