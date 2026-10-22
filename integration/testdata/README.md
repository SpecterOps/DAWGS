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

Use `ordered_column_values` to check scalar values alongside returned entities,
for example `{"ordered_column_values": {"total": [2, 1]}}`. Combine it with
`ordered_node_ids` and `keys` to verify ranked node/count pairs and aliases.

`bound_expansion_endpoint_shapes.json` checks expansions that reuse a bound
terminal: false and genuine cycles, zero hops, repeated bindings through
`WITH`, per-row endpoint correlation, and mutation selection. Assertions check
complete returned node records and ordered path node IDs. Direction coverage
includes outbound/inbound expansions and undirected fixed prefixes; fully
undirected variable expansions are currently unsupported by the translator.

`node_representation_shapes.json` checks carried node IDs, aliases, inline node
and relationship map expressions, and exact mutation selection.
`leading_multipart_shapes.json` checks leading/attached UNWIND and CREATE across
WITH, including empty inputs, properties, relationship creation, and post-state.
`correlated_suffix_shapes.json` checks carried suffix predicates on boundaries,
nodes, and relationships, with ordered path nodes and duplicate row multiplicity.
PostgreSQL tests also execute these corpora with each compiler mode explicitly.

`bound_suffix_boundary_shapes.json` checks suffixes whose expansion boundary is
already bound, including root reuse, aliases, cycles, zero hops, and mutation
selection. `empty_list_alias_shapes.json` checks empty lists materialized through
WITH and their zero-row mutation effects. Inline-map function references are
covered in `node_representation_shapes.json`.

`multipart_write_visibility_shapes.json` keeps CREATE followed by MATCH, SET,
or DELETE in the same primary Cypher query. Post-assertions additionally check
exact created values/counts and unchanged decoys. Separate post-assertion queries
alone cannot establish visibility between clauses inside the primary query.
It also checks constant projection cardinality, entity aliases versus captured
scalar values, and filters after ordering/pagination but before later mutations.

`update_row_stream_shapes.json` checks fan-out, repeated mutation targets,
duplicate input values, nullable node and relationship targets, per-row assignment
expressions, aliases, and empty inputs. It asserts the complete node and relationship
post-state as well as returned row multiplicity. `update_row_stream_inline.json`
checks continuation through a later WITH mutation. PostgreSQL also executes the
template variants with each compiler mode explicitly.

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

Large string-list parameters use the same tagged parameter decoder without a
large handwritten JSON array:

```json
{
  "params": {
    "object_ids": {
      "$type": "string_list",
      "prefix": "missing",
      "count": 1000,
      "include": ["target-id"]
    }
  }
}
```

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
