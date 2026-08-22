# ASP N1 negative-exhaustion disposition v1

`ASP-N1-NEGATIVE-EXHAUSTION` is a default-off, bounded target-side reachability
preflight for the existing A1 all-shortest-path executor. It can return an
empty result only after one of two exact proofs:

- no eligible relationship enters the target in the logical traversal
  direction; or
- reverse breadth-first discovery exhausts before the configured maximum depth.

When the source is reached, or the reverse state sentinel is exceeded, N1
discards its temporary state and invokes `ASP-A1-DAG`. It never produces a
positive path itself, so complete predecessor-DAG enumeration and path
hydration remain A1 responsibilities.

The local `traversal_shapes` smoke run used one PostgreSQL V2 connection, two
warmups, and ten samples. The disconnected query measured 1.2 ms median and
1.7 ms p95, compared with the most recent Neo4j reference of 1.34 ms and
1.90 ms. A globally forced N1 executor regressed the reachable diamond because
the target-side probe is inconclusive there and must call A1 afterwards.

N1 therefore remains tool-only. A future production selector must establish
the degree-zero condition before choosing N1; it must not use query shape or a
global topology synopsis as a proxy for a negative proof. The candidate's
fallback states are recorded as `asp_n1_target_degree_zero`,
`asp_n1_reverse_exhausted`, `asp_n1_source_reached_a1`, and
`asp_n1_state_cap_a1` for that qualification work.
