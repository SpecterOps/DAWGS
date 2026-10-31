# Singleton shortest-path tie policy

Date: 2026-08-07

`shortestPath` promises one valid relationship-unique trail of minimum length.
It does not promise which equally short trail is selected, and PostgreSQL
physical relationship IDs or insertion order are not part of the public
contract. Callers that require every relationship-distinct minimum trail must
use `allShortestPaths`.

An executor may use a deterministic internal tie breaker for repeatability,
but changing that internal choice is not a semantic change when the returned
trail remains valid and minimal. PostgreSQL/Neo4j compatibility fixtures
therefore compare logical node identities, relationship kinds, and stable
`logical_key` properties. They do not require both backends to select the same
physical relationship ID for singleton output.

This policy permits a future singleton witness executor to retain one
predecessor per accepted node/depth state. It does not permit deduplication for
`allShortestPaths`, relationship/path predicates, relationship variables, or
other forms whose validity or output multiplicity depends on the complete
trail. Those forms retain their exact incumbent unless independently
qualified.
