# Cypher Walker Semantics

Cypher has two traversal needs that should stay separate:

- `walk.Cypher` is the semantic walker used by translation and optimizer code. It walks expression-bearing children that participate in translation order, and intentionally skips declaration-only fields such as projection aliases, pattern variables, kind metadata, and quantifier binding variables where those fields are handled by parent nodes or clause-specific logic.
- `walk.CypherStructural` is the structural walker for AST inspection. It should visit all modeled child nodes, including declarations, aliases, pattern metadata, relationship ranges, and map/list contents.

When adding a Cypher AST element, update both walker modes deliberately:

- Add semantic traversal only for fields that should affect translator/optimizer expression stack behavior.
- Add structural traversal for every modeled child field.
- Add tests that assert actual visited children, not only that cursor construction succeeds.

Nil handling is part of the contract. Optional nil pointer children should be skipped without panics, but valid empty syntax nodes such as empty map literals, empty list literals, empty kind lists, and empty identifiers should still be visitable when they are the traversal root.
