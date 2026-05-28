# Cypher Walker Semantics

Cypher has two traversal needs that should stay separate:

- `walk.Cypher` is the semantic walker used by translation and optimizer code. It walks expression-bearing children that participate in translation order, and intentionally skips declaration-only fields such as projection aliases, pattern variables, kind metadata, and quantifier binding variables where those fields are handled by parent nodes or clause-specific logic. Bare `cypher.MapLiteral` values are semantic leaves; only `*cypher.Properties` exposes map item/value children in semantic traversal.
- `walk.CypherStructural` is the structural walker for AST inspection. It should visit all modeled child nodes, including declarations, aliases, pattern metadata, relationship ranges, and map/list contents.

When adding a Cypher AST element, update both walker modes deliberately:

- Add semantic traversal only for fields that should affect translator/optimizer expression stack behavior.
- Add structural traversal for every modeled child field.
- Add tests that assert actual visited children, not only that cursor construction succeeds.

Nil handling is part of the contract. Nil traversal roots and nil branches should surface cursor negotiation errors, not successful no-op walks. Optional nil pointer children should be skipped by the cursor constructor that owns the optional field, while valid empty syntax nodes such as empty map literals, empty list literals, empty kind lists, and empty identifiers should still be visitable when they are the traversal root.

Visitor cancellation is immediate. `SetDone`, `SetError`, and `SetErrorf` stop traversal after the current callback returns; the walker does not unwind pending `Exit` callbacks for nodes still on the traversal stack. Visitors that need balanced enter/exit state should use `Consume` for subtree pruning and reserve cancellation/error APIs for terminal traversal.
