# CySQL - Cypher to SQL Translation

**Version 1.0**

## Supported Query Components

Below are the currently supported openCypher query components translated by CySQL.

### Node matching

```
match (n) where n.name = 'my name' return n
```

### Inline label matchers

```
match (n:User) return n
```

### Inline pattern property matchers

```
match (n {prop: 'value'}) return n
```

### Relationship matching

```
match (:User)-[r]->() return r
```

### Recursive expansion

```
match (:User)-[r:MemberOf*..]->(:Group) return r
```

#### Ranges for Recursive Expansion

```
match (:User)-[r:MemberOf*2..]->(:Group) return r
```

```
match (:User)-[r:MemberOf*2..4]->(:Group) return r
```

### Pattern projections

```
match p = (:Computer)-[:HasSession]->(:User) return p
```

### Multiple Reading Clauses

```
match (u:User) where u.is_eligible
match p = (u)<-[:HasSession]-(c:Computer)
return p
```

### All shortest paths

```
match p = allShortestPaths((u:User)-[*..]->(:Domain)) where u.objectid = 'UUID-1234-567890' return p
```

### Order

```
match (n:User) where n.hasPassword return n order by n.name
```

### Skip and Limit

```
match (n:User) where n.hasPassword return n order by n.name skip 10 limit 100
```

### Entity Updates

#### Setting Properties and Labels

```
match (n:Base) where n.obviously_is_user set n.other = 1 set n:User return n
```

```
match ()-[r:HasSession]->(:User) set r.special_property = true
```

#### Removing Properties and Labels

```
match (n:User) remove n.name remove n:User return n
```

```
match ()-[r:HasSession]->(:User) remove r.special_property
```

### Entity Deletion

```
match (s:User) detach delete s
```

```
match ()-[r:MemberOf]->() delete r
```

## Supported Query Filters

### Comparison Expressions

The following operators are supported in authoring comparison expressions:

* `=`
* `<>`
* `<`
* `>`
* `<=`
* `>=`

When authoring comparison statements user must be aware of the typing requirements of CySQL compared to Cypher as
executed by Neo4j.

For more information see the `Differences between Cypher and CySQL` subsection `Stricter Typing Requirements`.

### Negation

Negation in query filters is supported with the `not` operator:

* `match (n:User) where not(n.eligible) return n`

### Conjunction and Disjunction

Conjunction `and` and disjunction `or` operators are both supported:

* `match (n:User) where n.eligible and n.enabled return n`
* `match (n:User) where n.eligible or n.seen_as_active return n`

### String Searching

Searching strings may be performed a variety of ways. These matches are case-sensitive and do not support wildcard
expansions.

#### String Prefix Matching

A string property may be filtered by prefix matching:

* `match (n:User) where n.name starts with 'my prefix' return n`

#### String Contains Matching

A string property may be filtered by contains matching:

* `match (n:User) where n.details contains 'something interesting' return n`

#### String Suffix Matching

A string property may be filtered by suffix matching:

* `match (n:User) where n.name ends with 'my suffix' return n`

### Regular Expressions

### Pattern Predicates

Query filters may also include pattern lookups. For example, searching for users with no active login sessions:

* `match (n:User) where not((n)<-[:HasSession]-(:Computer)) return n`

## Supported Subquery Expressions

### `collect`

A collect subquery expression can be used to create a list with the rows returned by a given subquery.

### `count`

Aggregates and counts the results of the given subquery as an integer.

## Supported Cypher Functions

### `duration` Function

Parses a valid duration string into a time duration that can be used in conjunction with other duration or date types.

```
match (s) where s.created_at = date() - duration('P1D') return s
```

### `id` Function

Returns the entity identifier of the node or relationship.

```
match (s) where id(s) in [1, 2, 3, 4] return s
```

### `localtime`

Returns the local time without timezone information.

```
match (s) where s.created_at <= localtime() return s
```

### `localdatetime`

Returns the local datetime without timezone information.

```
match (s) where s.created_at > localdatetime() return s
```

### `date`

Returns the current date with timezone information.

```
match (s) where s.created_at = date() return s
```

### `datetime`

Returns the current datetime with timezone information.

```
match (s) where s.created_at < datetime() return s
```

### `type`

Returns the type of the given relationship reference. This function returns the relationship's type as a text value.
Type checks utilizing this function will not be index accelerated and may exhibit poor performance.

```
match ()-[r]->() where type(r) = 'EdgeKind1' return r
```

### `split`

Takes a given expression and text delimiter and returns a text array containing split components, if any. If the
given expression does not evaluate to a text value this function will raise an error.

```
match (u:User) where '255' in split(n.ip_addr, '.') reutrn n
```

### `tolower`

Returns the localized lower-case variant of a given expression. If the given expression does not evaluate to a text
value this function will raise an error.

```
match (u:User) return tolower(n.name)
```

### `toupper`

Returns the localized upper-case variant of a given expression. If the given expression does not evaluate to a text
value this function will raise an error.

```
match (u:User) return toupper(n.name)
```

### `tostring`

Returns the text value of a given expression. If the given expression represents a type that can not be converted to
text this function will raise an error.

```
match (u:User) return tostring(n.num_active_logins)
```

### `toint`

Returns the integer value of a given expression. If the given expression represents a type that can not be converted or
parsed to an integer this function will raise an error.

```
match (u:User) return toint(n.integer_in_text_property)
```

### `coalesce`

Returns the first non-null value in a list of expressions. This is critically useful for navigating differences in
`null` bhavior between Cypher and CySQL.

```
match (n:NodeKind1) where n.target = coalesce(n.a, n.b, 'last_resort') return n
```

### `size`

Returns the number of items in an expression that evaluates to any array type.

```
match (n:NodeKind1) where size(n.array_value) > 0 return n
```

#### Caveats

The `size` function is expected to behave differently if the given expression evaluates to a text value. In this case,
the function returns the number of Unicode characters present in the text value. This behavior is currently not
supported in CySQL translation.

## Known Defects in Supported Components

The below issues are known defects. They are classified as defects of CySQL as the intent is to correctly support their
use.

### Entity Creation

The `create` reserved Cypher keyword is currently unsupported. Future support of it is planned.

### `labels` Function

Returns the labels of the given node reference. This function returns the node's labels as a text array value. Label
checks utilizing this function will not be index accelerated and may exhibit poor performance.

```
match (n) where 'User' in labels(n) return n
```

#### Caveats

While currently implemented this function returns the smallint array of labels associated with a node when referenced in
CySQL. Future support to convert the smallint array of node labels into text values is planned.

### Pattern Lookup Functions

The pattern lookup functions `head`, `tail` and `last` are not currently supported These functions are typically used to
reference different parts of a matched pattern.

Support for them is planned for a future version of CySQL.

### Unpacking Arrays of Entities for Comparison

Arrays containing graph entities are not unpacked during comparisons:

```
match (n:User) where n.disabled
with collect(n) as disabled
match p = (:Computer)-[:HasSession]->(u:User)
where not u in disabled
return p limit 1
```

Queries that contain similar constructs will result in the following translation error:
`ERROR: column notation .id applied to type nodecomposite[], which is not a composite type (SQLSTATE 42809)`.

### Right-Hand Bound Node Lookups

Patterns that utilize a bound reference in the right-hand node pattern will not correctly author the required SQL joins:

```
match (e)
match p = ()-[]->(e)
return p
limit 1
```

Queries that contain similar constructs will result in the following translation error:
`ERROR: invalid reference to FROM-clause entry for table "s0" (SQLSTATE 42P01)`.

### Untyped Array References and Literals

Untyped array references, including empty arrays, fail to pass type inference checks in CySQL. Support for additional
type hinting and inference is required to better support these use-cases.

```
match (n:User) where n.auth_modes = [] return n
```

Queries that contain similar constructs will result in the following translation error:
`Error: array literal has no available type hints`.

### Quantifier Expressions

Quantifier Expressions are currently unsupported but planned.

## Unsupported Constructs

Below are constructs of the Cypher language that did not make the 1.0 definition of the CySQL specification. Future
efforts may be pursued to add support for these language features.

* XOR Operations
* Case Expressions
* List Comprehensions
* Pattern Comprehensions
* Existential Subqueries (e.g. exists)
* Merge Statements
* Unwind Expressions
* Pattern Predicates using Recursive Expansion

## Differences between Cypher and CySQL

Translating Cypher to SQL via CyCSQL comes with a few semantic differences that users should be aware of.

### Stricter Typing Requirements

SQL comparisons are stricter than comparisons executed in Neo4j. Some of these typing constraints are handled
automatically by CySQL, however, some type mismatches do make it down to the underlying SQL database.

Given the Cypher query: `match (n:User) where n.name = 123 return n limit 1;`

The translated SQL, when executed, results in the following error:
`Error: ERROR: invalid input syntax for type bigint: "MYUSER@DOMAIN.COM" (SQLSTATE 22P02)`

This indicates that there is a node with a value for `n.name` that is not parsable as an integer.

In the future, CySQL translation will cover most of the strict typing requirements automatically for users.

### Index Utilization

Indexing in CySQL does not require a label specifier to be utilized. If the node property `name` is indexed in CySQL,
both:

```
match (n:User) where n.name = '1234' return n
```

and

```
match (n) where n.name = '1234' return n
```

will use the `name` index regardless of node label.

### null Behavior

Behavior around `null` in SQL differs from how Neo4j executes Cypher. Certain expression operators in Neo4j's
implementation of Cypher will treat `null` differently than their SQL counterparts while some semantics are very
similar.

Ideally, entity properties should strive to remove `null` as a conditional case as much as possible. In cases where this
is not possible, users are advised to exercise the `coalesce(...)` function:

```
match (n:User) where coalesce(n.name, '') contains '123' return n limit 1;
```

#### Silent Query Failure

`null` can taint result sets and also further complicate future comparisons in the query:

```
match (n:User)
with n.name as n
where n = '123'
return 1
```

The reference `n` is being projected by the multipart `with` statement but this projection removes the resultset from
the original query, allowing for ambiguity to slip into future operations against `n.name` where some values of
`n.name` may be `null`.
