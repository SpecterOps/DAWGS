# BloodHound PgSQL Model

This package contains a syntax model for the PostgreSQL SQL dialect. It also contains a translation implementation to
take openCypher input and output valid PostgreSQL SQL. This model is not intended to be a complete implementation of all
available SQL dialect features but rather the subset of the dialect required to perform openCypher to PostgreSQL
translation.

**Expected PostgreSQL SQL dialect version**: `18+`

## Formatting

The `format` package contains the string rendering logic for the PgSQL syntax model.

## Translation

The `translate` package contains the openCypher translation implementation.

## Optimization

The `optimize` package analyzes Cypher query shape before PostgreSQL SQL emission. See
[PostgreSQL Translation](../../../docs/postgresql_translation.md) for optimizer coverage, indexing notes, and
validation workflow.

## Visualization

The `visualization` package contains a PUML digraph formatter for the PgSQL syntax model.

## Test Cases

The `test` package contains the test cases used to validate translation.

`Merge` is a statement and a set expression, so it can be used as a CTE body. It supports returning projections,
`MergeDoNothing`, and optional `SourceQuery` while retaining table sources for existing callers. `FunctionMergeAction`
represents PostgreSQL's `merge_action()`. Empty SQL windows support pipeline row numbering.
