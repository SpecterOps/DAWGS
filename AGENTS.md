# Agent Instructions

These instructions apply to the entire repository.

## Working Style

- Inspect the relevant code and tests before changing behavior.
- Implement straightforward requested changes directly after inspection.
- Propose a plan before non-trivial work. Non-trivial work includes schema changes, breaking interface changes to exported symbols likely used by downstream projects, and changes with potential performance impact.
- Keep edits scoped to the request and update nearby documentation when behavior or workflow changes.
- Never revert a commit. Do not run `git revert`, rewrite history, or discard user work.
- Author commits only when explicitly directed by the user.

## Build, Format, And Test

- Always format after code edits. Use `make format` unless a narrower formatting command is clearly sufficient for the touched files.
- `make test_all` is the default validation command. It runs unit tests and all integration suites.
- Integration suites consume `CONNECTION_STRING`. If `CONNECTION_STRING` is not present in the LLM context, ask the user to add it before running `make test_all`.
- Run `make test_all` only for the backend selected by the scheme in `CONNECTION_STRING`. Tests for other backends should skip themselves.
- Core integration cases in `integration/testdata/cases` and `integration/testdata/templates` must be backend-equivalent. Do not add driver-specific skips or driver-specific expected assertions to these suites. If a backend capability needs dedicated coverage, put it in a clearly driver-scoped test that is skipped unless `CONNECTION_STRING` selects that backend.
- `make test` is available for unit tests only.
- `make test_integration` is available for integration tests only.
- Agents are empowered to update dependencies as part of normal maintenance when the change warrants it.

Benign local examples:

```bash
export CONNECTION_STRING="postgresql://dawgs:weneedbetterpasswords@localhost:65432/dawgs"
export CONNECTION_STRING="neo4j://neo4j:weneedbetterpasswords@localhost:7687"
```

New database drivers must add a benign `CONNECTION_STRING` example here.

## Test Coverage

- Changes must be covered by tests.
- Mutation and template test coverage is required for behavior that touches Cypher parsing, translation, query rendering, or integration query semantics.
- Do not lower overall test coverage during edits. If coverage drops, add tests before finalizing.
- For generated or fixture-backed test cases, update the source cases and generated artifacts according to the existing repository workflow.

## Documentation

- Keep `README.md` in lock-step with test, build, setup, and user-facing workflow changes.
- Prefer updating documentation aggressively when behavior, commands, environment variables, or supported drivers change.
