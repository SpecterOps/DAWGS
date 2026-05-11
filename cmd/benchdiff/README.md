# Benchdiff

Compares the existing benchmark suites between two committed git refs without changing the active worktree.

## Usage

```bash
# Unit Go benchmarks only
go run ./cmd/benchdiff -base main -target HEAD -kind unit

# Unit and integration benchmarks
export CONNECTION_STRING="postgresql://dawgs:weneedbetterpasswords@localhost:65432/dawgs"
go run ./cmd/benchdiff -base main -target HEAD -kind all -driver pg

# Fail if a benchmark median regresses by more than 10%
go run ./cmd/benchdiff -base main -target HEAD -kind unit -fail-regression 10%
```

`benchdiff` creates detached worktrees under `.bench/`, runs each selected benchmark suite, writes raw output, and
produces a Markdown report. Worktrees are removed by default after the run; pass `-keep-worktrees` to preserve them.

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-base` | `main` | Base git ref |
| `-target` | `HEAD` | Target git ref |
| `-kind` | `all` | Benchmark kind (`all`, `unit`, `integration`) |
| `-packages` | `./...` | Package list for Go benchmarks |
| `-bench` | `.` | Go benchmark regexp |
| `-bench-count` | `10` | Go benchmark repetition count |
| `-benchtime` | `1s` | Go benchmark benchtime |
| `-driver` | `pg` | Integration benchmark database driver |
| `-connection` | | Integration connection string (or `CONNECTION_STRING`) |
| `-dataset` | | Run only this integration dataset |
| `-local-dataset` | | Add a local integration dataset |
| `-dataset-dir` | `integration/testdata` | Integration testdata directory |
| `-integration-iterations` | `10` | Timed iterations per integration scenario |
| `-out` | `.bench/runs/<base>..<target>-<timestamp>` | Output directory |
| `-benchstat` | `auto` | `benchstat` command, `auto`, or `none` |
| `-fail-regression` | `0` | Median regression percentage that fails the command |
| `-keep-worktrees` | `false` | Preserve temporary worktrees |

If `benchstat` is not on `PATH` and `-benchstat auto` is used, the harness falls back to
`go run golang.org/x/perf/cmd/benchstat@latest`.

Integration comparisons use native `cmd/benchmark -format benchfmt` when both refs support it. If either ref predates
that flag, the harness runs both refs in Markdown compatibility mode and compares each scenario's median as a single
`ns/op` sample.
