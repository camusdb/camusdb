# Statement Nesting Limit

CamusDB refuses a statement whose parse tree is deeper than **1,000 levels**, with error
**CADB0413** (`StatementTooDeeplyNested`). The limit exists to protect the server process, not to
police query style: almost every statement is far below it.

## Why there is a limit

The engine walks a parsed statement many times — to bind it, plan it, render it, and to evaluate
its expressions once per row. Most of those walks are recursive, so every level of nesting uses
some stack. A stack overflow in .NET cannot be caught: it ends the whole server process, with no
error response and no rollback, and in a cluster it takes a node down.

Before the limit existed, small statements could do this. A `WHERE` clause of `value + 0 + 0 …`
with fewer than 900 terms (about 3 KiB of SQL) ended the process, and a generated
`a = 1 OR a = 2 OR …` filter did the same at about 6,500 terms.

The limit is checked once, right after parsing and before any other stage touches the statement.

## What counts toward the limit

Depth is the longest path from the statement root to a leaf. Each operator, function call,
`NOT`, `CASE` branch, list element and subquery level adds to it. Parentheses on their own do not.

Some shapes are cheap on purpose:

| Shape | Cost toward the limit |
|---|---|
| `a OR b OR c …`, `a AND b AND c …` | about log₂ of the number of terms — 20,000 terms cost about 15 levels |
| `x IN (v1, v2, …)`, `x NOT IN (…)` | about log₂ of the number of values |
| `ARRAY[e1, e2, …]` | about log₂ of the number of elements |
| Rows of a multi-row `INSERT … VALUES (…), (…)` | nothing — a 10,000-row statement is accepted |

The parser rebuilds long OR/AND chains and long value lists as balanced trees. The terms stay in
the order you wrote them, so results, short-circuit behavior and placeholder order do not change.
The same applies to the value list that an `IN (SELECT …)` subquery produces at run time.

Shapes that still cost one level per step:

- arithmetic chains: `a + b + c + …`
- nested function calls: `f(g(h(x)))`
- `NOT NOT …`
- `CASE` with many `WHEN` branches
- long select lists and column lists
- nested subqueries

A statement that names every column of a table at the default column limit (512) is about half the
limit deep.

## The error

```
CADB0413: Statement is nested too deeply: its parse tree exceeds 1000 levels. Reduce the nesting
of expressions, function calls, CASE branches or subqueries, or split the statement.
```

A second form, "Statement is nested too deeply to evaluate", comes from the same code when a nested
function-call chain runs short of stack while it is evaluated.

- HTTP status **400**.
- gRPC status **INVALID_ARGUMENT**.
- Not retryable: the same statement fails the same way. Rewrite it with less nesting.

## How to rewrite a refused statement

- Replace a long `col = 1 OR col = 2 OR …` with `col IN (1, 2, …)`. Both forms are accepted, but the
  `IN` form is shorter and can use an index.
- Replace a long arithmetic chain with an aggregate or with fewer, grouped terms.
- Replace a `CASE` with thousands of branches with a join against a mapping table.
- Split a very wide statement into several statements.

## Why the limit is not configurable

The safe depth depends on the size of the engine's stack frames and on the thread stack size, not
on the workload. A higher value would let a statement crash the server. The limit was chosen from a
probe that raises each nesting shape until the process fails: with the current code the weakest
shape survives about 5,300 levels on a 1.5 MB thread stack, so 1,000 keeps a five-fold margin for
1 MB stacks and for code paths the probe does not reach.

The probe lives in `CamusDB.MicroBenchmarks` and runs with:

```sh
dotnet run -c Release -- probe exprscan [shape ...]
```

It runs each size in a child process, because the parent cannot survive a stack overflow.
