# Statement Nesting Limit

A statement whose parse tree is deeper than **1,000 levels** returns error **CADB0413**
(`StatementTooDeeplyNested`). The limit protects the server process. Almost all statements are far
below it.

## Why there is a limit

The engine walks a parsed statement many times, for example to plan it and to evaluate its
expressions once per row. Most of those walks are recursive. This means that each level of nesting
uses some stack. A stack overflow in .NET cannot be caught. It ends the server process before an
error response is sent or the transaction is rolled back, and in a cluster it takes a node down.

Before the limit existed, small statements could do this. A `WHERE` clause of `value + 0 + 0 …`
with fewer than 900 terms (about 3 KiB of SQL) ended the process, and a generated
`a = 1 OR a = 2 OR …` filter did the same at about 6,500 terms.

The limit is checked once, right after parsing and before any other stage touches the statement.

## What counts toward the limit

Depth is the longest path from the statement root to a leaf. Each operator, function call,
`NOT`, `CASE` branch, list element and subquery level adds to it. Parentheses on their own do not.

Some shapes are cheap on purpose. The table shows how much each one costs.

| Shape | Cost toward the limit |
|---|---|
| `a OR b OR c …`, `a AND b AND c …` | about log₂ of the number of terms — 20,000 terms cost about 15 levels |
| `x IN (v1, v2, …)`, `x NOT IN (…)` | about log₂ of the number of values |
| `ARRAY[e1, e2, …]` | about log₂ of the number of elements |
| Rows of a multi-row `INSERT … VALUES (…), (…)` | none — a 10,000-row statement is accepted |
| `q1 UNION ALL q2 UNION ALL …` (also `UNION`, `INTERSECT`, `INTERSECT ALL`) | about 2 × log₂ of the number of operands |

The parser rebuilds long OR/AND chains and long value lists as balanced trees. The terms are left
in the order you wrote them. This means that results, short-circuit behavior and placeholder order
do not change.
The same applies to the value list that an `IN (SELECT …)` subquery produces at run time.

The following shapes cost one level per step.

- arithmetic chains: `a + b + c + …`
- nested function calls: `f(g(h(x)))`
- `NOT NOT …`
- `CASE` with many `WHEN` branches
- long select lists and column lists
- nested subqueries
- comma lists and `JOIN` chains, one level for each relation. The number of relations in a
  statement has a separate limit, `max_tables_per_statement` (see
  [Tables per statement](request-resource-limits.md#tables-per-statement)).

A statement that names all columns of a table at the default column limit (512) is about half the
limit deep.

## A separate limit for set operations

`UNION`, `INTERSECT` and `EXCEPT` use much more stack per level than an expression does. A
statement may nest at most **64** of them on one path, with the same error code. A long chain of
one associative operator is rebalanced and does not come near this limit. A chain of `EXCEPT`, or
of mixed operators, adds one level per operator. See [set-operations.md](./set-operations.md).

## A separate limit for WITH queries

The parser writes each reference to a CTE out as a copy of the CTE's query (see
[WITH queries](common-table-expressions.md)). The references of one `WITH` can copy at most 250,000
parse nodes. A statement that needs more returns the same `CADB0413` error. The limit stops a
chain of CTEs in which each reads the one before two times: such a chain doubles at each step.

## The error

```
CADB0413: Statement is nested too deeply: its parse tree exceeds 1000 levels. Reduce the nesting
of expressions, function calls, CASE branches or subqueries, or split the statement.
```

A second form, "Statement is nested too deeply to evaluate", comes from the same code when a nested
function-call chain uses up the available stack while it is evaluated.

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

The safe depth depends on the size of the engine's stack frames and on the thread stack size. The
workload does not change it. A higher value would let a statement crash the server. The limit was
chosen from a probe that raises each nesting shape until the process fails. With the current code
the weakest shape survives about 5,300 levels on a 1.5 MB thread stack. A limit of 1,000 leaves a
five-fold margin for 1 MB stacks and for code paths the probe does not reach.

The probe is in `CamusDB.MicroBenchmarks`. You can start it with the following command.

```sh
dotnet run -c Release -- probe exprscan [shape ...]
```

The probe tests each size in a child process. The parent cannot survive a stack overflow.
