# Set-returning functions in FROM

A set-returning function returns rows, not one value. CamusDB accepts one in the `FROM` clause of a
`SELECT`, where it acts as a relation with one column. The first and only such function is
`generate_series`. It is the usual way to make test data:

```sql
INSERT INTO scores (id, score)
  SELECT i, i * 10
  FROM generate_series(1, 10000) AS i;

SELECT COUNT(*) FROM generate_series(1, 1000000);
```

## generate_series(start, stop [, step])

The arguments are integers (`int64`). The default step is 1. The output column type is `int64`.
The results are the same as in PostgreSQL:

| Call | Rows |
|------|------|
| `generate_series(1, 5)` | 1, 2, 3, 4, 5 |
| `generate_series(5, 1, -2)` | 5, 3, 1 |
| `generate_series(1, 10, 3)` | 1, 4, 7, 10 |
| `generate_series(1, 4, 2)` | 1, 3 (the steps do not land on 4) |
| `generate_series(7, 7)` | 7 |
| `generate_series(1, 0)` | no rows (the step goes the wrong way) |
| `generate_series(1, NULL)` | no rows (a NULL argument gives no rows) |
| `generate_series(1, NULL, 0)` | no rows (the NULL rule applies before the step check) |
| `generate_series(1, 5, 0)` | error: `generate_series step size cannot equal zero` |

The series includes `stop` when a step lands on it exactly. A negative step counts down.

**Overflow ends the series.** When the next value does not fit in `int64`, the series stops. It
does not wrap to the other end of the range. So
`generate_series(9223372036854775806, 9223372036854775807)` returns two rows.

**Arguments.** An argument can be a literal, a parameter (`@from`), an operator expression, or a
scalar function call: `generate_series(1 + 1, @to, abs(@step))`. Each argument is evaluated one time,
before the first row. These arguments are refused:

- A column of another relation in the same `FROM`, as in `FROM t, generate_series(1, t.n)`. That form
  is `LATERAL`, which is not supported.
- A subquery, as in `generate_series(1, (SELECT MAX(n) FROM t))`.
- A non-integer value, such as `1.5` or `'x'`.

## Names: the alias rule

A function with one output column names the relation and the column with the same word:

| `FROM` clause | Relation | Column | Valid references |
|---------------|----------|--------|------------------|
| `generate_series(1, 3)` | `generate_series` | `generate_series` | `generate_series`, `generate_series.generate_series` |
| `generate_series(1, 3) AS i` | `i` | `i` | `i`, `i.i` |
| `generate_series(1, 3) i` | `i` | `i` | `i`, `i.i` |
| `generate_series(1, 3) AS t(n)` | `t` | `n` | `n`, `t.n` |

A column-alias list must name exactly one column. `AS t(a, b)` is an error.

When a joined table also has a column with the same name, a bare reference to that name is
ambiguous, as in any join. Qualify it:

```sql
-- items has a column id, and the series column is also id.
SELECT items.id, id.id FROM items, generate_series(1, 2) id;
```

## Where the function can appear

The function is a relation, so it goes where a table goes:

- alone: `SELECT * FROM generate_series(1, 5)`
- in a comma join or an explicit join: `FROM items t, generate_series(1, 3) g`,
  `FROM generate_series(1, 9) g JOIN items t ON t.id = g`, `LEFT JOIN`, `CROSS JOIN`
- with `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `DISTINCT`, `LIMIT` and `OFFSET`
- in a derived table, a view body, a materialized view, and an `IN` or `EXISTS` subquery
- as the source of `INSERT ... SELECT` and `CREATE TABLE ... AS SELECT`

A view over a series survives a restart. `SHOW CREATE VIEW` prints the call as it was written.

## How it runs

- **The rows are streamed.** The function makes each row when the next operator asks for it. A
  million-row series does not become a list in memory, and `LIMIT 10` over a billion-row series
  returns at once. When a join reads the series again, the function makes the rows again.
- **No storage access.** The function reads no table. It takes no lock, adds nothing to a
  Serializable read set, and needs no privilege. A table joined to it still needs `SELECT` on that
  table. See [`sql-authentication.md`](./sql-authentication.md).
- **Row estimate.** When every argument is a literal, the planner uses the exact row count
  `(stop - start) / step + 1`. When an argument is a parameter or a function call, the planner uses
  its default table estimate. `EXPLAIN` shows the source as a `function-scan` node. See
  [`explain.md`](./explain.md).
- **Distributed execution.** The function runs on the node that runs the statement. It is never sent
  to another node as a query fragment. When it is the build side of a broadcast hash join, the node
  ships the generated rows, not the function.
- **Order.** The rows come in series order. After a join, the order is not defined unless the query
  has an `ORDER BY`.
- **Result cache.** A `{cache=...}` hint cannot go on the function. A hinted query that also reads a
  function is a join or a view read, so it bypasses the cache with the reason `join` or
  `derived-source`. See [`query-result-cache.md`](./query-result-cache.md).

## Not supported

| Form | Result |
|------|--------|
| `SELECT generate_series(1, 3)` (a set-returning function in the select list) | error `CADB0533`: the function is supported only in `FROM` |
| `FROM t, generate_series(1, t.n)` (`LATERAL`) | error `CADB0533` |
| A timestamp, date or `numeric` series | error: the arguments must be integers |
| `WITH ORDINALITY` | parse error |
| `unnest`, `generate_subscripts`, `regexp_matches`, `regexp_split_to_table` in `FROM` | error `CADB0533`: not supported in `FROM` yet |
| A scalar function in `FROM`, such as `FROM upper('a')` | error `CADB0400`: it is a scalar function |
| An unknown name, such as `FROM no_such_function(1)` | error `CADB0400` that names the function |
| A correlated `EXISTS` whose subquery reads a function | error: a correlated `EXISTS` over a multi-source subquery is not supported yet (the function counts as a derived source) |

An `INSERT ... SELECT` from a series writes all its rows in one transaction, so it is subject to the
transaction mutation limit. See [`transaction-limits.md`](./transaction-limits.md).
