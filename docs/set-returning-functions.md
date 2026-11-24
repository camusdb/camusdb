# Set-returning functions in FROM

A set-returning function returns a set of rows. You can use one in the `FROM` clause of a
`SELECT`, where it acts as a relation with one column. The first and only such function is
`generate_series`. It is the usual way to make test data, as in the following statements.

```sql
INSERT INTO scores (id, score)
  SELECT i, i * 10
  FROM generate_series(1, 10000) AS i;

SELECT COUNT(*) FROM generate_series(1, 1000000);
```

## generate_series(start, stop [, step])

The arguments are integers (`int64`). The default step is 1. The output column type is `int64`.
The results are the same as in PostgreSQL.

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
before the first row.

An argument can also name a column of an item to the left of the function in the same `FROM`, as in
`FROM t, generate_series(1, t.n) AS g`. Such a call is `LATERAL` whether or not you write the
keyword, as in PostgreSQL. It is evaluated one time for each row of `t`, with that row's `n`. See
[LATERAL](./joins.md#lateral). In a subquery or a `LATERAL` derived table, an argument can also name a
column of the query around it, as in `CROSS JOIN LATERAL (SELECT g FROM generate_series(1, t.n) g) s`
or `EXISTS (SELECT 1 FROM generate_series(1, t.n) g WHERE g = 2)`. A function that is the only item of
the `FROM` of a statement can name no column.

Two kinds of argument return an error.

- A subquery, as in `generate_series(1, (SELECT MAX(n) FROM t))`.
- A non-integer value, such as `1.5` or `'x'`.

## Names: the alias rule

A function with one output column names the relation and the column with the same word. A
column-alias list gives the column a separate name.

| `FROM` clause | Relation | Column | Valid references |
|---------------|----------|--------|------------------|
| `generate_series(1, 3)` | `generate_series` | `generate_series` | `generate_series`, `generate_series.generate_series` |
| `generate_series(1, 3) AS i` | `i` | `i` | `i`, `i.i` |
| `generate_series(1, 3) i` | `i` | `i` | `i`, `i.i` |
| `generate_series(1, 3) AS t(n)` | `t` | `n` | `n`, `t.n` |

A column-alias list must name exactly one column. `AS t(a, b)` is an error.

When a joined table also has a column with the same name, a bare reference to that name is
ambiguous, as in any join. You should qualify it, as in this example.

```sql
-- items has a column id, and the series column is also id.
SELECT items.id, id.id FROM items, generate_series(1, 2) id;
```

## Where the function can appear

The function is a relation. It can appear in any position where a table can appear.

- alone: `SELECT * FROM generate_series(1, 5)`
- in a comma join or an explicit join: `FROM items t, generate_series(1, 3) g`,
  `FROM generate_series(1, 9) g JOIN items t ON t.id = g`, `LEFT JOIN`, `CROSS JOIN`
- with `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `DISTINCT`, `LIMIT` and `OFFSET`
- in a derived table or a view body, including a materialized view
- in an `IN` or `EXISTS` subquery
- as the source of `INSERT ... SELECT` and `CREATE TABLE ... AS SELECT`

A view over a series survives a restart. `SHOW CREATE VIEW` prints the call as it was written.

## How it runs

- **The rows are streamed.** The function makes each row when the next operator asks for it. A
  million-row series does not become a list in memory, and `LIMIT 10` over a billion-row series
  returns at once. When a join reads the series again, the function makes the rows again.
- **Row limit.** One read of the function makes at most `max_table_function_rows` rows (default
  5,000,000). The row past the limit fails the statement with `CADB0561`. The count includes the
  rows that a `WHERE` filter drops. A `LIMIT` that stops the read early can keep the read under the
  limit. However, a `LIMIT` above an `ORDER BY`, a `GROUP BY`, or an aggregate cannot. These
  operators read all rows first. See [`request-resource-limits.md`](./request-resource-limits.md#functions-in-from).
- **No storage access.** The function reads no table. It takes no lock and needs no privilege. It
  adds no keys to a Serializable read set. A table joined to it needs `SELECT` on that table as
  usual. See [`sql-authentication.md`](./sql-authentication.md).
- **Row estimate.** When all arguments are literals, the planner uses the exact row count
  `(stop - start) / step + 1`. When an argument is a parameter or a function call, the planner uses
  its default table estimate. `EXPLAIN` shows the source as a `function-scan` node. See
  [`explain.md`](./explain.md).
- **Distributed execution.** The function is evaluated on the node that executes the statement. It
  is not sent to another node as a query fragment. When it is the build side of a broadcast hash
  join, the node ships the generated rows to the other nodes.
- **Order.** The rows come in series order. After a join, the order is not defined unless the query
  has an `ORDER BY`.
- **Result cache.** A `{cache=...}` hint cannot go on the function. A hinted query that also reads a
  function is a join or a view read. This means that it bypasses the cache with the reason `join`
  or `derived-source`. See [`query-result-cache.md`](./query-result-cache.md).

## Not supported

| Form | Result |
|------|--------|
| `SELECT generate_series(1, 3)` (a set-returning function in the select list) | error `CADB0533`: the function is supported only in `FROM` |
| A timestamp, date or `numeric` series | error: the arguments must be integers |
| `WITH ORDINALITY` | parse error |
| `unnest`, `generate_subscripts`, `regexp_matches`, `regexp_split_to_table` in `FROM` | error `CADB0533`: not supported in `FROM` yet |
| A scalar function in `FROM`, such as `FROM upper('a')` | error `CADB0400`: it is a scalar function |
| An unknown name, such as `FROM no_such_function(1)` | error `CADB0400` that names the function |

An `INSERT ... SELECT` from a series writes all its rows in one transaction. It is therefore subject
to the transaction mutation limit. See [`transaction-limits.md`](./transaction-limits.md).
