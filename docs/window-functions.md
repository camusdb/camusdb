# Window Functions

A window function computes a value for each row from a set of related rows, the *window*. Unlike a
grouped aggregate, it does not merge those rows into one. CamusDB supports the ranking functions, the offset and value functions,
and the aggregates `count`, `sum`, `avg`, `min` and `max` with an `OVER` clause. The semantics are
PostgreSQL's, except for the placement of NULL in a sort (see [Order and NULL](#order-and-null)).

```sql
-- Top three orders per customer
SELECT * FROM (
  SELECT customer, id, total,
         row_number() OVER (PARTITION BY customer ORDER BY total DESC, id) AS rn
  FROM orders
) ranked
WHERE rn <= 3;

-- Running total and the change from the previous day
SELECT day, amount,
       sum(amount) OVER (ORDER BY day) AS running,
       amount - lag(amount, 1, 0) OVER (ORDER BY day) AS change
FROM daily_sales;
```

## Syntax

```sql
function(args) OVER ( [PARTITION BY expr [, …]]
                      [ORDER BY expr [ASC | DESC] [, …]]
                      [frame] )

frame := { ROWS | RANGE } start
       | { ROWS | RANGE } BETWEEN start AND end

start, end := UNBOUNDED PRECEDING | n PRECEDING | CURRENT ROW | n FOLLOWING | UNBOUNDED FOLLOWING
```

- `OVER ()` puts all rows of the result in one partition.
- `n` is a non-negative integer literal or a placeholder (`@n`). It is allowed with `ROWS` only.
- A window call is allowed in the select list and in the final `ORDER BY` only. It can be part of a
  larger expression: `v - avg(v) OVER (PARTITION BY g)`, `coalesce(lag(v) OVER (ORDER BY id), 0)`.
- The arguments and the window specification can use group keys and aggregates of a grouped query:
  `rank() OVER (ORDER BY sum(amount) DESC)` with `GROUP BY customer`.

### `OVER` is a reserved word

`OVER` cannot be a table name, a column name or an alias unless it is quoted. The other window words
(`PARTITION`, `ROWS`, `RANGE`, `GROUPS`, `UNBOUNDED`, `PRECEDING`, `FOLLOWING`, `CURRENT`, `ROW`,
`EXCLUDE`) are not reserved. A column can be named `range` or `rows`.

`OVER` must be reserved because of the grammar. In `OVER (ORDER BY f(x) ROWS …)` the word after
`f(x)` is a frame word or another `OVER`. The LALR(1) parser reads one word of lookahead, which is
not enough to tell the two apart. The SQL standard also reserves `OVER`.

## Functions

| Function | Result type | Value for each row |
|---|---|---|
| `row_number()` | int64 | The position of the row in its partition, from 1 |
| `rank()` | int64 | 1 + the number of rows before the first peer of the row. Peers share a rank, and a gap follows |
| `dense_rank()` | int64 | The number of peer groups up to and including the row's group. No gaps |
| `percent_rank()` | float64 | `(rank - 1) / (rows - 1)`; 0 for a one-row partition |
| `cume_dist()` | float64 | (rows up to and including the last peer of the row) / rows |
| `ntile(k)` | int64 | The bucket of the row when the partition is cut into `k` buckets as equal as possible, larger buckets first |
| `lag(x [, offset [, default]])` | type of `x` | `x` of the row `offset` rows before (default 1), or `default` (default NULL) |
| `lead(x [, offset [, default]])` | type of `x` | `x` of the row `offset` rows after, or `default` |
| `first_value(x)` | type of `x` | `x` of the first row of the frame |
| `last_value(x)` | type of `x` | `x` of the last row of the frame |
| `nth_value(x, n)` | type of `x` | `x` of row `n` of the frame (from 1), or NULL when the frame is shorter |
| `count(*)`, `count(x)` | int64 | The rows of the frame, or the rows where `x` is not NULL |
| `sum(x)`, `avg(x)`, `min(x)`, `max(x)` | the type of the non-window aggregate | The aggregate over the frame |

- The ranking functions, `lag` and `lead` ignore a frame.
- `ntile` reads `k` from the first row of each partition. `k` must be greater than zero.
- `lag` and `lead` read `offset` and `default` on the current row. This means that either can be an
  expression of the row. A negative offset reads in the other direction. The default must have the
  type of `x` or be NULL, unless both are numeric. Two numeric types give the result the type of
  `x + default`: `lag(int_col, 1, 0.5)` returns float64 for all rows. A placeholder default is checked
  at execution time. It can be narrower than `x` (an int64 default for a float64 `x`). A wider
  placeholder default fails this check.
- An offset or a frame bound past the end of the partition stops at the end, whatever its size.
- `nth_value` reads `n` on the current row. `n` must be greater than zero.
- The aggregates have the result types and the NULL rules of the non-window aggregates: `sum` of int64 is
  int64, `avg` of int64 is float64, `sum` and `avg` of numeric are numeric. An empty frame gives NULL,
  and 0 for `count`.
- `sum` of int64 values adds the frame exactly. A frame total outside the int64 range returns the
  error `CADB0420` and is never wrapped (see [INT64 overflow](data-types.md#int64-overflow)).
- `sum` and `avg` over float64 values add the exact values of the frame and round once. This
  means that a frame's result does not depend on the order of its rows, and a sliding frame costs the
  same as a running one. A frame total past the float64 range from finite values returns the error
  `CADB0421`. A frame that contains an infinity or a NaN gives the IEEE total (see
  [Float values](data-types.md#float-values-infinity-nan-and-overflow)). The aggregate form `sum(x)`
  adds in row order. This means that `sum(x) OVER ()` and `sum(x)` can differ in the last binary digit
  of a float64.

## Frames and peers

Rows with equal `ORDER BY` values in the window are *peers*. If the window omits `ORDER BY`, all rows
of the partition are peers.

If the frame clause is omitted, the default frame is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT
ROW`. In a `RANGE` frame, `CURRENT ROW` means the first peer at the start and the last peer at the
end. This means that the default frame of a row ends at its last peer.

- A running `sum(x) OVER (ORDER BY day)` gives the same value to all rows of one day.
- If `ORDER BY` is omitted, the default frame covers all rows of the partition, and
  `sum(x) OVER (PARTITION BY g)` returns the partition total on each row.
- `last_value(x) OVER (ORDER BY t)` returns `x` of the last **peer** of the row. A frame that ends at
  `UNBOUNDED FOLLOWING` returns `x` of the last row of the partition.

  ```sql
  last_value(x) OVER (ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
  ```

A `ROWS` frame counts rows from the current row. `ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING` covers at
most four rows, from two rows before the current row to one row after it. The frame is cut at the ends of the
partition.

## Order and NULL

- The window `ORDER BY` sorts NULL the same way as the query's `ORDER BY`: NULL comes first in
  ascending order and last in descending order. PostgreSQL does the opposite by default. This
  means that a ranking over a nullable column can differ from PostgreSQL. `NULLS FIRST` and `NULLS LAST` are not
  supported yet. See [NULL in ORDER BY](./postgresql-differences.md#null-in-order-by).
- `PARTITION BY` puts all rows with a NULL key into one partition, as `GROUP BY` does.
- `row_number()` over rows with equal `ORDER BY` values numbers them in no defined order. Add a
  unique column to the window `ORDER BY` when the order must be repeatable.

## Where windows run

SQL computes windows after `WHERE`, `GROUP BY` and `HAVING`. The select list, `DISTINCT`, `ORDER BY`
and `LIMIT` come after the windows. A windowed `SELECT` is executed in two parts that preserve this
order.

1. An inner query computes `FROM`, `WHERE`, `GROUP BY` and `HAVING`. It also computes all aggregates
   and all values that the windows read.
2. The window step sorts those rows once for each distinct `(PARTITION BY, ORDER BY)` and computes
   all windows of that group, one partition at a time.

The outer query then computes the select list, `DISTINCT`, `ORDER BY` and `LIMIT`.

- A filter on a window result is applied after the window. Top-N per group works through a derived
  table or a view:
  `SELECT * FROM (SELECT …, row_number() OVER (PARTITION BY g ORDER BY t) AS rn FROM x) d WHERE rn <= 3`.
- A `LIMIT` does not reduce the rows a window reads. `SELECT …, count(*) OVER () FROM t LIMIT 5`
  returns five rows, each with the row count of the table.
- With distributed query execution, the inner query ships its scans and partial aggregates to the
  nodes that own the data, as any query does. The window step is computed on the node that
  executes the statement, over the gathered rows.

## Spill to disk

With `spill_enabled`, the window step uses the spill path of the sort for its sorts. The full rows
of the current partition are stored in a spill-capable buffer. A partition larger than the spill
threshold moves to disk. The values that the windows read (their arguments and their `ORDER BY`
values) and the results stay in memory for the current partition, a few values per row. A single
partition of tens of millions of rows therefore needs memory in proportion to its row count, even with spill
enabled.

Spill files are removed when the query ends, fails or is cancelled. See
[spill-to-disk.md](./spill-to-disk.md).

## EXPLAIN

The windows of a query show as one `window` node, with one entry for each distinct
`(PARTITION BY, ORDER BY)`.

```text
window(partition=[g] order=[id DESC] functions=[row_number(), sum(v) rows between 1 preceding and current row]; partition=[] order=[v] functions=[rank()])
```

A function that reads a frame shows its frame. In `EXPLAIN (ANALYZE)`, `partitions=` is the
number of partitions computed and `rows=` is the number of rows they held, both summed over the
entries. `spilled=` is the number of partition buffers that moved to disk. See [explain.md](./explain.md).

## Not supported

Each form in the following table returns an error that names it.

| Form | Error |
|---|---|
| A window call in `WHERE`, `GROUP BY`, `HAVING` or a `JOIN … ON` | InvalidInput: window functions are not allowed in that clause |
| A window call nested in another window call | InvalidInput |
| A window call in `UPDATE … SET`, `INSERT … VALUES` or a `CHECK` | InvalidInput |
| `RANGE` with `n PRECEDING` or `n FOLLOWING` | InvalidInput: use `ROWS` |
| `GROUPS` frames, `EXCLUDE` | InvalidInput |
| `OVER name` and the `WINDOW name AS (…)` clause | `OVER name` is InvalidInput; the `WINDOW` clause is a syntax error |
| `FILTER (WHERE …)` | Syntax error |
| A window call in a column `DEFAULT` or a `BETWEEN` bound | Syntax error |
| A function that is not in the table above, including C# extension aggregates | InvalidInput |
| `SELECT … FOR UPDATE` with a window | InvalidInput |
| `SELECT DISTINCT * … ORDER BY <window call>` | InvalidInput |

A subquery can hold a window call, correlated or not, in the `WHERE` and in the select list: the window
is computed over the rows the subquery reads for each outer row. A correlated subquery can also stand in
the select list of a query with a window, or in the `ORDER BY` of a window.
