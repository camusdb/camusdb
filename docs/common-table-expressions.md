# WITH queries (common table expressions)

A `WITH` clause names one or more queries, and the statement after it reads them by name, as it reads a
table. Each named query is a common table expression (CTE). A `WITH RECURSIVE` query can also read
itself, to walk a tree or a graph; see [recursive queries](#recursive-queries). CamusDB uses the
PostgreSQL forms, as in this example.

```sql
WITH totals AS (
         SELECT account_id, SUM(amount) AS total FROM deltas GROUP BY account_id
     ),
     big (account, amount) AS (
         SELECT account_id, total FROM totals WHERE total > 1000
     )
SELECT a.name, b.amount
  FROM accounts a JOIN big b ON b.account = a.id;
```

## Where a WITH can go

- In front of a `SELECT`, also under `EXPLAIN`. A `SELECT … FOR UPDATE` that reads a CTE is refused
  (`CADB0400`), as for a derived table.
- In front of an `INSERT`, an `UPSERT`, an `UPDATE` or a `DELETE`:
  `WITH s AS (…) UPDATE t SET … WHERE t.id IN (SELECT id FROM s)`.
- In the source query of `INSERT INTO t WITH s AS (…) SELECT …` and of `CREATE TABLE … AS`.
- In a view body and a materialized-view body.
- In parentheses: a subquery (`IN`, `EXISTS`, a scalar subquery), a derived table, or an operand of a
  set operation (`UNION`, `INTERSECT`, `EXCEPT`).

## The definition

`name [(c1, c2, …)] AS [[NOT] MATERIALIZED] (query)`

- The name is not qualified. Two definitions of one `WITH` cannot have the same name (`CADB0400`).
- The column list names the columns of the query in order. A shorter list renames the first columns,
  and the later columns keep their names. A longer list is refused (`CADB0400`). A list can give one
  name two times, as in PostgreSQL; a reference to that name is then ambiguous (`CADB0400`).
- `MATERIALIZED` and `NOT MATERIALIZED` choose how the query is executed. See below.
- The query can be any query, including joins, `GROUP BY`, window functions, set operations, and `ORDER BY` with
  `LIMIT`. It can also start with a nested `WITH`.

A derived table takes a column list too: `SELECT a + b FROM (SELECT 1, 2) AS d (a, b)`.

## Which relation a name reads

- A CTE is visible in the statement after its `WITH`, including subqueries. It is also visible in the
  CTEs after it.
- A CTE is not visible in the query that defines it or in an earlier CTE. There the name reads the table of that
  name. So `WITH items AS (SELECT * FROM items WHERE id < 10) SELECT * FROM items` reads the table in
  the CTE and the CTE in the statement.
- A CTE name hides a table or a view of the same name.
- A `WITH` in parentheses hides a CTE of the same name from an enclosing `WITH`.
- A write does not target a CTE: `WITH t AS (…) UPDATE t …` updates the table `t`. A CTE
  can be read in `UPDATE … FROM` or `DELETE … USING`, and in any subquery of the write.
- An index hint on a CTE reference is not allowed (`CADB0400`), because a CTE does not have indexes.

## How the statement runs a CTE

The parser writes each reference to a CTE out as a derived table over a copy of the CTE's query. Later
steps, such as privilege checks, planning, `EXPLAIN` and spill, then treat it as a derived table. See
[joins](joins.md) for derived tables.

A CTE is either **inlined** or **computed once**.

- **Inlined.** Each reader executes a separate copy of the query, as a derived table it wrote itself.
  The planner can then push a filter of the reader down. This is the rule for a CTE that the statement
  reads one time and for `NOT MATERIALIZED`. It is also the rule for all CTEs of a `WITH` in
  parentheses.
- **Computed once.** The first reader executes the query and stores its rows for the statement. The
  other readers read the stored rows. This is the rule for `MATERIALIZED`, and for a CTE that the
  statement reads two or more times when its `WITH` is at the start of the statement or of its source
  query. A volatile function in such a CTE, such as `random()`, gives all readers the same values.

Each reader applies a separate filter to the stored rows. A filter of one reader does not change the
rows of another. The stored rows are spilled to disk under the rules of
[spill to disk](spill-to-disk.md), and the statement deletes them when it ends.

A CTE that is computed once is computed in full before any reader gets a row, also under a `LIMIT`.
For a large CTE that one reader stops early, `NOT MATERIALIZED` can be faster.

`EXPLAIN` marks each reader of a CTE that is computed once with `computed_once=<name>`.

A `WITH` in parentheses is always inlined. Such a query can read the columns of the query around it,
and one set of rows would then be wrong for some rows of the outer query.

## Recursive queries

`WITH RECURSIVE` lets a CTE read itself. The usual use is a walk over a tree or a graph: an org chart,
a bill of materials, a category tree, a path search.

```sql
WITH RECURSIVE walk (id, depth, path) AS (
    SELECT id, 0, CAST(id AS STRING) FROM node WHERE parent_id IS NULL
  UNION ALL
    SELECT n.id, w.depth + 1, w.path || '/' || CAST(n.id AS STRING)
      FROM walk w JOIN node n ON n.parent_id = w.id
)
SELECT id, depth, path FROM walk ORDER BY path;
```

### The form

A CTE that reads itself must have the form `non-recursive term UNION [ALL] recursive term`. The
terms follow these rules.

- The non-recursive term does not read the CTE. It can be any query, also a chain of `UNION ALL`.
- The recursive term reads the CTE one time, in its `FROM` or in a derived table in its `FROM`.
- The `UNION` cannot take an `ORDER BY`, `LIMIT` or `OFFSET` clause (`CADB0533`). A recursive term in
  parentheses can have them.

The reference to the CTE is not allowed in the following places (`CADB0400`), as in PostgreSQL. In
each of them, a row of one step would depend on rows that a later step adds.

- a subquery in an expression, such as `IN (SELECT …)`, `EXISTS` or a scalar subquery;
- the side of an outer join that can be NULL: the right side of `LEFT JOIN`, the left side of
  `RIGHT JOIN`;
- `INTERSECT ALL`, the right side of `EXCEPT`, and the left side of `EXCEPT ALL`;
- a SELECT whose own `FROM` reads the CTE and that has an aggregate. A SELECT over a derived table that
  reads the CTE can aggregate.

Window functions, `DISTINCT`, `GROUP BY` and `HAVING` are allowed in the recursive term.

### Names

Under `WITH RECURSIVE`, each CTE of the list is visible in its own query and in the queries of the
other CTEs, in any order. This means that a CTE can read a CTE that is written after it. A CTE name hides a table
of the same name, also in the CTE's own query. Two or more CTEs that read each other are refused
(`CADB0533`). A CTE of the list that does not read itself is an ordinary CTE.

### Types

The column types are the types of the non-recursive term. A NULL or a placeholder alone in that term
has the type STRING. When the recursive term gives a column another type, the query has the
wider type, and the statement returns an error (`CADB0400`). For example,
`SELECT 1 UNION ALL SELECT n + 1.5 …` returns an error. The rows of the first step are INT64. Cast the non-recursive term to the
type of the query: `SELECT CAST(1 AS FLOAT64) UNION ALL …`.

### How it runs

The query is executed in steps, with the work-table method of PostgreSQL.

1. Step 0 executes the non-recursive term.
2. Each next step executes the recursive term again. The reference to the CTE reads only the rows that
   the step before made (the work table).
3. The query ends after a step that makes no row.

`UNION ALL` returns all rows. `UNION` drops a row that an earlier step or the same step returned. This
means that a walk over a graph with a cycle ends when no new row appears. If you use `UNION ALL`, such a
walk does not end.

Each row is returned when its step makes it. This means that a `LIMIT` over a recursive CTE that the
statement reads one time stops the recursion. This is also true for an endless recursion.

```sql
WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t) SELECT n FROM t LIMIT 5;
```

A recursive CTE that the statement reads two or more times is computed once, as any CTE is (see
above). That run makes all the rows before any reader gets one. A `LIMIT` in a reader does not stop it.

### Memory

The rows of one step are kept for the next step, and they spill to disk under the rules of
[spill to disk](spill-to-disk.md). Only the rows of the last two steps are kept at one time. `UNION`
also stores one copy of each row that it returned, in memory, to drop a row that comes again.

### Limits of a recursion

By default, a statement does not have a time limit. This means that an endless recursion would continue until the disk is full. Two
cluster settings stop it with `CADB0560` (`RecursiveQueryLimitExceeded`). The message names the
setting.

| Setting | Default | What it bounds |
|---|---|---|
| `max_recursive_cte_iterations` | `10000` | The steps of the recursive term that make rows. A recursion of exactly this many steps ends normally. |
| `max_recursive_cte_rows` | `10000000` | The rows of one run of the CTE, the rows of the non-recursive term included. |

Both are runtime settings: `SET CLUSTER SETTING` changes them, and the next statement uses the new
value. A value of `0` turns a limit off. See [request resource limits](request-resource-limits.md).

### EXPLAIN

`EXPLAIN` shows a reader of a recursive CTE as `recursive-union`, with the CTE name and the reader's
alias, and then the plans of the two terms. The reference to the CTE in the recursive term is a
`work-table-scan`. `EXPLAIN (ANALYZE)` does not count the rows of the two terms.

### Not supported

- The `SEARCH` and `CYCLE` clauses of PostgreSQL (`CADB0406`). Use `UNION` to end a walk over a cycle.
- `CREATE RECURSIVE VIEW`. Write the view body with `WITH RECURSIVE`.
- Mutual recursion between CTEs (`CADB0533`), as in PostgreSQL.

## Views

A view stores its body with each CTE written out as a derived table. `SHOW CREATE VIEW` shows the
derived tables, and the `WITH` is gone. The stored text parses again to the same view. Since the
stored body does not contain a `WITH`, a CTE that the body reads two times is executed two times when
the view is read.

A recursive CTE cannot be written out as a derived table over a copy of its query, as a non-recursive CTE is. It is stored where the body reads it, as
`(WITH RECURSIVE name (columns) AS (…) SELECT * FROM name) AS alias`. This text also parses again to
the same view. When the recursive query also reads a table of the CTE's name, the CTE is shown under the
first free name, such as `name_1`. That happens after a table rename, or when the query reads the table
through an outer CTE. Its reference uses the old name as its alias. This means that the text has the same meaning.

## Limits

- A data-modifying CTE, such as `WITH d AS (DELETE … RETURNING …)`, is not supported.
- A CTE that the statement does not read is not checked. An unknown table in it gives no error.
- The references of one `WITH` can copy at most 250,000 parse nodes (`CADB0413`). A chain of CTEs in
  which each reads the one before two times doubles at each step, and reaches the limit after about
  twenty steps.

See [differences from PostgreSQL](postgresql-differences.md#with-queries) for the answers that differ
from PostgreSQL.
