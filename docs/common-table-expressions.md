# WITH queries (common table expressions)

A `WITH` clause names one or more queries, and the statement after it reads them by name, as it reads a
table. Each named query is a common table expression (CTE). CamusDB uses the PostgreSQL forms:

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
- Inside parentheses: a subquery (`IN`, `EXISTS`, a scalar subquery), a derived table, and an operand of
  `UNION`, `INTERSECT` or `EXCEPT`.

## The definition

`name [(c1, c2, …)] AS [[NOT] MATERIALIZED] (query)`

- The name is not qualified. Two definitions of one `WITH` cannot have the same name (`CADB0400`).
- The column list names the columns of the query in order. A shorter list renames the first columns,
  and the later columns keep their names. A longer list is refused (`CADB0400`). A list can give one
  name two times, as in PostgreSQL; a reference to that name is then ambiguous (`CADB0400`).
- `MATERIALIZED` and `NOT MATERIALIZED` choose how the statement runs the query. See below.
- The query can be any query: joins, `GROUP BY`, window functions, set operations, `ORDER BY` and
  `LIMIT`, and a `WITH` of its own.

A derived table takes a column list too: `SELECT a + b FROM (SELECT 1, 2) AS d (a, b)`.

## Which relation a name reads

- A CTE is visible in the statement after its `WITH`, subqueries included, and in the CTEs after it.
- A CTE is not visible in its own query or in an earlier CTE. There the name reads the table of that
  name. So `WITH items AS (SELECT * FROM items WHERE id < 10) SELECT * FROM items` reads the table in
  the CTE and the CTE in the statement.
- A CTE name hides a table or a view of the same name.
- A `WITH` inside parentheses hides a CTE of the same name from an enclosing `WITH`.
- The target of a write is never a CTE: `WITH t AS (…) UPDATE t …` updates the table `t`. A CTE can be
  read in `UPDATE … FROM`, `DELETE … USING`, and any subquery of the write.
- An index hint on a CTE reference is refused (`CADB0400`): a CTE has no indexes.

## How the statement runs a CTE

The parser writes each reference to a CTE out as a derived table over a copy of the CTE's query. Every
later step (privileges, planning, `EXPLAIN`, spill) is then the step of a derived table. See
[joins](joins.md) for derived tables.

A CTE is **inlined** or **computed once**:

- **Inlined.** Each reader runs its own copy of the query, as a derived table it wrote itself. The
  planner can then push a filter of the reader down. This is the rule for a CTE that the statement reads
  one time, for `NOT MATERIALIZED`, and for every CTE of a `WITH` inside parentheses.
- **Computed once.** The first reader runs the query and keeps its rows for the statement, and every
  other reader reads the kept rows. This is the rule for a CTE of a `WITH` at the start of the
  statement, or at the start of its source query, that the statement reads two or more times, and for
  `MATERIALIZED`. A volatile function in such a CTE, such as `random()`, gives every reader the same
  values.

Each reader applies its own filter to the kept rows, so a filter of one reader does not change the
rows of another. The kept rows are spilled to disk under the rules of
[spill to disk](spill-to-disk.md), and the statement deletes them when it ends.

A CTE that is computed once is computed in full before any reader gets a row, also under a `LIMIT`.
For a large CTE that one reader stops early, `NOT MATERIALIZED` can be faster.

`EXPLAIN` marks each reader of a CTE that is computed once with `computed_once=<name>`.

A `WITH` inside parentheses is always inlined because such a query can read the columns of the query
around it: one set of rows is then not right for every row of the outer query.

## Views

A view stores its body with each CTE written out as a derived table, so `SHOW CREATE VIEW` shows the
derived tables and no `WITH`. The stored text parses again to the same view. Because the stored body
has no `WITH`, a CTE that the body reads two times runs two times when the view is read.

## Limits

- `WITH RECURSIVE` is not supported (`CADB0533`).
- A data-modifying CTE, such as `WITH d AS (DELETE … RETURNING …)`, is not supported.
- A CTE that the statement does not read is not checked. An unknown table in it gives no error.
- The references of one `WITH` can copy at most 250,000 parse nodes (`CADB0413`). A chain of CTEs in
  which each reads the one before two times doubles at each step, and reaches the limit after about
  twenty steps.

See [differences from PostgreSQL](postgresql-differences.md#with-queries) for the answers that differ
from PostgreSQL.
