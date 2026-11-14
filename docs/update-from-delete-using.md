# UPDATE … FROM and DELETE … USING

An `UPDATE` can read other relations to decide which rows change and to get the new values. A
`DELETE` can read other relations to decide which rows it deletes. CamusDB uses the PostgreSQL forms:

```sql
UPDATE accounts a
   SET balance = a.balance + d.amount
  FROM deltas d
 WHERE d.account_id = a.id;

DELETE FROM orders o
 USING customers c
 WHERE c.id = o.customer_id AND c.closed;
```

The statement changes rows of its target table only. The other relations are read, never written.

## The forms

- `UPDATE <table> [[AS] <alias>] SET … FROM <relations> WHERE <condition> [RETURNING …]`
- `DELETE FROM <table> [[AS] <alias>] USING <relations> WHERE <condition> [RETURNING …]`

The list after `FROM` or `USING` takes every form that the `FROM` clause of a `SELECT` takes: tables,
views, derived tables, set-returning functions such as `generate_series`, a comma list, and a chain
of `JOIN`s. See [joins](joins.md) and [set-returning functions](set-returning-functions.md).

The target can have an alias without a relation list too: `UPDATE accounts AS a SET … WHERE a.id = 4`
and `DELETE FROM accounts AS a WHERE a.closed` work.

A `VALUES` list is not a relation in `FROM`, in a `SELECT` or here. Use a derived table instead:

```sql
UPDATE input_state s
   SET down = v.d
  FROM (SELECT 'forward' AS k, true AS d UNION ALL SELECT 'back', false) v
 WHERE v.k = s.k;
```

## Which relation a name reads

- A name qualified with the alias of the target reads the target. When the target has no alias, the
  table name qualifies it. When the target has an alias, the table name does not qualify it, as in
  PostgreSQL.
- A bare name reads the relation that has a column of that name. A bare name that the target and
  another relation both have is ambiguous, and the statement fails with `CADB0400`.
- The target can appear in the relation list again only with an alias on one side. That is a self
  join. Without an alias on either side, the statement fails with `CADB0400`.

## What the statement does

1. It finds the rows. It runs one `SELECT` over the target and the listed relations with the
   statement's `WHERE`, at the statement's read timestamp. That query reads the primary key of each
   target row and the values of the other relations that the `SET` list needs.
2. It changes the rows. For each target row, it locks the row, reads it again, checks the `WHERE` again
   and writes the row. The rules of a plain `UPDATE` or `DELETE` apply: triggers, foreign keys,
   `CHECK` constraints, unique indexes, large values and the mutation limit.

Step 1 finishes before step 2 starts, so the statement does not see its own changes. In a self join,
each row reads the other rows as the statement found them.

A `SET` value reads the target row as step 2 locked and read it. So
`SET balance = a.balance + d.amount` adds the amount to the latest committed balance, also when a
concurrent transaction changed the balance after step 1. The values of the other relations are those
that step 1 found. Each `SET` value reads the old row, so `SET a = b, b = a` swaps the two values.

A `SET` value that holds a subquery is computed in step 1, at the statement's read timestamp, as the
correlated subquery of a plain `UPDATE` is.

## The check after the lock

Step 2 checks each part of the `WHERE` (each `AND` term) that names a target column and holds no
subquery again, with the row that it locked and the values that step 1 found. A row that a concurrent
commit moved out of the `WHERE` is not changed and not counted. This is what PostgreSQL does with an
updated row at Read Committed.

## One change for each row

An `UPDATE` fails with `CADB0400` when one target row matches two or more rows of the relation list.
The error names the primary key of the row: `UPDATE ... FROM matched the row (id)=(1) of table
'accounts' more than once`. Nothing is written. PostgreSQL updates such a row once with one of the
matches, and does not say which, so the result would depend on the plan. To update from many rows,
make one row for each target row first, for example with `GROUP BY`:

```sql
UPDATE accounts a
   SET balance = a.balance + t.total
  FROM (SELECT account_id, SUM(amount) AS total FROM deltas GROUP BY account_id) t
 WHERE t.account_id = a.id;
```

A `DELETE` deletes such a row once, because the result does not depend on the match.

## Rules

- A `WHERE` clause is necessary, as for a plain `UPDATE` or `DELETE`. Write `WHERE TRUE` to change
  every row that the relations allow.
- `LIMIT` is not supported in these forms (`CADB0400`).
- `RETURNING` reads the target row only. A column of another relation is refused with `CADB0400`.
  With a `FROM` or `USING` list, a bare `*` is refused too, because in PostgreSQL it also returns the
  columns of the other relations. With a target alias only, `*` returns the target's columns. See
  [RETURNING](returning.md).
- The primary key of the target cannot be assigned, as in a plain `UPDATE`.
- `EXPLAIN` does not take an `UPDATE` or a `DELETE`.

## Privileges

The statement needs `UPDATE` (or `DELETE`) on the target, and `SELECT` on every relation it reads,
the target included. Without `SELECT` on a source table, the statement would be a way to read that
table: which rows change depends on its rows. PostgreSQL also asks for `SELECT` on the columns that a
statement reads. A plain `UPDATE` without `RETURNING` needs `UPDATE` only.

## Transactions

Step 1 reads like the source query of `INSERT … SELECT`: the rows it reads decide what is written, so
under Serializable its range locks are exclusive, on the target and on the other relations. Step 2
takes the exclusive row locks of a plain `UPDATE` or `DELETE`.
See [transactions, locking and isolation](transactions-locking-and-isolation.md).

## USING is a reserved word

`USING` is a reserved word, as in PostgreSQL: after the target of `DELETE FROM`, a plain word could be
the alias of the target or the start of the `USING` list, and the parser reads one word ahead only. A
table or column named `using` must be written with backticks:

```sql
SELECT `using` FROM t;      -- works
SELECT using FROM t;        -- syntax error
```

Longer names, such as `usings` or `using_id`, still work without backticks. `SHOW CREATE TABLE`
writes every name with backticks, so a schema with such a column still round-trips.
