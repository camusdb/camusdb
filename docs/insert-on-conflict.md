# INSERT … ON CONFLICT and UPSERT

An `INSERT` can say what to do with a proposed row whose unique key is already taken: skip the
row, or update the stored row. `UPSERT INTO` is a short form that updates on the primary key.

```sql
INSERT INTO t [(cols)] { VALUES … | SELECT … }
  ON CONFLICT [ (col, …) | ON CONSTRAINT name ] DO NOTHING
  [RETURNING …];

INSERT INTO t [(cols)] { VALUES … | SELECT … }
  ON CONFLICT ( (col, …) | ON CONSTRAINT name )
  DO UPDATE SET c = expr [, …] [WHERE cond]
  [RETURNING …];

UPSERT INTO t [(cols)] { VALUES … | SELECT … } [RETURNING …];
```

The clause works with every INSERT form, including `OVERRIDING { SYSTEM | USER } VALUE` and
`INSERT … SELECT`. It comes after the row source and before `RETURNING`.

## Example

```sql
CREATE TABLE counters (k string NOT NULL, n int64 NOT NULL, PRIMARY KEY (k));

INSERT INTO counters (k, n) VALUES ('hits', 1)
  ON CONFLICT (k) DO UPDATE SET n = counters.n + excluded.n
  RETURNING k, n;
```

The first run inserts `('hits', 1)`. Each later run adds 1 to the stored `n`.

## The arbiter index

The arbiter is the unique index that decides whether a proposed row conflicts.

| Target | Arbiter |
|---|---|
| `ON CONFLICT (a, b)` | Every unique index whose column set is exactly `{a, b}`. The order of the columns does not matter. |
| `ON CONFLICT ON CONSTRAINT name` | The unique index with that name. The primary key answers to `<table>_pkey`. |
| no target (`DO NOTHING` only) | Every unique index of the table, the primary key included. |
| `UPSERT` | The primary key. |

- No matching unique index gives an `InvalidInput` error. A name that is not a unique index (a
  non-unique index, a CHECK or a foreign key) is an error too.
- `DO UPDATE` must name a target. `ON CONFLICT DO UPDATE …` without one is refused when the
  statement is parsed.
- A unique index that `CREATE INDEX` is still building cannot be the arbiter yet.

## What happens to each proposed row

1. **The key is free.** The row is inserted. A duplicate on a unique index that is **not** an
   arbiter is still an error (`DuplicateUniqueKeyValue`), and it ends the transaction.
2. **The key is taken, with DO NOTHING.** The row is skipped.
3. **The key is taken, with DO UPDATE.** The stored row is locked and read. The `WHERE` is evaluated;
   when it is not true, the row is skipped. Otherwise the `SET` list is applied through the normal
   update path: index maintenance, NOT NULL, CHECK, foreign keys and triggers.

**NULL keys never conflict.** A unique index has no entry for a key with a NULL column, so a
proposed row with a NULL in the arbiter key is always inserted.

**The proposed row is checked first.** NOT NULL and CHECK constraints are evaluated on every
proposed row before the conflict check, as in PostgreSQL. A row that breaks one fails even when its
key is taken.

## Reading the two rows

In `SET` and in the `DO UPDATE … WHERE`:

- a bare `c`, or `t.c`, is the **stored** row;
- `excluded.c` is the **proposed** row, after column defaults, type coercion and the BEFORE INSERT
  triggers. A column that the INSERT did not list holds its default there.

Every `SET` expression reads the stored row as it was before the update, so
`SET a = b, b = a` swaps two values. A qualifier other than the table name or `excluded` is an
`UnknownColumn` error.

A scalar, `IN` or `EXISTS` subquery that does not read the conflicting row is evaluated once, before
the first row is written. A correlated subquery is refused.

A `SET` cannot assign a primary-key column, as `UPDATE` cannot. A `SET` that moves a unique key onto
a key another row holds fails with `DuplicateUniqueKeyValue`.

## One stored row, one change per statement

- **DO UPDATE:** two proposed rows that resolve to the same stored row, or to the same new key, fail
  with `InvalidInput`: "ON CONFLICT DO UPDATE cannot affect a row a second time". This is PostgreSQL
  error 21000. Without this rule, the result would depend on the order of the rows. The rule applies
  even when the `WHERE` skipped the first of the two rows.
- **DO NOTHING:** the second proposed row conflicts with the row that the first one inserted, so it
  is skipped.

## UPSERT

`UPSERT INTO t (cols) …` is the CockroachDB form. It means exactly:

```sql
INSERT INTO t (cols) … ON CONFLICT (<primary key>) DO UPDATE SET c = excluded.c
```

for every listed column that is not part of the primary key. With no column list, every column is
listed. A column that the statement does not list keeps its stored value on update, and gets its
default on insert. When every listed column is part of the primary key, a taken key leaves the stored
row as it is.

`UPSERT` is not a reserved word: a table or a column can still be named `upsert`. The same is true
of `CONFLICT`, `DO`, `NOTHING` and `EXCLUDED`.

## RETURNING and the row count

`RETURNING` returns the inserted rows and the updated rows (their new values), in the order of the
proposed rows. A skipped row returns nothing. The statement's row count is the number of inserted
rows plus the number of updated rows.

## Triggers

The order is PostgreSQL's:

1. BEFORE INSERT runs for every proposed row, **before** the conflict check, because a trigger can
   change the key.
2. An inserted row runs AFTER INSERT.
3. An updated row runs BEFORE UPDATE and AFTER UPDATE. They see the stored row as OLD and the new row
   as NEW.
4. A skipped row (DO NOTHING, or a false WHERE) runs no further trigger.

All AFTER triggers run after the last write of the statement and after its foreign-key checks.

## Privileges

- `DO NOTHING` needs `INSERT` on the target table.
- `DO UPDATE` and `UPSERT` need `INSERT` **and** `UPDATE`. They are refused without `UPDATE` even
  when no proposed row conflicts, so the result does not depend on the data.
- `RETURNING` also needs `SELECT`; see [returning.md](returning.md).

## Concurrency

The statement finds each conflict **before** it writes. For each chunk of proposed rows it takes an
exclusive lock on every arbiter key, and only then reads the keys. This is necessary because there
are no savepoints: a duplicate-key error would end the transaction, so "insert, then catch the
duplicate" cannot work.

While the lock is held, no other transaction can write those keys:

| Case | Result |
|---|---|
| Two transactions upsert the same new key. | The second one meets the lock. By wait-die it fails with a retryable error (`CADB0502`, or `CADB0504` at the lock-wait deadline). Its retry finds the committed row and updates it or skips it. |
| A plain `INSERT` of a key that an upsert locked. | The insert waits, then fails with a retryable error. |
| An upsert of a key that an uncommitted plain `INSERT` wrote. | The upsert's lock meets the insert's write. The younger upsert fails with `CADB0502`, in both locking modes. |
| A row committed after the transaction began (Read Committed or Serializable). | The lookup reads the latest committed state, so the upsert finds the row and updates it. It never inserts a second row. |

The same holds in pessimistic and optimistic locking mode, at Read Committed and at Serializable.
An autocommit `INSERT … ON CONFLICT` is retried on these codes like any other autocommit statement;
see [serializable-retry-contract.md](serializable-retry-contract.md). `DuplicateUniqueKeyValue` is
not retried.

Each insert and each update counts against the per-transaction mutation limit. A skipped row counts
nothing. The locks are not mutations.

## Notes

- **Sequence values.** Values for sequence-backed defaults are reserved for the whole `VALUES` list
  before any row is checked. A row that `ON CONFLICT` skips or turns into an update still uses its
  value. PostgreSQL behaves the same way.
- **Branch databases.** A key that exists only in an ancestor conflicts exactly as a key the branch
  wrote. The update writes the branch's own copy and leaves the ancestor unchanged. A key the branch
  deleted does not conflict. See [database-branching.md](database-branching.md).
- **Transports.** The clause is SQL only: the `/execute-sql-*` endpoints and the gRPC `CamusSql`
  service. The typed row APIs (`/insert`, gRPC `InsertRow`) have no upsert form yet.

## Not supported

- `ON CONFLICT ON CONSTRAINT` for a CHECK or a foreign-key constraint. Only a unique index can be an
  arbiter.
- The name a `CONSTRAINT name PRIMARY KEY (…)` clause declares. Name the primary key as
  `<table>_pkey`, or give its columns.
- `INSERT … ON DUPLICATE KEY UPDATE` (MySQL) and `MERGE`.
- A table alias on the INSERT target (`INSERT INTO t AS x …`).

## Related

- [returning.md](returning.md) — `RETURNING`.
- [insert-select-and-ctas.md](insert-select-and-ctas.md) — `INSERT … SELECT`.
- [transactions-locking-and-isolation.md](transactions-locking-and-isolation.md) — locking modes and isolation.
- [sql-authentication.md](sql-authentication.md) — privileges.
