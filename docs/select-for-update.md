# Row-locking reads: `SELECT … FOR UPDATE`

A row-locking read returns rows and locks each returned row until the transaction ends. Use it when
you read a row and then decide what to write to it. Two common users are:

- ORM pessimistic locking (Rails `lock`, Django `select_for_update`).
- Job queues, where several workers each take the next free job:

```sql
BEGIN;
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
SELECT id FROM jobs WHERE state = 'ready' ORDER BY id LIMIT 10 FOR UPDATE SKIP LOCKED;
UPDATE jobs SET state = 'running' WHERE id IN (...);
COMMIT;
```

## Syntax

```sql
SELECT … FROM t [WHERE …] [ORDER BY …] [LIMIT n [OFFSET m]]
  FOR UPDATE [OF t] [NOWAIT | SKIP LOCKED];
```

- `FOR NO KEY UPDATE` is accepted as a synonym of `FOR UPDATE`.
- `OF t` must name the table in `FROM`, by its name or its alias.
- `FOR SHARE` and `FOR KEY SHARE` fail with `CADB0533` (`FeatureNotSupported`).
- The clause is accepted only at the end of a top-level `SELECT`, and at the end of the query of
  an `EXPLAIN` (see [EXPLAIN](#explain)). In a subquery, a derived table, a view body,
  `CREATE TABLE … AS` or `INSERT … SELECT` it is a syntax error.

## What is locked

Each row that the statement **returns** is locked, and only those rows. The lock is the exclusive row
lock that an `UPDATE` or a `DELETE` of the row takes. So:

- Another transaction cannot update, delete or lock the row until your transaction commits or rolls
  back.
- A plain `SELECT` of the row is not blocked.
- A later `UPDATE` or `DELETE` of the row in your own transaction does not wait. A second
  `FOR UPDATE` of the row in your transaction does not wait either.

The statement runs in this order:

1. Scan the table and apply the `WHERE` clause.
2. Sort (`ORDER BY`).
3. Lock the candidate rows, in pages, in sort order.
4. Read each locked row again, at its latest committed value.
5. Check the `WHERE` clause again on that read.
6. Apply `OFFSET` and `LIMIT`.

The values you get are the values from step 4, not from the scan. A row that another transaction
deleted or moved out of the `WHERE` clause before step 3 is not returned, and the next row takes its
place in the `LIMIT`.

Two notes:

- **The lock of a dropped row stays.** A row that step 5 drops is still locked until your transaction
  ends. The pages are sized to the rows that the `LIMIT` still needs, so this is a small number of rows.
- **`ORDER BY` order after a concurrent change.** A concurrent commit can change a sorted column of a
  row between step 2 and step 4. The row is then returned in its old position. PostgreSQL has the same
  behavior.

## When a row is held by another transaction

| Option | Result |
| --- | --- |
| none | The statement fails with `CADB0502` (`TransactionConflict`), as an `UPDATE` of the same row fails. The code is retryable. |
| `NOWAIT` | The statement fails with `CADB0555` (`LockNotAvailable`). |
| `SKIP LOCKED` | The row is left out of the result. It does not count toward `LIMIT` or `OFFSET`. |

In all three cases, a holder that is only finishing its commit (its intent waits for replication or
settlement) is not a held lock. The statement waits for it a short time, and then continues.

`CADB0555` is not in the automatic retry set: a retry is a wait, and `NOWAIT` asked not to wait. It
maps to HTTP 409 and gRPC `ABORTED`, with the code in the `camus-error-code` trailer. Any failed
statement ends its transaction, because CamusDB has no savepoints.

## Isolation levels and locking modes

- **Read Committed.** All three options work. This is the level for job queues.
- **Serializable.** `FOR UPDATE` and `NOWAIT` work. `SKIP LOCKED` fails with `CADB0533`. The reason:
  at Serializable, a scan takes a shared lock over the range it reads. Two workers that scan the same
  queue both hold that lock, so each worker's later `UPDATE` of its own job conflicts with the other
  worker's lock. That defeats `SKIP LOCKED`.
- **Serializable, full scans.** The shared range lock of the scan comes before the row locks. If the
  scanned range covers a row that another transaction holds, the scan fails at that lock with
  `CADB0502`, also with `NOWAIT`. A lookup by key goes directly to the row locks.
- **Optimistic transactions** (`SET TRANSACTION LOCKING OPTIMISTIC`) take no locks. The statement
  fails with `CADB0400`: "FOR UPDATE requires LOCKING PESSIMISTIC".
- **Read-only transactions** cannot own a lock. The statement fails with `CADB0400`.

## Autocommit

Outside an explicit transaction, the statement runs in its own short read-write transaction, as an
`INSERT … RETURNING` does. The locks are released when the statement ends. The rows are sent only
after that transaction commits. This is the same on REST (buffered and streaming), gRPC
`ExecuteQuery`, gRPC batch query ops and prepared statements.

The autocommit transaction uses the request's isolation level, or the server default
(`default_isolation_level`, Serializable unless changed). For `SKIP LOCKED` in autocommit, send the
request at Read Committed.

## What is not supported

These fail with `CADB0400`, because the rows they produce are not table rows:

- `DISTINCT`, `GROUP BY`, `HAVING` and aggregate functions.
- Joins, views, derived tables and table functions in `FROM`. A joined row does not carry the row id
  of each table it came from.
- `UNION`, `INTERSECT` and `EXCEPT`.
- `AS OF SYSTEM TIME`: a historical row cannot be locked.
- A materialized view.
- A `SELECT` without `FROM`.

## EXPLAIN

Every `EXPLAIN` form accepts the clause, as in PostgreSQL. The plan shows the lock step as
`lock_rows(for update[, nowait | , skip locked])`, below the `LIMIT` and above the sort.

- **Plain `EXPLAIN`** (also `LOGICAL` and `PHYSICAL`) plans the query and takes no lock. The
  statement checks and the privilege check apply. The transaction rules do not, so it also works in
  a read-only or optimistic transaction.
- **`EXPLAIN (ANALYZE)`** runs the statement. Every check applies, and the rows it returns stay
  locked until the transaction ends. `NOWAIT` fails it with `CADB0555`. In autocommit it runs in a
  writable transaction, as the statement does.

Under `ANALYZE`, the lock step reports three counters that PostgreSQL does not have: `locked=`,
`skipped-held=` and `recheck-dropped=`. See
[explain.md → The lock step under EXPLAIN ANALYZE](explain.md#the-lock-step-under-explain-analyze).

## Privileges

`FOR UPDATE` needs `SELECT` **and** `UPDATE` on the table, as in PostgreSQL. `EXPLAIN` of a locking
read needs the same two privileges, also in its plain form.

## Routing

Hash routing and key-range sharding behave the same way. Each row lock is sent to the leader of the
row's key, so a transaction on any node sees the same locks.

## Where this lives in the code

- Grammar and AST: `SQLParser/RowLockingAst.cs`, node types `SelectForUpdate` and `LockingClause`.
- Statement and transaction checks: `Commands/Executor/Controllers/Queries/RowLockingRules.cs`.
- Plan node: `LockRowsNode`, run by `QueryRowLocker`.
- Locks: `KvTableStore.LockAndReadRowsForLockingReadAsync` and
  `KvBatchWriter.AcquireRowLocksForLockingReadAsync`.

See also [transactions-locking-and-isolation.md](transactions-locking-and-isolation.md) §6.6.
