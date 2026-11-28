# Row-locking reads: `SELECT … FOR UPDATE`

A row-locking read returns rows and locks each returned row until the transaction ends. Use it when
you read a row and then decide what to write to it. ORM pessimistic locking and job queues are two
common uses.

- ORM pessimistic locking (Rails `lock`, Django `select_for_update`).
- Job queues, where several workers each take the next free job. The following transaction takes
  up to 10 free jobs.

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
- `FOR SHARE` and `FOR KEY SHARE` return `CADB0533` (`FeatureNotSupported`).
- The clause is accepted only at the end of a top-level `SELECT`, and at the end of the query of
  an `EXPLAIN` (see [EXPLAIN](#explain)). In a subquery, a derived table, a view body,
  `CREATE TABLE … AS` or `INSERT … SELECT` it is a syntax error.

## What is locked

Each row that the statement **returns** is locked, and only those rows. The lock is the exclusive row
lock that an `UPDATE` or a `DELETE` of the row takes. This has three effects.

- Another transaction cannot update, delete or lock the row until your transaction commits or rolls
  back.
- An ordinary `SELECT` of the row is not blocked.
- A later `UPDATE` or `DELETE` of the row in your own transaction does not wait. A second
  `FOR UPDATE` of the row in your transaction does not wait either.

The statement is executed in the following order.

1. Scan the table and apply the `WHERE` clause.
2. Sort (`ORDER BY`).
3. Lock the candidate rows, in pages, in sort order.
4. Read each locked row again, at its latest committed value.
5. Check the `WHERE` clause again on that read.
6. Apply `OFFSET` and `LIMIT`.

The values you get are the values from step 4. The values from the scan are not used. A row that another transaction
deleted or moved out of the `WHERE` clause before step 3 is not returned, and the next row takes its
place in the `LIMIT`.

Note the following two effects of this order.

- **The lock of a dropped row is not released.** A row that step 5 drops is locked until your
  transaction ends. The pages are sized to the rows that the `LIMIT` needs at that point. This means
  that only a small number of rows are affected.
- **`ORDER BY` order after a concurrent change.** A concurrent commit can change a sorted column of a
  row between step 2 and step 4. The row is then returned in its old position. PostgreSQL has the same
  behavior.

## When a row is held by another transaction

| Option | Result |
| --- | --- |
| none | The statement waits for the holder, as an `UPDATE` of the same row does (see below). When it may not wait, or the holder keeps the row past `lock_wait_deadline_ms`, it returns `CADB0502` (`TransactionConflict`). The code is retryable. |
| `NOWAIT` | The statement returns `CADB0555` (`LockNotAvailable`) at once. |
| `SKIP LOCKED` | The row is left out of the result at once. It does not count toward `LIMIT` or `OFFSET`. |

With no option, the wait follows the wait-die rule of every row lock (see
[transactions-locking-and-isolation.md](transactions-locking-and-isolation.md) §6.1):

- A transaction that started before the holder waits, for at most `lock_wait_deadline_ms` (500 ms by
  default).
- A transaction that holds no lock and no write yet also waits, whatever its age.
- A transaction that started after the holder and already holds a lock returns `CADB0502` at once. A
  row that an earlier `FOR UPDATE` of the same transaction locked counts as held.

When the holder ends in time, the statement locks the row, reads it again and checks the `WHERE`
clause on that read, so a row the holder changed or deleted is handled as in step 5 above.

In all three cases, a holder that is only finishing its commit (its intent waits for replication or
settlement) is not a held lock. The statement waits for it a short time, and then continues.

`CADB0555` is not in the automatic retry set: a retry is a wait, and `NOWAIT` asked not to wait. It
maps to HTTP 409 and gRPC `ABORTED`, with the code in the `camus-error-code` trailer. Any failed
statement ends its transaction. CamusDB does not support savepoints.

## Isolation levels and locking modes

- **Read Committed.** All three options work. This is the level for job queues.
- **Serializable.** `FOR UPDATE` and `NOWAIT` work. `SKIP LOCKED` returns `CADB0533`.
  At Serializable, a scan takes a shared lock over the range it reads. Two workers that scan the same
  queue both get that lock. As a result, the later `UPDATE` that each worker makes to the job it took
  conflicts with the other worker's lock. That defeats `SKIP LOCKED`.
- **Serializable, full scans.** The shared range lock of the scan comes before the row locks. If the
  scanned range covers a row that another transaction has locked, the scan meets the holder at that
  lock and waits by the same rule. If the holder does not end in time, the scan returns `CADB0502`,
  also with `NOWAIT`. A lookup by key goes directly to the row locks.
- **Optimistic transactions** (`SET TRANSACTION LOCKING OPTIMISTIC`) take no locks. The statement
  returns `CADB0400` with the message "FOR UPDATE requires LOCKING PESSIMISTIC".
- **Read-only transactions** cannot own a lock. The statement returns `CADB0400`.

## Autocommit

Outside an explicit transaction, the statement is executed in a separate short read-write
transaction, as an `INSERT … RETURNING` is. The locks are released when the statement ends. The rows are sent only
after that transaction commits. This is the same on REST (buffered and streaming), gRPC
`ExecuteQuery`, gRPC batch query ops and prepared statements.

The autocommit transaction uses the request's isolation level, or the server default
(`default_isolation_level`, Serializable unless changed). For `SKIP LOCKED` in autocommit, send the
request at Read Committed.

## What is not supported

The following forms return `CADB0400`. The rows they produce are not table rows.

- `DISTINCT`, `GROUP BY`, `HAVING` and aggregate functions.
- Joins, views, derived tables and table functions in `FROM`. A joined row does not carry the row id
  of each table it came from.
- `UNION`, `INTERSECT` and `EXCEPT`.
- `AS OF SYSTEM TIME`: a historical row cannot be locked.
- A materialized view.
- A `SELECT` that does not have a `FROM` clause.

## EXPLAIN

All `EXPLAIN` forms allow the clause, as in PostgreSQL. The plan shows the lock step as
`lock_rows(for update[, nowait | , skip locked])`, below the `LIMIT` and above the sort.

- **Plain `EXPLAIN`** (also `LOGICAL` and `PHYSICAL`) plans the query and takes no lock. The
  statement checks and the privilege check apply. The transaction rules do not apply. This means
  that it also works in a read-only or optimistic transaction.
- **`EXPLAIN (ANALYZE)`** executes the statement. All checks apply, and the rows it returns are
  locked until the transaction ends. With `NOWAIT`, it can return `CADB0555`. In autocommit it is
  executed in a writable transaction, as the statement is.

Under `ANALYZE`, the lock step shows three counters that PostgreSQL does not have: `locked=`,
`skipped-held=` and `recheck-dropped=`. See
[explain.md → The lock step under EXPLAIN ANALYZE](explain.md#the-lock-step-under-explain-analyze).

## Privileges

`FOR UPDATE` needs `SELECT` **and** `UPDATE` on the table, as in PostgreSQL. `EXPLAIN` of a locking
read needs the same two privileges, also when it is not `ANALYZE`.

## Routing

Hash routing and key-range sharding behave the same way. Each row lock is sent to the leader of the
row's key. This means that a transaction on any node sees the same locks.

## Where this lives in the code

- Grammar and AST: `SQLParser/RowLockingAst.cs`, node types `SelectForUpdate` and `LockingClause`.
- Statement and transaction checks: `Commands/Executor/Controllers/Queries/RowLockingRules.cs`.
- Plan node: `LockRowsNode`, run by `QueryRowLocker`.
- Locks: `KvTableStore.LockAndReadRowsForLockingReadAsync` and
  `KvBatchWriter.AcquireRowLocksForLockingReadAsync`.

See also [transactions-locking-and-isolation.md](transactions-locking-and-isolation.md) §6.6.
