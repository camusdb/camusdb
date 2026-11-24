# `TRUNCATE TABLE`

`TRUNCATE` empties a base table. It does not delete rows one by one. It replaces the physical
key-space that stores the table's rows and index entries. This means that the work it does is
independent of the number of rows in the table.

```sql
TRUNCATE TABLE orders;
TRUNCATE orders;             -- the TABLE keyword is optional
```

The table itself survives. Its name, its id, its columns, its indexes, its constraints, its settings,
its comment, its grants and its schema version are all exactly what they were. Only its **contents
generation** moves.

---

## At a glance

```sql
-- Empty a table of any size.
TRUNCATE TABLE orders;

-- The previous contents are retained and recoverable for the retention window.
SHOW ORPHAN TABLES;
-- id | kind             | former_name | dropped_at | expires_at
-- A0 | retired contents | orders      | …          | …

-- Bring the previous contents back as a separate table.
CREATE TABLE orders_before_truncate RELINK TO 'A0';
```

---

## What the statement accepts

- Exactly **one** base table. Several tables in one statement are not supported.
- The `TABLE` keyword is optional.
- `RESTART IDENTITY` returns each sequence that a column of the table owns to its start value.
  `CONTINUE IDENTITY`, the default, leaves the sequences alone. See [sequences.md](sequences.md).
- There is no `CASCADE`: a table that a foreign key references cannot be truncated (see below).

`TRUNCATE` returns an error for the targets in this table.

| Target | Error | What to use instead |
|--------|-------|---------------------|
| A table that another table's foreign key references | `CADB0530` (`DependentObjectsExist`) | Truncate or drop the child first, or drop its constraint. See [foreign-keys.md](foreign-keys.md). |
| A materialized view | `CADB0525` (`ViewNotUpdatable`) | `REFRESH MATERIALIZED VIEW … WITH NO DATA` |
| A plain view | `CADB0525` (`ViewNotUpdatable`) | Truncate the underlying table |
| A table that does not exist | `CADB0002` (`TableDoesntExist`) | — |

---

## Performance

The statement is **constant in row and index-entry cardinality**. A table with one row and a table
with a billion rows do the same amount of row work: none. No row is read or deleted, and no index
entry is touched.

Wall-clock time is not constant. The statement has to wait for each of these operations.

- the exclusive fence over the table's entire row key-space (which waits for conflicting writers);
- the replicated schema entry to commit and to be acknowledged by all nodes;
- the metadata checkpoint, whose cost grows with the table's indexes and stored column layouts.

Physically reclaiming the retired rows is asynchronous and *is* proportional to the row count. It
happens in the background collector, long after the statement returned.

This table compares the chosen strategy with two alternatives that CamusDB does **not** use.

| Strategy | Data work | Atomic | Why not |
|----------|-----------|--------|---------|
| Contents generation swap | none | yes | **chosen** |
| Chunked `DELETE` | proportional to rows and index entries | no | a partial failure leaves the table half-empty |
| Single-transaction `DELETE` | proportional to rows and index entries | yes | exceeds the per-transaction mutation limit on any useful table |

---

## Privileges

`TRUNCATE` requires **both** `DELETE` and `DROP` on the target table. It removes all rows (a
`DELETE`-shaped effect) and it retires a key-space (a `DROP`-shaped effect). One of the two
privileges is not enough.

The two privileges are checked independently. They may come from separate grants.

```sql
GRANT DELETE ON shop.orders TO alice;
GRANT DROP   ON shop.orders TO alice;   -- a second statement is fine
```

A superuser, a database-wide grant (`shop.*`) and a global grant all satisfy the check as usual.

---

## Transactions

`TRUNCATE` uses a separate internal transaction. It **is not allowed in an explicit transaction**,
and returns `CADB0538` (`StatementNotAllowedInTransaction`) there.

```sql
BEGIN;
TRUNCATE TABLE orders;   -- CADB0538
ROLLBACK;
```

`TRUNCATE` commits a replicated schema entry, and a
`ROLLBACK` of your transaction cannot undo that entry. Allowing the statement in a transaction
would promise rollback semantics the engine cannot deliver. Commit or roll back your transaction
before you truncate.

Once the schema entry commits, the truncate has happened cluster-wide. A failure *after* that point —
finalizing the internal transaction, say — is logged, and the statement returns success. Returning a
rollback error would tell you that the table contains rows that it no longer contains.

---

## Concurrent readers and writers

Before anything is proposed, the statement takes an **exclusive range lock over the table's entire
row key-space**. That lock makes the swap atomic with respect to data transactions, as follows.

- A writer — optimistic or pessimistic — that staged a row into the old key-space **before** the lock
  was taken is aborted at commit. It can't receive success for a row that would be stored only in a
  key-space that no reader uses.
- A writer that arrives **while** the lock is held waits, or is asked to retry. When it proceeds it
  binds the new, empty key-space.
- A reader that bound the old contents **before** the cut may finish against its snapshot. A
  statement bound after the truncate is acknowledged sees the new, empty contents.

Two concurrent `TRUNCATE`s on the same table serialize through the schema leader. Each one that
succeeds creates a separate retired generation. One may lose the race and return `CADB0534`
(`ConcurrentSchemaChange`). The losing statement made no change, and the error is retryable.

Retryable conflicts follow the usual contract; see
[Transactions, locking and isolation](transactions-locking-and-isolation.md).

---

## Recovering the previous contents

A truncate does not destroy anything. The key-space the table stopped reading is kept as
**retired contents** — the same machinery that makes `DROP TABLE` recoverable, described in
[Recovering dropped databases and tables](recover-dropped-objects.md).

```sql
SHOW ORPHAN TABLES;
```

| Column | Meaning |
|--------|---------|
| `id` | The retired key-space's id — the value `RELINK TO` takes. |
| `kind` | `retired contents` for a truncate, `dropped table` for a drop. |
| `former_name` | The table the contents belonged to. For retired contents, that table is the **live table** that was truncated. |
| `dropped_at` | When the contents were retired. |
| `expires_at` | When the background collector may reclaim them. |

Recovering publishes the retained rows as a **separate, new table**.

```sql
CREATE TABLE orders_before_truncate RELINK TO 'A0';
```

The recovered table gets a **fresh relation id** and reads the retired key-space. It needs a fresh id. On a
table's first truncate, the retired key-space is named by the id of the table, which is *live*.
Reusing that id would create a second name for a live relation.

The original table is untouched by the recovery. Its id and name are the same, and it contains
whatever was written into it after the truncate.

Once the retention window (`orphan_retention_ms`) elapses, the background collector purges the
retired rows together with their index entries and metadata. After that, recovery is no longer possible.

---

## Time travel

A time-travel read of a truncated table **returns an error** for any snapshot earlier than the moment
the current contents began.

```sql
TRUNCATE TABLE orders;
SELECT * FROM orders AS OF SYSTEM TIME '-10s';
-- CADB0537: … its contents were replaced at … and the rows it held before that point are no
--           longer reachable through this table.
```

The old rows are on disk, but they are in a key-space the live table can no longer name. The read
would scan the new, empty generation and answer "no rows". For a point in time when the table was full, that
answer would look like a correct empty answer. This is why the read returns an error.

- A snapshot **exactly at** the cut is allowed, and observes the new, empty contents.
- A snapshot **after** the cut behaves normally.
- After several truncates, only the latest cut is known. A snapshot before that cut returns an error.
- A table that has not been truncated is unaffected.

To read the old rows, recover them with `RELINK` and query the recovered table. See
[Time-travel reads](time-travel-reads.md).

---

## Branch databases

In a [branch (copy-on-write) database](database-branching.md) a table's rows are the branch's own
overlay merged with the rows it inherits from its ancestors. All levels are addressed by the same
storage id, which gives truncate a simple and total meaning in a branch.

- **Truncating in a branch** assigns the branch a new storage id. The branch's view becomes empty:
  its overlay *and* its inherited rows disappear from that branch. Neither is deleted. The overlay
  becomes retired contents, and the ancestor's rows are not touched at all.
- **Recovering in a branch** reconstructs the branch's pre-truncate merged view, overlay plus
  inherited rows, under a new table name.
- **Reclaiming** is scoped to the database id of the branch. It never deletes an ancestor's key.
- **Truncating the source** after a branch was forked does not rewrite the descendant's copied
  schema. The descendant reads its forked contents.

---

## Dependent views

- A **plain view** doesn't store rows. It reads the emptied table immediately.
- A **materialized view** contains its previously refreshed rows and is now stale.

That matches how CamusDB already treats `INSERT`, `UPDATE` and `DELETE`: no base-table mutation
invalidates a dependent materialized view. Run `REFRESH` when you want the view to agree with the
table.

```sql
TRUNCATE TABLE orders;
REFRESH MATERIALIZED VIEW orders_by_day;   -- now empty too
```

---

## Statistics

The optimizer's statistics are keyed by the table's identity, which a truncate deliberately preserves.
They are stamped with the contents generation they describe. After a truncate, the previous
distribution is ignored: `SHOW STATISTICS` and the planner treat the new generation as not yet
measured. Run `ANALYZE` when you have repopulated the table.

---

## Row-level TTL

A TTL run in flight when a truncate lands becomes inert immediately. Each run records the storage
generation it was planned against, and both the scheduler and each worker span re-check it. This
means that a worker can't delete from the retired generation, which is recoverable. It also can't
delete from the new one, which its span plan didn't describe. The stale run's records are cleaned up
in the background.

---

## Error codes

| Code | Name | When |
|------|------|------|
| `CADB0002` | `TableDoesntExist` | The named table does not exist. |
| `CADB0517` | `InsufficientPrivilege` | The caller lacks `DELETE` or `DROP` on the table. |
| `CADB0525` | `ViewNotUpdatable` | The target is a view or a materialized view. |
| `CADB0534` | `ConcurrentSchemaChange` | Another contents change won the race. Retryable. |
| `CADB0537` | `SnapshotPrecedesContentsGeneration` | A time-travel read named a point before the current contents began. |
| `CADB0538` | `StatementNotAllowedInTransaction` | `TRUNCATE` was issued in an explicit transaction. `ALTER TABLE ... REWRITE STORAGE` returns the same error. |

---

## Not supported

- `TRUNCATE a, b, c` — several tables in one statement.
- `CASCADE` — truncating a table that a foreign key references returns `CADB0530`. The child table,
  and a table that references only itself, can be truncated.
- Automatically refreshing or invalidating a dependent materialized view.
- Reading a snapshot from before the current contents generation.
