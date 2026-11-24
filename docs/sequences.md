# Sequences

A sequence is a named counter that hands out a number at a time. Use one when you want a short,
readable, ever-increasing value per row — an order number, an invoice number, a ticket id — where
`gen_uuid_v7()` would be unique but unreadable.

```sql
CREATE SEQUENCE order_no;

CREATE TABLE orders (
    id oid PRIMARY KEY,
    no int64 DEFAULT(nextval('order_no')),
    total float64
);

INSERT INTO orders (id, total) VALUES (gen_id(), 19.90);
SELECT no FROM orders;   -- 1
```

Behind the counter is Kahuna's sequencer: a durable, replicated, partition-leader-owned register.
That is where the four properties in [What will surprise you](#what-will-surprise-you) come from.
Read them before you build on a sequence value.

---

## Creating a sequence

```sql
CREATE SEQUENCE [IF NOT EXISTS] <name> [ <option> ... ]
```

| Option | Default | Meaning |
|---|---|---|
| `START [WITH] n` | `MINVALUE` | The first value the sequence issues. |
| `INCREMENT [BY] n` | `1` | The step between values. Must be positive. |
| `MINVALUE n` / `NO MINVALUE` | `1` | The lowest value the sequence may hold. |
| `MAXVALUE n` / `NO MAXVALUE` | none | The highest value it may issue. |
| `CACHE n` | `1` | How many values are reserved per durable commit. |
| `NO CYCLE` | — | Accepted, and the only behavior. See below. |

```sql
CREATE SEQUENCE invoice_no
    START WITH 1000
    INCREMENT BY 1
    MINVALUE 1000
    MAXVALUE 999999
    CACHE 1;
```

A sequence shares one namespace with tables and views, as it does in PostgreSQL. A name held by
any of the three is not available to the others.

### `CYCLE` is refused

`CYCLE` parses, and the statement returns `CADB0533` with this message.

```
CYCLE is not supported. A sequence value is guaranteed unique for the life of the sequence, and
wrapping the counter back to its minimum would reissue values that committed rows already hold.
Write NO CYCLE, or raise MAXVALUE.
```

The refusal is deliberate. Accepting the keyword and ignoring it would tell you the register wraps
while giving you one that does not, and you would find out from your data.

---

## Drawing values

| Function | What it does |
|---|---|
| `nextval('s')` | Advances the sequence and returns the new value. |
| `currval('s')` | The last value **this transaction** drew from `s`. |
| `lastval()` | The last value this transaction drew from any sequence. |
| `setval('s', n [, is_called])` | Moves the counter. See [`setval`](#setval). |

### Where a sequence call is allowed

Before a statement starts, the engine reserves the values it needs in **one** call. It does not make
one call per row. This means that an insert of ten thousand rows takes one round trip. It also means
that the engine has to know, before the statement starts, how many values it will draw.

A sequence call is allowed only in positions where that count is known.

- An `INSERT … VALUES` expression.
- A column `DEFAULT`, on `INSERT … VALUES` and on `INSERT … SELECT`.
- A `SELECT` that has no `FROM` clause.

In any other position, the statement returns `CADB0547`. Examples are a `WHERE` clause, an aggregate, a
subquery, an `ORDER BY`, a projection over a relation, a stored view body, a `CHECK` condition and,
today, an `UPDATE … SET` assignment.

`UPDATE … SET c = nextval('s')` is a gap in the implementation; the design does not rule it out. The matched-row count
is not known when the statement is bound, and the update path drains its matched rows in chunks.
Supporting it means reserving values per chunk. It returns the same message as the other unbounded
positions. This means that nobody discovers the gap from their data.

`COALESCE`, `IFNULL` and `NVL` stop at the first argument that is not NULL. A call after that argument
is not evaluated and draws no value. In the allowed positions, a call after a non-NULL literal or
parameter is not reserved. For example, `COALESCE(@p, nextval('s'))` draws a value only if `@p` is
NULL. A call after any other argument is reserved. That argument's value is known only after it is
evaluated. If the call is then skipped, the unused value is a gap: `COALESCE(abs(-1), nextval('s'))`
returns `1` and uses one value. In a position that is not allowed, the error comes when a row reaches
the call. For example, `SELECT COALESCE(n, nextval('s')) FROM t` returns `CADB0547` in a row where `n`
is NULL, and a row where `n` is not NULL does not fail.

### `currval` and `lastval` are scoped to the transaction

PostgreSQL scopes them to a backend connection. CamusDB does not have a connection to scope them to,
because the REST path is stateless. Defining "session" as the bearer token would make two unrelated
concurrent requests from one user share a value. That would be wrong in a way that is very hard to
debug.

They are therefore scoped to the **transaction**, which is a real boundary that no two callers share.

```sql
BEGIN;
INSERT INTO orders (id, total) VALUES (gen_id(), 19.90);
SELECT currval('order_no');   -- the value that row got
COMMIT;
```

Outside an explicit transaction, each statement is a separate transaction. This means that `currval`
in a later statement returns `CADB0546`.

```
currval('order_no') is not defined: this transaction has not drawn a value from that sequence yet.
```

A transaction remembers at most 64 distinct sequences. Past that, an entry is dropped, and `currval`
on a dropped one returns "not defined". It does not return a stale number, because an error is far
better here than a wrong answer. Which entry is dropped is unspecified. Tracking recency would cost a
second structure on each draw, to improve a case that is not realistic.

### `setval`

```sql
SELECT setval('order_no', 5000);          -- next nextval returns 5001
SELECT setval('order_no', 5000, false);   -- next nextval returns 5000
```

`setval` returns the value it was given. It does not return the value that comes next.

**It is allowed only in that shape**: by itself, as the complete projection of a `SELECT` that has no
`FROM` clause, and at most once per statement. Any other use returns `CADB0547`. For example, `setval`
cannot be nested in a larger expression or a `CASE`, and it cannot appear twice in one statement or
in an `INSERT … VALUES` row. The reason is the same in each case. `setval` moves the counter *before*
the statement is evaluated. A call in a branch that the statement does not take would already have
reset the sequence, irrevocably and invisibly.

**Moving a counter downwards is the dangerous direction.** It makes the sequence reissue values
that committed rows may already hold, and a unique index will then reject the insert. PostgreSQL
permits it and so does CamusDB. The value is not clamped. A clamp that nobody asked for would be a
surprise too. Know what the highest value in the column is before you move the counter below it.

**A `setval` takes about five seconds.** So does `ALTER SEQUENCE … RESTART`. The wait is
the guarantee: see [Moving a live counter costs one lease](#moving-a-live-counter-costs-one-lease).

---

## Identity columns

Two spellings declare a column that numbers itself. Both create a sequence, mark it **owned** by
the column, and set the column's default to draw from it.

```sql
CREATE TABLE invoices (id oid PRIMARY KEY, no serial, total float64);
CREATE TABLE tickets  (id oid PRIMARY KEY, n int64 GENERATED ALWAYS AS IDENTITY, subject string);
CREATE TABLE notes    (id oid PRIMARY KEY, n int64 GENERATED BY DEFAULT AS IDENTITY, body string);
```

`serial` is a declaration shorthand: the column is stored as `int64`. It does not have a 32-bit range. `bigserial`
is accepted as a synonym.

The generated sequence is named `{table}_{column}_seq`, with a numeric suffix if that name is
taken.

### `ALWAYS` versus `BY DEFAULT`

The two spellings exist for one difference: whether an `INSERT` may supply a value for the column.

- **`GENERATED ALWAYS AS IDENTITY`** returns an error for an `INSERT` that supplies a value for the
  column. The column's values can only ever come from its sequence. This includes the implicit
  all-columns form, `INSERT INTO t VALUES (…)`, which supplies a value for each column. Name the
  columns you mean. `OVERRIDING SYSTEM VALUE` (below) is the explicit way to write one anyway.
- **`GENERATED BY DEFAULT AS IDENTITY`**, and `serial`, accept a supplied value and write it. The
  sequence supplies a value if the column is omitted.

A supplied value does **not** advance the sequence, in either spelling. Load rows with explicit
values and the sequence will hand out numbers those rows already hold; `setval` past them
afterwards.

The typed row insert follows the same rules as `INSERT`. This applies to REST `/insert`, to gRPC
`InsertRow` and to the insert calls of the client libraries. A column left out of the row takes its
default. That default can be a constant or a function such as `gen_id()`. It can also be a `nextval`
default or the identity sequence. A `GENERATED ALWAYS` column supplied in the row returns an error. A
column supplied with a JSON `null` counts as a supplied NULL. It does not count as an omitted column.

#### `OVERRIDING SYSTEM VALUE` and `OVERRIDING USER VALUE`

An `INSERT` can change how identity columns treat the values it supplies. The clause goes after the
column list (or after the table name when there is none), before `VALUES` or the source `SELECT`.

```sql
INSERT INTO tickets (id, n, subject) OVERRIDING SYSTEM VALUE VALUES (gen_id(), 41, 'restored');
INSERT INTO tickets OVERRIDING SYSTEM VALUE VALUES (gen_id(), 42, 'restored');
INSERT INTO tickets (id, n, subject) OVERRIDING SYSTEM VALUE SELECT id, n, subject FROM old_tickets;
INSERT INTO notes (id, n, body) OVERRIDING USER VALUE VALUES (gen_id(), 999, 'n comes from the sequence');
```

- **`OVERRIDING SYSTEM VALUE`** lets the statement supply a value for a `GENERATED ALWAYS` column,
  which is otherwise refused. This is how a dump restores such a column: `camus-dump` writes it in
  the `INSERT` statements of a table that has one. Like any supplied value, an overriding value does
  not advance the sequence. Call `setval` afterwards to move the sequence past the loaded values.
- **`OVERRIDING USER VALUE`** discards the values the statement supplies for all identity columns
  (`ALWAYS`, `BY DEFAULT` and `serial`). Each of those columns draws from its sequence as if it had
  been omitted. A column whose default only calls `nextval` is not an identity column. The value
  supplied for it is written.

These are PostgreSQL's spellings and meanings. The words are case-insensitive, and none of
`OVERRIDING`, `SYSTEM` or `VALUE` becomes a reserved word. The clause works with `RETURNING`, in a
prepared statement and in an explicit transaction. The typed row insert does not support the clause.
It follows the rules of an `INSERT` that omits the clause.

### How identity columns and sequence defaults are shown

`SHOW CREATE TABLE` renders an identity column as its clause, after `NOT NULL`. It renders a
sequence default as a `nextval` call that names the sequence by its current name.

```sql
CREATE TABLE p (id int64 PRIMARY KEY, no int64 DEFAULT(nextval('s1')), n serial);
SHOW CREATE TABLE p;
-- CREATE TABLE `p` ( `id` INT64 NOT NULL, `no` INT64 NULL DEFAULT(nextval('s1')),
--   `n` INT64 NOT NULL GENERATED BY DEFAULT AS IDENTITY, PRIMARY KEY (`id`));
```

`serial` renders as `GENERATED BY DEFAULT AS IDENTITY`, which is what it declares. The identity
clause names no sequence: running the statement creates a new owned sequence, named as described
above. After `ALTER SEQUENCE s1 RENAME TO s2`, the default renders as `nextval('s2')`.

`SHOW COLUMNS` has the same six fields. A `nextval` default shows in `Default` as `nextval('s1')`. An
identity column shows its clause in `Extra`, and `NULL` in `Default`, the way MySQL shows an
auto-increment column.

### An owned sequence belongs to its column

- A bare `DROP SEQUENCE` on it is refused with `CADB0548`. Drop the column or the table instead.
- `DROP TABLE … FORCE` and `ALTER TABLE … DROP COLUMN` take it with them.
- `DROP TABLE` without `FORCE` is a *deferred* drop. The relation is retained and can be relinked.
  Its sequence is also retained. It goes when the garbage collector reclaims the orphan.
- `TRUNCATE … RESTART IDENTITY` returns it to its start value.

A free-standing sequence a column merely defaults from is **not** owned, and none of the above
applies to it: other relations may draw from the same counter.

**A column cannot default from a sequence another relation owns.** `DEFAULT nextval('t_n_seq')`
naming an identity sequence of `t` is refused with `CADB0548`: that sequence goes when `t` does,
and the default would be left pointing at a counter that is gone. Create a free-standing sequence
with `CREATE SEQUENCE` and default from that instead.

**An owned sequence whose relation has left the schema does not issue values**. A draw returns
`CADB0548`. That covers a deferred-dropped relation awaiting a `RELINK`, and one whose orphan has
already been reclaimed. No legitimate caller draws there. The only column that could is on the
relation that is no longer present.

---

## `TRUNCATE` and identity

```sql
TRUNCATE TABLE orders RESTART IDENTITY;   -- owned sequences return to their start value
TRUNCATE TABLE orders CONTINUE IDENTITY;  -- the default: sequences are left alone
TRUNCATE TABLE orders;                    -- the same as CONTINUE IDENTITY
```

`RESTART IDENTITY` restarts only the sequences **owned by** a column of the truncated relation. A
sequence a column merely defaults from is shared, and silently resetting a counter other tables
draw from would be a data-loss-shaped surprise. That is PostgreSQL's rule too.

The two halves of the statement can come apart. Emptying the relation is a committed schema change
and cannot be undone. If a reset fails afterwards, the error is returned to you and names the
sequences that were not restarted. Those sequences continue to climb. This is safe, because any value
they issue is above the values the relation held. Run `ALTER SEQUENCE … RESTART` on each of them to
finish the job.

---

## Changing a sequence

```sql
ALTER SEQUENCE order_no RESTART WITH 500;
ALTER SEQUENCE order_no RESTART;              -- back to the recorded START value
ALTER SEQUENCE order_no INCREMENT BY 10;
ALTER SEQUENCE order_no MAXVALUE 100000;
ALTER SEQUENCE order_no NO MAXVALUE;
ALTER SEQUENCE order_no RENAME TO order_number;
DROP SEQUENCE order_number;
```

**An `ALTER` is checked against the complete definition it produces.** Raising
`MINVALUE` above the recorded `START`, or lowering `MAXVALUE` below it, returns `CADB0545`
even though the statement named only one number. `CREATE SEQUENCE` follows the same rule.
To make such a change, change both values in one statement.

```sql
ALTER SEQUENCE order_no MINVALUE 100 START WITH 100;
```

A rename is metadata only. The counter is named after the sequence's immutable id. This means that
the values continue to climb from where they were, and a column default bound to the sequence
continues to work.

`ALTER SEQUENCE … RESTART` is not allowed in an explicit transaction. Moving a counter is
irrevocable, and a later `ROLLBACK` could not undo it.

---

## Inspecting sequences

```sql
SHOW SEQUENCES;
SHOW SEQUENCES LIKE 'order%';
SHOW CREATE SEQUENCE order_no;
COMMENT ON SEQUENCE order_no IS 'one per customer order';
```

`SHOW SEQUENCES` shows the name, the sequence's position, the start value, increment, minimum,
maximum, cache, owning column and comment.

### Reading the `reserved_upto` column

That column is the sequence's position, and **it is a ceiling**. It is not the last value issued.

```sql
CREATE SEQUENCE s CACHE 1000;
SHOW SEQUENCES LIKE 's';        -- reserved_upto is NULL: nothing has been drawn yet

SELECT nextval('s');            -- 1
SELECT nextval('s');            -- 2
SHOW SEQUENCES LIKE 's';        -- reserved_upto is 1000, not 2
```

Two values were issued, out of a thousand reserved to issue them from. The number means **"no
value above this has been issued"**. It does not mean "this value was issued". That is why the column
is not called `last_value`. A name like that would be wrong by up to a full cache block, and readers
build on it. Under the default `CACHE 1` the two numbers are the same, which is exactly what makes the
distinction easy to forget.

This has three consequences.

- **`NULL` means no value has been drawn** since the sequence was created or last moved. The
  counter is at its seed, and the seed is one increment below the first value the sequence will
  issue. Showing the seed would invite exactly the misreading above.
- **With `CACHE 1`, the default, the column is exact.** Each value is reserved durably before it is
  handed out. A sequence that asks for a larger cache gives up that exactness.
- **Do not compare two readings** to decide whether anything changed. Two readings can be equal
  across hundreds of issued values, and can differ by a thousand across one. Use `currval` for a
  value that was actually issued.

Reading the column costs a routed call per sequence. The listing reads them in bounded parallel
batches. If a sequence's partition momentarily has no leader, its row shows `NULL`, and the rest of
the listing succeeds.

---

## Privileges

| Statement | Privilege, on the database |
|---|---|
| `CREATE SEQUENCE` | `CreateTable` |
| `DROP SEQUENCE` | `Drop` |
| `ALTER SEQUENCE`, `COMMENT ON SEQUENCE` | `Alter` |
| `SHOW SEQUENCES`, `SHOW CREATE SEQUENCE` | `Select` |
| `nextval`, `setval` | `Update` |
| `currval`, `lastval` | `Select` |

The statements reuse existing privilege bits. A new bit was not added. `ALL PRIVILEGES` is expanded
to a stored mask at grant time. This means that a new bit would not be picked up by a grant that was
already issued. An account that an operator believes has all privileges would be quietly denied until
the grants were re-issued.

---

## What will surprise you

These four properties are all consequences of how the counter works. Each one is reported as a bug
by someone who has not read this section.

### 1. Gaps are normal above `CACHE 1`

The node that owns a sequence reserves a **block** of values with one durable commit. It then hands
them out from memory, and this generates no storage traffic. Whatever is left of that block is abandoned
when the node restarts, when the sequence's partition changes owner, or when the sequence is
evicted from memory.

The default is `CACHE 1`, as in PostgreSQL. A sequence with the default cache has no reserved values
to abandon. This means that it does not skip values.

```sql
CREATE SEQUENCE invoice_no;   -- 1, 2, 3 ... restart ... 4
```

The cost is one Raft commit, with its fsync, per value drawn. That is a large throughput
difference on an insert path that draws a value per row. If you want the throughput, ask for it with
`CACHE`, and accept the gaps that come with it.

```sql
CREATE SEQUENCE event_no CACHE 1000;   -- 1, 2, 3 ... restart ... 1001
```

The values are unique, but they are not consecutive. `CACHE` makes exactly the same trade
in PostgreSQL.

A sequence created before this default was introduced did not record a cache size. It uses the
node-wide block size. `SHOW SEQUENCES` prints the size it actually uses, and
`ALTER SEQUENCE <name> CACHE 1` moves it.

### 2. A sequence value is not an insert order and not a sort key

For up to five seconds after a partition changes owner, a former owner that has not yet learned it
lost the partition can continue to drain the window it already reserved. At the same time, the new
owner issues higher values. During that window a value handed out **later** can be numerically **lower**.

The values stay unique. Only their order does not hold. If you sort by a sequence column and get a
surprising order, this is the cause. Order by a timestamp, or by the row id.

### 3. An advance does not roll back

A rolled-back transaction consumes the values it drew. These values are not reissued.

```sql
BEGIN;
INSERT INTO orders (id, total) VALUES (gen_id(), 1.0);   -- takes 4
ROLLBACK;
INSERT INTO orders (id, total) VALUES (gen_id(), 1.0);   -- takes 5, not 4
```

This is PostgreSQL's behavior and the only workable one. Rolling an advance back would mean one of two things. The sequence's partition would be locked for
the life of each writing transaction, or values would be reissued. Reissuing values would break the one
guarantee that the feature rests on.

The same applies to a retry. A serializable conflict (`CADB0502`, or `CADB0504` after the batched
lock-wait deadline) retried to success leaves a row carrying a **higher** number than the first
attempt drew.

### 4. The reported value is a ceiling, not the last value issued

Kahuna returns a sequence's **reserved high-water mark**. After four `nextval` calls on a fresh
sequence declared `CACHE 1000`, that number is 1000. Four values were issued, out of a thousand
reserved to issue them from. Under the default `CACHE 1` it reads 4. No values are held back.

That is the number `SHOW SEQUENCES` prints in its `reserved_upto` column. It is also why there is no
`last_value` column. Publishing a ceiling under that name would be wrong by up to a full block, and
readers build on it. See [Reading the `reserved_upto` column](#reading-the-reserved_upto-column).

To learn a value that was actually issued, use `currval` on a transaction that drew one. Do not
compare two readings of a sequence to decide whether anything changed: two readings can be equal
across hundreds of issued values, and can differ by a thousand across one.

---

## Moving a live counter costs one lease

`setval`, `ALTER SEQUENCE … RESTART` and `TRUNCATE … RESTART IDENTITY` all wait about five seconds
before they return success. That interval is the storage layer's block lease, and the wait is the
guarantee.

Values from a reserved block are served from memory. This means that a node that lost the sequence's
partition, and did not notice, continues to hand out values from its reserved window. For one lease after
the record is rewritten, there would otherwise be two live streams. One is the replaced incarnation's
window on that node, and the other is the new value on the owner. Kahuna closes both sides. It
withholds success until no stale window can be served, and for the same interval, it does not allow
allocations from the new value. When your statement returns, the move is true on all nodes.

These statements are operator actions, off the hot path. A bounded delay is the right price for
them. **A client or proxy deadline shorter than five seconds turns a correct `setval` into a
timeout.**

---

## Lifecycle notes

**Dropping a database** removes its sequences' counters. A prefix purge cannot reach the counters,
because they are stored outside the database's keyspaces. The dropper reads the catalog to learn
which counters the database owns before it deletes that catalog.

**Branching a database** ([database branching](database-branching.md)) gives the branch separate
counters, each seeded at the source's reserved ceiling. Seeding from the ceiling is correct. The
ceiling is above all values that the source issued. This means that the branch cannot collide with
the rows it inherited. The cost is a gap, and a gap is allowed.

**Backups** ([backups and point-in-time recovery](backups-and-point-in-time-recovery.md)) carry
sequences. A sequence's record is an ordinary replicated key-value entry in the node's store, and a
backup covers the complete node. This means that a restore brings the counters back with the rows.
They are restored at the reserved ceiling, which is above all values in the backup's rows. No reseeding step is needed.

---

## Error codes

| Code | Meaning |
|---|---|
| `CADB0542` | The name is already taken by a sequence, a table or a view. |
| `CADB0543` | The named sequence does not exist. |
| `CADB0544` | The sequence cannot issue another value: the next one would pass `MAXVALUE`. |
| `CADB0545` | The definition cannot hold — a non-positive increment, `CACHE` below 1, `MAXVALUE` below `MINVALUE`, `START` outside the two. |
| `CADB0546` | `currval` or `lastval` called before this transaction drew a value. |
| `CADB0547` | A sequence call was written where the engine cannot bound how many values it would draw. |
| `CADB0548` | The sequence is owned by an identity column, or a column default draws from it. |
| `CADB0535` | The sequence's partition had no confirmed leader at any point in the retry window. Transient; re-issue the statement. |

---

## See also

- [Data types](data-types.md) — the range of `int64`, and why a sequence column uses it.
- [Database branching](database-branching.md) — what a fork copies, and what it recreates.
- [Backups and point-in-time recovery](backups-and-point-in-time-recovery.md).
- [Configuration](configuration.md) — `sequence_retry_budget_ms`, the wall-clock budget a sequence
  call rides a leadership change out on.
