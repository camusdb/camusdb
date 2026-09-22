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
That is where the four properties in [What will surprise you](#what-will-surprise-you) come from,
and they are the part of this page to read before you build on a sequence value.

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

`CYCLE` parses and is rejected with `CADB0533`:

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

The engine reserves the values a statement needs in **one** call before the statement runs, rather
than one call per row. That is what keeps an insert of ten thousand rows to one round trip instead
of ten thousand — and it means the engine has to know, before the statement starts, how many values
it will draw.

So a sequence call is accepted only where that count is knowable:

- an `INSERT … VALUES` expression;
- a column `DEFAULT`, on `INSERT … VALUES` and on `INSERT … SELECT`;
- a `SELECT` with no `FROM` clause.

Everywhere else it is refused with `CADB0547`: a `WHERE` clause, an aggregate, a subquery, an
`ORDER BY`, a projection over a relation, a stored view body, a `CHECK` condition — and, today, an
`UPDATE … SET` assignment.

`UPDATE … SET c = nextval('s')` is a real gap rather than a design position. The matched-row count
is not known when the statement is bound, and the update path drains its matched rows in chunks, so
supporting it means reserving per chunk. It is refused with the same message as every other
unbounded position, so nobody discovers the gap from their data.

### `currval` and `lastval` are scoped to the transaction

PostgreSQL scopes them to a backend connection. CamusDB has no connection to scope them to: the
REST path is stateless, and defining "session" as the bearer token would make two unrelated
concurrent requests from one user share a value — wrong in a way that is very hard to debug.

They are therefore scoped to the **transaction**, which is a real boundary no two callers share:

```sql
BEGIN;
INSERT INTO orders (id, total) VALUES (gen_id(), 19.90);
SELECT currval('order_no');   -- the value that row got
COMMIT;
```

Outside an explicit transaction each statement is its own transaction, so `currval` in a later
statement reports `CADB0546`:

```
currval('order_no') is not defined: this transaction has not drawn a value from that sequence yet.
```

A transaction remembers at most 64 distinct sequences. Past that an entry is dropped, and `currval`
on a dropped one reports "not defined" rather than a stale number — a refusal being far better here
than a wrong answer. Which entry goes is unspecified: tracking recency would cost a second
structure on every draw, to improve a case that has no realistic shape.

### `setval`

```sql
SELECT setval('order_no', 5000);          -- next nextval returns 5001
SELECT setval('order_no', 5000, false);   -- next nextval returns 5000
```

`setval` returns the value it was given, not the value that comes next.

**It is accepted only in that shape**: on its own, as a whole projection of a `SELECT` with no
`FROM` clause, at most one per statement. Anything else is refused with `CADB0547` — nested in a
larger expression, inside a `CASE`, twice in one statement, or in an `INSERT … VALUES` row. The
reason is the same in each case: `setval` moves the counter *before* the statement is evaluated,
so a call in a branch the statement never takes would still have reset the sequence, irrevocably
and invisibly.

**Moving a counter downwards is the dangerous direction.** It makes the sequence reissue values
that committed rows may already hold, and a unique index will then reject the insert. PostgreSQL
permits it and so does CamusDB; the value is not clamped, because a clamp nobody asked for is its
own surprise. Know what the highest value in the column is before you move the counter below it.

**A `setval` takes about five seconds.** So does `ALTER SEQUENCE … RESTART`. That is not slowness,
it is the guarantee: see [Moving a live counter costs one lease](#moving-a-live-counter-costs-one-lease).

---

## Identity columns

Two spellings declare a column that numbers itself. Both create a sequence, mark it **owned** by
the column, and set the column's default to draw from it.

```sql
CREATE TABLE invoices (id oid PRIMARY KEY, no serial, total float64);
CREATE TABLE tickets  (id oid PRIMARY KEY, n int64 GENERATED ALWAYS AS IDENTITY, subject string);
CREATE TABLE notes    (id oid PRIMARY KEY, n int64 GENERATED BY DEFAULT AS IDENTITY, body string);
```

`serial` is a declaration shorthand, not a storage type: the column is `int64`. `bigserial` is
accepted as a synonym.

The generated sequence is named `{table}_{column}_seq`, with a numeric suffix if that name is
taken.

### `ALWAYS` versus `BY DEFAULT`

That difference is the whole reason both spellings exist.

- **`GENERATED ALWAYS AS IDENTITY`** refuses an `INSERT` that supplies a value for the column. The
  column's values can only ever come from its sequence. Note that this includes the implicit
  all-columns form, `INSERT INTO t VALUES (…)`, which supplies a value for every column — name the
  columns you mean.
- **`GENERATED BY DEFAULT AS IDENTITY`**, and `serial`, accept a supplied value and write it. The
  sequence supplies one only when the column is omitted.

A supplied value does **not** advance the sequence, in either spelling. Load rows with explicit
values and the sequence will hand out numbers those rows already hold; `setval` past them
afterwards.

### An owned sequence belongs to its column

- A bare `DROP SEQUENCE` on it is refused with `CADB0548`. Drop the column or the table instead.
- `DROP TABLE … FORCE` and `ALTER TABLE … DROP COLUMN` take it with them.
- A plain `DROP TABLE` is a *deferred* drop: the relation is retained and can be relinked, so its
  sequence is kept too. It goes when the garbage collector reclaims the orphan.
- `TRUNCATE … RESTART IDENTITY` returns it to its start value.

A free-standing sequence a column merely defaults from is **not** owned, and none of the above
applies to it: other relations may draw from the same counter.

**A column cannot default from a sequence another relation owns.** `DEFAULT nextval('t_n_seq')`
naming an identity sequence of `t` is refused with `CADB0548`: that sequence goes when `t` does,
and the default would be left pointing at a counter that is gone. Create a free-standing sequence
with `CREATE SEQUENCE` and default from that instead.

**An owned sequence whose relation has left the schema refuses to issue**, also with `CADB0548`.
That covers a deferred-dropped relation awaiting a `RELINK`, and one whose orphan has already been
reclaimed. Nothing legitimate draws there — the only column that could is on the relation that is
no longer present.

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
and cannot be undone; a reset that fails afterwards is reported rather than swallowed, naming the
sequences that were not restarted. They keep climbing, which is safe — every value they issue is
above anything the relation held — and `ALTER SEQUENCE … RESTART` on each finishes the job.

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

**An `ALTER` is checked against the definition it produces, not the values it names.** Raising
`MINVALUE` above the recorded `START`, or lowering `MAXVALUE` below it, is refused with `CADB0545`
even though the statement named only one number — the same rule `CREATE SEQUENCE` is held to.
Change both together:

```sql
ALTER SEQUENCE order_no MINVALUE 100 START WITH 100;
```

A rename is metadata only. The counter is named after the sequence's immutable id, so the values
keep climbing from where they were, and a column default bound to the sequence keeps working.

`ALTER SEQUENCE … RESTART` cannot run inside an explicit transaction: moving a counter is
irrevocable, and a later `ROLLBACK` could not undo it.

---

## Inspecting sequences

```sql
SHOW SEQUENCES;
SHOW SEQUENCES LIKE 'order%';
SHOW CREATE SEQUENCE order_no;
COMMENT ON SEQUENCE order_no IS 'one per customer order';
```

`SHOW SEQUENCES` reports the name, the sequence's position, the start value, increment, minimum,
maximum, cache, owning column and comment.

### Reading the `reserved_upto` column

That column is the sequence's position, and **it is a ceiling, not the last value issued**:

```sql
CREATE SEQUENCE s CACHE 1000;
SHOW SEQUENCES LIKE 's';        -- reserved_upto is NULL: nothing has been drawn yet

SELECT nextval('s');            -- 1
SELECT nextval('s');            -- 2
SHOW SEQUENCES LIKE 's';        -- reserved_upto is 1000, not 2
```

Two values were issued, out of a thousand reserved to issue them from. The number means **"no
value above this has been issued"** — never "this value was issued". That is why the column is not
called `last_value`: a name like that is wrong by up to a whole cache block, and readers build on
it. Under the default `CACHE 1` the two numbers are the same, which is exactly what makes the
distinction easy to forget.

Three rules follow:

- **`NULL` means nothing has been drawn** since the sequence was created or last moved. The
  counter is still sitting on its seed, and the seed is one increment below the first value the
  sequence will issue — showing it would invite exactly the misreading above.
- **With `CACHE 1`, the default, the column is exact**, because each value is reserved durably
  before it is handed out. A sequence that asks for a larger cache gives up that exactness.
- **Do not compare two readings** to decide whether anything changed. Two readings can be equal
  across hundreds of issued values, and can differ by a thousand across one. Use `currval` for a
  value that was actually issued.

Reading the column costs a routed call per sequence, so the listing reads them in bounded parallel
batches. A sequence whose partition is momentarily without a leader reports `NULL` rather than
failing the whole listing.

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

Existing privilege bits are reused rather than a new one added. `ALL PRIVILEGES` is expanded to a
stored mask at grant time, so a new bit would not be picked up by any grant already issued — every
account an operator believes has everything would be quietly denied until every grant was re-issued.

---

## What will surprise you

These four properties are all consequences of how the counter works. Each one is reported as a bug
by someone who has not read this section.

### 1. Gaps are normal above `CACHE 1`

The node that owns a sequence reserves a **block** of values with one durable commit. It then hands
them out from memory with no storage traffic at all. Whatever is left of that block is abandoned
when the node restarts, when the sequence's partition changes owner, or when the sequence is
evicted from memory.

The default is `CACHE 1`, as in PostgreSQL, so a plain sequence has no block to abandon and skips
nothing:

```sql
CREATE SEQUENCE invoice_no;   -- 1, 2, 3 ... restart ... 4
```

The cost is one Raft commit, with its fsync, per value drawn. That is a large throughput
difference on an insert path that draws a value per row. Ask for the throughput by name when you
want it, and accept the gaps that come with it:

```sql
CREATE SEQUENCE event_no CACHE 1000;   -- 1, 2, 3 ... restart ... 1001
```

The values are still unique; they are just not consecutive. `CACHE` makes exactly the same trade
in PostgreSQL.

A sequence created before this default was introduced recorded no cache of its own and still
follows the node-wide block size. `SHOW SEQUENCES` prints the size it actually uses, and
`ALTER SEQUENCE <name> CACHE 1` moves it.

### 2. A sequence value is not an insert order and not a sort key

For up to five seconds after a partition changes owner, a former owner that has not yet learned it
lost the partition can still be draining the window it already reserved, while the new owner issues
higher values. During that window a value handed out **later** can be numerically **lower**.

The values stay unique. Only their order does not hold. If you sort by a sequence column and get a
surprising order, you have found this, not a bug. Order by a timestamp, or by the row id.

### 3. An advance does not roll back

A rolled-back transaction consumes the values it drew, and they are never reissued:

```sql
BEGIN;
INSERT INTO orders (id, total) VALUES (gen_id(), 1.0);   -- takes 4
ROLLBACK;
INSERT INTO orders (id, total) VALUES (gen_id(), 1.0);   -- takes 5, not 4
```

This is PostgreSQL's behavior and the only workable one. Rolling an advance back would mean either
holding the sequence's partition under lock for the life of every writing transaction, or reissuing
values — which would break the one guarantee the whole feature rests on.

The same applies to a retry. A serializable conflict (`CADB0502`, or `CADB0504` after the batched
lock-wait deadline) retried to success leaves a row carrying a **higher** number than the first
attempt drew.

### 4. The reported value is a ceiling, not the last value issued

Kahuna reports a sequence's **reserved high-water mark**. After four `nextval` calls on a fresh
sequence declared `CACHE 1000`, that number is 1000: four values were issued, out of a thousand
reserved to issue them from. Under the default `CACHE 1` it reads 4, because nothing is held
back.

That is the number `SHOW SEQUENCES` prints in its `reserved_upto` column, and it is why there is
no `last_value` column: publishing a ceiling under that name is wrong by up to a whole block, and
readers build on it. See [Reading the `reserved_upto` column](#reading-the-reserved_upto-column).

To learn a value that was actually issued, use `currval` on a transaction that drew one. Do not
compare two readings of a sequence to decide whether anything changed: two readings can be equal
across hundreds of issued values, and can differ by a thousand across one.

---

## Moving a live counter costs one lease

`setval`, `ALTER SEQUENCE … RESTART`, and `TRUNCATE … RESTART IDENTITY` all wait about five seconds
before they report success. That interval is the storage layer's block lease, and the wait is the
guarantee rather than an inefficiency.

A reserved block is served with no storage traffic at all, so a node that has lost the sequence's
partition without noticing keeps handing out values from the window it already holds. For one lease
after the record is rewritten there would otherwise be two live streams: the replaced incarnation's
window on that node, and the new value on the owner. Kahuna closes both sides — it withholds
success until no stale window can still be served, and it refuses allocations from the new value for
the same interval — so that when your statement returns, the move is true everywhere.

Every caller of these statements is an operator action, not a hot path, so a bounded delay is the
right price. **A client or proxy deadline shorter than five seconds turns a correct `setval` into a
timeout.**

---

## Lifecycle notes

**Dropping a database** removes its sequences' counters. A counter lives outside every keyspace a
prefix purge can scan, so the dropper reads the catalog to learn which counters the database owns
before it deletes that catalog.

**Branching a database** ([database branching](database-branching.md)) gives the branch its own
counters, each seeded at the source's reserved ceiling. Seeding from the ceiling is correct
precisely because it *is* a ceiling: it sits above every value the source ever issued, so the branch
cannot collide with the rows it inherited. The cost is a gap, and a gap is allowed.

**Backups** ([backups and point-in-time recovery](backups-and-point-in-time-recovery.md)) carry
sequences. A sequence's record is an ordinary replicated key-value entry in the node's store, and a
backup covers the whole node, so a restore brings the counters back with the rows — at the reserved
ceiling, which is above every value the backup's rows hold. No reseeding step is needed.

---

## Error codes

| Code | Meaning |
|---|---|
| `CADB0542` | The name is already taken by a sequence, a table or a view. |
| `CADB0543` | The named sequence does not exist. |
| `CADB0544` | The sequence has no value left to issue: the next one would pass `MAXVALUE`. |
| `CADB0545` | The definition cannot hold — a non-positive increment, `CACHE` below 1, `MAXVALUE` below `MINVALUE`, `START` outside the two. |
| `CADB0546` | `currval` or `lastval` called before this transaction drew a value. |
| `CADB0547` | A sequence call was written where the engine cannot bound how many values it would draw. |
| `CADB0548` | The sequence is owned by an identity column, or a column default draws from it. |
| `CADB0535` | The sequence's partition had no confirmed leader for the whole retry window. Transient; re-issue the statement. |

---

## See also

- [Data types](data-types.md) — what `int64` holds, and why a sequence column is one.
- [Database branching](database-branching.md) — what a fork copies, and what it recreates.
- [Backups and point-in-time recovery](backups-and-point-in-time-recovery.md).
- [Configuration](configuration.md) — `sequence_retry_budget_ms`, the wall-clock budget a sequence
  call rides a leadership change out on.
