# Transactions, Locking & Isolation in CamusDB

> **Audience:** new contributors getting started on CamusDB.
> **What you'll learn:** how CamusDB reads and writes data when many clients hit it at once — the
> timestamps, locks, and versions involved — and exactly what consistency you can rely on today, both
> on a single node and across a cluster.
>
> No prior knowledge of CamusDB's internals is assumed. If you've used any SQL database and have a
> rough idea of what a "transaction" is, you're ready.

---

## 1. The big picture: CamusDB is built on two other systems

CamusDB is a SQL layer on top of two lower-level building blocks. They provide the storage engine,
the locking and the replication. Understanding isolation in CamusDB means
understanding how these three layers talk to each other.

```
┌─────────────────────────────────────────────────────────────┐
│  CamusDB            SQL parsing, schema/catalog, query planner│
│                     turns "SELECT * FROM robots" into reads   │
│                     and writes of key/value pairs.            │
├─────────────────────────────────────────────────────────────┤
│  Kahuna             A transactional key/value store. Provides │
│                     MULTI-VERSION values, per-key locks, and  │
│                     two-phase commit. This is where most of   │
│                     the locking & isolation actually lives.   │
├─────────────────────────────────────────────────────────────┤
│  Kommander (Raft)   Consensus. Replicates every change to a   │
│                     majority of nodes so data survives a node │
│                     failure and every node agrees on order.   │
└─────────────────────────────────────────────────────────────┘
```

**Rule of thumb:** the isolation CamusDB gives you comes from how CamusDB *uses* Kahuna's
transactions. CamusDB rows and indexes are key/value pairs in Kahuna.

---

## 2. Vocabulary (read this once, refer back as needed)

- **Transaction** — a unit of work that either fully happens or doesn't (atomic). Every read and write
  in CamusDB runs inside one, even a one-line `SELECT`.
- **HLC timestamp (Hybrid Logical Clock)** — the "version stamp" every transaction gets. It combines
  physical wall-clock time with a logical counter, so timestamps are unique and reflect causal order
  even across machines. **It is always assigned by the server (Kahuna), never by the client.**
- **MVCC (Multi-Version Concurrency Control)** — instead of overwriting a value in place, the store
  keeps multiple committed *versions* of each key, each stamped with the timestamp that wrote it. A
  read picks the right version for its timestamp. This is what lets reads and writes not block each
  other.
- **Write intent** — a *provisional*, not-yet-committed write a transaction places on a key. Other
  transactions can see "this key is being written by someone" and react, but they don't see the new
  value until it commits.
- **Lock** — a reservation a transaction holds on a key (or a range of keys) to keep others out.
  CamusDB uses two kinds: **per-key locks** (for individual rows/index entries) and **range locks**
  (for whole scans). More in §6.
- **Two-phase commit (2PC)** — the protocol that makes a multi-key write atomic: first *prepare* all
  the keys (place intents), then *commit* them all at once.
- **Partition** — a shard of the keyspace. Each partition is its own Raft group with its own leader.
  A key is routed to exactly one partition.
- **Read-only vs read-write transaction** — a crucial distinction in CamusDB; see §4 and §5. A
  transaction also declares this *mode* up front when it wants the stronger isolation level.
- **Isolation level** — how strongly a transaction is shielded from concurrent ones. CamusDB has two:
  **Serializable** (the default, described in §9) and **Read Committed** (explicit opt-out,
  described in §4/§5 as a baseline).

---

## 3. How CamusDB lays out data as keys

Every row and index entry is a key/value pair. The key encodes which table, what kind of data, and
which row:

```
Primary rows        {dbId}:{tableId}|r/{rowId}                → the serialized row bytes
Unique index        {dbId}:{tableId}|i:{indexId}/{value}      → the rowId it points to
Non-unique index    {dbId}:{tableId}|i:{indexId}/{value}{rowId}
Schema / catalog    {dbId}/meta/...                           → table definitions, etc.
```

The part of the key *before the last `/`* is its **key space** (e.g. `{dbId}:{tableId}|r`). Key
spaces matter for routing and range locks (§6, §8). The part of a key space *before its first `|`*
is its **placement group** (`{dbId}:{tableId}`): under hash routing Kahuna hashes the group, so a
table's rows and all of its indexes share one partition.

**Routing — how a key finds its partition.** Kahuna offers two strategies:

- **Hash routing (the default).** The key space's placement group is hashed to a partition, so a
  table and its indexes stay together. Simple and even across tables, but a table's keys are
  indivisible: one partition holds all of them, and a hot table cannot be spread.
- **Key-range routing (opt-in, off by default).** Contiguous keys are kept together in one range, so a
  scan over `[A, B)` touches one place and can be locked as a range. Enabled per key space, and only
  has an effect when the cluster has at least two partitions. Set with `key_range_sharding` (or the
  `CAMUS_KEY_RANGE_SHARDING` environment variable) when the cluster is created; the mode is fixed
  from then on (see `key-range-sharding.md`).

For most of this document, assume **hash routing** (the default). §8 covers what changes with
key-range routing.

---

## 4. The life of a READ (a `SELECT`)

A plain `SELECT` (no surrounding `BEGIN`) runs as a **read-only transaction**. Read-only transactions
are special: they are cheap, take **no locks**, and read straight from MVCC.

```
Client: SELECT * FROM robots WHERE speed > 100
   │
   ▼
CamusDB query layer
   │  1. Start a READ-ONLY transaction (no round-trip to begin one;
   │     it uses a special "read latest committed" timestamp).
   │  2. Plan the query → decide to scan the table (or an index).
   ▼
Kahuna
   │  3. Scan the key range, returning, for each key, the latest
   │     COMMITTED version. Uncommitted write intents from other
   │     in-flight transactions are skipped — you never see them.
   ▼
CamusDB
   │  4. Decode each row, apply the WHERE filter, stream results back.
   ▼
Client: rows
```

Two things make reads concurrent:

1. **No locks are taken.** Many `SELECT`s can run over the same table at the same time without waiting
   on each other.
2. **MVCC means readers ignore in-flight writers.** A concurrent `INSERT`/`UPDATE` has only placed a
   *write intent*; the reader reads the previous committed version instead of blocking. Conversely the
   writer doesn't wait for readers.

So **readers never block readers, and readers never block writers** (and vice-versa). This is where
CamusDB's good read throughput comes from.

> **Note on read-only vs read-write SELECTs.** If a `SELECT` runs *inside* a transaction you opened
> explicitly (a read-write transaction — see §5), it participates in that transaction instead, and may
> take locks. The lock-free, fully-concurrent behavior above applies to standalone autocommit
> `SELECT`s.

---

## 5. The life of a WRITE (`INSERT` / `UPDATE` / `DELETE`)

Writes run as **read-write transactions** and go through two-phase commit so that a multi-key change
(a row plus all its index entries) is atomic.

```
Client: INSERT INTO robots ...
   │
   ▼
CamusDB
   │  1. BEGIN: ask Kahuna to start a transaction. Kahuna's clock
   │     assigns this transaction an HLC timestamp (its identity).
   │
   │  2. Build the keys to write: the row key + one key per index.
   ▼
Kahuna — PREPARE phase
   │  3. For each key, acquire a lock and place a WRITE INTENT
   │     (the provisional new value, not yet visible to others).
   │     If another transaction already holds the key → conflict.
   ▼
Kahuna — COMMIT phase
   │  4. Replicate the change through Raft to a majority of nodes,
   │     then flip every intent to a committed version at the
   │     transaction's timestamp — atomically.
   ▼
CamusDB
   │  5. Release the locks. The transaction is durably committed.
   ▼
Client: OK
```

Key points for a newcomer:

- **Write intents + per-key locks are how two writers are kept apart.** If transaction A and
  transaction B both try to write the same row, one of them sees the other's intent/lock and fails,
  so no write is silently overwritten. This is **write-write conflict detection**.
- **2PC makes the row and its indexes commit together.** You never observe a state where the row
  exists but its index entry doesn't.
- **Locks are held until the transaction ends** (commit or rollback), then released. Kahuna's
  **transaction coordinator** owns the transaction's working set, and CamusDB keeps no list of its
  own. Every modified key and held lock is registered with the coordinator as the operation happens,
  so commit and rollback hand the coordinator a *handle* and it finalizes the set it accumulated. See
  `kahuna-transaction-coordinator.md` for how that registration works.
- **Schema safety.** A write also "pins" the schema version of the tables it touches at the start, and
  the commit is rejected if the schema changed underneath it (e.g., a column was dropped mid-write).
  This keeps DDL and DML from corrupting each other.

---

## 6. The locks CamusDB uses, and when

There are two distinct lock mechanisms. Knowing which is taken when is most of the mental model.

### 6.1 Per-key locks & write intents — used by every write
These are taken automatically during the PREPARE phase of any write (§5), one per key being modified.
They serialize **conflicting writes to the same key**. They are *not* taken by reads.

### 6.2 Range locks — phantom protection for scans
A range lock reserves a whole *range* of keys at once, e.g. "all index entries between 10 and 50."
Their purpose is **phantom protection**: stopping another transaction from inserting, changing, or
deleting a row inside a range you are scanning, which would otherwise make a repeated scan return
different results.

Two things keep range locks from becoming a bottleneck:

- **They are *shared* for scans.** Two transactions scanning overlapping ranges both succeed and run
  side by side — a scan never blocks another scan. (Reads don't block reads, even serializable ones.)
- **Writes respect them automatically.** A write does not need to take a range lock itself: when a
  transaction tries to insert/update/delete a key that falls inside a range another transaction has
  locked, that write is held back (it retries). This is what stops a phantom row from
  appearing mid-scan — including a *brand-new* key that didn't exist when the scan started.

Range locks are acquired by every scan path in the query executor: the full-table scan, the index
scan, the bounded index range scan and the `IN`-list scan of a single-table `SELECT`, and the same four
shapes when they are the leaves of a `JOIN`. The sub-reads of a predicate take them too: the inner scan
of a semi-join (`IN (SELECT …)`), the inner scan of a correlated `EXISTS`, and the equality probe of a
non-unique index inside an index nested-loop join (one lock per probed key, collapsed into one
whole-index lock past the escalation threshold of §6.3). A join leaf resolves the statement's
parameters as a single-table scan does, so `JOIN … WHERE j.k = @k` reads `j` through its index and
locks that one key, present or absent; only a leaf filter no index serves reads the whole table and
locks its whole row range.

An `IN`-list scan locks one range for each list value, and not the span from the smallest to the
largest value. So `WHERE k IN ('a', 'z')` fences `'a'` and `'z'`, present or absent, and a write to
`'m'` does not conflict with it. This holds for a non-unique index and for the first column of a
unique index of several columns (a composite primary key); a unique index of one column locks the
point of each value. When the list holds more values than the escalation threshold of §6.3 allows
for the index, the scan takes one lock over the span of its values instead, which is still narrower
than the whole index. `x = v1 OR x = v2` is the same list and takes the same locks.

Because a table scan locks the whole row range, the planner keeps an index scan for a statement that
holds these locks when its predicate names rows by equality or by an `IN` list, or when the table is at
or below the escalation threshold of §6.3, even where a table scan would read fewer keys. See "The lock
of a table scan" in `docs/query-planner.md`.

They fire in one situation:

- **For a Serializable read-write transaction** (see §9), on *any* configuration including the default
  single-node hash setup — a serializable scan needs phantom protection to be correct, so the lock is
  taken regardless of routing. In key-range routing mode the lock is clipped to each range the scan
  touches, so a scan over `[A, B)` doesn't block a writer on the disjoint `[B, C)`.

Otherwise **no range lock is taken**. That includes every scan in an explicit Read Committed
transaction, in every routing mode: Read Committed promises no phantom protection, so its scans never
block or abort a writer. That includes every autocommit `SELECT`, in every routing mode:
it reads a pinned snapshot under either default, and it has no transaction identity that could own a
lock.

### 6.3 Point read locks (Serializable read-write only)
A Serializable read-write transaction also locks the *individual* keys it reads with `GetRow` or a
unique-index lookup — a one-key "shared point lock," held until the transaction ends. This is what
makes "read a row, then act on it" safe: no other transaction can change that exact key underneath you.

- Two serializable transactions reading the **same** key coexist (both hold a shared lock).
- A writer trying to change a key a serializable reader holds is **held back / fails to commit** until
  the reader finishes.
- If the *same* transaction later **writes** a key it read, its shared lock is **promoted to exclusive**
  in place, so from that point no one else can even read that key until it commits.

**Which read paths take them.** Every path that reads a row by its id or an index entry by its key:
the point read of `SELECT … WHERE pk = …`, the row fetch behind an index scan, the unique probe of an
index nested-loop join (the index entry and then the row), the rows a join leaf fetches after its index
walk, and the inner lookup of a semi-join or a correlated `EXISTS`. A key read as **absent** is locked
as well (the lock sits on the key the entry would have), which is what keeps a repeated read of an
absent key stable. A read through a `JOIN` therefore leaves exactly the locks the same read by primary
key leaves: the predicate lock of each leaf (§6.2) plus the point locks of every row and entry it read.

These point locks are taken **only** by Serializable read-write transactions. Read Committed reads and
Serializable read-only (snapshot) reads take none.

**Escalation for large reads.** To keep a transaction that reads thousands of rows from accumulating
thousands of individual point locks, once the per-table point-lock count passes a threshold the
transaction takes a single whole-table shared lock and stops taking new per-row locks. The threshold
is `lock_escalation_threshold` (1000 by default). The count is for one transaction and one table (or
one index), over all of its statements.

The whole-table lock conflicts with every write to the table by another transaction until the
reader ends. So the default sits above the row count of an
ordinary page read, and a read that names a bounded set of rows (`WHERE usersId = @p`, an `IN` list
of ids) locks only the rows it read. A transaction that reads more rows of one table than the
threshold still takes the whole-table lock. If that is not acceptable, read the rows in a
Serializable read-only transaction (a snapshot, no locks), or raise the threshold.

A read that fetches a page of rows through an index takes the point locks of the page together: it
drops the rows the transaction already holds, decides escalation once for the page, and then acquires
the locks with bounded concurrency. A page that crosses the threshold takes only the whole-table lock.

```
                            Reads                         Writes (INSERT/UPDATE/DELETE)
                            ──────────────────────────    ─────────────────────────────
Read Committed (opt-out)    no locks                      one per-key lock per modified key
Serializable read-only      no locks (consistent          (read-only — no writes)
                            MVCC snapshot, §9)
Serializable read-write     shared point lock per key     per-key lock; plus any key it read
                            read; shared range lock        is promoted from shared→exclusive.
                            per scan (held to commit)      A write into a range/key another txn
                                                           locked is held back (phantom + write
                                                           conflict protection)
```

### 6.4 Pessimistic vs optimistic (an advanced, opt-in choice)

Everything above describes **pessimistic** locking — take the lock first, block a conflicting party,
resolve at lock time. That is the default and what almost all callers use.

A transaction can instead be begun in **optimistic** mode. An optimistic transaction skips the explicit
**exclusive write lock**: it stages its writes and records the versions of the rows it reads, and
write-write / read-write conflicts are checked only at **commit**. If another transaction committed a
change to a key it wrote (write-write) or to a key it read (read-write / write skew) in the meantime, the
commit is aborted and the transaction retries. This trades write-time blocking for commit-time validation
— good for low-contention, read-mostly workloads.

The locking mode is a *concurrency strategy*, independent of the isolation level: any
`{ReadCommitted, Serializable} × {Pessimistic, Optimistic}` combination is valid. **"Optimistic" gives
different behavior at the two isolation levels:**

| | **Read Committed + Optimistic** | **Serializable + Optimistic** |
|---|---|---|
| Explicit write locks | none | none (write folds an implicit intent) |
| Locks for a read of one key | none | none by default (proved at commit) |
| Locks for a scan | none | **taken** (shared range locks) |
| Lock-free? | **yes**, fully | for single-key reads; scans lock |
| Phantom protection | ✗ (validates observed rows only) | ✓ (range locks fence the scan) |

**Serializable + Optimistic locks ranges, not single keys.** A scan takes its shared range lock whatever
the locking mode, because a range is not a key: a row inserted into a scanned range cannot be found by
checking the keys the scan saw. A read of one key (a row by its primary key, a unique-index lookup, the
rows a page of an index read fetches) takes no lock. Three things stand in for it:

- **The read is proved at commit.** The key and the revision read, or the fact that the key was
  missing, go into the transaction's read set, and the commit is refused if the key changed. A key that
  was read and then written is proved as the base of that write.
- **The reads are held to one point in time.** A read of a key the transaction has not read yet is
  answered only if every key it read before still has the revision it read; otherwise that read is
  refused. So a transaction is never handed two rows that were not committed together, which without a
  lock could happen between its first read and its second.
- **A write gives way to an older reader.** A write or a delete of a key that an older transaction read,
  while that transaction is still running, is refused at once. The younger of the two gives way at its
  statement and the older one goes on, the order a lock on the key would give.

What changes for an application: a reader no longer blocks a writer of the row. A conflict that a lock
would refuse at the writer's statement can instead be refused at the reader's next read or at its commit.
Every such refusal is retryable from the start of the transaction.

`serializable_optimistic_point_locks: true` restores the earlier form, in which a Serializable read takes
a shared lock on each key it reads and a following write upgrades it to exclusive, even when the
transaction is optimistic. That form blocks writers behind readers, and every commit of such a
transaction takes the two-phase path. **Read Committed + Optimistic** is lock-free in both forms (and is
phantom-vulnerable: its validation covers the specific rows it read, not predicates). See
`kahuna-transaction-coordinator.md` for the mechanism.

**What "observed rows" means.** An observation is recorded per *key*, for every key a read touched: the
key of a point read, and every key a scan visited — including rows the `WHERE` clause then rejected,
because the filter runs after the store returned the page. The observation is the row's revision, so a
committed write to *any* column of that row invalidates it, including columns the transaction never
read. Two consequences follow. A read-modify-write of one row is validated exactly as intended. But an
optimistic transaction that scans a table under sustained writes — a report, a `COUNT(*)`, an
`INSERT … SELECT` out of it — depends on every row the scan passed and does not commit while the
writers run, at Read Committed as much as at Serializable; a pessimistic Read Committed scan records
nothing and has no such dependency. For a bulk read out of a busy table under optimistic locking, read
a snapshot (`AS OF SYSTEM TIME`) or run that statement pessimistically — see
[`insert-select-and-ctas.md`](insert-select-and-ctas.md#concurrent-writers-on-the-source).

**A row read waits for an uncommitted write of the row.** When an optimistic read-write transaction
reads a row by its key and another transaction has written that row and not yet committed, the read
waits for the other transaction to commit or roll back, and then returns the committed result. Without
the wait the read would return the value the other transaction is about to replace, and a later
`UPDATE` of the row by the reading transaction would fail with `CADB0504` once the other one commits.
With it, two read-modify-write transactions on one row run one after the other and both commit. The
wait is bounded by `optimistic_intent_wait_ms` (default 100 ms); at the limit the read returns the
committed value as before. `0` turns the wait off. Three limits:

- Only reads by row key wait: a point lookup, and the rows an index lookup fetches. A range scan does
  not wait.
- At Read Committed a transaction waits also after it has written. Two transactions that each wrote
  the row the other one reads then wait for each other, until the limit; each read returns the
  committed value and the next `UPDATE` of either is refused. Keep the limit well above the commit time
  (a low limit turns waits into retries) and well below the client's statement timeout.
- At Serializable a transaction does not wait for an older transaction's write, because it holds a
  lock from its first read on and the two could wait for each other. It reads the committed value as
  before.
- When two transactions both read a row before either writes it, the second `UPDATE` cannot succeed.
  It is refused at once with `CADB0504`, without waiting for the first transaction to commit, and the
  client retries the transaction.

#### Selecting the locking mode

Pessimistic is the default. Optimistic is opt-in, chosen three ways (precedence: per-request >
`SET TRANSACTION LOCKING` > server default):

- **HTTP request field.** Send `"locking": "Optimistic"` (case-insensitive; `"Pessimistic"` is the
  other value) on `/start-transaction` for an explicit transaction, or on `/execute-sql-non-query` /
  `/execute-sql-ddl` for a single autocommit statement — it sits next to `isolationLevel` /
  `transactionMode`. An unrecognized value is rejected with `InvalidInput` (HTTP 400). The read-only
  `/execute-sql-query` path ignores it: an autocommit `SELECT` runs as a read-only snapshot, which has
  no locking mode.

- **`SET TRANSACTION LOCKING { PESSIMISTIC | OPTIMISTIC }`.** Inside an explicit transaction, issued
  **before** any data statement (exactly like `SET TRANSACTION ISOLATION LEVEL …`; it may be combined
  with it in either order). It must run first because the locking mode is pinned when the Kahuna
  coordinator session opens — so an explicit transaction defers opening its session until the first
  statement, letting `SET TRANSACTION LOCKING` configure it. Issuing it after a data statement is an
  error.

- **Server default.** `default_transaction_locking: pessimistic | optimistic` in `config.yml` sets the
  cluster-wide default for any transaction that makes no explicit selection.

See `kahuna-transaction-coordinator.md` for the deferred-session-start mechanism and how optimistic
folding and validation work.

### 6.5 Foreign-key rendezvous locks — every isolation level
A foreign key (see [foreign-keys.md](foreign-keys.md)) adds one more lock, and it is the one
exception to "Read Committed takes no read locks." A child writer and a parent writer must not both
commit when one of them removes what the other needs, at **any** isolation level. They meet on one
key: the parent's unique-index entry for the referenced key.

- A **child** INSERT or UPDATE takes a **shared point lock** on that entry, then reads it. It takes
  the lock at Read Committed and at Serializable, pessimistic or optimistic, and holds it to the end
  of the transaction.
- A **parent** DELETE, or an UPDATE that changes the key, deletes that entry. Its write meets the
  child's shared lock and waits (up to `lock_wait_deadline_ms`), so it cannot remove the key while a
  child that read it is still open. After the write, the parent reads the child's index for the
  key, and fails if a child row holds it.
- An **optimistic** parent stages its delete without a write intent, so it takes an **exclusive**
  point lock on the entry before its read. Range locks conflict by mode both ways, so the effect is
  the same.
- A parent UPDATE that does not change the key does not touch the entry, and never waits.

These locks **never escalate** to a whole-index lock (unlike the point read locks of §6.3): an
escalated shared lock on the parent's index would block every new parent INSERT until the child
transaction ends. A child transaction holds at most one such lock per distinct parent key.

### 6.6 Row-locking reads — `SELECT … FOR UPDATE`
`SELECT … FOR UPDATE` takes, on each row it returns, the per-key exclusive lock of §6.1: the same lock
an `UPDATE` of the row takes. It takes it at **every** isolation level, also at Serializable, where an
`UPDATE` relies on its §6.3 point lock until it writes. Without the key lock, a Read Committed
`FOR UPDATE` of the same row would not see the Serializable one, because a key-lock grant does not
check range locks. At Serializable the statement also makes its point lock on the row exclusive.

The statement locks first, then reads each row again at its latest committed value, then checks the
`WHERE` clause again, as the write phase of an `UPDATE` does (§5). It needs a pessimistic, read-write
transaction. The options:

- no option: a held row fails the statement with `CADB0502`, as an `UPDATE` fails;
- `NOWAIT`: a held row fails the statement with `CADB0555`, which is not retried automatically;
- `SKIP LOCKED`: a held row is left out. Read Committed only.

At Serializable, the shared scan lock of §6.2 comes before the row locks. A full scan that covers a row
another transaction holds fails at that lock with `CADB0502`, also under `NOWAIT`.

The user-facing rules, the refused shapes and the autocommit behavior are in
[select-for-update.md](select-for-update.md).

---

## 7. MVCC, in one picture

MVCC is the reason reads stay fast. Imagine three versions of one key over time:

```
key "db:robots|r/42"

   v1 (committed @ t=10)   speed=80      ← a read at t=12 sees this
   v2 (committed @ t=20)   speed=120     ← a read at t=25 sees this
   v3 (WRITE INTENT @ t=30, uncommitted) ← a read at t=31 SKIPS this,
                                            and still sees v2

A reader never waits for v3 to commit; it reads the latest *committed*
version as of its own timestamp. The writer of v3 never waits for the
reader. They pass like ships in the night.
```

When v3's transaction commits, the intent becomes a real version; future reads then see it.

---

## 8. Single node vs cluster — what actually differs

The **locking and isolation model is the same** in both. The cluster distributes the same
machinery across more nodes and partitions. Here's the mapping:

| Concept | Single node (default) | Cluster |
|---|---|---|
| Partitions | usually one | many; each is its own Raft group with a leader |
| Routing | one place, so routing is trivial | keys routed to partitions by hash (or by range, if enabled) |
| Where a write commits | local Raft (a one-node "quorum") | the partition **leader**, replicated to a node majority |
| Multi-key write | 2PC, local | 2PC across the **leaders** of every partition the keys touch |
| Range locks | active for Serializable read-write transactions; otherwise dormant | identical: Serializable read-write only (clipped per range under key-range routing); never for Read Committed or an autocommit `SELECT` |
| Reads | MVCC, lock-free | MVCC, lock-free (served by the partition leader of each key) |
| Isolation levels | Read Committed + Serializable, both work | identical — same levels, same guarantees |
| Schema/DDL | a dedicated, totally-ordered key space | same key space, kept on a single partition so all nodes agree on schema order |

A few cluster-specific notes for newcomers:

- **Every partition has a leader.** Writes and locks for a key are coordinated by that key's partition
  leader, then replicated. The HLC timestamp a transaction gets is meaningful across the whole cluster
  (that's the point of a hybrid logical clock).
- **A transaction can span partitions.** If your write touches keys that route to different
  partitions, the two-phase commit coordinates across those partition leaders. The atomicity guarantee
  still holds.
- **Key-range routing buys range-scan concurrency.** Today every node still
  stores all the data; key-range routing changes *where locking and ordering happen*, letting a scan
  over `[A,B)` avoid blocking a writer working on a disjoint `[B,C)`. It does not spread storage across
  nodes.
- **Isolation behaves identically on one node or many.** A Serializable transaction gives the same
  result whether the data lives on one partition or several — single node is the one-partition
  case of the same algorithm. A client can't tell from the isolation behavior how many nodes there are.

For how schema changes (DDL) replicate and stay consistent across the cluster, see
`distributed-schema-architecture.md`. This document covers data-path reads/writes.

---

## 9. What consistency you can rely on **today**

This is the part app developers need most, so it is stated precisely. What CamusDB guarantees right
now:

- ✅ **Atomic transactions.** A transaction's changes all apply, or none do.
- ✅ **Durable commits.** A committed write has been replicated to a majority and survives node
  failure.
- ✅ **No dirty reads.** You only ever read committed data; in-flight write intents are invisible.
- ✅ **Write-write conflict detection.** Two transactions that modify the same row don't silently
  overwrite each other — one fails and can retry.
- ✅ **Non-blocking reads.** Concurrent reads never block each other or writers.
- ✅ **A failed statement aborts its transaction.** Any error inside an explicit transaction makes
  it rollback-only, as in PostgreSQL: later statements are refused and the commit rolls it back
  (`CADB0824`). A write batch Kahuna refuses stages none of its keys. See §9.4.

Those six hold at **every** isolation level. On top of them, CamusDB offers **two isolation levels**,
chosen per transaction (the default applies when you don't ask for anything).

### 9.1 Serializable (the default)

Every autocommit statement and every transaction runs at Serializable unless it opts down to Read
Committed. See §9.3 for the full description.

**Opting out to Read Committed.** If you don't need the full serializable guarantee and want the
absolute minimum overhead, you can request Read Committed per transaction — either via the isolation
field on the begin-request, or as the first statement of an explicit transaction:

```sql
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
```

There is also a server-wide knob (`default_isolation_level` in `config.yml`, `CamusDBOptions.DefaultIsolationLevel`) to revert the default
globally, if you need to roll back an environment to the old behaviour.

### 9.2 Read Committed (explicit opt-out)

This is the lock-free behavior described in §4/§5. What it does **not** promise (design around these):

- ✅ **One snapshot per `SELECT` in a transaction.** In a pessimistic transaction that you begin
  explicitly, each `SELECT` reads at one statement timestamp. The node takes this timestamp from its hybrid logical clock
  when the statement starts. Every scan page, partition, index lookup, row fetch and large value of the
  statement is read at that timestamp, so one statement never sees half of another transaction's
  commit, also when its pages come from several partitions. The statement also sees the uncommitted
  writes of its own transaction. The next statement gets a new timestamp and sees the commits that came
  between the two statements. An optimistic transaction, or one with read validation, keeps reading the
  latest committed value of each key: it validates every read at commit, so it never commits on a torn
  read, and a lost update fails early, at the statement that writes the changed row.
- ✅ **One snapshot for the read phase of a write statement.** In the same transactions, the locate
  scan of an `UPDATE` or a `DELETE` reads at one statement timestamp, and so do the subqueries in its
  `WHERE` and `SET`. The source query of an `INSERT … SELECT` also reads at one statement timestamp.
  The write phase then reads the latest value: it locks each located row, reads it again and checks
  the `WHERE` again. A row that a later commit deleted or moved out of the `WHERE` is skipped. A row
  that a commit after the timestamp inserted or moved into the `WHERE` is not changed by the
  statement, as if the statement ran at its timestamp. Uniqueness and foreign-key checks read the
  latest value.
- ⚠️ **A read at a statement timestamp can wait for a commit in progress.** A read does not wait for
  a transaction that is still open. When it meets a write that such a transaction staged, it reads the
  last committed value, and Kahuna puts the commit of that transaction above the read's timestamp. So
  a transaction that holds a row for a long time before it commits does not hold the reads of that row.
  A read waits only for a transaction that already started its commit, when that commit can land at or
  before the read's timestamp. The wait is about as long as the commit. It has a limit: about 5 s for a
  scan page and about 16 s for a point read, by default. After the limit, the statement fails with a
  retryable error. CamusDB does not send a read again after 15 s of attempts, so a point read that
  reached the limit once is not repeated and the statement does not wait longer. A read of the latest
  value does not wait. This applies to a `SELECT` and to the
  read phase of a write statement.
- ⚠️ **Pass the causal token at `BEGIN` when you read after a commit on another node.** A read at a
  timestamp does not show a value that another node committed at a later clock time, even when that
  commit finished first. If the clock of the node that runs your transaction is behind, a `SELECT` can
  miss your own last commit. Pass the causal token of that commit when you begin the transaction
  (`causal_token_*` on gRPC `StartTxnRequest` or on the batch `START`, `causalToken` on HTTP
  `/start-transaction`). Every statement timestamp of the transaction is then at least that token.
  A commit returns its token: gRPC `CommitReply`, and `causalToken` in the HTTP `/commit-transaction`
  response and in the HTTP autocommit query and write responses (not in the DDL response). The token
  is not the commit timestamp, which a commit also reports (§9.5).
- ✅ **An autocommit `SELECT` reads one snapshot under a Read Committed default too.** It reads at
  one timestamp, which the node takes from its clock when the statement starts, as under the
  Serializable default. Every page, partition and sub-read (a join, a subquery, an index lookup) of
  the statement is read at that timestamp. The read takes no lock and has no Kahuna round trip. It
  has the two costs of every read at a timestamp, above: it can wait for a commit in progress, and it
  needs the causal token to see a commit that another node made at a later clock time. Pass the token as `causal_token_*` on the gRPC `SqlRequest`, or as
  `causalToken` on HTTP `/execute-sql-query`.
- ⚠️ **No repeatable reads / phantom protection across statements.** Read a set of rows, read again,
  and a concurrent transaction may have changed or inserted rows in between.
- ⚠️ **Write skew is possible.** Two transactions can each read a set, then write disjoint keys based on
  what they read, in a way no serial order would allow.
- ⚠️ **A `SELECT` does not pin what it read.** A statement-timestamp read leaves no read pin in Kahuna,
  so a later statement is never refused because a key it read earlier changed. It also means that
  nothing guards a value you read in one statement and write in a later one: that read-then-write
  pattern needs Serializable, or a single `UPDATE … SET v = f(v)`.
- ✅ **Writes and their checks read the latest committed value.** A uniqueness check, a foreign-key
  check, and the rows an `UPDATE` or `DELETE` locks and reads are never read at a statement
  timestamp. They see every commit that came before them, including commits after the transaction's
  last `SELECT`.
- ✅ **One `UPDATE` or `DELETE` statement never loses a concurrent commit.** The statement finds its rows
  with an unlocked scan, then, for each batch of rows: it takes the exclusive row locks, reads the rows
  again, evaluates the `WHERE` clause again on that read, and computes every `SET` expression from that
  read. So `UPDATE t SET v = v + 1` always adds to the latest committed value, a row that a concurrent
  commit moved out of the `WHERE` clause is left alone, and a row that a concurrent commit deleted is
  skipped. These rows are not counted in the result. This is the behavior of PostgreSQL at Read
  Committed. `WHERE` conditions that contain an `EXISTS` subquery keep the answer the first scan gave.
  Under optimistic locking no lock is taken; the commit fails instead if one of the rows changed.
  A read in one statement and a write in a *later* statement are not protected: that read-then-write
  pattern needs Serializable, or a single `UPDATE … SET v = f(v)`.

For invariants that must hold under concurrency at this level (e.g. "no two robots with the same
serial"), lean on **unique constraints** (enforced at the key level) rather than read-then-decide logic.

### 9.3 Serializable (detail)

A transaction can ask for **Serializable** — the strongest level, where the end result is always
equivalent to running the transactions one-at-a-time in *some* order. Under the hood CamusDB uses two
different strategies, picked automatically from whether the transaction is read-only or read-write:

- **Serializable read-only → a consistent snapshot.** The transaction is pinned to a single timestamp
  at the moment it begins and reads *every* key as of that instant — across statements and across
  partitions. It sees no write committed after it started: no read skew, no phantoms, fully repeatable.
  And it does this **without taking any locks**, so it never blocks writers and is never blocked. This
  holds under key-range sharding too (`key-range-sharding.md`): the engine takes no scan lock for a
  transaction that reads at a pinned timestamp, so a snapshot read is never refused by a writer's range
  lock and never makes a writer wait. An autocommit `SELECT` gets the same kind of snapshot, pinned when
  the statement begins, with no round trip to Kahuna, in every routing mode. You can
  hold one open across **several reads** (in separate requests, resumed by transaction id) and they all
  observe the same instant — ideal for a consistent multi-query report.
- **Serializable read-write → strict locking.** Reads take the shared point/range locks from §6.2–6.3
  and hold them to commit; a key the transaction reads can't be changed under it, and a key it writes
  becomes exclusive. This catches the anomalies Read Committed allows — including **write skew**: if two
  serializable read-write transactions would conflict, one commits and the other is aborted and must
  retry.

**How you ask for a specific mode.** Serializable is the default, but you can be explicit — either
via the isolation field on the begin-request, or as the **first** statement of an explicit transaction:

```sql
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;             -- keeps the access mode (read-write by default)
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY;   -- snapshot, no locks, no writes
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE;  -- strict locking
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;           -- opt down to read committed, keeps the access mode
```

A form that names no access mode keeps the mode the transaction has, as in PostgreSQL. It does not
make a read-only transaction writable. §9.6 describes read-only transactions.

It must come *before* any read or write — CamusDB rejects it once the transaction has executed a
statement, because retroactively upgrading the level would skip locks the earlier reads needed.

**Two practical rules:**
- **Retry on conflict.** A serializable read-write transaction can be aborted (a serialization conflict,
  or a lock-wait that won't clear) — the whole transaction must be replayed from the beginning. Write
  your serializable read-write logic inside a retry loop. For single-statement (autocommit) work there's
  a built-in helper that does the replay-with-backoff for you (`SerializableRetryHelper`); for explicit
  multi-statement transactions you replay from `BEGIN` yourself. The full contract — which error codes are
  retryable, and the patterns — is in `serializable-retry-contract.md`.
- **Long-running is supported, but bounded.** A serializable read-write transaction's range locks are
  kept alive by the **Kahuna coordinator**, which renews a live transaction's range-lock leases for the
  duration of its session and releases them on commit/rollback — so a transaction can stay open for a
  long interactive session with no client-side heartbeat. There is still a hard **maximum
  lifetime** (about one hour) as a backstop: if a transaction outlives it, it is **aborted with a clear
  error** at its next operation or at commit — it never silently loses its isolation, and an abandoned
  session is reclaimed by the server after that window. (This bound only applies to serializable
  read-write.)

Conflicts are detected and resolved **promptly** — a blocked writer or a deadlock fails fast (sub-second)
rather than stalling on a lock timeout. Deadlocks have a **deterministic winner**: when two transactions
contend, the older one waits and commits while the younger aborts and retries (a *wait-die* ordering), so
two transactions can never both abort each other. One requester is exempt from the age rule: a transaction
that holds no lock and has staged no write yet is nobody's holder, so its wait can never close a cycle, and
it waits out the lock-wait deadline whatever its age. That is what lets a fresh `SELECT`, or a DDL fence such
as `TRUNCATE`, ride out a short in-flight writer instead of failing on its first lock. A staged write counts
as a lock holder here: a shared or exclusive range lock is never granted over another transaction's live
write intent, and the requester is ordered against that writer by the same rule. And all of this works the same on one node or a whole cluster
(§8). Reserve serializable read-write transactions for logic that truly needs an invariant; everything
else is cheaper and fully concurrent under Read Committed.

> **Status — complete and verified.** Serializable is fully implemented, acceptance-tested, and is now
> the **default isolation level**. Read Committed remains available as an explicit opt-out. Both paths
> are reachable through the transaction API: the **read-write**
> (strict-locking) path, and the lock-free **read-only snapshot** — which you can open, resume across
> several requests by transaction id, and commit/roll back like any explicit transaction. The anomaly
> suite (read skew, phantoms, write skew, lost update) passes identically on a **single node and on a
> 3-node cluster**, including multi-partition read-write transactions — the isolation behavior does not
> depend on topology.
>
> Two things to keep in mind. Neither is a correctness gap. **(1)** A conflicting read-write transaction
> is aborted, and nothing resolves it automatically end-to-end: use the retry helper (autocommit) or
> replay from `BEGIN` (explicit), per the retry rule above. **(2)** CamusDB gives serializable ordering
> that is *logically* consistent (via hybrid logical clocks). It does **not** give the real-time,
> wall-clock ordering that systems with specialized clock hardware provide: there is no
> externally-consistent / commit-wait guarantee. That is a deliberate design choice.
>
> The robustness refinements that were once outstanding are now in place: a *wait-die*
> **deadlock-fairness** ordering (a deterministic winner instead of mutual aborts), **lock escalation**
> for very large reads (per-row read locks collapse to one whole-table lock past a threshold), and
> **tighter predicate-lock bounds** so a bounded scan / `UPDATE` / `DELETE` locks only its key range
> rather than the whole table.

### 9.4 A failed statement inside an explicit transaction

CamusDB follows PostgreSQL here. **Any statement that fails inside an explicit transaction aborts
the transaction.** From then on every statement on it, reads included, is refused with `CADB0824`
(the message says the transaction is aborted and commands are ignored until the end of the
transaction block), `COMMIT` rolls it back and reports the same code, and `ROLLBACK` ends it. Each
refusal names the statement that failed. There are no savepoints, so there is no statement-level
recovery: a client that wants to continue after an error rolls back and starts a new transaction.

The rule is the same whether or not the failed statement wrote anything. A
transactional Kahuna batch is all-or-nothing, so a multi-row `INSERT` whose later row conflicts on
the primary key stages nothing: no sibling row, and no second record for the conflicting key. But a
`DELETE` or `UPDATE` that a foreign-key check refuses has its row writes staged, because that check
runs after them, and a statement whose rows needed several batches has the earlier batches staged.
A client cannot tell these cases apart from the error it receives, so one rule covers them all.
MySQL and Oracle instead undo the failed statement alone and keep the transaction usable; that needs
statement-level undo in the storage layer, which Kahuna does not offer.

The engine enforces the rule itself, at every statement entry point: the SQL query and non-query
dispatchers (which the `RETURNING` hand-off, procedures and an extension's SQL session also use), the
DDL dispatcher, and the typed insert / update / delete / query services behind the row endpoints. A
failure while a query's rows stream aborts the transaction too, and so do a cancelled statement and
a statement refused because it cannot run inside a transaction block (such as `ALTER TABLE … REWRITE
STORAGE`), exactly as in PostgreSQL. The HTTP and gRPC transports go one step further and roll an explicit transaction back as soon as a statement in it fails, so a client
of those APIs sees the failure once and then an unknown-transaction error on its next use of the
handle. The engine rule is the guarantee underneath: a caller that reaches the engine directly, an
extension that catches its own SQL error, or a future transport that forgets the rollback cannot
commit, and cannot run anything else on the aborted transaction.

### 9.5 Commit timestamp

The commit of an explicit transaction reports its **commit timestamp**. This is the HLC timestamp
that Kahuna gives to every write of the transaction. The Kahuna transaction coordinator mints it on
its own clock, and that clock can be on a node other than the one that ran your statements.

Where a client reads it:

- gRPC: `commit_timestamp_n`, `commit_timestamp_l` and `commit_timestamp_c` on `CommitReply`, from
  the unary `CommitTransaction` and from a batched `COMMIT` or `COMMIT_IF_OK`.
- HTTP: `commitTimestamp` (`n`, `l`, `c`) on the `/commit-transaction` response.

Compare two values by `l`, then `c`, then `n`, as `HLCTimestamp` does. Keep all three parts. The
value is zero on gRPC, and null on HTTP, in these cases:

- The transaction wrote nothing, for example a read-only transaction or `BEGIN` then `COMMIT`.
- The transaction rolled back, or its commit failed.
- The server is older than these fields.

An autocommit statement does not report a commit timestamp. Use an explicit transaction when you
need the value. A commit that you retry on the same transaction after `CADB0509` reports the same
value as the commit that landed. This is also true when the node that coordinated the commit died
before it answered.

These rules state which transactions are in commit-timestamp order. T1 commits first at c1, and T2
commits at c2.

- ✅ **Write-write on one row.** When T1 and T2 write the same row, c2 is above c1. Each write of a
  Kahuna key gets a later commit timestamp than the write before it, on every node.
- ⚠️ **Write-read across partitions.** T2 reads a value that T1 wrote, and then T2 commits. Kahuna
  mints c2 above c1 only when T2's read was registered with the coordinator. CamusDB registers the
  reads of an optimistic transaction and of a transaction with read validation, and a read of a key
  that the transaction already wrote. A pessimistic transaction (the default locking mode) does not
  register its other reads. For such a read, c2 can be below c1 when T1's coordinator clock is ahead
  of T2's nodes. Raft messages carry a clock lead from node to node, so the lag is usually about one
  Raft heartbeat, and it can be longer on an idle partition. The Kahuna transaction lifecycle guide
  (section 6.1.1, "Commit timestamp order") gives the full rule.
- ⚠️ **Read-write anti-dependency.** T1 reads a row, T2 then overwrites that row, and T1 commits
  after T2. No order is promised: c1 can be above c2.
- ⚠️ **No shared row.** Two transactions that share no row and no read have no order promise. Their
  commit timestamps come from clocks that can differ between nodes.

The **causal token** is a different value. It is a reading of the clock of the node that ran the
commit, taken after the commit, so it is at or above the commit timestamp. Use the token as the floor
for a later read (§9.2). Do not use it to put commits in order: when two commits run through different
nodes, their tokens can be in the opposite order of their commits.

To put the requests of one client session in order, make each request write the same row, for
example a row that holds the session's last request number. The write-write rule then puts the
commits of the session in commit-timestamp order, on every node.

### 9.6 Read-only transactions

A transaction is read-only when one of these is true:

- It was started with `"transactionMode": "ReadOnly"` on HTTP, or `TRANSACTION_MODE_READ_ONLY` on
  gRPC. This applies to `/start-transaction` and to a single autocommit statement.
- Its first statement was `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY`.

A read-only transaction refuses two groups of statements with `CADB0849` (HTTP 400, gRPC
`FAILED_PRECONDITION`). The message names the command, for example
`cannot execute UPDATE in a read-only transaction`. This is PostgreSQL's `read_only_sql_transaction`.

- Statements that write rows: `INSERT` (with or without `ON CONFLICT`), `UPSERT`, `INSERT … SELECT`,
  `UPDATE`, `DELETE`, `UPDATE … FROM`, `DELETE … USING`, and `CREATE TABLE … AS`.
- Statements that change state outside the transaction, which a rollback cannot undo: schema DDL,
  `COMMENT ON`, `REFRESH MATERIALIZED VIEW`, `nextval()` and `setval()`. DDL and `REFRESH` commit in
  transactions of their own, and a sequence is not transactional.

The statement is refused before it changes anything, reads a row or takes a lock. The `RETURNING`
form, the typed row endpoints, and the SQL of a procedure or a trigger get the same refusal. A refused
statement is a failed statement, so §9.4 applies to the transaction after it. `SELECT … FOR UPDATE` is
refused too (§6.6), with `CADB0400`, at both isolation levels.

The second group is refused only when the client asked for the read-only mode
(`KvTransaction.IsDeclaredReadOnly`). An autocommit `SELECT` runs in a read-only snapshot that the
server picks, and a request that asked for no mode can still call `nextval()` there. The same request
with `"transactionMode": "ReadOnly"` is refused. An internal job, such as a restarted materialized-view
rebuild, runs `REFRESH` under a server-picked snapshot too. The first group is refused in every
read-only transaction, because such a snapshot cannot write.

The rule protects against a lost write. The commit of a Serializable read-only transaction is a
rollback of its empty Kahuna transaction, so a write in it would be discarded while the client is told
that it committed. As a second guard, the commit of a read-only transaction that staged a write fails
with `CADB0849` and rolls the transaction back.

Database-scoped statements (`CREATE DATABASE`, `CREATE USER`, `GRANT`, `SET CLUSTER SETTING`) are not
affected: every transport runs them with no transaction, so no access mode applies to them.
`CREATE TABLE … AS … WITH NO DATA` loads no rows and is DDL, so it is in the second group.

`SET TRANSACTION` and the access mode:

- `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY` pins one snapshot when it runs, as a
  transaction started read-only at Serializable has. Every later read sees the data as of that
  instant, and takes no lock.
- `SET TRANSACTION … READ WRITE` is refused with `CADB0400` in a transaction that reads at a fixed
  snapshot: one started read-only at Serializable, or one that `SET TRANSACTION … READ ONLY` already
  pinned. Such a transaction has no write session.
- A Read Committed read-only transaction has no fixed snapshot. As its first statement,
  `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE` makes it a Serializable read-write
  transaction. PostgreSQL also allows a read-only transaction to become read-write there.

---

## 10. Where this lives in the code

A map for when you want to read the real thing:

- **`CamusDB.Core/Transactions/`** — `KvTransaction` (a transaction's identity — its Kahuna timestamp
  and coordinator key — plus the concurrency policy it was begun with) and `KvTransactionsManager`
  (begin / commit / rollback, including the retry on transient commit conflicts). The authoritative set
  of modified keys and held locks lives in the Kahuna coordinator, not on `KvTransaction`.
  `KvTransaction.CommitTimestamp` keeps the commit timestamp from the Kahuna commit answer (§9.5).
  `KvTransaction.QueryReadView` decides the timestamp and Kahuna read mode of a query read (the
  statement timestamp of a Read Committed `SELECT`, §9.2), and `LatestReadView` is the read of a
  write path. `SelectStatementExecutor` opens the statement snapshot and closes it when the cursor
  ends. A write statement opens it only for its read phase (`StatementSnapshotScope`, minted in
  `NonQueryStatementDispatcher.MintStatementTimestamp`) and closes it before the write phase.
  `ReadOnlyStatementRule` (in `Commands/Executor/Controllers/`) decides which statements a read-only
  transaction refuses (§9.6); the CTAS executor and `SequenceStatementBinder` check their own cases.
- **`CamusDB.Core/Storage/Kv/KvTableStore.cs`** — the per-table data layer: how rows and index entries
  are written/read as keys, the batched write path, and the range-lock methods
  (`AcquireRowRangeLockAsync`, `AcquireIndexRangeLockAsync`, `AcquireBoundedIndexRangeLockAsync`).
- **`CamusDB.Core/Commands/Executor/Controllers/Queries/`** and `QueryExecutor.cs` — the scan paths
  where read locks (when enabled) are acquired before scanning.
- **`CamusDB/App/Controllers/`** — the HTTP entry points; note where queries begin **read-only**
  transactions vs writes begin read-write ones.
- **Kahuna (dependency)** — the actual MVCC store, per-key locks, range locks, and the server-owned
  two-phase-commit **transaction coordinator**. CamusDB calls into it through the `IKahuna` interface.
  How CamusDB drives that coordinator — operation registration, folding, handle-based commit, optimistic
  locking, and coordinator-owned range-lock renewal — is documented in
  `kahuna-transaction-coordinator.md`.

---

## 11. Quick mental checklist

When reasoning about a concurrency question in CamusDB, ask in this order:

1. **What isolation level is this transaction?** Serializable (the default) or Read Committed
   (explicit opt-out)? That decides almost everything below. If nothing opted out, it's Serializable.
2. **Is it a read or a write?** Read Committed reads and Serializable read-only reads are lock-free
   MVCC; Serializable read-write reads take locks; all writes take per-key locks + 2PC.
3. **Is it a scan or a point lookup?** A point lookup reads one key; a scan over a range is what can
   take a range (predicate) lock — for Serializable read-write transactions only, in every routing
   mode. A Read Committed scan and an autocommit `SELECT` never take one.
4. **Single partition or many?** Many partitions means writes/locks happen on partition leaders and a
   transaction may run a 2PC across them — but the isolation guarantees are the same either way.
5. **What guarantee does the app need?** For a consistent multi-statement read, use a
   Serializable **read-only** transaction (lock-free snapshot). For a read-then-write invariant, use a
   Serializable **read-write** transaction (the default) — keep it short and wrap it in a retry loop
   (it can be aborted on conflict) — or enforce it with a unique constraint. For maximum throughput with
   no invariant requirements, opt down to Read Committed explicitly.
