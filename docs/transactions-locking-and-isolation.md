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

CamusDB does **not** implement its own storage engine, locking, or replication from scratch. It is a
SQL layer on top of two lower-level building blocks. Understanding isolation in CamusDB means
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

**Rule of thumb:** when you ask "what isolation does CamusDB give me?", you're really asking "how does
CamusDB *use* Kahuna's transactions?" CamusDB rows and indexes are just key/value pairs in Kahuna.

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
- **Read-only vs read-write transaction** — a crucial distinction in CamusDB; see §4 and §5.

---

## 3. How CamusDB lays out data as keys

Every row and index entry is a key/value pair. The key encodes which table, what kind of data, and
which row:

```
Primary rows        {tableId}:r/{rowId}                → the serialized row bytes
Unique index        {tableId}:i:{indexId}/{value}      → the rowId it points to
Non-unique index    {tableId}:i:{indexId}/{value}{rowId}
Schema / catalog    {db}/meta/...                      → table definitions, etc.
```

The part of the key *before the last `/`* is its **key space** (e.g. `{tableId}:r`). Key spaces matter
for routing and range locks (§6, §8).

**Routing — how a key finds its partition.** Kahuna offers two strategies:

- **Hash routing (the default).** The key space is hashed to a partition. Simple and even, but
  contiguous keys land on *different* partitions, so you can't lock "all rows between A and B" cheaply.
- **Key-range routing (opt-in, off by default).** Contiguous keys are kept together in one range, so a
  scan over `[A, B)` touches one place and can be locked as a range. Enabled per key space, and only
  has an effect when the cluster has at least two partitions. Toggled with the
  `CAMUS_KEY_RANGE_SHARDING` environment variable.

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
  transaction B both try to write the same row, one of them sees the other's intent/lock and fails
  (rather than silently clobbering). This is **write-write conflict detection**.
- **2PC makes the row and its indexes commit together.** You never observe a state where the row
  exists but its index entry doesn't.
- **Locks are held until the transaction ends** (commit or rollback), then released. CamusDB tracks
  every lock and intent on the transaction object so it can release them all at the end.
- **Schema safety.** A write also "pins" the schema version of the tables it touches at the start, and
  the commit is rejected if the schema changed underneath it (e.g., a column was dropped mid-write).
  This keeps DDL and DML from corrupting each other.

---

## 6. The locks CamusDB uses, and when

There are two distinct lock mechanisms. Knowing which is taken when is most of the mental model.

### 6.1 Per-key locks & write intents — used by every write
These are taken automatically during the PREPARE phase of any write (§5), one per key being modified.
They serialize **conflicting writes to the same key**. They are *not* taken by reads.

### 6.2 Range locks — used by serializable scans, only in key-range mode
A range lock reserves a whole *range* of keys at once, e.g. "all index entries between 10 and 50."
Their purpose is **phantom protection**: stopping another transaction from inserting, changing, or
deleting a row inside a range you are scanning, which would otherwise make a repeated scan return
different results.

Two things make range locks useful rather than a bottleneck:

- **They are *shared* for scans.** Two transactions scanning overlapping ranges both succeed and run
  side by side — a scan never blocks another scan. (Reads don't block reads, even serializable ones.)
- **Writes respect them automatically.** A write does not need to take a range lock itself: when a
  transaction tries to insert/update/delete a key that falls inside a range another transaction has
  locked, that write is held back (it retries). This is what actually stops a phantom row from
  appearing mid-scan — including a *brand-new* key that didn't exist when the scan started.

Range locks are acquired by the scan paths in the query executor (full-table scans, index scans,
bounded index range scans, and `IN`-list scans). But there are important conditions:

- They are a **no-op unless key-range routing is enabled** (`CAMUS_KEY_RANGE_SHARDING=1`) for that key
  space. In the default hash-routed, single-partition setup, **no range locks are taken at all**.
- A scan only holds one if it has a real transaction identity. In key-range mode a standalone `SELECT`
  is automatically promoted to a lightweight read-only transaction so it *can* hold a shared range lock
  (and releases it the moment the query finishes). **Point lookups by id** don't scan a range, so they
  skip this entirely and stay on the fast path.

So in the default configuration the range-lock machinery is effectively dormant; it only comes alive
when you opt into key-range routing on a multi-partition cluster — and there it gives scans a
serializable, phantom-free view without readers ever blocking each other.

```
            Reads (SELECT)                Writes (INSERT/UPDATE/DELETE)
            ──────────────────────────    ─────────────────────────────
per-key     none                          one per modified key
range       none in hash mode;            none taken directly; but a write
            a SHARED range lock for a     into a range another txn has
            scan in key-range mode        locked is held back until that
            (scans never block scans)     txn finishes (phantom protection)
```

---

## 7. MVCC, in one picture

MVCC is the reason reads stay fast. Imagine three versions of one key over time:

```
key "robots:r/42"

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

The **locking and isolation model is the same** in both. The cluster simply distributes the same
machinery across more nodes and partitions. Here's the mapping:

| Concept | Single node (default) | Cluster |
|---|---|---|
| Partitions | usually one | many; each is its own Raft group with a leader |
| Routing | one place, so routing is trivial | keys routed to partitions by hash (or by range, if enabled) |
| Where a write commits | local Raft (a one-node "quorum") | the partition **leader**, replicated to a node majority |
| Multi-key write | 2PC, local | 2PC across the **leaders** of every partition the keys touch |
| Range locks | dormant (hash, single partition) | active if key-range routing is enabled (needs ≥ 2 partitions): shared scan locks + writes held back from locked ranges |
| Reads | MVCC, lock-free | MVCC, lock-free (served by the partition leader of each key) |
| Schema/DDL | a dedicated, totally-ordered key space | same key space, kept on a single partition so all nodes agree on schema order |

A few cluster-specific notes for newcomers:

- **Every partition has a leader.** Writes and locks for a key are coordinated by that key's partition
  leader, then replicated. The HLC timestamp a transaction gets is meaningful across the whole cluster
  (that's the point of a hybrid logical clock).
- **A transaction can span partitions.** If your write touches keys that route to different
  partitions, the two-phase commit coordinates across those partition leaders. The atomicity guarantee
  still holds.
- **Key-range routing buys range-scan concurrency, not data distribution.** Today every node still
  stores all the data; key-range routing changes *where locking and ordering happen*, letting a scan
  over `[A,B)` avoid blocking a writer working on a disjoint `[B,C)`. It does not spread storage across
  nodes.

If you want the deep story on how schema changes (DDL) replicate and stay consistent across the
cluster, see `distributed-schema-architecture.md` — this document deliberately focuses on data-path
reads/writes.

---

## 9. What consistency you can rely on **today**

Be precise about this — it's the part app developers most need, and where it's easy to over-promise.
What CamusDB guarantees right now:

- ✅ **Atomic transactions.** A transaction's changes all apply, or none do.
- ✅ **Durable commits.** A committed write has been replicated to a majority and survives node
  failure.
- ✅ **No dirty reads.** You only ever read committed data; in-flight write intents are invisible.
- ✅ **Write-write conflict detection.** Two transactions that modify the same row don't silently
  overwrite each other — one fails and can retry.
- ✅ **Non-blocking reads.** Concurrent reads never block each other or writers.

What is **not yet fully guaranteed** in the **default (hash-routed) configuration** (so design around
it):

- ⚠️ **A single global snapshot per query is not guaranteed.** A standalone `SELECT` reads the *latest
  committed* version of each key. A single scan is internally consistent, but a query that does
  several sub-reads (e.g. an index lookup followed by fetching the matching rows) can observe values
  committed at slightly different instants.
- ⚠️ **Repeatable reads / phantom protection across a multi-statement transaction are not guaranteed**
  in the default configuration. If you read a set of rows, then read again, a concurrent transaction
  may have changed or inserted rows in between.

In classic terms, the practical isolation level in the default configuration is approximately **Read
Committed**, plus atomic durable writes and write-write conflict detection. For invariants that must
hold under concurrency (e.g. "no two robots with the same serial"), rely on **unique constraints**
(enforced at the key level) and explicit transactions, rather than assuming serializable behavior.

**With key-range routing enabled, scans get stronger guarantees.** Because a scan holds a shared range
lock and any conflicting write is held back until the scan's transaction finishes, a `SELECT` over a
range in key-range mode is **phantom-free and sees a consistent view of that range** — two readers
still run concurrently, but a writer can't slip a row into the range mid-scan. This is the serializable,
range-scan behavior key-range mode is designed to provide. Point lookups by id and all reads in the
default hash mode remain at the Read Committed level described above.

> Stronger isolation (consistent snapshot reads and full serializability) is an area of active
> development. The building blocks — server-assigned timestamps, MVCC, per-key locks, range locks, and
> a transaction coordinator that can validate read sets — are already in place; wiring them into a
> stronger guarantee is ongoing.

---

## 10. Where this lives in the code

A map for when you want to read the real thing:

- **`CamusDB.Core/Transactions/`** — `KvTransaction` (a transaction's identity, its tracked locks and
  modified keys) and `KvTransactionsManager` (begin / commit / rollback, including the retry on
  transient commit conflicts).
- **`CamusDB.Core/Storage/Kv/KvTableStore.cs`** — the per-table data layer: how rows and index entries
  are written/read as keys, the batched write path, and the range-lock methods
  (`AcquireRowRangeLockAsync`, `AcquireIndexRangeLockAsync`, `AcquireBoundedIndexRangeLockAsync`).
- **`CamusDB.Core/Commands/Executor/Controllers/Queries/`** and `QueryExecutor.cs` — the scan paths
  where read locks (when enabled) are acquired before scanning.
- **`CamusDB/App/Controllers/`** — the HTTP entry points; note where queries begin **read-only**
  transactions vs writes begin read-write ones.
- **Kahuna (dependency)** — the actual MVCC store, per-key locks, range locks, and two-phase commit
  coordinator. CamusDB calls into it through the `IKahuna` interface.

---

## 11. Quick mental checklist

When reasoning about a concurrency question in CamusDB, ask in this order:

1. **Is it a read or a write?** Reads are lock-free MVCC; writes take per-key locks + 2PC.
2. **Is it a scan or a point lookup?** A point lookup (by id) just reads its key. A scan over a range is
   what can take a range lock — and only in key-range mode.
3. **Is key-range routing enabled?** If not (the default), range locks are dormant — only per-key write
   locks are in play, and the isolation level is ≈ Read Committed. If it is, a scan holds a shared range
   lock and writes are held back from a locked range, giving phantom-free, serializable range scans
   (readers still never block readers).
4. **Single partition or many?** Many partitions means writes/locks happen on partition leaders and a
   transaction may run a 2PC across them.
5. **What guarantee does the app actually need?** If it needs an invariant under concurrency, lean on
   unique constraints and explicit transactions; remember the default level is ≈ Read Committed, with
   stronger range-scan guarantees available under key-range routing.
