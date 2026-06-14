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

### 6.2 Range locks — phantom protection for scans
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
bounded index range scans, and `IN`-list scans). They fire in two situations:

- **For a Serializable read-write transaction** (see §9), on *any* configuration including the default
  single-node hash setup — a serializable scan needs phantom protection to be correct, so the lock is
  taken regardless of routing.
- **In key-range routing mode**, for ordinary scans, so a scan over `[A, B)` doesn't block a writer on
  the disjoint `[B, C)`.

Otherwise — a plain Read Committed `SELECT` in the default hash setup — **no range lock is taken**.

### 6.3 Point read locks (Serializable read-write only)
A Serializable read-write transaction also locks the *individual* keys it reads with `GetRow` or a
unique-index lookup — a one-key "shared point lock," held until the transaction ends. This is what
makes "read a row, then act on it" safe: no other transaction can change that exact key underneath you.

- Two serializable transactions reading the **same** key coexist (both hold a shared lock).
- A writer trying to change a key a serializable reader holds is **held back / fails to commit** until
  the reader finishes.
- If the *same* transaction later **writes** a key it read, its shared lock is **promoted to exclusive**
  in place, so from that point no one else can even read that key until it commits.

These point locks are taken **only** by Serializable read-write transactions. Read Committed reads and
Serializable read-only (snapshot) reads take none.

**Escalation for large reads.** To keep a transaction that reads thousands of rows from accumulating
thousands of individual point locks, once the per-table point-lock count passes a threshold the
transaction collapses them into a single whole-table shared lock and stops taking new per-row locks.
This bounds the lock bookkeeping at the cost of locking a little more of the table than strictly read.

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
| Range locks | active for Serializable transactions; otherwise dormant in the hash default | additionally active for ordinary scans when key-range routing is enabled (≥ 2 partitions) |
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
- **Key-range routing buys range-scan concurrency, not data distribution.** Today every node still
  stores all the data; key-range routing changes *where locking and ordering happen*, letting a scan
  over `[A,B)` avoid blocking a writer working on a disjoint `[B,C)`. It does not spread storage across
  nodes.
- **Isolation behaves identically on one node or many.** A Serializable transaction gives the same
  result whether the data lives on one partition or several — single node is just the one-partition
  case of the same algorithm. A client can't tell from the isolation behavior how many nodes there are.

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

Those five hold at **every** isolation level. On top of them, CamusDB offers **two isolation levels**,
chosen per transaction (the default applies when you don't ask for anything).

### 9.1 Serializable (the default)

Every autocommit statement and every transaction runs at Serializable unless it opts down to Read
Committed. See §9.2 for the full description.

**Opting out to Read Committed.** If you don't need the full serializable guarantee and want the
absolute minimum overhead, you can request Read Committed per transaction — either via the isolation
field on the begin-request, or as the first statement of an explicit transaction:

```sql
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
```

There is also a server-wide knob (`DefaultIsolationLevel` in `CamusDBConfig`) to revert the default
globally, if you need to roll back an environment to the old behaviour.

### 9.2 Read Committed (explicit opt-out)

This is the lock-free behavior described in §4/§5. What it does **not** promise (design around these):

- ⚠️ **No single global snapshot per query.** A statement reads the *latest committed* version of each
  key. One scan is internally consistent, but a query that does several sub-reads (an index lookup,
  then fetching the rows) can observe values committed at slightly different instants.
- ⚠️ **No repeatable reads / phantom protection across statements.** Read a set of rows, read again,
  and a concurrent transaction may have changed or inserted rows in between.
- ⚠️ **Write skew is possible.** Two transactions can each read a set, then write disjoint keys based on
  what they read, in a way no serial order would allow.

For invariants that must hold under concurrency at this level (e.g. "no two robots with the same
serial"), lean on **unique constraints** (enforced at the key level) rather than read-then-decide logic.

### 9.3 Serializable (detail)

A transaction can ask for **Serializable** — the strongest level, where the end result is always
equivalent to running the transactions one-at-a-time in *some* order. Under the hood CamusDB uses two
different strategies, picked automatically from whether the transaction is read-only or read-write:

- **Serializable read-only → a consistent snapshot.** The transaction is pinned to a single timestamp
  at the moment it begins and reads *every* key as of that instant — across statements and across
  partitions. It sees no write committed after it started: no read skew, no phantoms, fully repeatable.
  And it does this **without taking any locks**, so it never blocks writers and is never blocked. You can
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
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;            -- read-write (strict locking) — the default
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY;  -- snapshot, no locks
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;          -- opt down to read committed
```

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
  kept alive by a background lease-renewal heartbeat, so it can stay open for a genuinely long
  interactive session. There is still a hard **maximum lifetime** (about one hour) as a backstop: if a
  transaction outlives it, it is **aborted with a clear error** at its next operation or at commit — it
  never silently loses its isolation. (This bound only applies to serializable read-write.)

Conflicts are detected and resolved **promptly** — a blocked writer or a deadlock fails fast (sub-second)
rather than stalling on a lock timeout. Deadlocks have a **deterministic winner**: when two transactions
contend, the older one waits and commits while the younger aborts and retries (a *wait-die* ordering), so
two transactions can never both abort each other. And all of this works the same on one node or a whole cluster
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
> Two things to keep in mind, neither a correctness gap. **(1)** A conflicting read-write transaction is
> aborted, not auto-resolved end-to-end: use the retry helper (autocommit) or replay from `BEGIN`
> (explicit), per the retry rule above. **(2)** CamusDB gives serializable ordering that is *logically*
> consistent (via hybrid logical clocks), **not** the real-time, wall-clock ordering that systems with
> specialized clock hardware provide — there is no externally-consistent / commit-wait guarantee, and
> that is a deliberate design choice, not a missing piece.
>
> The robustness refinements that were once outstanding are now in place: a *wait-die*
> **deadlock-fairness** ordering (a deterministic winner instead of mutual aborts), **lock escalation**
> for very large reads (per-row read locks collapse to one whole-table lock past a threshold), and
> **tighter predicate-lock bounds** so a bounded scan / `UPDATE` / `DELETE` locks only its key range
> rather than the whole table. Any further optional refinements are tracked in
> `../specs/serializable-isolation-future-work.md`.

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

1. **What isolation level is this transaction?** Serializable (the default) or Read Committed
   (explicit opt-out)? That decides almost everything below. If nothing opted out, it's Serializable.
2. **Is it a read or a write?** Read Committed reads and Serializable read-only reads are lock-free
   MVCC; Serializable read-write reads take locks; all writes take per-key locks + 2PC.
3. **Is it a scan or a point lookup?** A point lookup reads one key; a scan over a range is what can
   take a range (predicate) lock — for Serializable transactions, or for ordinary scans under key-range
   routing.
4. **Single partition or many?** Many partitions means writes/locks happen on partition leaders and a
   transaction may run a 2PC across them — but the isolation guarantees are the same either way.
5. **What guarantee does the app actually need?** Need a consistent multi-statement read? Use a
   Serializable **read-only** transaction (lock-free snapshot). Need a read-then-write invariant? Use a
   Serializable **read-write** transaction (the default) — keep it short and wrap it in a retry loop
   (it can be aborted on conflict) — or enforce it with a unique constraint. For maximum throughput with
   no invariant requirements, opt down to Read Committed explicitly.
