# The Kahuna Transaction Coordinator

> **Audience:** engineers who work on the CamusDB storage and transaction layers, or on DML and
> DDL code.
> **What you'll learn:** how CamusDB drives transactions through Kahuna's *server-owned*
> transaction coordinator. The guide covers what the coordinator owns and how each operation is
> registered and folded into the transaction. It also covers commit and rollback, what
> "pessimistic" and "optimistic" locking do, and the invariants you must preserve when you change
> a path that accesses or locks data.
>
> Assumes you've read **Transactions, Locking & Isolation** for the isolation-level picture.
> This guide is about the *mechanism* underneath it.

---

## 1. The big picture

CamusDB embeds **Kahuna** (a transactional KV store) in-process and drives its `IKahuna`
surface directly. Transaction operations on a single node make no network hop.
Kahuna now provides a **server-owned transaction coordinator**. The server owns the
authoritative state of an in-flight transaction, and the client contributes to that state one
operation at a time. Under the old model, the client assembled that state locally and shipped it
at commit.

The change moves *ownership* of the working set from the client to the server.

```
        OLD: client-owned working set                NEW: server-owned coordinator
  ┌────────────────────────────────┐          ┌────────────────────────────────┐
  │ KvTransaction accumulates:      │          │ Coordinator (server) owns:      │
  │  • every lock it acquired       │          │  • modified keys                │
  │  • every key it modified        │   ==>    │  • held point/prefix/range locks│
  │                                 │          │  • read observations            │
  │ commit(id, locks, modified, …)  │          │  • policies + 2PC state         │
  │ ships the whole set to the srv  │          │ commit(handle)  ← handle only   │
  └────────────────────────────────┘          └────────────────────────────────┘
```

Under the old model the client had to remember everything the transaction touched and hand
that list to `LocateAndCommitTransaction`. Under the new model the client hands the server only a
**handle**. The server already knows the working set: each operation *registered* its confirmed
effect as it happened.

---

## 2. The transaction handle

A transaction is identified by a `TransactionHandle`, which has two parts.

- **`TransactionId`** — the Kahuna `HLCTimestamp` minted by `LocateAndStartTransaction`. It is
  passed as the `transactionId` argument on each KV operation.
- **`CoordinatorKey`** — a stable string (CamusDB uses a fresh GUID per transaction). It pins
  the transaction's server-side session to one partition leader and routes each registered
  operation, plus commit and rollback, to that session.

`KvTransaction` exposes `TransactionId` and `CoordinatorKey`, plus the combined `Handle`.
Commit and rollback take the handle as their only transaction argument.

```csharp
(result, _) = await kahuna.LocateAndCommitTransaction(tx.Handle, cancellationToken);
await kahuna.LocateAndRollbackTransaction(tx.Handle, cancellationToken);
```

There is no client-built key list at finalize. The coordinator finalizes exactly the working
set it accumulated.

---

## 3. Operation registration and folding

Each transactional operation carries two extra arguments: the transaction's `CoordinatorKey`
and a per-operation `TransactionOperationId`. Together they *register* the operation with the
coordinator. The coordinator then **folds** the operation's confirmed effect into the
server-owned working set, and what folds depends on the operation.

| Operation | What folds into the working set |
|---|---|
| A successful write / delete | the modified key **and its implicit point lock** |
| An explicit point-lock acquire | the held point-lock descriptor |
| A range-lock acquire | the held range-lock descriptor (bounds + mode) |
| A read of the latest committed value | an observation: the key's existence and base revision |
| A registered scan | one read observation per row it returns |

Registration has two consequences for code that writes or takes locks.

1. **A transactional write or lock that is *not* registered is invisible to commit.** If a
   write goes out with an empty coordinator key, its effect does not fold into the working set.
   This means that `commit(handle)` does not commit it, and the row reads back as if it had not
   been written. Each transactional mutation and lock on a real (non-zero) transaction id
   **must** pass the coordinator key and an operation id.

2. **A write already registers its implicit point lock.** A separate explicit lock is not needed
   for the write to commit, since the write folds both the modified key and the lock. Explicit locks provide *mutual exclusion
   before* the write (see pessimistic locking below). They do not make the write durable.

The zero-identity fast path is the exception. A read-only transaction backed by
`HLCTimestamp.Zero` performs non-transactional, read-committed reads. No coordinator session is
opened for it. It registers no operations, and its commit and rollback are no-ops. Point reads and
single-partition read paths use this path.

---

## 4. Idempotent retry

Kahuna's transport may re-deliver a call, and CamusDB itself retries transient routing signals
(`MustRetry` / `WaitingForReplication`). Registration makes these retries safe, and the operation
id decides whether a retry is replayed or applied as new work.

- A retry that reuses the **same** operation id replays the cached completion. The operation is
  applied once.
- A retry that describes **new** work uses a **new** operation id.

CamusDB follows this rule mechanically. A single-key operation mints one operation id and
reuses it across its transient retries (a stable id = idempotent replay). A batch operation
that resends only the pending subset mints a **fresh** id per attempt. A shrunken batch is
different work. When in doubt: one logical operation, one operation id.

**The replay includes a failure.** The cached completion is whatever the first attempt returned.
A lock attempt that got `AlreadyLocked` replays `AlreadyLocked` (with an unknown holder) for as
long as the same id is sent, even after the holder commits and leaves. A retry that waits for a
holder is therefore new work. Mint a fresh id after `AlreadyLocked`, and keep the id only after
`MustRetry`, where the first attempt may not have finished. `CatalogKeyLock.AcquireExclusiveAsync`
does this for catalog, registry and auth key locks.

---

## 5. Pessimistic vs optimistic locking

The concurrency strategy is chosen per transaction when it begins (via `BeginAsync`), and the
default is pessimistic. It is **orthogonal** to the SQL isolation level: a transaction is
independently Serializable-or-ReadCommitted *and* Pessimistic-or-Optimistic. Clients select it
through the `locking` request field or `SET TRANSACTION LOCKING` (see
`transactions-locking-and-isolation.md` §6.4). The rest of this section describes the mechanism.

### Pessimistic (the default)

Before each write, CamusDB acquires an exclusive point lock on the key and then writes. A second
transaction that wants the same key **blocks** (or is aborted by deadlock avoidance) until the
first finalizes. Conflicts are resolved at lock-acquisition time. The engine has always behaved
this way. It gives straightforward read-committed-or-stronger isolation, and clients do not need a
retry loop for the common case.

### Optimistic (opt-in)

An optimistic transaction skips the explicit **exclusive write lock**. Its writes fold their
implicit point locks and modified keys as usual, and its reads fold observations. No operation
blocks *on the write path*. Write-write and read-write conflicts are detected at **commit**,
during prepare.

- **Write–write:** two optimistic transactions that modified the same key both try to place a
  write intent on it at prepare. The second one conflicts and aborts. Exactly one wins.
- **Read–write / write-skew:** the transaction's read observations are re-validated against the
  current committed revisions. If a key it read was modified and committed by another transaction in
  the meantime, the commit aborts. This happens even when the two transactions wrote disjoint keys.

The write path skips explicit lock acquisition for an optimistic transaction, and the read path
registers reads so they can be validated (see the next section). A losing optimistic transaction
surfaces as a `CamusDBException` from `CommitAsync`.

**The isolation level decides whether "optimistic" is lock-free.** The locking mode changes one
thing: whether the write lock is skipped. The *isolation level* gates the read/scan **predicate
locks** in both locking modes.

- **Read Committed + Optimistic** takes no predicate locks and is fully lock-free. Its validation
  covers *read observations*, which are the specific rows the transaction read. It does not cover
  *predicates*. This means that it does **not** protect against a concurrent insert of a new row
  that would have matched a `WHERE` clause (a phantom). If you need phantom protection here, use
  Serializable.
- **Serializable + Optimistic** is a **hybrid**. The write path is optimistic (no explicit write
  lock, commit-time validation). However, reads and scans take **shared range/point predicate
  locks**, and a following write upgrades them to exclusive, exactly as under Serializable +
  Pessimistic. This means that it *is* phantom-free and it is *not* lock-free. Serializable is
  deliberately not weakened to lock-free. That would silently break its isolation guarantee.

(Read Committed is the fully phantom-vulnerable, lock-free read path. The lock-free *and*
phantom-free option is a Serializable **read-only snapshot**. It acquires no locks: it reads a
fixed MVCC timestamp, which is a different mechanism from optimistic locking.)

### Deferred session start (so `SET TRANSACTION LOCKING` can work)

Kahuna **pins the locking mode when the coordinator session opens**. The mode cannot change once
the session exists. This works when the mode is known at `BeginAsync` (the request field, or the
server default). However, `SET TRANSACTION LOCKING` is issued as the *first statement* of an
already-open client transaction, after `/start-transaction` has returned. That is later than the
session would normally start.

To handle this case, an **explicit** transaction begun through `/start-transaction`
(`BeginAsync(deferStart: true)`) is created as a shell, and its Kahuna session is not opened yet.
Its `TransactionId` is `Zero` until then, and a separately minted, stable `ClientId` is what the
coordinator tracks and returns to the client. The session opens lazily on the transaction's
**first** write or lock, or its first folded read, through `EnsureSessionStartedAsync`. That method
reads the *then-current* locking mode. This means that a `SET TRANSACTION LOCKING` or
`SET TRANSACTION ISOLATION` issued first takes effect. `EnsureSessionStartedAsync` opens the
session exactly once even under concurrent first operations, and it is a no-op once the session
exists.

Deferral is **opt-in and used only for that explicit path**. Internally begun transactions
(catalog, registry, statistics) and autocommit statements start eagerly. Their write and lock
paths always have a live session, and deferral does not affect them. A deferred transaction can
only reach the write/lock/scan paths in `KvTableStore`, which call `EnsureSessionStartedAsync`
first. DDL executes in a separate eager transaction and does not use the deferred one. A deferred
transaction committed or rolled back before it opened a session (for example, `BEGIN` followed
directly by `COMMIT`) finalizes through the zero-identity fast path, and no Kahuna round-trip is
made.

---

## 6. Read-set folding

For read-set validation to work, the reads a transaction performs must be registered so their
observations fold into the working set. CamusDB folds reads only if the transaction wants
validation. `KvTransaction.FoldReads` captures that condition. It is true if **all** three
conditions below hold, and false otherwise.

- the transaction is Optimistic, **or** explicitly requested read-set tracking;
- it has a real transaction identity (not the `HLCTimestamp.Zero` fast path);
- it reads the *latest* committed value. A read at a pinned historical snapshot is not a live
  dependency and folds no observation.

When `FoldReads` is true, the read path passes the coordinator key and an operation id on each
read. This applies to point reads and batch row fetches as well as table and index scans. When it
is false, the read path passes an empty coordinator key and behaves exactly as it always did. The
read is unregistered and relies on locks, or on neither locks nor registration for a snapshot
read. This gate leaves the default pessimistic path byte-for-byte unchanged.

A table or index scan has **two**
implementations: a direct streaming path for root tables and a merge path for branch databases.
Both must thread the coordinator key. Otherwise an optimistic scan on one table shape would
silently skip validation.

---

## 7. Commit-decision durability

Each transaction also names a commit-decision durability, `best_effort` (the default) or
`durable`. **For SQL data the two give the same guarantee.** Kahuna finalizes a transaction
that modified a persistent key through its durable-intent path. It writes a canonical decision
record and prepared intents, and recovery resolves an undecided transaction by presumed abort.
A transaction whose persistent writes cannot take that path aborts. Neither value uses an
in-memory decision for persistent keys. All of CamusDB's row, index, and schema writes are
persistent.

The two values differ only in how they treat ephemeral (in-memory) keys.

- **Best-effort** lets a transaction mix ephemeral keys with persistent ones. The ephemeral writes
  are applied in memory after the durable decision. A transaction whose only write is one
  ephemeral key may also finalize in one turn of the actor that owns that key.
- Under **Durable**, a transaction that modified any ephemeral key is not allowed, and a decision
  anchor is required. The coordinator assigns the anchor from the first confirmed persistent write.
  This means that the client does not need to supply it. A durable transaction does not take the
  one-actor-turn shortcut.

Because CamusDB writes no ephemeral keys, neither difference changes what a SQL transaction
commits or what survives a coordinator crash.

---

## 8. Range locks are coordinator-owned

Serializable read-write transactions take **range locks** to fence out phantoms during a scan.
These locks are registered like any other operation. They fold into the working set, and the
coordinator renews them while the session is alive.

A range lock has a TTL. Previously CamusDB used a background *heartbeat* that periodically
re-acquired the range locks of each live transaction. It refreshed the TTL so that long
transactions kept their locks. That heartbeat is **gone**. Range-lock acquisitions now register
with the coordinator. The coordinator renews a live session's range locks for the duration of the
session and releases them on finalize. An abandoned session (a client that opened a transaction
and vanished) is bounded by the session timeout. The server reaper reclaims it and releases its
locks.

This gives one timing rule. The coordinator renews on its periodic collection tick. That tick is a
coarse interval (tens of seconds), much longer than the sub-second cadence the old client heartbeat
used. The **initial** range-lock TTL CamusDB requests must therefore comfortably exceed that tick.
Otherwise a lock would lapse before the first server renewal. For this reason the configured
range-lock TTL is set generously. A value shorter than the coordinator's collection interval would
silently break serializable isolation.

---

## 8b. When the coordinator does not know the transaction

A commit or rollback is routed to the coordinator that owns the session. Sometimes the coordinator
finds no session, no retained outcome from a recent finalize, and no durable record to consult. It
then answers with its unknown outcome and logs `Trying to rollback unknown transaction`.

That answer is terminal, and it leaves state behind. The coordinator releases what is in its
working set, and that working set died with the session. This means that no state is released.
The writes of the transaction planted point intents that do not expire (the write path takes its
exclusive lock with `expiresMs = 0`, which the storage node reads as "owned by the session,
cleared by its cleanup"). With the session gone, these intents are not cleared. A snapshot scan
that reaches such a key cannot prove that the undecided intent lands outside its snapshot. The
scan fails or waits for as long as the intent lives, which, absent cleanup, is forever.

The one remaining record of those keys is on the client, in the modified-key set that each
transaction tracks for cache invalidation. A rollback that meets the unknown outcome therefore
replays that set as per-key releases keyed by the transaction id. Three conditions gate this
release, and all three are required.

1. **The transaction did not begin a commit.** Only a commit prepares an intent, and the fate of a
   prepared intent belongs to the decision machinery. Releasing one would discard the only route to
   a value that may already be committed. A transaction that was `Active` when the rollback started
   cannot own one.
2. **It is older than `abandoned_transaction_release_after_ms`.** An unknown answer is not always
   proof of a dead session. A leadership change can route a finalize to a node that did not hold
   it. Past the session-timeout ceiling plus the coordinator's reclaim grace (the default that the
   setting derives), no session is alive anywhere, whatever the routing did.
3. **The release is keyed by `(transaction id, key)`.** It removes only state that the
   transaction currently owns. A release for a key that another transaction now owns is refused.
   For a key whose transaction committed or aborted, no state is left to remove. The pass is
   therefore idempotent and cannot take anything from a live transaction.

Condition 2 is usually not met at the moment of the reap. The reaper reclaims an abandoned
transaction after a few idle minutes. The age after which no session can own its holdings is the
session ceiling, an hour at the shipped defaults. For this reason the rollback **parks the key
mirror** and finishes the transaction. The transaction is then terminal and untracked, and it is
not finalized again. The reaper's ordinary tick releases the mirror once it reaches that age.
Parking is what makes the keys releasable once they are safe to release. After the transaction is
dropped, no other part of the system knows which keys it planted. Parked mirrors are capped in
total keys. A mirror refused by that cap is logged, and for that mirror the storage node's expiry
is then the only backstop.

The pass itself is best-effort and is attempted once. A key it cannot release is left to that same
expiry. Range locks are outside it: the coordinator owns them, and the client cannot enumerate
them. Either way, the transaction is terminal from the first unknown answer. A coordinator that
does not know a transaction will not come to know it later. This means that re-issuing the same
rollback can only repeat the answer.

What a run's artifacts show: the counter `camus.transaction.coordinator_unknown`, tagged `released`,
`deferred` (parked until its age), `dropped` (refused by the cap), `disabled` or `no_keys`, plus one
warning per transaction naming it and how many of its mirrored keys were released.

---

## 9. What a transaction's life looks like now

```
  BeginAsync(isolation, locking, readValidation, decisionDurability)
      │  LocateAndStartTransaction(options{ CoordinatorKey, Locking,
      │      ReadValidation, DecisionDurability, Timeout })  → TransactionHandle
      ▼
  … per statement …
      • writes   → register (coordinatorKey, opId): fold modified key + implicit point lock
      • locks    → register: fold the lock descriptor        (pessimistic only, for point locks)
      • reads    → register when FoldReads: fold observation (optimistic / tracked)
      • range locks (serializable scans) → register: coordinator renews + releases them
      ▼
  CommitAsync
      • validate read set (optimistic / tracked) — abort on a stale observation
      • prepare mutations — abort on a write-intent conflict
      • commit; return an HLC token
  — or —
  RollbackAsync / RollbackIfNotCompletedAsync
      • coordinator rolls back from its own working set
```

The client no longer assembles locks or modified keys for finalize. It supplies the handle, and
the coordinator does the rest.

---

## 10. Invariants for contributors

The invariants below apply to any change to the transaction lifecycle or to a path that accesses
or locks data.

1. **Register transactional mutations and locks on a real transaction.** Pass the
   transaction's coordinator key and an operation id. `commit(handle)` will not commit an
   unregistered write on a non-zero transaction id. The classic symptom is "I wrote the row and
   committed, but it reads back as missing / the table looks empty."

2. **One logical operation, one operation id — but mint a fresh id per batch attempt.** Reuse
   the same id across transient retries of the *same* work (idempotent replay). Mint a new id
   when the work changes, for example a batch that resends only its pending subset.

3. **Do not reuse a finalized transaction.** Once a `KvTransaction` has committed or rolled back,
   the coordinator does not allow further operations on it (a lock or write returns `Aborted`).
   A follow-up statement must use a **fresh** transaction. This is stricter than the old
   client-owned model, which silently tolerated writes on a spent transaction (and did not
   persist them).

4. **In a `catch` that might catch a commit failure, use `RollbackIfNotCompletedAsync`.** A
   failed commit already transitions the transaction to rolled-back and throws. Calling the
   strict `RollbackAsync` afterward throws "already rolled back." The outcome-agnostic variant
   is safe to call even if you do not know whether the commit got far enough to finalize.

5. **Gate read folding on `FoldReads`, which covers more than the locking mode.** Snapshot reads
   and the zero-identity fast path must not fold. The default pessimistic path must stay
   unregistered so it is unchanged. When you add a new read call site, thread the coordinator key
   exactly the way the existing scan and point-read helpers do.

6. **A deferred transaction must open its session before any op that needs a real id.** Deferral
   is opt-in (`BeginAsync(deferStart: true)`, used only by the `/start-transaction` path). Any
   write or lock path, and any *folded* read path, that a deferred transaction can reach must call
   `EnsureSessionStartedAsync` before it embeds `tx.TransactionId` or reads `tx.FoldReads`.
   Otherwise the operation uses the `Zero` id (unregistered / unfolded). Today that surface is
   exactly `KvTableStore`'s write/lock/scan paths. If you make deferral reachable from another
   subsystem, ensure-start there too. Do **not** defer internally begun or autocommit
   transactions. Eager start makes their paths valid and needs no guard. Do not change the
   locking mode after the session is open (`ApplyLocking` enforces this).

---

## 11. Configuration touchpoints

- **Locking / read-validation / decision-durability** are selected per transaction as arguments
  to `BeginAsync`, each with a process-wide default in `CamusDBOptions`
  (`DefaultTransactionLocking`, `DefaultReadValidation`, `DefaultDecisionDurability`). The
  shipped defaults preserve the historical behavior: pessimistic, no extra read validation,
  best-effort decisions. The locking default is also wired to `config.yml`'s
  `default_transaction_locking` (`pessimistic` | `optimistic`). Clients override it per
  transaction with the `locking` request field or `SET TRANSACTION LOCKING`.
- **`RangeLockExpiresMs`** is the *initial* range-lock TTL. It must exceed the coordinator's
  collection interval so a lock survives to its first server renewal.
- **`MaxSerializableTransactionLifetimeMs`** doubles as the Kahuna session `Timeout`. It bounds
  an abandoned session: the server reaper reclaims its locks after that window plus a grace
  period.
- **`AbandonedTransactionReleaseAfterMs`** is the age past which a rollback that meets the
  coordinator-unknown outcome releases the transaction's holdings from the client-side key mirror
  (§8b). `0` derives it from the session-timeout ceiling plus the coordinator's reclaim grace. A
  positive value overrides that and must be at least the serializable lifetime. A negative value
  disables the release.

---

## 12. Mental checklist

- Commit and rollback take a **handle**, and the server owns the working set.
- Each write and lock carries a **coordinator key + operation id**, and so does a read when reads
  are folded.
- A write folds its **implicit point lock**. Optimistic writes rely on that lock and take no
  explicit one.
- Optimistic = lock-free writes + commit-time validation (write–write and read–write). The
  validation does not cover phantoms; phantom protection comes from the Serializable predicate locks.
- Range locks are **renewed and released by the coordinator**; there is no client heartbeat.
- A finalized transaction is **dead** — begin a new one.
