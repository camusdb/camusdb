# The Kahuna Transaction Coordinator

> **Audience:** engineers working on CamusDB's storage, transaction, or DML/DDL layers.
> **What you'll learn:** how CamusDB drives transactions through Kahuna's *server-owned*
> transaction coordinator — what the coordinator owns, how each operation is registered and
> folded into the transaction, how commit and rollback work, and what "pessimistic" vs
> "optimistic" locking actually do. It also lists the invariants you must preserve when you
> touch a write, read, or lock path.
>
> Assumes you've read **Transactions, Locking & Isolation** for the isolation-level picture.
> This guide is about the *mechanism* underneath it.

---

## 1. The big picture

CamusDB embeds **Kahuna** (a transactional KV store) in-process and drives its `IKahuna`
surface directly — there is no network hop for transaction operations on a single node.
Kahuna now provides a **server-owned transaction coordinator**: the server keeps the
authoritative state of an in-flight transaction, and the client contributes to that state one
operation at a time instead of assembling it locally and shipping it at commit.

The change is best understood as a move of *ownership*:

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
that list to `LocateAndCommitTransaction`. Under the new model the client hands the server a
**handle** and nothing else; the server already knows the working set because every operation
*registered* its confirmed effect as it happened.

---

## 2. The transaction handle

A transaction is identified by a `TransactionHandle`:

- **`TransactionId`** — the Kahuna `HLCTimestamp` minted by `LocateAndStartTransaction`. It is
  passed as the `transactionId` argument on every KV operation.
- **`CoordinatorKey`** — a stable string (CamusDB uses a fresh GUID per transaction) that pins
  the transaction's server-side session to one partition leader and routes every registered
  operation, plus commit and rollback, to that session.

`KvTransaction` exposes both (`TransactionId`, `CoordinatorKey`, and the combined `Handle`).
Commit and rollback take only the handle:

```csharp
(result, _) = await kahuna.LocateAndCommitTransaction(tx.Handle, cancellationToken);
await kahuna.LocateAndRollbackTransaction(tx.Handle, cancellationToken);
```

There is no client-built key list at finalize. The coordinator finalizes exactly the working
set it accumulated.

---

## 3. Operation registration and folding

Every transactional operation carries two extra arguments: the transaction's `CoordinatorKey`
and a per-operation `TransactionOperationId`. Together they *register* the operation with the
coordinator, which then **folds** the operation's confirmed effect into the server-owned
working set:

| Operation | What folds into the working set |
|---|---|
| A successful write / delete | the modified key **and its implicit point lock** |
| An explicit point-lock acquire | the held point-lock descriptor |
| A range-lock acquire | the held range-lock descriptor (bounds + mode) |
| A read of the latest committed value | an observation: the key's existence and base revision |
| A registered scan | one read observation per row it returns |

Two consequences you must internalize:

1. **A transactional write or lock that is *not* registered is invisible to commit.** If a
   write goes out with an empty coordinator key, its effect never folds into the working set,
   so `commit(handle)` does not commit it — the row reads back as if it were never written.
   Every transactional mutation and lock on a real (non-zero) transaction id **must** pass the
   coordinator key and an operation id.

2. **A write already registers its own implicit point lock.** You do not need a separate
   explicit lock just to make a write commit — the write folds both the modified key and the
   lock. Explicit locks exist to provide *mutual exclusion before* the write (see pessimistic
   locking below), not to make the write durable.

The zero-identity fast path is the exception: a read-only transaction backed by
`HLCTimestamp.Zero` performs non-transactional, read-committed reads. It has no coordinator
session to fold into, so it registers nothing and its commit/rollback are no-ops. This is what
point reads and single-partition read paths use.

---

## 4. Idempotent retry

Kahuna's transport may re-deliver a call, and CamusDB itself retries transient routing signals
(`MustRetry` / `WaitingForReplication`). Registration makes these retries safe:

- A retry that reuses the **same** operation id replays the cached completion instead of
  applying the operation twice.
- A retry that describes **new** work uses a **new** operation id.

CamusDB follows this rule mechanically. A single-key operation mints one operation id and
reuses it across its own transient retries (a stable id = idempotent replay). A batch operation
that resends only the still-pending subset mints a **fresh** id per attempt, because a shrunken
batch is genuinely different work. When in doubt: one logical operation, one operation id.

---

## 5. Pessimistic vs optimistic locking

The concurrency strategy is chosen per transaction when it begins (via `BeginAsync`), defaulting
to pessimistic. It is **orthogonal** to the SQL isolation level: a transaction is independently
Serializable-or-ReadCommitted *and* Pessimistic-or-Optimistic. Clients select it through the
`locking` request field or `SET TRANSACTION LOCKING` (see
`transactions-locking-and-isolation.md` §6.4); the rest of this section is the mechanism.

### Pessimistic (the default)

Before each write, CamusDB acquires an exclusive point lock on the key, then writes. A second
transaction that wants the same key **blocks** (or is aborted by deadlock avoidance) until the
first finalizes. Conflicts are resolved at lock-acquisition time. This is the behavior the
engine has always had; it gives straightforward read-committed-or-stronger isolation with no
client retry loop for the common case.

### Optimistic (opt-in)

An optimistic transaction skips the explicit **exclusive write lock**. Its writes still fold their
implicit point locks and modified keys, and its reads fold observations. Nothing blocks *on the
write path*; write-write and read-write conflicts are detected at **commit** during prepare:

- **Write–write:** two optimistic transactions that modified the same key both try to place a
  write intent on it at prepare. The second one conflicts and aborts. Exactly one wins.
- **Read–write / write-skew:** the transaction's read observations are re-validated against the
  current committed revisions. If a key it read was modified and committed by another
  transaction in the meantime, the commit aborts — even when the two transactions wrote
  disjoint keys.

The write path skips explicit lock acquisition for an optimistic transaction; the read path
registers reads so they can be validated (see the next section). A losing optimistic transaction
surfaces as a `CamusDBException` from `CommitAsync`.

**"Optimistic" is not lock-free at every isolation level — this is the key boundary.** Skipping the
write lock is the *only* thing the locking mode changes. The read/scan **predicate locks** are gated on
the *isolation level*, not on the locking mode:

- **Read Committed + Optimistic** takes no predicate locks, so it is fully lock-free. Its validation
  covers *read observations* — the specific rows the transaction read — not *predicates*, so it does
  **not** protect against a concurrent insert of a new row that would have matched a `WHERE` clause
  (a phantom). If you need phantom protection here, use Serializable.
- **Serializable + Optimistic** is a **hybrid**: the write path is optimistic (no explicit write lock,
  commit-time validation), but reads and scans still take **shared range/point predicate locks** and a
  following write upgrades them to exclusive — exactly as under Serializable + Pessimistic. So it *is*
  phantom-free, and it is *not* lock-free. Serializable is deliberately never weakened to lock-free,
  because that would silently break its isolation guarantee.

(The fully phantom-vulnerable, lock-free read path is Read Committed; the lock-free *and* phantom-free
option is a Serializable **read-only snapshot**, which holds no locks because it reads a fixed MVCC
timestamp — a different mechanism from optimistic locking.)

### Deferred session start (so `SET TRANSACTION LOCKING` can work)

Kahuna **pins the locking mode when the coordinator session opens** — it cannot change once the
session exists. That is fine when the mode is known at `BeginAsync` (the request field, or the
server default). But `SET TRANSACTION LOCKING` runs as the *first statement* of an already-open
client transaction, after `/start-transaction` has returned — later than the session would
normally start.

To reconcile this, an **explicit** transaction begun through `/start-transaction`
(`BeginAsync(deferStart: true)`) is created as a shell with **no Kahuna session yet**:
`TransactionId` stays `Zero`, and a separately minted, stable `ClientId` is what the coordinator
tracks and returns to the client. The session opens lazily on the transaction's **first**
write, lock, or folded read — via `EnsureSessionStartedAsync`, which reads the *then-current*
locking mode, so any `SET TRANSACTION LOCKING`/`SET TRANSACTION ISOLATION` issued first takes
effect. `EnsureSessionStartedAsync` opens the session exactly once even under concurrent
first-operations, and is a no-op once the session exists.

Deferral is **opt-in and used only for that explicit path**. Every internally-begun transaction
(catalog, registry, statistics) and every autocommit statement starts eagerly, so their write and
lock paths always have a live session — deferral never touches them. A deferred transaction can
only reach the write/lock/scan paths in `KvTableStore`, which call `EnsureSessionStartedAsync`
first; DDL runs in its own eager transaction, never on the deferred one. A deferred transaction
committed or rolled back before it ever opened a session (e.g. `BEGIN` then `COMMIT` with no
statement) finalizes through the zero-identity fast path — no Kahuna round-trip.

---

## 6. Read-set folding

For read-set validation to work, the reads a transaction performs must be registered so their
observations fold into the working set. CamusDB folds reads only when the transaction actually
wants validation — that condition is captured by `KvTransaction.FoldReads`, which is true only
when **all** of the following hold:

- the transaction is Optimistic, **or** explicitly requested read-set tracking;
- it has a real transaction identity (not the `HLCTimestamp.Zero` fast path);
- it reads the *latest* committed value, not a pinned historical snapshot (a snapshot read has
  no live dependency and folds nothing).

When `FoldReads` is true, the read path passes the coordinator key and an operation id on every
read — point reads, table scans, index scans, and batch row fetches alike. When it is false, the
read path passes an empty coordinator key and behaves exactly as it always did: unregistered,
relying on locks (or nothing, for a plain snapshot read). This gate is what keeps the default
pessimistic path byte-for-byte unchanged.

One subtlety worth flagging for anyone touching the scan code: a table or index scan has **two**
implementations — a direct streaming path for root tables and a merge path for branch databases.
Both must thread the coordinator key, or an optimistic scan on one table shape would silently
skip validation.

---

## 7. Commit-decision durability

Each transaction also chooses how durable its commit *decision* must be, defaulting to
best-effort:

- **Best-effort** — the decision may live in memory and can be lost if the coordinator crashes
  before its next checkpoint. This is the historical behavior and the right default for most
  workloads.
- **Durable** — the decision is written to durable storage before the outcome is returned, so it
  survives coordinator loss and recovery can finish the transaction's participants.

The coordinator assigns the durable decision's anchor from the first confirmed persistent write,
so there is no client-side plumbing: a transaction simply opts in when it begins. (Durable mode
rejects a transaction that confirmed any ephemeral modification; all of CamusDB's row, index, and
schema writes are persistent, so this never trips in practice.)

---

## 8. Range locks are coordinator-owned

Serializable read-write transactions take **range locks** to fence out phantoms during a scan.
These locks are registered like any other operation, so they fold into the working set — and
that changes who keeps them alive.

A range lock has a TTL. Previously CamusDB ran a background *heartbeat* that periodically
re-acquired every range lock a live transaction held, refreshing the TTL so long transactions
kept their locks. That heartbeat is **gone**. Because range-lock acquisitions now register with
the coordinator, the coordinator itself renews a live session's range locks for the duration of
the session and releases them on finalize. An abandoned session (a client that opened a
transaction and vanished) is bounded by the session timeout: the server reaper reclaims it and
releases its locks.

There is one timing rule that falls out of this. The coordinator renews on its periodic
collection tick, which is a coarse interval (tens of seconds), not the sub-second cadence the old
client heartbeat used. The **initial** range-lock TTL CamusDB requests must therefore comfortably
exceed that tick, or a lock would lapse before the first server renewal. This is why the
configured range-lock TTL is set generously; a value shorter than the coordinator's collection
interval would silently break serializable isolation.

---

## 8b. When the coordinator does not know the transaction

A commit or rollback is routed to the coordinator that owns the session. Sometimes it finds nothing:
no session, no retained outcome from a recent finalize, and no durable record to consult. The
coordinator answers with its unknown outcome and logs `Trying to rollback unknown transaction`.

That answer is terminal, but it is not clean. The coordinator releases from its own working set, and
that working set died with the session — so it releases nothing. Every write the transaction made
planted a point intent with **no expiry** (the write path takes its exclusive lock with
`expiresMs = 0`, which the storage node reads as "owned by the session, cleared by its cleanup").
With the session gone, nothing clears them: a snapshot scan that reaches such a key can never prove
the undecided intent lands outside its snapshot, so it fails or waits for as long as the intent
lives — which, absent cleanup, is forever.

The client holds the one remaining record of which keys those are: the modified-key set each
transaction keeps for cache invalidation. So a rollback that meets the unknown outcome replays that
set as per-key releases keyed by the transaction id. Three conditions gate it, and all three are
required:

1. **The transaction never began a commit.** Only a commit prepares an intent, and a prepared
   intent's fate belongs to the decision machinery — releasing one would discard the only route to a
   value that may already be committed. A transaction still `Active` when the rollback started cannot
   own one.
2. **It is older than `abandoned_transaction_release_after_ms`.** An unknown answer is not always
   proof of a dead session: a leadership change can route a finalize to a node that never held it.
   Past the session-timeout ceiling plus the coordinator's reclaim grace — what the setting derives
   by default — no session is alive anywhere, whatever the routing did.
3. **The release is keyed by `(transaction id, key)`.** It removes only state that transaction still
   owns. A key another transaction now holds is refused; a key whose transaction committed or aborted
   has nothing left to remove. The pass is therefore idempotent and cannot take anything from a live
   transaction.

Condition 2 is usually not met at the moment of the reap, and that is the point of what follows. The
reaper reclaims an abandoned transaction after a few idle minutes; the age at which no session can
still own its holdings is the session ceiling, an hour at the shipped defaults. So the rollback
**parks the key mirror** and finishes the transaction — terminal, untracked, never finalized again —
and the reaper's ordinary tick releases the mirror once its age is reached. Parking is what keeps
"not yet safe to release" from meaning "never released": once the transaction is dropped, nothing
else in the system still knows which keys it planted. Parked mirrors are capped in total keys, and a
mirror refused by that cap is logged, because the storage node's own expiry is then its only backstop.

The pass itself is best-effort and runs once: a key it cannot release is left to that same expiry.
Range locks are outside it — the coordinator owns them, and the client cannot enumerate them. The
transaction is terminal from the first unknown answer either way; a coordinator that does not know a
transaction will never come to know it, so re-issuing the same rollback can only repeat the answer.

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

The client no longer assembles locks or modified keys for finalize; it supplies the handle and
the coordinator does the rest.

---

## 10. Invariants for contributors

If you touch a write, read, lock, or transaction-lifecycle path, keep these true:

1. **Register every transactional mutation and lock on a real transaction.** Pass the
   transaction's coordinator key and an operation id. An unregistered write on a non-zero
   transaction id will not be committed by `commit(handle)` — the classic symptom is "I wrote
   the row and committed, but it reads back as missing / the table looks empty."

2. **One logical operation, one operation id — but mint a fresh id per batch attempt.** Reuse
   the same id across transient retries of the *same* work (idempotent replay); mint a new id
   when the work changes (e.g. a batch that resends only its still-pending subset).

3. **Never reuse a finalized transaction.** Once a `KvTransaction` has committed or rolled back,
   the coordinator rejects further operations on it (you'll see `Aborted` from a lock or write).
   A follow-up statement must run on a **fresh** transaction. This is stricter than the old
   client-owned model, which silently tolerated writes on a spent transaction (and never
   actually persisted them).

4. **In a `catch` that might catch a commit failure, use `RollbackIfNotCompletedAsync`.** A
   failed commit already transitions the transaction to rolled-back and throws; calling the
   strict `RollbackAsync` afterward throws "already rolled back." The outcome-agnostic variant
   is safe to call without knowing whether the commit got far enough to finalize.

5. **Gate read folding on `FoldReads`, not on the locking mode alone.** Snapshot reads and the
   zero-identity fast path must never fold; the default pessimistic path must stay unregistered
   so it is unchanged. When you add a new read call site, thread the coordinator key exactly the
   way the existing scan and point-read helpers do.

6. **A deferred transaction must open its session before any op that needs a real id.** Deferral
   is opt-in (`BeginAsync(deferStart: true)`, only the `/start-transaction` path). Any write, lock,
   or *folded* read path a deferred transaction can reach must call `EnsureSessionStartedAsync`
   before it embeds `tx.TransactionId` or reads `tx.FoldReads` — otherwise it runs with the `Zero`
   id (unregistered / unfolded). Today that surface is exactly `KvTableStore`'s write/lock/scan
   paths; if you make deferral reachable from another subsystem, ensure-start there too. Do **not**
   defer internally-begun or autocommit transactions — eager start keeps their paths valid without
   a guard. Never change the locking mode after the session is open (`ApplyLocking` enforces this).

---

## 11. Configuration touchpoints

- **Locking / read-validation / decision-durability** are selected per transaction as arguments
  to `BeginAsync`, each with a process-wide default in `CamusDBConfig`
  (`DefaultTransactionLocking`, `DefaultReadValidation`, `DefaultDecisionDurability`). The
  shipped defaults preserve the historical behavior: pessimistic, no extra read validation,
  best-effort decisions. The locking default is also wired to `config.yml`'s
  `default_transaction_locking` (`pessimistic` | `optimistic`); clients override it per transaction
  with the `locking` request field or `SET TRANSACTION LOCKING`.
- **`RangeLockExpiresMs`** is the *initial* range-lock TTL. It must exceed the coordinator's
  collection interval so a lock survives to its first server renewal.
- **`MaxSerializableTransactionLifetimeMs`** doubles as the Kahuna session `Timeout`: it bounds
  an abandoned session so the server reaper reclaims its locks after that window plus a grace
  period.
- **`AbandonedTransactionReleaseAfterMs`** is the age past which a rollback that meets the
  coordinator-unknown outcome releases the transaction's holdings from the client-side key mirror
  (§8b). `0` derives it from the session-timeout ceiling plus the coordinator's reclaim grace; a
  positive value overrides that and must be at least the serializable lifetime; a negative value
  disables the release.

---

## 12. Mental checklist

- Commit/rollback take a **handle**, not a working set.
- Every write, lock, and (when folding) read carries a **coordinator key + operation id**.
- A write folds its **implicit point lock**; optimistic writes rely on that instead of taking an
  explicit lock.
- Optimistic = lock-free writes + commit-time validation (write–write and read–write), **non-
  phantom**.
- Range locks are **renewed and released by the coordinator**; there is no client heartbeat.
- A finalized transaction is **dead** — begin a new one.
