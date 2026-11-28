# Serializable isolation: retry contract

Serializable is the **default isolation level** in CamusDB. Serializable+ReadWrite transactions use
**strict two-phase locking** (S2PL). When two transactions conflict on a lock, the wait-die rule
decides: an older requester, or one that holds nothing yet, waits up to `lock_wait_deadline_ms` for
the holder to finish; a younger requester that already holds a lock is aborted at once. The same rule
covers the row locks of pessimistic writes at Read Committed (see
[transactions-locking-and-isolation.md](transactions-locking-and-isolation.md) §6.1). An aborted
transaction must be **replayed from `BEGIN`** — retrying a single statement in the same
transaction is not safe.

For single-statement (autocommit) operations, CamusDB automatically retries on your behalf. This
means that most applications do not see retryable errors in practice. For explicit multi-statement transactions,
the application owns the retry loop.

---

## The replay-from-BEGIN rule

A conflicting transaction is dead in its entirety. Its lock set is released and its writes are
rolled back. The only correct recovery is to start a new transaction and re-execute all of its
steps from the beginning.

```
BEGIN
  READ  a
  READ  b
  WRITE a ← new value
  WRITE b ← new value
COMMIT
```

If `WRITE a` returns `TransactionConflict`, do **not** retry just `WRITE a`. The earlier reads
of `a` and `b` were taken under the (now dead) transaction's shared locks. Their values may be
stale. Replay the transaction from `BEGIN`. The new execution will then read current values and acquire
fresh locks.

---

## Retryable error codes

There are two distinct kinds of "retry". Each one needs a different recovery, and they must not be confused.

- **Replay from `BEGIN`** (three codes below): the transaction is dead and none of its writes survive.
  Recovery re-runs the business operation from the start on a fresh transaction.
- **Retry the *same* finalize** (`CADB0509`, see the next section): the transaction is **not** dead —
  a commit or rollback has not resolved yet. You re-issue the *same* `COMMIT`/`ROLLBACK` on the
  *same* transaction. You must **not** replay the operation. It may already have committed.

All codes not listed as retryable are permanent. They must propagate to the caller, and the operation must not be retried.

| Code      | Name                       | When raised                                                                                       | Safe to retry? |
|-----------|----------------------------|---------------------------------------------------------------------------------------------------|----------------|
| CADB0502  | `TransactionConflict`      | A shared or exclusive lock conflicted with a concurrent holder — or a commit returned a definite `Aborted` — or the transaction wrote a table that gained an index or a constraint before the commit (`CREATE INDEX`, `ADD CONSTRAINT`), which its staged writes do not maintain — or a table the transaction read or wrote changed its row layout after the statement was planned (a column was added or dropped, a column element changed state, the table was dropped or recreated). The last case is raised at the commit, which rolls the transaction back first, or at the next statement on that table. In all of these cases, no write survived. | ✓ replay from `BEGIN` |
| CADB0504  | `TransactionMustRetry`     | A **pre-write** transient: a routing failure during start (leader election / partition move) after the bounded start retries, a lock-wait deadline / write conflict in the storage layer, or a DDL statement whose schema-log proposal found that the schema leader moved while it was in flight. **No data was written** when it is raised. | ✓ replay from `BEGIN` |
| CADB0505  | `TransactionLifetimeExceeded` | The transaction was held open longer than `MaxSerializableTransactionLifetimeMs`. Its staged writes/locks are rolled back through the coordinator before this error is raised. | ✓ replay from `BEGIN` |
| CADB0509  | `TransactionFinalizeUnresolved` | A `COMMIT`/`ROLLBACK` returned the coordinator's non-terminal `MustRetry` until the finalize retry budget (`transaction_finalize_retry_budget_ms`) was used up: the outcome is **not known yet** (leadership flip mid-finalize, an in-progress drain, a participant write shed under load, or a durable decision not yet marked complete). | ↻ retry the **same** finalize — **do not** replay |
| CADB0555  | `LockNotAvailable`         | A `SELECT … FOR UPDATE NOWAIT` found a row that another transaction has locked. The statement failed and the transaction ended. It is deliberately outside the retry set: a retry is a wait, and NOWAIT asked not to wait. See [select-for-update.md](select-for-update.md). | ✗ no  |
| CADB0300  | `DuplicateUniqueKeyValue`  | A unique-index constraint was violated.                                                           | ✗ no  |
| CADB0301  | `NotNullViolation`         | A NOT NULL constraint was violated.                                                               | ✗ no  |
| CADB0400  | `InvalidInput`             | Bad request (wrong type, missing field, `SET TRANSACTION` after statement, etc.).                | ✗ no  |
| CADB0501  | `TransactionAlreadyCompleted` | Commit/rollback on a transaction that was already finalized, or a commit whose coordinator session is gone and whose outcome is unavailable (`Errored`). | ✗ no  |
| others    | —                          | Schema errors, system corruption, unknown columns, etc.                                           | ✗ no  |

`SerializableRetryHelper.IsRetryable` covers only the three **replay-from-`BEGIN`** codes (CADB0502,
CADB0504, CADB0505). `CADB0509` is deliberately **excluded** from it — replaying the operation on a
commit that may already have succeeded would double-apply it. See the next section for how to handle it.

### Checking retryability in code

```csharp
using CamusDB.Core.Transactions;

catch (CamusDBException ex) when (SerializableRetryHelper.IsRetryable(ex))
{
    // safe to replay the whole transaction from BEGIN
}
```

---

## Unresolved finalize: `TransactionFinalizeUnresolved` (CADB0509)

Committing (or rolling back) a transaction asks the Kahuna coordinator for a **terminal** answer:
`Committed` or `RolledBack`. The coordinator can also answer `MustRetry`, which means *"the final
outcome is not known yet, or transient cleanup work remains — ask again on the same handle."* This is
**not** a failure and **not** a rollback. It happens when leadership flips during the finalize, when the
coordinator is draining operations that were in flight when finalize began, or (in durable mode)
when a commit decision has been made but not yet fully acknowledged.

CamusDB retries this automatically on the same transaction with a backing-off loop bounded by a
wall-clock budget — `transaction_finalize_retry_budget_ms`, 15 s by default. The bound is a duration because each condition above resolves on a
separate schedule. A saturated node makes each one take longer,
but it does not make it take more attempts. This means that a cap on attempts would shrink the real
budget exactly when the node most needs it. Raise the setting on nodes that run
hot; lower it if you would rather take the unresolved answer sooner and retry it yourself.

If the outcome is unresolved when the budget is used up, `CommitAsync`/`RollbackAsync` throw
**CADB0509 `TransactionFinalizeUnresolved`**. The engine does not guess the outcome. When this happens,
the transaction is left unfinished, and only its finalize can be resumed.

- The transaction is left in the `Finalizing` state — **not** `Committed` and **not** `RolledBack`.
- It is tracked and its handle is valid. This means that the *same* finalize can be resumed.
- No further data operations are accepted on it (the finalize fence is installed).

**The rule:** re-issue the **same** `COMMIT` (or `ROLLBACK`) on the **same**
transaction. Do **not** start a new transaction and replay the statements — the original commit may have
already durably committed server-side. If so, replaying would apply the write twice.

```csharp
// Explicit transaction: resume the SAME finalize, never replay the operation.
const int MaxFinalizeAttempts = 10;
for (int attempt = 0; ; attempt++)
{
    try
    {
        await db.Transactions.CommitAsync(tx);   // same tx, same handle
        break;                                    // Committed
    }
    catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.TransactionFinalizeUnresolved)
    {
        if (attempt >= MaxFinalizeAttempts) throw; // surface it; the session's timeout is the backstop
        await Task.Delay(50 * (1 << Math.Min(attempt, 6)));
    }
}
```

Over HTTP the same rule applies: a `COMMIT`/`ROLLBACK` request that comes back as CADB0509 should be
re-sent for the same transaction id. Do not turn it into a fresh `BEGIN` + re-run of the statements.

An abandoned transaction left `Finalizing` forever is bounded by the Kahuna session timeout, which
reclaims the server-side session and releases its locks as the ultimate backstop.

---

## Auto-retry for autocommit statements

For **single-statement autocommit** operations the entire transaction is that one statement.
This means that replaying from `BEGIN` is the same as running the operation again. `SerializableRetryHelper.ExecuteAutocommitAsync`
gives bounded automatic retry with exponential back-off.

```csharp
await SerializableRetryHelper.ExecuteAutocommitAsync(async ct =>
{
    KvTransaction tx = await db.Transactions.BeginAsync(
        CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite, ct);
    try
    {
        await executor.Update(updateTicket with { TxnState = tx }, ct);
        await db.Transactions.CommitAsync(tx, ct);
    }
    catch
    {
        await db.Transactions.RollbackIfNotCompletedAsync(tx, ct);
        throw;
    }
}, maxAttempts: 5, cancellationToken);
```

`ExecuteAutocommitAsync` retries only on the three retryable codes above. Any other exception
propagates immediately. After `maxAttempts` exhausted retries the last retryable exception is
re-thrown.

Back-off schedule (default): `min(20 ms × 2^attempt, 400 ms)` ± 25 % jitter per attempt.

---

## Explicit multi-statement transactions

For explicit transactions, the application is responsible for the retry loop. Only the
application knows which statements to replay.

```csharp
const int MaxAttempts = 5;
int attempt = 0;

while (true)
{
    KvTransaction tx = await db.Transactions.BeginAsync(
        CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
    try
    {
        long balance = await ReadBalance(tx, accountId);

        if (balance < amount)
            throw new InvalidOperationException("Insufficient funds");

        await UpdateBalance(tx, accountId, balance - amount);
        await db.Transactions.CommitAsync(tx);
        break; // success
    }
    catch (CamusDBException ex) when (SerializableRetryHelper.IsRetryable(ex))
    {
        await db.Transactions.RollbackIfNotCompletedAsync(tx);

        if (++attempt >= MaxAttempts)
            throw;

        // Optional: back-off before retrying
        await Task.Delay(20 * (1 << attempt));
    }
    catch
    {
        await db.Transactions.RollbackIfNotCompletedAsync(tx);
        throw;
    }
}
```

The example handles a retryable error safely.

- The `catch` block rolls back the dead transaction before it decides whether to retry.
- `RollbackIfNotCompletedAsync` is safe to call even if the transaction already rolled back
  internally (e.g., `TransactionConflict` — Kahuna aborted server-side).
- The retry reads `balance` again from scratch: the new read picks up any committed writes made
  by the winning transaction and re-evaluates the business logic against the current state.

---

## When retry is not enough

Two transactions in a repeated mutual conflict will keep aborting each other. The bounded-wait →
abort policy (`LockWaitDeadlineMs = 500 ms`) prevents indefinite stall, but does not
guarantee fairness. If an operation consistently fails despite retrying, investigate whether the
contention pattern can be reduced (shorter transactions, fewer rows per transaction, staggered
access order).

The `TransactionLifetimeExceeded` error (CADB0505) means the transaction was open for longer than
`MaxSerializableTransactionLifetimeMs` (default 1 hour). Shorten long-running serializable transactions so they complete within the deadline.

A wait that is not a conflict has a separate budget. A key whose value is replicating, or
whose previous writer has committed but not yet settled, answers the store's `WaitingForReplication`.
This answer is distinct from a lock refusal. No transaction has the key locked, and there is no deadlock
to resolve. The engine waits up to `replication_wait_deadline_ms` (5 s by default). After that, the
statement returns `TransactionMustRetry` (CADB0504). A round in which even one key answered a lock
refusal uses `lock_wait_deadline_ms`. The CADB0504 message names which budget ran out and how many
keys answered each way in the last round.

When the Kahuna coordinator refuses a commit (CADB0502), the error names the refusal class in its message — a
conflict, a moved staged base, the decision deadline, an exhausted prepare, a lost lock or staged
intent — with the coordinator's reason, the number of staged mutations and how long the finalize ran.

---

## Configuration

| Setting                             | Default | Description                                              |
|-------------------------------------|---------|----------------------------------------------------------|
| `CamusDBOptions.DefaultIsolationLevel` | `Serializable` | Server default; opt down per-transaction via `BEGIN` or `SET TRANSACTION ISOLATION LEVEL READ COMMITTED`. |
| `CamusDBOptions.MaxSerializableTransactionLifetimeMs` | 3 600 000 ms (1 h) | Maximum lifetime for a Serializable+RW transaction; aborts with CADB0505 if exceeded. |
