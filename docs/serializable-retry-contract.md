# Serializable isolation: retry contract

Serializable is the **default isolation level** in CamusDB. Serializable+ReadWrite transactions use
**strict two-phase locking** (S2PL). When two transactions conflict, one is aborted immediately. The
aborted transaction must be **replayed from `BEGIN`** — retrying a single statement inside the same
transaction is not safe.

For single-statement (autocommit) operations, CamusDB automatically retries on your behalf, so
most applications never see retryable errors in practice. For explicit multi-statement transactions,
the application owns the retry loop.

---

## The replay-from-BEGIN rule

A conflicting transaction is dead in its entirety. Its lock set is released and its writes are
rolled back. The only correct recovery is to start a new transaction and re-execute every step
from the beginning:

```
BEGIN
  READ  a
  READ  b
  WRITE a ← new value
  WRITE b ← new value
COMMIT
```

If `WRITE a` fails with `TransactionConflict`, do **not** retry just `WRITE a`. The earlier reads
of `a` and `b` were taken under the (now dead) transaction's shared locks. Their values may be
stale. Replay the whole transaction so the new execution reads current values and acquires fresh
locks.

---

## Retryable error codes

Only three error codes indicate a transient serialization failure that retry can resolve.
All others are permanent and must propagate to the caller without retrying.

| Code      | Name                       | When raised                                                                                       | Safe to retry? |
|-----------|----------------------------|---------------------------------------------------------------------------------------------------|----------------|
| CADB0502  | `TransactionConflict`      | A shared or exclusive lock conflicted with a concurrent holder. Kahuna rejected at lock-acquire time; no 2PC was attempted. | ✓ yes |
| CADB0504  | `TransactionMustRetry`     | Kahuna returned `MustRetry` after exhausting its routing retry budget (leader election, partition move). No data was written. | ✓ yes |
| CADB0505  | `TransactionLifetimeExceeded` | The transaction was held open longer than `MaxSerializableTransactionLifetimeMs` (about 1 h; range locks are kept alive by a renewal heartbeat up to that backstop). Its range locks were released before this error was raised. | ✓ yes |
| CADB0300  | `DuplicateUniqueKeyValue`  | A unique-index constraint was violated.                                                           | ✗ no  |
| CADB0301  | `NotNullViolation`         | A NOT NULL constraint was violated.                                                               | ✗ no  |
| CADB0400  | `InvalidInput`             | Bad request (wrong type, missing field, `SET TRANSACTION` after statement, etc.).                | ✗ no  |
| CADB0501  | `TransactionAlreadyCompleted` | Commit/rollback on a transaction that was already finalized.                                   | ✗ no  |
| others    | —                          | Schema errors, system corruption, unknown columns, etc.                                           | ✗ no  |

### Checking retryability in code

```csharp
using CamusDB.Core.Transactions;

catch (CamusDBException ex) when (SerializableRetryHelper.IsRetryable(ex))
{
    // safe to replay the whole transaction from BEGIN
}
```

---

## Auto-retry for autocommit statements

For **single-statement autocommit** operations the entire transaction is just that one statement,
so replaying from `BEGIN` means running the operation again. Use `SerializableRetryHelper.ExecuteAutocommitAsync`
to get bounded automatic retry with exponential back-off:

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

For explicit transactions the application is responsible for the retry loop because only the
application knows which statements to replay:

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

Key points:
- The `catch` block rolls back the dead transaction first, then decides whether to retry.
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
25 seconds. Shorten long-running serializable transactions so they complete within the deadline.

---

## Configuration

| Setting                             | Default | Description                                              |
|-------------------------------------|---------|----------------------------------------------------------|
| `CamusDBConfig.DefaultIsolationLevel` | `Serializable` | Server default; opt down per-transaction via `BEGIN` or `SET TRANSACTION ISOLATION LEVEL READ COMMITTED`. |
| `CamusDBConfig.MaxSerializableTransactionLifetimeMs` | 3 600 000 ms (1 h) | Maximum lifetime for a Serializable+RW transaction; aborts with CADB0505 if exceeded. |
