# Transactions

Transaction lifecycle management backed by Kahuna.

`KvTransactionsManager` coordinates `BEGIN` / `COMMIT` / `ROLLBACK` by delegating to the embedded `IKahuna` instance. It tracks all active transactions in memory so they can be rolled back on shutdown.

`KvTransaction` is the per-operation context passed through every storage call. It accumulates the Kahuna transaction handle, acquired key locks, and modified keys needed for the 2-phase commit.

**Concurrency model: pessimistic locking.** Each write acquires an exclusive Kahuna lock on the key before writing, giving read-committed isolation without client-side retry loops. MVCC snapshot isolation is a future option.

Typical usage:

```csharp
KvTransaction tx = await manager.BeginAsync(ct);
try {
    await store.InsertRow(tx, id, data, ct);
    await manager.CommitAsync(tx, ct);
} catch {
    await manager.RollbackIfNotCompletedAsync(tx, ct);
    throw;
}
```
