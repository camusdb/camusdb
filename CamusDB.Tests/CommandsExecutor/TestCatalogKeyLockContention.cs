/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Auth;
using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A catalog, auth or registry key that another transaction holds is contention, not a fault: the
/// writer must get the lock once the holder commits, inside its retry budget.
///
/// <para>Kahuna registers a transaction-scoped lock with the coordinator under an operation id, and it
/// answers a repeated id with the outcome it cached for that id. A lock loop that retries
/// <c>AlreadyLocked</c> under one id therefore replays the first denial on every attempt and never sees
/// the holder leave. Each test here holds the key in a second transaction, starts the write, commits the
/// holder while the write is still in its retry loop, and requires the write to succeed.</para>
///
/// <para>A lock that is still held when the budget runs out must surface as the retryable
/// <see cref="CamusDBErrorCodes.TransactionMustRetry"/>, never as
/// <see cref="CamusDBErrorCodes.SystemSpaceCorrupt"/>: nothing is wrong with the store.</para>
/// </summary>
[NonParallelizable]
internal sealed class TestCatalogKeyLockContention : BaseTest
{
    /// <summary>How long the holder keeps the key after the write starts. Inside every budget under test.</summary>
    private const int HoldMs = 100;

    private KvTransactionsManager NewTransactions() =>
        new(TestNode!.Kahuna, Options,
            _ => TestNode!.Raft.HybridLogicalClock.SendOrLocalEvent(TestNode!.Raft.GetLocalNodeId()));

    private static string UniqueMetaKey() => $"lockretry{Guid.NewGuid():n}/meta/probe";

    /// <summary>Begins a transaction that holds the exclusive lock on <paramref name="key"/> and has written <paramref name="value"/>.</summary>
    private async Task<KvTransaction> HoldAsync(KvTransactionsManager transactions, string key, byte[] value)
    {
        KvTransaction holder = await transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        await MetaKeyWriter.WriteMetaKey(TestNode!.Kahuna, holder, key, value);
        return holder;
    }

    private async Task<byte[]?> ReadCommittedAsync(string key)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await TestNode!.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None);

        return type == KeyValueResponseType.Get ? entry?.Value : null;
    }

    [Test]
    public async Task MetaWriteGetsTheLockAfterTheHolderCommits()
    {
        using KvTransactionsManager transactions = NewTransactions();
        string key = UniqueMetaKey();

        KvTransaction holder = await HoldAsync(transactions, key, [1]);

        KvTransaction writer = await transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        Task write = MetaKeyWriter.WriteMetaKey(TestNode!.Kahuna, writer, key, [2]);

        await Task.Delay(HoldMs);
        Assert.IsFalse(write.IsCompleted, "the write must wait while another transaction holds the key");

        await transactions.CommitAsync(holder);
        await write;
        await transactions.CommitAsync(writer);

        Assert.AreEqual(new byte[] { 2 }, await ReadCommittedAsync(key), "the waiting write lands after the holder's");
    }

    [Test]
    public async Task MetaDeleteGetsTheLockAfterTheHolderCommits()
    {
        using KvTransactionsManager transactions = NewTransactions();
        string key = UniqueMetaKey();

        KvTransaction holder = await HoldAsync(transactions, key, [1]);

        KvTransaction deleter = await transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        Task delete = MetaKeyWriter.DeleteMetaKey(TestNode!.Kahuna, deleter, key);

        await Task.Delay(HoldMs);
        Assert.IsFalse(delete.IsCompleted, "the delete must wait while another transaction holds the key");

        await transactions.CommitAsync(holder);
        await delete;
        await transactions.CommitAsync(deleter);

        Assert.IsNull(await ReadCommittedAsync(key), "the waiting delete removes the holder's value");
    }

    [Test]
    public async Task MetaWriteReportsALockStillHeldAtTheEndOfItsBudgetAsRetryable()
    {
        using KvTransactionsManager transactions = NewTransactions();
        string key = UniqueMetaKey();

        KvTransaction holder = await HoldAsync(transactions, key, [1]);
        KvTransaction writer = await transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);

        try
        {
            CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
                () => MetaKeyWriter.WriteMetaKey(TestNode!.Kahuna, writer, key, [2]))!;

            Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ex.Code, ex.Message);
        }
        finally
        {
            await transactions.RollbackIfNotCompletedAsync(writer);
            await transactions.RollbackIfNotCompletedAsync(holder);
        }
    }

    [Test]
    public async Task AuthWriteGetsTheLockAfterTheHolderCommits()
    {
        AuthCatalog auth = await AuthCatalog.OpenAsync(TestNode!, Options);
        using KvTransactionsManager transactions = NewTransactions();

        // Every user change increments the auth generation under its lock, so a transaction holding
        // the generation key makes CREATE USER contend on it.
        const string generationKey = "_system/auth/generation";
        (KeyValueResponseType getType, ReadOnlyKeyValueEntry? current) = await TestNode!.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, generationKey, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None);
        byte[] generation = getType == KeyValueResponseType.Get && current?.Value is { } v ? v : BitConverter.GetBytes(0L);

        KvTransaction holder = await HoldAsync(transactions, generationKey, generation);

        string user = "lr_" + Guid.NewGuid().ToString("n");
        Task create = auth.CreateUserAsync(user, PasswordHasher.Hash("x", Options.PasswordHashIterations), ifNotExists: false);

        await Task.Delay(HoldMs);
        Assert.IsFalse(create.IsCompleted, "CREATE USER must wait while another transaction holds the generation key");

        await transactions.CommitAsync(holder);
        await create;

        Assert.IsNotNull(await auth.TryGetUserAsync(user), "the user is created once the holder leaves");
    }

    [Test]
    public async Task AuthWriteReportsALockStillHeldAtTheEndOfItsBudgetAsRetryable()
    {
        AuthCatalog auth = await AuthCatalog.OpenAsync(TestNode!, Options);
        using KvTransactionsManager transactions = NewTransactions();

        const string generationKey = "_system/auth/generation";
        KvTransaction holder = await HoldAsync(transactions, generationKey, BitConverter.GetBytes(0L));

        try
        {
            string user = "lr_" + Guid.NewGuid().ToString("n");
            CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
                () => auth.CreateUserAsync(user, PasswordHasher.Hash("x", Options.PasswordHashIterations), ifNotExists: false))!;

            Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ex.Code, ex.Message);
        }
        finally
        {
            await transactions.RollbackIfNotCompletedAsync(holder);
        }
    }
}
