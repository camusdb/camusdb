/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Isolation of rows whose values are stored out of line, under both concurrency strategies.
///
/// <para>A large value lives under its own key, but every write of that key also rewrites the row key,
/// because the pointer in the row changes with the value. These tests prove the consequence: the row
/// key's locks and read validation are what isolate the large value. Two Serializable writers of the
/// same large value conflict and only one commits, the loser's value is never stored, and a writer that
/// fails after it took its locks leaves nothing that blocks the next writer.</para>
/// </summary>
[TestFixture(KeyValueTransactionLocking.Pessimistic)]
[TestFixture(KeyValueTransactionLocking.Optimistic)]
[NonParallelizable]
internal sealed class TestLargeValueConcurrency : SharedNodeBaseTest
{
    private readonly KeyValueTransactionLocking locking;

    public TestLargeValueConcurrency(KeyValueTransactionLocking locking) => this.locking = locking;

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { DefaultTransactionLocking = locking };

    private static readonly Random Rng = new(90210);

    private static string Incompressible(int length)
    {
        const string alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        char[] chars = new char[length];
        lock (Rng)
        {
            for (int i = 0; i < length; i++)
                chars[i] = alphabet[Rng.Next(alphabet.Length)];
        }
        return new string(chars);
    }

    private static Task<UpdateResult> UpdateAsync(CommandExecutor executor, string db, KvTransaction tx, string title, Dictionary<string, ColumnValue> values)
        => executor.Update(new UpdateTicket(
            txnState: tx,
            databaseName: db,
            tableName: "docs",
            plainValues: values,
            exprValues: null,
            where: null,
            filters: new() { new("title", "=", new ColumnValue(ColumnType.String, title)) },
            parameters: null));

    private static async Task<string?> ReadBodyAsync(CommandExecutor executor, string db, KvTransaction tx, string title)
    {
        QueryTicket ticket = new(
            txnState: tx,
            databaseName: db,
            tableName: "docs",
            index: null,
            projection: null,
            where: null,
            filters: new() { new("title", "=", new ColumnValue(ColumnType.String, title)) },
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null);

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        return rows.Count == 0 ? null : rows[0].Row["body"].StrValue;
    }

    private async Task<(string db, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, db,
            "CREATE TABLE docs (id object_id PRIMARY KEY, title string UNIQUE, body string)", null));

        KvTransaction setup = await database.Transactions.BeginAsync();
        foreach (string title in new[] { "a", "b" })
        {
            await executor.Insert(new InsertTicket(setup, db, "docs", new()
            {
                new()
                {
                    { "id", new ColumnValue(ColumnType.Id, Core.Util.ObjectIds.ObjectIdGenerator.Generate().ToString()) },
                    { "title", new ColumnValue(ColumnType.String, title) },
                    { "body", new ColumnValue(ColumnType.String, Incompressible(5000)) },
                },
            }));
        }
        await database.Transactions.CommitAsync(setup);

        return (db, database, executor);
    }

    private async Task<List<(string key, ReadOnlyKeyValueEntry entry)>> LargeValueKeysAsync(DatabaseDescriptor database)
    {
        string bucket = $"{database.Id}:{database.Schema.Tables["docs"].EffectiveStorageId}|v";
        List<(string, ReadOnlyKeyValueEntry)> keys = [];
        await foreach ((string key, ReadOnlyKeyValueEntry entry) in SharedKahuna.LocateAndScanRange(
            HLCTimestamp.Zero, bucket, null, true, null, true, 1000,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None))
        {
            if (key.StartsWith(bucket + "/", StringComparison.Ordinal) && entry.Value is { Length: > 1 } && entry.Value[0] == (byte)BranchKvKind.Value)
                keys.Add((key, entry));
        }
        return keys;
    }

    [Test]
    public async Task TwoSerializableWriters_OfTheSameLargeValue_Conflict_AndOnlyOneValueIsStored()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction txA = await database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        KvTransaction txB = await database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        // Both read the large value, so both hold a shared lock on the row key that points at it.
        Assert.IsNotNull(await ReadBodyAsync(executor, db, txA, "a"));
        Assert.IsNotNull(await ReadBodyAsync(executor, db, txB, "a"));

        string valueA = Incompressible(6000);
        string valueB = Incompressible(6000);

        CamusDBException? conflict = Assert.ThrowsAsync<CamusDBException>(() =>
            UpdateAsync(executor, db, txA, "a", new() { { "body", new ColumnValue(ColumnType.String, valueA) } }));
        Assert.That(conflict?.Code, Is.AnyOf(CamusDBErrorCodes.TransactionConflict, CamusDBErrorCodes.TransactionMustRetry),
            "the first writer cannot upgrade its lock on the row while the second writer holds a shared lock on it");
        await database.Transactions.RollbackAsync(txA);

        await UpdateAsync(executor, db, txB, "a", new() { { "body", new ColumnValue(ColumnType.String, valueB) } });
        await database.Transactions.CommitAsync(txB);

        KvTransaction verify = await database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
        Assert.AreEqual(valueB, await ReadBodyAsync(executor, db, verify, "a"), "the committed writer's value must be stored");
        await database.Transactions.CommitAsync(verify);

        Assert.AreEqual(2, (await LargeValueKeysAsync(database)).Count, "the loser must leave no extra value behind");
    }

    [Test]
    public async Task ConcurrentReadCommittedWriters_OfTheSameLargeValue_NeverMixTheirWrites()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        // Six writers race to replace the same large value, each retrying on a conflict. Whatever order
        // they commit in, the row must end with exactly one writer's complete value.
        string[] candidates = Enumerable.Range(0, 6).Select(_ => Incompressible(4000)).ToArray();

        async Task<bool> WriteAsync(string value)
        {
            for (int attempt = 0; attempt < 20; attempt++)
            {
                KvTransaction tx = await database.Transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
                try
                {
                    await UpdateAsync(executor, db, tx, "b", new() { { "body", new ColumnValue(ColumnType.String, value) } });
                    await database.Transactions.CommitAsync(tx);
                    return true;
                }
                catch (CamusDBException ex) when (SerializableRetryHelper.IsRetryable(ex))
                {
                    await database.Transactions.RollbackIfNotCompletedAsync(tx);
                    await Task.Delay(Rng.Next(5, 30));
                }
                finally
                {
                    await database.Transactions.RollbackIfNotCompletedAsync(tx);
                }
            }

            return false;
        }

        bool[] committed = await Task.WhenAll(candidates.Select(value => Task.Run(() => WriteAsync(value))));
        Assert.IsTrue(committed.Any(c => c), "at least one writer must commit");

        KvTransaction verify = await database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
        string? final = await ReadBodyAsync(executor, db, verify, "b");
        await database.Transactions.CommitAsync(verify);

        CollectionAssert.Contains(candidates, final, "the stored value must be one writer's complete value, never a mix");
        Assert.AreEqual(2, (await LargeValueKeysAsync(database)).Count);
    }

    [Test]
    public async Task WriterThatFailsAfterTakingItsLocks_BlocksNoLaterWriter()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        // The update takes its locks on row "a", on row "a"'s large value and on the new unique key,
        // then fails because title "b" already exists.
        KvTransaction failing = await database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        CamusDBException? duplicate = Assert.ThrowsAsync<CamusDBException>(() => UpdateAsync(executor, db, failing, "a", new()
        {
            { "title", new ColumnValue(ColumnType.String, "b") },
            { "body", new ColumnValue(ColumnType.String, Incompressible(6000)) },
        }));
        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, duplicate?.Code);
        await database.Transactions.RollbackAsync(failing);

        // A later writer of the same row and the same large value proceeds at once.
        string next = Incompressible(6000);
        Stopwatch elapsed = Stopwatch.StartNew();
        KvTransaction later = await database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await UpdateAsync(executor, db, later, "a", new() { { "body", new ColumnValue(ColumnType.String, next) } });
        await database.Transactions.CommitAsync(later);
        elapsed.Stop();

        Assert.Less(elapsed.ElapsedMilliseconds, Options.LockWaitDeadlineMs * 4L,
            "a lock left behind by the failed writer would make the later writer wait out its deadline");

        KvTransaction verify = await database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
        Assert.AreEqual(next, await ReadBodyAsync(executor, db, verify, "a"));
        await database.Transactions.CommitAsync(verify);
        Assert.AreEqual(2, (await LargeValueKeysAsync(database)).Count, "the failed writer's value must not be stored");
    }
}
