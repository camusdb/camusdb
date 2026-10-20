/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Under key-range sharding a serializable read-only transaction is the lock-free snapshot the
/// isolation guide promises: a writer on the slot it looked up is neither refused by it nor refuses
/// it, and its reads stay pinned at its timestamp throughout.
///
/// <para>The shape is the one a workload of index lookups against moving rows produces: a reader
/// looks one slot up through a secondary index while a writer updates, or deletes and re-inserts, the
/// row behind that slot. With the shared lock the snapshot used to take here, the writer's exclusive
/// lock over the slot was refused with a conflict while the snapshot was open, and the snapshot's
/// next lookup was refused while the writer held the slot.</para>
///
/// <para>Every writer here starts after the snapshot. Kahuna lets a snapshot read pass a foreign
/// write intent that cannot commit at or before the snapshot's timestamp, which a younger writer's
/// cannot. An older in-flight writer is waited for by every snapshot read, lock or no lock, and that
/// intent rule is not what these tests are about.</para>
/// </summary>
[TestFixture]
// Serial: boots a per-test embedded node with two partitions for key-range routing.
[NonParallelizable]
public sealed class TestSerializableReadOnlyKeyRange : BaseTest
{
    private const int Slots = 20;

    /// <summary>
    /// A hang guard on a snapshot lookup. A lookup refused by a lock fails with a conflict first; a
    /// lookup that waits on a lock or an intent it should pass would otherwise stall the test.
    /// </summary>
    private static readonly TimeSpan ReadBudget = TimeSpan.FromSeconds(5);

    protected override int NodeInitialPartitions => 2;

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { KeyRangeShardingEnabled = true };

    private static async Task ExecuteDdl(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>
    /// Creates <c>tokens</c> with an index on <c>slot</c> (an Integer64 index, so it is range-routed)
    /// and one row per slot.
    /// </summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE TABLE tokens (id INT64 PRIMARY KEY, slot INT64, token STRING)");
        await ExecuteDdl(executor, dbname, "CREATE INDEX tokens_slot ON tokens (slot)");

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            for (int slot = 1; slot <= Slots; slot++)
            {
                await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
                    $"INSERT INTO tokens (id, slot, token) VALUES ({slot}, {slot}, 'token-{slot}')", null));
            }

            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        return (dbname, database, executor);
    }

    /// <summary>The tokens the index lookup of <paramref name="slot"/> returns in <paramref name="tx"/>.</summary>
    private static async Task<List<string>> LookupSlot(CommandExecutor executor, string dbname, KvTransaction tx, long slot)
    {
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, dbname, $"SELECT token FROM tokens WHERE slot = {slot}", null));

        List<string> tokens = [];

        await foreach (QueryResultRow row in cursor)
            tokens.Add(row.Row["token"].StrValue!);

        return tokens;
    }

    /// <summary>A snapshot lookup that must complete inside <see cref="ReadBudget"/>.</summary>
    private static async Task<List<string>> LookupSlotPromptly(CommandExecutor executor, string dbname, KvTransaction tx, long slot)
    {
        long started = Stopwatch.GetTimestamp();
        List<string> tokens = await LookupSlot(executor, dbname, tx, slot);

        Assert.That(Stopwatch.GetElapsedTime(started), Is.LessThan(ReadBudget),
            "a snapshot lookup must not wait on a writer");

        return tokens;
    }

    /// <summary>
    /// Runs the scenario: the snapshot looks slot 7 up, a younger writer runs <paramref name="writerSql"/>
    /// against slot 7 and stays open, the snapshot looks slot 7 up again under the writer's locks and
    /// intents, the writer commits, and the snapshot still reads its pinned instant.
    /// </summary>
    private async Task RunScenario(
        CamusIsolationLevel writerIsolation, KeyValueTransactionLocking writerLocking, params string[] writerSql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction snapshot = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
        CollectionAssert.AreEqual(new[] { "token-7" }, await LookupSlotPromptly(executor, dbname, snapshot, 7));

        KvTransaction writer = await database.Transactions.BeginAsync(writerIsolation, locking: writerLocking);
        try
        {
            // Before the fix the snapshot held a shared lock on the slot's index range, and this younger
            // writer's exclusive locate-scan lock over the same range was refused with a conflict.
            Assert.DoesNotThrowAsync(async () =>
            {
                foreach (string sql in writerSql)
                    await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(writer, dbname, sql, null));
            }, "a writer on the slot the snapshot looked up must not be refused");

            // With the writer's exclusive lock and live intents on the slot, the snapshot looks it up
            // again. Before the fix this shared acquire waited out the lock-wait deadline and was refused.
            CollectionAssert.AreEqual(new[] { "token-7" }, await LookupSlotPromptly(executor, dbname, snapshot, 7),
                "the snapshot must read its pinned instant while a younger writer holds the slot");

            Assert.DoesNotThrowAsync(async () => await database.Transactions.CommitAsync(writer),
                "the writer's commit must not be fenced by the snapshot");
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(writer);
        }

        CollectionAssert.AreEqual(new[] { "token-7" }, await LookupSlotPromptly(executor, dbname, snapshot, 7),
            "the snapshot stays pinned after the writer commits");
        Assert.AreEqual(0, snapshot.GetAcquiredRangeLocks().Count, "the snapshot took no range lock throughout");
        await database.Transactions.CommitAsync(snapshot);

        KvTransaction latest = await database.Transactions.BeginAsync();
        try
        {
            CollectionAssert.AreEqual(new[] { "moved" }, await LookupSlot(executor, dbname, latest, 7),
                "a latest-state read sees the writer's committed change");
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(latest);
        }
    }

    /// <summary>
    /// A pessimistic Serializable read-write <c>UPDATE</c> of the slot: its locate scan takes an exclusive
    /// lock over the slot's index range and it stages the row behind the slot.
    /// </summary>
    [Test]
    public Task Snapshot_UpdateOfTheLookedUpSlot_NeitherRefusesNorIsRefused()
        => RunScenario(CamusIsolationLevel.Serializable, KeyValueTransactionLocking.Pessimistic,
            "UPDATE tokens SET token = 'moved' WHERE slot = 7");

    /// <summary>
    /// The move shape: an optimistic Read Committed writer deletes the row behind the slot and inserts a
    /// new one, so the slot's index entries carry live intents when the snapshot looks it up.
    /// </summary>
    [Test]
    public Task Snapshot_MoveOfTheLookedUpSlot_NeitherRefusesNorIsRefused()
        => RunScenario(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Optimistic,
            "DELETE FROM tokens WHERE slot = 7",
            "INSERT INTO tokens (id, slot, token) VALUES (107, 7, 'moved')");
}
