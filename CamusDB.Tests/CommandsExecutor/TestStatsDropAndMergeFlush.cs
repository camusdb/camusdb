
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

using NUnit.Framework;
using Nito.AsyncEx;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Statistics.Models;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Statistics lifecycle around DROP TABLE and the cross-node flush merge:
///   1. An immediate (FORCE) DROP TABLE deletes the persisted <c>{dbId}:stats:{tableId}</c> blob
///      (previously only DROP DATABASE ever removed it).
///   2. A deferred (recoverable-orphan) DROP TABLE keeps the blob so a RELINK can reload it.
///   3. A flush is a read-merge-write: it adds this node's unflushed delta onto the currently
///      persisted counters instead of last-writer-wins overwriting deltas another node flushed.
/// </summary>
[TestFixture]
// Serial: shares one embedded Kahuna node across the fixture, so concurrent fixtures would
// interleave transactions and database names on the same node.
[NonParallelizable]
public sealed class TestStatsDropAndMergeFlush : SharedNodeBaseTest
{

    // Disable background flush scheduling so only the explicit FlushAsync calls in these tests
    // touch the persisted blob — a delayed scheduled flush racing the external blob writes or
    // the drop would make the assertions timing-dependent.
    /// <summary>
    /// These tests assert on flushes they trigger themselves, so the background flush timer is disabled
    /// for every engine this fixture builds — a scheduled flush landing mid-test would race the
    /// assertions.
    /// </summary>
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { StatsFlushIntervalMs = -1 };

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table)>
        SetupRobotsWithRows(int rows)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "robots",
            columns:
            [
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
            ],
            ifNotExists: false));
        await database.Transactions.CommitAsync(txn);

        TableDescriptor table = await (database.TableDescriptors["robots"]);

        // Ensure the stats entry is loaded before tracking so counters are absolute.
        await executor.Statistics.LoadByIdAsync(database, table.Id);

        await InsertRows(executor, database, dbname, rows);

        return (dbname, database, executor, table);
    }

    private static async Task InsertRows(CommandExecutor executor, DatabaseDescriptor database, string dbname, int count)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < count; i++)
            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", new(ColumnType.String, "r" + i) },
                    }
                }));
        await database.Transactions.CommitAsync(txn);
    }

    private async Task<TableStatistics?> ReadStatsBlob(string dbId, string tableId)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await SharedKahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, $"{dbId}:stats:{tableId}", -1, HLCTimestamp.Zero,
            KeyValueDurability.Persistent, CancellationToken.None);

        if (type != KeyValueResponseType.Get || entry?.Value is null)
            return null;

        return MetaJsonSerializer.DeserializeCompat(entry.Value, MetaJsonContext.Default.TableStatistics);
    }

    private async Task WriteStatsBlob(string dbId, string tableId, TableStatistics stats)
    {
        byte[] bytes = MetaJsonSerializer.Serialize(stats, MetaJsonContext.Default.TableStatistics);
        await SharedKahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, $"{dbId}:stats:{tableId}", bytes, null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, CancellationToken.None);
    }

    [Test]
    public async Task ForceDropTable_DeletesPersistedStatsBlob()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) =
            await SetupRobotsWithRows(3);

        await executor.Statistics.FlushAsync(database, table);

        TableStatistics? before = await ReadStatsBlob(database.Id, table.Id);
        Assert.IsNotNull(before, "Stats blob must exist after an explicit flush.");
        Assert.AreEqual(3, before!.RowCount);

        Assert.IsTrue(await executor.DropTable(new DropTableTicket(dbname, "robots", ifExists: false, force: true)));

        TableStatistics? after = await ReadStatsBlob(database.Id, table.Id);
        Assert.IsNull(after, "FORCE DROP TABLE must delete the persisted stats blob.");
    }

    [Test]
    public async Task DeferredDropTable_KeepsPersistedStatsBlobForRelink()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) =
            await SetupRobotsWithRows(3);

        await executor.Statistics.FlushAsync(database, table);

        Assert.IsTrue(await executor.DropTable(new DropTableTicket(dbname, "robots", ifExists: false)));

        TableStatistics? after = await ReadStatsBlob(database.Id, table.Id);
        Assert.IsNotNull(after,
            "A deferred (recoverable) drop must keep the stats blob so a RELINK can reload it; " +
            "the orphan reclaimer deletes it with the rest of the table keyspace.");
    }

    [Test]
    public async Task Flush_MergesLocalDeltaOntoPersistedCounters()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) =
            await SetupRobotsWithRows(3);

        // Flush 1: persists rowCount=3 and advances this node's flush baseline to 3.
        await executor.Statistics.FlushAsync(database, table);

        TableStatistics? flushed = await ReadStatsBlob(database.Id, table.Id);
        Assert.IsNotNull(flushed);
        Assert.AreEqual(3, flushed!.RowCount, "First flush must persist the tracked count.");

        // Simulate another node flushing its own deltas: bump the persisted count to 10.
        flushed.RowCount = 10;
        await WriteStatsBlob(database.Id, table.Id, flushed);

        // Track 2 more local rows (local absolute view = 5, unflushed delta = 2).
        await InsertRows(executor, database, dbname, 2);

        // Flush 2 must produce 10 + 2 = 12 — NOT last-writer-wins 5, which would silently
        // discard the other node's flushed contribution.
        await executor.Statistics.FlushAsync(database, table);

        TableStatistics? merged = await ReadStatsBlob(database.Id, table.Id);
        Assert.IsNotNull(merged);
        Assert.AreEqual(12, merged!.RowCount,
            "Flush must add the local unflushed delta onto the persisted counter, not overwrite it.");
    }
}
