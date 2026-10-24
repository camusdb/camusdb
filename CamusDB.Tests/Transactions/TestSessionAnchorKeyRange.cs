/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Tests.CommandsExecutor;
using CamusDB.Tests.Storage;

namespace CamusDB.Tests.Transactions;

/// <summary>
/// Where a deferred session is placed when its first table is key-range routed. Kahuna routes an
/// unpinned coordinator key by hash, which names no key-range table's partition, so the session is
/// pinned to the partition of the table's one row range and the transaction carries Kahuna's pinned
/// key form. A hash-routed engine keeps the unpinned anchored key, and a refused pin falls back to an
/// unpinned start. The split cases run on a cluster (<c>TestSessionAnchorKeyRangeCluster</c>).
/// </summary>
[TestFixture]
// Serial: boots per-test embedded nodes with two partitions for key-range routing.
[NonParallelizable]
public sealed class TestSessionAnchorKeyRange : BaseTest
{
    protected override int NodeInitialPartitions => 2;

    private static async Task CreateAndFillAsync(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        foreach (string sql in new[]
        {
            "CREATE TABLE accounts (id INT64 PRIMARY KEY, balance INT64)",
            "CREATE INDEX balance_idx ON accounts (balance)",
        })
        {
            KvTransaction ddl = await database.Transactions.BeginAsync();
            try
            {
                await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, dbname, sql, null));
            }
            finally
            {
                await database.Transactions.RollbackIfNotCompletedAsync(ddl);
            }
        }

        KvTransaction insert = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(insert, dbname,
            "INSERT INTO accounts (id, balance) VALUES (1, 100), (2, 200)", null));
        await database.Transactions.CommitAsync(insert);
    }

    /// <summary>A deferred transaction that updates one row and commits, as a client transaction does.</summary>
    private static async Task<KvTransaction> RunUpdateAsync(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(deferStart: true);
        ExecuteNonSQLResult update = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            tx, dbname, "UPDATE accounts SET balance = balance + 1 WHERE id = 1", null));
        Assert.That(update.ModifiedRows, Is.EqualTo(1));
        await database.Transactions.CommitAsync(tx);
        return tx;
    }

    private static async Task<long> BalanceAsync(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(
            database.Transactions.BeginReadOnly(), dbname, "SELECT balance FROM accounts WHERE id = 1", null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        Assert.That(rows, Has.Count.EqualTo(1));
        return rows[0].Row["balance"].LongValue;
    }

    [Test]
    public async Task KeyRangeTable_PinsTheSessionToTheRowRangePartition()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(Options with { KeyRangeShardingEnabled = true });
        await CreateAndFillAsync(executor, database, dbname);

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, "accounts"));
        TablePlacement rows = TestNode!.ReadPlacementUncached(table.Store.RowKeySpace, out _);
        Assert.That(rows.IsKeyRange, Is.True, "the row space is key-range routed");
        Assert.That(rows.Spans, Has.Count.EqualTo(1), "the row space is one range");

        KvTransaction tx = await RunUpdateAsync(executor, database, dbname);

        Assert.Multiple(() =>
        {
            Assert.That(CoordinatorKeys.TryGetPinnedPartition(tx.CoordinatorKey, out int pinned), Is.True,
                $"the session key is pinned: {tx.CoordinatorKey}");
            Assert.That(pinned, Is.EqualTo(rows.Spans[0].PartitionId), "the pin names the row range's partition");
            Assert.That(tx.CoordinatorKey, Does.EndWith(
                KvKeyBuilder.SessionAnchorKeyOf(table.Store.PlacementGroup, tx.UniqueId)), "the pinned key wraps the anchored key");
        });

        Assert.That(await BalanceAsync(executor, database, dbname), Is.EqualTo(101L), "the pinned session committed");
    }

    [Test]
    public async Task HashTable_KeepsTheUnpinnedAnchoredKey()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(Options with { KeyRangeShardingEnabled = false });
        await CreateAndFillAsync(executor, database, dbname);
        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, "accounts"));

        KvTransaction tx = await RunUpdateAsync(executor, database, dbname);

        Assert.That(tx.CoordinatorKey, Is.EqualTo(KvKeyBuilder.SessionAnchorKeyOf(table.Store.PlacementGroup, tx.UniqueId)));
        Assert.That(CoordinatorKeys.IsReserved(tx.CoordinatorKey), Is.False);
        Assert.That(await BalanceAsync(executor, database, dbname), Is.EqualTo(101L));
    }

    /// <summary>
    /// Test double: reports the row space of <c>db1:t1</c> as one key-range span on a partition that
    /// does not exist, and refuses a pinned start with <see cref="KeyValueResponseType.InvalidInput"/> as
    /// Kahuna refuses an id that names no data partition. It records every start's options.
    /// </summary>
    private sealed class StalePartitionKahuna(IKahuna inner) : DelegatingKahuna(inner)
    {
        public const int MissingPartition = 99;

        public List<int?> StartPartitions { get; } = [];

        public override KahunaRangeMapResponse GetRangeMap(string? keySpace = null)
        {
            if (keySpace != "db1:t1|r")
                return base.GetRangeMap(keySpace);

            KahunaKeySpaceRangesResponse space = new() { KeySpace = keySpace, RoutingMode = "KeyRange" };
            space.Descriptors.Add(new KahunaRangeDescriptorResponse { PartitionId = MissingPartition, Generation = 1 });

            KahunaRangeMapResponse map = new() { Initialized = true };
            map.KeySpaces.Add(space);
            return map;
        }

        public override Task<(KeyValueResponseType, TransactionHandle)> LocateAndStartTransaction(
            KeyValueTransactionOptions options, CancellationToken cancellationToken)
        {
            lock (StartPartitions)
                StartPartitions.Add(options.CoordinatorPartitionId);

            if (options.CoordinatorPartitionId is not null)
                return Task.FromResult((KeyValueResponseType.InvalidInput, new TransactionHandle(default, options.CoordinatorKey)));

            return base.LocateAndStartTransaction(options, cancellationToken);
        }
    }

    [Test]
    public async Task RefusedPin_StartsTheSessionUnpinned()
    {
        StalePartitionKahuna kahuna = new(TestNode!.Kahuna);
        KvTransactionsManager manager = new(
            kahuna, Options with { KeyRangeShardingEnabled = true },
            _ => TestNode.Raft.HybridLogicalClock.SendOrLocalEvent(TestNode.Raft.GetLocalNodeId()));

        KvTransaction tx = await manager.BeginAsync(deferStart: true);
        await tx.EnsureSessionStartedAsync(CancellationToken.None, "db1:t1");

        Assert.Multiple(() =>
        {
            Assert.That(kahuna.StartPartitions, Is.EqualTo(new int?[] { StalePartitionKahuna.MissingPartition, null }),
                "one pinned start, refused, then one unpinned start");
            Assert.That(tx.TransactionId, Is.Not.EqualTo(default(Kommander.Time.HLCTimestamp)), "the session is open");
            Assert.That(tx.CoordinatorKey, Is.EqualTo(KvKeyBuilder.SessionAnchorKeyOf("db1:t1", tx.UniqueId)));
        });

        await manager.RollbackAsync(tx);
    }
}
