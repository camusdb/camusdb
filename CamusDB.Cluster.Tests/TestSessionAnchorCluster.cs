/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// Where a client-owned transaction's Kahuna session lives on a real three-node cluster. Kahuna
/// counts every operation registration it sends to the session owner (meter <c>Kahuna</c>,
/// instrument <c>kahuna.durable_tx.session_registration_forwards</c>, tag <c>route</c>). With the
/// session anchored to the first touched table, a transaction executed on that table's data leader
/// registers everything locally; the same transaction executed on another node forwards every
/// registration. The meter is process-wide, so each test measures the delta around its own
/// transaction and runs serially.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSessionAnchorCluster
{
    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger = sharedLoggerFactory.CreateLogger<ICamusDB>();

    /// <summary>Sums the registration counter by <c>route</c> while it is alive.</summary>
    private sealed class RegistrationProbe : IDisposable
    {
        private readonly MeterListener listener = new();
        private readonly object gate = new();
        private long forwarded;
        private long local;

        public RegistrationProbe()
        {
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == "Kahuna" && instrument.Name == "kahuna.durable_tx.session_registration_forwards")
                    l.EnableMeasurementEvents(instrument);
            };
            listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                foreach (KeyValuePair<string, object?> tag in tags)
                {
                    if (tag.Key != "route")
                        continue;
                    lock (gate)
                    {
                        if (Equals(tag.Value, "forwarded")) forwarded += value;
                        else if (Equals(tag.Value, "local")) local += value;
                    }
                }
            });
            listener.Start();
        }

        public long Forwarded { get { lock (gate) return forwarded; } }

        public long Local { get { lock (gate) return local; } }

        public void Dispose() => listener.Dispose();
    }

    private static async Task<(InProcessSchemaCluster cluster, string db, string rowId)> SetupAsync()
    {
        InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3, partitions: 3, loggerFactory: sharedLoggerFactory, logger: logger);

        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "accounts",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("balance", ColumnType.Integer64),
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        string rowId = ObjectIdGenerator.Generate().ToString();
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            KvTransaction setup = await leader.Database!.Transactions.BeginAsync();
            await leader.Executor.Insert(new InsertTicket(
                txnState: setup, databaseName: db, tableName: "accounts",
                values: new() { new() {
                    { "id",      new(ColumnType.Id,        rowId) },
                    { "balance", new(ColumnType.Integer64, 100L) },
                }}));
            await leader.Database.Transactions.CommitAsync(setup);
        });

        return (cluster, db, rowId);
    }

    /// <summary>The node that currently leads the partition holding the table's rows and indexes.</summary>
    private static async Task<InProcessSchemaCluster.Node> DataLeaderAsync(InProcessSchemaCluster cluster, string db)
    {
        for (int attempt = 0; attempt < 50; attempt++)
        {
            InProcessSchemaCluster.Node any = cluster.Nodes[0];
            TableDescriptor table = await any.Executor.OpenTable(new OpenTableTicket(db, "accounts"));
            TablePlacement placement = any.Kahuna.ReadPlacementUncached(table.Store.RowKeySpace, out bool initialized);
            string? leader = initialized && placement.Spans.Count == 1 ? placement.Spans[0].LeaderEndpoint : null;
            if (leader is not null)
            {
                InProcessSchemaCluster.Node? node = cluster.Nodes.FirstOrDefault(n => n.Kahuna.Raft.GetLocalEndpoint() == leader);
                if (node is not null)
                    return node;
            }
            await Task.Delay(200);
        }

        throw new Exception("No data leader for the accounts table became known");
    }

    /// <summary>A client-owned transaction: read the row, update it, commit — all on one node.</summary>
    private static async Task<KvTransaction> RunTransferAsync(InProcessSchemaCluster.Node node, string db, string rowId)
    {
        DatabaseDescriptor database = node.Database!;
        KvTransaction tx = await database.Transactions.BeginAsync(deferStart: true);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(
            txnState: tx, database: db, sql: $"SELECT balance FROM accounts WHERE id = '{rowId}'", parameters: null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        Assert.That(rows, Has.Count.EqualTo(1));

        ExecuteNonSQLResult update = await node.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: tx, database: db, sql: $"UPDATE accounts SET balance = balance + 1 WHERE id = '{rowId}'", parameters: null));
        Assert.That(update.ModifiedRows, Is.EqualTo(1));

        await database.Transactions.CommitAsync(tx);
        return tx;
    }

    [Test]
    public async Task TransactionOnTheDataLeader_RegistersEverythingLocally()
    {
        (InProcessSchemaCluster cluster, string db, string rowId) = await SetupAsync();
        await using InProcessSchemaCluster _ = cluster;

        InProcessSchemaCluster.Node leader = await DataLeaderAsync(cluster, db);
        TableDescriptor table = await leader.Executor.OpenTable(new OpenTableTicket(db, "accounts"));

        using RegistrationProbe probe = new();
        KvTransaction tx = await RunTransferAsync(leader, db, rowId);

        Assert.That(tx.CoordinatorKey, Is.EqualTo(KvKeyBuilder.SessionAnchorKeyOf(table.Store.PlacementGroup, tx.UniqueId)));
        Assert.That(probe.Local, Is.GreaterThan(0), "the transaction registered operations");
        Assert.That(probe.Forwarded, Is.EqualTo(0),
            "a session anchored on the executing node's own partition forwards no registration");
    }

    [Test]
    public async Task TransactionOffTheDataLeader_ForwardsRegistrationsToTheAnchor()
    {
        (InProcessSchemaCluster cluster, string db, string rowId) = await SetupAsync();
        await using InProcessSchemaCluster _ = cluster;

        InProcessSchemaCluster.Node leader = await DataLeaderAsync(cluster, db);
        InProcessSchemaCluster.Node follower = cluster.Nodes.First(n => n.Index != leader.Index);
        TableDescriptor table = await follower.Executor.OpenTable(new OpenTableTicket(db, "accounts"));

        using RegistrationProbe probe = new();
        KvTransaction tx = await RunTransferAsync(follower, db, rowId);

        Assert.That(tx.CoordinatorKey, Is.EqualTo(KvKeyBuilder.SessionAnchorKeyOf(table.Store.PlacementGroup, tx.UniqueId)));
        Assert.That(probe.Forwarded, Is.GreaterThan(0),
            "executed away from the anchor, every registration is a forward to the data leader");
    }
}
