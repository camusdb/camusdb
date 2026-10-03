/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// <c>ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY</c> and <c>DROP CONSTRAINT</c> across a three-node
/// cluster: a statement sent to a follower is forwarded and converges on every node, a leader change
/// after the constraint is WriteOnly leaves a job that the new leader finishes, and an ADD racing a DROP
/// TABLE of the parent sent to another node lets exactly one of them win.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster. Concurrent clusters contend for ports and skew
// each other's Raft election timing.
[NonParallelizable]
public sealed class TestClusterForeignKeyAlter
{
    private const string AddConstraint =
        "ALTER TABLE weather ADD CONSTRAINT weather_city_fk FOREIGN KEY (city) REFERENCES cities (name)";

    private const string OwnedIndex = "~fk_weather_city_fk";

    [Test]
    public async Task AddAndDropSentToAFollowerAreForwardedAndConverge()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3, wireLeaderForwarder: true);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);
        await SeedAsync(cluster, db);

        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);
        InProcessSchemaCluster.Node follower = cluster.Nodes.First(n => n.Index != leader.Index);
        InProcessSchemaCluster.Node other = cluster.Nodes.First(n => n.Index != leader.Index && n.Index != follower.Index);

        await DdlAsync(follower, db, AddConstraint);
        await cluster.WaitForSchemaConvergenceAsync(db, leader.Database!.Schema.SchemaVersion, timeout: TimeSpan.FromSeconds(30));

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            TableSchema weather = node.Database!.Schema.Tables["weather"];
            ForeignKeySchema constraint = weather.ForeignKeys!.Single();
            Assert.AreEqual(SchemaElementState.Public, constraint.State, $"node {node.Index}");
            Assert.AreEqual(constraint.Id, weather.Indexes!.Single(i => i.Name == OwnedIndex).OwnerConstraintId, $"node {node.Index}");
        }

        CamusDBException orphan = Assert.ThrowsAsync<CamusDBException>(async () =>
            await DmlAsync(other, db, "INSERT INTO weather (id, city) VALUES (900, 'atlantis')"))!;
        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, orphan.Code, orphan.Message);

        await DdlAsync(follower, db, "ALTER TABLE weather DROP CONSTRAINT weather_city_fk");
        await cluster.WaitForSchemaConvergenceAsync(db, leader.Database!.Schema.SchemaVersion, timeout: TimeSpan.FromSeconds(30));

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            TableSchema weather = node.Database!.Schema.Tables["weather"];
            Assert.IsNull(weather.ForeignKeys, $"node {node.Index}");
            Assert.IsFalse(weather.Indexes!.Any(i => i.Name == OwnedIndex), $"node {node.Index}: the owned index must go with the constraint");
        }

        await DmlAsync(other, db, "INSERT INTO weather (id, city) VALUES (900, 'atlantis')");
    }

    /// <summary>
    /// The coordinator stops after every node acked the constraint in WriteOnly, and leadership moves.
    /// The ALTER fails, the constraint stays enforced in WriteOnly, and the new leader's resume validates
    /// it and publishes it on every node.
    /// </summary>
    [Test]
    public async Task LeaderChangeDuringAddLeavesItWriteOnlyAndTheNewLeaderFinishesIt()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);
        await SeedAsync(cluster, db);

        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);
        InProcessSchemaCluster.Node next = cluster.Nodes.First(n => n.Index != leader.Index);

        long writeOnlyVersion = -1;
        List<SchemaElementState> statesAtTheStop = [];

        leader.Executor.TestInterceptBeforeForeignKeyValidation = async () =>
        {
            leader.Executor.TestInterceptBeforeForeignKeyValidation = null;
            writeOnlyVersion = leader.Database!.Schema.SchemaVersion;
            await cluster.WaitForSchemaConvergenceAsync(db, writeOnlyVersion, timeout: TimeSpan.FromSeconds(30));

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
                statesAtTheStop.Add(node.Database!.Schema.Tables["weather"].ForeignKeys!.Single().State);

            await cluster.TransferSchemaLeadershipAsync(db, next, timeout: TimeSpan.FromSeconds(30));
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "The coordinator stopped with its leadership");
        };

        CamusDBException stopped = Assert.ThrowsAsync<CamusDBException>(async () => await DdlAsync(leader, db, AddConstraint))!;
        Assert.That(stopped.Message, Does.Contain("stopped with its leadership"));

        Assert.AreEqual(Enumerable.Repeat(SchemaElementState.WriteOnly, cluster.Nodes.Length), statesAtTheStop,
            "Every node must enforce the constraint in WriteOnly before the validation runs");

        // The new leader's resume, started by the leadership callback, makes the constraint Public.
        await cluster.WaitForSchemaConvergenceAsync(db, writeOnlyVersion + 1, timeout: TimeSpan.FromSeconds(60));

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            Assert.AreEqual(SchemaElementState.Public, node.Database!.Schema.Tables["weather"].ForeignKeys!.Single().State, $"node {node.Index}");

        Assert.IsEmpty(await next.Executor.Catalogs.LoadCoordinatorJobsAsync(next.Database!), "The resumed job must be deleted once it finished");
    }

    /// <summary>
    /// ADD CONSTRAINT on the leader and DROP TABLE of the parent sent to a follower, at the same time.
    /// Exactly one wins, and every node agrees on the survivor after a reopen.
    /// </summary>
    [Test]
    public async Task AddRacingADropOfTheParentFromAnotherNodeLetsExactlyOneWin()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3, wireLeaderForwarder: true);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);
        await SeedAsync(cluster, db);

        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);
        InProcessSchemaCluster.Node follower = cluster.Nodes.First(n => n.Index != leader.Index);

        Task add = Task.Run(() => DdlAsync(leader, db, AddConstraint));
        Task drop = Task.Run(() => DdlAsync(follower, db, "DROP TABLE cities"));

        Exception? addError = await Capture(add);
        Exception? dropError = await Capture(drop);

        Assert.IsTrue((addError is null) ^ (dropError is null),
            $"Exactly one statement must succeed. ADD: {addError?.Message ?? "ok"}; DROP: {dropError?.Message ?? "ok"}");

        await cluster.WaitForSchemaConvergenceAsync(db, leader.Database!.Schema.SchemaVersion, timeout: TimeSpan.FromSeconds(30));

        for (int i = 0; i < cluster.Nodes.Length; i++)
            await cluster.ReopenDatabaseAsync(i, db);

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            Schema schema = node.Database!.Schema;
            TableSchema weather = schema.Tables["weather"];

            if (addError is null)
            {
                Assert.IsTrue(schema.Tables.ContainsKey("cities"), $"node {node.Index}");
                Assert.AreEqual(SchemaElementState.Public, weather.ForeignKeys!.Single().State, $"node {node.Index}");
                Assert.IsTrue(schema.ForeignKeys.ChildPlansOf(weather.Id!).Single().IsEnforced, $"node {node.Index}");
            }
            else
            {
                Assert.IsFalse(schema.Tables.ContainsKey("cities"), $"node {node.Index}");
                Assert.IsNull(weather.ForeignKeys, $"node {node.Index}");
                Assert.IsFalse(weather.Indexes!.Any(ix => ix.Name == OwnedIndex), $"node {node.Index}");
            }
        }

        if (addError is null)
            Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, ((CamusDBException)dropError!).Code, dropError.Message);
        else
            Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, ((CamusDBException)addError).Code, addError.Message);
    }

    private static async Task SeedAsync(InProcessSchemaCluster cluster, string db)
    {
        long version = 0;

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await DdlAsync(leader, db, "CREATE TABLE cities (id int64 PRIMARY KEY NOT NULL, name string NOT NULL, UNIQUE KEY cities_name (name))");
            await DdlAsync(leader, db, "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string)");
            await DmlAsync(leader, db, "INSERT INTO cities (id, name) VALUES (1, 'lima'), (2, 'quito')");
            await DmlAsync(leader, db, "INSERT INTO weather (id, city) VALUES (1, 'lima'), (2, 'quito'), (3, NULL)");
            version = leader.Database!.Schema.SchemaVersion;
        });

        await cluster.WaitForSchemaConvergenceAsync(db, version, timeout: TimeSpan.FromSeconds(30));
    }

    private static Task DdlAsync(InProcessSchemaCluster.Node node, string db, string sql) =>
        node.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: db, sql: sql, parameters: null))
            .WaitAsync(TimeSpan.FromSeconds(60));

    private static async Task DmlAsync(InProcessSchemaCluster.Node node, string db, string sql)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        try
        {
            await node.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: tx, database: db, sql: sql, parameters: null));
            await node.Database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<Exception?> Capture(Task task)
    {
        try
        {
            await task;
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }
}
