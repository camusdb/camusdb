/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Tests.Cluster;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Concurrent transactions that change the schema on three nodes, with transactional DDL on; see
/// <see cref="TransactionalDdlStress"/>. The workers of a scenario run on every node.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestClusterTransactionalDdlConcurrency
{
    private static async Task RunAsync(int seed, Func<InProcessSchemaCluster, string, TransactionalDdlStress, Task> scenario)
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3,
            partitions: 3,
            wireLeaderForwarder: true,
            options: CamusDBOptions.Default with { TransactionalDdlEnabled = true, CoordinatorJobSweepIntervalMs = 2_000 });

        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        TransactionalDdlStress stress = new(db, [.. cluster.Nodes.Select(n => new StressNode(n.Executor, n.Database!))], seed);
        await scenario(cluster, db, stress);
    }

    [Test]
    public Task DdlTransactionsOnOneTable([Values(1, 2)] int seed) =>
        RunAsync(seed, (_, _, stress) => stress.DdlTransactionsOnOneTableAsync(workers: 4, rounds: 10));

    [Test]
    public Task DdlTransactionsThatAddTheSameColumn([Values(1, 2)] int seed) =>
        RunAsync(seed, (_, _, stress) => stress.SameColumnAsync(workers: 3, rounds: 8));

    [Test]
    public Task DdlTransactionsBesideAutocommitDdl([Values(1, 2)] int seed) =>
        RunAsync(seed, (_, _, stress) => stress.DdlTransactionsBesideAutocommitDdlAsync(workers: 2, rounds: 10));

    [Test]
    public Task DdlTransactionsBesideWriters([Values(1, 2)] int seed) =>
        RunAsync(seed, (_, _, stress) => stress.DdlTransactionsBesideWritersAsync(
            writers: 6, schemaWorkers: 2, readers: 3, duration: TimeSpan.FromSeconds(10)));

    /// <summary>The schema leadership moves every two seconds while the transactions run.</summary>
    [Test]
    public Task DdlTransactionsBesideWriters_WhileTheSchemaLeaderMoves([Values(1, 2)] int seed) =>
        RunAsync(seed, (cluster, db, stress) => stress.DdlTransactionsBesideWritersAsync(
            writers: 6, schemaWorkers: 2, readers: 3, duration: TimeSpan.FromSeconds(20),
            disturb: async stop =>
            {
                while (!stop.IsCancellationRequested)
                {
                    InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);
                    await cluster.TransferSchemaLeadershipAsync(db, cluster.Nodes[(leader.Index + 1) % cluster.Nodes.Length]);
                    await Task.Delay(2_000);
                }
            }));
}
