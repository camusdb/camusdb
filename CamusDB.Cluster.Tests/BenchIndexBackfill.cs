/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

using Kahuna.Shared.KeyValue;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// Measures what an online <c>CREATE INDEX</c> costs on a three-node cluster: how long the build takes
/// for <see cref="Rows"/> rows with no other load, how long it takes while writers on every node run
/// two-row transfers, and how many transfers per second commit before, during and after the build.
/// The build and the transfers want the locks of the same rows, so a build that holds its row locks
/// for a shorter time both ends sooner and takes less throughput from the writers.
///
/// <para>Explicit: it loads tens of thousands of rows into an in-process cluster and prints numbers;
/// it asserts only that the index came out complete. The nodes keep their data in memory and share one
/// machine, so compare the numbers of two builds of this code on one machine, never with a server.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class BenchIndexBackfill
{
    private const int Rows = 20_000;

    private const int WritersPerNode = 4;

    private const int Runs = 3;

    private static readonly TimeSpan Window = TimeSpan.FromSeconds(6);

    private static readonly ILoggerFactory loggerFactory = LoggerFactory.Create(static builder => builder.SetMinimumLevel(LogLevel.Error));

    [Test]
    [Explicit("benchmark — run alone; prints the build time with no other load")]
    public async Task IdleBuild()
    {
        await using InProcessSchemaCluster cluster = await StartAsync();
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);
        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);

        await SeedAsync(cluster, db);

        // A first build that is not reported: the JIT and the caches of the path under test.
        await BuildAsync(leader, db, "warm", unique: false);

        for (int run = 1; run <= Runs; run++)
        {
            TimeSpan plain = await BuildAsync(leader, db, $"plain_{run}", unique: false);
            TimeSpan unique = await BuildAsync(leader, db, $"unique_{run}", unique: true);
            TestContext.Progress.WriteLine(
                $"run {run}, idle: plain {plain.TotalMilliseconds:F0} ms ({Rows / plain.TotalSeconds:F0} rows/s), " +
                $"unique {unique.TotalMilliseconds:F0} ms ({Rows / unique.TotalSeconds:F0} rows/s)");
        }
    }

    /// <summary>
    /// One build under load per cluster: the in-memory log and the intents of one loaded run slow the
    /// next one down, so two runs on one cluster are not comparable.
    /// </summary>
    [Test]
    [Explicit("benchmark — run alone; prints the build time and the foreground throughput")]
    public async Task BuildUnderLoad([Range(1, Runs)] int run)
    {
        await using InProcessSchemaCluster cluster = await StartAsync();
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);
        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);

        await SeedAsync(cluster, db);

        using CancellationTokenSource stop = new();
        long committed = 0;
        List<Task> writers = [];
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            for (int w = 0; w < WritersPerNode; w++)
            {
                int seed = run * 1_000 + node.Index * 100 + w;
                writers.Add(Task.Run(() => TransferUntilStopped(node, db, seed, () => Interlocked.Increment(ref committed), stop.Token)));
            }
        }

        try
        {
            // Not measured: the first writers pay for the JIT and for the cold caches.
            await Task.Delay(Window);

            double before = await RateOverAsync(() => Volatile.Read(ref committed), Task.Delay(Window));

            Stopwatch build = Stopwatch.StartNew();
            Task<TimeSpan> building = BuildAsync(leader, db, "balance_idx", unique: false);
            double during = await RateOverAsync(() => Volatile.Read(ref committed), building);
            build.Stop();

            double after = await RateOverAsync(() => Volatile.Read(ref committed), Task.Delay(Window));

            TestContext.Progress.WriteLine(
                $"run {run}, under load: build {build.Elapsed.TotalSeconds:F2} s ({Rows / build.Elapsed.TotalSeconds:F0} rows/s); " +
                $"transfers/s before {before:F0}, during {during:F0} ({100 * during / before:F0}%), after {after:F0} ({100 * after / before:F0}%)");
        }
        finally
        {
            stop.Cancel();
            await Task.WhenAll(writers);
        }
    }

    private static Task<InProcessSchemaCluster> StartAsync() =>
        InProcessSchemaCluster.StartAsync(
            nodeCount: 3, partitions: 3, wireLeaderForwarder: true,
            options: CamusDBOptions.Default with
            {
                DefaultTransactionLocking = KeyValueTransactionLocking.Optimistic,
                DefaultIsolationLevel = CamusIsolationLevel.ReadCommitted,
            },
            loggerFactory: loggerFactory, logger: loggerFactory.CreateLogger<ICamusDB>());

    /// <summary>The commits per second counted by <paramref name="count"/> while <paramref name="window"/> runs.</summary>
    private static async Task<double> RateOverAsync(Func<long> count, Task window)
    {
        long start = count();
        Stopwatch elapsed = Stopwatch.StartNew();
        await window;
        return (count() - start) / elapsed.Elapsed.TotalSeconds;
    }

    private static async Task<TimeSpan> BuildAsync(InProcessSchemaCluster.Node leader, string db, string index, bool unique)
    {
        string sql = unique
            ? $"CREATE UNIQUE INDEX {index} ON accounts (id, version)"
            : $"CREATE INDEX {index} ON accounts (balance) INCLUDE (version)";

        Stopwatch elapsed = Stopwatch.StartNew();
        await DdlAsync(leader, db, sql);
        return elapsed.Elapsed;
    }

    private static async Task TransferUntilStopped(
        InProcessSchemaCluster.Node node, string db, int seed, Action onCommit, CancellationToken stop)
    {
        Random random = new(seed);

        while (!stop.IsCancellationRequested)
        {
            long a = random.Next(1, Rows + 1);
            long b = random.Next(1, Rows + 1);
            if (a == b)
                continue;

            KvTransaction tx = await node.Database!.Transactions.BeginAsync();
            try
            {
                await node.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, $"UPDATE accounts SET balance = balance - 1 WHERE id = {a}", null));
                await node.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, $"UPDATE accounts SET balance = balance + 1 WHERE id = {b}", null));
                await node.Database.Transactions.CommitAsync(tx);
                onCommit();
            }
            catch (CamusDBException ex) when (SerializableRetryHelper.IsRetryable(ex) || ex.Code == CamusDBErrorCodes.TransactionFinalizeUnresolved)
            {
                await Task.Delay(1);
            }
            finally
            {
                await node.Database!.Transactions.RollbackIfNotCompletedAsync(tx);
            }
        }
    }

    private static async Task SeedAsync(InProcessSchemaCluster cluster, string db)
    {
        long version = 0;

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await DdlAsync(leader, db, "CREATE TABLE accounts (id INT64 PRIMARY KEY NOT NULL, balance INT64 NOT NULL, version INT64 NOT NULL)");

            const int rowsPerStatement = 500;
            for (int start = 1; start <= Rows; start += rowsPerStatement)
            {
                int end = Math.Min(Rows, start + rowsPerStatement - 1);
                string values = string.Join(", ", Enumerable.Range(start, end - start + 1).Select(i => $"({i}, {1_000_000 + i}, 0)"));
                await DmlAsync(leader, db, "INSERT INTO accounts (id, balance, version) VALUES " + values);
            }

            version = leader.Database!.Schema.SchemaVersion;
        });

        await cluster.WaitForSchemaConvergenceAsync(db, version, timeout: TimeSpan.FromSeconds(60));
    }

    private static Task DdlAsync(InProcessSchemaCluster.Node node, string db, string sql) =>
        node.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: db, sql: sql, parameters: null))
            .WaitAsync(TimeSpan.FromSeconds(600));

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
}
