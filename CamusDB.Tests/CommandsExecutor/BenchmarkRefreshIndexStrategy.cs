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
using System.Text;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Measures the two ways a materialized-view rebuild could maintain the staging relation's secondary
/// indexes, through the same engine paths a rebuild would use:
///
/// <list type="bullet">
/// <item><b>Eager</b> (what a refresh does): the staging relation is created with every index, and each
/// chunk of rows writes its index entries in the same transaction as the rows.</item>
/// <item><b>Deferred</b>: the rows are loaded with the primary key only, and each secondary index is
/// built afterwards by the <c>CREATE INDEX</c> backfill.</item>
/// </list>
///
/// <para>Both load in one transaction per chunk, sized the way a refresh sizes its chunks under the
/// default mutation limit: 70% of the limit, divided by the row plus one entry per index.</para>
///
/// <para>Explicit: it loads hundreds of thousands of rows and is run by hand. The outcome decides
/// whether refresh should defer its index builds; the numbers are recorded with the decision.</para>
/// </summary>
[Explicit("benchmark: run by hand")]
[NonParallelizable]
public sealed class BenchmarkRefreshIndexStrategy : SharedNodeBaseTest
{
    private static async Task Exec(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, bool ddl)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);

        if (ddl)
            await executor.ExecuteDDLSQL(ticket);
        else
            await executor.ExecuteNonSQLQuery(ticket);

        await database.Transactions.CommitAsync(tx);
    }

    /// <summary>The rows a refresh puts in one chunk for a relation with <paramref name="indexCount"/> indexes, primary key included.</summary>
    private static int ChunkRows(CamusDB.Core.CamusDBOptions options, int indexCount)
        => Math.Min(options.MaterializedViewRefreshChunkRows, options.MaxMutationsPerTransaction * 70 / 100 / (1 + indexCount));

    private static async Task LoadInChunks(DatabaseDescriptor database, CommandExecutor executor, string dbname, string table, int rows, int chunk)
    {
        for (int low = 0; low < rows; low += chunk)
            await Exec(database, executor, dbname,
                $"INSERT INTO {table} (id, a, b, c, s) SELECT id, a, b, c, s FROM src WHERE id >= {low} AND id < {low + chunk}", ddl: false);
    }

    [TestCase(20_000, 1)]
    [TestCase(20_000, 3)]
    [TestCase(100_000, 1)]
    [TestCase(100_000, 3)]
    public async Task CompareEagerAndDeferredIndexMaintenance(int rows, int indexes)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase();

        await Exec(database, executor, dbname,
            "CREATE TABLE src (id int64 PRIMARY KEY, a int64, b int64, c int64, s string(32))", ddl: true);

        // Loaded in chunks of 5,000, the size a refresh chunk has with a few indexes.
        Random random = new(42);
        for (int start = 0; start < rows; start += 5_000)
        {
            StringBuilder insert = new("INSERT INTO src (id, a, b, c, s) VALUES ");
            insert.Append(string.Join(", ", Enumerable.Range(start, Math.Min(5_000, rows - start)).Select(i =>
                $"({i}, {random.Next(0, 1_000_000)}, {random.Next(0, 1_000)}, {random.Next()}, 'name-{random.Next(0, 100_000)}')")));
            await Exec(database, executor, dbname, insert.ToString(), ddl: false);
        }

        string[] indexColumns = ["a", "b", "s"];
        string Indexes(string table) => string.Join(", ", indexColumns.Take(indexes).Select(c => $"KEY {table}_{c} ({c})"));

        // Eager: the relation has every index while the rows go in.
        await Exec(database, executor, dbname,
            $"CREATE TABLE eager (id int64 PRIMARY KEY, a int64, b int64, c int64, s string(32), {Indexes("eager")})", ddl: true);

        Stopwatch eager = Stopwatch.StartNew();
        await LoadInChunks(database, executor, dbname, "eager", rows, ChunkRows(Options, indexCount: 1 + indexes));
        eager.Stop();

        // Deferred: the rows go in with the primary key only, then each index is built.
        await Exec(database, executor, dbname,
            "CREATE TABLE deferred (id int64 PRIMARY KEY, a int64, b int64, c int64, s string(32))", ddl: true);

        Stopwatch deferredLoad = Stopwatch.StartNew();
        await LoadInChunks(database, executor, dbname, "deferred", rows, ChunkRows(Options, indexCount: 1));
        deferredLoad.Stop();

        Stopwatch deferredBuild = Stopwatch.StartNew();
        foreach (string column in indexColumns.Take(indexes))
            await Exec(database, executor, dbname, $"CREATE INDEX deferred_{column} ON deferred ({column})", ddl: true);
        deferredBuild.Stop();

        long deferredTotal = deferredLoad.ElapsedMilliseconds + deferredBuild.ElapsedMilliseconds;

        TestContext.Progress.WriteLine(
            $"rows={rows} indexes={indexes} eager={eager.ElapsedMilliseconds} ms " +
            $"deferred={deferredTotal} ms (load {deferredLoad.ElapsedMilliseconds} + build {deferredBuild.ElapsedMilliseconds}) " +
            $"ratio={(double)deferredTotal / Math.Max(1, eager.ElapsedMilliseconds):F2}");
    }
}
