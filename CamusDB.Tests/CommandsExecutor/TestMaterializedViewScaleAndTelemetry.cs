/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Scale cases and telemetry of materialized views. The always-on cases are small enough for every run;
/// the large ones are explicit and run by hand.
/// </summary>
[NonParallelizable]
public sealed class TestMaterializedViewScaleAndTelemetry : SharedNodeBaseTest
{
    private static async Task Exec(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, bool ddl = true)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
            if (ddl)
                await executor.ExecuteDDLSQL(ticket);
            else
                await executor.ExecuteNonSQLQuery(ticket);
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<int> Count(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            int count = (await cursor.ToListAsync()).Count;
            await database.Transactions.CommitAsync(tx);
            return count;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task InsertRows(DatabaseDescriptor database, CommandExecutor executor, string dbname, int rows, int width = 0)
    {
        for (int start = 0; start < rows; start += 1_000)
        {
            StringBuilder insert = new("INSERT INTO src (id, a, b, s) VALUES ");
            insert.Append(string.Join(", ", Enumerable.Range(start, System.Math.Min(1_000, rows - start))
                .Select(i => $"({i}, {i % 97}, {i % 13}, '{new string('x', width)}{i}')")));
            await Exec(database, executor, dbname, insert.ToString(), ddl: false);
        }
    }

    [TearDown]
    public void ClearSeams() => MaterializedViewRefresher.FaultInjectionForTesting = null;

    /// <summary>
    /// A refresh that runs longer than its snapshot hold's lease: the hold is renewed while the rebuild
    /// runs, so the refresh publishes rather than failing closed, and the renewals are counted.
    /// </summary>
    [Test]
    public async Task ARefreshLongerThanItsHoldLeaseKeepsTheHoldAndPublishes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options with
        {
            MaterializedViewRefreshHoldLeaseMs = 5_000,
            MaterializedViewRefreshChunkRows = 10,
        });

        await Exec(database, executor, dbname, "CREATE TABLE src (id int64 PRIMARY KEY, a int64, b int64, s string(64))");
        await InsertRows(database, executor, dbname, 70);
        await Exec(database, executor, dbname, "CREATE MATERIALIZED VIEW m AS SELECT id, a FROM src WITH NO DATA");

        long renewalsBefore = MaterializedViewRefreshMetrics.HoldRenewals;
        long publishedBefore = MaterializedViewRefreshMetrics.Published;

        // Seven chunks of one second each: longer than the five-second lease.
        MaterializedViewRefresher.FaultInjectionForTesting = point =>
            point == MaterializedViewRefresher.FaultPoint.AfterChunk ? Task.Delay(1_000) : Task.CompletedTask;

        await Exec(database, executor, dbname, "REFRESH MATERIALIZED VIEW m", ddl: false);

        Assert.AreEqual(70, await Count(database, executor, dbname, "SELECT id FROM m"));
        Assert.Greater(MaterializedViewRefreshMetrics.HoldRenewals, renewalsBefore, "the hold must have been renewed during the rebuild");
        Assert.AreEqual(publishedBefore + 1, MaterializedViewRefreshMetrics.Published);
    }

    /// <summary>Each rebuild's work and outcome is counted, a failure as a failure and a conflict as a conflict.</summary>
    [Test]
    public async Task RebuildsAreCountedByOutcomeAndWork()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Exec(database, executor, dbname, "CREATE TABLE src (id int64 PRIMARY KEY, a int64, b int64, s string(64))");
        await InsertRows(database, executor, dbname, 50);

        long published = MaterializedViewRefreshMetrics.Published;
        long rows = MaterializedViewRefreshMetrics.Rows;
        long mutations = MaterializedViewRefreshMetrics.Mutations;
        long bytes = MaterializedViewRefreshMetrics.Bytes;
        long failed = MaterializedViewRefreshMetrics.Failed;
        long conflicted = MaterializedViewRefreshMetrics.Conflicted;

        await Exec(database, executor, dbname, "CREATE MATERIALIZED VIEW m AS SELECT id, a FROM src");
        await Exec(database, executor, dbname, "CREATE INDEX m_a ON m (a)");
        await Exec(database, executor, dbname, "REFRESH MATERIALIZED VIEW m", ddl: false);

        Assert.AreEqual(published + 2, MaterializedViewRefreshMetrics.Published, "the create and the refresh");
        Assert.AreEqual(rows + 100, MaterializedViewRefreshMetrics.Rows);
        Assert.GreaterOrEqual(MaterializedViewRefreshMetrics.Mutations - mutations, 50 * 2 + 50 * 3,
            "each row costs itself plus one write per index");
        Assert.Greater(MaterializedViewRefreshMetrics.Bytes, bytes);

        MaterializedViewRefresher.FaultInjectionForTesting = point =>
            point == MaterializedViewRefresher.FaultPoint.AfterFinalChunk
                ? throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "fails")
                : Task.CompletedTask;
        Assert.ThrowsAsync<CamusDBException>(async () => await Exec(database, executor, dbname, "REFRESH MATERIALIZED VIEW m", ddl: false));
        Assert.AreEqual(failed + 1, MaterializedViewRefreshMetrics.Failed);

        MaterializedViewRefresher.FaultInjectionForTesting = async point =>
        {
            if (point != MaterializedViewRefresher.FaultPoint.AfterStaging)
                return;
            MaterializedViewRefresher.FaultInjectionForTesting = null;
            await Exec(database, executor, dbname, "CREATE INDEX m_b ON m (id)");
        };
        Assert.ThrowsAsync<CamusDBException>(async () => await Exec(database, executor, dbname, "REFRESH MATERIALIZED VIEW m", ddl: false));
        Assert.AreEqual(conflicted + 1, MaterializedViewRefreshMetrics.Conflicted);
    }

    /// <summary>Many indexes: every index survives the rebuild and serves seeks, and the chunk size keeps under the mutation cap.</summary>
    [Test]
    public async Task ManyIndexesSurviveARefreshUnderTheMutationCap()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options with
        {
            MaxMutationsPerTransaction = 2_000,
        });

        await Exec(database, executor, dbname, "CREATE TABLE src (id int64 PRIMARY KEY, a int64, b int64, s string(64))");
        await InsertRows(database, executor, dbname, 3_000);
        await Exec(database, executor, dbname, "CREATE MATERIALIZED VIEW m AS SELECT id, a, b, s FROM src");

        foreach (string index in new[] { "CREATE INDEX m_a ON m (a)", "CREATE INDEX m_b ON m (b)", "CREATE INDEX m_s ON m (s)",
                                         "CREATE INDEX m_ab ON m (a, b)", "CREATE INDEX m_ba ON m (b, a)" })
            await Exec(database, executor, dbname, index);

        await Exec(database, executor, dbname, "REFRESH MATERIALIZED VIEW m", ddl: false);

        Assert.AreEqual(3_000, await Count(database, executor, dbname, "SELECT id FROM m"));
        Assert.AreEqual(3_000 / 97 + 1, await Count(database, executor, dbname, "SELECT id FROM m WHERE a = 0"));
        CollectionAssert.IsSubsetOf(new[] { "m_a", "m_b", "m_s", "m_ab", "m_ba" },
            (database.Schema.Tables["m"].Indexes ?? []).Select(i => i.Name).ToList(), "every index survives the rebuild");
    }

    /// <summary>Large rows, a wide definition and high dependency fan-out at a scale too slow for every run.</summary>
    [Test, Explicit("scale: run by hand")]
    public async Task LargeRowsWideDefinitionsAndHighFanOut()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options with { MaxMutationsPerTransaction = 0 });

        await Exec(database, executor, dbname, "CREATE TABLE src (id int64 PRIMARY KEY, a int64, b int64, s string(4096))");
        await InsertRows(database, executor, dbname, 20_000, width: 3_000);

        System.Diagnostics.Stopwatch watch = System.Diagnostics.Stopwatch.StartNew();
        await Exec(database, executor, dbname, "CREATE MATERIALIZED VIEW big AS SELECT id, s FROM src");
        TestContext.Progress.WriteLine($"20,000 rows of 3 KB materialized in {watch.ElapsedMilliseconds} ms");
        Assert.AreEqual(20_000, await Count(database, executor, dbname, "SELECT id FROM big"));

        string wide = string.Join(", ", Enumerable.Range(0, 500).Select(i => $"a + {i} AS c{i}"));
        await Exec(database, executor, dbname, $"CREATE VIEW wide AS SELECT {wide} FROM src");
        await Exec(database, executor, dbname, $"CREATE MATERIALIZED VIEW wide_m AS SELECT {wide} FROM src WHERE id < 100");
        Assert.AreEqual(100, await Count(database, executor, dbname, "SELECT c0 FROM wide_m"));

        await Exec(database, executor, dbname, "CREATE VIEW base AS SELECT id, a FROM src");
        for (int i = 0; i < 500; i++)
            await Exec(database, executor, dbname, $"CREATE VIEW fan{i} AS SELECT id FROM base WHERE a = {i % 97}");

        watch.Restart();
        await Exec(database, executor, dbname, "DROP VIEW base CASCADE");
        TestContext.Progress.WriteLine($"cascade over 500 dependents in {watch.ElapsedMilliseconds} ms");
        Assert.IsFalse(database.Schema.Views.Keys.Any(name => name.StartsWith("fan")));
    }
}
