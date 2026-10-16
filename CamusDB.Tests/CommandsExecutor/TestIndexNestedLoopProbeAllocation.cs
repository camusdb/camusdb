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
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using NUnit.Framework;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The allocation of one index nested-loop probe that matches one row, the common case of a join
/// through a foreign key. The budget covers the whole pipeline of one outer row: the scan of the
/// outer row, the probe, the fetch and decode of the one inner row, the pair, the projection and
/// the result row, on both sides of the Kahuna boundary. A probe used to build, per outer row, a
/// large-value fetch with a concurrent dictionary inside, a wrapper enumerator, a closure and a
/// delegate for the retry, a task per synchronous answer, and a string per index entry.
/// <para>
/// The budgets are about 15 percent above the cost measured on 2026-10-15 in a Debug build (where
/// every async state machine is a class): 15.9 KB for the non-unique probe and 10.4 KB for the
/// unique one, most of it the depth of the async chain on both sides and Kahuna's own objects per
/// read. The margin covers the run-to-run noise of the measurement, so the test catches the return
/// of a per-probe object of a kilobyte or more, such as the ones above, and not a small one.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestIndexNestedLoopProbeAllocation : BaseTest
{
    private sealed record Db(string Name, DatabaseDescriptor Database, CommandExecutor Executor);

    private const int Parents = 2_000;

    /// <summary>Bytes allocated per outer row by a join whose probe walks a non-unique index to one entry.</summary>
    private const long NonUniqueProbeBudgetBytes = 18_000;

    /// <summary>Bytes allocated per outer row by a join whose probe reads a unique index entry and its row.</summary>
    private const long UniqueProbeBudgetBytes = 12_000;

    /// <summary>
    /// The heuristic join order keeps the FROM order, so the outer table is the one named first.
    /// Auto-analyze is off; the test runs <c>ANALYZE</c> itself.
    /// </summary>
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) =>
        defaults with { CostBasedJoinOrderEnabled = false, AutoAnalyzeEnabled = false };

    [Test]
    public async Task NonUniqueProbe_OneMatch_StaysInsideTheAllocationBudget()
    {
        Db db = await CreateTables();

        long perRow = await AllocatedBytesPerOuterRow(db,
            "SELECT p.id AS parent, c.id AS child FROM parents p JOIN children c ON c.parent_id = p.id");

        TestContext.Out.WriteLine($"non-unique probe, one match: {perRow} bytes per outer row");
        Assert.LessOrEqual(perRow, NonUniqueProbeBudgetBytes,
            $"a non-unique probe that matches one row allocated {perRow} bytes per outer row, over the budget of {NonUniqueProbeBudgetBytes}");
    }

    [Test]
    public async Task UniqueProbe_OneMatch_StaysInsideTheAllocationBudget()
    {
        Db db = await CreateTables();

        long perRow = await AllocatedBytesPerOuterRow(db,
            "SELECT c.id AS child, p.id AS parent FROM children c JOIN parents p ON p.id = c.parent_id");

        TestContext.Out.WriteLine($"unique probe, one match: {perRow} bytes per outer row");
        Assert.LessOrEqual(perRow, UniqueProbeBudgetBytes,
            $"a unique probe allocated {perRow} bytes per outer row, over the budget of {UniqueProbeBudgetBytes}");
    }

    /// <summary>
    /// Runs <paramref name="sql"/> once to warm the caches, then three times under
    /// <see cref="GC.GetTotalAllocatedBytes"/>, and returns the smallest allocation per outer row.
    /// The count is process-wide because a probe resumes on the thread that answers it, so a
    /// per-thread count would miss most of the work; the node's background work adds a few
    /// kilobytes per second, which the smallest of three runs over two thousand rows absorbs.
    /// </summary>
    private static async Task<long> AllocatedBytesPerOuterRow(Db db, string sql)
    {
        db.Executor.Statistics.ForceIndexNestedLoopForTesting = true;

        int warm = await RunAndCount(db, sql);
        Assert.AreEqual(Parents, warm, "every outer row matches exactly one inner row");

        long best = long.MaxValue;
        for (int run = 0; run < 3; run++)
        {
            long before = GC.GetTotalAllocatedBytes(precise: true);
            int rows = await RunAndCount(db, sql);
            long after = GC.GetTotalAllocatedBytes(precise: true);
            Assert.AreEqual(Parents, rows);
            best = Math.Min(best, (after - before) / Parents);
        }

        return best;
    }

    private static async Task<int> RunAndCount(Db db, string sql)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await db.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: sql, parameters: null));

        int count = 0;
        await foreach (QueryResultRow _ in cursor)
            count++;

        await db.Database.Transactions.CommitAsync(txn);
        return count;
    }

    /// <summary>
    /// parents(id) with two thousand rows and children(id, parent_id) with one child per parent,
    /// a non-unique index on the child key; both tables analyzed, so the probe has an estimate of
    /// one match per key.
    /// </summary>
    private async Task<Db> CreateTables()
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);
        Db db = new(name, database, executor);

        await Ddl(db, "CREATE TABLE parents (id int64 primary key, v int64)");
        await Ddl(db, "CREATE TABLE children (id int64 primary key, parent_id int64)");
        await Ddl(db, "CREATE INDEX children_parent_idx ON children (parent_id)");

        for (int batch = 0; batch < Parents; batch += 500)
        {
            await Exec(db, "INSERT INTO parents (id, v) VALUES " +
                string.Join(", ", Enumerable.Range(batch + 1, 500).Select(i => $"({i}, {i * 7})")));
            await Exec(db, "INSERT INTO children (id, parent_id) VALUES " +
                string.Join(", ", Enumerable.Range(batch + 1, 500).Select(i => $"({i}, {i})")));
        }

        await Analyze(db, "parents");
        await Analyze(db, "children");
        return db;
    }

    private static async Task Ddl(Db db, string sql)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        await db.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: sql, parameters: null));
        await db.Database.Transactions.CommitAsync(txn);
    }

    private static async Task Exec(Db db, string sql)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        await db.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: sql, parameters: null));
        await db.Database.Transactions.CommitAsync(txn);
    }

    private static async Task Analyze(Db db, string table)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await db.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: "ANALYZE " + table, parameters: null));
        await foreach (QueryResultRow _ in cursor) { }
        await db.Database.Transactions.CommitAsync(txn);
    }
}
