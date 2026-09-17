/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end coverage for statements whose parse tree is very deep: long OR and AND chains, long
/// IN lists, a subquery that returns many values, and deeply nested arithmetic and function calls.
///
/// <para>Before the parser rebalanced chains and bounded depth, the long chains, the long lists and
/// the deep arithmetic and function nesting below each ended the test host with a stack overflow,
/// which no assertion can observe. So these tests
/// run the real SQL entry points, against a table that holds rows so the per-row evaluators run, and
/// assert the rows — a pass means the process survived <em>and</em> the answer is right.</para>
/// </summary>
[NonParallelizable]
public sealed class TestDeepExpressionEvaluation : BaseTest
{
    /// <summary>Far past the measured overflow of the old left-deep evaluators (about 6,500 terms).</summary>
    private const int LongChain = 20_000;

    /// <summary>Values that no row holds, so filler terms never match.</summary>
    private const int Filler = 1_000_000;

    [Test]
    public async Task LongOrChain_Select_ReturnsMatchingRows()
    {
        (string db, CommandExecutor executor) = await CreateSeededTable();

        string where = Chain(LongChain, " OR ", i => $"v = {Filler + i}") + " OR v = 3 OR v = 7";

        CollectionAssert.AreEquivalent(new long[] { 3, 7 }, await SelectValues(executor, db, where));
    }

    [Test]
    public async Task LongAndChain_Select_ReturnsMatchingRows()
    {
        (string db, CommandExecutor executor) = await CreateSeededTable();

        string where = Chain(LongChain, " AND ", i => $"v <> {Filler + i}") + " AND v >= 15";

        // The NULL row fails `v >= 15` as UNKNOWN and stays excluded.
        CollectionAssert.AreEquivalent(new long[] { 15, 16, 17, 18, 19 }, await SelectValues(executor, db, where));
    }

    [Test]
    public async Task LongOrChain_UpdateAndDelete_TouchOnlyMatchingRows()
    {
        (string db, CommandExecutor executor) = await CreateSeededTable();

        string where = Chain(LongChain, " OR ", i => $"v = {Filler + i}") + " OR v = 1 OR v = 2";

        Assert.AreEqual(2, await ExecuteNonQuery(executor, db, $"UPDATE t SET name = 'changed' WHERE {where}"));

        List<IReadOnlyDictionary<string, ColumnValue>> changed = await Query(executor, db, "SELECT v FROM t WHERE name = 'changed'");
        CollectionAssert.AreEquivalent(new long[] { 1, 2 }, changed.Select(r => r["v"].LongValue).ToList());

        Assert.AreEqual(2, await ExecuteNonQuery(executor, db, $"DELETE FROM t WHERE {where}"));
        Assert.AreEqual(19, (await Query(executor, db, "SELECT id FROM t")).Count);
    }

    [Test]
    public async Task LongInList_Select_ReturnsMatchingRows()
    {
        (string db, CommandExecutor executor) = await CreateSeededTable();

        string where = "v IN (" + Chain(LongChain, ", ", i => (Filler + i).ToString()) + ", 4, 9)";

        CollectionAssert.AreEquivalent(new long[] { 4, 9 }, await SelectValues(executor, db, where));
    }

    [Test]
    public async Task LongNotInList_ExcludesListedValues()
    {
        (string db, CommandExecutor executor) = await CreateSeededTable();

        // Only non-NULL rows are asserted here; how NOT IN treats a NULL operand is independent of the
        // length or shape of the list.
        string where = "v NOT IN (" + Chain(LongChain, ", ", i => (Filler + i).ToString()) + ", 0)";

        CollectionAssert.AreEquivalent(
            Enumerable.Range(1, 19).Select(i => (long?)i).ToList(),
            (await SelectNullableValues(executor, db, where)).Where(v => v is not null).ToList());
    }

    [Test]
    public async Task LongOrChain_UnderNot_KeepsThreeValuedLogic()
    {
        (string db, CommandExecutor executor) = await CreateSeededTable();

        // Long enough to be rebalanced. For the NULL row every term is UNKNOWN, so the chain is
        // UNKNOWN and NOT of it is UNKNOWN: the row must stay excluded, exactly as a short chain does.
        string longChain = Chain(ExpressionChains.BalanceThreshold * 4, " OR ", i => $"v = {i + 5}");
        string shortChain = Chain(3, " OR ", i => $"v = {i + 5}");

        List<long?> expectedShort = await SelectNullableValues(executor, db, $"NOT ({shortChain})");
        Assert.AreEqual(17, expectedShort.Count, "sanity: the short form excludes 5, 6, 7 and the NULL row");

        List<long?> actualLong = await SelectNullableValues(executor, db, $"NOT ({longChain})");
        CollectionAssert.AreEquivalent(Enumerable.Range(0, 5).Select(i => (long?)i).ToList(), actualLong);

        // With a term that is TRUE for the NULL row, the chain is TRUE and NOT excludes it; the
        // chain with `v IS NULL` alone selects only that row.
        List<long?> withIsNull = await SelectNullableValues(executor, db, $"{longChain} OR v IS NULL");
        Assert.IsTrue(withIsNull.Contains(null));
        Assert.AreEqual(15 + 1, withIsNull.Count);
    }

    [Test]
    public async Task InSubquery_ReturningManyValues_InDelete()
    {
        (string db, CommandExecutor executor) = await CreateSeededTable();

        await ExecuteDdl(executor, db, "CREATE TABLE big (id int64 PRIMARY KEY NOT NULL, w int64 NULL)");

        // Two statements keep each insert under the per-transaction mutation budget.
        const int Values = 12_000;
        for (int start = 0; start < Values; start += 6_000)
        {
            StringBuilder insert = new("INSERT INTO big (id, w) VALUES ");
            for (int i = start; i < start + 6_000; i++)
            {
                if (i > start)
                    insert.Append(", ");
                insert.Append('(').Append(i).Append(", ").Append(i + 10).Append(')');
            }

            await ExecuteNonQuery(executor, db, insert.ToString());
        }

        // Values 10..11,009 exist in big, so rows 10..19 of t go and 0..9 and the NULL row stay.
        Assert.AreEqual(10, await ExecuteNonQuery(executor, db, "DELETE FROM t WHERE v IN (SELECT w FROM big)"));

        List<long?> remaining = await SelectNullableValues(executor, db, "id >= 0");
        CollectionAssert.AreEquivalent(
            Enumerable.Range(0, 10).Select(i => (long?)i).Append(null).ToList(),
            remaining);
    }

    [Test]
    public async Task DeepArithmetic_JustBelowTheLimit_Evaluates()
    {
        (string db, CommandExecutor executor) = await CreateSeededTable();

        // Arithmetic on NULL is an error rather than UNKNOWN, so the NULL row goes first.
        await ExecuteNonQuery(executor, db, "DELETE FROM t WHERE id = 20");

        // Well past the old evaluator's overflow (about 875 terms), inside the depth limit.
        int terms = StatementDepthGuard.MaxDepth - 10;
        string where = "v" + Repeat(" + 1", terms) + $" = {5 + terms}";

        CollectionAssert.AreEquivalent(new long[] { 5 }, await SelectValues(executor, db, where));
    }

    [Test]
    public async Task DeepFunctionNesting_JustBelowTheLimit_Evaluates()
    {
        (string db, CommandExecutor executor) = await CreateSeededTable();

        // Arithmetic on NULL is an error rather than UNKNOWN, so the NULL row goes first.
        await ExecuteNonQuery(executor, db, "DELETE FROM t WHERE id = 20");

        // Past the old evaluator's overflow (about 718 levels), inside the depth limit.
        int levels = StatementDepthGuard.MaxDepth - 10;
        string where = Repeat("abs(", levels) + "v - 30" + Repeat(")", levels) + " = 12";

        CollectionAssert.AreEquivalent(new long[] { 18 }, await SelectValues(executor, db, where));
    }

    [Test]
    public async Task TooDeepStatement_IsRefused_AndTheNodeKeepsServing()
    {
        (string db, CommandExecutor executor) = await CreateSeededTable();

        string where = "v" + Repeat(" + 1", 20_000) + " > 0";

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () => await SelectValues(executor, db, where));
        Assert.AreEqual(CamusDBErrorCodes.StatementTooDeeplyNested, ex!.Code);

        CamusDBException? dml = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteNonQuery(executor, db, $"DELETE FROM t WHERE {where}"));
        Assert.AreEqual(CamusDBErrorCodes.StatementTooDeeplyNested, dml!.Code);

        Assert.AreEqual(21, (await Query(executor, db, "SELECT id FROM t")).Count, "nothing was deleted");
    }

    [Test]
    public async Task ManyRowInsert_IsNotRefusedByTheDepthLimit()
    {
        (string db, CommandExecutor executor) = await CreateSeededTable();

        int rows = StatementDepthGuard.MaxDepth * 2;

        StringBuilder insert = new("INSERT INTO t (id, v, name) VALUES ");
        for (int i = 0; i < rows; i++)
        {
            if (i > 0)
                insert.Append(", ");
            insert.Append('(').Append(1_000 + i).Append(", ").Append(Filler + i).Append(", 'bulk')");
        }

        Assert.AreEqual(rows, await ExecuteNonQuery(executor, db, insert.ToString()));
        Assert.AreEqual(rows, (await Query(executor, db, "SELECT id FROM t WHERE name = 'bulk'")).Count);
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Creates <c>t</c> with rows <c>v = 0 … 19</c> plus one row whose <c>v</c> is NULL (id 20), so
    /// every predicate is evaluated against real rows and against a NULL.
    /// </summary>
    private async Task<(string Db, CommandExecutor Executor)> CreateSeededTable()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, db, "CREATE TABLE t (id int64 PRIMARY KEY NOT NULL, v int64 NULL, name string NULL)");

        StringBuilder insert = new("INSERT INTO t (id, v, name) VALUES ");
        for (int i = 0; i < 20; i++)
            insert.Append('(').Append(i).Append(", ").Append(i).Append(", 'row'), ");
        insert.Append("(20, NULL, 'row')");

        await ExecuteNonQuery(executor, db, insert.ToString());

        return (db, executor);
    }

    private static async Task ExecuteDdl(CommandExecutor executor, string db, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<int> ExecuteNonQuery(CommandExecutor executor, string db, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            await database.Transactions.CommitAsync(tx);
            return result.ModifiedRows;
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    private static async Task<List<IReadOnlyDictionary<string, ColumnValue>>> Query(CommandExecutor executor, string db, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> rows) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));

            List<IReadOnlyDictionary<string, ColumnValue>> result = [];
            await foreach (QueryResultRow row in rows)
                result.Add(row.Row);

            await database.Transactions.CommitAsync(tx);
            return result;
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    private static async Task<List<long>> SelectValues(CommandExecutor executor, string db, string where) =>
        (await Query(executor, db, $"SELECT v FROM t WHERE {where}")).Select(r => r["v"].LongValue).ToList();

    private static async Task<List<long?>> SelectNullableValues(CommandExecutor executor, string db, string where) =>
        (await Query(executor, db, $"SELECT v FROM t WHERE {where}"))
            .Select(r => r["v"].Type == ColumnType.Null ? (long?)null : r["v"].LongValue)
            .ToList();

    private static string Chain(int count, string separator, Func<int, string> term) =>
        string.Join(separator, Enumerable.Range(0, count).Select(term));

    private static string Repeat(string text, int count) =>
        new StringBuilder(text.Length * count).Insert(0, text, count).ToString();
}
