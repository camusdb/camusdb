
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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end SQL three-valued logic for <c>IN</c> and <c>NOT IN</c> with a NULL on either side.
///
/// <para><c>x IN (a, b)</c> is <c>x = a OR x = b</c>, so a NULL <c>x</c> over a non-empty list is
/// UNKNOWN, and no match over a list that holds a NULL is UNKNOWN too. A WHERE clause drops an
/// UNKNOWN row for both IN and NOT IN, and <c>NOT UNKNOWN</c> stays UNKNOWN. A CHECK passes on
/// UNKNOWN and fails only on FALSE. Only an empty (subquery) list is definite for a NULL <c>x</c>:
/// IN is FALSE and NOT IN is TRUE.</para>
///
/// <para>Each test compares the rows the engine returns with a small in-test oracle of those rules,
/// over every row value (including the NULL row) and every list shape. Values are read as nullable,
/// because a reader that maps a NULL cell to 0 hides exactly this class of bug.</para>
///
/// <para>The paths covered: the literal-list evaluator, the prepared IN set that a top-level
/// <c>column IN (…)</c> conjunct uses, a subquery list materialized into a literal node, the anti
/// join that a top-level <c>NOT IN (SELECT …)</c> over an indexed column uses, DML, projections,
/// and CHECK constraints.</para>
/// </summary>
[NonParallelizable]
public sealed class TestInMembershipNullSemantics : BaseTest
{
    /// <summary>A list shape: its SQL text and the values it holds (null is SQL NULL).</summary>
    private sealed record ListCase(string Sql, long?[] Values)
    {
        public override string ToString() => Sql;
    }

    private static readonly ListCase[] LiteralLists =
    [
        new("(5)", [5]),
        new("(5, 6, 7)", [5, 6, 7]),
        new("(1000000, 5)", [1_000_000, 5]),
        new("(5, NULL)", [5, null]),
        new("(NULL, 1000000)", [null, 1_000_000]),
        new("(NULL)", [null]),
        // Past the prepared set's hash threshold, so the hash path is exercised as well.
        new("(100, 101, 102, 103, 104, 105, 106, 107, 108, 5, NULL)", [100, 101, 102, 103, 104, 105, 106, 107, 108, 5, null]),
    ];

    /// <summary>
    /// The predicate forms under test. <c>{0}</c> is the tested operand and <c>{1}</c> the list.
    /// </summary>
    private static readonly (string Sql, Func<bool?, bool?> FromIn)[] Forms =
    [
        ("{0} IN {1}", r => r),
        ("{0} NOT IN {1}", Not),
        ("NOT ({0} IN {1})", Not),
        ("NOT ({0} NOT IN {1})", r => r),
        ("({0} IN {1}) IS NULL", r => r is null),
        ("({0} NOT IN {1}) IS NULL", r => r is null),
        // A top-level column IN conjunct next to another conjunct takes the prepared-set path.
        ("{0} IN {1} AND id >= 0", r => r),
        ("{0} NOT IN {1} AND id >= 0", Not),
    ];

    [Test]
    public async Task LiteralList_Where_MatchesThreeValuedOracle()
    {
        (string db, CommandExecutor executor) = await CreateSeededTable();

        foreach (ListCase list in LiteralLists)
        {
            foreach ((string form, Func<bool?, bool?> fromIn) in Forms)
            {
                string where = string.Format(form, "v", list.Sql);

                CollectionAssert.AreEquivalent(
                    Expected(list.Values, fromIn),
                    await SelectValues(executor, db, where),
                    $"WHERE {where}");
            }
        }
    }

    [Test]
    public async Task NotInLiteral_ReproductionFromReport_ExcludesNullRow()
    {
        (string db, CommandExecutor executor) = await CreateSeededTable();

        Assert.AreEqual(19, (await SelectValues(executor, db, "v <> 5")).Count);
        Assert.AreEqual(19, (await SelectValues(executor, db, "v NOT IN (5)")).Count);
        Assert.AreEqual(17, (await SelectValues(executor, db, "v NOT IN (5, 6, 7)")).Count);
        Assert.AreEqual(19, (await SelectValues(executor, db, "NOT (v IN (1000000, 5))")).Count);

        CollectionAssert.DoesNotContain(await SelectValues(executor, db, "v NOT IN (5)"), null);
    }

    [Test]
    public async Task LiteralList_NullOperandLiteral_IsUnknown()
    {
        (string db, CommandExecutor executor) = await CreateSeededTable();

        Assert.IsEmpty(await SelectValues(executor, db, "NULL IN (5)"));
        Assert.IsEmpty(await SelectValues(executor, db, "NULL NOT IN (5)"));
        Assert.IsEmpty(await SelectValues(executor, db, "NOT (NULL IN (5))"));
        Assert.AreEqual(21, (await SelectValues(executor, db, "(NULL NOT IN (5)) IS NULL")).Count);
    }

    [Test]
    public async Task LiteralList_Projection_ReturnsNullForUnknown()
    {
        (string db, CommandExecutor executor) = await CreateSeededTable();

        List<IReadOnlyDictionary<string, ColumnValue>> rows = await Query(
            executor, db, "SELECT v, v IN (5, NULL) AS a, v NOT IN (5) AS b FROM t");

        Assert.AreEqual(21, rows.Count);

        foreach (IReadOnlyDictionary<string, ColumnValue> row in rows)
        {
            long? v = Nullable(row["v"]);
            string label = $"v = {(v is null ? "NULL" : v.ToString())}";

            Assert.AreEqual(Truth(InOracle(v, [5, null])), Truth(row["a"]), label);
            Assert.AreEqual(Truth(Not(InOracle(v, [5]))), Truth(row["b"]), label);
        }
    }

    [Test]
    public async Task SubqueryList_Materialized_MatchesThreeValuedOracle()
    {
        (string db, CommandExecutor executor) = await CreateSeededTable();
        Dictionary<string, long?[]> lists = await CreateListTables(executor, db, indexed: false);

        await AssertSubqueryForms(executor, db, lists);
    }

    [Test]
    public async Task SubqueryList_IndexedInnerColumn_MatchesThreeValuedOracle()
    {
        // With an index on the inner column, a top-level IN / NOT IN (SELECT …) becomes a semi or
        // anti join; under NOT or IS NULL it still materializes. Both must agree with the oracle.
        (string db, CommandExecutor executor) = await CreateSeededTable();
        Dictionary<string, long?[]> lists = await CreateListTables(executor, db, indexed: true);

        await AssertSubqueryForms(executor, db, lists);
    }

    [Test]
    public async Task LiteralList_DeleteAndUpdate_SkipUnknownRows()
    {
        (string db, CommandExecutor executor) = await CreateSeededTable();

        Assert.AreEqual(19, await ExecuteNonQuery(executor, db, "UPDATE t SET name = 'changed' WHERE v NOT IN (5)"));
        CollectionAssert.AreEquivalent(
            Enumerable.Range(0, 20).Where(i => i != 5).Select(i => (long?)i).ToList(),
            await SelectValues(executor, db, "name = 'changed'"));

        Assert.AreEqual(0, await ExecuteNonQuery(executor, db, "DELETE FROM t WHERE NOT (v IN (1000000, NULL))"));
        Assert.AreEqual(19, await ExecuteNonQuery(executor, db, "DELETE FROM t WHERE NOT (v IN (1000000, 5))"));

        CollectionAssert.AreEquivalent(new long?[] { 5, null }, await SelectValues(executor, db, "id >= 0"));
    }

    [Test]
    public async Task Check_NotInListWithNull_AcceptsUnknownRejectsFalse()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, db, "CREATE TABLE c (id int64 PRIMARY KEY NOT NULL, v int64 NULL CHECK (v NOT IN (1, NULL)))");

        // 2 NOT IN (1, NULL) is UNKNOWN, so the CHECK passes.
        Assert.AreEqual(1, await ExecuteNonQuery(executor, db, "INSERT INTO c (id, v) VALUES (1, 2)"));
        Assert.AreEqual(1, await ExecuteNonQuery(executor, db, "INSERT INTO c (id, v) VALUES (2, NULL)"));

        // 1 NOT IN (1, NULL) is FALSE, so the CHECK fails.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            () => ExecuteNonQuery(executor, db, "INSERT INTO c (id, v) VALUES (3, 1)"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code);
    }

    [Test]
    public async Task Check_InAndNegatedIn_FollowThreeValuedLogic()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, db,
            "CREATE TABLE c (id int64 PRIMARY KEY NOT NULL, a int64 NULL CHECK (a IN (1, NULL)), b int64 NULL CHECK (NOT (b IN (1, 2))))");

        // a: 7 IN (1, NULL) is UNKNOWN (passes). b: NOT (NULL IN (1, 2)) is UNKNOWN (passes).
        Assert.AreEqual(1, await ExecuteNonQuery(executor, db, "INSERT INTO c (id, a, b) VALUES (1, 7, NULL)"));
        Assert.AreEqual(1, await ExecuteNonQuery(executor, db, "INSERT INTO c (id, a, b) VALUES (2, 1, 3)"));

        // b: NOT (2 IN (1, 2)) is FALSE.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            () => ExecuteNonQuery(executor, db, "INSERT INTO c (id, a, b) VALUES (3, 1, 2)"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code);
    }

    // ── oracle ────────────────────────────────────────────────────────────────

    /// <summary>
    /// SQL <c>x IN (list)</c>: FALSE for an empty list; UNKNOWN for a NULL x; TRUE on a match;
    /// otherwise UNKNOWN if the list holds a NULL, else FALSE.
    /// </summary>
    private static bool? InOracle(long? x, long?[] list)
    {
        if (list.Length == 0)
            return false;

        if (x is null)
            return null;

        if (list.Any(item => item == x))
            return true;

        return list.Any(item => item is null) ? null : false;
    }

    private static bool? Not(bool? value) => value is null ? null : !value.Value;

    /// <summary>The v values of the rows a WHERE keeps: only those whose predicate is TRUE.</summary>
    private static List<long?> Expected(long?[] list, Func<bool?, bool?> fromIn) =>
        SeededValues().Where(v => fromIn(InOracle(v, list)) == true).ToList();

    private static IEnumerable<long?> SeededValues() =>
        Enumerable.Range(0, 20).Select(i => (long?)i).Append(null);

    private static string Truth(bool? value) => value is null ? "UNKNOWN" : value.Value ? "TRUE" : "FALSE";

    private static string Truth(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => "UNKNOWN",
        ColumnType.Bool => value.BoolValue ? "TRUE" : "FALSE",
        _ => $"unexpected {value.Type}"
    };

    private static long? Nullable(ColumnValue value) => value.Type == ColumnType.Null ? null : value.LongValue;

    // ── fixtures ──────────────────────────────────────────────────────────────

    private static async Task AssertSubqueryForms(CommandExecutor executor, string db, Dictionary<string, long?[]> lists)
    {
        foreach ((string table, long?[] values) in lists)
        {
            string subquery = $"(SELECT w FROM {table})";

            foreach ((string form, Func<bool?, bool?> fromIn) in Forms)
            {
                string where = string.Format(form, "v", subquery);

                CollectionAssert.AreEquivalent(
                    Expected(values, fromIn),
                    await SelectValues(executor, db, where),
                    $"WHERE {where} over {{{string.Join(", ", values.Select(x => x?.ToString() ?? "NULL"))}}}");
            }
        }
    }

    /// <summary>
    /// Creates one single-column table per subquery list shape, including an empty one and one that
    /// holds only NULL, and returns each table name with the values it holds.
    /// </summary>
    private static async Task<Dictionary<string, long?[]>> CreateListTables(CommandExecutor executor, string db, bool indexed)
    {
        Dictionary<string, long?[]> lists = new()
        {
            ["l_plain"] = [5, 6],
            ["l_nomatch"] = [1_000_000],
            ["l_withnull"] = [5, null],
            ["l_nomatchnull"] = [1_000_000, null],
            ["l_onlynull"] = [null],
            ["l_empty"] = [],
        };

        foreach ((string table, long?[] values) in lists)
        {
            await ExecuteDdl(executor, db, $"CREATE TABLE {table} (id int64 PRIMARY KEY NOT NULL, w int64 NULL)");

            if (indexed)
                await ExecuteDdl(executor, db, $"CREATE INDEX {table}_w ON {table} (w)");

            for (int i = 0; i < values.Length; i++)
            {
                string value = values[i]?.ToString() ?? "NULL";
                await ExecuteNonQuery(executor, db, $"INSERT INTO {table} (id, w) VALUES ({i}, {value})");
            }
        }

        return lists;
    }

    private async Task<(string Db, CommandExecutor Executor)> CreateSeededTable()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, db, "CREATE TABLE t (id int64 PRIMARY KEY NOT NULL, v int64 NULL, name string NULL)");

        List<string> rows = [];
        for (int i = 0; i < 20; i++)
            rows.Add($"({i}, {i}, 'row')");
        rows.Add("(20, NULL, 'row')");

        await ExecuteNonQuery(executor, db, "INSERT INTO t (id, v, name) VALUES " + string.Join(", ", rows));

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

    private static async Task<List<long?>> SelectValues(CommandExecutor executor, string db, string where) =>
        (await Query(executor, db, $"SELECT v FROM t WHERE {where}")).Select(r => Nullable(r["v"])).ToList();
}
