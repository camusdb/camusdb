/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// An item of an IN list may name a column (<c>WHERE a0.c1 IN (a0.c0, 5)</c>): the predicate is
/// <c>a0.c1 = a0.c0 OR a0.c1 = 5</c>, with the three-valued rules of IN. The list was evaluated as
/// a list of constants, with no row to read a column from, so such an item failed with
/// <see cref="CamusDBErrorCodes.UnknownColumn"/>; and the walkers that collect the columns a
/// predicate reads visited the tested value only, so the item's column was neither decoded nor
/// counted when a join decides which side a conjunct belongs to. These tests drive the forms through
/// SQL: SELECT, UPDATE, DELETE, a projection, both sides of a join, a long list, an indexed tested
/// column, a parameter next to a column, and a CHECK constraint.
/// </summary>
public sealed class TestInListColumnItems : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Ddl(database, executor, dbname, "CREATE TABLE t0 (c0 int64 PRIMARY KEY, c1 int64, c2 string, c3 int64)");
        await Ddl(database, executor, dbname, "CREATE TABLE t1 (c0 int64 PRIMARY KEY, c1 int64)");

        // c1 = c0 in rows 1, 2 and 4; c1 = c3 in row 2; c0 = c3 in rows 2, 3 and 7; c1 is NULL in row 3; c3 is NULL in row 4.
        await Write(database, executor, dbname, "INSERT INTO t0 (c0, c1, c2, c3) VALUES (1, 1, 'a', 9), (2, 2, 'b', 2), (3, NULL, 'c', 3), (4, 4, 'd', NULL), (7, 8, 'e', 7)");
        await Write(database, executor, dbname, "INSERT INTO t1 (c0, c1) VALUES (1, 1), (2, 7), (3, 3), (4, 0)");

        return (dbname, database, executor);
    }

    private static async Task Ddl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task Write(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
            await database.Transactions.CommitAsync(tx);
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    /// <summary>The rows of <paramref name="sql"/> as "a|b" text ("NULL" for a NULL), sorted, so a result compares without an ORDER BY.</summary>
    private static async Task<List<string>> Rows(
        DatabaseDescriptor database,
        CommandExecutor executor,
        string dbname,
        string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: parameters));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows.Select(r => string.Join("|", r.Row.Values.Select(Text))).OrderBy(x => x, System.StringComparer.Ordinal).ToList();
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    /// <summary>The plan of <paramref name="sql"/>, one EXPLAIN line per text line.</summary>
    private static async Task<string> Explain(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        List<string> lines = await Rows(database, executor, dbname, "EXPLAIN " + sql);
        return string.Join("\n", lines);
    }

    private static string Text(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => "NULL",
        ColumnType.Bool => value.BoolValue ? "true" : "false",
        ColumnType.Integer64 => value.LongValue.ToString(),
        ColumnType.String => value.StrValue ?? "",
        _ => value.ToString() ?? "",
    };

    [Test]
    public async Task ColumnItem_MatchesTheRowsWhereTheColumnsAreEqual()
    {
        // The query of the fuzzer finding.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<string> rows = await Rows(database, executor, dbname, "SELECT a0.c0 FROM t0 AS a0 WHERE a0.c1 IN (a0.c0)");

        Assert.That(rows, Is.EqualTo(new List<string> { "1", "2", "4" }));
    }

    [Test]
    public async Task ColumnItemTheQueryDoesNotProject_IsDecoded()
    {
        // c3 is read by the list item only, so the scan must decode it for the predicate.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<string> rows = await Rows(database, executor, dbname, "SELECT a0.c0 FROM t0 AS a0 WHERE a0.c1 IN (a0.c3)");

        Assert.That(rows, Is.EqualTo(new List<string> { "2" }));
    }

    [Test]
    public async Task ColumnItemsMixedWithLiteralsAndNull_FollowThreeValuedLogic()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        // A match is TRUE whatever else the list holds; no match plus a NULL item is UNKNOWN.
        Assert.That(await Rows(database, executor, dbname, "SELECT a0.c0 FROM t0 AS a0 WHERE a0.c1 IN (a0.c3, 8, NULL)"), Is.EqualTo(new List<string> { "2", "7" }));

        // NOT IN is TRUE only when every item differs; a NULL column item (row 4) makes it UNKNOWN.
        Assert.That(await Rows(database, executor, dbname, "SELECT a0.c0 FROM t0 AS a0 WHERE a0.c1 NOT IN (a0.c3, 8)"), Is.EqualTo(new List<string> { "1" }));
        Assert.That(await Rows(database, executor, dbname, "SELECT a0.c0 FROM t0 AS a0 WHERE a0.c1 NOT IN (a0.c3, NULL)"), Is.Empty);
    }

    [Test]
    public async Task UpdateWithAColumnItem_WritesTheMatchingRowsOnly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        // The statement of the fuzzer finding: c1 NOT IN (c1) is FALSE or UNKNOWN for every row.
        await Write(database, executor, dbname, "UPDATE t0 SET c2 = 'x' WHERE c1 NOT IN (c1)");
        Assert.That(await Rows(database, executor, dbname, "SELECT c0 FROM t0 WHERE c2 = 'x'"), Is.Empty);

        await Write(database, executor, dbname, "UPDATE t0 SET c2 = 'y' WHERE c0 IN (c3, 1)");
        Assert.That(await Rows(database, executor, dbname, "SELECT c0 FROM t0 WHERE c2 = 'y'"), Is.EqualTo(new List<string> { "1", "2", "3", "7" }));
    }

    [Test]
    public async Task DeleteWithAColumnItem_RemovesTheMatchingRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        await Write(database, executor, dbname, "DELETE FROM t0 WHERE c0 IN (c1)");

        Assert.That(await Rows(database, executor, dbname, "SELECT c0 FROM t0"), Is.EqualTo(new List<string> { "3", "7" }));
    }

    [Test]
    public async Task ColumnItemFromTheOtherSideOfAJoin_IsEvaluatedAfterTheJoin()
    {
        // The conjunct reads both aliases, so it must stay above the join: pushed to one scan, the
        // other side's column is missing.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        Assert.That(
            await Rows(database, executor, dbname, "SELECT a0.c0, a1.c1 FROM t0 AS a0 INNER JOIN t1 AS a1 ON a0.c0 = a1.c0 WHERE a0.c1 IN (a1.c1, 2)"),
            Is.EqualTo(new List<string> { "1|1", "2|7" }));

        // On the null-extended side the padded item is NULL, so the unmatched left row (7) is UNKNOWN and dropped.
        Assert.That(
            await Rows(database, executor, dbname, "SELECT a0.c0 FROM t0 AS a0 LEFT JOIN t1 AS a1 ON a0.c0 = a1.c0 WHERE a0.c1 NOT IN (a1.c1)"),
            Is.EqualTo(new List<string> { "2", "4" }));
    }

    [Test]
    public async Task ColumnItemInAProjection_GivesTheThreeValuedResult()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<string> rows = await Rows(database, executor, dbname, "SELECT a0.c0, a0.c1 IN (a0.c3, 8) AS m FROM t0 AS a0");

        Assert.That(rows, Is.EqualTo(new List<string> { "1|false", "2|true", "3|NULL", "4|NULL", "7|true" }));
    }

    [Test]
    public async Task LongListWithAColumnItem_KeepsThePerRowScan()
    {
        // A list of more than eight constants is hashed once per statement; one column item keeps
        // the per-row scan. The second run reaches the memo's classification of the node.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();
        const string sql = "SELECT a0.c0 FROM t0 AS a0 WHERE a0.c1 IN (a0.c3, 10, 11, 12, 13, 14, 15, 16, 17, 18)";

        Assert.That(await Rows(database, executor, dbname, sql), Is.EqualTo(new List<string> { "2" }));
        Assert.That(await Rows(database, executor, dbname, sql), Is.EqualTo(new List<string> { "2" }));
    }

    [Test]
    public async Task PrimaryKeyWithAColumnItem_IsNotSeeked()
    {
        // A column item has a value per row, so the planner cannot seek the index on the tested column.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();
        const string sql = "SELECT a0.c0 FROM t0 AS a0 WHERE a0.c0 IN (a0.c1)";

        Assert.That(await Explain(database, executor, dbname, sql), Does.Not.Contain("in-list"));
        Assert.That(await Rows(database, executor, dbname, sql), Is.EqualTo(new List<string> { "1", "2", "4" }));
    }

    [Test]
    public async Task ParameterAndColumnItems_Together()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<string> rows = await Rows(
            database, executor, dbname,
            "SELECT a0.c0 FROM t0 AS a0 WHERE a0.c1 IN (@p, a0.c3)",
            new Dictionary<string, ColumnValue> { { "@p", new ColumnValue(ColumnType.Integer64, 8) } });

        Assert.That(rows, Is.EqualTo(new List<string> { "2", "7" }));
    }

    [Test]
    public async Task ColumnItemOfAnIncomparableType_IsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => Rows(database, executor, dbname, "SELECT a0.c0 FROM t0 AS a0 WHERE a0.c1 IN (a0.c2)"));

        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
    }

    [Test]
    public async Task CheckConstraintWithAColumnItem_UsesTheRowValues()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        await Ddl(database, executor, dbname, "CREATE TABLE t2 (c0 int64 PRIMARY KEY, c1 int64, c2 int64, CHECK (c1 IN (c2, 0)))");

        await Write(database, executor, dbname, "INSERT INTO t2 (c0, c1, c2) VALUES (1, 5, 5), (2, 0, 9), (3, NULL, 1)");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => Write(database, executor, dbname, "INSERT INTO t2 (c0, c1, c2) VALUES (4, 1, 2)"));
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.CheckConstraintViolation));

        // A column the INSERT leaves out is NULL, so the membership is UNKNOWN and the row passes.
        await Write(database, executor, dbname, "INSERT INTO t2 (c0, c1) VALUES (5, 3)");

        Assert.That(await Rows(database, executor, dbname, "SELECT c0 FROM t2"), Is.EqualTo(new List<string> { "1", "2", "3", "5" }));
    }
}
