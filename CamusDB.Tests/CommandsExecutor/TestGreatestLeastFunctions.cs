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
/// End-to-end coverage for <c>GREATEST(a, b, …)</c> and <c>LEAST(a, b, …)</c> with the PostgreSQL rules: a NULL
/// argument is left out and the result is NULL only when every argument is NULL; the result has the common
/// type of the arguments (the CASE / COALESCE rule), also in a row where the argument of the wider type is
/// NULL; the comparison is the one of <c>&lt;</c> and <c>&gt;</c>, with NaN above +Infinity; and a pair of
/// types with no common type fails when the statement is bound, over an empty table too.
/// </summary>
[NonParallelizable]
public sealed class TestGreatestLeastFunctions : SharedNodeBaseTest
{
    private static async Task ExecDdl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task ExecNonQuery(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        _ = await executor.ExecuteNonSQLQuery(new(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> ExecQuery(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<ColumnValue> ExecScalar(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, sql);
        Assert.AreEqual(1, rows.Count, sql);
        return rows[0].Row["0"];
    }

    /// <summary>Four rows of an INT64 and a FLOAT64 column, with each NULL pattern.</summary>
    private static async Task SeedPairs(DatabaseDescriptor database, CommandExecutor executor, string dbname)
    {
        await ExecDdl(database, executor, dbname, "CREATE TABLE pairs (id int64 PRIMARY KEY, a int64, b float64, s string)");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO pairs (id, a, b, s) VALUES (1, 5, 2.5, 'pear'), (2, NULL, 7.5, 'apple'), (3, 4, NULL, NULL), (4, NULL, NULL, 'fig')");
    }

    [TestCase("SELECT GREATEST(1, 3, 2)", ColumnType.Integer64, "3")]
    [TestCase("SELECT LEAST(1, 3, 2)", ColumnType.Integer64, "1")]
    [TestCase("SELECT greatest(7)", ColumnType.Integer64, "7")]
    [TestCase("SELECT GREATEST(3, 2.5)", ColumnType.Float64, "3")]
    [TestCase("SELECT LEAST(3, 2.5)", ColumnType.Float64, "2.5")]
    [TestCase("SELECT GREATEST(NULL, 2, NULL, 1)", ColumnType.Integer64, "2")]
    [TestCase("SELECT LEAST(NULL, 2, NULL, 1)", ColumnType.Integer64, "1")]
    [TestCase("SELECT GREATEST('apple', 'banana', 'Cherry')", ColumnType.String, "banana")]
    [TestCase("SELECT LEAST('apple', 'banana', 'Cherry')", ColumnType.String, "Cherry")]
    [TestCase("SELECT GREATEST(true, false)", ColumnType.Bool, "True")]
    [TestCase("SELECT LEAST(true, false)", ColumnType.Bool, "False")]
    [TestCase("SELECT GREATEST(-1, -1.0)", ColumnType.Float64, "-1")]
    public async Task TheExtremeValueHasTheCommonType(string sql, ColumnType type, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        ColumnValue value = await ExecScalar(database, executor, dbname, sql);

        Assert.AreEqual(type, value.Type, sql);
        string text = value.Type switch
        {
            ColumnType.Integer64 => value.LongValue.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ColumnType.Float64 => value.FloatValue.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ColumnType.Bool => value.BoolValue.ToString(),
            _ => value.StrValue!,
        };
        Assert.AreEqual(expected, text, sql);
    }

    [TestCase("SELECT GREATEST(NULL, NULL)")]
    [TestCase("SELECT LEAST(NULL)")]
    public async Task EveryArgumentNullGivesNull(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        Assert.AreEqual(ColumnType.Null, (await ExecScalar(database, executor, dbname, sql)).Type, sql);
    }

    [Test]
    public async Task NaNIsAboveInfinityAndADateTakesAStringLiteral()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        ColumnValue greatest = await ExecScalar(database, executor, dbname,
            "SELECT GREATEST(CAST('NaN' AS float64), CAST('Infinity' AS float64), 1.0)");
        Assert.That(double.IsNaN(greatest.FloatValue), Is.True);

        ColumnValue least = await ExecScalar(database, executor, dbname,
            "SELECT LEAST(CAST('NaN' AS float64), CAST('Infinity' AS float64))");
        Assert.That(double.IsPositiveInfinity(least.FloatValue), Is.True);

        ColumnValue date = await ExecScalar(database, executor, dbname,
            "SELECT GREATEST(CAST('2026-01-02' AS date), '2026-03-01')");
        Assert.AreEqual(ColumnType.Date, date.Type);
        Assert.AreEqual(
            (await ExecScalar(database, executor, dbname, "SELECT CAST('2026-03-01' AS date)")).ToString(),
            date.ToString());
    }

    [Test]
    public async Task ColumnsGiveTheWiderTypeAlsoWhenTheWiderArgumentIsNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedPairs(database, executor, dbname);

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT id, GREATEST(a, b) AS g, LEAST(a, b) AS l FROM pairs ORDER BY id");

        string[] Render(string column) => rows.Select(r =>
            r.Row[column].Type == ColumnType.Null ? "NULL" : $"{r.Row[column].Type}:{r.Row[column].FloatValue.ToString(System.Globalization.CultureInfo.InvariantCulture)}").ToArray();

        // Row 3 has only the INT64 4, which is still the FLOAT64 4: the column type does not depend on the row.
        CollectionAssert.AreEqual(new[] { "Float64:5", "Float64:7.5", "Float64:4", "NULL" }, Render("g"));
        CollectionAssert.AreEqual(new[] { "Float64:2.5", "Float64:7.5", "Float64:4", "NULL" }, Render("l"));

        List<QueryResultRow> filtered = await ExecQuery(database, executor, dbname,
            "SELECT id FROM pairs WHERE GREATEST(a, 0) > 4 OR LEAST(s, 'b') = 'apple' ORDER BY id");
        CollectionAssert.AreEqual(new long[] { 1, 2 }, filtered.Select(r => r.Row["id"].LongValue).ToArray());
    }

    [Test]
    public async Task ACreateTableAsSelectColumnHasTheCommonType()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedPairs(database, executor, dbname);

        await ExecDdl(database, executor, dbname, "CREATE TABLE clamped AS SELECT GREATEST(a, b) AS g, LEAST(s, 'b') AS t FROM pairs");

        Dictionary<string, string> types = (await ExecQuery(database, executor, dbname, "SHOW COLUMNS FROM clamped"))
            .ToDictionary(c => c.Row["Field"].StrValue!, c => c.Row["Type"].StrValue!);
        Assert.AreEqual("FLOAT64", types["g"]);
        Assert.AreEqual("STRING", types["t"]);
    }

    [TestCase("SELECT GREATEST(1, 'a')")]
    [TestCase("SELECT LEAST(true, 1)")]
    [TestCase("SELECT id FROM pairs WHERE GREATEST(a, s) IS NULL")]
    public async Task ArgumentsWithNoCommonTypeAreRefused(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(database, executor, dbname, "CREATE TABLE pairs (id int64 PRIMARY KEY, a int64, b float64, s string)");

        // The table is empty, so the error comes from the statement and not from a row.
        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(() => ExecQuery(database, executor, dbname, sql));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, error!.Code, sql);
    }

    [TestCase("SELECT GREATEST()")]
    [TestCase("SELECT LEAST()")]
    public async Task ACallWithNoArgumentIsRefused(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        Assert.ThrowsAsync<CamusDBException>(() => ExecQuery(database, executor, dbname, sql));
    }
}
