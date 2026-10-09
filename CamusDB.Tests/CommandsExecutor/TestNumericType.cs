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
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end tests for the NUMERIC column type through SQL: DDL (NUMERIC and the DECIMAL alias, the
/// refused NUMERIC(P,S) form), exact literal conversion on INSERT, UPDATE and DEFAULT, rounding and
/// the range at write time, the typed literal NUMERIC '…', CAST in both directions, SHOW COLUMNS and
/// SHOW CREATE TABLE, the JSON result form, and the row round trip across a close and reopen. The
/// expected values follow Spanner GoogleSQL NUMERIC (precision 38, scale 9).
/// </summary>
internal sealed class TestNumericType : SharedNodeBaseTest
{
    private const string MaxText = "99999999999999999999999999999.999999999";
    private const string MinText = "-99999999999999999999999999999.999999999";

    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)> SetupTable(string ddl)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, db, ddl);
        return (dbname, db, executor);
    }

    private static async Task ExecDDL(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db.Name, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task Exec(CommandExecutor executor, DatabaseDescriptor db, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, parameters));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> Select(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, db.Name, sql, null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task<CamusDBException> AssertExecThrows(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, null)))!;
        await db.Transactions.RollbackIfNotCompletedAsync(tx);
        return ex;
    }

    private static async Task<CamusDBException> AssertDDLThrows(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db.Name, sql, null)))!;
        await db.Transactions.RollbackIfNotCompletedAsync(tx);
        return ex;
    }

    /// <summary>The canonical text of column <c>p</c> per row id, ordered by id.</summary>
    private static async Task<string?[]> ReadP(CommandExecutor executor, DatabaseDescriptor db)
    {
        List<QueryResultRow> rows = await Select(executor, db, "SELECT id, p FROM t ORDER BY id");
        return rows.Select(r =>
        {
            ColumnValue p = r.Row["p"];
            if (p.Type == ColumnType.Null)
                return null;
            Assert.AreEqual(ColumnType.Numeric, p.Type);
            return p.NumericValue;
        }).ToArray();
    }

    [Test]
    [NonParallelizable]
    public async Task Insert_Literals_StoreExactly()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id int64 PRIMARY KEY, p NUMERIC)");

        // A bare decimal literal is a float literal, but it converts from its source text, so values a
        // double cannot hold arrive intact.
        await Exec(executor, db, "INSERT INTO t (id, p) VALUES (1, 0.1)");
        await Exec(executor, db, "INSERT INTO t (id, p) VALUES (2, 12345678901234567890.123456789)");
        await Exec(executor, db, "INSERT INTO t (id, p) VALUES (3, -42)");
        await Exec(executor, db, "INSERT INTO t (id, p) VALUES (4, '7.250')");
        await Exec(executor, db, "INSERT INTO t (id, p) VALUES (5, NUMERIC '0.000000001')");
        await Exec(executor, db, "INSERT INTO t (id, p) VALUES (6, NULL)");
        await Exec(executor, db, "INSERT INTO t (id, p) VALUES (7, -2.0000000005)");

        CollectionAssert.AreEqual(
            new[] { "0.1", "12345678901234567890.123456789", "-42", "7.25", "0.000000001", null, "-2.000000001" },
            await ReadP(executor, db));
    }

    [Test]
    [NonParallelizable]
    public async Task Insert_MoreThanNineFractionalDigits_RoundsHalfAwayFromZero()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id int64 PRIMARY KEY, p NUMERIC)");

        await Exec(executor, db, "INSERT INTO t (id, p) VALUES (1, 1.0000000005), (2, -1.0000000005), (3, 1.0000000004999)");

        CollectionAssert.AreEqual(new[] { "1.000000001", "-1.000000001", "1" }, await ReadP(executor, db));
    }

    [Test]
    [NonParallelizable]
    public async Task Insert_RangeBounds_AcceptedAndOneUnitPastRefused()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id int64 PRIMARY KEY, p NUMERIC)");

        await Exec(executor, db, $"INSERT INTO t (id, p) VALUES (1, {MaxText}), (2, {MinText})");
        CollectionAssert.AreEqual(new[] { MaxText, MinText }, await ReadP(executor, db));

        CamusDBException above = await AssertExecThrows(executor, db,
            "INSERT INTO t (id, p) VALUES (3, 100000000000000000000000000000.0)");
        Assert.AreEqual(CamusDBErrorCodes.NumericValueOutOfRange, above.Code);

        // An integer literal must fit in INT64, as in Spanner, so a wider one is refused before it
        // reaches the column. NUMERIC '…' or a decimal point writes such a value.
        CamusDBException wideInteger = await AssertExecThrows(executor, db,
            "INSERT INTO t (id, p) VALUES (3, 12345678901234567890123)");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, wideInteger.Code);

        CamusDBException below = await AssertExecThrows(executor, db,
            "INSERT INTO t (id, p) VALUES (3, -99999999999999999999999999999.9999999995)");
        Assert.AreEqual(CamusDBErrorCodes.NumericValueOutOfRange, below.Code);

        CamusDBException typed = await AssertExecThrows(executor, db,
            "INSERT INTO t (id, p) VALUES (3, NUMERIC '1e29')");
        Assert.AreEqual(CamusDBErrorCodes.NumericValueOutOfRange, typed.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task Insert_NotANumber_Refused()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id int64 PRIMARY KEY, p NUMERIC)");

        CamusDBException ex = await AssertExecThrows(executor, db, "INSERT INTO t (id, p) VALUES (1, 'abc')");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task Insert_ComputedFloat_ConvertsFromItsBinaryValue()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id int64 PRIMARY KEY, p NUMERIC)");

        // 0.1 + 0.2 is computed in FLOAT64 (0.30000000000000004); its exact value rounds to 0.3.
        await Exec(executor, db, "INSERT INTO t (id, p) VALUES (1, 0.1 + 0.2)");

        CollectionAssert.AreEqual(new[] { "0.3" }, await ReadP(executor, db));
    }

    [Test]
    [NonParallelizable]
    public async Task Update_SetsTheExactLiteral()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id int64 PRIMARY KEY, p NUMERIC)");

        await Exec(executor, db, "INSERT INTO t (id, p) VALUES (1, 1)");
        await Exec(executor, db, "UPDATE t SET p = 98765432109876543210.987654321 WHERE id = 1");

        CollectionAssert.AreEqual(new[] { "98765432109876543210.987654321" }, await ReadP(executor, db));
    }

    [Test]
    [NonParallelizable]
    public async Task Where_SameTypeComparison_FiltersRows()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id int64 PRIMARY KEY, p NUMERIC)");

        await Exec(executor, db, "INSERT INTO t (id, p) VALUES (1, 0.1), (2, 0.2), (3, -5)");

        List<QueryResultRow> rows = await Select(executor, db,
            "SELECT id FROM t WHERE p > NUMERIC '0.1' ORDER BY id");
        CollectionAssert.AreEqual(new[] { 2L }, rows.Select(r => r.Row["id"].LongValue).ToArray());

        List<QueryResultRow> ordered = await Select(executor, db, "SELECT id FROM t ORDER BY p DESC");
        CollectionAssert.AreEqual(new[] { 2L, 1L, 3L }, ordered.Select(r => r.Row["id"].LongValue).ToArray());
    }

    [Test]
    [NonParallelizable]
    public async Task DecimalAlias_CreatesANumericColumn()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id int64 PRIMARY KEY, p DECIMAL)");

        await Exec(executor, db, "INSERT INTO t (id, p) VALUES (1, 2.5)");
        CollectionAssert.AreEqual(new[] { "2.5" }, await ReadP(executor, db));

        List<QueryResultRow> cols = await Select(executor, db, "SHOW COLUMNS FROM t");
        Assert.AreEqual("NUMERIC", cols.Single(r => r.Row["Field"].StrValue == "p").Row["Type"].StrValue);
    }

    [TestCase("NUMERIC(10,2)")]
    [TestCase("DECIMAL(10,2)")]
    [TestCase("NUMERIC(10)")]
    [TestCase("decimal(5, 5)")]
    [NonParallelizable]
    public async Task ParameterizedForm_Refused(string type)
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = await AssertDDLThrows(executor, db, $"CREATE TABLE t (id int64 PRIMARY KEY, p {type})");
        Assert.AreEqual(CamusDBErrorCodes.FeatureNotSupported, ex.Code);
        StringAssert.Contains("fixed precision of 38", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task AlterAddColumn_Numeric_BackfillsTheExactDefault()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id int64 PRIMARY KEY, name string)");

        await Exec(executor, db, "INSERT INTO t (id, name) VALUES (1, 'a')");
        await ExecDDL(executor, db, "ALTER TABLE t ADD COLUMN p NUMERIC DEFAULT(12345678901234567890.25)");
        await Exec(executor, db, "INSERT INTO t (id, name, p) VALUES (2, 'b', -0.5)");

        CollectionAssert.AreEqual(new[] { "12345678901234567890.25", "-0.5" }, await ReadP(executor, db));

        CamusDBException ex = await AssertDDLThrows(executor, db, "ALTER TABLE t ADD COLUMN q DECIMAL(10,2)");
        Assert.AreEqual(CamusDBErrorCodes.FeatureNotSupported, ex.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task ArrayOfNumeric_Refused()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = await AssertDDLThrows(executor, db, "CREATE TABLE t (id int64 PRIMARY KEY, p array(numeric))");
        Assert.AreEqual(CamusDBErrorCodes.FeatureNotSupported, ex.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task Default_IsExact_AndRendersAsATypedLiteral()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id int64 PRIMARY KEY, p NUMERIC DEFAULT(12345678901234567890.5))");

        await Exec(executor, db, "INSERT INTO t (id) VALUES (1)");
        CollectionAssert.AreEqual(new[] { "12345678901234567890.5" }, await ReadP(executor, db));

        List<QueryResultRow> cols = await Select(executor, db, "SHOW COLUMNS FROM t");
        Assert.AreEqual("12345678901234567890.5", cols.Single(r => r.Row["Field"].StrValue == "p").Row["Default"].StrValue);

        // The rendered DDL re-creates the same default when it runs.
        List<QueryResultRow> shown = await Select(executor, db, "SHOW CREATE TABLE t");
        string ddl = shown[0].Row["Create Table"].StrValue!;
        StringAssert.Contains("NUMERIC", ddl);
        StringAssert.Contains("NUMERIC '12345678901234567890.5'", ddl);

        (_, DatabaseDescriptor copy, _) = await CreateDatabase();
        await ExecDDL(executor, copy, ddl);
        await Exec(executor, copy, "INSERT INTO t (id) VALUES (1)");
        CollectionAssert.AreEqual(new[] { "12345678901234567890.5" }, await ReadP(executor, copy));
    }

    [Test]
    [NonParallelizable]
    public async Task Cast_BothDirections()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id int64 PRIMARY KEY, p NUMERIC)");

        await Exec(executor, db, "INSERT INTO t (id, p) VALUES (1, -2.5)");

        List<QueryResultRow> rows = await Select(executor, db,
            "SELECT CAST(p AS STRING) AS s, CAST(p AS INT64) AS i, CAST(p AS FLOAT64) AS f, " +
            "CAST('1.10' AS NUMERIC) AS fromString, CAST(3 AS NUMERIC) AS fromInt, p::string AS postfix FROM t");

        IReadOnlyDictionary<string, ColumnValue> row = rows[0].Row;
        Assert.AreEqual("-2.5", row["s"].StrValue);
        Assert.AreEqual(-3L, row["i"].LongValue, "half away from zero");
        Assert.AreEqual(-2.5, row["f"].FloatValue);
        Assert.AreEqual("1.1", row["fromString"].NumericValue);
        Assert.AreEqual("3", row["fromInt"].NumericValue);
        Assert.AreEqual("-2.5", row["postfix"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Cast_ToFloat32_RoundsOnce()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id int64 PRIMARY KEY, p NUMERIC)");
        await Exec(executor, db, "INSERT INTO t (id, p) VALUES (1, NUMERIC '100000004.000000001')");

        List<QueryResultRow> rows = await Select(executor, db, "SELECT CAST(p AS FLOAT32) AS f FROM t");
        Assert.AreEqual(100000008f, (float)rows[0].Row["f"].FloatValue);
    }

    /// <summary>
    /// A sort, a DISTINCT and a GROUP BY over NUMERIC cells on an engine forced to spill after two
    /// rows: the spill codec must write and read the cells, and the results must equal the
    /// in-memory answer.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Queries_ThatSpill_CarryNumericCells()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) =
            await CreateDatabase(Options with { SpillEnabled = true, ForceSpillThresholdRows = 2 });
        await ExecDDL(executor, db, "CREATE TABLE t (id int64 PRIMARY KEY, p NUMERIC)");
        await Exec(executor, db,
            $"INSERT INTO t (id, p) VALUES (1, 2.5), (2, -1), (3, {MaxText}), (4, 2.5), (5, 0.000000001), (6, {MinText}), (7, -1), (8, NULL)");

        List<QueryResultRow> sorted = await Select(executor, db, "SELECT p FROM t ORDER BY p");
        CollectionAssert.AreEqual(
            new[] { null, MinText, "-1", "-1", "0.000000001", "2.5", "2.5", MaxText },
            sorted.Select(r => r.Row["p"].NumericValue).ToArray());

        List<QueryResultRow> distinct = await Select(executor, db, "SELECT DISTINCT p FROM t");
        CollectionAssert.AreEquivalent(
            new[] { null, MinText, "-1", "0.000000001", "2.5", MaxText },
            distinct.Select(r => r.Row["p"].NumericValue).ToArray());

        List<QueryResultRow> grouped = await Select(executor, db, "SELECT p, COUNT(*) AS c FROM t GROUP BY p");
        Assert.AreEqual(2L, grouped.Single(r => r.Row["p"].NumericValue == "2.5").Row["c"].LongValue);
        Assert.AreEqual(2L, grouped.Single(r => r.Row["p"].NumericValue == "-1").Row["c"].LongValue);
        Assert.AreEqual(6, grouped.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task CompactJsonForm_IsTheCanonicalString()
    {
        Assert.AreEqual("1.1", CompactRowEncoder.EncodeValue(ColumnValue.FromNumericString("1.10")));
        Assert.AreEqual(MinText, CompactRowEncoder.EncodeValue(ColumnValue.FromNumericString(MinText)));
        await Task.CompletedTask;
    }

    [Test]
    [NonParallelizable]
    public async Task Values_SurviveCloseAndReopen()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id int64 PRIMARY KEY, p NUMERIC DEFAULT(1.5), q NUMERIC)");

        await Exec(executor, db, $"INSERT INTO t (id, p, q) VALUES (1, {MaxText}, -0.000000001), (2, {MinText}, NULL)");
        await Exec(executor, db, "INSERT INTO t (id) VALUES (3)");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        List<QueryResultRow> rows = await Select(executor, reopened, "SELECT id, p, q FROM t ORDER BY id");
        CollectionAssert.AreEqual(new[] { MaxText, MinText, "1.5" }, rows.Select(r => r.Row["p"].NumericValue).ToArray());
        CollectionAssert.AreEqual(new[] { "-0.000000001", null, null }, rows.Select(r => r.Row["q"].NumericValue).ToArray());
    }
}
