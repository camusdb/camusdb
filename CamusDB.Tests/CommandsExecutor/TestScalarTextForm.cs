/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
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
/// End-to-end tests for the text form of each scalar type through SQL. <c>CAST(x AS string)</c>,
/// <c>x::string</c>, <c>to_string(x)</c> and <c>concat(x)</c> must give the same exact text, and the
/// text must cast back to the same value for the types the string cast parses. The tests also use the
/// text form in a <c>WHERE</c>, a column <c>DEFAULT</c> and a stored <c>CHECK</c>.
/// </summary>
internal sealed class TestScalarTextForm : SharedNodeBaseTest
{
    private const string AllTypesDdl =
        "CREATE TABLE t (id int64 PRIMARY KEY, d date, dt datetime, b bytes, f float32, g float64, " +
        "u uuid, n NUMERIC, i int64, s string, flag bool, tags array(int64))";

    private const string AllTypesInsert =
        "INSERT INTO t (id, d, dt, b, f, g, u, n, i, s, flag, tags) VALUES (1, '2026-03-15', " +
        "'2026-03-15T08:30:00.123Z', X'DEADBEEF', 1.1, 0.1, '550e8400-e29b-41d4-a716-446655440000', " +
        "NUMERIC '12.50', -7, 'text', true, ARRAY[1, 2])";

    private async Task<(DatabaseDescriptor db, CommandExecutor executor)> SetupAllTypes()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, db, AllTypesDdl);
        await Exec(executor, db, AllTypesInsert);
        return (db, executor);
    }

    private static async Task ExecDDL(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db.Name, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task Exec(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> Select(CommandExecutor executor, DatabaseDescriptor db, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, db.Name, sql, parameters));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task<CamusDBException> AssertSelectThrows(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, db.Name, sql, null));
            await cursor.ToListAsync();
        })!;
        await db.Transactions.RollbackIfNotCompletedAsync(tx);
        return ex;
    }

    private static async Task<CamusDBException> AssertExecThrows(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, null)))!;
        await db.Transactions.RollbackIfNotCompletedAsync(tx);
        return ex;
    }

    [TestCase("d", "2026-03-15")]
    [TestCase("dt", "2026-03-15T08:30:00.1230000Z")]
    [TestCase("b", "\\xdeadbeef")]
    [TestCase("f", "1.1")]
    [TestCase("g", "0.1")]
    [TestCase("u", "550e8400-e29b-41d4-a716-446655440000")]
    [TestCase("n", "12.5")]
    [TestCase("i", "-7")]
    [TestCase("s", "text")]
    [TestCase("flag", "true")]
    [NonParallelizable]
    public async Task EveryEntryPoint_GivesTheSameExactText(string column, string expected)
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupAllTypes();

        List<QueryResultRow> rows = await Select(executor, db,
            $"SELECT CAST({column} AS string) AS c, {column}::string AS p, to_string({column}) AS f, " +
            $"concat({column}) AS k FROM t");

        IReadOnlyDictionary<string, ColumnValue> row = rows[0].Row;

        foreach (string key in new[] { "c", "p", "f", "k" })
        {
            Assert.AreEqual(ColumnType.String, row[key].Type, key);
            Assert.AreEqual(expected, row[key].StrValue, key);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Concat_MixesTextFormsAndKeepsTheNullRule()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupAllTypes();

        List<QueryResultRow> rows = await Select(executor, db,
            "SELECT concat('Day: ', d, ' at ', dt) AS a, concat(b, '/', f, '/', n, '/', u) AS b, " +
            "concat('x', CAST(NULL AS date), 'y') AS c, concat(CAST(NULL AS date), CAST(NULL AS bytes)) AS e FROM t");

        IReadOnlyDictionary<string, ColumnValue> row = rows[0].Row;
        Assert.AreEqual("Day: 2026-03-15 at 2026-03-15T08:30:00.1230000Z", row["a"].StrValue);
        Assert.AreEqual("\\xdeadbeef/1.1/12.5/550e8400-e29b-41d4-a716-446655440000", row["b"].StrValue);
        Assert.AreEqual("xy", row["c"].StrValue, "a NULL argument adds nothing");
        Assert.AreEqual(ColumnType.Null, row["e"].Type, "all-NULL arguments give NULL");
    }

    [Test]
    [NonParallelizable]
    public async Task TextForm_CastsBackToTheSameValue()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupAllTypes();

        List<QueryResultRow> rows = await Select(executor, db,
            "SELECT d, dt, b, f, u, n, " +
            "CAST(CAST(d AS string) AS date) AS d2, CAST(CAST(dt AS string) AS datetime) AS dt2, " +
            "CAST(CAST(b AS string) AS bytes) AS b2, CAST(CAST(f AS string) AS float32) AS f2, " +
            "CAST(CAST(u AS string) AS uuid) AS u2, CAST(CAST(n AS string) AS numeric) AS n2 FROM t");

        IReadOnlyDictionary<string, ColumnValue> row = rows[0].Row;
        Assert.AreEqual(ColumnType.Date, row["d2"].Type);
        Assert.AreEqual(row["d"].LongValue, row["d2"].LongValue);
        Assert.AreEqual(ColumnType.DateTime, row["dt2"].Type);
        Assert.AreEqual(row["dt"].LongValue, row["dt2"].LongValue);
        Assert.AreEqual(ColumnType.Bytes, row["b2"].Type);
        CollectionAssert.AreEqual(row["b"].BytesValue, row["b2"].BytesValue);
        Assert.AreEqual(ColumnType.Float32, row["f2"].Type);
        Assert.AreEqual((float)row["f"].FloatValue, (float)row["f2"].FloatValue);
        Assert.AreEqual(ColumnType.Uuid, row["u2"].Type);
        Assert.AreEqual(row["u"].UuidValue, row["u2"].UuidValue);
        Assert.AreEqual(ColumnType.Numeric, row["n2"].Type);
        Assert.AreEqual(row["n"].NumericValue, row["n2"].NumericValue);
    }

    [Test]
    [NonParallelizable]
    public async Task DateTime_WithFullTickPrecision_RoundTrips()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        // 1234567 ticks after the second: all seven fractional digits are significant.
        long ticks = new DateTime(2026, 10, 11, 8, 30, 0, DateTimeKind.Utc).Ticks + 1_234_567;
        Dictionary<string, ColumnValue> parameters = new() { ["@dt"] = new ColumnValue(ColumnType.DateTime, ticks) };

        List<QueryResultRow> rows = await Select(executor, db,
            "SELECT CAST(@dt AS string) AS s, CAST(CAST(@dt AS string) AS datetime) AS back", parameters);

        Assert.AreEqual("2026-10-11T08:30:00.1234567Z", rows[0].Row["s"].StrValue);
        Assert.AreEqual(ticks, rows[0].Row["back"].LongValue);
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(1024 * 1024)]
    [NonParallelizable]
    public async Task Bytes_RoundTripThroughText(int length)
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        byte[] value = new byte[length];
        new Random(length).NextBytes(value);
        Dictionary<string, ColumnValue> parameters = new() { ["@b"] = new ColumnValue(value) };

        List<QueryResultRow> rows = await Select(executor, db,
            "SELECT CAST(@b AS string) AS s, CAST(CAST(@b AS string) AS bytes) AS back", parameters);

        Assert.AreEqual("\\x" + Convert.ToHexStringLower(value), rows[0].Row["s"].StrValue);
        Assert.AreEqual(ColumnType.Bytes, rows[0].Row["back"].Type);
        CollectionAssert.AreEqual(value, rows[0].Row["back"].BytesValue);
    }

    [Test]
    [NonParallelizable]
    public async Task StringToBytes_AcceptsBothHexPrefixes()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        List<QueryResultRow> rows = await Select(executor, db,
            "SELECT CAST('\\xDEad' AS bytes) AS pg, CAST('0xdeAD' AS bytes) AS hex, CAST('\\x' AS bytes) AS empty");

        CollectionAssert.AreEqual(new byte[] { 0xDE, 0xAD }, rows[0].Row["pg"].BytesValue);
        CollectionAssert.AreEqual(new byte[] { 0xDE, 0xAD }, rows[0].Row["hex"].BytesValue);
        CollectionAssert.AreEqual(Array.Empty<byte>(), rows[0].Row["empty"].BytesValue);

        CamusDBException odd = await AssertSelectThrows(executor, db, "SELECT CAST('\\xabc' AS bytes)");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, odd.Code);
        StringAssert.Contains("even number of hex digits", odd.Message);

        CamusDBException noPrefix = await AssertSelectThrows(executor, db, "SELECT CAST('abcd' AS bytes)");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, noPrefix.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task Array_StillHasNoTextForm()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupAllTypes();

        CamusDBException cast = await AssertSelectThrows(executor, db, "SELECT CAST(tags AS string) FROM t");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, cast.Code);
        StringAssert.Contains("cannot convert Array to String", cast.Message);

        CamusDBException concat = await AssertSelectThrows(executor, db, "SELECT concat('a', tags) FROM t");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, concat.Code);
        StringAssert.Contains("cannot convert argument of type Array to string", concat.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TextForm_FiltersInWhere()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupAllTypes();
        await Exec(executor, db, "INSERT INTO t (id, d, b) VALUES (2, '2025-01-02', X'00')");

        List<QueryResultRow> byDate = await Select(executor, db, "SELECT id FROM t WHERE CAST(d AS string) = '2026-03-15'");
        Assert.AreEqual(new[] { 1L }, byDate.Select(r => r.Row["id"].LongValue).ToArray());

        List<QueryResultRow> byBytes = await Select(executor, db, "SELECT id FROM t WHERE b::string = '\\x00'");
        Assert.AreEqual(new[] { 2L }, byBytes.Select(r => r.Row["id"].LongValue).ToArray());
    }

    [Test]
    [NonParallelizable]
    public async Task TextForm_InColumnDefault()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, db,
            "CREATE TABLE e (id int64 PRIMARY KEY, label string DEFAULT(concat('day ', CAST('2026-03-15' AS date))))");
        await Exec(executor, db, "INSERT INTO e (id) VALUES (1)");

        List<QueryResultRow> rows = await Select(executor, db, "SELECT label FROM e");
        Assert.AreEqual("day 2026-03-15", rows[0].Row["label"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TextForm_InStoredCheck()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, db,
            "CREATE TABLE c (id int64 PRIMARY KEY, d date, " +
            "CONSTRAINT only_2026 CHECK (starts_with(CAST(d AS string), '2026-')))");

        await Exec(executor, db, "INSERT INTO c (id, d) VALUES (1, '2026-01-02')");

        CamusDBException ex = await AssertExecThrows(executor, db, "INSERT INTO c (id, d) VALUES (2, '2025-12-31')");
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code);

        List<QueryResultRow> rows = await Select(executor, db, "SELECT id FROM c");
        Assert.AreEqual(new[] { 1L }, rows.Select(r => r.Row["id"].LongValue).ToArray());
    }

    [Test]
    public void DateText_IgnoresTheCurrentCulture()
    {
        CultureInfo saved = CultureInfo.CurrentCulture;

        try
        {
            // The Thai culture uses the Buddhist calendar, where 2026 is the year 2569.
            CultureInfo.CurrentCulture = new CultureInfo("th-TH");

            long ticks = new DateTime(2026, 3, 15, 0, 0, 0, DateTimeKind.Utc).Ticks;
            Assert.AreEqual("2026-03-15", new ColumnValue(ColumnType.Date, ticks).IsoValue);
            Assert.AreEqual("2026-03-15T00:00:00.0000000Z", new ColumnValue(ColumnType.DateTime, ticks).IsoValue);
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }
}
