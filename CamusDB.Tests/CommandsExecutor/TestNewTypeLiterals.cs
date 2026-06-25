
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
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// T8 acceptance tests: literal parsing / coercion for float32, date, datetime, bytes, and CAST.
/// </summary>
[NonParallelizable]
internal sealed class TestNewTypeLiterals : SharedNodeBaseTest
{
    // ── Setup helpers ─────────────────────────────────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)> SetupTable(string ddl)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, ddl, null));
        return (dbname, db, executor);
    }

    private static async Task ExecInsert(CommandExecutor executor, DatabaseDescriptor db, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, parameters));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> ExecSelect(
        CommandExecutor executor, DatabaseDescriptor db, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, db.Name, sql, parameters));
        return await cursor.ToListAsync();
    }

    // Each test table has exactly one row — SELECT * is the simplest reliable way
    // to read it back without a WHERE string-vs-Id type mismatch on the id column.
    private static async Task<QueryResultRow> SelectOnlyRow(
        CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        List<QueryResultRow> rows = await ExecSelect(executor, db, sql);
        Assert.AreEqual(1, rows.Count, $"Expected exactly 1 row from: {sql}");
        return rows[0];
    }

    private static string OID => ObjectIdGenerator.Generate().ToString();

    // ── float32 ───────────────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task Float32_InsertFloatLiteral_RoundTrips()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, v float32, PRIMARY KEY (id))");

        await ExecInsert(executor, db, $"INSERT INTO t (id, v) VALUES (\"{OID}\", 3.14)");

        QueryResultRow row = await SelectOnlyRow(executor, db, "SELECT v FROM t");

        Assert.AreEqual(ColumnType.Float32, row.Row["v"].Type);
        Assert.That((float)row.Row["v"].FloatValue, Is.EqualTo(3.14f).Within(1e-5f));
    }

    [Test]
    [NonParallelizable]
    public async Task Float32_InsertIntegerLiteral_Coerces()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, v float32, PRIMARY KEY (id))");

        await ExecInsert(executor, db, $"INSERT INTO t (id, v) VALUES (\"{OID}\", 42)");

        QueryResultRow row = await SelectOnlyRow(executor, db, "SELECT v FROM t");

        Assert.AreEqual(ColumnType.Float32, row.Row["v"].Type);
        Assert.That((float)row.Row["v"].FloatValue, Is.EqualTo(42f).Within(1e-5f));
    }

    // ── date ──────────────────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task Date_InsertStringLiteral_RoundTrips()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, d date, PRIMARY KEY (id))");

        await ExecInsert(executor, db, $"INSERT INTO t (id, d) VALUES (\"{OID}\", '2026-06-25')");

        QueryResultRow row = await SelectOnlyRow(executor, db, "SELECT d FROM t");
        ColumnValue col = row.Row["d"];

        Assert.AreEqual(ColumnType.Date, col.Type);
        DateTime decoded = new DateTime(col.LongValue, DateTimeKind.Utc);
        Assert.AreEqual(2026, decoded.Year);
        Assert.AreEqual(6,    decoded.Month);
        Assert.AreEqual(25,   decoded.Day);
        Assert.AreEqual(0,    decoded.Hour);
    }

    [Test]
    [NonParallelizable]
    public async Task Date_InsertUnparseableString_Throws()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, d date, PRIMARY KEY (id))");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecInsert(executor, db, $"INSERT INTO t (id, d) VALUES (\"{OID}\", 'not-a-date')"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        StringAssert.Contains("yyyy-MM-dd", ex.Message);
    }

    // ── datetime ──────────────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task DateTime_InsertStringLiteral_RoundTrips()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, ts datetime, PRIMARY KEY (id))");

        await ExecInsert(executor, db, $"INSERT INTO t (id, ts) VALUES (\"{OID}\", '2026-06-25T14:30:00Z')");

        QueryResultRow row = await SelectOnlyRow(executor, db, "SELECT ts FROM t");
        ColumnValue col = row.Row["ts"];

        Assert.AreEqual(ColumnType.DateTime, col.Type);
        DateTime decoded = new DateTime(col.LongValue, DateTimeKind.Utc);
        Assert.AreEqual(2026, decoded.Year);
        Assert.AreEqual(6,    decoded.Month);
        Assert.AreEqual(25,   decoded.Day);
        Assert.AreEqual(14,   decoded.Hour);
        Assert.AreEqual(30,   decoded.Minute);
    }

    [Test]
    [NonParallelizable]
    public async Task DateTime_WithMilliseconds_RoundTrips()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, ts datetime, PRIMARY KEY (id))");

        await ExecInsert(executor, db, $"INSERT INTO t (id, ts) VALUES (\"{OID}\", '2026-01-15T08:00:00.123Z')");

        QueryResultRow row = await SelectOnlyRow(executor, db, "SELECT ts FROM t");
        ColumnValue col = row.Row["ts"];

        Assert.AreEqual(ColumnType.DateTime, col.Type);
        Assert.AreEqual(123, new DateTime(col.LongValue, DateTimeKind.Utc).Millisecond);
    }

    [Test]
    [NonParallelizable]
    public async Task DateTime_InsertUnparseableString_Throws()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, ts datetime, PRIMARY KEY (id))");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecInsert(executor, db, $"INSERT INTO t (id, ts) VALUES (\"{OID}\", 'June 25 2026')"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        StringAssert.Contains("ISO-8601", ex.Message);
    }

    // ── bytes ─────────────────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task Bytes_Insert0xLiteral_RoundTrips()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, b bytes, PRIMARY KEY (id))");

        await ExecInsert(executor, db, $"INSERT INTO t (id, b) VALUES (\"{OID}\", '0xDEADBEEF')");

        QueryResultRow row = await SelectOnlyRow(executor, db, "SELECT b FROM t");
        ColumnValue col = row.Row["b"];

        Assert.AreEqual(ColumnType.Bytes, col.Type);
        Assert.AreEqual(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF }, col.BytesValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Bytes_InsertUppercaseHex_RoundTrips()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, b bytes, PRIMARY KEY (id))");

        await ExecInsert(executor, db, $"INSERT INTO t (id, b) VALUES (\"{OID}\", '0XABCD')");

        QueryResultRow row = await SelectOnlyRow(executor, db, "SELECT b FROM t");
        Assert.AreEqual(new byte[] { 0xAB, 0xCD }, row.Row["b"].BytesValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Bytes_InsertInvalidLiteral_Throws()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, b bytes, PRIMARY KEY (id))");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecInsert(executor, db, $"INSERT INTO t (id, b) VALUES (\"{OID}\", 'DEADBEEF')"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        StringAssert.Contains("0x", ex.Message);
    }

    // ── CAST to new scalar types ──────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task Cast_Float32_FromFloat64()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, v float64, PRIMARY KEY (id))");
        await ExecInsert(executor, db, $"INSERT INTO t (id, v) VALUES (\"{OID}\", 2.5)");

        QueryResultRow row = await SelectOnlyRow(executor, db, "SELECT CAST(v AS float32) FROM t");

        Assert.AreEqual(ColumnType.Float32, row.Row["0"].Type);
        Assert.That((float)row.Row["0"].FloatValue, Is.EqualTo(2.5f).Within(1e-5f));
    }

    [Test]
    [NonParallelizable]
    public async Task Cast_Date_FromString()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, v string, PRIMARY KEY (id))");
        await ExecInsert(executor, db, $"INSERT INTO t (id, v) VALUES (\"{OID}\", '2026-03-15')");

        QueryResultRow row = await SelectOnlyRow(executor, db, "SELECT CAST(v AS date) FROM t");
        ColumnValue col = row.Row["0"];

        Assert.AreEqual(ColumnType.Date, col.Type);
        DateTime decoded = new DateTime(col.LongValue, DateTimeKind.Utc);
        Assert.AreEqual(2026, decoded.Year);
        Assert.AreEqual(3,    decoded.Month);
        Assert.AreEqual(15,   decoded.Day);
    }

    [Test]
    [NonParallelizable]
    public async Task Cast_DateTime_FromString()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, v string, PRIMARY KEY (id))");
        await ExecInsert(executor, db, $"INSERT INTO t (id, v) VALUES (\"{OID}\", '2026-03-15T10:20:30Z')");

        QueryResultRow row = await SelectOnlyRow(executor, db, "SELECT CAST(v AS datetime) FROM t");
        ColumnValue col = row.Row["0"];

        Assert.AreEqual(ColumnType.DateTime, col.Type);
        DateTime decoded = new DateTime(col.LongValue, DateTimeKind.Utc);
        Assert.AreEqual(10, decoded.Hour);
        Assert.AreEqual(20, decoded.Minute);
        Assert.AreEqual(30, decoded.Second);
    }

    [Test]
    [NonParallelizable]
    public async Task Cast_Bytes_FromHexString()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, v string, PRIMARY KEY (id))");
        await ExecInsert(executor, db, $"INSERT INTO t (id, v) VALUES (\"{OID}\", '0x0102')");

        QueryResultRow row = await SelectOnlyRow(executor, db, "SELECT CAST(v AS bytes) FROM t");
        ColumnValue col = row.Row["0"];

        Assert.AreEqual(ColumnType.Bytes, col.Type);
        Assert.AreEqual(new byte[] { 0x01, 0x02 }, col.BytesValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Cast_Date_UnparseableString_Throws()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, v string, PRIMARY KEY (id))");
        await ExecInsert(executor, db, $"INSERT INTO t (id, v) VALUES (\"{OID}\", 'bad-date')");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecSelect(executor, db, "SELECT CAST(v AS date) FROM t"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
    }

    // ── to_float32 / to_date / to_datetime / to_bytes function aliases ────────

    [Test]
    [NonParallelizable]
    public async Task ToFloat32_Function_Works()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, v float64, PRIMARY KEY (id))");
        await ExecInsert(executor, db, $"INSERT INTO t (id, v) VALUES (\"{OID}\", 1.5)");

        QueryResultRow row = await SelectOnlyRow(executor, db, "SELECT to_float32(v) FROM t");

        Assert.AreEqual(ColumnType.Float32, row.Row["0"].Type);
        Assert.That((float)row.Row["0"].FloatValue, Is.EqualTo(1.5f).Within(1e-5f));
    }

    [Test]
    [NonParallelizable]
    public async Task ToDate_Function_Works()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, v string, PRIMARY KEY (id))");
        await ExecInsert(executor, db, $"INSERT INTO t (id, v) VALUES (\"{OID}\", '2026-12-31')");

        QueryResultRow row = await SelectOnlyRow(executor, db, "SELECT to_date(v) FROM t");
        ColumnValue col = row.Row["0"];

        Assert.AreEqual(ColumnType.Date, col.Type);
        Assert.AreEqual(31, new DateTime(col.LongValue, DateTimeKind.Utc).Day);
    }

    // ── parameterized insert — array round-trip ───────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task Array_ParameterInsert_RoundTrips()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, tags array(int64), PRIMARY KEY (id))");

        ColumnValue arrayParam = new(
            type: ColumnType.Array,
            strValue: null,
            longValue: 0,
            floatValue: 0,
            boolValue: false,
            bytesValue: null,
            arrayValues: [new(ColumnType.Integer64, 10L), new(ColumnType.Integer64, 20L), new(ColumnType.Integer64, 30L)],
            arrayElementType: ColumnType.Integer64);

        await ExecInsert(executor, db,
            "INSERT INTO t (id, tags) VALUES (@id, @tags)",
            parameters: new() { { "@id", new(ColumnType.Id, OID) }, { "@tags", arrayParam } });

        QueryResultRow row = await SelectOnlyRow(executor, db, "SELECT tags FROM t");
        ColumnValue col = row.Row["tags"];

        Assert.AreEqual(ColumnType.Array,    col.Type);
        Assert.AreEqual(ColumnType.Integer64, col.ArrayElementType);
        Assert.AreEqual(3,   col.ArrayValues!.Count);
        Assert.AreEqual(10L, col.ArrayValues[0].LongValue);
        Assert.AreEqual(20L, col.ArrayValues[1].LongValue);
        Assert.AreEqual(30L, col.ArrayValues[2].LongValue);
    }

    // ── UPDATE coercion ───────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task Date_UpdateStringLiteral_Coerces()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, d date, PRIMARY KEY (id))");
        string id = OID;

        await ExecInsert(executor, db, $"INSERT INTO t (id, d) VALUES (\"{id}\", '2026-01-01')");

        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name,
            "UPDATE t SET d = '2026-07-04' WHERE 1=1", null));
        await db.Transactions.CommitAsync(tx);

        QueryResultRow row = await SelectOnlyRow(executor, db, "SELECT d FROM t");
        ColumnValue col = row.Row["d"];

        Assert.AreEqual(ColumnType.Date, col.Type);
        DateTime decoded = new DateTime(col.LongValue, DateTimeKind.Utc);
        Assert.AreEqual(7, decoded.Month);
        Assert.AreEqual(4, decoded.Day);
    }
}
