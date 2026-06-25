
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.App.Models;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Acceptance tests: HTTP/API request and response models for new column types.
/// Covers CreateTableColumn DTO JSON deserialization, ColumnValue JSON round-trips
/// (bytes as base64, date/datetime with IsoValue, arrays as JSON arrays), and
/// end-to-end create/insert/query for each new type via the HTTP DTO → executor path.
/// </summary>
[NonParallelizable]
internal sealed class TestNewTypeHttpModels : SharedNodeBaseTest
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    // ── helpers ───────────────────────────────────────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)> SetupTable(string ddl)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, ddl, null));
        return (dbname, db, executor);
    }

    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)> SetupTableViaColumnInfo(
        ColumnInfo[] columns)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        CreateTableTicket ticket = new(
            databaseName: dbname,
            tableName: "t",
            columns: columns,
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        );
        await executor.CreateTable(ticket);
        return (dbname, db, executor);
    }

    private static async Task ExecInsert(CommandExecutor executor, DatabaseDescriptor db, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, parameters));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task<QueryResultRow> SelectOnlyRow(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, db.Name, sql, null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        Assert.AreEqual(1, rows.Count, $"Expected exactly 1 row from: {sql}");
        return rows[0];
    }

    private static string OID => ObjectIdGenerator.Generate().ToString();

    // ── CreateTableColumn DTO JSON deserialization ────────────────────────────

    [Test]
    [NonParallelizable]
    public void CreateTableColumn_Deserialize_NewTypes()
    {
        string json = """
            [
              {"name":"a","type":"float32"},
              {"name":"b","type":"date"},
              {"name":"c","type":"datetime"},
              {"name":"d","type":"bytes"},
              {"name":"e","type":"string","maxLength":32},
              {"name":"f","type":"array","arrayElementType":"int64"}
            ]
            """;

        CreateTableColumn[]? cols = JsonSerializer.Deserialize<CreateTableColumn[]>(json, JsonOpts);
        Assert.IsNotNull(cols);
        Assert.AreEqual(6, cols!.Length);

        Assert.AreEqual("float32", cols[0].Type);
        Assert.AreEqual("date",    cols[1].Type);
        Assert.AreEqual("datetime",cols[2].Type);
        Assert.AreEqual("bytes",   cols[3].Type);
        Assert.AreEqual("string",  cols[4].Type);
        Assert.AreEqual(32,        cols[4].MaxLength);
        Assert.AreEqual("array",   cols[5].Type);
        Assert.AreEqual("int64",   cols[5].ArrayElementType);
    }

    [Test]
    [NonParallelizable]
    public void CreateTableColumn_Deserialize_Aliases()
    {
        string json = """
            [
              {"name":"a","type":"int"},
              {"name":"b","type":"real"},
              {"name":"c","type":"blob"},
              {"name":"d","type":"timestamp"},
              {"name":"e","type":"integer"}
            ]
            """;

        CreateTableColumn[]? cols = JsonSerializer.Deserialize<CreateTableColumn[]>(json, JsonOpts);
        Assert.IsNotNull(cols);
        Assert.AreEqual("int",       cols![0].Type);
        Assert.AreEqual("real",      cols[1].Type);
        Assert.AreEqual("blob",      cols[2].Type);
        Assert.AreEqual("timestamp", cols[3].Type);
        Assert.AreEqual("integer",   cols[4].Type);
    }

    // ── ColumnValue JSON — bytes round-trip (base64) ──────────────────────────

    [Test]
    [NonParallelizable]
    public void ColumnValue_Bytes_SerializesAsBase64()
    {
        byte[] data = [0x01, 0x02, 0x03];
        ColumnValue cv = new(data);

        string json = JsonSerializer.Serialize(cv, JsonOpts);

        ColumnValue? back = JsonSerializer.Deserialize<ColumnValue>(json, JsonOpts);
        Assert.IsNotNull(back);
        Assert.AreEqual(ColumnType.Bytes, back!.Type);
        CollectionAssert.AreEqual(data, back.BytesValue);
    }

    // ── ColumnValue JSON — Date IsoValue ─────────────────────────────────────

    [Test]
    [NonParallelizable]
    public void ColumnValue_Date_HasIsoValue()
    {
        DateTime dt = new(2026, 7, 4, 0, 0, 0, DateTimeKind.Utc);
        ColumnValue cv = new(ColumnType.Date, dt.Ticks);

        Assert.AreEqual("2026-07-04", cv.IsoValue);
    }

    [Test]
    [NonParallelizable]
    public void ColumnValue_Date_JsonContainsIsoValue()
    {
        DateTime dt = new(2026, 3, 15, 0, 0, 0, DateTimeKind.Utc);
        ColumnValue cv = new(ColumnType.Date, dt.Ticks);

        string json = JsonSerializer.Serialize(cv, JsonOpts);

        Assert.IsTrue(json.Contains("\"isoValue\""), $"isoValue missing from: {json}");
        Assert.IsTrue(json.Contains("2026-03-15"), $"date string missing from: {json}");
    }

    [Test]
    [NonParallelizable]
    public void ColumnValue_Date_RoundTripsViaJson()
    {
        DateTime dt = new(2026, 1, 31, 0, 0, 0, DateTimeKind.Utc);
        ColumnValue cv = new(ColumnType.Date, dt.Ticks);

        string json = JsonSerializer.Serialize(cv, JsonOpts);
        ColumnValue? back = JsonSerializer.Deserialize<ColumnValue>(json, JsonOpts);

        Assert.IsNotNull(back);
        Assert.AreEqual(ColumnType.Date, back!.Type);
        Assert.AreEqual(dt.Ticks, back.LongValue);
    }

    // ── ColumnValue JSON — DateTime IsoValue ─────────────────────────────────

    [Test]
    [NonParallelizable]
    public void ColumnValue_DateTime_HasIsoValue()
    {
        DateTime dt = new(2026, 3, 15, 10, 20, 30, DateTimeKind.Utc);
        ColumnValue cv = new(ColumnType.DateTime, dt.Ticks);

        Assert.IsNotNull(cv.IsoValue);
        Assert.IsTrue(cv.IsoValue!.StartsWith("2026-03-15T"), $"Expected ISO datetime, got: {cv.IsoValue}");
    }

    [Test]
    [NonParallelizable]
    public void ColumnValue_DateTime_JsonContainsIsoValue()
    {
        DateTime dt = new(2026, 12, 31, 23, 59, 59, DateTimeKind.Utc);
        ColumnValue cv = new(ColumnType.DateTime, dt.Ticks);

        string json = JsonSerializer.Serialize(cv, JsonOpts);

        Assert.IsTrue(json.Contains("\"isoValue\""), $"isoValue missing from: {json}");
        Assert.IsTrue(json.Contains("2026-12-31T"), $"datetime string missing from: {json}");
    }

    [Test]
    [NonParallelizable]
    public void ColumnValue_OtherTypes_NoIsoValue()
    {
        Assert.IsNull(new ColumnValue(ColumnType.Integer64, 42L).IsoValue);
        Assert.IsNull(new ColumnValue(ColumnType.Float64,   3.14).IsoValue);
        Assert.IsNull(new ColumnValue(ColumnType.String,   "hi").IsoValue);
        Assert.IsNull(new ColumnValue(ColumnType.Bool,     true).IsoValue);
    }

    // ── ColumnValue JSON — Array round-trip ──────────────────────────────────

    [Test]
    [NonParallelizable]
    public void ColumnValue_Array_SerializesAsJsonArray()
    {
        ColumnValue arr = ColumnValue.FromArray(ColumnType.Integer64,
        [
            new(ColumnType.Integer64, 10L),
            new(ColumnType.Integer64, 20L),
        ]);

        string json = JsonSerializer.Serialize(arr, JsonOpts);
        ColumnValue? back = JsonSerializer.Deserialize<ColumnValue>(json, JsonOpts);

        Assert.IsNotNull(back);
        Assert.AreEqual(ColumnType.Array, back!.Type);
        Assert.AreEqual(ColumnType.Integer64, back.ArrayElementType);
        Assert.AreEqual(2, back.ArrayValues!.Count);
        Assert.AreEqual(10L, back.ArrayValues[0].LongValue);
        Assert.AreEqual(20L, back.ArrayValues[1].LongValue);
    }

    // ── End-to-end: create table via ColumnInfo (mirrors HTTP controller) ─────

    [Test]
    [NonParallelizable]
    public async Task CreateTable_StringWithMaxLength_EnforcedOnInsert()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTableViaColumnInfo(
        [
            new ColumnInfo("id",  ColumnType.Id,     notNull: true),
            new ColumnInfo("tag", ColumnType.String, maxLength: 4),
        ]);

        await ExecInsert(executor, db, $"INSERT INTO t (id, tag) VALUES (\"{OID}\", 'hi')");

        QueryResultRow row = await SelectOnlyRow(executor, db, "SELECT tag FROM t");
        Assert.AreEqual(ColumnType.String, row.Row["tag"].Type);
        Assert.AreEqual("hi", row.Row["tag"].StrValue);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecInsert(executor, db, $"INSERT INTO t (id, tag) VALUES (\"{OID}\", 'toolong')"));
        Assert.AreEqual(CamusDBErrorCodes.ValueTooLong, ex!.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task CreateTable_ArrayColumn_InsertAndQuery()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTableViaColumnInfo(
        [
            new ColumnInfo("id",   ColumnType.Id,    notNull: true),
            new ColumnInfo("tags", ColumnType.Array, arrayElementType: ColumnType.Integer64),
        ]);

        ColumnValue arrayParam = ColumnValue.FromArray(ColumnType.Integer64,
        [
            new(ColumnType.Integer64, 1L),
            new(ColumnType.Integer64, 2L),
            new(ColumnType.Integer64, 3L),
        ]);

        await ExecInsert(executor, db,
            "INSERT INTO t (id, tags) VALUES (@id, @tags)",
            parameters: new() { { "@id", new(ColumnType.Id, OID) }, { "@tags", arrayParam } });

        QueryResultRow row = await SelectOnlyRow(executor, db, "SELECT tags FROM t");
        Assert.AreEqual(ColumnType.Array,     row.Row["tags"].Type);
        Assert.AreEqual(ColumnType.Integer64, row.Row["tags"].ArrayElementType);
        Assert.AreEqual(3, row.Row["tags"].ArrayValues!.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task CreateTable_DateColumn_InsertAndQuery()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTableViaColumnInfo(
        [
            new ColumnInfo("id", ColumnType.Id,   notNull: true),
            new ColumnInfo("d",  ColumnType.Date),
        ]);

        await ExecInsert(executor, db, $"INSERT INTO t (id, d) VALUES (\"{OID}\", '2026-07-04')");

        QueryResultRow row = await SelectOnlyRow(executor, db, "SELECT d FROM t");
        Assert.AreEqual(ColumnType.Date, row.Row["d"].Type);
        DateTime decoded = new(row.Row["d"].LongValue, DateTimeKind.Utc);
        Assert.AreEqual(2026, decoded.Year);
        Assert.AreEqual(7,    decoded.Month);
        Assert.AreEqual(4,    decoded.Day);

        Assert.AreEqual("2026-07-04", row.Row["d"].IsoValue);
    }

    [Test]
    [NonParallelizable]
    public async Task CreateTable_DateTimeColumn_InsertAndQuery()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTableViaColumnInfo(
        [
            new ColumnInfo("id", ColumnType.Id,       notNull: true),
            new ColumnInfo("ts", ColumnType.DateTime),
        ]);

        await ExecInsert(executor, db, $"INSERT INTO t (id, ts) VALUES (\"{OID}\", '2026-03-15T10:20:30Z')");

        QueryResultRow row = await SelectOnlyRow(executor, db, "SELECT ts FROM t");
        Assert.AreEqual(ColumnType.DateTime, row.Row["ts"].Type);
        DateTime decoded = new(row.Row["ts"].LongValue, DateTimeKind.Utc);
        Assert.AreEqual(10, decoded.Hour);
        Assert.AreEqual(20, decoded.Minute);
        Assert.AreEqual(30, decoded.Second);

        Assert.IsNotNull(row.Row["ts"].IsoValue);
        Assert.IsTrue(row.Row["ts"].IsoValue!.StartsWith("2026-03-15T"));
    }

    [Test]
    [NonParallelizable]
    public async Task CreateTable_Float32Column_InsertAndQuery()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTableViaColumnInfo(
        [
            new ColumnInfo("id", ColumnType.Id,      notNull: true),
            new ColumnInfo("v",  ColumnType.Float32),
        ]);

        await ExecInsert(executor, db, $"INSERT INTO t (id, v) VALUES (\"{OID}\", 2.5)");

        QueryResultRow row = await SelectOnlyRow(executor, db, "SELECT v FROM t");
        Assert.AreEqual(ColumnType.Float32, row.Row["v"].Type);
        Assert.That((float)row.Row["v"].FloatValue, Is.EqualTo(2.5f).Within(1e-5f));
    }

    [Test]
    [NonParallelizable]
    public async Task CreateTable_BytesColumn_InsertAndQuery()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTableViaColumnInfo(
        [
            new ColumnInfo("id", ColumnType.Id,    notNull: true),
            new ColumnInfo("b",  ColumnType.Bytes),
        ]);

        await ExecInsert(executor, db, $"INSERT INTO t (id, b) VALUES (\"{OID}\", '0x0102')");

        QueryResultRow row = await SelectOnlyRow(executor, db, "SELECT b FROM t");
        Assert.AreEqual(ColumnType.Bytes, row.Row["b"].Type);
        CollectionAssert.AreEqual(new byte[] { 0x01, 0x02 }, row.Row["b"].BytesValue);
    }
}
