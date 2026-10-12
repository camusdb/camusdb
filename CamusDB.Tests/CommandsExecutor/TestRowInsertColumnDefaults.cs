/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

using CamusDB.App.Controllers;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The typed row insert (<c>CommandExecutor.Insert</c>, reached by REST <c>/insert</c> and gRPC
/// <c>InsertRow</c>) gives a column the row leaves out exactly what SQL <c>INSERT</c> gives a column
/// its column list leaves out. It used to write NULL for every omitted column, and refuse a
/// <c>NOT NULL DEFAULT</c> column outright, while <c>INSERT INTO t (id) VALUES (…)</c> applied the
/// defaults.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestRowInsertColumnDefaults : BaseTest
{
    private const string DefaultsTableDdl =
        "CREATE TABLE t (id int64 PRIMARY KEY, k int64 NOT NULL DEFAULT(7), s string DEFAULT('x'), " +
        "u uuid DEFAULT(gen_uuid_v7()), o oid DEFAULT(gen_id()), ts datetime DEFAULT(now()), " +
        "no int64 DEFAULT(nextval('s1')), ser serial, ga int64 GENERATED ALWAYS AS IDENTITY, plain string)";

    private ILogger<ICamusDB> Logger => logger;

    private static async Task DdlAsync(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
    }

    private static async Task<List<QueryResultRow>> QueryAsync(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task InsertRowAsync(CommandExecutor executor, string dbname, Dictionary<string, ColumnValue> row)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(tx, dbname, "t", [row]));
        await db.Transactions.CommitAsync(tx);
    }

    private async Task<(string dbname, CommandExecutor executor)> SetupAsync()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await DdlAsync(executor, dbname, "CREATE SEQUENCE s1");
        await DdlAsync(executor, dbname, DefaultsTableDdl);
        return (dbname, executor);
    }

    private static Dictionary<string, ColumnValue> Row(long id) => new() { { "id", new ColumnValue(ColumnType.Integer64, id) } };

    private static void AssertDefaulted(QueryResultRow row, long sequenceValue)
    {
        Assert.AreEqual(7L, row.Row["k"].LongValue, "constant default on a NOT NULL column");
        Assert.AreEqual("x", row.Row["s"].StrValue, "constant default");
        Assert.AreEqual(ColumnType.Uuid, row.Row["u"].Type, "gen_uuid_v7() default");
        Assert.AreEqual(ColumnType.Id, row.Row["o"].Type, "gen_id() default");
        Assert.AreEqual(ColumnType.DateTime, row.Row["ts"].Type, "now() default");
        Assert.AreEqual(sequenceValue, row.Row["no"].LongValue, "nextval default");
        Assert.AreEqual(sequenceValue, row.Row["ser"].LongValue, "serial");
        Assert.AreEqual(sequenceValue, row.Row["ga"].LongValue, "GENERATED ALWAYS AS IDENTITY");
        Assert.AreEqual(ColumnType.Null, row.Row["plain"].Type, "a column with no default stays NULL");
    }

    [Test]
    public async Task TypedInsert_AppliesEveryKindOfDefault()
    {
        (string dbname, CommandExecutor executor) = await SetupAsync();

        await InsertRowAsync(executor, dbname, Row(1));
        await InsertRowAsync(executor, dbname, Row(2));

        List<QueryResultRow> rows = await QueryAsync(executor, dbname, "SELECT * FROM t ORDER BY id");

        Assert.AreEqual(2, rows.Count);
        AssertDefaulted(rows[0], 1);
        AssertDefaulted(rows[1], 2);
        Assert.AreNotEqual(rows[0].Row["u"].UuidValue, rows[1].Row["u"].UuidValue, "a function default is evaluated per row");
    }

    /// <summary>
    /// The typed insert and SQL INSERT draw from the same sequences, so interleaving them hands out
    /// consecutive values with no duplicate.
    /// </summary>
    [Test]
    public async Task TypedInsert_AndSqlInsert_ShareTheSequences()
    {
        (string dbname, CommandExecutor executor) = await SetupAsync();

        await InsertRowAsync(executor, dbname, Row(1));

        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "INSERT INTO t (id) VALUES (2)", null));
        await db.Transactions.CommitAsync(tx);

        await InsertRowAsync(executor, dbname, Row(3));

        List<QueryResultRow> rows = await QueryAsync(executor, dbname, "SELECT * FROM t ORDER BY id");

        for (int i = 0; i < 3; i++)
            AssertDefaulted(rows[i], i + 1);
    }

    /// <summary>
    /// A supplied value wins over the default, including an explicit NULL, which is a value and not an
    /// omission; a BY DEFAULT identity accepts a supplied value; and a column supplied under a
    /// different case is not defaulted a second time.
    /// </summary>
    [Test]
    public async Task TypedInsert_SuppliedValuesWin()
    {
        (string dbname, CommandExecutor executor) = await SetupAsync();

        Dictionary<string, ColumnValue> row = Row(1);
        row["K"] = new ColumnValue(ColumnType.Integer64, 42L);
        row["s"] = ColumnValue.Null;
        row["ser"] = new ColumnValue(ColumnType.Integer64, 100L);

        await InsertRowAsync(executor, dbname, row);

        Assert.AreEqual(4, row.Count, "the caller's row must not be modified: a retry replays it");

        QueryResultRow stored = (await QueryAsync(executor, dbname, "SELECT * FROM t"))[0];

        Assert.AreEqual(42L, stored.Row["k"].LongValue);
        Assert.AreEqual(ColumnType.Null, stored.Row["s"].Type, "an explicit NULL is written as NULL");
        Assert.AreEqual(100L, stored.Row["ser"].LongValue);
        Assert.AreEqual(1L, stored.Row["ga"].LongValue);
    }

    /// <summary>
    /// <c>GENERATED ALWAYS AS IDENTITY</c> refuses a supplied value on this path as INSERT does.
    /// </summary>
    [Test]
    public async Task TypedInsert_RefusesAValueForAnAlwaysIdentity()
    {
        (string dbname, CommandExecutor executor) = await SetupAsync();

        Dictionary<string, ColumnValue> row = Row(1);
        row["ga"] = new ColumnValue(ColumnType.Integer64, 5L);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => InsertRowAsync(executor, dbname, row))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("GENERATED ALWAYS AS IDENTITY", ex.Message);
    }

    /// <summary>
    /// The REST route the TypeScript driver's <c>client.insert(table, values)</c> calls.
    /// </summary>
    [Test]
    public async Task RestInsert_AppliesDefaults()
    {
        (string dbname, CommandExecutor executor) = await SetupAsync();

        byte[] body = Encoding.UTF8.GetBytes(
            $"{{\"databaseName\":\"{dbname}\",\"tableName\":\"t\",\"values\":{{\"id\":{{\"type\":2,\"longValue\":1}}}}}}");

        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(body);
        http.Request.ContentLength = body.Length;

        InsertController controller = new(executor, new HttpTransactionCoordinator(executor), Logger, Options)
        {
            ControllerContext = new ControllerContext { HttpContext = http }
        };

        JsonResult result = await controller.Insert();
        InsertResponse response = (InsertResponse)result.Value!;

        Assert.AreEqual("ok", response.Status, JsonSerializer.Serialize(response));

        AssertDefaulted((await QueryAsync(executor, dbname, "SELECT * FROM t"))[0], 1);
    }
}
