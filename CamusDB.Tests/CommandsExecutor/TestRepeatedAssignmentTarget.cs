/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
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
/// No statement or row endpoint accepts two values for one column. Before, the values were folded
/// into a map keyed by column and the last one won without a trace: <c>UPDATE t SET v = 1, v = 2</c>
/// reported one modified row and stored 2. Each form now fails with
/// <see cref="CamusDBErrorCodes.DuplicateColumn"/> before anything is written, and each test reads the
/// table back to prove that the row is unchanged.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestRepeatedAssignmentTarget : BaseTest
{
    /// <summary>A database whose table <c>t</c> holds the one row (1, 10).</summary>
    private async Task<(string db, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, "CREATE TABLE t (id int64 PRIMARY KEY, v int64, w int64)", null));
        await database.Transactions.CommitAsync(tx);

        tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO t (id, v, w) VALUES (1, 10, 100)", null));
        await database.Transactions.CommitAsync(tx);

        return (db, database, executor);
    }

    /// <summary>The committed (id, v, w) rows of <c>t</c>, sorted by id.</summary>
    private static async Task<List<(long Id, long V, long W)>> StoredAsync(string db, DatabaseDescriptor database, CommandExecutor executor)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, db, "SELECT id, v, w FROM t", null));
            List<(long, long, long)> rows = [];
            await foreach (QueryResultRow row in cursor)
                rows.Add((row.Row["id"].LongValue, row.Row["v"].LongValue, row.Row["w"].LongValue));
            return rows.OrderBy(r => r.Item1).ToList();
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>Runs <paramref name="sql"/> in its own transaction and asserts the duplicate-column refusal.</summary>
    private static async Task AssertRefusedAsync(string db, DatabaseDescriptor database, CommandExecutor executor, string sql, string column)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null)), sql);

        Assert.AreEqual(CamusDBErrorCodes.DuplicateColumn, ex!.Code, sql);
        StringAssert.Contains(column, ex.Message, sql);

        await database.Transactions.RollbackIfNotCompletedAsync(tx);
    }

    private static readonly List<(long, long, long)> Seeded = [(1, 10, 100)];

    [Test]
    public void DuplicateColumn_MapsToHttp400()
    {
        Assert.AreEqual(400, CamusDBErrorCodes.GetHttpStatus(CamusDBErrorCodes.DuplicateColumn));
    }

    [Test]
    public async Task Update_TwoAssignmentsToOneColumn_IsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        await AssertRefusedAsync(db, database, executor, "UPDATE t SET v = 1, v = 2 WHERE id = 1", "v");
        await AssertRefusedAsync(db, database, executor, "UPDATE t SET v = 1, w = 5, v = 2 WHERE id = 1", "v");

        Assert.AreEqual(Seeded, await StoredAsync(db, database, executor));
    }

    /// <summary>An UPDATE resolves a SET target case-insensitively, so <c>v</c> and <c>V</c> are one column.</summary>
    [Test]
    public async Task Update_AssignmentsThatDifferOnlyInCase_AreRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        await AssertRefusedAsync(db, database, executor, "UPDATE t SET v = 1, V = 2 WHERE id = 1", "V");

        Assert.AreEqual(Seeded, await StoredAsync(db, database, executor));
    }

    /// <summary>One assignment to each of two columns is still accepted.</summary>
    [Test]
    public async Task Update_OneAssignmentPerColumn_IsAccepted()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "UPDATE t SET v = 2, w = 3 WHERE id = 1", null));
        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(new List<(long, long, long)> { (1, 2, 3) }, await StoredAsync(db, database, executor));
    }

    [Test]
    public async Task Insert_ColumnListNamesOneColumnTwice_IsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        await AssertRefusedAsync(db, database, executor, "INSERT INTO t (id, v, v) VALUES (2, 20, 21)", "v");

        Assert.AreEqual(Seeded, await StoredAsync(db, database, executor));
    }

    /// <summary>The INSERT column list resolves a name case-insensitively, so <c>v</c> and <c>V</c> are one column.</summary>
    [Test]
    public async Task Insert_ColumnListNamesOneColumnInTwoCases_IsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        await AssertRefusedAsync(db, database, executor, "INSERT INTO t (id, v, V) VALUES (2, 20, 21)", "V");
        await AssertRefusedAsync(db, database, executor, "INSERT INTO t (id, v, V) SELECT id + 1, v, w FROM t", "V");

        Assert.AreEqual(Seeded, await StoredAsync(db, database, executor));
    }

    [Test]
    public async Task InsertSelect_ColumnListNamesOneColumnTwice_IsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        await AssertRefusedAsync(db, database, executor, "INSERT INTO t (id, v, v) SELECT id + 1, v, w FROM t", "v");

        Assert.AreEqual(Seeded, await StoredAsync(db, database, executor));
    }

    [Test]
    public async Task OnConflictDoUpdate_TwoAssignmentsToOneColumn_IsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        await AssertRefusedAsync(db, database, executor,
            "INSERT INTO t (id, v, w) VALUES (1, 20, 200) ON CONFLICT (id) DO UPDATE SET v = 1, v = excluded.v", "v");

        // The proposed key is free here, so no conflict happens; the refusal still comes first.
        await AssertRefusedAsync(db, database, executor,
            "INSERT INTO t (id, v, w) VALUES (2, 20, 200) ON CONFLICT (id) DO UPDATE SET w = 1, W = 2", "W");

        Assert.AreEqual(Seeded, await StoredAsync(db, database, executor));
    }

    [Test]
    public async Task Upsert_ColumnListNamesOneColumnTwice_IsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        await AssertRefusedAsync(db, database, executor, "UPSERT INTO t (id, v, v) VALUES (1, 20, 21)", "v");

        Assert.AreEqual(Seeded, await StoredAsync(db, database, executor));
    }

    // ─── JSON row endpoints ─────────────────────────────────────────────────────────────

    /// <summary>A request whose body is the raw JSON text, so a repeated key reaches the controller as sent.</summary>
    private static ControllerContext RawContext(string json)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(json);
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Request.IsHttps = true;
        http.Response.Body = new MemoryStream();
        return new ControllerContext { HttpContext = http };
    }

    /// <summary>The wire form of an INT64 <see cref="ColumnValue"/>.</summary>
    private static string Int64(long value) => $"{{\"type\":{(int)ColumnType.Integer64},\"longValue\":{value}}}";

    private async Task<UpdateResponse> UpdateAsync(CommandExecutor executor, string json)
    {
        JsonResult result = await new UpdateController(executor, new HttpTransactionCoordinator(executor), logger, Options)
            { ControllerContext = RawContext(json) }.Update();
        Assert.AreEqual(400, result.StatusCode, json);
        return (UpdateResponse)result.Value!;
    }

    private async Task<InsertResponse> InsertAsync(CommandExecutor executor, string json)
    {
        JsonResult result = await new InsertController(executor, new HttpTransactionCoordinator(executor), logger, Options)
            { ControllerContext = RawContext(json) }.Insert();
        Assert.AreEqual(400, result.StatusCode, json);
        return (InsertResponse)result.Value!;
    }

    private static string UpdateBody(string db, string values) =>
        $"{{\"databaseName\":\"{db}\",\"tableName\":\"t\",\"values\":{{{values}}}," +
        $"\"filters\":[{{\"columnName\":\"id\",\"op\":\"=\",\"value\":{Int64(1)}}}]}}";

    [Test]
    public async Task UpdateEndpoint_RepeatedJsonKey_IsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        UpdateResponse response = await UpdateAsync(executor, UpdateBody(db, $"\"v\":{Int64(1)},\"v\":{Int64(2)}"));

        Assert.AreEqual("failed", response.Status);
        Assert.AreEqual(CamusDBErrorCodes.DuplicateColumn, response.Code, response.Message);
        Assert.AreEqual(Seeded, await StoredAsync(db, database, executor));
    }

    /// <summary>
    /// <c>v</c> and <c>V</c> are two keys of the JSON object, and both resolve to column <c>v</c>, so the
    /// request still gives that column two values.
    /// </summary>
    [Test]
    public async Task UpdateEndpoint_KeysThatDifferOnlyInCase_AreRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        UpdateResponse response = await UpdateAsync(executor, UpdateBody(db, $"\"v\":{Int64(1)},\"V\":{Int64(2)}"));

        Assert.AreEqual("failed", response.Status);
        Assert.AreEqual(CamusDBErrorCodes.DuplicateColumn, response.Code, response.Message);
        Assert.AreEqual(Seeded, await StoredAsync(db, database, executor));
    }

    [Test]
    public async Task InsertEndpoint_RepeatedJsonKey_IsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        InsertResponse response = await InsertAsync(executor,
            $"{{\"databaseName\":\"{db}\",\"tableName\":\"t\",\"values\":{{\"id\":{Int64(2)},\"v\":{Int64(20)},\"v\":{Int64(21)}}}}}");

        Assert.AreEqual("failed", response.Status);
        Assert.AreEqual(CamusDBErrorCodes.DuplicateColumn, response.Code, response.Message);
        Assert.AreEqual(Seeded, await StoredAsync(db, database, executor));
    }

    [Test]
    public async Task InsertEndpoint_KeysThatDifferOnlyInCase_AreRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        InsertResponse response = await InsertAsync(executor,
            $"{{\"databaseName\":\"{db}\",\"tableName\":\"t\",\"values\":{{\"id\":{Int64(2)},\"v\":{Int64(20)},\"V\":{Int64(21)}}}}}");

        Assert.AreEqual("failed", response.Status);
        Assert.AreEqual(CamusDBErrorCodes.DuplicateColumn, response.Code, response.Message);
        Assert.AreEqual(Seeded, await StoredAsync(db, database, executor));
    }
}
