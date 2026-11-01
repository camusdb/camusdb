/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;
using CamusDB.App.Controllers;
using CamusDB.App.Models;
using CamusDB.App.Services;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The JSON row endpoints <c>/update</c> and <c>/delete</c> follow the same rule as SQL UPDATE and
/// DELETE without WHERE: a request with no filter is refused, so that one request cannot change or
/// delete every row by accident. Each test drives the real controller and reads the table back, so a
/// refusal that still wrote rows fails. In an explicit transaction the refusal ends the transaction,
/// as a refused SQL statement does.
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node per test.
[NonParallelizable]
internal sealed class TestRowEndpointsRequireFilter : BaseTest
{
    private CommandExecutor executor = null!;
    private HttpTransactionCoordinator coordinator = null!;

    private static readonly JsonSerializerOptions WireJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    [SetUp]
    public void SetUpTransports()
    {
        CommandValidator validator = new(Options);
        CatalogsManager catalogs = new(logger);
        executor = new(validator, catalogs, logger, Options,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
        coordinator = new(executor);
    }

    [TearDown]
    public async Task TearDownTransports()
    {
        try { await executor.DisposeAsync(); } catch { }
    }

    private static ControllerContext Context(object body)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body, WireJson));
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Request.IsHttps = true;
        http.Response.Body = new MemoryStream();
        return new ControllerContext { HttpContext = http };
    }

    private async Task<UpdateResponse> UpdateAsync(object body) =>
        (UpdateResponse)(await new UpdateController(executor, coordinator, logger, Options) { ControllerContext = Context(body) }
            .Update()).Value!;

    private async Task<DeleteResponse> DeleteAsync(object body) =>
        (DeleteResponse)(await new DeleteController(executor, coordinator, logger, Options) { ControllerContext = Context(body) }
            .Delete()).Value!;

    /// <summary>The wire form of an INT64 <see cref="ColumnValue"/>.</summary>
    private static object Int64(long value) => new { type = (int)ColumnType.Integer64, longValue = value };

    /// <summary>A database whose <c>items</c> table holds (1, 10), (2, 20) and (3, 30).</summary>
    private async Task<string> SeedAsync()
    {
        string db = "rwf" + Guid.NewGuid().ToString("n")[..8];
        await executor.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, executor);

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, db, "CREATE TABLE items (id INT64 PRIMARY KEY, v INT64)", null));
        await database.Transactions.RollbackIfNotCompletedAsync(ddl);

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db,
            "INSERT INTO items (id, v) VALUES (1, 10), (2, 20), (3, 30)", null));
        await database.Transactions.CommitAsync(tx);
        return db;
    }

    /// <summary>The committed (id, v) pairs of <c>items</c>, sorted by id.</summary>
    private async Task<List<(long Id, long V)>> StoredAsync(string db)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, db, "SELECT id, v FROM items", null));
            List<(long, long)> rows = [];
            await foreach (QueryResultRow row in cursor)
                rows.Add((row.Row["id"].LongValue, row.Row["v"].LongValue));
            return rows.OrderBy(r => r.Item1).ToList();
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static readonly List<(long, long)> Seeded = [(1, 10), (2, 20), (3, 30)];

    [TestCase(false)]
    [TestCase(true)]
    public async Task UpdateWithoutAFilter_IsRefused_AndNoRowChanges(bool emptyList)
    {
        string db = await SeedAsync();

        object body = emptyList
            ? new { databaseName = db, tableName = "items", values = new { v = Int64(0) }, filters = Array.Empty<object>() }
            : new { databaseName = db, tableName = "items", values = new { v = Int64(0) } };
        UpdateResponse response = await UpdateAsync(body);

        Assert.AreEqual("failed", response.Status);
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, response.Code);
        StringAssert.StartsWith("UPDATE requires at least one filter", response.Message);
        StringAssert.Contains("WHERE TRUE", response.Message);
        CollectionAssert.AreEqual(Seeded, await StoredAsync(db));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task DeleteWithoutAFilter_IsRefused_AndNoRowIsDeleted(bool emptyList)
    {
        string db = await SeedAsync();

        object body = emptyList
            ? new { databaseName = db, tableName = "items", filters = Array.Empty<object>() }
            : new { databaseName = db, tableName = "items" };
        DeleteResponse response = await DeleteAsync(body);

        Assert.AreEqual("failed", response.Status);
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, response.Code);
        StringAssert.StartsWith("DELETE requires at least one filter", response.Message);
        StringAssert.Contains("WHERE TRUE", response.Message);
        CollectionAssert.AreEqual(Seeded, await StoredAsync(db));
    }

    [Test]
    public async Task InAnExplicitTransaction_TheRefusalEndsTheTransaction_AndItsEarlierWriteIsNotStored()
    {
        string db = await SeedAsync();
        KvTransaction tx = await coordinator.StartAsync(db);

        UpdateResponse first = await UpdateAsync(new
        {
            txnIdPT = tx.ClientId.L,
            txnIdCounter = tx.ClientId.C,
            databaseName = db,
            tableName = "items",
            values = new { v = Int64(99) },
            filters = new[] { new { columnName = "id", op = "=", value = Int64(1) } },
        });
        Assert.AreEqual("ok", first.Status, first.Message);

        DeleteResponse refused = await DeleteAsync(new
        {
            txnIdPT = tx.ClientId.L,
            txnIdCounter = tx.ClientId.C,
            databaseName = db,
            tableName = "items",
        });
        Assert.AreEqual("failed", refused.Status);
        StringAssert.StartsWith("DELETE requires at least one filter", refused.Message);

        // The transaction is gone, so its earlier update can never be committed.
        Assert.Throws<CamusDBException>(() => coordinator.GetState(tx.ClientId.L, tx.ClientId.C));
        CollectionAssert.AreEqual(Seeded, await StoredAsync(db));
    }

    [Test]
    public async Task WithAFilter_UpdateAndDeleteTouchOnlyTheMatchingRow()
    {
        string db = await SeedAsync();

        UpdateResponse updated = await UpdateAsync(new
        {
            databaseName = db,
            tableName = "items",
            values = new { v = Int64(99) },
            filters = new[] { new { columnName = "id", op = "=", value = Int64(2) } },
        });
        Assert.AreEqual("ok", updated.Status, updated.Message);
        Assert.AreEqual(1, updated.Rows);

        DeleteResponse deleted = await DeleteAsync(new
        {
            databaseName = db,
            tableName = "items",
            filters = new[] { new { columnName = "id", op = "=", value = Int64(3) } },
        });
        Assert.AreEqual("ok", deleted.Status, deleted.Message);
        Assert.AreEqual(1, deleted.Rows);

        CollectionAssert.AreEqual(new List<(long, long)> { (1, 10), (2, 99) }, await StoredAsync(db));
    }
}
