
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using System.IO;
using System.Text;
using System.Text.Json;

using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

using NUnit.Framework;

using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using ProtoQueryStreamMessage = CamusDB.Grpc.QueryStreamMessage;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A select list may name the same output column twice — <c>SELECT a.id, b.id</c> is what an ORM
/// emits when it projects two entities, and it then reads the result by ordinal. Every item must
/// return the value of the column it names; the items must not collapse onto one row key.
///
/// <para>Each query is read the way a client reads it: through the declared column schema and the
/// positional encoder the transports use. Reading the row dictionary by name would not show the
/// defect, because a name can address only one of the two cells.</para>
/// </summary>
public sealed class TestDuplicateOutputColumnNames : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE users (id int64 primary key, name string(32) not null, updated int64 not null)");
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE sessions (id int64 primary key, usersid int64 not null, device string(32) not null, updated int64 not null)");
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE tokens (id int64 primary key, sessionsid int64 not null, lookup string(32) not null)");
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE pairs (left_id int64 primary key, right_id int64 not null)");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO users (id, name, updated) VALUES (1, 'ann', 1001), (2, 'bob', 1002)");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO sessions (id, usersid, device, updated) VALUES (10, 1, 'phone', 2010), (20, 2, 'tablet', 2020)");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO tokens (id, sessionsid, lookup) VALUES (100, 10, 'tok-a'), (200, 20, 'tok-b')");

        return (dbname, database, executor);
    }

    private static async Task ExecDdl(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<int> ExecNonQuery(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(
            new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
        return result.ModifiedRows;
    }

    /// <summary>Runs a query and returns the declared column names plus the positionally encoded rows.</summary>
    private static async Task<(string[] Columns, List<object?[]> Rows)> ExecPositional(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        QuerySchemaHolder schemaHolder = new();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null),
            schemaOut: schemaHolder);

        List<object?[]> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(CompactRowEncoder.EncodeRow(row.Row, schemaHolder.Schema));

        await database.Transactions.CommitAsync(tx);
        return (schemaHolder.Schema.Select(c => c.Name).ToArray(), rows);
    }

    private const string ThreeWayJoin =
        " FROM tokens AS t" +
        " INNER JOIN sessions AS s ON t.sessionsid = s.id" +
        " INNER JOIN users AS u ON s.usersid = u.id" +
        " WHERE t.lookup = 'tok-a'";

    [Test]
    public async Task JoinReturnsEachSameNamedColumnAtItsOwnOrdinal()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        (string[] columns, List<object?[]> rows) = await ExecPositional(
            database, executor, dbname, "SELECT s.id, u.id" + ThreeWayJoin);

        Assert.AreEqual(new[] { "id", "id" }, columns, "an unaliased column keeps its bare name");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(new object?[] { 10L, 1L }, rows[0]);
    }

    /// <summary>The shape an ORM emits: two whole entities, several colliding names, mixed with unique ones.</summary>
    [Test]
    public async Task JoinReturnsEveryCollidingPairInAWideSelectList()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        (string[] columns, List<object?[]> rows) = await ExecPositional(
            database, executor, dbname,
            "SELECT s.usersid, s.id, s.device, s.updated, u.id, u.name, u.updated, t.id" + ThreeWayJoin);

        Assert.AreEqual(new[] { "usersid", "id", "device", "updated", "id", "name", "updated", "id" }, columns);
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(new object?[] { 1L, 10L, "phone", 2010L, 1L, "ann", 1001L, 100L }, rows[0]);
    }

    [Test]
    public async Task JoinWithOrderByAndLimitKeepsBothValues()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        (_, List<object?[]> rows) = await ExecPositional(
            database, executor, dbname,
            "SELECT s.id, u.id FROM sessions AS s INNER JOIN users AS u ON s.usersid = u.id ORDER BY u.id DESC LIMIT 2");

        Assert.AreEqual(2, rows.Count);
        Assert.AreEqual(new object?[] { 20L, 2L }, rows[0]);
        Assert.AreEqual(new object?[] { 10L, 1L }, rows[1]);
    }

    [Test]
    public async Task DistinctComparesBothSameNamedColumns()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        // Two sessions for one user: u.id repeats, s.id does not. A DISTINCT that saw only one of the
        // two cells would fold these rows into one.
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO sessions (id, usersid, device, updated) VALUES (11, 1, 'laptop', 2011)");

        (_, List<object?[]> rows) = await ExecPositional(
            database, executor, dbname,
            "SELECT DISTINCT u.id, s.id FROM sessions AS s INNER JOIN users AS u ON s.usersid = u.id WHERE u.id = 1");

        Assert.AreEqual(
            new[] { "1,10", "1,11" },
            rows.Select(r => $"{r[0]},{r[1]}").OrderBy(x => x).ToArray());
    }

    [Test]
    public async Task SingleTableRepeatedColumnAndSharedAlias()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        (string[] columns, List<object?[]> rows) = await ExecPositional(
            database, executor, dbname, "SELECT id, name AS id, id FROM users WHERE id = 2");

        Assert.AreEqual(new[] { "id", "id", "id" }, columns);
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(new object?[] { 2L, "bob", 2L }, rows[0]);
    }

    [Test]
    public async Task GroupedJoinReturnsBothGroupingColumns()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        (_, List<object?[]> rows) = await ExecPositional(
            database, executor, dbname,
            "SELECT s.id, u.id, COUNT(*) FROM sessions AS s INNER JOIN users AS u ON s.usersid = u.id" +
            " GROUP BY s.id, u.id ORDER BY s.id");

        Assert.AreEqual(2, rows.Count);
        Assert.AreEqual(new object?[] { 10L, 1L, 1L }, rows[0]);
        Assert.AreEqual(new object?[] { 20L, 2L, 1L }, rows[1]);
    }

    [Test]
    public async Task FromlessSelectKeepsBothSameNamedItems()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        (string[] columns, List<object?[]> rows) = await ExecPositional(
            database, executor, dbname, "SELECT 1 AS x, 2 AS x");

        Assert.AreEqual(new[] { "x", "x" }, columns);
        Assert.AreEqual(new object?[] { 1L, 2L }, rows[0]);
    }

    /// <summary>INSERT … SELECT maps the source to the target by position, so it must not lose a cell either.</summary>
    [Test]
    public async Task InsertSelectMapsSameNamedSourceColumnsByPosition()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        int inserted = await ExecNonQuery(database, executor, dbname,
            "INSERT INTO pairs (left_id, right_id)" +
            " SELECT s.id, u.id FROM sessions AS s INNER JOIN users AS u ON s.usersid = u.id");
        Assert.AreEqual(2, inserted);

        (_, List<object?[]> rows) = await ExecPositional(
            database, executor, dbname, "SELECT left_id, right_id FROM pairs ORDER BY left_id");

        Assert.AreEqual(new object?[] { 10L, 1L }, rows[0]);
        Assert.AreEqual(new object?[] { 20L, 2L }, rows[1]);
    }

    /// <summary>
    /// A <c>*</c> and an explicit item write into one row, so an item named like an expanded column
    /// must not share its cell — in either order.
    /// </summary>
    [Test]
    public async Task StarAndAnItemNamedLikeAnExpandedColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        (string[] columns, List<object?[]> rows) = await ExecPositional(
            database, executor, dbname, "SELECT name AS id, * FROM users WHERE id = 2");

        Assert.AreEqual(new[] { "id", "id", "name", "updated" }, columns);
        Assert.AreEqual(new object?[] { "bob", 2L, "bob", 1002L }, rows[0]);

        (columns, rows) = await ExecPositional(
            database, executor, dbname, "SELECT *, name AS id FROM users WHERE id = 2");

        Assert.AreEqual(new[] { "id", "name", "updated", "id" }, columns);
        Assert.AreEqual(new object?[] { 2L, "bob", 1002L, "bob" }, rows[0]);
    }

    [Test]
    public async Task StarOverAJoinPlusAQualifiedColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        (string[] columns, List<object?[]> rows) = await ExecPositional(
            database, executor, dbname,
            "SELECT u.id, *, s.id FROM sessions AS s INNER JOIN users AS u ON s.usersid = u.id WHERE s.id = 20");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("id", columns[0]);
        Assert.AreEqual("id", columns[^1]);
        Assert.AreEqual(2L, rows[0][0]);
        Assert.AreEqual(20L, rows[0][^1]);
        Assert.AreEqual(20L, rows[0][System.Array.IndexOf(columns, "s.id")]);
        Assert.AreEqual(2L, rows[0][System.Array.IndexOf(columns, "u.id")]);
    }

    /// <summary>Row keys compare case-insensitively, so names that differ only in case collide too.</summary>
    [Test]
    public async Task NamesThatDifferOnlyInCaseDoNotCollapse()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        (_, List<object?[]> rows) = await ExecPositional(
            database, executor, dbname,
            "SELECT s.id AS Total, u.id AS total FROM sessions AS s INNER JOIN users AS u ON s.usersid = u.id WHERE s.id = 10");

        Assert.AreEqual(new object?[] { 10L, 1L }, rows[0]);
    }

    /// <summary>
    /// A derived table may declare two columns with one name as long as nothing refers to that name
    /// ambiguously; the outer <c>*</c> must carry both cells through.
    /// </summary>
    [Test]
    public async Task DerivedTableCarriesBothSameNamedColumnsThroughAnOuterStar()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        (string[] columns, List<object?[]> rows) = await ExecPositional(
            database, executor, dbname,
            "SELECT * FROM (SELECT s.id, u.id, u.name FROM sessions AS s INNER JOIN users AS u ON s.usersid = u.id) AS d" +
            " WHERE d.name = 'bob'");

        Assert.AreEqual(new[] { "id", "id", "name" }, columns);
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(new object?[] { 20L, 2L, "bob" }, rows[0]);
    }

    /// <summary>HAVING reads an aggregate's cell out of the grouped row; it must find the right one of two same-named items.</summary>
    [Test]
    public async Task HavingReadsTheAggregateBehindASharedAlias()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO sessions (id, usersid, device, updated) VALUES (11, 1, 'laptop', 2011)");

        (_, List<object?[]> rows) = await ExecPositional(
            database, executor, dbname,
            "SELECT usersid AS n, COUNT(*) AS n FROM sessions GROUP BY usersid HAVING COUNT(*) > 1");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(new object?[] { 1L, 2L }, rows[0]);
    }

    /// <summary>The second execution reuses the cached bound statement and ticket state; it must agree with the first.</summary>
    [Test]
    public async Task RepeatedExecutionReturnsTheSameValues()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        for (int run = 0; run < 3; run++)
        {
            (_, List<object?[]> rows) = await ExecPositional(
                database, executor, dbname, "SELECT s.id, u.id" + ThreeWayJoin);

            Assert.AreEqual(new object?[] { 10L, 1L }, rows[0], $"run {run}");
        }
    }

    /// <summary>
    /// A query with no colliding names keeps its row keys equal to its column names, so in-process
    /// readers that index a row by name are unaffected.
    /// </summary>
    [Test]
    public async Task RowKeysEqualNamesWhenNothingCollides()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction tx = await database.Transactions.BeginAsync();
        QuerySchemaHolder schemaHolder = new();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: tx, database: dbname,
                sql: "SELECT s.id, u.name, s.id + 1, u.id AS userid FROM sessions AS s INNER JOIN users AS u ON s.usersid = u.id WHERE s.id = 10",
                parameters: null),
            schemaOut: schemaHolder);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);

        foreach (DerivedColumnSchema column in schemaHolder.Schema)
            Assert.AreEqual(column.Name, column.RowKey);

        Assert.AreEqual(10L, rows[0].Row["id"].LongValue);
        Assert.AreEqual(1L, rows[0].Row["userid"].LongValue);
    }

    /// <summary>
    /// A <c>*</c> beside another item still expands to every column. The scan used to decode only the
    /// columns the other items named, so the rest of the expansion came back NULL.
    /// </summary>
    [Test]
    public async Task StarBesideAnotherItemReturnsEveryColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        (string[] columns, List<object?[]> rows) = await ExecPositional(
            database, executor, dbname, "SELECT name AS label, * FROM users WHERE id = 2");

        Assert.AreEqual(new[] { "label", "id", "name", "updated" }, columns);
        Assert.AreEqual(new object?[] { "bob", 2L, "bob", 1002L }, rows[0]);

        (columns, rows) = await ExecPositional(
            database, executor, dbname,
            "SELECT u.name AS label, * FROM sessions AS s INNER JOIN users AS u ON s.usersid = u.id WHERE s.id = 20");

        Assert.AreEqual(
            new[] { "label", "s.id", "s.usersid", "s.device", "s.updated", "u.id", "u.name", "u.updated" }, columns);
        Assert.AreEqual(new object?[] { "bob", 20L, 2L, "tablet", 2020L, 2L, "bob", 1002L }, rows[0]);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Transports: the readers a client actually sits behind.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task RestEndpointReturnsBothValuesPositionally()
    {
        (string dbname, _, CommandExecutor executor) = await SetupAsync();

        byte[] body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(
            new { databaseName = dbname, sql = "SELECT s.id, u.id" + ThreeWayJoin }));
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(body);
        http.Request.ContentLength = body.Length;
        http.Request.IsHttps = true;

        ExecuteSQLController controller = new(
            executor, new HttpTransactionCoordinator(executor), new PreparedStatementRegistry(Options), logger, Options)
        {
            ControllerContext = new ControllerContext { HttpContext = http }
        };

        JsonResult result = await controller.ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status);
        Assert.AreEqual(new[] { "id", "id" }, response.Columns.Select(c => c.Name).ToArray());

        // Serialization is where the row set resolves each cell, so the wire JSON is the assertion.
        string json = JsonSerializer.Serialize(response);
        StringAssert.Contains("[[10,1]]", json);
    }

    [Test]
    public async Task GrpcRowBuilderReturnsBothValuesPositionally()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction tx = await database.Transactions.BeginAsync();
        QuerySchemaHolder schemaHolder = new();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: tx, database: dbname, sql: "SELECT s.id, u.id" + ThreeWayJoin, parameters: null),
            schemaOut: schemaHolder);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);

        ProtoQueryStreamMessage schemaMessage = CamusSqlService.BuildSchema(schemaHolder.Schema);
        Assert.AreEqual(new[] { "id", "id" }, schemaMessage.Schema.Columns.Select(c => c.Name).ToArray());

        ProtoQueryStreamMessage rowMessage = CamusSqlService.BuildRow(rows[0].Row, schemaHolder.Schema, new ResultRowBinder());
        Assert.AreEqual(2, rowMessage.Row.Values.Count);
        Assert.AreEqual(10L, rowMessage.Row.Values[0].Int64Value);
        Assert.AreEqual(1L, rowMessage.Row.Values[1].Int64Value);
    }
}
