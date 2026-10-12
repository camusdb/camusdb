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
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>INSERT … OVERRIDING { SYSTEM | USER } VALUE</c>, with PostgreSQL's meaning. A dump restores a
/// <c>GENERATED ALWAYS AS IDENTITY</c> column's values through <c>OVERRIDING SYSTEM VALUE</c>; without
/// it the restored table refuses every dumped row. <c>OVERRIDING USER VALUE</c> discards the values
/// supplied for identity columns and draws from their sequences instead.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestInsertOverriding : BaseTest
{
    private const string TableDdl =
        "CREATE TABLE c (id int64 PRIMARY KEY, g int64 GENERATED ALWAYS AS IDENTITY, h serial, " +
        "no int64 DEFAULT(nextval('s1')))";

    private ILogger<ICamusDB> Logger => logger;

    private static async Task DdlAsync(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
    }

    private static async Task<ExecuteNonSQLResult> NonQueryAsync(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            await db.Transactions.CommitAsync(tx);
            return result;
        }
        catch
        {
            await db.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    private static async Task<Dictionary<long, QueryResultRow>> RowsByIdAsync(CommandExecutor executor, string dbname)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, dbname, "SELECT id, g, h, no FROM c", null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db.Transactions.CommitAsync(tx);
        return rows.ToDictionary(r => r.Row["id"].LongValue);
    }

    private async Task<(string dbname, CommandExecutor executor)> SetupAsync()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await DdlAsync(executor, dbname, "CREATE SEQUENCE s1");
        await DdlAsync(executor, dbname, TableDdl);
        return (dbname, executor);
    }

    // ── Parsing ─────────────────────────────────────────────────────────────

    [TestCase("INSERT INTO c (id, g) OVERRIDING SYSTEM VALUE VALUES (1, 2)", NodeType.Insert, "system")]
    [TestCase("INSERT INTO c OVERRIDING SYSTEM VALUE VALUES (1, 2, 3, 4)", NodeType.Insert, "system")]
    [TestCase("insert into c (id, g) overriding user value values (1, 2)", NodeType.Insert, "user")]
    [TestCase("INSERT INTO c (id, g) OVERRIDING SYSTEM VALUE SELECT id, g FROM d", NodeType.InsertSelect, "system")]
    [TestCase("INSERT INTO c OVERRIDING USER VALUE SELECT * FROM d", NodeType.InsertSelect, "user")]
    [TestCase("INSERT INTO c (id, g) VALUES (1, 2)", NodeType.Insert, null)]
    [TestCase("INSERT INTO c (id, g) SELECT id, g FROM d", NodeType.InsertSelect, null)]
    public void Parse_OverridingClause(string sql, NodeType expectedType, string? expectedKind)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);

        Assert.AreEqual(expectedType, ast.nodeType);
        Assert.AreEqual(expectedKind, ast.yytext);
    }

    [Test]
    public void Parse_OverridingClause_RejectsOtherWords()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => SQLParserProcessor.Parse("INSERT INTO c (id) OVERRIDING SESSION VALUE VALUES (1)"))!;
        StringAssert.Contains("OVERRIDING { SYSTEM | USER } VALUE", ex.Message);

        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("INSERT INTO c (id) OVERRIDING USER VALUES (1)"));
    }

    /// <summary>The words of the clause stay usable as identifiers.</summary>
    [Test]
    public async Task ClauseWords_AreNotReserved()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await DdlAsync(executor, dbname, "CREATE TABLE overriding (id int64 PRIMARY KEY, system int64, value int64)");
        await NonQueryAsync(executor, dbname, "INSERT INTO overriding (id, system, value) VALUES (1, 2, 3)");
    }

    // ── OVERRIDING SYSTEM VALUE ────────────────────────────────────────────

    [Test]
    public async Task WithoutTheClause_AnAlwaysIdentityValueIsRefused()
    {
        (string dbname, CommandExecutor executor) = await SetupAsync();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            () => NonQueryAsync(executor, dbname, "INSERT INTO c (id, g) VALUES (1, 100)"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
    }

    /// <summary>
    /// The dump case: the supplied values are written, the sequences are not advanced by them, and the
    /// next omitted value still comes from the sequence.
    /// </summary>
    [Test]
    public async Task SystemValue_WritesSuppliedValues_AndDoesNotAdvanceTheSequence()
    {
        (string dbname, CommandExecutor executor) = await SetupAsync();

        await NonQueryAsync(executor, dbname,
            "INSERT INTO c (id, g, h, no) OVERRIDING SYSTEM VALUE VALUES (1, 100, 200, 300), (2, 101, 201, 301)");
        await NonQueryAsync(executor, dbname, "INSERT INTO c OVERRIDING SYSTEM VALUE VALUES (3, 102, 202, 302)");
        await NonQueryAsync(executor, dbname, "INSERT INTO c (id) VALUES (4)");

        Dictionary<long, QueryResultRow> rows = await RowsByIdAsync(executor, dbname);

        Assert.AreEqual(100L, rows[1].Row["g"].LongValue);
        Assert.AreEqual(200L, rows[1].Row["h"].LongValue);
        Assert.AreEqual(301L, rows[2].Row["no"].LongValue);
        Assert.AreEqual(102L, rows[3].Row["g"].LongValue, "the implicit all-columns form");

        Assert.AreEqual(1L, rows[4].Row["g"].LongValue, "a supplied value does not advance the sequence");
        Assert.AreEqual(1L, rows[4].Row["h"].LongValue);
        Assert.AreEqual(1L, rows[4].Row["no"].LongValue);
    }

    [Test]
    public async Task SystemValue_WorksForInsertSelect()
    {
        (string dbname, CommandExecutor executor) = await SetupAsync();

        await DdlAsync(executor, dbname, "CREATE TABLE src (id int64 PRIMARY KEY, g int64)");
        await NonQueryAsync(executor, dbname, "INSERT INTO src (id, g) VALUES (1, 50), (2, 51)");

        CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(
            () => NonQueryAsync(executor, dbname, "INSERT INTO c (id, g) SELECT id, g FROM src"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, refused.Code);

        await NonQueryAsync(executor, dbname, "INSERT INTO c (id, g) OVERRIDING SYSTEM VALUE SELECT id, g FROM src");

        Dictionary<long, QueryResultRow> rows = await RowsByIdAsync(executor, dbname);
        Assert.AreEqual(50L, rows[1].Row["g"].LongValue);
        Assert.AreEqual(51L, rows[2].Row["g"].LongValue);
        Assert.AreEqual(1L, rows[1].Row["h"].LongValue, "an omitted identity column still draws");
    }

    [Test]
    public async Task SystemValue_WithReturning()
    {
        (string dbname, CommandExecutor executor) = await SetupAsync();

        ExecuteNonSQLResult result = await NonQueryAsync(executor, dbname,
            "INSERT INTO c (id, g) OVERRIDING SYSTEM VALUE VALUES (1, 77) RETURNING g, h");

        Assert.AreEqual(1, result.ModifiedRows);
        Assert.IsNotNull(result.ReturningRows);
        Assert.AreEqual(77L, result.ReturningRows![0].Row["g"].LongValue);
        Assert.AreEqual(1L, result.ReturningRows![0].Row["h"].LongValue);
    }

    /// <summary>Several overriding statements inside one explicit transaction, then a rollback and a commit.</summary>
    [Test]
    public async Task SystemValue_InsideAnExplicitTransaction()
    {
        (string dbname, CommandExecutor executor) = await SetupAsync();
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);

        KvTransaction rolledBack = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(rolledBack, dbname,
            "INSERT INTO c (id, g) OVERRIDING SYSTEM VALUE VALUES (9, 900)", null));
        await db.Transactions.RollbackAsync(rolledBack);

        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
            "INSERT INTO c (id, g) OVERRIDING SYSTEM VALUE VALUES (1, 10)", null));
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
            "INSERT INTO c (id, g) OVERRIDING SYSTEM VALUE VALUES (2, 20)", null));
        await db.Transactions.CommitAsync(tx);

        Dictionary<long, QueryResultRow> rows = await RowsByIdAsync(executor, dbname);
        Assert.AreEqual(2, rows.Count);
        Assert.AreEqual(10L, rows[1].Row["g"].LongValue);
        Assert.AreEqual(20L, rows[2].Row["g"].LongValue);
    }

    /// <summary>A prepared statement over REST: the clause survives prepare and binds parameters.</summary>
    [Test]
    public async Task SystemValue_InAPreparedStatement()
    {
        (string dbname, CommandExecutor executor) = await SetupAsync();

        HttpTransactionCoordinator coordinator = new(executor);
        PreparedStatementRegistry registry = new(Options);

        PreparedStatementsController statements = new(executor, coordinator, registry, Logger, Options)
        {
            ControllerContext = Context(new { databaseName = dbname, sql = "INSERT INTO c (id, g) OVERRIDING SYSTEM VALUE VALUES (@id, @g)" })
        };
        PrepareStatementResponse prepared = (PrepareStatementResponse)(await statements.PrepareSQLStatement()).Value!;
        Assert.AreEqual("ok", prepared.Status, prepared.Message);

        ExecuteSQLController sql = new(executor, coordinator, registry, Logger, Options)
        {
            ControllerContext = Context(new
            {
                statementId = prepared.StatementId,
                positionalParameters = new object[]
                {
                    new { type = ColumnType.Integer64, longValue = 1L },
                    new { type = ColumnType.Integer64, longValue = 42L },
                },
            })
        };
        ExecuteNonSQLQueryResponse response = (ExecuteNonSQLQueryResponse)(await sql.ExecuteNonSQLQuery()).Value!;
        Assert.AreEqual("ok", response.Status, response.Message);

        Assert.AreEqual(42L, (await RowsByIdAsync(executor, dbname))[1].Row["g"].LongValue);
    }

    // ── OVERRIDING USER VALUE ──────────────────────────────────────────────

    /// <summary>
    /// The values supplied for identity columns, ALWAYS and BY DEFAULT alike, are discarded and the
    /// sequences supply them; a column that only defaults from <c>nextval</c> is not an identity column
    /// and keeps its supplied value.
    /// </summary>
    [Test]
    public async Task UserValue_IgnoresSuppliedIdentityValues()
    {
        (string dbname, CommandExecutor executor) = await SetupAsync();

        await NonQueryAsync(executor, dbname,
            "INSERT INTO c (id, g, h, no) OVERRIDING USER VALUE VALUES (1, 500, 600, 700), (2, 501, 601, 701)");

        Dictionary<long, QueryResultRow> rows = await RowsByIdAsync(executor, dbname);

        Assert.AreEqual(1L, rows[1].Row["g"].LongValue);
        Assert.AreEqual(1L, rows[1].Row["h"].LongValue);
        Assert.AreEqual(700L, rows[1].Row["no"].LongValue, "a nextval default is not an identity column");
        Assert.AreEqual(2L, rows[2].Row["g"].LongValue);
        Assert.AreEqual(2L, rows[2].Row["h"].LongValue);
    }

    [Test]
    public async Task UserValue_WorksForInsertSelect()
    {
        (string dbname, CommandExecutor executor) = await SetupAsync();

        await DdlAsync(executor, dbname, "CREATE TABLE src (id int64 PRIMARY KEY, g int64, h int64)");
        await NonQueryAsync(executor, dbname, "INSERT INTO src (id, g, h) VALUES (1, 50, 60)");

        await NonQueryAsync(executor, dbname, "INSERT INTO c (id, g, h) OVERRIDING USER VALUE SELECT id, g, h FROM src");

        QueryResultRow row = (await RowsByIdAsync(executor, dbname))[1];
        Assert.AreEqual(1L, row.Row["g"].LongValue);
        Assert.AreEqual(1L, row.Row["h"].LongValue);
    }

    private static ControllerContext Context(object body)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body));
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Response.Body = new MemoryStream();
        return new ControllerContext { HttpContext = http };
    }
}
