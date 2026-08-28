/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.App.Controllers;
using CamusDB.App.Models;
using CamusDB.App.Services;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A Kahuna read that cannot currently be served — a scan page whose retry budget expires on an
/// unresolved foreign write intent — must reach the caller as the retryable
/// <see cref="CamusDBErrorCodes.TransactionMustRetry"/> carrying the server's message (which names
/// the failed range), never as the generic internal error. A read is idempotent, so retrying is
/// always safe; buried under an internal error, a reconciliation-style caller abandons its retry
/// budget on a condition that resolves in seconds, and the range that failed is only recorded in
/// the node log. This drives the real chain end to end: a staged foreign intent with no commit
/// timestamp wedges a page of the database-registry key space, the configured scan budget fails
/// the scan loudly, and the query surface translates it.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestKahunaReadErrorMapping : BaseTest
{
    private CommandExecutor executor = null!;
    private HttpTransactionCoordinator coordinator = null!;
    private PreparedStatementRegistry statements = null!;

    /// <summary>Small budget so the wedged page fails in about a second instead of the 5 s default.</summary>
    private const int ScanBudgetMs = 1_200;

    protected override void ConfigureNodeOptions(Kahuna.EmbeddedKahunaOptions options)
        => options.ScanPageRetryBudgetMs = ScanBudgetMs;

    [SetUp]
    public void SetUpRest()
    {
        CommandValidator validator = new(Options);
        CatalogsManager catalogs = new(logger);
        executor = new(validator, catalogs, logger, Options,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
        coordinator = new(executor);
        statements = new(Options);
    }

    [TearDown]
    public async Task TearDownRest()
    {
        try { await executor.DisposeAsync(); } catch { }
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

    private ExecuteSQLController Sql(object body) =>
        new(executor, coordinator, statements, logger, Options) { ControllerContext = Context(body) };

    [Test]
    public async Task WedgedTableScan_SurfacesAsRetryableWithTheRangeNamed()
    {
        CancellationToken ct = CancellationToken.None;

        string dbName = "db" + Guid.NewGuid().ToString("n");
        await Sql(new { sql = $"CREATE DATABASE {dbName}" }).ExecuteSQLDDL();
        await Sql(new { databaseName = dbName, sql = "CREATE TABLE robots (id oid PRIMARY KEY, name string(64))" }).ExecuteSQLDDL();
        await Sql(new { databaseName = dbName, sql = "INSERT INTO robots (id, name) VALUES (gen_id(), 'r1')" }).ExecuteNonSQLQuery();

        // The table's row key space, learned from the opened table rather than assumed from the
        // registry's id sequence.
        CamusDB.Core.CommandsExecutor.Models.DatabaseDescriptor db =
            await executor.OpenDatabase(dbName).ConfigureAwait(false);
        CamusDB.Core.CommandsExecutor.Models.TableDescriptor table =
            await db.TableDescriptors["robots"];
        string wedgeKey = table.Store.RowKeySpace + "/zzzzzzzzzzzzzzzzzzzzzzzz";

        // A foreign transaction stages a write inside the row key space and never decides. The
        // staged intent carries no commit timestamp, so a scan page containing the key cannot
        // prove the write lands outside its snapshot and answers transient until the scan budget
        // fails it loudly — the production wedge shape, on the exact path reconciliation reads.
        HLCTimestamp foreignTx = TestNode!.Raft.HybridLogicalClock.SendOrLocalEvent(
            TestNode.Raft.GetLocalNodeId());
        (KeyValueResponseType staged, _, _) = await TestNode.Kahuna.LocateAndTrySetKeyValue(
            foreignTx, wedgeKey, Encoding.UTF8.GetBytes("staged"), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.That(staged, Is.EqualTo(KeyValueResponseType.Set));

        JsonResult result = await Sql(new { databaseName = dbName, sql = "SELECT id, name FROM robots" }).ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.Multiple(() =>
        {
            Assert.That(response.Status, Is.EqualTo("failed"));
            Assert.That(response.Code, Is.EqualTo(CamusDBErrorCodes.TransactionMustRetry),
                $"a failed Kahuna read must surface retryably, got {response.Code}: {response.Message}");
            Assert.That(response.Message, Does.Contain("did not settle"),
                "the server's message, which names the failed range, must reach the caller");
            Assert.That(response.Message, Does.Contain($"{ScanBudgetMs} ms"),
                "the configured budget must be the one that fired");
        });
    }
}
