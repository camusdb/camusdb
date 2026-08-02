
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Grpc;
using CamusDB.App.Grpc;
using CamusDB.App.Services;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// Verifies that the gRPC query paths report the same cache verdict the REST envelope carries.
/// Both transports read the verdict from the <see cref="CacheMetadataHolder"/> the query executor
/// fills as the cursor drains, so on gRPC it can only travel <b>after</b> the rows: the unary
/// <c>ExecuteQuery</c> stream appends a trailing <c>CacheMetadata</c> message and the multiplexed
/// batch stream folds it into the <c>QueryComplete</c> terminator.
///
/// <para>The service is driven directly against a real embedded Kahuna node with a real
/// <see cref="QueryResultCache"/> wired in — an executor built without a cache would report
/// <c>cache-disabled</c> and hide a broken hit path.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestGrpcQueryCacheMetadata : BaseTest
{
    private CamusSqlService service = null!;
    private CommandExecutor serviceExecutor = null!;
    private QueryResultCache cache = null!;

    [SetUp]
    public void SetUpGrpcCacheService()
    {
        // sweepIntervalMs: -1 keeps the background sweeper off so an entry cannot expire mid-test.
        cache = new QueryResultCache(Options, sweepIntervalMs: -1);
        serviceExecutor = new CommandExecutor(
            new CommandValidator(Options), new CatalogsManager(logger), logger, Options,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false,
            cache: cache);
        service = new CamusSqlService(
            serviceExecutor, new HttpTransactionCoordinator(serviceExecutor), logger,
            TestHostApplicationLifetime.Instance, new ForegroundRequestGauge(), Options);
    }

    [TearDown]
    public async Task TearDownGrpcCacheService()
    {
        try { await serviceExecutor.DisposeAsync(); } catch { }
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private static TestServerCallContext Ctx() => new();

    private async Task<string> CreateOrdersDatabaseAsync()
    {
        string dbName = "db" + Guid.NewGuid().ToString("n");
        await service.ExecuteDdl(new SqlRequest { Database = "", Sql = $"CREATE DATABASE {dbName}" }, Ctx());
        await service.ExecuteDdl(new SqlRequest
        {
            Database = dbName,
            Sql = "CREATE TABLE orders (id STRING NOT NULL PRIMARY KEY, amount int64 NOT NULL)",
        }, Ctx());
        await service.ExecuteNonQuery(new SqlRequest
        {
            Database = dbName,
            Sql = "INSERT INTO orders (id, amount) VALUES (\"a\", 10)",
        }, Ctx());
        return dbName;
    }

    /// <summary>Runs a unary ExecuteQuery and returns the full message sequence written to the stream.</summary>
    private async Task<List<QueryStreamMessage>> UnaryQueryAsync(string db, string sql)
    {
        CapturingStreamWriter<QueryStreamMessage> writer = new();
        await service.ExecuteQuery(new SqlRequest { Database = db, Sql = sql }, writer, Ctx());
        return writer.Written;
    }

    /// <summary>Runs one QUERY op over the duplex batch stream and returns its QueryComplete terminator.</summary>
    private async Task<QueryComplete> BatchQueryAsync(string db, string sql)
    {
        FakeAsyncStreamReader<BatchExecuteRequest> reader = new(new[]
        {
            new BatchExecuteRequest
            {
                RequestId = 1,
                Kind      = BatchStatementKind.Query,
                Request   = new SqlRequest { Database = db, Sql = sql },
            },
        });
        CapturingStreamWriter<BatchExecuteResponse> writer = new();
        await service.BatchExecute(reader, writer, Ctx());

        BatchExecuteResponse? terminator = writer.Written.SingleOrDefault(
            m => m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.QueryComplete);
        Assert.That(terminator, Is.Not.Null, "Batched QUERY must end with a QueryComplete terminator");
        return terminator!.QueryComplete;
    }

    private static CacheMetadata? TrailingCacheMetadata(List<QueryStreamMessage> messages)
        => messages.LastOrDefault()?.PayloadCase == QueryStreamMessage.PayloadOneofCase.CacheMetadata
            ? messages[^1].CacheMetadata
            : null;

    // ─── Unary ExecuteQuery ───────────────────────────────────────────────────

    [Test]
    public async Task UnaryQuery_CacheHint_Miss_ReportsMissAfterRows()
    {
        string db = await CreateOrdersDatabaseAsync();

        List<QueryStreamMessage> messages = await UnaryQueryAsync(db, "SELECT * FROM orders{cache=grpc_miss}");

        CacheMetadata? meta = TrailingCacheMetadata(messages);
        Assert.That(meta, Is.Not.Null, "A hinted query must append a trailing CacheMetadata message");
        Assert.That(meta!.Status, Is.EqualTo("miss"));
        Assert.That(meta.Name, Is.EqualTo("grpc_miss"));
        Assert.That(meta.BypassReason, Is.Empty, "A miss carries no bypass reason");
        Assert.That(meta.CachedAtHlc, Is.Null, "Nothing was served from the cache, so there is no stored-at instant");
        Assert.That(meta.HasAgeMs, Is.False);

        // Ordering: schema first, rows next, verdict strictly last.
        Assert.That(messages[0].PayloadCase, Is.EqualTo(QueryStreamMessage.PayloadOneofCase.Schema));
        Assert.That(messages.Count(m => m.PayloadCase == QueryStreamMessage.PayloadOneofCase.Row), Is.EqualTo(1));
        Assert.That(messages.Count(m => m.PayloadCase == QueryStreamMessage.PayloadOneofCase.CacheMetadata), Is.EqualTo(1));
    }

    [Test]
    public async Task UnaryQuery_CacheHint_Hit_ReportsHlcAndAge()
    {
        string db = await CreateOrdersDatabaseAsync();

        await UnaryQueryAsync(db, "SELECT * FROM orders{cache=grpc_hit}");            // cold — populates
        List<QueryStreamMessage> warm = await UnaryQueryAsync(db, "SELECT * FROM orders{cache=grpc_hit}");

        CacheMetadata? meta = TrailingCacheMetadata(warm);
        Assert.That(meta, Is.Not.Null);
        Assert.That(meta!.Status, Is.EqualTo("hit"));
        Assert.That(meta.Name, Is.EqualTo("grpc_hit"));
        Assert.That(meta.CachedAtHlc, Is.Not.Null, "A served entry must report the instant it was computed");
        Assert.That(meta.CachedAtHlc.L, Is.GreaterThan(0));
        Assert.That(meta.HasAgeMs, Is.True, "A served entry must report its age");
        Assert.That(meta.AgeMs, Is.GreaterThanOrEqualTo(0));

        // The cached rows are still delivered — a hit must not short-circuit the result set.
        Assert.That(warm.Count(m => m.PayloadCase == QueryStreamMessage.PayloadOneofCase.Row), Is.EqualTo(1));
    }

    [Test]
    public async Task UnaryQuery_NoCacheHint_EmitsNoCacheMetadata()
    {
        string db = await CreateOrdersDatabaseAsync();

        List<QueryStreamMessage> messages = await UnaryQueryAsync(db, "SELECT * FROM orders");

        Assert.That(messages.Any(m => m.PayloadCase == QueryStreamMessage.PayloadOneofCase.CacheMetadata), Is.False,
            "An unhinted query must emit no CacheMetadata — its absence is how a client tells 'not hinted' from 'bypassed'");
        Assert.That(messages.Count(m => m.PayloadCase == QueryStreamMessage.PayloadOneofCase.Row), Is.EqualTo(1));
    }

    // ─── Batched QUERY ────────────────────────────────────────────────────────

    [Test]
    public async Task BatchQuery_CacheHint_MissThenHit_ReportedOnTerminator()
    {
        string db = await CreateOrdersDatabaseAsync();

        QueryComplete cold = await BatchQueryAsync(db, "SELECT * FROM orders{cache=grpc_batch}");
        Assert.That(cold.CacheMetadata, Is.Not.Null, "The batch terminator must carry the verdict for a hinted query");
        Assert.That(cold.CacheMetadata.Status, Is.EqualTo("miss"));
        Assert.That(cold.CacheMetadata.Name, Is.EqualTo("grpc_batch"));

        QueryComplete warm = await BatchQueryAsync(db, "SELECT * FROM orders{cache=grpc_batch}");
        Assert.That(warm.CacheMetadata, Is.Not.Null);
        Assert.That(warm.CacheMetadata.Status, Is.EqualTo("hit"));
        Assert.That(warm.CacheMetadata.CachedAtHlc, Is.Not.Null);
        Assert.That(warm.CacheMetadata.HasAgeMs, Is.True);
    }

    [Test]
    public async Task BatchQuery_PreparedCacheHint_MissThenHit_ReportedOnTerminator()
    {
        string db = await CreateOrdersDatabaseAsync();

        // A prepared execution reuses the SQL instance captured at registration, so the hint — which
        // rides that text — must resolve exactly as it does inline. If the prepared path ever stopped
        // handing the executor the same statement, the second run would miss instead of hit.
        ChannelAsyncStreamReader<BatchExecuteRequest> reader = new();
        ObservingStreamWriter<BatchExecuteResponse> writer = new();
        Task server = service.BatchExecute(reader, writer, Ctx());

        reader.Push(new BatchExecuteRequest
        {
            RequestId = 1,
            Kind = BatchStatementKind.Prepare,
            Request = new SqlRequest { Database = db, Sql = "SELECT * FROM orders{cache=grpc_prepared}" },
        });

        BatchExecuteResponse prepared = await writer.WaitFor(
            m => m.RequestId == 1 && m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.PrepareReply);
        int statementId = prepared.PrepareReply.StatementId;

        async Task<QueryComplete> ExecuteAsync(int requestId)
        {
            reader.Push(new BatchExecuteRequest
            {
                RequestId = requestId,
                Kind = BatchStatementKind.Query,
                Request = new SqlRequest { StatementId = statementId },
            });

            BatchExecuteResponse terminator = await writer.WaitFor(m =>
                m.RequestId == requestId && m.PayloadCase is
                    BatchExecuteResponse.PayloadOneofCase.QueryComplete or
                    BatchExecuteResponse.PayloadOneofCase.Error);

            Assert.That(terminator.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.QueryComplete),
                terminator.Error?.Message);
            return terminator.QueryComplete;
        }

        QueryComplete cold = await ExecuteAsync(2);
        QueryComplete warm = await ExecuteAsync(3);

        reader.Complete();
        await server;

        Assert.That(cold.CacheMetadata, Is.Not.Null, "a prepared hinted query must still report a verdict");
        Assert.That(cold.CacheMetadata.Status, Is.EqualTo("miss"));
        Assert.That(cold.CacheMetadata.Name, Is.EqualTo("grpc_prepared"));

        Assert.That(warm.CacheMetadata, Is.Not.Null);
        Assert.That(warm.CacheMetadata.Status, Is.EqualTo("hit"));
    }

    [Test]
    public async Task BatchQuery_NoCacheHint_TerminatorHasNoCacheMetadata()
    {
        string db = await CreateOrdersDatabaseAsync();

        QueryComplete complete = await BatchQueryAsync(db, "SELECT * FROM orders");

        Assert.That(complete.CacheMetadata, Is.Null,
            "An unhinted query must leave the terminator's cache verdict absent");
    }
}
