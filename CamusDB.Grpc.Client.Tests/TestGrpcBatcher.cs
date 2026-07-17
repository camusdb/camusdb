
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;

using NUnit.Framework;

using CamusDB.Grpc;
using CamusDB.Grpc.Client;
using CamusDB.Grpc.Client.Batching;

namespace CamusDB.Grpc.Client.Tests;

/// <summary>
/// Tests the multiplexing client batcher against an in-memory <see cref="FakeBatchTransport"/> — the
/// same "inject a fake shared streaming" approach Kahuna's client tests use. Verifies correlation,
/// the two routing regimes, saturation/queueing, reconnect, deadlines, and causal-token threading.
/// </summary>
[TestFixture]
public class TestGrpcBatcher
{
    private readonly List<FakeBatchTransport> created = new();

    private GrpcBatcher NewBatcher(CamusGrpcOptions options, Action<FakeBatchTransport>? configure = null)
    {
        created.Clear();
        return new GrpcBatcher(options, id =>
        {
            FakeBatchTransport t = new(id);
            configure?.Invoke(t);
            created.Add(t);
            return t;
        });
    }

    [Test]
    public async Task ConcurrentAutocommitOps_AllCorrelateCorrectly()
    {
        await using GrpcBatcher batcher = NewBatcher(new CamusGrpcOptions { ChannelPoolSize = 2 });
        CamusConnection conn = new(batcher);

        List<Task> work = new();
        int queries = 0, nonQueries = 0;
        for (int i = 0; i < 40; i++)
        {
            if (i % 2 == 0)
                work.Add(conn.ExecuteQueryAsync("db", "SELECT n FROM t").ContinueWith(t =>
                {
                    Assert.That(t.Result.Rows.Count, Is.EqualTo(2));
                    Assert.That(t.Result.Schema.Columns[0].Name, Is.EqualTo("n"));
                    Interlocked.Increment(ref queries);
                }));
            else
                work.Add(conn.ExecuteNonQueryAsync("db", "INSERT ...").ContinueWith(t =>
                {
                    Assert.That(t.Result.AffectedRows, Is.EqualTo(1));
                    Interlocked.Increment(ref nonQueries);
                }));
        }

        await Task.WhenAll(work);
        Assert.That(queries, Is.EqualTo(20));
        Assert.That(nonQueries, Is.EqualTo(20));
    }

    [Test]
    public async Task TransactionOps_PinToOneTransport()
    {
        await using GrpcBatcher batcher = NewBatcher(new CamusGrpcOptions { ChannelPoolSize = 2 });
        CamusConnection conn = new(batcher);

        CamusTransactionSession tx = await conn.BeginTransactionAsync("db");
        await tx.ExecuteNonQueryAsync("INSERT INTO t VALUES (1)");
        await tx.ExecuteNonQueryAsync("INSERT INTO t VALUES (2)");
        await tx.CommitAsync();

        // All four ops (START + 2 statements + COMMIT) must have gone to exactly one transport, and the
        // other transport must have seen none of them — the transactional pinning regime.
        List<FakeBatchTransport> withTraffic = created.Where(t => !t.Received.IsEmpty).ToList();
        Assert.That(withTraffic.Count, Is.EqualTo(1), "A transaction's ops must pin to a single stream");
        Assert.That(withTraffic[0].Received.Count, Is.EqualTo(4));

        List<BatchStatementKind> kinds = withTraffic[0].Received.Select(r => r.Kind).ToList();
        Assert.That(kinds, Is.EqualTo(new[]
        {
            BatchStatementKind.Start, BatchStatementKind.NonQuery,
            BatchStatementKind.NonQuery, BatchStatementKind.Commit,
        }), "Same-stream ops must arrive in issue order");
    }

    [Test]
    public async Task AutocommitOps_SpreadAcrossThePool()
    {
        await using GrpcBatcher batcher = NewBatcher(new CamusGrpcOptions { ChannelPoolSize = 2 });
        CamusConnection conn = new(batcher);

        // Round-robin autocommit routing must touch both transports over enough ops.
        for (int i = 0; i < 8; i++)
            await conn.ExecuteNonQueryAsync("db", "INSERT ...");

        Assert.That(created.Count(t => !t.Received.IsEmpty), Is.EqualTo(2),
            "Autocommit ops must spread across the pool, not pin to one stream");
    }

    [Test]
    public async Task SaturatedBurst_OnOneStream_AllComplete()
    {
        await using GrpcBatcher batcher = NewBatcher(new CamusGrpcOptions { ChannelPoolSize = 1 });
        CamusConnection conn = new(batcher);

        Task<NonQueryResult>[] burst = Enumerable.Range(0, 50)
            .Select(_ => conn.ExecuteNonQueryAsync("db", "INSERT ..."))
            .ToArray();

        NonQueryResult[] results = await Task.WhenAll(burst);
        Assert.That(results.Length, Is.EqualTo(50));
        Assert.That(results.All(r => r.AffectedRows == 1), Is.True);
        Assert.That(created[0].Received.Count, Is.EqualTo(50), "All ops queued onto the single stream");
    }

    [Test]
    public async Task StreamDrop_FailsPending_AndReconnects()
    {
        // First transport holds (never replies) so we can fault it while an op is in flight.
        await using GrpcBatcher batcher = NewBatcher(
            new CamusGrpcOptions { ChannelPoolSize = 1 },
            configure: t => { if (t.Id == 1) t.Hold = true; });
        CamusConnection conn = new(batcher);

        Task<NonQueryResult> held = conn.ExecuteNonQueryAsync("db", "INSERT ...");
        await WaitUntil(() => created.Count == 1 && !created[0].Received.IsEmpty);

        created[0].FailStream(new IOException("stream dropped"));

        Assert.ThrowsAsync<IOException>(async () => await held, "A dropped stream must fault its pending ops");

        // The slot rebuilds with a fresh transport; a new op must succeed on it.
        await WaitUntil(() => created.Count >= 2);
        NonQueryResult afterReconnect = await conn.ExecuteNonQueryAsync("db", "INSERT ...");
        Assert.That(afterReconnect.AffectedRows, Is.EqualTo(1));
    }

    [Test]
    public async Task NoCallerToken_HonorsDefaultDeadline()
    {
        // Every transport holds — nothing ever replies — so an op with no token must time out.
        await using GrpcBatcher batcher = NewBatcher(
            new CamusGrpcOptions { ChannelPoolSize = 1, OperationTimeout = TimeSpan.FromMilliseconds(200) },
            configure: t => t.Hold = true);
        CamusConnection conn = new(batcher);

        Assert.CatchAsync<OperationCanceledException>(async () =>
            await conn.ExecuteNonQueryAsync("db", "INSERT ..."));
    }

    [Test]
    public async Task Session_ThreadsCausalTokenForward()
    {
        await using GrpcBatcher batcher = NewBatcher(new CamusGrpcOptions { ChannelPoolSize = 1 });
        CamusConnection conn = new(batcher);

        CamusTransactionSession tx = await conn.BeginTransactionAsync("db");
        NonQueryResult first = await tx.ExecuteNonQueryAsync("INSERT INTO t VALUES (1)");
        await tx.ExecuteQueryAsync("SELECT n FROM t");

        // The fake echoes token L = request id. The query issued after the non-query must carry the
        // non-query's token forward on its handle (all of N/L/C).
        List<BatchExecuteRequest> reqs = created[0].Received.ToList();
        BatchExecuteRequest nonQuery = reqs.First(r => r.Kind == BatchStatementKind.NonQuery);
        BatchExecuteRequest query    = reqs.First(r => r.Kind == BatchStatementKind.Query);

        Assert.That(first.Token.L, Is.EqualTo(nonQuery.RequestId));
        Assert.That(query.Request.TxnHandle.CausalTokenL, Is.EqualTo(nonQuery.RequestId));
        Assert.That(query.Request.TxnHandle.CausalTokenN, Is.EqualTo(1));
        Assert.That(query.Request.TxnHandle.CausalTokenC, Is.EqualTo(1));
    }

    private static async Task WaitUntil(Func<bool> condition, int timeoutMs = 5000)
    {
        int waited = 0;
        while (!condition())
        {
            if (waited >= timeoutMs)
                Assert.Fail("Condition not met within timeout");
            await Task.Delay(20);
            waited += 20;
        }
    }
}
