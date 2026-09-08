
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using NUnit.Framework;
using CamusDB.Grpc;
using CamusDB.Grpc.Client.Batching;

namespace CamusDB.Grpc.Client.Tests;

/// <summary>
/// Routing behavior of a multi-endpoint <see cref="CamusConnection"/> over scripted fake
/// transports: negotiation per mode, learning and reuse of advertised destinations, rejection of
/// unusable advice, clear handling, TTL expiry against the injected clock, endpoint cooldown after
/// transport failure, lazy prepared registration on a learned endpoint, and transaction-start
/// affinity with endpoint pinning.
/// </summary>
[TestFixture]
public sealed class TestRoutedConnection
{
    private sealed class TestEndpoint
    {
        public string Address = "";
        public string? NodeId;
        public GrpcBatcher Batcher = null!;
        public readonly List<FakeBatchTransport> Transports = new();

        public int Count(BatchStatementKind kind)
        {
            int total = 0;
            foreach (FakeBatchTransport transport in Transports)
                foreach (BatchExecuteRequest request in transport.Received)
                    if (request.Kind == kind)
                        total++;
            return total;
        }

        public IEnumerable<BatchExecuteRequest> Requests()
        {
            foreach (FakeBatchTransport transport in Transports)
                foreach (BatchExecuteRequest request in transport.Received)
                    yield return request;
        }
    }

    private static TestEndpoint NewEndpoint(
        string address, string? nodeId, CamusGrpcOptions options,
        Func<BatchExecuteRequest, RoutingAdvice?>? advice = null,
        Action<FakeBatchTransport>? configure = null)
    {
        TestEndpoint endpoint = new() { Address = address, NodeId = nodeId };
        endpoint.Batcher = new GrpcBatcher(options, id =>
        {
            FakeBatchTransport transport = new(id) { AdviceFactory = advice };
            configure?.Invoke(transport);
            lock (endpoint.Transports)
                endpoint.Transports.Add(transport);
            return transport;
        });
        return endpoint;
    }

    private static CamusGrpcOptions NewOptions(CamusRoutingMode mode)
    {
        CamusGrpcOptions options = new() { ChannelPoolSize = 1, CoalescingThreshold = 1 };
        options.Routing.Mode = mode;
        return options;
    }

    private static RoutingAdvice Prefer(string nodeId, int maxAgeMs = 5_000) => new()
    {
        Version = 1,
        Disposition = RoutingDisposition.Prefer,
        PreferredNodeId = nodeId,
        ReuseScope = RoutingReuseScope.StatementParametersIndependent,
        DependencyToken = "tok",
        MaxAgeMs = maxAgeMs,
        Provenance = "placementHint",
        Reason = "singleTableHash",
    };

    private static RoutingAdvice ClearAdvice() => new()
    {
        Version = 1,
        Disposition = RoutingDisposition.Clear,
        Reason = "ineligible",
    };

    private static (CamusConnection Connection, TestEndpoint A, TestEndpoint B) NewPair(
        CamusRoutingMode mode,
        Func<BatchExecuteRequest, RoutingAdvice?>? advice = null,
        bool mapB = true,
        Action<FakeBatchTransport>? configureA = null)
    {
        CamusGrpcOptions options = NewOptions(mode);
        TestEndpoint a = NewEndpoint("addr-a", "node-a", options, advice, configureA);
        TestEndpoint b = NewEndpoint("addr-b", mapB ? "node-b" : null, options, advice);
        CamusConnection connection = CamusConnection.CreateForTesting(
            options, [(a.Address, a.NodeId, a.Batcher), (b.Address, b.NodeId, b.Batcher)]);
        return (connection, a, b);
    }

    [Test]
    public async Task OffMode_RotatesAndNeverNegotiates()
    {
        (CamusConnection connection, TestEndpoint a, TestEndpoint b) = NewPair(
            CamusRoutingMode.Off, advice: static _ => Prefer("node-b"));
        await using CamusConnection _ = connection;

        for (int i = 0; i < 4; i++)
            await connection.ExecuteQueryAsync("db1", "select 1");

        Assert.That(a.Count(BatchStatementKind.Query), Is.EqualTo(2));
        Assert.That(b.Count(BatchStatementKind.Query), Is.EqualTo(2));

        foreach (BatchExecuteRequest request in a.Requests())
            Assert.That(request.Request.RoutingAcceptVersion, Is.EqualTo(0));
    }

    [Test]
    public async Task AutoMode_WithOneMappedNode_StaysOff()
    {
        (CamusConnection connection, TestEndpoint a, TestEndpoint b) = NewPair(
            CamusRoutingMode.Auto, advice: static _ => Prefer("node-a"), mapB: false);
        await using CamusConnection _ = connection;

        for (int i = 0; i < 4; i++)
            await connection.ExecuteQueryAsync("db1", "select 1");

        // One mapped identity is not a routing decision: rotation continues, nothing negotiated.
        Assert.That(a.Count(BatchStatementKind.Query), Is.EqualTo(2));
        Assert.That(b.Count(BatchStatementKind.Query), Is.EqualTo(2));
        foreach (BatchExecuteRequest request in a.Requests())
            Assert.That(request.Request.RoutingAcceptVersion, Is.EqualTo(0));
    }

    [Test]
    public async Task LearnedMode_PrefersAdvertisedNode_AcrossParameterValues()
    {
        (CamusConnection connection, TestEndpoint a, TestEndpoint b) = NewPair(
            CamusRoutingMode.Learned, advice: static _ => Prefer("node-b"));
        await using CamusConnection _ = connection;

        for (int i = 0; i < 4; i++)
            await connection.ExecuteQueryAsync("db1", "select balance from accounts where id = @id");

        // First execution rotates (endpoint A), learns node-b; every later one goes straight to B.
        Assert.That(a.Count(BatchStatementKind.Query), Is.EqualTo(1));
        Assert.That(b.Count(BatchStatementKind.Query), Is.EqualTo(3));

        foreach (BatchExecuteRequest request in b.Requests())
            Assert.That(request.Request.RoutingAcceptVersion, Is.EqualTo(1));
    }

    [Test]
    public async Task Advice_NamingUnmappedNode_IsIgnored()
    {
        (CamusConnection connection, TestEndpoint a, TestEndpoint b) = NewPair(
            CamusRoutingMode.Learned, advice: static _ => Prefer("node-unknown"));
        await using CamusConnection _ = connection;

        for (int i = 0; i < 4; i++)
            await connection.ExecuteQueryAsync("db1", "select 1");

        Assert.That(connection.LearnedRouteCount, Is.EqualTo(0));
        Assert.That(a.Count(BatchStatementKind.Query), Is.EqualTo(2));
        Assert.That(b.Count(BatchStatementKind.Query), Is.EqualTo(2));
    }

    [Test]
    public async Task Advice_WithUnknownReuseScope_IsNotLearned()
    {
        RoutingAdvice advice = Prefer("node-b");
        advice.ReuseScope = RoutingReuseScope.Unspecified;

        (CamusConnection connection, TestEndpoint a, TestEndpoint b) = NewPair(
            CamusRoutingMode.Learned, advice: _ => advice);
        await using CamusConnection _ = connection;

        for (int i = 0; i < 4; i++)
            await connection.ExecuteQueryAsync("db1", "select 1");

        Assert.That(connection.LearnedRouteCount, Is.EqualTo(0));
        Assert.That(a.Count(BatchStatementKind.Query), Is.EqualTo(2));
        Assert.That(b.Count(BatchStatementKind.Query), Is.EqualTo(2));
    }

    [Test]
    public async Task Advice_WithUnknownVersion_IsIgnored()
    {
        RoutingAdvice advice = Prefer("node-b");
        advice.Version = 2;

        (CamusConnection connection, TestEndpoint a, TestEndpoint b) = NewPair(
            CamusRoutingMode.Learned, advice: _ => advice);
        await using CamusConnection _ = connection;

        for (int i = 0; i < 4; i++)
            await connection.ExecuteQueryAsync("db1", "select 1");

        Assert.That(connection.LearnedRouteCount, Is.EqualTo(0));
    }

    [Test]
    public async Task Clear_RemovesTheLearnedRoute()
    {
        // Script: first reply prefers node-b, second clears, later replies carry nothing.
        ConcurrentQueue<RoutingAdvice?> script = new();
        script.Enqueue(Prefer("node-b"));
        script.Enqueue(ClearAdvice());

        (CamusConnection connection, TestEndpoint a, TestEndpoint b) = NewPair(
            CamusRoutingMode.Learned, advice: _ => script.TryDequeue(out RoutingAdvice? next) ? next : null);
        await using CamusConnection _ = connection;

        await connection.ExecuteQueryAsync("db1", "select 1");   // rotation → A, learns node-b
        await connection.ExecuteQueryAsync("db1", "select 1");   // learned → B, reply clears
        Assert.That(connection.LearnedRouteCount, Is.EqualTo(0));

        await connection.ExecuteQueryAsync("db1", "select 1");   // rotation again
        await connection.ExecuteQueryAsync("db1", "select 1");

        Assert.That(a.Count(BatchStatementKind.Query), Is.EqualTo(2));
        Assert.That(b.Count(BatchStatementKind.Query), Is.EqualTo(2));
    }

    [Test]
    public async Task ExpiredRoute_FallsBackToRotation()
    {
        ConcurrentQueue<RoutingAdvice?> script = new();
        script.Enqueue(Prefer("node-b", maxAgeMs: 5_000));

        (CamusConnection connection, TestEndpoint a, TestEndpoint b) = NewPair(
            CamusRoutingMode.Learned, advice: _ => script.TryDequeue(out RoutingAdvice? next) ? next : null);
        await using CamusConnection _ = connection;

        long now = 0;
        connection.Clock = () => Volatile.Read(ref now);

        await connection.ExecuteQueryAsync("db1", "select 1");   // rotation → A, learns node-b until 5000
        Volatile.Write(ref now, 4_999);
        await connection.ExecuteQueryAsync("db1", "select 1");   // still fresh → B
        Volatile.Write(ref now, 5_000);
        await connection.ExecuteQueryAsync("db1", "select 1");   // expired → rotation
        await connection.ExecuteQueryAsync("db1", "select 1");

        Assert.That(a.Count(BatchStatementKind.Query), Is.EqualTo(2));
        Assert.That(b.Count(BatchStatementKind.Query), Is.EqualTo(2));
    }

    [Test]
    public async Task TransportFailure_CoolsTheEndpointDown()
    {
        FakeBatchTransport? held = null;
        (CamusConnection connection, TestEndpoint a, TestEndpoint b) = NewPair(
            CamusRoutingMode.Off,
            configureA: t =>
            {
                // Only the endpoint's first transport is held+failed; its rebuilt stream works.
                if (held is null)
                {
                    held = t;
                    t.Hold = true;
                }
            });
        await using CamusConnection _ = connection;

        long now = 0;
        connection.Clock = () => Volatile.Read(ref now);

        Task<QueryResult> failing = connection.ExecuteQueryAsync("db1", "select 1");   // rotation → A
        await WaitUntilAsync(() => held is not null && !held.Received.IsEmpty);
        held!.FailStream(new IOException("boom"));
        Assert.ThrowsAsync<IOException>(async () => await failing);

        // While A cools down, rotation lands everything on B — including the turn that was A's.
        await connection.ExecuteQueryAsync("db1", "select 1");
        await connection.ExecuteQueryAsync("db1", "select 1");
        Assert.That(b.Count(BatchStatementKind.Query), Is.EqualTo(2));
        Assert.That(a.Count(BatchStatementKind.Query), Is.EqualTo(1));

        // After the cooldown elapses A serves again (on its rebuilt stream). Wait for the rebuild
        // so the post-cooldown send cannot race the reader loop's reconnect.
        await WaitUntilAsync(() =>
        {
            lock (a.Transports)
                return a.Transports.Count >= 2;
        });
        Volatile.Write(ref now, 1_001);
        await connection.ExecuteQueryAsync("db1", "select 1");   // rotation parity → B
        await connection.ExecuteQueryAsync("db1", "select 1");   // → A
        Assert.That(a.Count(BatchStatementKind.Query), Is.EqualTo(2));
    }

    [Test]
    public async Task PreparedStatement_FollowsLearnedRoute_AndRegistersLazily()
    {
        (CamusConnection connection, TestEndpoint a, TestEndpoint b) = NewPair(
            CamusRoutingMode.Learned, advice: static _ => Prefer("node-b"));
        await using CamusConnection _ = connection;

        CamusPreparedStatement statement = await connection.PrepareAsync(
            "db1", "select balance from accounts where id = @a");

        await statement.ExecuteQueryAsync([1L]);   // rotation, learns node-b from the reply
        await statement.ExecuteQueryAsync([2L]);   // learned → B, registers there on demand
        await statement.ExecuteQueryAsync([3L]);   // warmed on B: no further registration

        Assert.That(b.Count(BatchStatementKind.Query), Is.GreaterThanOrEqualTo(2));
        Assert.That(b.Count(BatchStatementKind.Prepare), Is.EqualTo(1));

        await statement.DisposeAsync();

        // Disposal swept every endpoint: each PREPARE that minted a handle got a CLOSE.
        int prepared = a.Count(BatchStatementKind.Prepare) + b.Count(BatchStatementKind.Prepare);
        int closed = a.Count(BatchStatementKind.Close) + b.Count(BatchStatementKind.Close);
        Assert.That(closed, Is.EqualTo(prepared));
    }

    [Test]
    public async Task TransactionAffinity_SelectsLearnedEndpoint_AndStaysPinned()
    {
        (CamusConnection connection, TestEndpoint a, TestEndpoint b) = NewPair(
            CamusRoutingMode.Learned, advice: static _ => Prefer("node-b"));
        await using CamusConnection _ = connection;

        CamusPreparedStatement statement = await connection.PrepareAsync(
            "db1", "select balance from accounts where id = @a");
        await statement.ExecuteQueryAsync([1L]);   // warms the route to node-b

        CamusTransactionSession session = await connection.BeginTransactionAsync(
            "db1", affinity: statement);

        Assert.That(b.Count(BatchStatementKind.Start), Is.EqualTo(1));
        Assert.That(a.Count(BatchStatementKind.Start), Is.EqualTo(0));

        // Every op of the session stays on the endpoint START chose, advice notwithstanding.
        await session.ExecuteNonQueryAsync("update accounts set balance = 1");
        await session.ExecuteQueryAsync(statement, [2L]);
        await session.CommitAsync();

        Assert.That(a.Count(BatchStatementKind.NonQuery), Is.EqualTo(0));
        Assert.That(b.Count(BatchStatementKind.NonQuery), Is.EqualTo(1));
        Assert.That(b.Count(BatchStatementKind.Commit), Is.EqualTo(1));
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        for (int i = 0; i < 500; i++)
        {
            if (condition())
                return;
            await Task.Delay(10);
        }

        Assert.Fail("Condition was not reached in time");
    }
}
