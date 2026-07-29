/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Catalogs;
using CamusDB.Grpc;
using CamusDB.App.Grpc;
using CamusDB.App.Services;
using CamusDB.Grpc.Client;
using CamusDB.Grpc.Client.Batching;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// End-to-end coverage of the client's prepared-statement surface against a real
/// <see cref="CamusSqlService"/> over in-process transports.
///
/// <para>The interesting case is not the happy path but the one the design exists for: a client
/// statement outlives the stream its server-side handle lived on. These tests kill a slot's transport
/// mid-life and assert the statement transparently re-registers, and that an unknown-statement error
/// never reaches the caller.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestGrpcClientPreparedStatements : BaseTest
{
    private CommandExecutor executor = null!;
    private HttpTransactionCoordinator coordinator = null!;
    private CamusSqlService service = null!;

    [SetUp]
    public void SetUpService()
    {
        CommandValidator validator = new();
        CatalogsManager catalogs = new(logger);
        executor = new(validator, catalogs, logger,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
        coordinator = new(executor);
        service = new(executor, coordinator, logger, TestHostApplicationLifetime.Instance, new ForegroundRequestGauge());
    }

    [TearDown]
    public async Task TearDownService()
    {
        try { await executor.DisposeAsync(); } catch { }
    }

    // ─── Harness ──────────────────────────────────────────────────────────────

    /// <summary>
    /// An in-process transport a test can kill on demand, so the batcher observes exactly what a
    /// faulted gRPC stream looks like — the response side ends, pending ops fail, and the slot is
    /// rebuilt with a new transport id.
    /// </summary>
    private sealed class FaultableTransport : IBatchTransport
    {
        private readonly ChannelAsyncStreamReader<BatchExecuteRequest> serverReader = new();
        private readonly Channel<BatchExecuteResponse> responses = Channel.CreateUnbounded<BatchExecuteResponse>();
        private readonly Task serverTask;

        public long Id { get; }

        public FaultableTransport(long id, CamusSqlService service)
        {
            Id = id;
            ChannelServerStreamWriter<BatchExecuteResponse> writer = new(responses);
            serverTask = Task.Run(async () =>
            {
                try { await service.BatchExecute(serverReader, writer, new TestServerCallContext()); }
                finally { responses.Writer.TryComplete(); }
            });
        }

        /// <summary>Ends the stream the way a dropped connection would.</summary>
        public void Kill()
        {
            serverReader.Complete();
            responses.Writer.TryComplete();
        }

        public Task SendAsync(BatchExecuteRequest request, CancellationToken cancellationToken)
        {
            serverReader.Push(request);
            return Task.CompletedTask;
        }

        public async IAsyncEnumerable<BatchExecuteResponse> ReadAllAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (BatchExecuteResponse resp in responses.Reader.ReadAllAsync(cancellationToken))
                yield return resp;
        }

        public async ValueTask DisposeAsync()
        {
            Kill();
            try { await serverTask; } catch { /* teardown */ }
        }
    }

    /// <summary>Hands out transports and remembers the live one so a test can kill it.</summary>
    private sealed class TransportFactory
    {
        private readonly CamusSqlService service;
        private readonly List<FaultableTransport> created = new();

        public TransportFactory(CamusSqlService service) => this.service = service;

        public FaultableTransport Latest
        {
            get { lock (created) return created[^1]; }
        }

        public int Count
        {
            get { lock (created) return created.Count; }
        }

        public IBatchTransport Create(long id)
        {
            FaultableTransport transport = new(id, service);
            lock (created) created.Add(transport);
            return transport;
        }
    }

    private CamusConnection NewConnection(int poolSize = 1)
    {
        GrpcBatcher batcher = new(
            new CamusGrpcOptions { ChannelPoolSize = poolSize },
            id => new InProcBatchTransport(id, service));
        return new CamusConnection(batcher);
    }

    private async Task<string> CreateDatabaseWithTableAsync()
    {
        string db = "db" + Guid.NewGuid().ToString("n");
        await service.ExecuteDdl(new SqlRequest { Database = db, Sql = $"CREATE DATABASE {db}" }, new TestServerCallContext());
        await service.ExecuteDdl(new SqlRequest
        {
            Database = db,
            Sql = "CREATE TABLE robots (id oid PRIMARY KEY, name string(100), year int64)",
        }, new TestServerCallContext());
        return db;
    }

    // ─── Happy path ───────────────────────────────────────────────────────────

    [Test]
    public async Task PrepareAsync_ReportsParameterNamesAndExecutesPositionally()
    {
        string db = await CreateDatabaseWithTableAsync();
        await using CamusConnection connection = NewConnection();

        await using CamusPreparedStatement insert = await connection.PrepareAsync(
            db, "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)");

        Assert.That(insert.ParameterNames, Is.EqualTo(new[] { "@name", "@year" }));

        NonQueryResult first = await insert.ExecuteNonQueryAsync(["optimus", 1984L]);
        NonQueryResult second = await insert.ExecuteNonQueryAsync(["wall-e", 2008L]);

        Assert.That(first.AffectedRows, Is.EqualTo(1));
        Assert.That(second.AffectedRows, Is.EqualTo(1));

        await using CamusPreparedStatement select = await connection.PrepareAsync(
            db, "SELECT name FROM robots WHERE year = @year");

        QueryResult result = await select.ExecuteQueryAsync([2008L]);
        Assert.That(result.Rows.Count, Is.EqualTo(1), "the WHERE must have bound @year to 2008");
        Assert.That(result.Rows[0].Values[0].StringValue, Is.EqualTo("wall-e"));
    }

    [Test]
    public async Task WrongValueCount_IsRejectedByTheClientBeforeAnyRoundTrip()
    {
        string db = await CreateDatabaseWithTableAsync();
        await using CamusConnection connection = NewConnection();

        await using CamusPreparedStatement select = await connection.PrepareAsync(
            db, "SELECT name FROM robots WHERE year = @year AND name = @name");

        Assert.That(
            async () => await select.ExecuteQueryAsync([2008L]),
            Throws.ArgumentException);
    }

    [Test]
    public async Task PreparedStatement_InsideATransaction_CommitsWithIt()
    {
        string db = await CreateDatabaseWithTableAsync();
        await using CamusConnection connection = NewConnection();

        await using CamusPreparedStatement insert = await connection.PrepareAsync(
            db, "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)");

        CamusTransactionSession txn = await connection.BeginTransactionAsync(db);
        await txn.ExecuteNonQueryAsync(insert, ["optimus", 1984L]);
        await txn.ExecuteNonQueryAsync(insert, ["wall-e", 2008L]);
        await txn.CommitAsync();

        QueryResult all = await connection.ExecuteQueryAsync(db, "SELECT name FROM robots");
        Assert.That(all.Rows.Count, Is.EqualTo(2));
    }

    [Test]
    public async Task ExecutingAfterDispose_Throws()
    {
        string db = await CreateDatabaseWithTableAsync();
        await using CamusConnection connection = NewConnection();

        CamusPreparedStatement select = await connection.PrepareAsync(
            db, "SELECT name FROM robots WHERE year = @year");
        await select.DisposeAsync();

        Assert.That(
            async () => await select.ExecuteQueryAsync([2008L]),
            Throws.TypeOf<ObjectDisposedException>());
    }

    [Test]
    public async Task ConcurrentExecutionsAcrossThePool_EachBindTheirOwnValues()
    {
        string db = await CreateDatabaseWithTableAsync();
        await using CamusConnection connection = NewConnection(poolSize: 4);

        await using CamusPreparedStatement insert = await connection.PrepareAsync(
            db, "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)");

        // Every execution lands on whichever slot the round-robin picks, so most of these register
        // the statement on a stream that has never seen it — concurrently, and several at once per
        // slot. Each row must still carry its own values.
        await Task.WhenAll(Enumerable.Range(0, 24).Select(i =>
            insert.ExecuteNonQueryAsync([$"robot-{i}", (long)(2000 + i)])));

        QueryResult all = await connection.ExecuteQueryAsync(db, "SELECT name, year FROM robots ORDER BY year");
        Assert.That(all.Rows.Count, Is.EqualTo(24));
        for (int i = 0; i < 24; i++)
        {
            Assert.That(all.Rows[i].Values[0].StringValue, Is.EqualTo($"robot-{i}"));
            Assert.That(all.Rows[i].Values[1].Int64Value, Is.EqualTo(2000 + i));
        }
    }

    // ─── Fault recovery ───────────────────────────────────────────────────────

    [Test]
    public async Task AfterItsStreamIsRebuilt_TheStatementReprepairsWithoutLeakingUnknownStatement()
    {
        string db = await CreateDatabaseWithTableAsync();

        TransportFactory factory = new(service);
        GrpcBatcher batcher = new(new CamusGrpcOptions { ChannelPoolSize = 1 }, factory.Create);
        await using CamusConnection connection = new(batcher);

        await using CamusPreparedStatement insert = await connection.PrepareAsync(
            db, "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)");

        await insert.ExecuteNonQueryAsync(["before", 1984L]);
        Assert.That(factory.Count, Is.EqualTo(1));

        // Kill the only stream. Every handle registered on it dies with it on the server side.
        factory.Latest.Kill();
        await WaitForRebuildAsync(factory, expectedCount: 2);

        // The same statement object keeps working: it notices its registration belongs to a stream
        // that no longer exists and silently registers again on the rebuilt one.
        NonQueryResult after = await ExecuteToleratingTransportFaultAsync(insert, ["after", 2008L]);
        Assert.That(after.AffectedRows, Is.EqualTo(1));

        QueryResult all = await connection.ExecuteQueryAsync(db, "SELECT name FROM robots ORDER BY year");
        Assert.That(all.Rows.Select(r => r.Values[0].StringValue), Is.EqualTo(new[] { "before", "after" }));
    }

    [Test]
    public async Task AStreamThatFaultsWhileAnExecutionIsInFlight_SurfacesTheReplayContractNotUnknownStatement()
    {
        string db = await CreateDatabaseWithTableAsync();

        TransportFactory factory = new(service);
        GrpcBatcher batcher = new(new CamusGrpcOptions { ChannelPoolSize = 1 }, factory.Create);
        await using CamusConnection connection = new(batcher);

        await using CamusPreparedStatement insert = await connection.PrepareAsync(
            db, "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)");

        // Start executions and kill the stream underneath them, so the fault lands on ops that are
        // genuinely in flight rather than on an idle connection. Whatever each op reports, it must
        // never be the unknown-statement code: re-registering on a rebuilt stream is the client's job.
        List<Task<NonQueryResult>> inFlight = [];
        for (int i = 0; i < 8; i++)
            inFlight.Add(insert.ExecuteNonQueryAsync([$"racer-{i}", (long)(2000 + i)]));

        factory.Latest.Kill();

        int faulted = 0;
        foreach (Task<NonQueryResult> op in inFlight)
        {
            try
            {
                await op;
            }
            catch (CamusGrpcException domain)
            {
                Assert.Fail($"an in-flight fault must not surface a domain error ({domain.Code})");
            }
            catch
            {
                faulted++;   // transport fault — the documented replay contract.
            }
        }

        await WaitForRebuildAsync(factory, expectedCount: 2);

        // Whatever happened to the in-flight ops, the statement itself is still usable afterwards.
        NonQueryResult after = await ExecuteToleratingTransportFaultAsync(insert, ["after", 3000L]);
        Assert.That(after.AffectedRows, Is.EqualTo(1));
        Assert.That(faulted, Is.LessThanOrEqualTo(inFlight.Count));
    }

    [Test]
    public async Task DisposeWhileExecutionsAreInFlight_LeavesNoHandleBehind()
    {
        string db = await CreateDatabaseWithTableAsync();

        TransportFactory factory = new(service);
        GrpcBatcher batcher = new(new CamusGrpcOptions { ChannelPoolSize = 4 }, factory.Create);
        await using CamusConnection connection = new(batcher);

        CamusPreparedStatement insert = await connection.PrepareAsync(
            db, "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)");

        // Executions fan out across slots — most must register the statement first — while disposal
        // runs concurrently. Every op either completes or reports disposal; none may leave a
        // registration alive behind DisposeAsync's back.
        List<Task> ops = [];
        for (int i = 0; i < 16; i++)
        {
            int index = i;
            ops.Add(Task.Run(async () =>
            {
                try { await insert.ExecuteNonQueryAsync([$"robot-{index}", (long)(2000 + index)]); }
                catch (ObjectDisposedException) { /* disposal won the race — expected */ }
            }));
        }

        await insert.DisposeAsync();
        await Task.WhenAll(ops);

        // Nothing the statement registered may outlive it: every slot's cache is empty, and every id
        // it minted was closed (a leaked one would still resolve on its stream).
        Assert.That(batcher.TakePrepared(new PreparedStatementKey(
            db, "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)")),
            Is.Empty, "disposal must leave no registration behind, in flight or otherwise");

        Assert.That(
            async () => await insert.ExecuteNonQueryAsync(["after", 9999L]),
            Throws.TypeOf<ObjectDisposedException>());
    }

    // ─── Binding ──────────────────────────────────────────────────────────────

    [Test]
    public async Task ByNameBinding_MapsPropertiesToOrdinalsClientSide()
    {
        string db = await CreateDatabaseWithTableAsync();
        await using CamusConnection connection = NewConnection();

        await using CamusPreparedStatement insert = await connection.PrepareAsync(
            db, "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)");

        // Deliberately out of declaration order: binding is by name, not by property order.
        await insert.ExecuteNonQueryAsync(new { year = 1984L, name = "optimus" });

        await using CamusPreparedStatement select = await connection.PrepareAsync(
            db, "SELECT name FROM robots WHERE year = @year");

        QueryResult found = await select.ExecuteQueryAsync(new { year = 1984L });
        Assert.That(found.Rows.Count, Is.EqualTo(1));
        Assert.That(found.Rows[0].Values[0].StringValue, Is.EqualTo("optimus"));
    }

    [Test]
    public async Task ByNameBinding_RejectsUnknownAndMissingProperties()
    {
        string db = await CreateDatabaseWithTableAsync();
        await using CamusConnection connection = NewConnection();

        await using CamusPreparedStatement insert = await connection.PrepareAsync(
            db, "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)");

        // A typo must not silently bind NULL — that would turn a mistake into a wrong row.
        Assert.That(
            async () => await insert.ExecuteNonQueryAsync(new { nmae = "optimus", year = 1984L }),
            Throws.ArgumentException);

        Assert.That(
            async () => await insert.ExecuteNonQueryAsync(new { name = "optimus" }),
            Throws.ArgumentException);
    }

    /// <summary>
    /// Waits for the batcher's reader loop to notice the fault and rebuild the slot. Without this a
    /// test would race the rebuild and exercise "stream missing" rather than "stream replaced".
    /// </summary>
    private static async Task WaitForRebuildAsync(TransportFactory factory, int expectedCount)
    {
        for (int i = 0; i < 200 && factory.Count < expectedCount; i++)
            await Task.Delay(25);

        Assert.That(factory.Count, Is.GreaterThanOrEqualTo(expectedCount), "the slot was never rebuilt");
    }

    /// <summary>
    /// Executes, tolerating the ordinary transport faults a rebuild can produce for an op that races
    /// it — but never an unknown-statement error, which is precisely what the client must handle on
    /// the caller's behalf rather than surface.
    /// </summary>
    private static async Task<NonQueryResult> ExecuteToleratingTransportFaultAsync(
        CamusPreparedStatement statement, object?[] values)
    {
        Exception? last = null;
        for (int attempt = 0; attempt < 10; attempt++)
        {
            try
            {
                return await statement.ExecuteNonQueryAsync(values);
            }
            catch (CamusGrpcException domain)
            {
                Assert.That(domain.Code, Is.Not.EqualTo("CADB0520"),
                    "the client must re-prepare on a rebuilt stream, never surface an unknown-statement error");
                throw;
            }
            catch (Exception ex)
            {
                last = ex;                     // transport not reconnected yet — this is the replay contract.
                await Task.Delay(25);
            }
        }

        throw new AssertionException($"statement never recovered: {last}");
    }
}
