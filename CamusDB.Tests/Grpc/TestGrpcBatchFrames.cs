
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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Catalogs;
using CamusDB.Grpc;
using CamusDB.App.Grpc;
using CamusDB.App.Services;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// Stream frames through the real <c>BatchExecute</c> entry point: a request frame is admitted item
/// by item exactly as separate messages are, and a response frame reaches only a peer that sent a
/// request frame. Every test reads the stream the way a client does — a response frame is opened in
/// place, and each item is then a single message with its own <c>request_id</c>.
/// </summary>
[TestFixture]
[NonParallelizable]
public class TestGrpcBatchFrames : BaseTest
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(30);

    private CommandExecutor serviceExecutor = null!;
    private CamusSqlService service = null!;

    [SetUp]
    public void SetUpGrpcService()
    {
        serviceExecutor = new(new CommandValidator(Options), new CatalogsManager(logger), logger, Options,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
        service = ServiceWith(Options);
    }

    [TearDown]
    public async Task TearDownGrpcService()
    {
        try { await serviceExecutor.DisposeAsync(); } catch { }
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    /// <summary>The stream limits are read by the service, so a test that needs other limits needs
    /// another service over the same engine.</summary>
    private CamusSqlService ServiceWith(CamusDBOptions serviceOptions)
        => new(serviceExecutor, new HttpTransactionCoordinator(serviceExecutor), logger,
            TestHostApplicationLifetime.Instance, new ForegroundRequestGauge(), serviceOptions);

    private async Task<string> CreateDatabaseWithTableAsync()
    {
        string db = "framesdb" + Guid.NewGuid().ToString("n");
        await service.ExecuteDdl(new SqlRequest { Database = "", Sql = $"CREATE DATABASE {db}" }, new TestServerCallContext());
        TrackDatabase(db, serviceExecutor);
        await service.ExecuteDdl(
            new SqlRequest { Database = db, Sql = "CREATE TABLE items (id INT64 PRIMARY KEY NOT NULL, name STRING)" },
            new TestServerCallContext());
        return db;
    }

    private static BatchExecuteRequest Op(BatchStatementKind kind, int id, string db, string sql = "", TxnHandle? handle = null, int startRef = 0)
    {
        SqlRequest request = new() { Database = db, Sql = sql };
        if (handle is not null)
            request.TxnHandle = handle;
        return new BatchExecuteRequest { RequestId = id, Kind = kind, Request = request, TxnStartRef = startRef };
    }

    private static BatchExecuteRequest Start(int id, string db) => Op(BatchStatementKind.Start, id, db);

    /// <summary>A conditional commit that names the START earlier in its frame, or carries the handle.</summary>
    private static BatchExecuteRequest CommitIfOk(int id, string db, int startRef = 0, TxnHandle? handle = null)
        => Op(BatchStatementKind.CommitIfOk, id, db, handle: handle, startRef: startRef);

    private static bool IsError(BatchExecuteResponse message)
        => message.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Error;

    private static bool IsRow(BatchExecuteResponse message)
        => message.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Row;

    private static BatchExecuteRequest Query(int id, string db, string sql, TxnHandle? handle = null, int startRef = 0)
        => Op(BatchStatementKind.Query, id, db, sql, handle, startRef);

    private static BatchExecuteRequest NonQuery(int id, string db, string sql, TxnHandle? handle = null, int startRef = 0)
        => Op(BatchStatementKind.NonQuery, id, db, sql, handle, startRef);

    private static BatchExecuteRequest Frame(params BatchExecuteRequest[] items)
    {
        BatchExecuteRequest frame = new() { Kind = BatchStatementKind.Frame };
        frame.Items.AddRange(items);
        return frame;
    }

    private static bool IsFrame(BatchExecuteResponse message)
        => message.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Frame;

    private static List<BatchExecuteResponse> Flatten(IEnumerable<BatchExecuteResponse> written)
    {
        List<BatchExecuteResponse> flat = new();
        foreach (BatchExecuteResponse message in written)
        {
            if (IsFrame(message))
            {
                Assert.IsFalse(message.Frame.Items.Any(IsFrame), "an item of a response frame never holds a frame");
                flat.AddRange(message.Frame.Items);
            }
            else
            {
                flat.Add(message);
            }
        }
        return flat;
    }

    private static List<BatchExecuteResponse> ForId(IEnumerable<BatchExecuteResponse> flat, int id)
        => flat.Where(m => m.RequestId == id).ToList();

    /// <summary>Runs a closed sequence of stream messages and returns the single messages answered.</summary>
    private async Task<List<BatchExecuteResponse>> RunAsync(CamusSqlService target, params BatchExecuteRequest[] messages)
    {
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        await target.BatchExecute(new FakeAsyncStreamReader<BatchExecuteRequest>(messages), transport, new TestServerCallContext())
            .WaitAsync(Patience);
        return Flatten(transport.Written);
    }

    /// <summary>
    /// Waits for the first answer to one op on an open stream. It looks inside response frames, as a
    /// client does: on a stream that carried a request frame, any answer may arrive in a frame.
    /// </summary>
    private static async Task<BatchExecuteResponse> AnswerToAsync(GatedStreamWriter<BatchExecuteResponse> transport, int id)
    {
        DateTime deadline = DateTime.UtcNow + Patience;
        while (true)
        {
            BatchExecuteResponse? answer = Flatten(transport.Written).FirstOrDefault(m => m.RequestId == id);
            if (answer is not null)
                return answer;
            Assert.Less(DateTime.UtcNow, deadline, $"op {id} was never answered");
            await Task.Delay(10);
        }
    }

    /// <summary>What a query answered, reduced to what a client sees: column names, then row values.</summary>
    private static string Shape(List<BatchExecuteResponse> messages)
        => string.Join(" | ", messages.Select(m => m.PayloadCase switch
        {
            BatchExecuteResponse.PayloadOneofCase.Schema => "schema:" + string.Join(",", m.Schema.Columns.Select(c => c.Name)),
            BatchExecuteResponse.PayloadOneofCase.Row => "row:" + string.Join(",", m.Row.Values.Select(v => v.ToString())),
            BatchExecuteResponse.PayloadOneofCase.NonQuery => "nonquery:" + m.NonQuery.AffectedRows,
            BatchExecuteResponse.PayloadOneofCase.Error => "error:" + m.Error.Code,
            _ => m.PayloadCase.ToString(),
        }));

    // ─── Negotiation ──────────────────────────────────────────────────────────

    [Test]
    public async Task TheStreamAnnouncesFrameSupportOnItsResponseHeaders()
    {
        TestServerCallContext context = new();
        await service.BatchExecute(
            new FakeAsyncStreamReader<BatchExecuteRequest>(Array.Empty<BatchExecuteRequest>()),
            new GatedStreamWriter<BatchExecuteResponse>(), context).WaitAsync(Patience);

        Assert.IsNotNull(context.ResponseHeaders, "the header must be written even on a stream that carries no op");
        Assert.AreEqual("2", context.ResponseHeaders!.GetValue(BatchFrames.HeaderName),
            "version 2 is what tells a client it may name a START inside a frame and send COMMIT_IF_OK");
        Assert.AreEqual(BatchFrames.Version.ToString(), context.ResponseHeaders!.GetValue(BatchFrames.HeaderName));
    }

    // ─── Request frames ───────────────────────────────────────────────────────

    [Test]
    public async Task AMixedFrameAnswersEveryItemByItsOwnIdAndAFailureStaysAlone()
    {
        string db = await CreateDatabaseWithTableAsync();

        List<BatchExecuteResponse> flat = await RunAsync(service,
            Frame(
                NonQuery(1, db, "INSERT INTO items (id, name) VALUES (1, 'one')"),
                Query(2, db, "SELECT nope FROM missing_table"),
                NonQuery(3, db, "INSERT INTO items (id, name) VALUES (3, 'three')"),
                Op(BatchStatementKind.Commit, 4, db),
                Op(BatchStatementKind.Prepare, 5, db, "SELECT id FROM items WHERE id = @id")));

        // Autocommit ops of one stream run at the same time, so the check reads on a later stream.
        flat.AddRange(await RunAsync(service, Query(6, db, "SELECT id FROM items ORDER BY id")));

        Assert.AreEqual(1, ForId(flat, 1).Single().NonQuery.AffectedRows);
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.Error, ForId(flat, 2).Single().PayloadCase);
        Assert.AreEqual(1, ForId(flat, 3).Single().NonQuery.AffectedRows);
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ForId(flat, 4).Single().Error.Code, "a COMMIT without a handle fails alone");
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.PrepareReply, ForId(flat, 5).Single().PayloadCase);
        Assert.AreEqual(2, ForId(flat, 6).Count(m => m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Row),
            "both inserts beside the failed items were applied");
    }

    [Test]
    public async Task TheSameOpsAnswerTheSameFramedAndUnframed()
    {
        string db = await CreateDatabaseWithTableAsync();
        await RunAsync(service,
            NonQuery(1, db, "INSERT INTO items (id, name) VALUES (1, 'one')"),
            NonQuery(2, db, "INSERT INTO items (id, name) VALUES (2, 'two')"));

        BatchExecuteRequest[] ops =
        {
            Query(10, db, "SELECT id, name FROM items ORDER BY id"),
            Query(11, db, "SELECT name FROM items WHERE id = 2"),
            Query(12, db, "SELECT broken FROM"),
            NonQuery(13, db, "UPDATE items SET name = 'same' WHERE id = 99"),
        };

        List<BatchExecuteResponse> alone = await RunAsync(service, ops.Select(o => o.Clone()).ToArray());
        List<BatchExecuteResponse> framed = await RunAsync(service, Frame(ops.Select(o => o.Clone()).ToArray()));

        foreach (BatchExecuteRequest op in ops)
            Assert.AreEqual(Shape(ForId(alone, op.RequestId)), Shape(ForId(framed, op.RequestId)), $"op {op.RequestId}");

        Assert.IsTrue(Shape(ForId(framed, 10)).StartsWith("schema:id,name | row:", StringComparison.Ordinal));
    }

    [Test]
    public async Task ATransactionInOneFrameRunsInItemOrder()
    {
        string db = await CreateDatabaseWithTableAsync();

        ChannelAsyncStreamReader<BatchExecuteRequest> requests = new();
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        Task server = service.BatchExecute(requests, transport, new TestServerCallContext());

        requests.Push(Frame(Op(BatchStatementKind.Start, 1, db)));
        TxnHandle handle = (await AnswerToAsync(transport, 1)).StartReply;

        // Statements and COMMIT in one frame. Each later item depends on the one before it, so any
        // order other than item order fails: the update finds no row, or the commit runs too early.
        requests.Push(Frame(
            NonQuery(2, db, "INSERT INTO items (id, name) VALUES (1, 'first')", handle),
            NonQuery(3, db, "UPDATE items SET name = 'second' WHERE id = 1", handle),
            Query(4, db, "SELECT name FROM items WHERE id = 1", handle),
            Op(BatchStatementKind.Commit, 5, db, handle: handle)));
        requests.Complete();
        await server.WaitAsync(Patience);

        List<BatchExecuteResponse> flat = Flatten(transport.Written);
        Assert.AreEqual(1, ForId(flat, 2).Single().NonQuery.AffectedRows);
        Assert.AreEqual(1, ForId(flat, 3).Single().NonQuery.AffectedRows, "the update ran after the insert");
        Assert.AreEqual("second", ForId(flat, 4).Single(m => m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Row).Row.Values[0].StringValue);
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.CommitReply, ForId(flat, 5).Single().PayloadCase);

        List<BatchExecuteResponse> after = await RunAsync(service, Query(9, db, "SELECT name FROM items WHERE id = 1"));
        Assert.AreEqual("second", after.Single(m => m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Row).Row.Values[0].StringValue);
    }

    [Test]
    public async Task TwoTransactionsInterleavedAcrossFramesKeepTheirOwnOrder()
    {
        string db = await CreateDatabaseWithTableAsync();

        ChannelAsyncStreamReader<BatchExecuteRequest> requests = new();
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        Task server = service.BatchExecute(requests, transport, new TestServerCallContext());

        requests.Push(Frame(Op(BatchStatementKind.Start, 1, db), Op(BatchStatementKind.Start, 2, db)));
        TxnHandle a = (await AnswerToAsync(transport, 1)).StartReply;
        TxnHandle b = (await AnswerToAsync(transport, 2)).StartReply;

        requests.Push(Frame(
            NonQuery(10, db, "INSERT INTO items (id, name) VALUES (1, 'a0')", a),
            NonQuery(20, db, "INSERT INTO items (id, name) VALUES (2, 'b0')", b),
            NonQuery(11, db, "UPDATE items SET name = 'a1' WHERE id = 1", a)));
        requests.Push(Frame(
            NonQuery(21, db, "UPDATE items SET name = 'b1' WHERE id = 2", b),
            NonQuery(12, db, "UPDATE items SET name = 'a2' WHERE id = 1", a),
            Op(BatchStatementKind.Commit, 22, db, handle: b),
            Op(BatchStatementKind.Commit, 13, db, handle: a)));
        requests.Complete();
        await server.WaitAsync(Patience);

        List<BatchExecuteResponse> flat = Flatten(transport.Written);
        foreach (int id in new[] { 10, 11, 12, 20, 21 })
            Assert.AreEqual(1, ForId(flat, id).Single().NonQuery.AffectedRows, $"op {id} found the row its predecessor wrote");
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.CommitReply, ForId(flat, 13).Single().PayloadCase);
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.CommitReply, ForId(flat, 22).Single().PayloadCase);

        List<BatchExecuteResponse> after = await RunAsync(service, Query(9, db, "SELECT name FROM items ORDER BY id"));
        CollectionAssert.AreEqual(new[] { "a2", "b1" },
            after.Where(m => m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Row).Select(m => m.Row.Values[0].StringValue));
    }

    [Test]
    public async Task ItemsPastTheItemLimitAreRefusedAsRetryableAndNeverRun()
    {
        string db = await CreateDatabaseWithTableAsync();

        const int extra = 3;
        BatchExecuteRequest[] items = Enumerable.Range(1, BatchFrames.MaxItems + extra)
            .Select(i => NonQuery(i, db, $"INSERT INTO items (id, name) VALUES ({i}, 'n')"))
            .ToArray();

        List<BatchExecuteResponse> flat = await RunAsync(service, Frame(items));
        flat.AddRange(await RunAsync(service, Query(9000, db, "SELECT COUNT(*) FROM items")));

        for (int i = 1; i <= BatchFrames.MaxItems; i++)
            Assert.AreEqual(1, ForId(flat, i).Single().NonQuery.AffectedRows, $"item {i} is inside the limit and runs");
        for (int i = BatchFrames.MaxItems + 1; i <= BatchFrames.MaxItems + extra; i++)
            Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ForId(flat, i).Single().Error.Code, $"item {i} is past the limit");

        Assert.AreEqual(BatchFrames.MaxItems,
            ForId(flat, 9000).Single(m => m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Row).Row.Values[0].Int64Value,
            "a refused item must not have run");
    }

    [Test]
    public async Task AnEmptyFrameStartsNothingAndANestedFrameIsNeverRun()
    {
        string db = await CreateDatabaseWithTableAsync();

        List<BatchExecuteResponse> flat = await RunAsync(service,
            Frame(),
            Frame(
                NonQuery(1, db, "INSERT INTO items (id, name) VALUES (1, 'outer')"),
                Frame(
                    NonQuery(2, db, "INSERT INTO items (id, name) VALUES (2, 'nested')"),
                    Frame(NonQuery(3, db, "INSERT INTO items (id, name) VALUES (3, 'deeper')"))),
                NonQuery(4, db, "INSERT INTO items (id, name) VALUES (4, 'outer')")));
        flat.AddRange(await RunAsync(service, Query(9, db, "SELECT id FROM items ORDER BY id")));

        Assert.AreEqual(1, ForId(flat, 1).Single().NonQuery.AffectedRows);
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ForId(flat, 2).Single().Error.Code, "an op inside a nested frame is refused by its own id");
        Assert.IsEmpty(ForId(flat, 3), "a deeper frame is not followed at all");
        Assert.AreEqual(1, ForId(flat, 4).Single().NonQuery.AffectedRows, "the item after the nested frame still runs");
        Assert.IsEmpty(ForId(flat, 0), "a frame itself is never answered");

        CollectionAssert.AreEqual(new long[] { 1, 4 },
            ForId(flat, 9).Where(m => m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Row).Select(m => m.Row.Values[0].Int64Value));
    }

    [Test]
    public async Task AFrameLargerThanTheReadBufferMakesProgress()
    {
        string db = await CreateDatabaseWithTableAsync();

        // One execution slot, so eight read-buffer slots — and a frame of 60 ops, several of them
        // chained on one transaction. Slots taken up front would block the read loop for ever.
        CamusSqlService narrow = ServiceWith(Options with { GrpcBatchMaxInFlight = 1 });

        ChannelAsyncStreamReader<BatchExecuteRequest> requests = new();
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        Task server = narrow.BatchExecute(requests, transport, new TestServerCallContext());

        requests.Push(Op(BatchStatementKind.Start, 1, db));
        TxnHandle handle = (await AnswerToAsync(transport, 1)).StartReply;

        List<BatchExecuteRequest> items = new();
        for (int i = 0; i < 30; i++)
        {
            items.Add(NonQuery(100 + i, db, $"INSERT INTO items (id, name) VALUES ({100 + i}, 'txn')", handle));
            items.Add(Query(200 + i, db, "SELECT 1"));
        }
        items.Add(Op(BatchStatementKind.Commit, 300, db, handle: handle));

        requests.Push(Frame(items.ToArray()));
        requests.Complete();
        await server.WaitAsync(TimeSpan.FromSeconds(60));

        List<BatchExecuteResponse> flat = Flatten(transport.Written);
        for (int i = 0; i < 30; i++)
        {
            Assert.AreEqual(1, ForId(flat, 100 + i).Single().NonQuery.AffectedRows);
            Assert.IsTrue(ForId(flat, 200 + i).Any(m => m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.QueryComplete));
        }
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.CommitReply, ForId(flat, 300).Single().PayloadCase);
    }

    [Test]
    public async Task AClientThatLeavesWithAWriteInProgressEndsTheStreamCleanly()
    {
        string db = await CreateDatabaseWithTableAsync();

        using CancellationTokenSource client = new();
        ChannelAsyncStreamReader<BatchExecuteRequest> requests = new();
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        Task server = service.BatchExecute(requests, transport, new TestServerCallContext(client.Token));

        requests.Push(Frame(Op(BatchStatementKind.Start, 1, db)));
        TxnHandle handle = (await AnswerToAsync(transport, 1)).StartReply;
        requests.Push(Frame(NonQuery(2, db, "INSERT INTO items (id, name) VALUES (1, 'open')", handle)));
        Assert.AreEqual(1, (await AnswerToAsync(transport, 2)).NonQuery.AffectedRows);

        // The client stops reading in the middle of a result, then goes away.
        transport.ParkNextWrite();
        requests.Push(Frame(Query(3, db, "SELECT 1"), Query(4, db, "SELECT 2")));
        await transport.WriteIsParked.WaitAsync(Patience);
        client.Cancel();

        await server.WaitAsync(Patience);

        int writtenAtTheEnd = transport.Written.Count;
        await Task.Delay(200);
        Assert.AreEqual(writtenAtTheEnd, transport.Written.Count, "no write may follow the end of the call");

        // The transaction the stream left open was rolled back: its row is gone and its lock is free.
        List<BatchExecuteResponse> after = await RunAsync(service, NonQuery(9, db, "INSERT INTO items (id, name) VALUES (1, 'again')"));
        Assert.AreEqual(1, ForId(after, 9).Single().NonQuery.AffectedRows);
    }

    [Test]
    public async Task AClientThatAcceptsFramesInItsRequestHeadersGetsALoneResultAsOneMessage()
    {
        string db = await CreateDatabaseWithTableAsync();
        await RunAsync(service, Enumerable.Range(1, 100)
            .Select(i => NonQuery(i, db, $"INSERT INTO items (id, name) VALUES ({i}, 'row{i}')")).ToArray());

        TestServerCallContext context = new();
        context.RequestHeaders.Add(BatchFrames.AcceptHeaderName, "1");
        GatedStreamWriter<BatchExecuteResponse> transport = new();

        // One lone query, never a request frame: only the accept header can make the answer a frame.
        await service.BatchExecute(
            new FakeAsyncStreamReader<BatchExecuteRequest>(new[] { Query(7, db, "SELECT id FROM items ORDER BY id") }),
            transport, context).WaitAsync(Patience);

        IReadOnlyList<BatchExecuteResponse> written = transport.Written;
        List<BatchExecuteResponse> flat = Flatten(written);
        Assert.AreEqual(102, ForId(flat, 7).Count, "schema, 100 rows, terminator");
        CollectionAssert.AreEqual(Enumerable.Range(1, 100).Select(i => (long)i),
            ForId(flat, 7).Where(m => m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Row).Select(m => m.Row.Values[0].Int64Value));
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.QueryComplete, ForId(flat, 7).Last().PayloadCase);
        Assert.LessOrEqual(written.Count, 8, $"a lone 100-row read must travel in a few frames, not 102 messages; got {written.Count}");
        Assert.IsTrue(written.Any(IsFrame));
    }

    [Test]
    public async Task ALoneOneRowReadIsOneStreamMessageForAnAcceptingClient()
    {
        string db = await CreateDatabaseWithTableAsync();
        await RunAsync(service, NonQuery(1, db, "INSERT INTO items (id, name) VALUES (1, 'one')"));

        TestServerCallContext context = new();
        context.RequestHeaders.Add(BatchFrames.AcceptHeaderName, "1");
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        await service.BatchExecute(
            new FakeAsyncStreamReader<BatchExecuteRequest>(new[] { Query(7, db, "SELECT id, name FROM items WHERE id = 1") }),
            transport, context).WaitAsync(Patience);

        Assert.AreEqual(1, transport.Written.Count, "schema, the row and the terminator travel as one frame");
        Assert.IsTrue(IsFrame(transport.Written[0]));
        Assert.AreEqual(3, transport.Written[0].Frame.Items.Count);
    }

    [Test]
    public async Task AnAcceptHeaderBelowTheContractVersionIsNoProof()
    {
        string db = await CreateDatabaseWithTableAsync();
        await RunAsync(service, NonQuery(1, db, "INSERT INTO items (id, name) VALUES (1, 'one')"));

        TestServerCallContext context = new();
        context.RequestHeaders.Add(BatchFrames.AcceptHeaderName, "0");
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        await service.BatchExecute(
            new FakeAsyncStreamReader<BatchExecuteRequest>(new[] { Query(7, db, "SELECT id, name FROM items WHERE id = 1") }),
            transport, context).WaitAsync(Patience);

        Assert.AreEqual(3, transport.Written.Count);
        Assert.IsFalse(transport.Written.Any(IsFrame));
    }

    [Test]
    public async Task AQueryThatFailsAfterRowsAnswersOnlyItsError()
    {
        string db = await CreateDatabaseWithTableAsync();
        await RunAsync(service,
            NonQuery(1, db, "INSERT INTO items (id, name) VALUES (1, '10')"),
            NonQuery(2, db, "INSERT INTO items (id, name) VALUES (2, 'not a number')"));

        // The cast fails on the second row, after the first row was produced.
        List<BatchExecuteResponse> flat = await RunAsync(service,
            Frame(Query(7, db, "SELECT CAST(name AS INT64) FROM items ORDER BY id")));

        List<BatchExecuteResponse> answers = ForId(flat, 7);
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.Error, answers.Last().PayloadCase);
        Assert.IsFalse(answers.Any(m => m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Row), "held rows of a failed query are dropped");
    }

    // ─── Response frames ──────────────────────────────────────────────────────

    [Test]
    public async Task APeerThatSentAFrameReceivesASmallResultInFewMessages()
    {
        string db = await CreateDatabaseWithTableAsync();
        await RunAsync(service, Enumerable.Range(1, 10)
            .Select(i => NonQuery(i, db, $"INSERT INTO items (id, name) VALUES ({i}, 'row{i}')")).ToArray());

        IReadOnlyList<BatchExecuteResponse> written = await ResultBehindAParkedWriteAsync(db, framed: true);

        List<BatchExecuteResponse> result = ForId(Flatten(written), 60);
        Assert.AreEqual(12, result.Count, "schema, ten rows, terminator");
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.Schema, result[0].PayloadCase);
        CollectionAssert.AreEqual(Enumerable.Range(1, 10).Select(i => (long)i),
            result.Skip(1).Take(10).Select(m => m.Row.Values[0].Int64Value), "rows keep the order of the cursor");
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.QueryComplete, result[11].PayloadCase);

        int carriers = written.Count(m => IsFrame(m) ? m.Frame.Items.Any(i => i.RequestId == 60) : m.RequestId == 60);
        Assert.AreEqual(1, carriers, "a result that waited as a whole travels as one stream message");
    }

    [Test]
    public async Task APeerThatSentNoFrameReceivesEveryMessageOnItsOwn()
    {
        string db = await CreateDatabaseWithTableAsync();
        await RunAsync(service, Enumerable.Range(1, 10)
            .Select(i => NonQuery(i, db, $"INSERT INTO items (id, name) VALUES ({i}, 'row{i}')")).ToArray());

        IReadOnlyList<BatchExecuteResponse> written = await ResultBehindAParkedWriteAsync(db, framed: false);

        Assert.IsFalse(written.Any(IsFrame), "a client built before frames does not know the frame payload");
        Assert.AreEqual(12, written.Count(m => m.RequestId == 60));
    }

    // ─── A transaction in one frame: txn_start_ref and COMMIT_IF_OK ───────────

    [Test]
    public async Task AWholeTransactionRidesOneFrameByNamingItsStart()
    {
        string db = await CreateDatabaseWithTableAsync();

        // Every item after the START names it instead of a handle, and each depends on the one
        // before it, so any order other than item order fails: the update finds no row, the read sees
        // the wrong name, or the commit runs too early.
        List<BatchExecuteResponse> flat = await RunAsync(service,
            Frame(
                Start(1, db),
                NonQuery(2, db, "INSERT INTO items (id, name) VALUES (1, 'first')", startRef: 1),
                NonQuery(3, db, "UPDATE items SET name = 'second' WHERE id = 1", startRef: 1),
                Query(4, db, "SELECT name FROM items WHERE id = 1", startRef: 1),
                CommitIfOk(5, db, startRef: 1)));

        TxnHandle handle = ForId(flat, 1).Single().StartReply;
        Assert.Greater(handle.TxnIdPt, 0, "the START still answers with the handle it minted");
        Assert.AreEqual(1, ForId(flat, 2).Single().NonQuery.AffectedRows);
        Assert.AreEqual(1, ForId(flat, 3).Single().NonQuery.AffectedRows, "the update ran after the insert");
        Assert.AreEqual("second", ForId(flat, 4).Single(IsRow).Row.Values[0].StringValue);
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.CommitReply, ForId(flat, 5).Single().PayloadCase);

        List<BatchExecuteResponse> after = await RunAsync(service, Query(9, db, "SELECT name FROM items WHERE id = 1"));
        Assert.AreEqual("second", after.Single(IsRow).Row.Values[0].StringValue, "the conditional commit stored the row");
    }

    [Test]
    public async Task ATransactionNamedInOneFrameContinuesByHandleInTheNext()
    {
        // The bank-transfer shape: [START, reads] then [writes, COMMIT_IF_OK] by handle. The second
        // frame is pushed as soon as the handle is known, while the first frame's statement may still
        // be running, so it proves the handle joins the chain the first frame's items are on.
        string db = await CreateDatabaseWithTableAsync();

        ChannelAsyncStreamReader<BatchExecuteRequest> requests = new();
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        Task server = service.BatchExecute(requests, transport, new TestServerCallContext());

        requests.Push(Frame(
            Start(1, db),
            NonQuery(2, db, "INSERT INTO items (id, name) VALUES (1, 'first')", startRef: 1),
            Query(3, db, "SELECT name FROM items WHERE id = 1", startRef: 1)));
        TxnHandle handle = (await AnswerToAsync(transport, 1)).StartReply;

        requests.Push(Frame(
            NonQuery(4, db, "UPDATE items SET name = 'second' WHERE id = 1", handle),
            NonQuery(5, db, "UPDATE items SET name = 'third' WHERE id = 1", handle),
            CommitIfOk(6, db, handle: handle)));
        requests.Complete();
        await server.WaitAsync(Patience);

        List<BatchExecuteResponse> flat = Flatten(transport.Written);
        Assert.AreEqual(1, ForId(flat, 2).Single().NonQuery.AffectedRows);
        Assert.AreEqual("first", ForId(flat, 3).Single(IsRow).Row.Values[0].StringValue);
        Assert.AreEqual(1, ForId(flat, 4).Single().NonQuery.AffectedRows, "the second frame's update ran after the first frame's insert");
        Assert.AreEqual(1, ForId(flat, 5).Single().NonQuery.AffectedRows);
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.CommitReply, ForId(flat, 6).Single().PayloadCase);

        List<BatchExecuteResponse> after = await RunAsync(service, Query(9, db, "SELECT name FROM items WHERE id = 1"));
        Assert.AreEqual("third", after.Single(IsRow).Row.Values[0].StringValue);
    }

    [Test]
    public async Task AConditionalCommitAnswersTheFirstFailureAndStoresNothing()
    {
        string db = await CreateDatabaseWithTableAsync();

        ChannelAsyncStreamReader<BatchExecuteRequest> requests = new();
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        Task server = service.BatchExecute(requests, transport, new TestServerCallContext());

        // The read in the middle fails. The insert after it meets a transaction the failure already
        // ended, and the commit must answer the read's failure — the first one — not the insert's.
        requests.Push(Frame(
            Start(1, db),
            NonQuery(2, db, "INSERT INTO items (id, name) VALUES (1, 'one')", startRef: 1),
            Query(3, db, "SELECT nope FROM missing_table", startRef: 1),
            NonQuery(4, db, "INSERT INTO items (id, name) VALUES (2, 'two')", startRef: 1),
            CommitIfOk(5, db, startRef: 1)));
        TxnHandle handle = (await AnswerToAsync(transport, 1)).StartReply;
        BatchExecuteResponse commit = await AnswerToAsync(transport, 5);

        // What a client does next: roll back by handle. It must succeed as a no-op.
        requests.Push(Op(BatchStatementKind.Rollback, 6, db, handle: handle));
        requests.Complete();
        await server.WaitAsync(Patience);

        List<BatchExecuteResponse> flat = Flatten(transport.Written);
        Assert.AreEqual(1, ForId(flat, 2).Single().NonQuery.AffectedRows, "the insert before the failure ran");
        BatchExecuteResponse read = ForId(flat, 3).Single();
        Assert.IsTrue(IsError(read));
        Assert.IsTrue(IsError(ForId(flat, 4).Single()), "a statement after the failure meets an ended transaction");
        Assert.IsTrue(IsError(commit), "the conditional commit did not commit");
        Assert.AreEqual(read.Error.Code, commit.Error.Code, "the commit answers the first failure's code");
        Assert.AreEqual(read.Error.Message, commit.Error.Message, "and its message");
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.RollbackReply, ForId(flat, 6).Single().PayloadCase);

        List<BatchExecuteResponse> after = await RunAsync(service, Query(9, db, "SELECT COUNT(*) FROM items"));
        Assert.AreEqual(0, after.Single(IsRow).Row.Values[0].Int64Value, "nothing of the transaction was stored");
    }

    [Test]
    public async Task AConditionalCommitRemembersAFailureFromAnEarlierFrame()
    {
        // "Earlier" means earlier on the transaction, not earlier in the frame: a commit that is sent
        // alone after a failed statement answers that failure as well.
        string db = await CreateDatabaseWithTableAsync();

        ChannelAsyncStreamReader<BatchExecuteRequest> requests = new();
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        Task server = service.BatchExecute(requests, transport, new TestServerCallContext());

        requests.Push(Frame(Start(1, db)));
        TxnHandle handle = (await AnswerToAsync(transport, 1)).StartReply;
        requests.Push(Frame(
            NonQuery(2, db, "INSERT INTO items (id, name) VALUES (1, 'one')", handle),
            Query(3, db, "SELECT nope FROM missing_table", handle)));
        BatchExecuteResponse read = await AnswerToAsync(transport, 3);
        requests.Push(CommitIfOk(4, db, handle: handle));
        requests.Complete();
        await server.WaitAsync(Patience);

        BatchExecuteResponse commit = ForId(Flatten(transport.Written), 4).Single();
        Assert.IsTrue(IsError(read));
        Assert.IsTrue(IsError(commit));
        Assert.AreEqual(read.Error.Code, commit.Error.Code);
    }

    [Test]
    public async Task AConditionalCommitWithNoEarlierFailureIsAPlainCommit()
    {
        string db = await CreateDatabaseWithTableAsync();

        ChannelAsyncStreamReader<BatchExecuteRequest> requests = new();
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        Task server = service.BatchExecute(requests, transport, new TestServerCallContext());

        // Outside any frame, by handle, with nothing failed before it.
        requests.Push(Start(1, db));
        TxnHandle handle = (await AnswerToAsync(transport, 1)).StartReply;
        requests.Push(NonQuery(2, db, "INSERT INTO items (id, name) VALUES (1, 'one')", handle));
        requests.Push(CommitIfOk(3, db, handle: handle));
        requests.Complete();
        await server.WaitAsync(Patience);

        List<BatchExecuteResponse> flat = Flatten(transport.Written);
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.CommitReply, ForId(flat, 3).Single().PayloadCase);

        List<BatchExecuteResponse> after = await RunAsync(service, Query(9, db, "SELECT COUNT(*) FROM items"));
        Assert.AreEqual(1, after.Single(IsRow).Row.Values[0].Int64Value);

        // Without a transaction to name, it fails alone, as COMMIT does.
        List<BatchExecuteResponse> bare = await RunAsync(service, CommitIfOk(11, db));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, bare.Single().Error.Code);
    }

    [Test]
    public async Task AFailedStartFailsEveryItemThatNamesItAndRunsNone()
    {
        string db = await CreateDatabaseWithTableAsync();
        string missing = "nosuchdb" + Guid.NewGuid().ToString("n");

        List<BatchExecuteResponse> flat = await RunAsync(service,
            Frame(
                Start(1, missing),
                NonQuery(2, missing, "INSERT INTO items (id, name) VALUES (1, 'one')", startRef: 1),
                CommitIfOk(3, missing, startRef: 1),
                // A neighbour that names nothing is untouched.
                NonQuery(4, db, "INSERT INTO items (id, name) VALUES (4, 'four')")));

        BatchExecuteResponse start = ForId(flat, 1).Single();
        Assert.IsTrue(IsError(start), "the START on a missing database fails");
        foreach (int id in new[] { 2, 3 })
        {
            BatchExecuteResponse answer = ForId(flat, id).Single();
            Assert.IsTrue(IsError(answer), $"op {id} is answered, and with an error");
            Assert.AreEqual(start.Error.Code, answer.Error.Code, $"op {id} answers the START's failure");
            Assert.AreEqual(start.Error.Message, answer.Error.Message);
        }
        Assert.AreEqual(1, ForId(flat, 4).Single().NonQuery.AffectedRows);
    }

    [Test]
    public async Task AStartReferenceMustPointBackAtAStartOfTheSameFrame()
    {
        string db = await CreateDatabaseWithTableAsync();

        // Outside a frame there is nothing to name.
        List<BatchExecuteResponse> lone = await RunAsync(service,
            NonQuery(1, db, "INSERT INTO items (id, name) VALUES (1, 'lone')", startRef: 1));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, lone.Single().Error.Code);

        List<BatchExecuteResponse> flat = await RunAsync(service,
            Frame(
                // Forward: the START comes later in the frame.
                NonQuery(10, db, "INSERT INTO items (id, name) VALUES (10, 'forward')", startRef: 11),
                Start(11, db),
                // Names an op that is not a START.
                NonQuery(12, db, "INSERT INTO items (id, name) VALUES (12, 'plain')"),
                NonQuery(13, db, "INSERT INTO items (id, name) VALUES (13, 'notastart')", startRef: 12),
                // A START, a PREPARE and a CLOSE may not name a START.
                Start(14, db),
                Op(BatchStatementKind.Start, 15, db, startRef: 14),
                Op(BatchStatementKind.Prepare, 16, db, "SELECT id FROM items", startRef: 14),
                // A reference and a handle together are ambiguous.
                NonQuery(17, db, "INSERT INTO items (id, name) VALUES (17, 'both')", new TxnHandle { TxnIdPt = 1, TxnIdCounter = 1 }, startRef: 14),
                // Names a START of another frame (by its id), which this frame cannot see.
                CommitIfOk(18, db, startRef: 1)),
            // A later frame cannot name the earlier frame's START either.
            Frame(NonQuery(19, db, "INSERT INTO items (id, name) VALUES (19, 'late')", startRef: 11)));

        foreach (int id in new[] { 10, 13, 15, 16, 17, 18, 19 })
            Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ForId(flat, id).Single().Error.Code, $"op {id} is refused");
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.StartReply, ForId(flat, 11).Single().PayloadCase, "the START itself is fine");
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.StartReply, ForId(flat, 14).Single().PayloadCase);
        Assert.AreEqual(1, ForId(flat, 12).Single().NonQuery.AffectedRows, "the plain insert ran");

        List<BatchExecuteResponse> after = await RunAsync(service, Query(9, db, "SELECT id FROM items ORDER BY id"));
        CollectionAssert.AreEqual(new[] { 12L }, after.Where(IsRow).Select(m => m.Row.Values[0].Int64Value),
            "a refused reference never ran, in or out of a transaction");
    }

    [Test]
    public async Task AConditionalCommitPastTheItemLimitIsRefusedAndTheTransactionStaysOpen()
    {
        string db = await CreateDatabaseWithTableAsync();

        ChannelAsyncStreamReader<BatchExecuteRequest> requests = new();
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        Task server = service.BatchExecute(requests, transport, new TestServerCallContext());

        // START + 255 inserts fill the limit; the 256th insert and the commit behind it are past it.
        List<BatchExecuteRequest> items = new() { Start(1, db) };
        for (int i = 1; i <= BatchFrames.MaxItems; i++)
            items.Add(NonQuery(1 + i, db, $"INSERT INTO items (id, name) VALUES ({i}, 'n')", startRef: 1));
        int commitId = 2 + BatchFrames.MaxItems;
        items.Add(CommitIfOk(commitId, db, startRef: 1));
        requests.Push(Frame(items.ToArray()));

        TxnHandle handle = (await AnswerToAsync(transport, 1)).StartReply;
        BatchExecuteResponse refusedInsert = await AnswerToAsync(transport, 1 + BatchFrames.MaxItems);
        BatchExecuteResponse refusedCommit = await AnswerToAsync(transport, commitId);
        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, refusedInsert.Error.Code, "the insert past the limit was not run");
        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, refusedCommit.Error.Code, "nor was the commit");

        // The transaction is still open: a refused op never ran, so it is no failure of the
        // transaction, and the client decides — here it commits what did run.
        requests.Push(CommitIfOk(9000, db, handle: handle));
        requests.Complete();
        await server.WaitAsync(Patience);

        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.CommitReply, ForId(Flatten(transport.Written), 9000).Single().PayloadCase);
        List<BatchExecuteResponse> after = await RunAsync(service, Query(9, db, "SELECT COUNT(*) FROM items"));
        Assert.AreEqual(BatchFrames.MaxItems - 1, after.Single(IsRow).Row.Values[0].Int64Value);
    }

    [Test]
    public async Task AStreamThatEndsRollsBackATransactionStartedInsideAFrame()
    {
        string db = await CreateDatabaseWithTableAsync();

        // No commit of any kind: teardown must roll the frame-started transaction back like any other.
        List<BatchExecuteResponse> flat = await RunAsync(service,
            Frame(
                Start(1, db),
                NonQuery(2, db, "INSERT INTO items (id, name) VALUES (1, 'open')", startRef: 1)));
        Assert.AreEqual(1, ForId(flat, 2).Single().NonQuery.AffectedRows);

        List<BatchExecuteResponse> after = await RunAsync(service, Query(9, db, "SELECT COUNT(*) FROM items"));
        Assert.AreEqual(0, after.Single(IsRow).Row.Values[0].Int64Value);
    }

    [Test]
    public async Task TwoTransactionsStartedInOneFrameKeepTheirOwnChainsAndOutcomes()
    {
        string db = await CreateDatabaseWithTableAsync();

        // Transaction A succeeds; transaction B fails on its second statement. Their items are
        // interleaved, and B's failure must not touch A's commit.
        List<BatchExecuteResponse> flat = await RunAsync(service,
            Frame(
                Start(1, db),
                Start(2, db),
                NonQuery(10, db, "INSERT INTO items (id, name) VALUES (1, 'a0')", startRef: 1),
                NonQuery(20, db, "INSERT INTO items (id, name) VALUES (2, 'b0')", startRef: 2),
                NonQuery(11, db, "UPDATE items SET name = 'a1' WHERE id = 1", startRef: 1),
                Query(21, db, "SELECT nope FROM missing_table", startRef: 2),
                CommitIfOk(22, db, startRef: 2),
                CommitIfOk(12, db, startRef: 1)));

        Assert.AreEqual(1, ForId(flat, 11).Single().NonQuery.AffectedRows, "A's update found A's row");
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.CommitReply, ForId(flat, 12).Single().PayloadCase);
        Assert.IsTrue(IsError(ForId(flat, 22).Single()));
        Assert.AreEqual(ForId(flat, 21).Single().Error.Code, ForId(flat, 22).Single().Error.Code);

        List<BatchExecuteResponse> after = await RunAsync(service, Query(9, db, "SELECT name FROM items ORDER BY id"));
        CollectionAssert.AreEqual(new[] { "a1" }, after.Where(IsRow).Select(m => m.Row.Values[0].StringValue));
    }

    /// <summary>
    /// Parks the transport on the reply of a first op, then runs a ten-row query (id 60) and a marker
    /// insert behind it on one execution slot. When the marker's row is visible from outside the
    /// stream, the query before it has completed, so its whole result waits behind the parked write —
    /// a fixed state, reached without a timing assumption. Then the transport is released.
    /// </summary>
    private async Task<IReadOnlyList<BatchExecuteResponse>> ResultBehindAParkedWriteAsync(string db, bool framed)
    {
        // One execution slot makes autocommit ops run one after another, in arrival order.
        CamusSqlService serial = ServiceWith(Options with { GrpcBatchMaxInFlight = 1 });

        ChannelAsyncStreamReader<BatchExecuteRequest> requests = new();
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        Task server = serial.BatchExecute(requests, transport, new TestServerCallContext());

        transport.ParkNextWrite();
        BatchExecuteRequest[] ops =
        {
            Query(50, db, "SELECT 1"),
            Query(60, db, "SELECT id, name FROM items WHERE id <= 10 ORDER BY id"),
            NonQuery(70, db, "INSERT INTO items (id, name) VALUES (1000, 'marker')"),
        };
        if (framed)
            requests.Push(Frame(ops));
        else
            foreach (BatchExecuteRequest op in ops)
                requests.Push(op);

        await transport.WriteIsParked.WaitAsync(Patience);

        DateTime deadline = DateTime.UtcNow + Patience;
        while (true)
        {
            List<BatchExecuteResponse> probe = await RunAsync(service, Query(1, db, "SELECT id FROM items WHERE id = 1000"));
            if (probe.Any(m => m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Row))
                break;
            Assert.Less(DateTime.UtcNow, deadline, "the marker op never ran");
            await Task.Delay(20);
        }

        requests.Complete();
        transport.Release();
        await server.WaitAsync(Patience);

        return transport.Written;
    }
}
