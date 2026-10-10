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
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.Diagnostics;

/// <summary>
/// The running-statement list on its own, with no engine: when an entry appears and leaves, what a
/// viewer may see, and that a cancel racing the end of a statement is safe. The engine-level paths
/// are covered by <c>TestQueryActivityStatements</c>.
/// </summary>
[TestFixture]
internal sealed class TestQueryActivityRegistry
{
    private static QueryActivityRegistry NewRegistry(CamusDBOptions? options = null, string node = "localhost:7070")
        => new(options ?? CamusDBOptions.Default, () => node);

    private static ExecuteSQLTicket Ticket(string sql = "SELECT 1", Principal? principal = null, CancellationToken token = default)
        => new(null!, "db", sql, null, principal, token);

    private static async IAsyncEnumerable<QueryResultRow> Rows(int count, [EnumeratorCancellation] CancellationToken token = default)
    {
        for (int i = 0; i < count; i++)
        {
            await Task.Yield();
            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue> { { "n", new ColumnValue(ColumnType.Integer64, i) } });
        }
    }

    private static async IAsyncEnumerable<QueryResultRow> Failing()
    {
        await Task.Yield();
        yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>());
        throw new InvalidOperationException("boom");
    }

    [Test]
    public async Task EntryStaysWhileTheCursorIsOpenAndLeavesWhenItIsDisposed()
    {
        QueryActivityRegistry registry = NewRegistry();
        QueryActivityEntry entry = registry.Begin(Ticket(), cancellable: true, executing: false)!;
        entry.Describe(NodeType.Select);

        await using IAsyncEnumerator<QueryResultRow> cursor = entry.Wrap(Rows(5)).GetAsyncEnumerator();
        Assert.That(await cursor.MoveNextAsync(), Is.True);

        List<QueryActivityRow> rows = registry.SnapshotQueries(ActivityViewer.All);
        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(rows[0].QueryId, Is.EqualTo(entry.Id));
        Assert.That(rows[0].RowsReturned, Is.EqualTo(1));
        Assert.That(rows[0].Phase, Is.EqualTo(QueryActivityRegistry.ExecutingPhase));
        Assert.That(rows[0].Kind, Is.EqualTo("select"));
        Assert.That(rows[0].Transport, Is.EqualTo(StatementOrigin.EmbeddedTransport));

        await cursor.DisposeAsync();
        Assert.That(registry.Count, Is.Zero);
    }

    [Test]
    public async Task EntryLeavesWhenTheCursorIsDrained()
    {
        QueryActivityRegistry registry = NewRegistry();
        QueryActivityEntry entry = registry.Begin(Ticket(), cancellable: true, executing: false)!;

        int seen = 0;
        await foreach (QueryResultRow _ in entry.Wrap(Rows(3)))
            seen++;

        Assert.That(seen, Is.EqualTo(3));
        Assert.That(registry.Count, Is.Zero);
    }

    [Test]
    public void EntryLeavesWhenTheCursorFails()
    {
        QueryActivityRegistry registry = NewRegistry();
        QueryActivityEntry entry = registry.Begin(Ticket(), cancellable: true, executing: false)!;

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (QueryResultRow _ in entry.Wrap(Failing())) { }
        });

        Assert.That(registry.Count, Is.Zero);
    }

    [Test]
    public void DisabledRegistryRegistersNothing()
    {
        QueryActivityRegistry registry = NewRegistry(CamusDBOptions.Default with { QueryActivityEnabled = false });

        Assert.That(registry.Begin(Ticket(), cancellable: true, executing: false), Is.Null);
        Assert.That(registry.Count, Is.Zero);
    }

    [Test]
    public void TurningTheRegistryOnAtRuntimeTakesEffectOnTheNextStatement()
    {
        QueryActivityRegistry registry = NewRegistry(CamusDBOptions.Default with { QueryActivityEnabled = false });
        Assert.That(registry.Begin(Ticket(), cancellable: true, executing: false), Is.Null);

        registry.ApplyOptions(CamusDBOptions.Default with { QueryActivityEnabled = true });

        QueryActivityEntry? entry = registry.Begin(Ticket(), cancellable: true, executing: false);
        Assert.That(entry, Is.Not.Null);
        entry!.End();
    }

    [Test]
    public async Task CancelStopsTheCursorWithTheCancelledError()
    {
        QueryActivityRegistry registry = NewRegistry();
        QueryActivityEntry entry = registry.Begin(Ticket(), cancellable: true, executing: false)!;

        await using IAsyncEnumerator<QueryResultRow> cursor = entry.Wrap(Rows(100), entry.CancellationToken).GetAsyncEnumerator();
        Assert.That(await cursor.MoveNextAsync(), Is.True);

        Assert.That(registry.TryCancel(entry.Id, ActivityViewer.All), Is.EqualTo(QueryCancelOutcome.Cancelled));
        Assert.That(registry.SnapshotQueries(ActivityViewer.All)[0].CancelRequested, Is.True);

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () => await cursor.MoveNextAsync());
        Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.QueryCancelled));

        await cursor.DisposeAsync();
        Assert.That(registry.Count, Is.Zero);
        Assert.That(registry.TryCancel(entry.Id, ActivityViewer.All), Is.EqualTo(QueryCancelOutcome.NotFound));
    }

    [Test]
    public void WriteIsNotCancellable()
    {
        QueryActivityRegistry registry = NewRegistry();
        QueryActivityEntry entry = registry.Begin(Ticket("UPDATE t SET v = 1"), cancellable: false, executing: true)!;

        Assert.That(registry.TryCancel(entry.Id, ActivityViewer.All), Is.EqualTo(QueryCancelOutcome.NotCancellable));
        Assert.That(entry.CancellationToken.IsCancellationRequested, Is.False);

        entry.End();
    }

    /// <summary>
    /// The row-returning path also runs <c>INSERT … RETURNING</c>, so its entry accepts no cancel
    /// until the parse shows a read. A write never gets there, so no cancel is ever accepted for it,
    /// and its token stays untouched.
    /// </summary>
    [Test]
    public void RowReturningStatementAcceptsACancelOnlyAfterItsParseShowsARead()
    {
        QueryActivityRegistry registry = NewRegistry();

        QueryActivityEntry write = registry.BeginRowReturning(Ticket("INSERT INTO t VALUES (1) RETURNING *"))!;
        Assert.That(registry.TryCancel(write.Id, ActivityViewer.All), Is.EqualTo(QueryCancelOutcome.NotCancellable), "before the parse");
        Assert.That(registry.SnapshotQueries(ActivityViewer.All).Single().Cancellable, Is.False);

        write.Describe(NodeType.Insert);
        Assert.That(registry.TryCancel(write.Id, ActivityViewer.All), Is.EqualTo(QueryCancelOutcome.NotCancellable), "a write");
        Assert.That(write.CancellationToken.IsCancellationRequested, Is.False);
        Assert.That(write.CancelRequested, Is.False);
        write.End();

        QueryActivityEntry read = registry.BeginRowReturning(Ticket())!;
        read.Describe(NodeType.Select);
        read.AllowCancel();
        Assert.That(registry.TryCancel(read.Id, ActivityViewer.All), Is.EqualTo(QueryCancelOutcome.Cancelled));
        Assert.That(read.CancellationToken.IsCancellationRequested, Is.True);
        read.End();
    }

    /// <summary>
    /// The kind is what the statement's own parse found; reading the list does not parse the text.
    /// Before the parse ends the kind is <c>unknown</c>, and the first kind recorded stays.
    /// </summary>
    [Test]
    public void KindComesFromTheStatementParse()
    {
        QueryActivityRegistry registry = NewRegistry();
        QueryActivityEntry entry = registry.Begin(Ticket("this text does not parse"), cancellable: false, executing: true)!;

        Assert.That(registry.SnapshotQueries(ActivityViewer.All).Single().Kind, Is.EqualTo("unknown"));

        entry.Describe(NodeType.CreateTable);
        entry.Describe(NodeType.Select);
        Assert.That(registry.SnapshotQueries(ActivityViewer.All).Single().Kind, Is.EqualTo("create_table"));

        entry.End();
    }

    /// <summary>
    /// A request whose token fired before the statement registered. The abort callback runs at once,
    /// and it must find the entry in the list to remove it; an entry it could not find would be
    /// published already ended and would stay listed for the life of the process.
    /// </summary>
    [Test]
    public void RequestCancelledBeforeRegistrationLeavesNoEntry()
    {
        QueryActivityRegistry registry = NewRegistry();
        using CancellationTokenSource request = new();
        request.Cancel();

        QueryActivityEntry entry = registry.Begin(Ticket(token: request.Token), cancellable: true, executing: false)!;
        Assert.That(registry.Count, Is.Zero, "the abort callback ended the entry");

        entry.End();
        Assert.That(registry.Count, Is.Zero);
        Assert.That(registry.TryCancel(entry.Id, ActivityViewer.All), Is.EqualTo(QueryCancelOutcome.NotFound));

        QueryActivityEntry rowReturning = registry.BeginRowReturning(Ticket(token: request.Token))!;
        Assert.That(registry.Count, Is.Zero);
        rowReturning.End();
    }

    /// <summary>
    /// The request token fires on another thread while statements register, many times over. No
    /// interleaving may leave an entry behind once each statement ends.
    /// </summary>
    [Test]
    public async Task RequestCancelledDuringRegistrationLeavesNoEntry()
    {
        QueryActivityRegistry registry = NewRegistry();

        for (int i = 0; i < 2000; i++)
        {
            using CancellationTokenSource request = new();
            using Barrier start = new(2);

            Task<QueryActivityEntry> begin = Task.Run(() =>
            {
                start.SignalAndWait();
                return registry.Begin(Ticket(token: request.Token), cancellable: true, executing: false)!;
            });
            Task abort = Task.Run(() => { start.SignalAndWait(); request.Cancel(); });

            await Task.WhenAll(begin, abort);
            (await begin).End();
        }

        Assert.That(registry.Count, Is.Zero);
    }

    /// <summary>
    /// A caller that takes a cursor and disposes it without reading a row drops the result the normal
    /// way. The entry must leave then, also with no request token to end it.
    /// </summary>
    [Test]
    public async Task CursorDisposedBeforeItsFirstRowLeaves()
    {
        QueryActivityRegistry registry = NewRegistry();
        QueryActivityEntry entry = registry.Begin(Ticket(), cancellable: true, executing: false)!;

        await entry.Wrap(Rows(3), entry.CancellationToken).GetAsyncEnumerator().DisposeAsync();

        Assert.That(registry.Count, Is.Zero);
        Assert.That(registry.TryCancel(entry.Id, ActivityViewer.All), Is.EqualTo(QueryCancelOutcome.NotFound));
    }

    private sealed class ThrowingCursor : IAsyncEnumerable<QueryResultRow>
    {
        public IAsyncEnumerator<QueryResultRow> GetAsyncEnumerator(CancellationToken cancellationToken = default)
            => throw new InvalidOperationException("no enumerator");
    }

    /// <summary>A source cursor that fails to give an enumerator still ends the entry.</summary>
    [Test]
    public async Task CursorWhoseSourceFailsToOpenLeaves()
    {
        QueryActivityRegistry registry = NewRegistry();
        QueryActivityEntry entry = registry.Begin(Ticket(), cancellable: true, executing: false)!;

        await using IAsyncEnumerator<QueryResultRow> cursor = entry.Wrap(new ThrowingCursor(), entry.CancellationToken).GetAsyncEnumerator();
        Assert.ThrowsAsync<InvalidOperationException>(async () => await cursor.MoveNextAsync());

        Assert.That(registry.Count, Is.Zero);
    }

    [Test]
    public void ForeignOrMalformedIdIsNotFound()
    {
        QueryActivityRegistry registry = NewRegistry();
        QueryActivityEntry entry = registry.Begin(Ticket(), cancellable: true, executing: false)!;

        // Same sequence number, another process prefix: a stale id from before a restart.
        string stale = "000000ffff" + entry.Id[ActivityIdMinter.PrefixLength..];

        Assert.That(registry.TryCancel(stale, ActivityViewer.All), Is.EqualTo(QueryCancelOutcome.NotFound));
        Assert.That(registry.TryCancel("not-an-id", ActivityViewer.All), Is.EqualTo(QueryCancelOutcome.NotFound));
        Assert.That(registry.TryCancel("", ActivityViewer.All), Is.EqualTo(QueryCancelOutcome.NotFound));
        Assert.That(entry.CancellationToken.IsCancellationRequested, Is.False);

        entry.End();
    }

    [Test]
    public void IdsCarryTheNodeTagAndAreUnique()
    {
        QueryActivityRegistry registry = NewRegistry(node: "camus1:7070");

        HashSet<string> ids = [];
        for (int i = 0; i < 100; i++)
        {
            QueryActivityEntry entry = registry.Begin(Ticket(), cancellable: false, executing: true)!;
            Assert.That(ids.Add(entry.Id), Is.True);
            Assert.That(registry.IsLocalId(entry.Id), Is.True);
            entry.End();
        }

        Assert.That(registry.IdPrefix, Has.Length.EqualTo(ActivityIdMinter.PrefixLength));
        Assert.That(NewRegistry(node: "camus1:7070").IdPrefix[..6], Is.EqualTo(registry.IdPrefix[..6]),
            "the node tag depends on the endpoint only");
        Assert.That(NewRegistry(node: "camus2:7070").IdPrefix[..6], Is.Not.EqualTo(registry.IdPrefix[..6]));
    }

    [Test]
    public void NonSuperuserSeesAndCancelsOnlyOwnStatements()
    {
        QueryActivityRegistry registry = NewRegistry();
        Principal alice = new("alice", isSuperuser: false, grants: [], userId: "u-alice");
        Principal bob = new("bob", isSuperuser: false, grants: [], userId: "u-bob");
        Principal root = new("root", isSuperuser: true, grants: [], userId: "u-root");

        QueryActivityEntry aliceEntry = registry.Begin(Ticket(principal: alice), cancellable: true, executing: false)!;
        QueryActivityEntry bobEntry = registry.Begin(Ticket(principal: bob), cancellable: true, executing: false)!;

        ActivityViewer aliceView = ActivityViewer.For(alice, authenticationEnabled: true);
        ActivityViewer rootView = ActivityViewer.For(root, authenticationEnabled: true);

        Assert.That(registry.SnapshotQueries(aliceView).Select(r => r.QueryId), Is.EqualTo(new[] { aliceEntry.Id }));
        Assert.That(registry.SnapshotQueries(rootView), Has.Count.EqualTo(2));
        Assert.That(registry.SnapshotQueries(ActivityViewer.For(null, authenticationEnabled: false)), Has.Count.EqualTo(2));
        Assert.That(registry.SnapshotQueries(ActivityViewer.For(null, authenticationEnabled: true)), Is.Empty);

        // Another user's statement is reported as not found, so the answer does not reveal it exists.
        Assert.That(registry.TryCancel(bobEntry.Id, aliceView), Is.EqualTo(QueryCancelOutcome.NotFound));
        Assert.That(bobEntry.CancellationToken.IsCancellationRequested, Is.False);

        Assert.That(registry.TryCancel(bobEntry.Id, rootView), Is.EqualTo(QueryCancelOutcome.Cancelled));

        aliceEntry.End();
        bobEntry.End();
    }

    [Test]
    public void SqlIsRedactedThenTruncatedWhenRead()
    {
        QueryActivityRegistry registry = NewRegistry(CamusDBOptions.Default with { QueryActivityMaxSqlLength = 40 });
        QueryActivityEntry entry = registry.Begin(
            Ticket("CREATE USER bob IDENTIFIED BY 'hunter2-secret-password-value'"), cancellable: false, executing: true)!;

        string sql = registry.SnapshotQueries(ActivityViewer.All)[0].Sql;

        Assert.That(sql, Does.Not.Contain("hunter2"));
        Assert.That(sql.Length, Is.LessThanOrEqualTo(40));
        entry.End();
    }

    /// <summary>
    /// A transport that receives the cursor and fails before it reads it must not leave the entry
    /// listed for ever. The gRPC query path writes the schema frame first; a client that is gone by
    /// then makes that write throw, and the cursor is never enumerated.
    /// </summary>
    [Test]
    public void CursorNeverReadLeavesWhenTheRequestEnds()
    {
        QueryActivityRegistry registry = NewRegistry();
        using CancellationTokenSource request = new();
        QueryActivityEntry entry = registry.Begin(Ticket(token: request.Token), cancellable: true, executing: false)!;

        IAsyncEnumerable<QueryResultRow> neverRead = entry.Wrap(Rows(3), entry.CancellationToken);
        Assert.That(registry.Count, Is.EqualTo(1));

        request.Cancel();

        Assert.That(registry.Count, Is.Zero);
        GC.KeepAlive(neverRead);
    }

    /// <summary>A cursor that is being read is not ended by the request token; its own end does that.</summary>
    [Test]
    public async Task StartedCursorIsNotEndedByTheRequestToken()
    {
        QueryActivityRegistry registry = NewRegistry();
        using CancellationTokenSource request = new();
        QueryActivityEntry entry = registry.Begin(Ticket(token: request.Token), cancellable: true, executing: false)!;

        await using IAsyncEnumerator<QueryResultRow> cursor = entry.Wrap(Rows(3), entry.CancellationToken).GetAsyncEnumerator();
        Assert.That(await cursor.MoveNextAsync(), Is.True);

        request.Cancel();
        Assert.That(registry.Count, Is.EqualTo(1), "the reader still owns the entry");

        Assert.ThrowsAsync<OperationCanceledException>(async () => await cursor.MoveNextAsync());
        await cursor.DisposeAsync();
        Assert.That(registry.Count, Is.Zero);
    }

    [Test]
    public void ClientDisconnectStillCancelsTheLinkedToken()
    {
        QueryActivityRegistry registry = NewRegistry();
        using CancellationTokenSource request = new();
        QueryActivityEntry entry = registry.Begin(Ticket(token: request.Token), cancellable: true, executing: false)!;

        request.Cancel();

        Assert.That(entry.CancellationToken.IsCancellationRequested, Is.True);
        Assert.That(entry.CancelRequested, Is.False, "a disconnect is not a CANCEL QUERY");
        entry.End();
    }

    [Test]
    public void ConnectionRowsCountTheirRunningStatements()
    {
        QueryActivityRegistry registry = NewRegistry();
        ClientConnection connection = registry.Connections.Open("kestrel-1", "10.0.0.7");
        connection.RequestStarted("HTTP/2", internalPath: false);

        StatementOrigin? previous = StatementOrigin.Current;
        StatementOrigin.Current = connection.OriginFor("grpc");
        QueryActivityEntry first;
        QueryActivityEntry second;
        try
        {
            first = registry.Begin(Ticket(principal: new Principal("alice", false, [], "u-alice")), cancellable: true, executing: false)!;
            second = registry.Begin(Ticket(), cancellable: true, executing: false)!;
        }
        finally
        {
            StatementOrigin.Current = previous;
        }

        QueryActivityRow query = registry.SnapshotQueries(ActivityViewer.All)[0];
        Assert.That(query.ConnectionId, Is.EqualTo(connection.Id));
        Assert.That(query.ClientAddress, Is.EqualTo("10.0.0.7"));
        Assert.That(query.Transport, Is.EqualTo("grpc"));

        ConnectionActivityRow row = registry.SnapshotConnections(ActivityViewer.All).Single();
        Assert.That(row.ConnectionId, Is.EqualTo(connection.Id));
        Assert.That(row.ActiveQueries, Is.EqualTo(2));
        Assert.That(row.Protocol, Is.EqualTo("HTTP/2"));
        Assert.That(row.Kind, Is.EqualTo("client"));
        Assert.That(row.UserName, Is.EqualTo("alice"));
        Assert.That(row.Requests, Is.EqualTo(1));

        first.End();
        second.End();
        connection.RequestEnded();
        connection.Close();
        connection.Close();

        Assert.That(registry.SnapshotConnections(ActivityViewer.All), Is.Empty);
    }

    [Test]
    public void InternalRequestMarksThePeerKind()
    {
        QueryActivityRegistry registry = NewRegistry();
        ClientConnection connection = registry.Connections.Open("kestrel-2", "10.0.0.8");

        connection.RequestStarted("HTTP/1.1", internalPath: true);
        connection.RequestEnded();

        Assert.That(registry.SnapshotConnections(ActivityViewer.All).Single().Kind, Is.EqualTo("peer"));
        connection.Close();
    }

    /// <summary>
    /// A cancel and the end of the statement on two threads, many times over. Either order must
    /// leave the entry gone and must not throw: a cancel on a disposed token source throws
    /// <see cref="ObjectDisposedException"/>, which is the bug the entry's lock exists to prevent.
    /// </summary>
    [Test]
    public async Task CancelRacingTheEndIsSafe()
    {
        QueryActivityRegistry registry = NewRegistry();

        for (int i = 0; i < 2000; i++)
        {
            QueryActivityEntry entry = registry.Begin(Ticket(), cancellable: true, executing: false)!;
            using Barrier start = new(2);

            Task cancel = Task.Run(() => { start.SignalAndWait(); registry.TryCancel(entry.Id, ActivityViewer.All); });
            Task end = Task.Run(() => { start.SignalAndWait(); entry.End(); });

            await Task.WhenAll(cancel, end);
        }

        Assert.That(registry.Count, Is.Zero);
    }

    /// <summary>
    /// Two cancels and the end of the statement. Cancel A stops inside a token callback; cancel B
    /// then runs and returns, and the statement ends. The source must stay alive until A leaves: the
    /// end must not dispose it because B left, while A still runs on it.
    /// </summary>
    [Test]
    public async Task SecondCancelDoesNotLetTheEndDisposeTheSourceUnderTheFirst()
    {
        QueryActivityRegistry registry = NewRegistry();
        QueryActivityEntry entry = registry.Begin(Ticket(), cancellable: true, executing: false)!;

        using ManualResetEventSlim inCallback = new();
        using ManualResetEventSlim release = new();
        Exception? callbackError = null;

        entry.CancellationToken.Register(() =>
        {
            inCallback.Set();
            release.Wait(TimeSpan.FromSeconds(10));
            try
            {
                _ = entry.CancellationToken.WaitHandle;
            }
            catch (Exception exception)
            {
                callbackError = exception;
            }
        });

        Task<QueryCancelOutcome> first = Task.Run(() => registry.TryCancel(entry.Id, ActivityViewer.All));
        Assert.That(inCallback.Wait(TimeSpan.FromSeconds(10)), Is.True);

        Assert.That(registry.TryCancel(entry.Id, ActivityViewer.All), Is.EqualTo(QueryCancelOutcome.Cancelled));
        entry.End();
        release.Set();

        Assert.That(await first, Is.EqualTo(QueryCancelOutcome.Cancelled));
        Assert.That(callbackError, Is.Null, "the source was disposed while a cancel still ran on it");
        Assert.That(registry.Count, Is.Zero);
    }

    /// <summary>Many cancels and the end on parallel threads. None may throw, and the entry must leave.</summary>
    [Test]
    public async Task ConcurrentCancelsRacingTheEndAreSafe()
    {
        QueryActivityRegistry registry = NewRegistry();

        for (int i = 0; i < 1000; i++)
        {
            QueryActivityEntry entry = registry.Begin(Ticket(), cancellable: true, executing: false)!;
            using Barrier start = new(4);

            Task[] racers =
            [
                .. Enumerable.Range(0, 3).Select(_ => Task.Run(() => { start.SignalAndWait(); registry.TryCancel(entry.Id, ActivityViewer.All); })),
                Task.Run(() => { start.SignalAndWait(); entry.End(); }),
            ];

            await Task.WhenAll(racers);
        }

        Assert.That(registry.Count, Is.Zero);
    }

    /// <summary>
    /// One connection carries statements of two users in turn while another thread lists the
    /// connections as one of them. The user that decides visibility and the user the row shows must
    /// be the same pair, so the viewer never receives a row with the other user's name.
    /// </summary>
    [Test]
    public async Task ConnectionRowNeverShowsAnotherUsersIdentity()
    {
        QueryActivityRegistry registry = NewRegistry();
        ClientConnection connection = registry.Connections.Open("kestrel-mixed", "10.0.0.9");
        Principal alice = new("alice", isSuperuser: false, grants: [], userId: "u-alice");
        Principal bob = new("bob", isSuperuser: false, grants: [], userId: "u-bob");
        ActivityViewer aliceView = ActivityViewer.For(alice, authenticationEnabled: true);

        using CancellationTokenSource stop = new();

        Task writer = Task.Run(() =>
        {
            StatementOrigin.Current = connection.OriginFor("grpc");
            bool turn = false;
            while (!stop.IsCancellationRequested)
            {
                turn = !turn;
                registry.Begin(Ticket(principal: turn ? alice : bob), cancellable: false, executing: true)?.End();
            }
        });

        int foreign = 0;
        int seen = 0;
        try
        {
            for (int i = 0; i < 100_000; i++)
            {
                foreach (ConnectionActivityRow row in registry.SnapshotConnections(aliceView))
                {
                    seen++;
                    if (row.UserName != "alice")
                        foreign++;
                }
            }
        }
        finally
        {
            stop.Cancel();
            await writer;
            connection.Close();
        }

        Assert.That(foreign, Is.Zero, $"{foreign} of {seen} rows showed another user");
        Assert.That(seen, Is.GreaterThan(0), "the reader saw alice's rows at all");
    }

    /// <summary>
    /// With the statement list off, a connection still records its user, so the user still sees
    /// their own connection. Covered from the start and after a change at runtime.
    /// </summary>
    [Test]
    public void ConnectionRecordsItsUserWhileTheStatementListIsOff()
    {
        Principal alice = new("alice", isSuperuser: false, grants: [], userId: "u-alice");
        Principal bob = new("bob", isSuperuser: false, grants: [], userId: "u-bob");

        QueryActivityRegistry off = NewRegistry(CamusDBOptions.Default with { QueryActivityEnabled = false });
        ClientConnection offConnection = off.Connections.Open("kestrel-off", "10.0.0.10");

        QueryActivityRegistry toggled = NewRegistry();
        ClientConnection toggledConnection = toggled.Connections.Open("kestrel-toggled", "10.0.0.11");

        StatementOrigin? previous = StatementOrigin.Current;
        try
        {
            StatementOrigin.Current = offConnection.OriginFor("http");
            Assert.That(off.Begin(Ticket(principal: alice), cancellable: false, executing: true), Is.Null);

            StatementOrigin.Current = toggledConnection.OriginFor("http");
            toggled.Begin(Ticket(principal: alice), cancellable: false, executing: true)!.End();
            toggled.ApplyOptions(CamusDBOptions.Default with { QueryActivityEnabled = false });
            Assert.That(toggled.Begin(Ticket(principal: bob), cancellable: false, executing: true), Is.Null);
        }
        finally
        {
            StatementOrigin.Current = previous;
        }

        Assert.That(off.SnapshotConnections(ActivityViewer.For(alice, authenticationEnabled: true)).Single().UserName, Is.EqualTo("alice"));

        Assert.That(toggled.SnapshotConnections(ActivityViewer.For(alice, authenticationEnabled: true)), Is.Empty, "the connection moved to bob");
        Assert.That(toggled.SnapshotConnections(ActivityViewer.For(bob, authenticationEnabled: true)).Single().UserName, Is.EqualTo("bob"));

        offConnection.Close();
        toggledConnection.Close();
    }

    [Test]
    public async Task ConcurrentStatementsAllRegisterAndLeave()
    {
        QueryActivityRegistry registry = NewRegistry();

        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
        {
            for (int i = 0; i < 500; i++)
            {
                QueryActivityEntry entry = registry.Begin(Ticket(), cancellable: true, executing: false)!;
                await foreach (QueryResultRow __ in entry.Wrap(Rows(1))) { }
            }
        })));

        Assert.That(registry.Count, Is.Zero);
    }
}
