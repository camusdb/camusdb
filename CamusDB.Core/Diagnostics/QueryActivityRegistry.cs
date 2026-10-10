/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using CamusDB.Core.Auth;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using Kommander.Time;
using YamlDotNet.Serialization.NamingConventions;

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// The statements that run on this node now, read by <c>SHOW QUERIES</c> and cancelled by
/// <c>CANCEL QUERY</c>.
///
/// <para><b>Where statements enter.</b> Every SQL statement a client sends reaches the engine
/// through one of three methods — <c>ExecuteDDLSQL</c>, <c>ExecuteNonSQLQuery</c> and
/// <c>SelectStatementExecutor.ExecuteSQLQuery</c> — and each calls <see cref="Begin"/> at its top.
/// The typed row APIs and peer query fragments do not pass through them and are not listed.</para>
///
/// <para><b>When a statement leaves.</b> A no-rows statement leaves when its method returns. A
/// row-returning statement leaves when its cursor ends, not when its method returns: the method
/// returns a lazy cursor before any row is read, and the work happens while the transport drains
/// it. <see cref="QueryActivityEntry.Wrap"/> is the one correct place to end such an entry.</para>
///
/// <para><b>Cost on the statement path.</b> One entry, one linked token source and one dictionary
/// insert and remove per statement. The SQL text is held by reference; redaction and truncation
/// run only when someone reads the list. The statement kind is the node type its own parse found,
/// so reading the list never parses SQL again. With
/// <see cref="CamusDBOptions.QueryActivityEnabled"/> off, <see cref="Begin"/> returns null and the
/// statement runs as before, except that its connection still records its user.</para>
/// </summary>
public sealed class QueryActivityRegistry
{
    /// <summary>The phase of a statement that is parsed, authorized and planned but has produced no row yet.</summary>
    public const string PlanningPhase = "planning";

    /// <summary>The phase of a statement that executes: a mutation, or a query whose cursor is read.</summary>
    public const string ExecutingPhase = "executing";

    private readonly ConcurrentDictionary<long, QueryActivityEntry> running = new();

    private readonly ActivityIdMinter ids;

    private CamusDBOptions options;

    /// <param name="options">The configuration snapshot in force when the engine is built.</param>
    /// <param name="nodeLabel">
    /// Returns this node's Raft endpoint, or an empty string while the node has none. It is a
    /// function because the endpoint is not known yet when the engine is built.
    /// </param>
    public QueryActivityRegistry(CamusDBOptions options, Func<string> nodeLabel)
    {
        this.options = options;
        ids = new ActivityIdMinter(nodeLabel);
        Connections = new ClientConnectionRegistry(ids);
    }

    /// <summary>The client connections the host reports for this node.</summary>
    public ClientConnectionRegistry Connections { get; }

    /// <summary>This node's Raft endpoint, or an empty string when the node has none yet.</summary>
    public string NodeLabel => ids.NodeLabel;

    /// <summary>The prefix every query id and connection id of this process starts with.</summary>
    public string IdPrefix => ids.Prefix;

    /// <summary>
    /// The number of statements registered now. It takes every lock of the list, so it is for tests
    /// and diagnostics, never for the statement path.
    /// </summary>
    public int Count => running.Count;

    /// <summary>
    /// Adopts a newly published configuration snapshot. The enabled flag affects the next statement;
    /// a statement already registered stays registered until it ends.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next) => options = next;

    /// <summary>
    /// Registers a statement, or returns null when the registry is disabled now.
    ///
    /// <para>The caller must put <see cref="QueryActivityEntry.CancellationToken"/> on the ticket
    /// it runs, and must end the entry on every path: <see cref="QueryActivityEntry.End"/> for a
    /// statement that returns no cursor or that fails before it has one, and
    /// <see cref="QueryActivityEntry.Wrap"/> for a cursor.</para>
    /// </summary>
    /// <param name="ticket">The statement as the transport sent it.</param>
    /// <param name="cancellable">
    /// True when the statement observes the ticket's token for its whole run. Only a read does: a
    /// write ignores the token once its first mutation lands, so a cancel cannot honestly stop it.
    /// </param>
    /// <param name="executing">
    /// True to start the entry in <see cref="ExecutingPhase"/>, for a statement that has no planning
    /// phase separate from its execution.
    /// </param>
    public QueryActivityEntry? Begin(in ExecuteSQLTicket ticket, bool cancellable, bool executing)
        => BeginCore(ticket, ownsToken: cancellable, cancellableNow: cancellable, executing);

    /// <summary>
    /// Registers a statement on the row-returning path, or returns null when the registry is
    /// disabled now.
    ///
    /// <para>The text alone does not tell a read from a write: <c>INSERT … RETURNING</c> also
    /// returns rows. So the entry gets its own token at once, because the ticket must carry it from
    /// the start, but a cancel is refused until the caller parses the statement and calls
    /// <see cref="QueryActivityEntry.AllowCancel"/> for a read. A write never gets that call, so no
    /// cancel can be accepted for it, not even in the short time before its parse ends.</para>
    /// </summary>
    public QueryActivityEntry? BeginRowReturning(in ExecuteSQLTicket ticket)
        => BeginCore(ticket, ownsToken: true, cancellableNow: false, executing: false);

    private QueryActivityEntry? BeginCore(in ExecuteSQLTicket ticket, bool ownsToken, bool cancellableNow, bool executing)
    {
        StatementOrigin? origin = StatementOrigin.Current;

        // The connection records its user whether or not statements are listed: SHOW CONNECTIONS
        // stays available with the list off, and its visibility rule depends on that user.
        origin?.Connection?.NoteStatement(ticket.Principal?.UserName, ticket.Principal?.UserId);

        if (!options.QueryActivityEnabled)
            return null;

        (long sequence, string id) = ids.NextQueryId();

        QueryActivityEntry entry = new(this, sequence, id, ticket, origin, ownsToken, cancellableNow, executing);
        running[sequence] = entry;

        // Armed only after the entry is in the list. The callback runs at once on a token that has
        // already fired, and it must find the entry to remove it; armed earlier, it would remove
        // nothing, and the insert above would then publish an entry that has already ended.
        entry.ArmRequestAbort(ticket.CancellationToken);
        return entry;
    }

    internal void Remove(QueryActivityEntry entry)
        => running.TryRemove(new KeyValuePair<long, QueryActivityEntry>(entry.Sequence, entry));

    /// <summary>
    /// The statements registered now that <paramref name="viewer"/> may see, oldest first.
    ///
    /// <para>The SQL text is redacted before it is truncated, as in the slow query log: the other
    /// order can cut a masked literal short and leave the start of a password behind.</para>
    /// </summary>
    public List<QueryActivityRow> SnapshotQueries(ActivityViewer viewer)
    {
        CamusDBOptions current = options;
        int maxSql = Math.Max(1, current.QueryActivityMaxSqlLength);
        string node = ids.NodeLabel;

        List<QueryActivityRow> rows = [];

        // Enumerating the dictionary itself takes no lock. Its Values and Count properties take every
        // lock of the dictionary, which would stall each statement that registers meanwhile.
        foreach (KeyValuePair<long, QueryActivityEntry> pair in running)
        {
            QueryActivityEntry entry = pair.Value;

            if (!viewer.CanSee(entry.UserName, entry.UserId))
                continue;

            rows.Add(entry.ToRow(node, maxSql));
        }

        rows.Sort(static (a, b) => a.StartedAt.CompareTo(b.StartedAt));
        return rows;
    }

    /// <summary>
    /// The connections open now that <paramref name="viewer"/> may see, oldest first. A connection
    /// with no authenticated statement yet has no user, and only a viewer who sees everything sees it.
    /// </summary>
    public List<ConnectionActivityRow> SnapshotConnections(ActivityViewer viewer)
    {
        string node = ids.NodeLabel;

        // One pass over the running statements counts them per connection, instead of one pass per
        // connection.
        Dictionary<ClientConnection, int>? activeByConnection = null;
        foreach (KeyValuePair<long, QueryActivityEntry> pair in running)
        {
            if (pair.Value.Connection is not { } connection)
                continue;

            activeByConnection ??= new Dictionary<ClientConnection, int>(ReferenceEqualityComparer.Instance);
            activeByConnection[connection] = activeByConnection.GetValueOrDefault(connection) + 1;
        }

        List<ConnectionActivityRow> rows = [];

        foreach (ClientConnection connection in Connections.List())
        {
            // One read of the user: the same pair decides visibility and fills the row. Two reads
            // could pass the check for one user and then return another user's name.
            ConnectionUser? user = connection.User;
            if (!viewer.CanSee(user?.Name, user?.Id))
                continue;

            rows.Add(new ConnectionActivityRow(
                connection.Id,
                node,
                connection.ClientAddress,
                connection.Protocol,
                connection.IsPeer ? "peer" : "client",
                user?.Name,
                connection.OpenedAt,
                connection.AgeMs,
                // A connection that runs a statement now is not idle, whatever its timestamps say.
                (activeByConnection?.GetValueOrDefault(connection) ?? 0) > 0 ? 0 : connection.IdleMs,
                connection.Requests,
                activeByConnection?.GetValueOrDefault(connection) ?? 0,
                connection.OpenStreams,
                Error: null));
        }

        rows.Sort(static (a, b) => a.OpenedAt.CompareTo(b.OpenedAt));
        return rows;
    }

    /// <summary>
    /// Cancels the statement with id <paramref name="queryId"/> on this node, when
    /// <paramref name="requester"/> may cancel it.
    ///
    /// <para>A statement the requester may not see is reported as not found rather than as not
    /// permitted, so a user cannot learn which ids other users' statements hold.</para>
    /// </summary>
    public QueryCancelOutcome TryCancel(string queryId, ActivityViewer requester)
    {
        if (!ActivityIdMinter.TryParseSequence(queryId, out long sequence) ||
            !running.TryGetValue(sequence, out QueryActivityEntry? entry) ||
            !string.Equals(entry.Id, queryId, StringComparison.Ordinal) ||
            !requester.CanSee(entry.UserName, entry.UserId))
            return QueryCancelOutcome.NotFound;

        return entry.RequestCancel();
    }

    /// <summary>True when <paramref name="queryId"/> was minted by this process.</summary>
    public bool IsLocalId(string queryId) => queryId.StartsWith(ids.Prefix, StringComparison.Ordinal) &&
                                             queryId.Length > ids.Prefix.Length &&
                                             queryId[ids.Prefix.Length] == '-';
}

/// <summary>
/// One running statement. Created by <see cref="QueryActivityRegistry.Begin"/>; ended exactly once
/// by <see cref="End"/>, whichever way the statement finishes.
///
/// <para><b>The cancel and the end race, and both must be safe.</b> A <c>CANCEL QUERY</c> on
/// another thread can find the entry just as the statement ends, and two cancels can run at once.
/// The token source must not be disposed while <see cref="CancellationTokenSource.Cancel()"/> runs
/// on it, and must not be cancelled after it is disposed. A small lock orders them: it counts the
/// cancels that run now, the last cancel to leave disposes the source when the statement ended
/// meanwhile, and a cancel that starts after the end does nothing. The lock is never held while
/// <c>Cancel</c> runs, because <c>Cancel</c> runs callbacks that can end this entry.</para>
/// </summary>
public sealed class QueryActivityEntry
{
    private static readonly ConcurrentDictionary<NodeType, string> KindNames = new();

    private readonly QueryActivityRegistry owner;

    private readonly CancellationTokenSource? cancellation;

    private readonly CancellationToken token;

    private readonly long startedAtTicks;

    private readonly object gate = new();

    private long rowsReturned;

    private int phase;

    /// <summary>The parsed <see cref="NodeType"/> as an int, or -1 before the statement's parse ends.</summary>
    private int kind = -1;

    private bool ended;

    /// <summary>Cancels inside <see cref="CancellationTokenSource.Cancel()"/> now. Guarded by <see cref="gate"/>.</summary>
    private int cancellers;

    /// <summary>
    /// True once one path has taken the decision about the source: it disposed it, or it left it to
    /// the garbage collector. Guarded by <see cref="gate"/>.
    /// </summary>
    private bool sourceReleased;

    private int cancelRequested;

    private bool cancellable;

    private int cursorStarted;

    private CancellationTokenRegistration requestAborted;

    internal QueryActivityEntry(
        QueryActivityRegistry owner,
        long sequence,
        string id,
        in ExecuteSQLTicket ticket,
        StatementOrigin? origin,
        bool ownsToken,
        bool cancellable,
        bool executing)
    {
        this.owner = owner;
        this.cancellable = cancellable;
        Sequence = sequence;
        Id = id;
        Sql = ticket.Sql;
        Database = ticket.DatabaseName;
        UserName = ticket.Principal?.UserName;
        UserId = ticket.Principal?.UserId;
        Transport = origin?.Transport ?? StatementOrigin.EmbeddedTransport;
        Connection = origin?.Connection;
        ClientAddress = origin?.ClientAddress;

        // A server-level statement runs with no transaction, and the struct then holds null.
        if (ticket.TxnState is { } txn)
        {
            TransactionClientId = txn.ClientId;
            Isolation = txn.IsolationLevel.ToString();
        }

        phase = executing ? 1 : 0;
        StartedAt = DateTime.UtcNow;
        startedAtTicks = Stopwatch.GetTimestamp();

        // Linked, so a client that disconnects still stops the read exactly as it did before. A
        // statement a cancel cannot stop gets no source of its own: it keeps the transport's token,
        // and the statement path allocates nothing it would never use.
        if (ownsToken)
        {
            cancellation = CancellationTokenSource.CreateLinkedTokenSource(ticket.CancellationToken);
            token = cancellation.Token;
        }
        else
        {
            token = ticket.CancellationToken;
        }
    }

    internal long Sequence { get; }

    /// <summary>The id <c>SHOW QUERIES</c> reports and <c>CANCEL QUERY</c> takes. Unique across the cluster.</summary>
    public string Id { get; }

    /// <summary>The statement text, unredacted. Never return it to a reader as is.</summary>
    internal string Sql { get; }

    internal string Database { get; }

    internal string? UserName { get; }

    internal string? UserId { get; }

    internal string Transport { get; }

    internal ClientConnection? Connection { get; }

    internal string? ClientAddress { get; }

    internal HLCTimestamp TransactionClientId { get; }

    internal string? Isolation { get; }

    internal DateTime StartedAt { get; }

    /// <summary>
    /// The token the statement must run under. It fires when the client's own request token fires,
    /// and when <c>CANCEL QUERY</c> names this statement.
    ///
    /// <para>It follows the rule of <see cref="ExecuteSQLTicket.CancellationToken"/>: it bounds
    /// reads only and must never reach a commit, a rollback or a lock release.</para>
    /// </summary>
    public CancellationToken CancellationToken => token;

    /// <summary>True when <c>CANCEL QUERY</c> can stop this statement now.</summary>
    public bool Cancellable
    {
        get
        {
            lock (gate)
                return cancellable;
        }
    }

    /// <summary>
    /// Lets <c>CANCEL QUERY</c> stop the statement. Called on the row-returning path when the parse
    /// shows that the statement is a read. It only ever turns cancellation on, never off, so a cancel
    /// accepted before it cannot apply to a write.
    /// </summary>
    public void AllowCancel()
    {
        if (cancellation is null)
            return;

        lock (gate)
            cancellable = true;
    }

    /// <summary>Moves the statement from planning to execution.</summary>
    public void MarkExecuting() => Volatile.Write(ref phase, 1);

    /// <summary>
    /// Records the statement kind that its parse found. The first call wins: a statement that hands
    /// its ticket to another dispatcher keeps the kind of its own text.
    /// </summary>
    public void Describe(NodeType nodeType) => Interlocked.CompareExchange(ref kind, (int)nodeType, -1);

    /// <summary>
    /// Ends an entry whose cursor never started when the request that owns it ends. Called once,
    /// after the entry is in the list: on a token that already fired, the callback runs inside this
    /// call and must find the entry there to remove it.
    /// </summary>
    internal void ArmRequestAbort(CancellationToken requestToken)
    {
        if (cancellation is null || !requestToken.CanBeCanceled)
            return;

        // A transport can receive the cursor and then fail before it reads it — the client went
        // away while the schema frame was being written — and a cursor that is never read never
        // runs the wrapper's finally. Without this the entry would stay listed for the life of the
        // process. When the request ends that way, an entry whose cursor never started ends here.
        CancellationTokenRegistration registration = requestToken.UnsafeRegister(
            static state => ((QueryActivityEntry)state!).OnRequestAborted(), this);

        bool unregisterNow;
        lock (gate)
        {
            // Ended already: the callback ran inside UnsafeRegister. Nothing reads the registration
            // after the end, so it is released here.
            unregisterNow = ended;
            if (!unregisterNow)
                requestAborted = registration;
        }

        if (unregisterNow)
            registration.Unregister();
    }

    /// <summary>
    /// Cancels the statement's token when the statement still runs and accepts a cancel.
    /// </summary>
    internal QueryCancelOutcome RequestCancel()
    {
        if (cancellation is null)
            return QueryCancelOutcome.NotCancellable;

        // The checks and the count are under the lock that the end takes, so the end cannot dispose
        // the source between the check and the cancel.
        lock (gate)
        {
            if (ended)
                return QueryCancelOutcome.NotFound;

            if (!cancellable)
                return QueryCancelOutcome.NotCancellable;

            cancellers++;
        }

        Volatile.Write(ref cancelRequested, 1);

        try
        {
            // Outside the lock: Cancel runs the token's callbacks synchronously, and one of them can
            // end this entry on this same thread.
            cancellation.Cancel();
        }
        finally
        {
            bool disposeNow;
            lock (gate)
            {
                cancellers--;
                disposeNow = ended && cancellers == 0 && !sourceReleased;
                if (disposeNow)
                    sourceReleased = true;
            }

            if (disposeNow)
                cancellation.Dispose();
        }

        return QueryCancelOutcome.Cancelled;
    }

    /// <summary>
    /// Ends an entry whose cursor never started, when the request that owns it ends.
    ///
    /// <para>It does not dispose the token source, and no later cancel does either. This callback
    /// runs among the request token's own callbacks, before the one that cancels the linked source,
    /// and a transport that reads the cursor after all would then hold a token of a disposed source.
    /// The request token has already fired, so the source holds no live registration, and the
    /// garbage collector reclaims it.</para>
    /// </summary>
    private void OnRequestAborted()
    {
        if (Volatile.Read(ref cursorStarted) == 0)
            EndCore(disposeSource: false);
    }

    /// <summary>
    /// Ends the statement: removes it from the list and releases its token source. Safe to call more
    /// than once; only the first call has an effect.
    /// </summary>
    public void End() => EndCore(disposeSource: true);

    private void EndCore(bool disposeSource)
    {
        bool disposeNow;
        CancellationTokenRegistration registration;
        lock (gate)
        {
            if (ended)
                return;

            ended = true;
            registration = requestAborted;

            // A cancel that runs now disposes the source when it leaves; a later cancel sees `ended`
            // and returns before it touches the source.
            disposeNow = disposeSource && cancellers == 0;
            if (disposeNow || !disposeSource)
                sourceReleased = true;
        }

        owner.Remove(this);

        // Unregister, not Dispose: Dispose waits for a running callback, and End can run inside it.
        registration.Unregister();

        if (disposeNow)
            cancellation?.Dispose();
    }

    /// <summary>
    /// Wraps a result cursor so the entry ends when the cursor ends, whichever way it ends, and
    /// counts the rows the cursor returns.
    ///
    /// <para>The returned cursor ends the entry when its enumerator is disposed, also before the
    /// first row: a caller that takes a cursor and disposes it unread is the normal way to drop a
    /// result. A transport that never enumerates the cursor at all leaves the entry registered until
    /// its request ends; every transport in this repository enumerates or disposes what it
    /// receives.</para>
    ///
    /// <para><b>A cancel is checked before every row</b>, not only where the operators check the
    /// token: a cursor whose rows are already in memory — a SHOW statement, a buffered sort — observes
    /// no token of its own, and would otherwise run to its end after a cancel reported success.</para>
    /// </summary>
    public IAsyncEnumerable<QueryResultRow> Wrap(IAsyncEnumerable<QueryResultRow> cursor, CancellationToken cancellationToken = default)
        => new EndingCursor(this, Step(cursor, cancellationToken));

    private async IAsyncEnumerable<QueryResultRow> Step(
        IAsyncEnumerable<QueryResultRow> cursor,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        MarkExecuting();
        Volatile.Write(ref cursorStarted, 1);

        IAsyncEnumerator<QueryResultRow>? enumerator = null;

        try
        {
            enumerator = cursor.GetAsyncEnumerator(cancellationToken);
            long rows = 0;

            while (true)
            {
                bool more;

                // Stepping by hand rather than with `await foreach` is what lets the cancellation be
                // translated here: a `yield return` cannot sit inside a try block that has a catch.
                try
                {
                    token.ThrowIfCancellationRequested();
                    more = await enumerator.MoveNextAsync().ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (CancelRequested)
                {
                    throw Cancelled();
                }

                if (!more)
                    break;

                // One reader per cursor, so a plain store published with release semantics is enough
                // for the thread that reads the list.
                Volatile.Write(ref rowsReturned, ++rows);
                yield return enumerator.Current;
            }
        }
        finally
        {
            try
            {
                if (enumerator is not null)
                    await enumerator.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                End();
            }
        }
    }

    /// <summary>True when <c>CANCEL QUERY</c> named this statement.</summary>
    public bool CancelRequested => Volatile.Read(ref cancelRequested) != 0;

    /// <summary>
    /// The error a cancelled statement reports. A client that is still connected must learn why its
    /// statement stopped; the bare cancellation it would otherwise see is what a disconnect looks
    /// like, and the transports treat it as one.
    /// </summary>
    public CamusDBException Cancelled()
        => new(CamusDBErrorCodes.QueryCancelled, $"Query '{Id}' was cancelled by CANCEL QUERY");

    /// <summary>Records the row count of a statement that returns no cursor, before it ends.</summary>
    public void SetRowsAffected(long rows) => Volatile.Write(ref rowsReturned, rows);

    internal QueryActivityRow ToRow(string node, int maxSqlLength)
    {
        string sql = SqlCredentialRedactor.Redact(Sql);
        if (sql.Length > maxSqlLength)
            sql = sql[..maxSqlLength];

        int parsed = Volatile.Read(ref kind);

        return new QueryActivityRow(
            Id,
            node,
            Connection?.Id,
            ClientAddress,
            Transport,
            UserName,
            Database,
            TransactionClientId == HLCTimestamp.Zero
                ? null
                : string.Create(CultureInfo.InvariantCulture, $"{TransactionClientId.L}.{TransactionClientId.C}"),
            Isolation,
            parsed < 0 ? "unknown" : KindName((NodeType)parsed),
            Volatile.Read(ref phase) == 0 ? QueryActivityRegistry.PlanningPhase : QueryActivityRegistry.ExecutingPhase,
            StartedAt,
            Stopwatch.GetElapsedTime(startedAtTicks).TotalMilliseconds,
            Volatile.Read(ref rowsReturned),
            Cancellable,
            Volatile.Read(ref cancelRequested) != 0,
            sql,
            Error: null);
    }

    /// <summary>The kind name of a node type, for example <c>create_table</c>, as the slow query log spells it.</summary>
    private static string KindName(NodeType nodeType)
        => KindNames.GetOrAdd(nodeType, static type => UnderscoredNamingConvention.Instance.Apply(type.ToString()));

    /// <summary>
    /// The cursor <see cref="Wrap"/> returns. Its enumerator ends the entry when it is disposed, so
    /// a cursor disposed before its first row still leaves the list: the compiler-built iterator
    /// inside it runs its <c>finally</c> only once its body has started.
    /// </summary>
    private sealed class EndingCursor(QueryActivityEntry entry, IAsyncEnumerable<QueryResultRow> rows) : IAsyncEnumerable<QueryResultRow>
    {
        public IAsyncEnumerator<QueryResultRow> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            IAsyncEnumerator<QueryResultRow> inner;
            try
            {
                inner = rows.GetAsyncEnumerator(cancellationToken);
            }
            catch
            {
                entry.End();
                throw;
            }

            return new EndingEnumerator(entry, inner);
        }
    }

    private sealed class EndingEnumerator(QueryActivityEntry entry, IAsyncEnumerator<QueryResultRow> inner) : IAsyncEnumerator<QueryResultRow>
    {
        public QueryResultRow Current => inner.Current;

        public ValueTask<bool> MoveNextAsync() => inner.MoveNextAsync();

        public async ValueTask DisposeAsync()
        {
            try
            {
                await inner.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                entry.End();
            }
        }
    }
}

/// <summary>
/// One row of <c>SHOW QUERIES</c>. Also the wire shape a peer returns for
/// <c>SHOW CLUSTER QUERIES</c>, so it holds only plain values.
/// </summary>
/// <param name="TransactionId">
/// The transaction handle as <c>{L}.{C}</c>, the same pair a client passes back to resume the
/// transaction; null for a statement with no transaction.
/// </param>
/// <param name="Error">
/// Null for a statement row. Set only on the one row that stands in for a peer that could not be
/// reached, whose other columns except <see cref="Node"/> are then empty.
/// </param>
public sealed record QueryActivityRow(
    string QueryId,
    string Node,
    string? ConnectionId,
    string? ClientAddress,
    string Transport,
    string? UserName,
    string Database,
    string? TransactionId,
    string? Isolation,
    string Kind,
    string Phase,
    DateTime StartedAt,
    double ElapsedMs,
    long RowsReturned,
    bool Cancellable,
    bool CancelRequested,
    string Sql,
    string? Error);

/// <summary>
/// One row of <c>SHOW CONNECTIONS</c>. Also the wire shape a peer returns for
/// <c>SHOW CLUSTER CONNECTIONS</c>.
/// </summary>
/// <param name="Kind"><c>client</c>, or <c>peer</c> for a connection a peer node opened to call an <c>/internal/</c> endpoint.</param>
/// <param name="Error">Null for a connection row; set only on the row that stands in for an unreachable peer.</param>
public sealed record ConnectionActivityRow(
    string ConnectionId,
    string Node,
    string? ClientAddress,
    string? Protocol,
    string Kind,
    string? UserName,
    DateTime OpenedAt,
    double AgeMs,
    double IdleMs,
    long Requests,
    int ActiveQueries,
    int OpenStreams,
    string? Error);

/// <summary>What <c>CANCEL QUERY</c> did on the node that owns the statement.</summary>
public enum QueryCancelOutcome
{
    /// <summary>The statement's token was cancelled. The statement stops at its next read.</summary>
    Cancelled,

    /// <summary>No running statement has this id, or the requester may not see it.</summary>
    NotFound,

    /// <summary>
    /// The statement is a write, which ignores its token once its first mutation lands, or its parse
    /// has not ended yet, so it is not known to be a read.
    /// </summary>
    NotCancellable,
}

/// <summary>
/// Who reads the activity lists or asks for a cancel, reduced to what the visibility rule needs.
///
/// <para>With authentication off there is no user to compare, and every caller sees everything —
/// the same as every other privilege gate in the engine. A superuser sees everything. Any other
/// user sees only the rows whose user is their own: by user id when both sides have one, else by
/// name.</para>
///
/// <para>It is a plain value so a peer can receive it over the node-secret channel and apply the
/// same rule to its own rows.</para>
/// </summary>
public readonly record struct ActivityViewer(bool SeesAll, string? UserName, string? UserId)
{
    /// <summary>A viewer that sees every row.</summary>
    public static ActivityViewer All => new(true, null, null);

    /// <summary>The viewer for <paramref name="principal"/> under the given authentication setting.</summary>
    public static ActivityViewer For(Principal? principal, bool authenticationEnabled)
    {
        if (!authenticationEnabled || principal is null)
            return authenticationEnabled ? new ActivityViewer(false, null, null) : All;

        return principal.IsSuperuser ? All : new ActivityViewer(false, principal.UserName, principal.UserId);
    }

    /// <summary>True when this viewer may see a row owned by the given user.</summary>
    public bool CanSee(string? ownerName, string? ownerId)
    {
        if (SeesAll)
            return true;

        if (UserId is not null && ownerId is not null)
            return string.Equals(UserId, ownerId, StringComparison.Ordinal);

        return UserName is not null && string.Equals(UserName, ownerName, StringComparison.Ordinal);
    }
}

/// <summary>
/// Mints the query and connection ids of this process.
///
/// <para>An id is <c>{nodeTag}{instanceTag}-{sequence}</c> for a query and
/// <c>{nodeTag}{instanceTag}-c{sequence}</c> for a connection. The node tag is six hex digits of a
/// hash of the Raft endpoint, so a node that receives <c>CANCEL QUERY</c> for an id it did not mint
/// can find the member that did. The instance tag is four random hex digits per process start:
/// a restarted node starts its sequence at one again, and without the tag an old id an operator
/// copied before the restart could cancel an unrelated new statement.</para>
/// </summary>
internal sealed class ActivityIdMinter
{
    /// <summary>Length of the node tag plus the instance tag.</summary>
    internal const int PrefixLength = 10;

    private readonly Func<string> nodeLabel;

    private readonly string instanceTag = Random.Shared.Next(0, 0x10000).ToString("x4", CultureInfo.InvariantCulture);

    private string? cachedLabel;

    private string? cachedPrefix;

    private long querySequence;

    private long connectionSequence;

    internal ActivityIdMinter(Func<string> nodeLabel)
    {
        this.nodeLabel = nodeLabel;
    }

    /// <summary>This node's Raft endpoint, cached once it is known.</summary>
    internal string NodeLabel
    {
        get
        {
            string? label = Volatile.Read(ref cachedLabel);
            if (label is not null)
                return label;

            string resolved;
            try
            {
                resolved = nodeLabel();
            }
            catch (Exception)
            {
                resolved = "";
            }

            // An empty label means the node has not started yet. It is not cached, so the ids switch
            // to the real tag as soon as the endpoint is known.
            if (resolved.Length > 0)
                Volatile.Write(ref cachedLabel, resolved);

            return resolved;
        }
    }

    internal string Prefix
    {
        get
        {
            string? prefix = Volatile.Read(ref cachedPrefix);
            if (prefix is not null)
                return prefix;

            string label = NodeLabel;
            prefix = NodeTagOf(label) + instanceTag;

            if (label.Length > 0)
                Volatile.Write(ref cachedPrefix, prefix);

            return prefix;
        }
    }

    internal (long Sequence, string Id) NextQueryId()
    {
        long sequence = Interlocked.Increment(ref querySequence);
        return (sequence, string.Create(CultureInfo.InvariantCulture, $"{Prefix}-{sequence}"));
    }

    internal string NextConnectionId()
        => string.Create(CultureInfo.InvariantCulture, $"{Prefix}-c{Interlocked.Increment(ref connectionSequence)}");

    /// <summary>
    /// The six-hex-digit tag of a Raft endpoint: FNV-1a over its characters, low 24 bits. A collision
    /// between two members only costs a cancel one extra peer call.
    /// </summary>
    internal static string NodeTagOf(string endpoint)
    {
        uint hash = 2166136261;
        foreach (char c in endpoint)
        {
            hash ^= c;
            hash *= 16777619;
        }

        return (hash & 0xFFFFFF).ToString("x6", CultureInfo.InvariantCulture);
    }

    /// <summary>The node tag of a query id, or false when the text is not shaped like one.</summary>
    internal static bool TryGetNodeTag(string queryId, out string nodeTag)
    {
        if (queryId.Length > PrefixLength + 1 && queryId[PrefixLength] == '-')
        {
            nodeTag = queryId[..6];
            return true;
        }

        nodeTag = "";
        return false;
    }

    /// <summary>The sequence number of a query id, or false when the text is not shaped like one.</summary>
    internal static bool TryParseSequence(string queryId, out long sequence)
    {
        sequence = 0;

        if (queryId.Length <= PrefixLength + 1 || queryId[PrefixLength] != '-')
            return false;

        return long.TryParse(queryId.AsSpan(PrefixLength + 1), NumberStyles.None, CultureInfo.InvariantCulture, out sequence);
    }
}
