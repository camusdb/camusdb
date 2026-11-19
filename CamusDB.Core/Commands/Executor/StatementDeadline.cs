/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor;

/// <summary>
/// The time limit of one SQL statement, <see cref="CamusDBOptions.StatementTimeoutMs"/>. A statement
/// that is still running at its deadline fails with <see cref="CamusDBErrorCodes.StatementTimeout"/>.
///
/// <para><b>Why a time limit per statement.</b> The budgets of an inline function apply to one call,
/// and a statement can call a function once per row. Without a limit per statement,
/// <c>SELECT f(x) FROM big_table</c> runs for hours with every call inside its budget. The limit also
/// covers plain SQL: a cross join that returns few rows, a scan of a large table.</para>
///
/// <para><b>Two mechanisms, because reads and writes stop differently.</b></para>
/// <list type="bullet">
///   <item><b>A read</b> gets a token that fires at the deadline (<see cref="LinkReadToken"/>,
///   <see cref="ArmReadToken"/>). Every operator that stops for <c>CANCEL QUERY</c> stops for the
///   deadline the same way, lock waits and peer fragments included. The cancellation is turned into
///   <see cref="CamusDBErrorCodes.StatementTimeout"/> where the statement's cursor ends
///   (<see cref="Wrap"/>).</item>
///   <item><b>A write</b> never gets that token: the rule of
///   <see cref="Models.Tickets.ExecuteSQLTicket.CancellationToken"/> is that a write ignores a cancel
///   once its first mutation lands. A write checks the deadline itself instead
///   (<see cref="ThrowIfExpired"/>) at the start of each row its scans read and each row it writes,
///   and fails with an ordinary statement error. A failed write takes the path of any failed write:
///   the transaction is marked rollback-only, and an autocommit statement is rolled back.</item>
/// </list>
///
/// <para><b>Inline code checks the deadline too.</b> A call of an inline function gets the smaller
/// of its own time budget and the time the statement has left, and so does the fire of an inline
/// trigger. A call that runs when the time is up therefore stops at the deadline, not at the end of
/// its own budget.</para>
///
/// <para><b>It travels as an ambient value.</b> The scalar evaluator and the scan operators take no
/// statement context, so the deadline of the statement this flow runs is an
/// <see cref="AsyncLocal{T}"/> (<see cref="Current"/>). The entry points set it in their own async
/// frame, so it never reaches their caller; the cursor of a read sets it again in the flow that reads
/// the cursor (<see cref="Wrap"/>). Work a statement started and that outlives it (a background task
/// that inherited the flow) finds the deadline ended, and <see cref="Current"/> reports none.</para>
///
/// <para><b>A statement inside a statement inherits the deadline.</b> The SQL that an inline
/// trigger or a procedure runs is part of the statement that fired it, so it does not get a new
/// clock: it runs until the outer statement's deadline, and fails at its start when that has
/// passed.</para>
///
/// <para><b>Schema DDL is not bounded</b> (<see cref="Suspend"/>), with the exception of
/// <c>CREATE TABLE … AS</c>: an index or column backfill and a replicated schema change run as jobs
/// that a statement error cannot stop honestly. <c>CREATE TABLE … AS</c> loads its rows in the
/// statement and drops the new table when the load fails, so it is bounded like
/// <c>INSERT … SELECT</c>.</para>
/// </summary>
public sealed class StatementDeadline
{
    private static readonly AsyncLocal<StatementDeadline?> current = new();

    /// <summary>The deadline on the <see cref="Environment.TickCount64"/> clock.</summary>
    private readonly long deadline;

    private int ended;

    /// <summary>The token source of a read, linked to the request's token; null for a write.</summary>
    private CancellationTokenSource? readSource;

    /// <summary>The request's own token, which tells a timeout from a client that went away.</summary>
    private CancellationToken requestToken;

    private StatementDeadline(int timeoutMs)
    {
        TimeoutMs = timeoutMs;
        deadline = Environment.TickCount64 + timeoutMs;
    }

    /// <summary>The limit the statement started with, in milliseconds.</summary>
    public int TimeoutMs { get; }

    /// <summary>
    /// The deadline of the statement this flow runs, or null when the statement has no limit, when no
    /// statement runs, or when the statement that set it has ended.
    /// </summary>
    public static StatementDeadline? Current => current.Value is { } live && Volatile.Read(ref live.ended) == 0 ? live : null;

    /// <summary>The deadline as a value of <see cref="Environment.TickCount64"/>.</summary>
    public long DeadlineTicks => deadline;

    /// <summary>The milliseconds left before the deadline; zero once it has passed.</summary>
    public long RemainingMs => Math.Max(0, deadline - Environment.TickCount64);

    /// <summary>True when the deadline has passed and the statement has not ended.</summary>
    public bool IsExpired => Volatile.Read(ref ended) == 0 && Environment.TickCount64 >= deadline;

    /// <summary>
    /// True when the read token fired because of the deadline: it fired, and the request's own token
    /// did not. A client that went away gets the bare cancellation it got before, not this error.
    /// </summary>
    internal bool TimedOut =>
        readSource is { IsCancellationRequested: true } && !requestToken.IsCancellationRequested;

    /// <summary>Fails the statement when its deadline has passed.</summary>
    public void ThrowIfExpired()
    {
        if (IsExpired)
            throw Exceeded();
    }

    /// <summary>The error of a statement that ran past its deadline.</summary>
    public CamusDBException Exceeded() => Exceeded(TimeoutMs);

    /// <summary>The error of a statement that ran past a limit of <paramref name="timeoutMs"/> milliseconds.</summary>
    public static CamusDBException Exceeded(int timeoutMs) =>
        new(CamusDBErrorCodes.StatementTimeout,
            $"The statement ran longer than {timeoutMs} ms (statement_timeout_ms) and was stopped");

    /// <summary>
    /// Checks the deadline of the statement this flow runs. For a loop that reads the ambient value
    /// once per row; a loop that runs many rows reads <see cref="Current"/> once and calls
    /// <see cref="ThrowIfExpired"/> on it.
    /// </summary>
    public static void ThrowIfCurrentExpired() => Current?.ThrowIfExpired();

    /// <summary>
    /// Starts the clock of a statement in this flow and returns it, or returns the deadline of the
    /// statement this one runs inside.
    ///
    /// <para>Call it from the async method that owns the statement, so the value does not reach the
    /// caller, and end the deadline (<see cref="End"/>) on every path when <paramref name="owned"/> is
    /// true. A statement inside a statement gets the outer deadline with <paramref name="owned"/>
    /// false, and fails here when that deadline has already passed.</para>
    /// </summary>
    /// <param name="timeoutMs">The limit, <see cref="CamusDBOptions.StatementTimeoutMs"/>; zero or less for none.</param>
    /// <param name="owned">True when this call started the clock, and so must end it.</param>
    /// <returns>The deadline the statement runs under, or null when it has none.</returns>
    internal static StatementDeadline? Begin(int timeoutMs, out bool owned)
    {
        StatementDeadline? outer = Current;

        if (outer is not null)
        {
            owned = false;
            outer.ThrowIfExpired();
            return outer;
        }

        if (timeoutMs <= 0)
        {
            owned = false;
            return null;
        }

        StatementDeadline started = new(timeoutMs);
        current.Value = started;
        owned = true;
        return started;
    }

    /// <summary>
    /// Takes the deadline off this flow: for schema DDL, which is not bounded. Call it from the async
    /// method that runs the statement, so the deadline comes back when that method returns.
    /// </summary>
    internal static void Suspend()
    {
        if (current.Value is not null)
            current.Value = null;
    }

    /// <summary>
    /// True for a schema statement that the deadline bounds: <c>CREATE TABLE … AS</c>, which loads its
    /// rows in the statement and drops the new table when the load fails.
    /// </summary>
    internal static bool BoundsSchemaStatement(NodeType nodeType) =>
        nodeType is NodeType.CreateTableAsSelect or NodeType.CreateTableAsSelectIfNotExists;

    /// <summary>
    /// Ends the statement. A flow that still holds the value (a background task the statement
    /// started) finds no deadline afterwards. Releases the read token source.
    /// </summary>
    internal void End()
    {
        if (Interlocked.Exchange(ref ended, 1) != 0)
            return;

        // Dispose stops a pending deadline timer, and releases the registration on the request's token.
        // Nothing reads the token after the end: the cursor that carried it has ended too.
        readSource?.Dispose();
    }

    /// <summary>
    /// The token a row-returning statement runs under: it fires when <paramref name="request"/>
    /// fires, and at the deadline once <see cref="ArmReadToken"/> has run. Until then the deadline
    /// does not fire it, because the parse has not yet shown whether the statement is a read.
    /// </summary>
    internal CancellationToken LinkReadToken(CancellationToken request)
    {
        requestToken = request;
        readSource = CancellationTokenSource.CreateLinkedTokenSource(request);
        return readSource.Token;
    }

    /// <summary>
    /// Lets the deadline fire the read token. Called once the parse shows a read: a write never gets
    /// this call, and its token fires only when the request ends, as before. A no-op on a deadline
    /// with no read token (a write, or a statement inside one).
    /// </summary>
    internal void ArmReadToken()
    {
        if (readSource is null || Volatile.Read(ref ended) != 0)
            return;

        long remaining = RemainingMs;

        if (remaining <= 0)
        {
            readSource.Cancel();
            return;
        }

        readSource.CancelAfter(TimeSpan.FromMilliseconds(remaining));
    }

    /// <summary>
    /// Wraps the cursor of a row-returning statement: every row is read with this deadline as the
    /// flow's ambient value, a cancellation that the deadline caused becomes
    /// <see cref="CamusDBErrorCodes.StatementTimeout"/>, and the deadline ends when the cursor is
    /// disposed.
    /// </summary>
    internal IAsyncEnumerable<T> Wrap<T>(IAsyncEnumerable<T> cursor) => new DeadlineEnumerable<T>(this, cursor);

    /// <summary>Makes this deadline the ambient value of the calling flow, if it is not already.</summary>
    private void Enter()
    {
        if (!ReferenceEquals(current.Value, this))
            current.Value = this;
    }

    /// <summary>
    /// Bounds a peer's query fragment by the time its coordinator had left when it sent it
    /// (<see cref="Models.Queries.QueryFragmentRequest.StatementTimeoutRemainingMs"/>). The coordinator
    /// cancels its fragments at its own deadline; this is the bound when that cancel does not arrive.
    /// A fragment is a read, so the token stops it.
    /// </summary>
    internal static async IAsyncEnumerable<T> BoundFragment<T>(
        long remainingMs,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken request,
        Func<CancellationToken, IAsyncEnumerable<T>> run)
    {
        using CancellationTokenSource source = CancellationTokenSource.CreateLinkedTokenSource(request);
        source.CancelAfter(TimeSpan.FromMilliseconds(Math.Max(1, remainingMs)));

        await using IAsyncEnumerator<T> rows = run(source.Token).GetAsyncEnumerator(source.Token);

        while (true)
        {
            bool next;

            try
            {
                next = await rows.MoveNextAsync().ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (source.IsCancellationRequested && !request.IsCancellationRequested)
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.StatementTimeout,
                    "The statement this query fragment serves passed its time limit (statement_timeout_ms)");
            }

            if (!next)
                yield break;

            yield return rows.Current;
        }
    }

    private sealed class DeadlineEnumerable<T>(StatementDeadline owner, IAsyncEnumerable<T> inner) : IAsyncEnumerable<T>
    {
        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            owner.Enter();
            return new DeadlineEnumerator<T>(owner, inner.GetAsyncEnumerator(cancellationToken));
        }
    }

    /// <summary>
    /// Sets the ambient value in the synchronous part of each call, so the inner cursor and every
    /// continuation it schedules run with it; see <c>ExtensionScopedCursor</c> for the same pattern.
    /// The value stays in the reader's flow after the call, and <see cref="End"/> makes it inert there.
    /// </summary>
    private sealed class DeadlineEnumerator<T>(StatementDeadline owner, IAsyncEnumerator<T> inner) : IAsyncEnumerator<T>
    {
        public T Current => inner.Current;

        public ValueTask<bool> MoveNextAsync()
        {
            owner.Enter();

            ValueTask<bool> next;

            try
            {
                next = inner.MoveNextAsync();
            }
            catch (OperationCanceledException) when (owner.TimedOut)
            {
                throw owner.Exceeded();
            }

            return next.IsCompletedSuccessfully ? next : AwaitNext(next);
        }

        private async ValueTask<bool> AwaitNext(ValueTask<bool> next)
        {
            try
            {
                return await next.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (owner.TimedOut)
            {
                throw owner.Exceeded();
            }
        }

        public async ValueTask DisposeAsync()
        {
            owner.Enter();

            try
            {
                await inner.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                owner.End();
            }
        }
    }
}
