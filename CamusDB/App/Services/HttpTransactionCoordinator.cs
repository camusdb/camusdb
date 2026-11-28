
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Diagnostics;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Transactions;
using CamusDB.App.Models;
using Kommander.Time;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging.Abstractions;

namespace CamusDB.App.Services;

/// <summary>
/// Tracks in-flight <see cref="KvTransaction"/> instances for the HTTP and gRPC API and delegates
/// lifecycle operations to each database's <see cref="KvTransactionsManager"/>.
///
/// <para>A statement of an explicit transaction runs between <see cref="EnterStatement"/> and the
/// dispose of the <see cref="RunningStatement"/> it returns. An autocommit transaction belongs to its
/// request from its begin until that request finalizes it. The idle reaper rolls back neither one while
/// it runs; see <see cref="ActiveTransaction"/> for the rule.</para>
///
/// <para>It is also the engine's <see cref="ITransactionActivitySource"/>: <c>SHOW TRANSACTIONS</c> lists
/// its explicit transactions and <c>KILL TRANSACTION</c> ends one (<see cref="KillAsync"/>). It attaches
/// itself to the engine when it is built.</para>
/// </summary>
public sealed class HttpTransactionCoordinator : ITransactionActivitySource
{
    /// <summary>
    /// How long the record of a killed transaction stays, so its client's next request reports the kill
    /// rather than an unknown transaction. A client that comes back later gets "Unknown transaction".
    /// </summary>
    internal static readonly TimeSpan KillRecordRetention = TimeSpan.FromHours(1);

    /// <summary>
    /// The most kill records kept at once. Past it a kill keeps no record, and its client gets "Unknown
    /// transaction" instead of the kill's error: the kill itself is unaffected.
    /// </summary>
    internal const int MaxKillRecords = 10_000;

    private readonly CommandExecutor executor;

    private readonly ILogger logger;

    private readonly ConcurrentDictionary<(long L, uint C), ActiveTransaction> active = new();

    /// <summary>
    /// Killed transactions that already left <see cref="active"/>, by client handle. A record is added
    /// before its entry leaves the map, so a request of the killed client finds one or the other.
    /// </summary>
    private readonly ConcurrentDictionary<(long L, uint C), KillRecord> killRecords = new();

    /// <param name="Message">The error message the client of the killed transaction reads.</param>
    /// <param name="OwnerName">The owner, so a kill repeated by another user still reports "not found".</param>
    /// <param name="AtTicks">The <see cref="Stopwatch"/> tick of the kill, for the retention.</param>
    /// <param name="RollbackFailure">
    /// The message of a rollback that failed, or null when it succeeded. A repeated kill reports it as
    /// <see cref="CamusDBErrorCodes.TransactionKillRollbackFailed"/> instead of success, because the
    /// release of the transaction's locks was not confirmed.
    /// </param>
    private sealed record KillRecord(string Message, string? OwnerName, string? OwnerId, long AtTicks, string? RollbackFailure);

    /// <summary>
    /// Test seam: wraps the rollback that a kill runs. It receives the real rollback, so a test can
    /// delay it or make it fail after it ran. Production code leaves it null, and the kill calls
    /// <see cref="KvTransactionsManager.AbandonAsync"/> directly.
    /// </summary>
    internal Func<Func<Task>, Task>? KillRollbackForTest { get; set; }

    /// <summary>
    /// Explicit transactions this node tracks, for <see cref="CamusDBOptions.MaxOpenExplicitTransactions"/>.
    /// A slot is reserved before the begin and released when the begin fails or when the entry leaves
    /// <see cref="active"/>. The release rides the map's removal, which succeeds exactly once per
    /// entry, so a retried commit or a reap that races a rollback cannot release a slot twice.
    /// </summary>
    private int openExplicit;

    /// <param name="executor">The engine. The coordinator attaches itself as the engine's source of open transactions.</param>
    /// <param name="logger">Receives the audit line of each kill. Null writes nothing.</param>
    public HttpTransactionCoordinator(CommandExecutor executor, ILogger<HttpTransactionCoordinator>? logger = null)
    {
        this.executor = executor;
        this.logger = logger ?? (ILogger)NullLogger.Instance;
        executor.QueryActivityService.AttachTransactions(this);
    }

    /// <summary>
    /// Number of in-flight explicit transactions this node is tracking. Used as a lightweight
    /// foreground-load signal so background work (e.g. auto-analyze) can back off when the node is busy.
    /// </summary>
    public int ActiveCount => active.Count;

    /// <summary>Number of explicit (client-owned) transactions this node is tracking.</summary>
    public int OpenExplicitCount => Volatile.Read(ref openExplicit);

    public async Task<KvTransaction> StartAsync(string databaseName, CancellationToken cancellationToken = default) =>
        await StartAsync(databaseName, isolationLevel: null, transactionMode: null, locking: null, cancellationToken: cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Starts a new transaction with the requested isolation level, mode, locking strategy, and
    /// admission priority. When any argument is <see langword="null"/> the server default applies:
    /// <see cref="CamusDBOptions.DefaultIsolationLevel"/>, <see cref="CamusTransactionMode.ReadWrite"/>,
    /// <see cref="CamusDBOptions.DefaultTransactionLocking"/>, and
    /// <see cref="CamusDBOptions.DefaultTransactionPriority"/> respectively.
    ///
    /// <para><paramref name="deferStart"/> requests deferred Kahuna-session start: the transaction is
    /// begun without opening a coordinator session so a following <c>SET TRANSACTION LOCKING</c> or
    /// <c>SET TRANSACTION PRIORITY</c> can still choose those values (which Kahuna pins at session
    /// start). Pass it only for explicit interactive transactions (<c>/start-transaction</c>);
    /// autocommit statements start eagerly.</para>
    /// </summary>
    /// <param name="sessionOwned">
    /// True when the caller is handling an explicit <c>BEGIN</c> / <c>START TRANSACTION</c> and the
    /// client will commit or roll the transaction back itself. The transaction is marked so a
    /// statement that cannot honour a later <c>ROLLBACK</c> — <c>TRUNCATE</c>, or
    /// <c>ALTER TABLE ... REWRITE STORAGE</c> — can refuse to run inside it. Leave false for the
    /// server-side transaction wrapped around one autocommit statement: the request owns that one and
    /// must finalize it on every path, because the idle reaper takes it only after a finalize failed.
    /// </param>
    /// <param name="cancellationToken">
    /// Ends the begin. A token that is cancelled before the begin refuses it, and nothing was written yet.
    /// Do not use the token of a call for the commit or the rollback of the transaction: a finalize that a
    /// disconnect cuts leaves the locks of the transaction to expire.
    /// </param>
    /// <param name="causalToken">
    /// The causal token of the client's last commit, or null. Every Read Committed SELECT of the
    /// transaction reads at a statement timestamp at least this value; see
    /// <see cref="KvTransaction.CausalFloor"/>.
    /// </param>
    public async Task<KvTransaction> StartAsync(
        string databaseName,
        CamusIsolationLevel? isolationLevel,
        CamusTransactionMode? transactionMode,
        KeyValueTransactionLocking? locking = null,
        bool deferStart = false,
        TransactionPriority? priority = null,
        bool sessionOwned = false,
        CancellationToken cancellationToken = default,
        HLCTimestamp? causalToken = null)
    {
        // The begin itself looks at the token only where it waits, for example for a remote leader. A
        // caller that is already gone must not start a transaction on any node.
        cancellationToken.ThrowIfCancellationRequested();

        if (string.IsNullOrEmpty(databaseName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "DatabaseName is required to start a transaction");

        if (sessionOwned)
            ReserveExplicitSlot();

        bool registered = false;
        try
        {
            DatabaseDescriptor database = await executor.OpenDatabase(databaseName).ConfigureAwait(false);

            KvTransaction tx = await database.Transactions.BeginAsync(
                isolationLevel, transactionMode, locking: locking, deferStart: deferStart,
                priority: priority, cancellationToken: cancellationToken, causalToken: causalToken).ConfigureAwait(false);
            if (sessionOwned)
            {
                tx.MarkSessionOwned();

                // Before the handle is published, so every reader of the list sees the origin. The
                // principal is the ambient one of a REST request; a transport that resolved none leaves
                // the owner to the first statement.
                tx.Activity = new TransactionActivity(
                    databaseName, StatementOrigin.Current, AuthorizationContext.Current.Principal);
            }

            await RegisterAsync(database.Transactions, tx, sessionOwned).ConfigureAwait(false);
            registered = true;
            return tx;
        }
        finally
        {
            // A begin that failed holds no entry, so nothing else will release its slot.
            if (sessionOwned && !registered)
                Interlocked.Decrement(ref openExplicit);
        }
    }

    /// <summary>
    /// Takes one slot of <see cref="CamusDBOptions.MaxOpenExplicitTransactions"/>, or refuses with
    /// <see cref="CamusDBErrorCodes.ConcurrencyLimitExceeded"/>. Increment first, then compare, so two
    /// concurrent begins cannot both take the last slot. The limit is read from the engine's current
    /// snapshot on each begin, so a changed value applies to the next begin.
    /// </summary>
    private void ReserveExplicitSlot()
    {
        int limit = executor.Options.MaxOpenExplicitTransactions;
        int open = Interlocked.Increment(ref openExplicit);

        if (limit > 0 && open > limit)
        {
            Interlocked.Decrement(ref openExplicit);
            throw new CamusDBException(
                CamusDBErrorCodes.ConcurrencyLimitExceeded,
                $"This node already has {limit} open explicit transactions, the most it allows " +
                "(max_open_explicit_transactions). Commit or roll back open transactions, or retry later.");
        }
    }

    /// <summary>Removes the entry under <paramref name="key"/> and releases its slot when it holds one.</summary>
    private bool TryRemoveEntry((long L, uint C) key, out ActiveTransaction? entry)
    {
        if (!active.TryRemove(key, out entry))
            return false;

        if (entry.IsExplicit)
            Interlocked.Decrement(ref openExplicit);

        return true;
    }

    /// <summary>
    /// Begins the read-only transaction of an autocommit SELECT in <paramref name="databaseName"/>.
    /// It is a zero-identity snapshot in every routing mode (see
    /// <see cref="KvTransactionsManager.BeginReadOnly"/>): no <c>StartTransaction</c> /
    /// <c>CommitTransaction</c> round trips and no range locks, so commit and rollback are no-ops and
    /// the transaction is not registered here.
    ///
    /// <para><paramref name="declaredReadOnly"/> is true when the request asked for
    /// <c>ReadOnly</c>. Then the statement also refuses <c>nextval()</c> and <c>setval()</c> (see
    /// <see cref="KvTransaction.IsDeclaredReadOnly"/>). A request that asked for no mode can still
    /// draw from a sequence.</para>
    /// </summary>
    public async Task<KvTransaction> BeginReadOnlyAsync(string databaseName, HLCTimestamp? causalToken = null, bool declaredReadOnly = false)
    {
        if (string.IsNullOrEmpty(databaseName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "DatabaseName is required");

        DatabaseDescriptor database = await executor.OpenDatabase(databaseName).ConfigureAwait(false);
        return database.Transactions.BeginReadOnly(causalToken, declaredReadOnly);
    }

    /// <summary>
    /// Resolves the handle of a tracked transaction for a commit or a rollback, and starts its idle time
    /// from now. A statement must use <see cref="EnterStatement"/> instead: this method does not mark
    /// anything as running, so the reaper could take the transaction before the statement ends.
    /// </summary>
    public KvTransaction GetState(long txnIdPT, uint txnIdCounter)
    {
        ActiveTransaction entry = Resolve(txnIdPT, txnIdCounter);

        if (!entry.TryTouch())
            throw FinalizeInProgress(entry);

        return entry.Transaction;
    }

    /// <summary>
    /// Starts a statement on the explicit transaction the handle names. The transaction is not idle
    /// until the returned value is disposed, so the caller holds it for the whole statement, the read of
    /// its rows included.
    ///
    /// <para>Throws <see cref="CamusDBErrorCodes.InvalidInput"/> for a handle that is not tracked, and
    /// <see cref="CamusDBErrorCodes.TransactionAlreadyCompleted"/> while a commit, a rollback or a reap
    /// owns the transaction. The second refusal keeps a data statement from running beside a finalize
    /// of the same session, ahead of the server-side fence and of the transaction's own status
    /// check.</para>
    /// </summary>
    public RunningStatement EnterStatement(long txnIdPT, uint txnIdCounter)
    {
        ActiveTransaction entry = Resolve(txnIdPT, txnIdCounter);

        if (!entry.TryEnterStatement())
            throw FinalizeInProgress(entry);

        return new RunningStatement(entry);
    }

    /// <summary>
    /// <see cref="EnterStatement"/> that answers the default value instead of an error when the handle
    /// is not tracked or a finalize owns the transaction. For a request that already arrived and waits
    /// for the server before its statement can start: the wait is not idle time. The statement itself
    /// still enters on its own and reports the error.
    /// </summary>
    public RunningStatement TryEnterStatement(long txnIdPT, uint txnIdCounter) =>
        active.TryGetValue((txnIdPT, txnIdCounter), out ActiveTransaction? entry) && entry.TryEnterStatement()
            ? new RunningStatement(entry)
            : default;

    private ActiveTransaction Resolve(long txnIdPT, uint txnIdCounter)
    {
        if (!active.TryGetValue((txnIdPT, txnIdCounter), out ActiveTransaction? entry))
        {
            // A killed transaction leaves the map when its rollback ends. Its client reads why.
            if (killRecords.TryGetValue((txnIdPT, txnIdCounter), out KillRecord? record))
                throw new CamusDBException(CamusDBErrorCodes.TransactionKilled, record.Message);

            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Unknown transaction");
        }

        return entry;
    }

    /// <summary>
    /// The error for a statement, commit or rollback that a finalize claim or a kill refused: the kill's
    /// own error when a kill fenced the transaction, and "in progress" otherwise.
    /// </summary>
    private static CamusDBException Refusal(ActiveTransaction entry, string inProgress) =>
        entry.IsKilled ? entry.Transaction.KilledError() : new CamusDBException(CamusDBErrorCodes.TransactionAlreadyCompleted, inProgress);

    private static CamusDBException FinalizeInProgress(ActiveTransaction entry) => Refusal(
        entry, "A commit or rollback is in progress for this transaction; no further statements may run");

    private static CamusDBException CommitInProgress(ActiveTransaction entry) => Refusal(
        entry, "A commit or rollback is already in progress for this transaction");

    /// <summary>
    /// Rolls back every tracked transaction that has been idle for at least
    /// <paramref name="idleTimeout"/>, releasing its locks and dropping it from the in-flight map.
    /// Called periodically by the background reaper to reclaim abandoned transactions: a client that
    /// opened a transaction and never committed or rolled back, and an autocommit transaction whose
    /// request failed to finalize it. Returns the number of transactions reaped.
    ///
    /// <para>A transaction with a running statement is not idle, however long the statement runs, and
    /// neither is an autocommit transaction whose request still runs. The idle time counts from the end
    /// of the last statement. The check and the finalize claim are one step
    /// (<see cref="ActiveTransaction.TryClaimForReap"/>), so a statement that starts during the sweep is
    /// either refused or stops the reap of its transaction.</para>
    ///
    /// <para>Best-effort per entry: a rollback that throws (e.g. a transient Kahuna error) is swallowed
    /// so one stuck entry cannot stall the sweep. A commit or rollback that claimed the entry first
    /// keeps it, and a later one loses the claim or no longer finds the entry. A transaction the client
    /// left in the middle of its finalize cannot be rolled back and only gives up the DDL lock it may
    /// hold (<see cref="KvTransactionsManager.AbandonAsync"/>).</para>
    /// </summary>
    public async Task<int> ReapIdleAsync(TimeSpan idleTimeout, CancellationToken cancellationToken = default)
    {
        int reaped = 0;

        foreach (KeyValuePair<(long L, uint C), ActiveTransaction> pair in active)
        {
            cancellationToken.ThrowIfCancellationRequested();

            ActiveTransaction entry = pair.Value;

            // The claim comes first and decides everything: a running statement or a commit/rollback in
            // flight refuses it, and after it no statement and no other finalize can start.
            if (!entry.TryClaimForReap(idleTimeout))
                continue;

            // No await between the claim and the removal, so a client's next request finds the entry
            // gone. False when a rollback retired an entry whose transaction was already terminal.
            if (!TryRemoveEntry(pair.Key, out _))
                continue;

            try
            {
                await entry.Manager.AbandonAsync(entry.Transaction, cancellationToken).ConfigureAwait(false);
                reaped++;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch
            {
                // Best-effort: the transaction's lock TTL / lifetime cap is the ultimate backstop.
            }
        }

        return reaped;
    }

    /// <summary>
    /// Adds this node's open explicit transactions for <c>SHOW TRANSACTIONS</c>. Each entry's state is
    /// read under its own lock; the list as a whole is not one instant, as no list of a concurrent map is.
    /// </summary>
    public void Snapshot(List<TrackedTransaction> into)
    {
        // Enumerating the dictionary takes no lock; its Values property would take every one.
        foreach (KeyValuePair<(long L, uint C), ActiveTransaction> pair in active)
        {
            ActiveTransaction entry = pair.Value;
            if (!entry.IsExplicit)
                continue;

            (string state, double ageMs, double idleMs) = entry.Describe();
            into.Add(new TrackedTransaction(entry.Transaction, state, ageMs, idleMs));
        }
    }

    /// <summary>
    /// Ends an open explicit transaction for <c>KILL TRANSACTION</c>; see
    /// <see cref="ITransactionActivitySource.KillAsync"/> for the contract.
    ///
    /// <para><b>One finalize wins.</b> The kill and a commit, a rollback or the reaper compete for the
    /// same claim (<see cref="ActiveTransaction.TryBeginKill"/>). When another one holds it, the kill
    /// waits up to <paramref name="wait"/> for it to end and reports what it did: committed, rolled back,
    /// or still in progress. A finalize that failed and released its claim lets the kill try again.</para>
    ///
    /// <para><b>Running statements.</b> A kill that meets a running statement fences the transaction,
    /// stops its reads through <see cref="KvTransaction.KillToken"/> and its writes at their next write,
    /// and leaves the rollback to the end of the last statement.</para>
    ///
    /// <para><b>The rollback.</b> It is the reaper's: <see cref="KvTransactionsManager.AbandonAsync"/>
    /// with no cancellation, so the change set of transactional DDL is discarded and the DDL lock freed,
    /// and a coordinator session that is unknown parks the key mirror for a later release. The
    /// coordinator owns it as a task, also when no statement runs, and this call waits for it up to
    /// <paramref name="wait"/>; past that it reports <see cref="TransactionKillOutcome.Pending"/>, and the
    /// rollback runs anyway. A record of the kill is kept before the entry leaves the map, so the
    /// client's next request reports the kill, and a repeated kill reports a rollback that failed as
    /// failed. One audit line names who killed which transaction.</para>
    /// </summary>
    public async Task<TransactionKillOutcome> KillAsync(
        long physical, uint counter, ActivityViewer viewer, TransactionKiller killedBy, TimeSpan wait)
    {
        (long L, uint C) key = (physical, counter);
        long deadline = Stopwatch.GetTimestamp() + (long)(wait.TotalSeconds * Stopwatch.Frequency);
        KvTransaction? seen = null;

        while (true)
        {
            if (!active.TryGetValue(key, out ActiveTransaction? entry))
            {
                // The entry left while this kill waited for another finalize: report what that one did.
                if (seen is not null)
                    return seen.Status switch
                    {
                        KvTransactionStatus.Committed => TransactionKillOutcome.Committed,
                        KvTransactionStatus.RolledBack => TransactionKillOutcome.RolledBack,
                        _ => TransactionKillOutcome.Finalizing,
                    };

                // A kill that already ended reports its outcome again, to a viewer who may see it. A
                // rollback that failed is reported as failed every time, never as a kill that worked.
                if (!killRecords.TryGetValue(key, out KillRecord? record) || !viewer.CanSee(record.OwnerName, record.OwnerId))
                    return TransactionKillOutcome.NotFound;

                if (record.RollbackFailure is not null)
                    throw new CamusDBException(CamusDBErrorCodes.TransactionKillRollbackFailed, record.RollbackFailure);

                return TransactionKillOutcome.Killed;
            }

            if (!entry.IsExplicit || entry.Transaction.Activity is not { } activity || !activity.VisibleTo(viewer))
                return TransactionKillOutcome.NotFound;

            seen = entry.Transaction;

            KillClaim claim = entry.TryBeginKill(
                killedBy.ReasonForClient(DateTime.UtcNow), out Task? whenStatementsEnd, out Task<TransactionKillOutcome>? outcome);

            switch (claim)
            {
                case KillClaim.Claimed:
                    LogKill(entry, activity, killedBy, pending: false);

                    // The coordinator owns the rollback, and this call waits for it only up to the
                    // deadline: a slow rollback answers Pending, as a running statement does, so a
                    // forwarded kill gets its answer before the peer call times out.
                    _ = RollBackKilledAsync(key, entry);
                    return await WaitForKillAsync(outcome!, deadline).ConfigureAwait(false);

                case KillClaim.Pending:
                    LogKill(entry, activity, killedBy, pending: true);

                    // Outside the entry's lock: a read of the transaction stops at its next token check
                    // with the kill's error, and a write stops at its next write.
                    entry.Transaction.CancelKillToken();

                    _ = RollBackWhenStatementsEndAsync(key, entry, whenStatementsEnd!);
                    return await WaitForKillAsync(outcome!, deadline).ConfigureAwait(false);

                case KillClaim.AlreadyKilled:
                    return outcome is null
                        ? TransactionKillOutcome.Pending
                        : await WaitForKillAsync(outcome, deadline).ConfigureAwait(false);

                case KillClaim.OutcomeUnknown:
                    return TransactionKillOutcome.Finalizing;

                default:
                    // A commit, rollback or reap holds the claim. Look again until it ends or the wait does.
                    if (Stopwatch.GetTimestamp() >= deadline)
                        return TransactionKillOutcome.Finalizing;

                    await Task.Delay(10).ConfigureAwait(false);
                    continue;
            }
        }
    }

    /// <summary>
    /// Waits for a kill's rollback until <paramref name="deadline"/>, and reports
    /// <see cref="TransactionKillOutcome.Pending"/> when it has not ended by then. A rollback that failed
    /// throws its error.
    /// </summary>
    private static async Task<TransactionKillOutcome> WaitForKillAsync(Task<TransactionKillOutcome> outcome, long deadline)
    {
        long remaining = deadline - Stopwatch.GetTimestamp();
        if (outcome.IsCompleted || remaining <= 0)
            return outcome.IsCompleted ? await outcome.ConfigureAwait(false) : TransactionKillOutcome.Pending;

        try
        {
            return await outcome.WaitAsync(TimeSpan.FromSeconds((double)remaining / Stopwatch.Frequency)).ConfigureAwait(false);
        }
        catch (TimeoutException) when (!outcome.IsCompleted)
        {
            return TransactionKillOutcome.Pending;
        }
    }

    /// <summary>
    /// Runs the rollback of a fenced transaction when its last running statement ends. The coordinator
    /// owns the task: it never throws, and its outcome reaches the kills that wait through
    /// <see cref="ActiveTransaction.CompleteKill"/> or <see cref="ActiveTransaction.FailKill"/>.
    /// </summary>
    private async Task RollBackWhenStatementsEndAsync((long L, uint C) key, ActiveTransaction entry, Task whenStatementsEnd)
    {
        await whenStatementsEnd.ConfigureAwait(false);
        await RollBackKilledAsync(key, entry).ConfigureAwait(false);
    }

    /// <summary>
    /// Rolls back a transaction whose finalize claim the kill holds, then retires its entry. Never
    /// throws: the outcome, or the error of a rollback that failed, goes to the kills that wait, and
    /// stays in the kill record for the kills that come later.
    /// </summary>
    private async Task RollBackKilledAsync((long L, uint C) key, ActiveTransaction entry)
    {
        KvTransaction tx = entry.Transaction;
        CamusDBException? failure = null;

        try
        {
            Func<Task> rollback = () => entry.Manager.AbandonAsync(tx, CancellationToken.None);
            await (KillRollbackForTest is { } wrap ? wrap(rollback) : rollback()).ConfigureAwait(false);

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("KILL TRANSACTION: transaction {TransactionId} is rolled back", tx.UniqueId);

            entry.CompleteKill(TransactionKillOutcome.Killed);
        }
        catch (Exception exception)
        {
            // The transaction stays fenced and leaves the list, as a reaped one does; the storage
            // node's session expiry is the backstop for whatever the rollback did not release.
            logger.LogWarning(exception, "KILL TRANSACTION: the rollback of transaction {TransactionId} failed", tx.UniqueId);

            failure = new CamusDBException(
                CamusDBErrorCodes.TransactionKillRollbackFailed,
                $"Transaction {tx.UniqueId} is killed, but its rollback failed: {exception.Message}. It accepts no " +
                "further statement; the storage node releases its locks when its session there expires");

            entry.FailKill(failure);
        }
        finally
        {
            AddKillRecord(key, entry, failure);
            TryRemoveEntry(key, out _);
        }
    }

    /// <summary>The audit line of a kill: who killed which transaction, and whether a statement still ran.</summary>
    private void LogKill(ActiveTransaction entry, TransactionActivity activity, TransactionKiller killedBy, bool pending)
    {
        if (!logger.IsEnabled(LogLevel.Warning))
            return;

        logger.LogWarning(
            "KILL TRANSACTION: {KilledBy} killed transaction {TransactionId} (handle {Physical}.{Counter}) of user {Owner} " +
            "in database {Database}, open for {AgeMs} ms{Pending}",
            killedBy.ToString(),
            entry.Transaction.UniqueId,
            entry.Transaction.ClientId.L,
            entry.Transaction.ClientId.C,
            activity.OwnerName ?? "(unknown)",
            activity.Database,
            (long)entry.Describe().AgeMs,
            pending ? "; the rollback waits for its running statement" : "");
    }

    private void AddKillRecord((long L, uint C) key, ActiveTransaction entry, CamusDBException? failure)
    {
        if (killRecords.Count >= MaxKillRecords)
        {
            PurgeKillRecords();
            if (killRecords.Count >= MaxKillRecords)
                return;
        }

        // The client of a transaction whose rollback failed is not told that it is rolled back.
        KvTransaction tx = entry.Transaction;
        string message = failure is null
            ? tx.KilledError().Message
            : $"Transaction {tx.UniqueId} was {tx.KillReason ?? "killed by KILL TRANSACTION"}, and its rollback failed. " +
              "It accepts no further statement; begin a new transaction";

        TransactionActivity? activity = tx.Activity;
        killRecords[key] = new KillRecord(message, activity?.OwnerName, activity?.OwnerId, Stopwatch.GetTimestamp(), failure?.Message);
    }

    /// <summary>Drops the kill records older than <see cref="KillRecordRetention"/>. Called by the reaper on each sweep.</summary>
    public void PurgeKillRecords()
    {
        if (killRecords.IsEmpty)
            return;

        foreach (KeyValuePair<(long L, uint C), KillRecord> pair in killRecords)
        {
            if (Stopwatch.GetElapsedTime(pair.Value.AtTicks) >= KillRecordRetention)
                killRecords.TryRemove(pair);
        }
    }

    /// <summary>
    /// Releases the key mirrors of abandoned transactions that have reached the age at which no
    /// coordinator session can still own their holdings, across every database this node holds open.
    /// Returns how many keys were released.
    ///
    /// <para>Separate from <see cref="ReapIdleAsync"/> and deliberately so: a reap that met the
    /// coordinator-unknown outcome finishes the transaction there and then, but its holdings can only
    /// be released once the transaction is old enough, which is usually later than the reap. The
    /// transaction is gone from the in-flight map by then, so this drains a queue rather than
    /// re-visiting a tracked entry — nothing re-attempts a finalize on a handle already reaped.</para>
    /// </summary>
    public Task<int> ReleaseDueMirroredHoldingsAsync(CancellationToken cancellationToken = default) =>
        executor.ReleaseDueMirroredHoldingsAsync(cancellationToken);

    /// <summary>
    /// Commits the transaction. When the transaction is tracked, a per-transaction finalize claim
    /// ensures at most one commit/rollback drives it at a time: a concurrent duplicate (e.g. a
    /// client that timed out on a slow commit and re-issued it) is rejected with
    /// <see cref="CamusDBErrorCodes.TransactionAlreadyCompleted"/> rather than racing a second
    /// Kahuna 2PC. A commit that fails releases the claim so a later <em>sequential</em> retry can
    /// still proceed. Zero-snapshot / untracked read-only transactions have no shared finalize state
    /// and fall through to the manager, whose own <c>Status</c> guard covers repeat calls.
    /// </summary>
    public async Task<HLCTimestamp> CommitAsync(DatabaseDescriptor database, KvTransaction tx, CancellationToken cancellationToken = default)
    {
        if (!active.TryGetValue(Key(tx), out ActiveTransaction? entry))
        {
            // Not tracked (zero-snapshot read-only fast path, or already finalized+removed by a
            // concurrent winner). Delegate to the manager, which no-ops the zero case and rejects a
            // repeat commit via its own Status check.
            HLCTimestamp untrackedToken = await database.Transactions.CommitAsync(tx, cancellationToken).ConfigureAwait(false);
            Unregister(tx);
            return untrackedToken;
        }

        if (!entry.TryBeginFinalize())
            throw CommitInProgress(entry);

        try
        {
            HLCTimestamp token = await database.Transactions.CommitAsync(tx, cancellationToken).ConfigureAwait(false);
            Unregister(tx);
            return token;
        }
        catch
        {
            ReleaseAfterFailedFinalize(entry, tx);
            throw;
        }
    }

    /// <summary>
    /// Commits the transaction against <paramref name="database"/>, or — when the statement produced
    /// no descriptor — releases the transaction it never used and reports no causal token.
    ///
    /// <para>Every autocommit path calls this instead of <see cref="CommitAsync(DatabaseDescriptor,KvTransaction,CancellationToken)"/>
    /// — query, no-rows and DDL alike — so a statement that opens no database cannot crash the one it
    /// reaches. Two families do that: the server-level queries
    /// (<see cref="CamusDB.Core.SQLParser.StatementScope.IsServerLevelQuery"/>, e.g. <c>SHOW GRANTS</c>)
    /// and the database-scoped mutations
    /// (<see cref="CamusDB.Core.SQLParser.StatementScope.IsDatabaseScopedMutation"/>, e.g.
    /// <c>CREATE USER</c> and <c>GRANT</c>). Each transport is supposed to keep both off this path
    /// entirely; this is the second line of defence for the day a statement is added to the executor
    /// and to neither list. Both such gaps were live and both surfaced the same way — a
    /// <see cref="NullReferenceException"/> reported to the caller <em>after</em> the statement had
    /// already taken effect.</para>
    ///
    /// <para>The rollback is the right release in both cases, and for the same reason: the statement
    /// wrote nothing through this transaction. A server-level query is read-only, and a
    /// database-scoped mutation writes the shared registry or auth keyspace through its own
    /// machinery. So the work is already done and correct by the time this runs, and rolling the
    /// unused transaction back merely releases its session and any shared range locks.</para>
    /// </summary>
    public async Task<HLCTimestamp> CommitOrReleaseAsync(
        DatabaseDescriptor? database, KvTransaction tx, CancellationToken cancellationToken = default)
    {
        if (database is not null)
            return await CommitAsync(database, tx, cancellationToken).ConfigureAwait(false);

        await RollbackIfNotCompletedAsync(tx, cancellationToken).ConfigureAwait(false);
        return default;
    }

    /// <summary>
    /// Commits a tracked transaction using the manager recorded at <see cref="StartAsync"/> time,
    /// without requiring the caller to supply a <see cref="DatabaseDescriptor"/>. Intended for use
    /// by the gRPC service layer, where the transaction handle carries no database reference.
    ///
    /// <para>Shares the per-transaction finalize claim with <see cref="CommitAsync(DatabaseDescriptor,KvTransaction,CancellationToken)"/>
    /// so a concurrent duplicate commit is rejected rather than double-driven. Throws
    /// <see cref="CamusDBErrorCodes.InvalidInput"/> when the transaction is not tracked (i.e.
    /// the handle is unknown or already finalized).</para>
    /// </summary>
    public async Task<HLCTimestamp> CommitTrackedAsync(KvTransaction tx, CancellationToken cancellationToken = default)
    {
        ActiveTransaction entry = Resolve(Key(tx).L, Key(tx).C);

        if (!entry.TryBeginFinalize())
            throw CommitInProgress(entry);

        try
        {
            HLCTimestamp token = await entry.Manager.CommitAsync(tx, cancellationToken).ConfigureAwait(false);
            Unregister(tx);
            return token;
        }
        catch
        {
            ReleaseAfterFailedFinalize(entry, tx);
            throw;
        }
    }

    /// <summary>
    /// Rolls back the transaction. Shares the per-transaction finalize claim with
    /// <see cref="CommitAsync"/> so an explicit rollback and a commit for the same id cannot run
    /// concurrently; the loser is rejected with <see cref="CamusDBErrorCodes.TransactionAlreadyCompleted"/>.
    /// </summary>
    public async Task RollbackAsync(KvTransaction tx, CancellationToken cancellationToken = default)
    {
        if (!active.TryGetValue(Key(tx), out ActiveTransaction? entry))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Unknown transaction");

        if (!entry.TryBeginFinalize())
            throw CommitInProgress(entry);

        try
        {
            await entry.Manager.RollbackAsync(tx, cancellationToken).ConfigureAwait(false);
            Unregister(tx);
        }
        catch
        {
            ReleaseAfterFailedFinalize(entry, tx);
            throw;
        }
    }

    /// <summary>
    /// Rolls back the transaction identified by its client id, treating an id that is no longer
    /// tracked as a successful no-op and returning <see langword="false"/> for it.
    ///
    /// <para>This is the idempotent entry point for a client-issued ROLLBACK. A statement that
    /// fails inside an explicit transaction is already rolled back and untracked by the request
    /// handler that caught it, and the abandoned-transaction reaper does the same for an idle
    /// transaction — so a client that then sends its own rollback (the correct thing for it to do
    /// after an error) would otherwise get "Unknown transaction" for a transaction that is, in
    /// fact, rolled back. Rollback is defined by its end state, and that end state has been
    /// reached, so this reports success instead of an error.</para>
    ///
    /// <para>A finalize that is still <em>in flight</em> for the same id is rejected with
    /// <see cref="CamusDBErrorCodes.TransactionAlreadyCompleted"/> rather than being reported as a
    /// no-op: it may be a commit, whose outcome is the opposite of what the caller asked for, and
    /// its result is not yet known. Once that finalize completes the entry is gone, so a rollback
    /// arriving afterwards takes the no-op path — a client that raced its own commit against its
    /// own rollback cannot be told which one won, and never could.</para>
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when this call performed the rollback; <see langword="false"/> when
    /// the transaction was already finalized and no longer tracked.
    /// </returns>
    public async Task<bool> RollbackByIdAsync(long txnIdPT, uint txnIdCounter, CancellationToken cancellationToken = default)
    {
        if (!active.TryGetValue((txnIdPT, txnIdCounter), out ActiveTransaction? entry))
            return false;

        // A tracked entry whose transaction is already terminal has nothing left to roll back: the
        // manager reached the end state on a finalize that then threw (a commit the coordinator
        // aborted is marked RolledBack before CADB0502 is raised). Retire the entry and report the
        // no-op, exactly as for an entry the reaper or a failed statement already removed — otherwise
        // the manager rejects the rollback with TransactionAlreadyCompleted and the entry idles until
        // the reaper, which is where every commit-time conflict of the bank workload was ending up.
        // A killed transaction is being rolled back, or is rolled back when its running statement ends,
        // by the kill itself. That is the end state this rollback asks for. Checked first: the kill
        // retires its own entry after it records the kill, and an early retire here would let the
        // client's next request see an unknown transaction instead of the kill.
        if (entry.IsKilled)
            return false;

        if (IsTerminal(entry.Transaction))
        {
            Unregister(entry.Transaction);
            return false;
        }

        if (!entry.TryBeginFinalize())
            throw CommitInProgress(entry);

        try
        {
            await entry.Manager.RollbackAsync(entry.Transaction, cancellationToken).ConfigureAwait(false);
            Unregister(entry.Transaction);
            return true;
        }
        catch
        {
            ReleaseAfterFailedFinalize(entry, entry.Transaction);
            throw;
        }
    }

    /// <summary>
    /// Best-effort rollback used by DML cleanup paths. If another commit/rollback already owns the
    /// finalize claim this no-ops rather than throwing — the owning finalize is authoritative and
    /// this cleanup must never race it or mask the real outcome.
    /// </summary>
    public async Task RollbackIfNotCompletedAsync(KvTransaction tx, CancellationToken cancellationToken = default)
    {
        if (!active.TryGetValue(Key(tx), out ActiveTransaction? entry))
            return;

        if (!entry.TryBeginFinalize())
            return;

        try
        {
            await entry.Manager.RollbackIfNotCompletedAsync(tx, cancellationToken).ConfigureAwait(false);
            Unregister(tx);
        }
        catch
        {
            ReleaseAfterFailedFinalize(entry, tx);
            throw;
        }
    }

    /// <summary>
    /// Registers a transaction under its wire key. The key is the identity's (physical, counter)
    /// pair — the wire handle has no node component — so it is collision-free only because every
    /// tracked identity is minted by this node's clock (see <c>KvTransactionsManager.MintTrackingId</c>).
    /// A collision therefore means two live transactions would share one key, silently routing one
    /// transaction's statements and finalize to the other; refuse loudly instead of overwriting,
    /// and roll the newcomer back so it cannot leak a live session.
    /// </summary>
    private async Task RegisterAsync(KvTransactionsManager manager, KvTransaction tx, bool isExplicit)
    {
        if (active.TryAdd(Key(tx), new ActiveTransaction(manager, tx, isExplicit)))
            return;

        try { await manager.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false); }
        catch { /* best effort — the session lease is the backstop */ }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Transaction tracking key {Key(tx)} is already registered to a live transaction");
    }

    private void Unregister(KvTransaction tx) =>
        TryRemoveEntry(Key(tx), out _);

    /// <summary>
    /// Test seam: tracks a transaction that <see cref="StartAsync"/> did not begin, such as one whose
    /// coordinator session never existed, so a test can drive the kill and the reaper over it. Production
    /// code registers only through <see cref="StartAsync"/>.
    /// </summary>
    internal Task RegisterForTestAsync(KvTransactionsManager manager, KvTransaction tx, bool isExplicit)
    {
        if (isExplicit)
            Interlocked.Increment(ref openExplicit);

        return RegisterAsync(manager, tx, isExplicit);
    }

    /// <summary>Test seam: claims the finalize gate of a tracked transaction as a concurrent commit would,
    /// so a test can observe the in-flight rejection path without racing two real finalizes.</summary>
    internal bool TryClaimFinalizeForTest(KvTransaction tx) =>
        active.TryGetValue(Key(tx), out ActiveTransaction? entry) && entry.TryBeginFinalize();

    /// <summary>Test seam: releases a gate claimed by <see cref="TryClaimFinalizeForTest"/>.</summary>
    internal void ReleaseFinalizeForTest(KvTransaction tx)
    {
        if (active.TryGetValue(Key(tx), out ActiveTransaction? entry))
            entry.EndFinalize();
    }

    /// <summary>Test seam: how many statements run on a tracked transaction now, so a test can wait until
    /// the request of a statement has reached the server.</summary>
    internal int RunningStatementsForTest(KvTransaction tx) =>
        active.TryGetValue(Key(tx), out ActiveTransaction? entry) ? entry.RunningStatements : 0;

    private static bool IsTerminal(KvTransaction tx) =>
        tx.Status is KvTransactionStatus.Committed or KvTransactionStatus.RolledBack
        // A transaction whose rows committed while the publish of its schema changes is not confirmed
        // stays registered: the client's retried COMMIT completes the publish.
        && tx.SchemaChanges is not { PublicationOutstanding: true };

    /// <summary>
    /// Restores or retires an entry after its finalize attempt threw. A finalize that failed while the
    /// transaction is still live — a transport fault, or a MustRetry budget exhausted with the outcome
    /// unknown — keeps the entry and releases the claim so the client's next COMMIT or ROLLBACK can
    /// retry it. A finalize whose manager call drove the transaction to a terminal status and then
    /// threw leaves nothing to finalize: <see cref="KvTransactionsManager.CommitAsync"/> marks a
    /// coordinator Abort as RolledBack and untracks it before raising CADB0502. Keeping such an entry
    /// made the client's follow-up ROLLBACK fail with TransactionAlreadyCompleted and parked the entry
    /// for the idle reaper — under the bank workload roughly half of all commit-time conflicts were
    /// showing up 300 s later as "Reaped N abandoned transaction(s)", and <c>camus_transaction_active</c>
    /// read them as live the whole time. The entry is unregistered instead.
    /// </summary>
    private void ReleaseAfterFailedFinalize(ActiveTransaction entry, KvTransaction tx)
    {
        if (IsTerminal(tx))
            Unregister(tx);
        else
            entry.EndFinalize();
    }

    private static (long L, uint C) Key(KvTransaction tx) => (tx.ClientId.L, tx.ClientId.C);
}
