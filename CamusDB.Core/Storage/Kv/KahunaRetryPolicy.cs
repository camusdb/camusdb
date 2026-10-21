/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using Grpc.Core;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Transactions;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// The retry rules every Kahuna call made by one table's access paths goes through.
///
/// <para><c>MustRetry</c> and <c>WaitingForReplication</c> are transient: a key carries an
/// uncommitted 2PC write intent, or a partition is not yet serving. They are never an answer, so a
/// caller must retry rather than decode them — turning an unknown into a definitive "not there" is
/// the documented way a write gets silently dropped.</para>
///
/// <para>Two families live here. The <b>static</b> overloads retry on a fixed budget and are what the
/// read paths use, where a transient response is a 2PC signal rather than a lock conflict. The
/// <b>instance</b> members add a wall-clock deadline on every iteration and are what the write and
/// lock-acquisition paths use: they bound deadlock and persistent-lock-conflict latency to roughly
/// <see cref="CamusDBOptions.LockWaitDeadlineMs"/> per operation instead of spinning for the full
/// retry budget, and they raise the diagnostic messages built by
/// <see cref="KvConflictMessageBuilder"/>.</para>
///
/// <para><b>Registered</b> variants additionally fold the operation into the Kahuna coordinator's
/// server-owned working set. Folding is not optional for a transactional mutation or an exclusive
/// point lock: without it <c>LocateAndCommitTransaction(handle)</c> would finalize an empty set and
/// the write would never commit. It requires the transaction's coordinator key plus a per-operation
/// id, which these overloads mint ONCE and reuse across every lock-wait retry — so a retry after a
/// lost response replays the coordinator's cached effect instead of applying the mutation twice.
/// That is the idempotent-retry guarantee. Each logical operation gets its own id, because a
/// distinct key or digest under one id is rejected as a duplicate.</para>
/// </summary>
internal sealed class KahunaRetryPolicy
{
    /// <summary>Maximum attempts a transient response is retried before the operation is abandoned.</summary>
    internal const int MaxKahunaRetries = 32;

    private const int MaxRetryDelayMs = 50;

    private readonly KvConflictMessageBuilder messages;

    /// <summary>Configuration snapshot; swapped atomically by <see cref="ApplyOptions"/>.</summary>
    private CamusDBOptions options;

    internal KahunaRetryPolicy(KvConflictMessageBuilder messages, CamusDBOptions options)
    {
        this.messages = messages;
        this.options = options;
    }

    /// <summary>Swaps in a newly published configuration snapshot. See <see cref="KvTableStore.ApplyOptions"/>.</summary>
    internal void ApplyOptions(CamusDBOptions next) => options = next;

    /// <summary>
    /// Exponential back-off: 1 ms, 2 ms, 4 ms, … capped at <see cref="MaxRetryDelayMs"/>.
    /// Guards against int overflow: <c>1 &lt;&lt; attempt</c> becomes negative for attempt >= 31.
    /// </summary>
    internal static int RetryDelayMs(int attempt) => attempt < 6 ? 1 << attempt : MaxRetryDelayMs;

    /// <summary>
    /// Whether a Kahuna call failed because the node it was forwarded to could not be reached, as
    /// opposed to answering. Kahuna forwards a key-value operation whose partition leader is
    /// another node over its inter-node gRPC streams, and when that node is gone the forwarding
    /// throws the raw transport <see cref="RpcException"/> into this process (its transaction
    /// start/commit/rollback forwarding returns <c>MustRetry</c> instead; the key-value paths do
    /// not). Left alone that exception reached the RPC boundary as a generic internal error: run
    /// recorded ~8,000 <c>Error connecting to subchannel … Connection refused</c>
    /// failures per surviving node in the two seconds between a leader's SIGKILL and the placement
    /// moving, every one returned to a client as <c>CADB0000</c> and retried at once. The truthful
    /// contract is the one <c>MustRetry</c> already has: the operation did not happen (a refused
    /// connection sends nothing; a registered operation replays idempotently), so it is retried on
    /// the same budget and, past it, surfaced as <c>TransactionMustRetry</c> like any other
    /// unconfirmed answer.
    /// </summary>
    /// <remarks>
    /// The status set mirrors Kahuna's own <c>InterNodeTransportFailure</c>: <c>Unavailable</c> and
    /// <c>DeadlineExceeded</c> outright, <c>Cancelled</c> and <c>Internal</c> only when they wrap a
    /// transport cause (a disposed inter-node stream, a socket error) rather than an application
    /// error. A cancelled caller is never retried: its token is the reason, not the transport.
    /// </remarks>
    internal static bool IsTransientTransportFailure(RpcException ex, CancellationToken ct)
    {
        if (ct.IsCancellationRequested)
            return false;

        return ex.StatusCode is StatusCode.Unavailable or StatusCode.DeadlineExceeded
            || (ex.StatusCode is StatusCode.Cancelled or StatusCode.Internal && HasTransportCause(ex));
    }

    private static bool HasTransportCause(RpcException ex)
        => IsTransportException(ex.Status.DebugException) || IsTransportException(ex.InnerException)
           || ex.Status.Detail.Contains("stream write failed", StringComparison.OrdinalIgnoreCase)
           || ex.Status.Detail.Contains("call disposed", StringComparison.OrdinalIgnoreCase);

    private static bool IsTransportException(Exception? ex)
    {
        for (Exception? current = ex; current is not null; current = current.InnerException)
        {
            if (current is System.Net.Http.HttpRequestException or IOException or System.Net.Sockets.SocketException
                or ObjectDisposedException)
                return true;
        }

        return false;
    }

    /// <summary>
    /// Runs one attempt of a Kahuna call, answering <paramref name="transient"/> — the caller's
    /// <c>MustRetry</c>-shaped tuple — when the call failed at the transport instead of answering.
    /// Every other exception propagates unchanged.
    /// </summary>
    internal static async Task<T> InvokeOrTransient<T>(Func<Task<T>> fn, T transient, CancellationToken ct)
    {
        try
        {
            using QueryStageProfile.KahunaScope kahunaCall = QueryStageProfile.MeasureKahuna(KahunaCallKind.KeyValue);
            return await fn().ConfigureAwait(false);
        }
        catch (RpcException ex) when (IsTransientTransportFailure(ex, ct))
        {
            ServerDiagnostics.AddKvRetryWait("transport_unreachable");
            return transient;
        }
    }

    /// <summary>
    /// Runs one attempt of a batched Kahuna call that answers a list, returning <c>null</c> when the
    /// call failed at the transport instead of answering: no item was answered, so the caller treats
    /// every pending item as <c>MustRetry</c> and resends the unchanged batch under the same operation
    /// id. Every other exception propagates unchanged.
    /// </summary>
    internal static async Task<T?> InvokeOrUnanswered<T>(Func<Task<T>> fn, CancellationToken ct) where T : class
    {
        try
        {
            using QueryStageProfile.KahunaScope kahunaCall = QueryStageProfile.MeasureKahuna(KahunaCallKind.KeyValue);
            return await fn().ConfigureAwait(false);
        }
        catch (RpcException ex) when (IsTransientTransportFailure(ex, ct))
        {
            ServerDiagnostics.AddKvRetryWait("transport_unreachable");
            return null;
        }
    }

    /// <summary>
    /// The retryable failure for a Kahuna call that could not reach its partition leader and has no
    /// in-place retry (a streaming scan page, a statement at the API boundary): the same
    /// <see cref="CamusDBErrorCodes.TransactionMustRetry"/> a spent retry budget surfaces, so the
    /// client replays from BeginAsync instead of receiving a generic internal error.
    /// </summary>
    internal static CamusDBException ToMustRetry(RpcException ex, string what)
        => new(
            CamusDBErrorCodes.TransactionMustRetry,
            $"The {what} could not reach its Kahuna partition leader ({ex.StatusCode}: {ex.Status.Detail}) — " +
            "retry the statement from BeginAsync.");

    /// <summary>
    /// The wall-clock instant, as a <see cref="Stopwatch"/> timestamp, past which a deadline-aware
    /// retry loop gives up. Read once at the top of a loop so the whole loop shares one deadline.
    /// </summary>
    internal long LockWaitDeadlineTicks()
        => Stopwatch.GetTimestamp() + (long)(Stopwatch.Frequency * (options.LockWaitDeadlineMs / 1000.0));

    /// <summary>
    /// The instant past which a loop stops waiting on a key that is still replicating or still settling
    /// a committed predecessor (<see cref="KeyValueResponseType.WaitingForReplication"/>). Longer than
    /// <see cref="LockWaitDeadlineTicks"/>: that wait is not a lock conflict and has no deadlock to
    /// resolve. See <see cref="CamusDBOptions.ReplicationWaitDeadlineMs"/>.
    /// </summary>
    internal long ReplicationWaitDeadlineTicks()
        => Stopwatch.GetTimestamp() + (long)(Stopwatch.Frequency * (options.ReplicationWaitDeadlineMs / 1000.0));

    /// <summary>
    /// Retries a Kahuna call that reports only a response type (no value), on the fixed budget.
    /// </summary>
    internal static async Task<KeyValueResponseType> RetryOnMustRetry(
        Func<Task<KeyValueResponseType>> fn,
        CancellationToken ct)
    {
        KeyValueResponseType type;
        int retries = 0;

        do
        {
            type = await InvokeOrTransient(fn, KeyValueResponseType.MustRetry, ct).ConfigureAwait(false);
            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
            {
                ServerDiagnostics.AddKvRetryWait("mustretry_595");
                await Task.Delay(RetryDelayMs(retries), ct).ConfigureAwait(false);
            }
        }
        while (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication && ++retries < MaxKahunaRetries);

        return type;
    }

    /// <summary>
    /// Retries a Kahuna get call that returns <see cref="KeyValueResponseType.MustRetry"/> up to
    /// <see cref="MaxKahunaRetries"/> times with a 1 ms back-off. MustRetry is a transient
    /// condition that occurs when a key has an active write intent from a 2PC prepare phase
    /// that hasn't committed or rolled back yet.
    /// </summary>
    internal static async Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> RetryOnMustRetry(
        Func<Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)>> fn,
        CancellationToken ct)
    {
        KeyValueResponseType type;
        ReadOnlyKeyValueEntry? entry;
        int retries = 0;

        do
        {
            (type, entry) = await InvokeOrTransient(fn, (KeyValueResponseType.MustRetry, null), ct).ConfigureAwait(false);
            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
            {
                ServerDiagnostics.AddKvRetryWait("mustretry_2560");
                await Task.Delay(RetryDelayMs(retries), ct).ConfigureAwait(false);
            }
        }
        while (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication && ++retries < MaxKahunaRetries);

        return (type, entry);
    }

    /// <summary>
    /// The get retry with the arguments of the call passed as <paramref name="state"/> to a static
    /// <paramref name="fn"/>: the per-read form of <see cref="RetryOnMustRetry(Func{Task{ValueTuple{KeyValueResponseType, ReadOnlyKeyValueEntry}}}, CancellationToken)"/>,
    /// with the same retry contract. A point read on a hot path then allocates no closure and no
    /// delegate per call, a read Kahuna answers synchronously (a published head record) completes
    /// without a task object, and a read that suspends boxes one state machine here, not two: the
    /// transport guard of <see cref="InvokeOrTransient{T}"/> is inlined around each attempt.
    /// </summary>
    internal static async ValueTask<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> RetryOnMustRetry<TState>(
        Func<TState, Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)>> fn,
        TState state,
        CancellationToken ct)
    {
        KeyValueResponseType type;
        ReadOnlyKeyValueEntry? entry;
        int retries = 0;

        do
        {
            try
            {
                using QueryStageProfile.KahunaScope kahunaCall = QueryStageProfile.MeasureKahuna(KahunaCallKind.KeyValue);
                (type, entry) = await fn(state).ConfigureAwait(false);
            }
            catch (RpcException ex) when (IsTransientTransportFailure(ex, ct))
            {
                ServerDiagnostics.AddKvRetryWait("transport_unreachable");
                (type, entry) = (KeyValueResponseType.MustRetry, null);
            }

            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
            {
                ServerDiagnostics.AddKvRetryWait("mustretry_2560");
                await Task.Delay(RetryDelayMs(retries), ct).ConfigureAwait(false);
            }
        }
        while (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication && ++retries < MaxKahunaRetries);

        return (type, entry);
    }

    /// <summary>
    /// Retries a Kahuna set/delete call that returns <see cref="KeyValueResponseType.MustRetry"/>.
    /// </summary>
    internal static async Task<(KeyValueResponseType, long, HLCTimestamp)> RetryOnMustRetry(
        Func<Task<(KeyValueResponseType, long, HLCTimestamp)>> fn,
        CancellationToken ct)
    {
        KeyValueResponseType type;
        long revision;
        HLCTimestamp ts;
        int retries = 0;

        do
        {
            (type, revision, ts) = await InvokeOrTransient(fn, (KeyValueResponseType.MustRetry, 0L, HLCTimestamp.Zero), ct).ConfigureAwait(false);
            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
            {
                ServerDiagnostics.AddKvRetryWait("mustretry_2583");
                await Task.Delay(RetryDelayMs(retries), ct).ConfigureAwait(false);
            }
        }
        while (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication && ++retries < MaxKahunaRetries);

        return (type, revision, ts);
    }

    /// <summary>
    /// Retries a Kahuna lock-acquire call that returns <see cref="KeyValueResponseType.MustRetry"/>.
    /// </summary>
    internal static async Task<(KeyValueResponseType, string, KeyValueDurability)> RetryOnMustRetry(
        Func<Task<(KeyValueResponseType, string, KeyValueDurability)>> fn,
        CancellationToken ct)
    {
        KeyValueResponseType type;
        string endpoint;
        KeyValueDurability durability;
        int retries = 0;

        do
        {
            (type, endpoint, durability) = await InvokeOrTransient(fn, (KeyValueResponseType.MustRetry, "", KeyValueDurability.Persistent), ct).ConfigureAwait(false);
            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
            {
                ServerDiagnostics.AddKvRetryWait("mustretry_2606");
                await Task.Delay(RetryDelayMs(retries), ct).ConfigureAwait(false);
            }
        }
        while (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication && ++retries < MaxKahunaRetries);

        return (type, endpoint, durability);
    }

    /// <summary>
    /// Deadline-aware form of the set/delete retry: every transient iteration also checks the
    /// wall-clock deadline and throws <see cref="CamusDBErrorCodes.TransactionMustRetry"/> the moment
    /// it elapses, so a deadlocked pair both abort within roughly
    /// <see cref="CamusDBOptions.LockWaitDeadlineMs"/> instead of after the full retry budget.
    /// A <see cref="KeyValueResponseType.WaitingForReplication"/> answer is a wait on the store, not
    /// on another transaction, and runs on the replication-wait deadline instead, with no attempt cap.
    /// </summary>
    internal async Task<(KeyValueResponseType, long, HLCTimestamp)> RetryOnMustRetryLocked(
        KvTransaction? tx,
        string operation,
        string key,
        Func<Task<(KeyValueResponseType, long, HLCTimestamp)>> fn,
        CancellationToken ct)
    {
        long deadline = LockWaitDeadlineTicks();
        long replicationDeadline = ReplicationWaitDeadlineTicks();
        KeyValueResponseType type;
        long revision;
        HLCTimestamp ts;
        int retries = 0;
        int rounds = 0;

        do
        {
            (type, revision, ts) = await InvokeOrTransient(fn, (KeyValueResponseType.MustRetry, 0L, HLCTimestamp.Zero), ct).ConfigureAwait(false);
            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
            {
                ThrowIfTransientDeadlinePassed(tx, operation, key, type, deadline, replicationDeadline);
                ServerDiagnostics.AddKvRetryWait("mustretry_locked_2831");
                await Task.Delay(RetryDelayMs(rounds++), ct).ConfigureAwait(false);
            }
        }
        // A lag answer spends no attempt of the budget: its loop is bounded by the replication-wait
        // deadline alone, and the backoff keeps growing on its own round counter up to the cap.
        while (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
               && (type == KeyValueResponseType.WaitingForReplication || ++retries < MaxKahunaRetries));

        return (type, revision, ts);
    }

    /// <summary>
    /// Ends a single-key retry loop whose budget ran out: the lock-wait deadline for a
    /// <see cref="KeyValueResponseType.MustRetry"/> answer (a conflict, or a proposal the store turned
    /// away), the replication-wait deadline for a <see cref="KeyValueResponseType.WaitingForReplication"/>
    /// answer. Each throws the message of its own budget.
    /// </summary>
    private void ThrowIfTransientDeadlinePassed(KvTransaction? tx, string operation, string key, KeyValueResponseType type, long lockDeadline, long replicationDeadline)
    {
        long now = Stopwatch.GetTimestamp();

        if (type == KeyValueResponseType.WaitingForReplication)
        {
            if (now >= replicationDeadline)
                throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, messages.ReplicationWaitDeadlineMessage(tx, operation, [key]));
            return;
        }

        if (now >= lockDeadline)
            throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, messages.LockWaitDeadlineMessage(tx, operation, [key]));
    }

    /// <summary>
    /// Deadline-aware form of the lock-acquire retry. See the sibling overload for the deadline rule.
    /// </summary>
    internal async Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)> RetryOnMustRetryLocked(
        KvTransaction? tx,
        string operation,
        string key,
        Func<Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)>> fn,
        CancellationToken ct)
    {
        long deadline = LockWaitDeadlineTicks();
        long replicationDeadline = ReplicationWaitDeadlineTicks();
        KeyValueResponseType type;
        string endpoint;
        KeyValueDurability durability;
        HLCTimestamp holder;
        int retries = 0;
        int rounds = 0;

        do
        {
            (type, endpoint, durability, holder) = await InvokeOrTransient(fn, (KeyValueResponseType.MustRetry, "", KeyValueDurability.Persistent, HLCTimestamp.Zero), ct).ConfigureAwait(false);
            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
            {
                ThrowIfTransientDeadlinePassed(tx, operation, key, type, deadline, replicationDeadline);
                ServerDiagnostics.AddKvRetryWait("mustretry_locked_2860");
                await Task.Delay(RetryDelayMs(rounds++), ct).ConfigureAwait(false);
            }
        }
        while (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
               && (type == KeyValueResponseType.WaitingForReplication || ++retries < MaxKahunaRetries));

        return (type, endpoint, durability, holder);
    }

    /// <summary>
    /// Deadline-aware set/delete retry that also registers the operation with the transaction
    /// coordinator. See the class summary for why the operation id is minted once and reused.
    /// </summary>
    internal Task<(KeyValueResponseType, long, HLCTimestamp)> RetryOnMustRetryRegistered(
        KvTransaction tx,
        string operation,
        string key,
        Func<string, TransactionOperationId, Task<(KeyValueResponseType, long, HLCTimestamp)>> fn,
        CancellationToken ct)
    {
        TransactionOperationId operationId = TransactionOperationId.NewRandom();
        return RetryOnMustRetryLocked(tx, operation, key, () => fn(tx.CoordinatorKey, operationId), ct);
    }

    /// <summary>
    /// Deadline-aware lock-acquire retry that also registers the operation with the transaction
    /// coordinator. See the class summary for why the operation id is minted once and reused.
    /// </summary>
    internal Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)> RetryOnMustRetryRegistered(
        KvTransaction tx,
        string operation,
        string key,
        Func<string, TransactionOperationId, Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)>> fn,
        CancellationToken ct)
    {
        TransactionOperationId operationId = TransactionOperationId.NewRandom();
        return RetryOnMustRetryLocked(tx, operation, key, () => fn(tx.CoordinatorKey, operationId), ct);
    }
}
