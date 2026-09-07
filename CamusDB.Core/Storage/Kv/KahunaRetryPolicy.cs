/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
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
    /// The wall-clock instant, as a <see cref="Stopwatch"/> timestamp, past which a deadline-aware
    /// retry loop gives up. Read once at the top of a loop so the whole loop shares one deadline.
    /// </summary>
    internal long LockWaitDeadlineTicks()
        => Stopwatch.GetTimestamp() + (long)(Stopwatch.Frequency * (options.LockWaitDeadlineMs / 1000.0));

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
            type = await fn().ConfigureAwait(false);
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
            (type, entry) = await fn().ConfigureAwait(false);
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
            (type, revision, ts) = await fn().ConfigureAwait(false);
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
            (type, endpoint, durability) = await fn().ConfigureAwait(false);
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
    /// </summary>
    internal async Task<(KeyValueResponseType, long, HLCTimestamp)> RetryOnMustRetryLocked(
        KvTransaction? tx,
        string operation,
        string key,
        Func<Task<(KeyValueResponseType, long, HLCTimestamp)>> fn,
        CancellationToken ct)
    {
        long deadline = LockWaitDeadlineTicks();
        KeyValueResponseType type;
        long revision;
        HLCTimestamp ts;
        int retries = 0;

        do
        {
            (type, revision, ts) = await fn().ConfigureAwait(false);
            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
            {
                if (Stopwatch.GetTimestamp() >= deadline)
                    throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, messages.LockWaitDeadlineMessage(tx, operation, [key]));
                ServerDiagnostics.AddKvRetryWait("mustretry_locked_2831");
                await Task.Delay(RetryDelayMs(retries), ct).ConfigureAwait(false);
            }
        }
        while (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication && ++retries < MaxKahunaRetries);

        return (type, revision, ts);
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
        KeyValueResponseType type;
        string endpoint;
        KeyValueDurability durability;
        HLCTimestamp holder;
        int retries = 0;

        do
        {
            (type, endpoint, durability, holder) = await fn().ConfigureAwait(false);
            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
            {
                if (Stopwatch.GetTimestamp() >= deadline)
                    throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, messages.LockWaitDeadlineMessage(tx, operation, [key]));
                ServerDiagnostics.AddKvRetryWait("mustretry_locked_2860");
                await Task.Delay(RetryDelayMs(retries), ct).ConfigureAwait(false);
            }
        }
        while (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication && ++retries < MaxKahunaRetries);

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
