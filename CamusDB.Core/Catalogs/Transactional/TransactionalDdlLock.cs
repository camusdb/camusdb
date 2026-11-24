
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using Kahuna;
using Kahuna.Shared.Locks;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Catalogs.Transactional;

/// <summary>
/// The DDL lock of one database: a Kahuna distributed lock that one DDL transaction holds from its first
/// schema change until it ends, and that the schema statements outside a DDL transaction hold together
/// while any of them runs, when transactional DDL is on. One lock per database: two DDL transactions of the
/// same database never build change sets against the same base, so the second waits instead of failing at
/// its commit.
///
/// <para><b>Two kinds of holder.</b> A DDL transaction takes the lock alone (<see cref="AcquireAsync"/>):
/// its owner value is unique. The schema statements outside a DDL transaction take it with one owner value
/// that all of them use on every node (<see cref="AcquireForStatementsAsync"/>), so they do not exclude each
/// other; <see cref="SchemaDdlGate"/> serializes them as it does when transactional DDL is off, and a
/// <c>CREATE TABLE</c> runs beside an index build that takes an hour. Kahuna frees a lock for its whole
/// owner value, so the last statement of one node also frees it for a statement of another node, whose
/// renewer takes it again within <see cref="StatementRenewIntervalMs"/>.</para>
///
/// <para><b>A Kahuna lock, so a transaction on any node can hold it.</b> The in-memory
/// <see cref="SchemaDdlGate"/> serializes proposals on one node only, and an explicit transaction can
/// run its statements on any node.</para>
///
/// <para><b>Order rule: take this lock before the <see cref="SchemaDdlGate"/> semaphore, on every
/// path.</b> A DDL transaction takes the semaphore at its commit while it holds this lock. A path that
/// held the semaphore and then waited for this lock would wait for that commit, and the commit would
/// wait for the semaphore.</para>
///
/// <para><b>A lost lock costs serialization, never correctness.</b> The lease lapses when its holder's
/// process dies, and a leader change of the lock's partition can lose it. The change set's base-version
/// check and the pending-change-set rule of the schema log keep the schema correct without it; a
/// transaction whose base moved fails at its commit with the retryable
/// <see cref="CamusDBErrorCodes.TransactionConflict"/>.</para>
///
/// <para>The lease is short and a renewer extends it while the holder is alive, so a crashed holder
/// blocks DDL for at most one lease.</para>
/// </summary>
internal sealed class TransactionalDdlLock : IAsyncDisposable
{
    /// <summary>How long a holder keeps the lock without a renewal.</summary>
    internal const int LeaseMs = 30_000;

    /// <summary>How often the renewer extends the lease of a DDL transaction.</summary>
    private const int RenewIntervalMs = 10_000;

    /// <summary>
    /// How often the renewer of the statements of one node extends their lease, and takes the lock again
    /// when the last statement of another node freed it.
    /// </summary>
    private const int StatementRenewIntervalMs = 2_000;

    /// <summary>The owner value of every schema statement outside a DDL transaction, on every node.</summary>
    private static readonly byte[] StatementOwner = "ddl:statement"u8.ToArray();

    /// <summary>
    /// How long an acquisition waits for another holder before it gives up with the retryable
    /// <see cref="CamusDBErrorCodes.TransactionMustRetry"/>.
    /// </summary>
    internal static readonly TimeSpan DefaultWaitTimeout = TimeSpan.FromSeconds(30);

    private const int MaxTransientRetries = 10;

    private readonly IKahuna kahuna;

    private readonly string resource;

    private readonly byte[] owner;

    private readonly ILogger? logger;

    private readonly CancellationTokenSource renewal = new();

    private Task renewer = Task.CompletedTask;

    private int released;

    private int lost;

    private TransactionalDdlLock(IKahuna kahuna, string resource, byte[] owner, ILogger? logger)
    {
        this.kahuna = kahuna;
        this.resource = resource;
        this.owner = owner;
        this.logger = logger;
    }

    /// <summary>
    /// True while the lock is known to be lost: its lease lapsed, or another holder took it. Only the
    /// renewer of <see cref="AcquireForStatementsAsync"/> gets it back.
    /// </summary>
    internal bool IsLost => Volatile.Read(ref lost) != 0;

    private bool IsStatementHolder => ReferenceEquals(owner, StatementOwner);

    /// <summary>The Kahuna lock resource of a database's DDL lock.</summary>
    internal static string ResourceOf(string databaseId) => $"{databaseId}/meta/ddllock";

    /// <summary>
    /// Takes the DDL lock of <paramref name="database"/>, waiting up to <paramref name="waitTimeout"/> for
    /// another holder. The owner value is unique per acquisition, so Kahuna never treats a second
    /// acquisition as a re-entry of the first.
    /// </summary>
    internal static async Task<TransactionalDdlLock> AcquireAsync(
        DatabaseDescriptor database, TimeSpan waitTimeout, ILogger? logger, CancellationToken cancellationToken = default)
    {
        TransactionalDdlLock acquired = new(
            database.Kahuna.Kahuna, ResourceOf(database.Id), NewOwner(), logger);

        await acquired.TakeAsync(database, waitTimeout, cancellationToken).ConfigureAwait(false);
        acquired.StartRenewer();

        return acquired;
    }

    /// <summary>
    /// Takes the DDL lock of <paramref name="database"/> for the schema statements of this node that run
    /// outside a DDL transaction, waiting up to <paramref name="waitTimeout"/> for a DDL transaction. Only
    /// <see cref="StatementDdlLock"/> calls it: it counts the statements, so that one holder serves them all.
    /// </summary>
    internal static async Task<TransactionalDdlLock> AcquireForStatementsAsync(
        DatabaseDescriptor database, TimeSpan waitTimeout, ILogger? logger, CancellationToken cancellationToken = default)
    {
        TransactionalDdlLock acquired = new(
            database.Kahuna.Kahuna, ResourceOf(database.Id), StatementOwner, logger);

        await acquired.TakeAsync(database, waitTimeout, cancellationToken).ConfigureAwait(false);
        acquired.StartRenewer();

        return acquired;
    }

    private static byte[] NewOwner() => System.Text.Encoding.UTF8.GetBytes($"ddl:{Guid.NewGuid():N}");

    private async Task TakeAsync(DatabaseDescriptor database, TimeSpan waitTimeout, CancellationToken cancellationToken)
    {
        long deadline = Environment.TickCount64 + (long)waitTimeout.TotalMilliseconds;
        int transient = 0;
        int busyDelayMs = 10;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            (LockResponseType type, _) = await kahuna.LocateAndTryLock(
                resource, owner, LeaseMs, LockDurability.Persistent, cancellationToken).ConfigureAwait(false);

            if (type == LockResponseType.Locked)
                return;

            if (type is LockResponseType.MustRetry or LockResponseType.WaitingForReplication && ++transient < MaxTransientRetries)
            {
                await Task.Delay(transient * 10, cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (type != LockResponseType.Busy && type is not (LockResponseType.MustRetry or LockResponseType.WaitingForReplication))
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionMustRetry,
                    $"The DDL lock of database '{database.Name}' could not be taken ({type}); retry the statement");

            if (Environment.TickCount64 >= deadline)
                throw Busy(database, waitTimeout);

            transient = 0;
            await Task.Delay(busyDelayMs, cancellationToken).ConfigureAwait(false);
            busyDelayMs = Math.Min(busyDelayMs * 2, 250);
        }
    }

    /// <summary>The retryable refusal of a wait for the DDL lock that another holder did not free in time.</summary>
    internal static CamusDBException Busy(DatabaseDescriptor database, TimeSpan waitTimeout) => new(
        CamusDBErrorCodes.TransactionMustRetry,
        $"Another transaction or schema statement holds the DDL lock of database '{database.Name}' and did not end within " +
        $"{(long)waitTimeout.TotalMilliseconds} ms; commit or roll back that transaction, or let the statement end, then retry");

    /// <summary>
    /// Takes the DDL lock of <paramref name="database"/> when nobody holds it, without waiting for a
    /// holder, and returns null when another holder has it. A transient refusal is retried a few times.
    /// For a background check that must not stall behind an open DDL transaction: the resume of a staged
    /// job that a transaction owns, which needs to know that no DDL transaction is open.
    /// </summary>
    internal static async Task<TransactionalDdlLock?> TryAcquireAsync(
        DatabaseDescriptor database, ILogger? logger, CancellationToken cancellationToken = default)
    {
        IKahuna kahuna = database.Kahuna.Kahuna;
        string resource = ResourceOf(database.Id);
        byte[] owner = NewOwner();

        for (int transient = 1; ; transient++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            (LockResponseType type, _) = await kahuna.LocateAndTryLock(
                resource, owner, LeaseMs, LockDurability.Persistent, cancellationToken).ConfigureAwait(false);

            if (type == LockResponseType.Locked)
            {
                TransactionalDdlLock acquired = new(kahuna, resource, owner, logger);
                acquired.StartRenewer();
                return acquired;
            }

            if (type is LockResponseType.MustRetry or LockResponseType.WaitingForReplication && transient < MaxTransientRetries)
            {
                await Task.Delay(transient * 10, cancellationToken).ConfigureAwait(false);
                continue;
            }

            return null;
        }
    }

    /// <summary>
    /// Frees the lock: stops the renewer and unlocks, so the next waiter starts at once instead of after
    /// the lease. Safe to call twice. Best effort: a failed unlock leaves the lease to lapse.
    /// </summary>
    internal async Task ReleaseAsync()
    {
        if (Interlocked.Exchange(ref released, 1) != 0)
            return;

        try
        {
            await renewal.CancelAsync().ConfigureAwait(false);
        }
        catch (ObjectDisposedException)
        {
        }

        // The renewer of the statements can be about to take the lock again, and a lock that it takes after the
        // unlock would stay until its lease lapses. It never throws.
        if (IsStatementHolder)
            await renewer.ConfigureAwait(false);

        try
        {
            await kahuna.LocateAndTryUnlock(resource, owner, LockDurability.Persistent, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger?.LogWarning(ex, "Failed to release the DDL lock {Resource}; its lease lapses in {LeaseMs} ms", resource, LeaseMs);
        }
    }

    /// <summary>Frees the lock; see <see cref="ReleaseAsync"/>. Lets a statement hold it with <c>await using</c>.</summary>
    public ValueTask DisposeAsync() => new(ReleaseAsync());

    private void StartRenewer() => renewer = RenewLoopAsync(renewal.Token);

    /// <summary>
    /// Extends the lease until <see cref="ReleaseAsync"/> cancels it, or until Kahuna says that a DDL
    /// transaction's lock is no longer its own. The lock of the statements is taken again instead: the last
    /// statement of another node frees it for all of them. A failed call is tried again at the next
    /// interval, which the lease outlives. Owned by this object and stopped by its release; it never throws.
    /// </summary>
    private async Task RenewLoopAsync(CancellationToken cancellationToken)
    {
        int intervalMs = IsStatementHolder ? StatementRenewIntervalMs : RenewIntervalMs;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(intervalMs, cancellationToken).ConfigureAwait(false);

                try
                {
                    (LockResponseType type, _) = await kahuna.LocateAndTryExtendLock(
                        resource, owner, LeaseMs, LockDurability.Persistent, cancellationToken).ConfigureAwait(false);

                    // Not cancellable: a release waits for the answer, so it knows whether there is a lock to free.
                    if (IsStatementHolder && type is (LockResponseType.InvalidOwner or LockResponseType.LockDoesNotExist))
                        (type, _) = await kahuna.LocateAndTryLock(
                            resource, owner, LeaseMs, LockDurability.Persistent, CancellationToken.None).ConfigureAwait(false);

                    // Extended is also the answer when a statement of another node took a lost lock again
                    // first: its owner value is the same.
                    if (type is LockResponseType.Extended or LockResponseType.Locked)
                    {
                        Volatile.Write(ref lost, 0);
                        continue;
                    }

                    if (type is not (LockResponseType.InvalidOwner or LockResponseType.LockDoesNotExist or LockResponseType.Busy))
                        continue;

                    if (Interlocked.Exchange(ref lost, 1) == 0)
                        logger?.LogWarning("The DDL lock {Resource} was lost before its holder ended; its commit checks the schema base instead", resource);

                    if (!IsStatementHolder)
                        return;
                }
                catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
                {
                    logger?.LogWarning(ex, "The DDL lock {Resource} could not be renewed; the next attempt is in {IntervalMs} ms", resource, intervalMs);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            renewal.Dispose();
        }
    }
}
