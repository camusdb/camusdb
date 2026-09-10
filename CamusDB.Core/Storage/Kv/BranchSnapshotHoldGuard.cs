
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Fail-closed verifier of a branch database's snapshot protection, consulted by every ancestor
/// read the branch issues.
///
/// <para><b>What it protects against.</b> A branch reads its ancestors frozen at the fork
/// timestamp, and that frozen view survives revision reclamation only while the chain of Kahuna
/// snapshot-floor holds stays registered (the branch's own hold on its immediate parent, plus
/// each non-root ancestor's hold on <em>its</em> parent). Losing that protection is silent:
/// reclamation resumes, and an ancestor read at the fork timestamp then reports reclaimed rows as
/// confirmed absences — a successful, wrong, possibly empty result. This guard turns that silent
/// state into a hard error.</para>
///
/// <para><b>How the proof works.</b> Since Kahuna 1.7.3 a hold's protection ends only when the
/// hold is <em>removed from the registry</em> — an explicit release, or the reaper's purge of an
/// expired hold — never at bare lease expiry: a registered hold constrains reclamation even while
/// its lease is lapsed, and a renew then revives it (this is what makes downtime longer than the
/// lease recoverable, inside the reaper's startup grace window). Renew is refused
/// (<c>DoesNotExist</c>) only for a removed hold, so one successful renew proves the hold's
/// protection was continuous from its acquisition until the renew. The guard renews the whole
/// chain and remembers the confirmation instant on a monotonic clock. A check passes only while
/// the last confirmation is recent enough that the lease granted by it cannot have expired yet
/// (past a lapse, the reaper may purge at any time, so a lapsed guard re-verifies rather than
/// presumes); past that window the guard re-verifies synchronously before the caller may use (or
/// trust the absence of) any ancestor data. By induction every passed check proves the chain's
/// protection never lapsed, so no reclamation past any fork boundary can have occurred.</para>
///
/// <para><b>Three outcomes.</b> A confirmed renew refreshes the window. A definitive refusal
/// latches <see cref="IsLost"/> permanently, best-effort persists a durable lost marker through
/// the injected callback, and throws <see cref="CamusDBErrorCodes.BranchSnapshotProtectionLost"/>
/// — the state cannot heal, because re-acquiring a hold at the old timestamp does not bring
/// reclaimed history back. A transient failure (transport, <c>MustRetry</c>) is tolerated while
/// the previous confirmation still proves the lease live; once it no longer can, reads fail with
/// a retryable <see cref="CamusDBErrorCodes.TransactionMustRetry"/> — unverifiable, not lost
/// (the next reachable renew settles it: revival on a registered hold, refusal on a removed
/// one).</para>
///
/// <para><b>Cost.</b> The fast path is one volatile read and one tick comparison per ancestor
/// access. A renew round-trip happens at most once per refresh window per node (plus bounded
/// retries during an outage), regardless of read volume.</para>
///
/// <para>Thread-safe. One instance per open branch <c>DatabaseDescriptor</c>, shared by every
/// ancestor-level <see cref="KvTableStore"/>/<see cref="KvBranchReader"/> of that branch.</para>
/// </summary>
public sealed class BranchSnapshotHoldGuard
{
    private readonly IKahuna kahuna;
    private readonly ILogger<ICamusDB> logger;
    private readonly string branchName;
    private readonly int leaseMs;

    /// <summary>Confirmations older than this force a synchronous re-verify before the read proceeds.</summary>
    private readonly int refreshThresholdMs;

    /// <summary>
    /// Oldest confirmation that still proves the lease live. Below 1.0 of the lease so the guard
    /// gives up while the hold is still (just) valid rather than after it lapsed — declaring the
    /// window closed a moment early costs a retry, a moment late returns wrong data.
    /// </summary>
    private readonly int hardLimitMs;

    /// <summary>Minimum spacing between verify attempts during an outage, so reads do not hammer Kahuna.</summary>
    private const int AttemptThrottleMs = 250;

    /// <summary>
    /// Resolves the full chain of hold ids this branch's frozen view depends on, ordered
    /// self-first. Resolved lazily (registry lookups can fail transiently at open time) and cached
    /// once resolved — hold ids are immutable after registration, a rename included.
    /// </summary>
    private readonly Func<CancellationToken, Task<IReadOnlyList<string>>> resolveChainHoldIds;

    /// <summary>
    /// Durably records the lost state (registry marker) so a later open of this branch fails fast
    /// with a definitive message instead of re-discovering the loss. Best-effort: correctness never
    /// depends on the marker — a removed hold refuses renewal forever, on every node.
    /// </summary>
    private readonly Func<string, Task>? persistLostAsync;

    private readonly SemaphoreSlim verifyGate = new(1, 1);

    private IReadOnlyList<string>? chainHoldIds;

    // 0 = never confirmed. Monotonic ticks, not HLC: the question is "how long since renewal was
    // last confirmed on this node", a local duration with no cross-node ordering in it.
    private long lastConfirmedTicks;

    private long lastAttemptTicks;

    // Latched reason once protection is definitively lost; null while healthy. Never resets.
    private volatile string? lostReason;

    public BranchSnapshotHoldGuard(
        IKahuna kahuna,
        ILogger<ICamusDB> logger,
        string branchName,
        int leaseMs,
        Func<CancellationToken, Task<IReadOnlyList<string>>> resolveChainHoldIds,
        Func<string, Task>? persistLostAsync = null)
    {
        this.kahuna = kahuna;
        this.logger = logger;
        this.branchName = branchName;
        this.leaseMs = leaseMs;
        this.resolveChainHoldIds = resolveChainHoldIds;
        this.persistLostAsync = persistLostAsync;
        refreshThresholdMs = Math.Max(1, leaseMs / 2);
        hardLimitMs = Math.Max(1, (int)(leaseMs * 0.8));
    }

    /// <summary>True once protection was definitively lost. Latches; never returns to false.</summary>
    public bool IsLost => lostReason is not null;

    /// <summary>
    /// Pre-latches the guard from a durable lost marker found at open time, so the first read fails
    /// immediately with the recorded reason instead of paying a renew round-trip to rediscover it.
    /// </summary>
    public void LatchLostFromDurableMarker(string reason)
    {
        lostReason ??= reason;
    }

    /// <summary>
    /// The check every ancestor access calls, after the underlying Kahuna read and before its
    /// result (a value, a tombstone, or an absence — absence is equally load-bearing) is used.
    /// Returns normally only while the hold chain is proven live "now", which by the induction in
    /// the class summary proves it never lapsed. Throws
    /// <see cref="CamusDBErrorCodes.BranchSnapshotProtectionLost"/> when protection is
    /// definitively gone, or <see cref="CamusDBErrorCodes.TransactionMustRetry"/> when it cannot
    /// currently be verified.
    /// </summary>
    public ValueTask EnsureProtectedAsync(CancellationToken cancellationToken)
    {
        ThrowIfLost();

        long last = Volatile.Read(ref lastConfirmedTicks);
        if (last != 0 && Environment.TickCount64 - last < refreshThresholdMs)
            return ValueTask.CompletedTask;

        return new ValueTask(VerifySlowAsync(cancellationToken));
    }

    private async Task VerifySlowAsync(CancellationToken cancellationToken)
    {
        await verifyGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfLost();

            long now = Environment.TickCount64;
            long last = Volatile.Read(ref lastConfirmedTicks);

            // Another caller confirmed while this one waited on the gate.
            if (last != 0 && now - last < refreshThresholdMs)
                return;

            // Throttle repeat attempts while the previous confirmation still proves the lease
            // live; without a live proof every read must attempt, so the throttle does not apply.
            if (last != 0 && now - last < hardLimitMs
                && lastAttemptTicks != 0 && now - lastAttemptTicks < AttemptThrottleMs)
                return;

            lastAttemptTicks = now;

            IReadOnlyList<string> chain;
            try
            {
                chain = chainHoldIds ??= await resolveChainHoldIds(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                HandleTransientFailure(last, now, ex.Message);
                return;
            }

            foreach (string holdId in chain)
            {
                KeyValueResponseType type;
                try
                {
                    (type, _) = await kahuna
                        .LocateAndRenewSnapshotHold(holdId, leaseMs, cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    HandleTransientFailure(last, now, ex.Message);
                    return;
                }

                if (type == KeyValueResponseType.Set)
                    continue;

                if (type == KeyValueResponseType.DoesNotExist)
                {
                    // Definitive: the hold was removed from the registry (released, or purged by
                    // the reaper after its lease lapsed). Removal is replicated and permanent, so
                    // this answer is the same on every node, forever.
                    await MarkLostAsync(holdId).ConfigureAwait(false);
                    ThrowIfLost();
                    return; // unreachable — ThrowIfLost always throws here
                }

                // MustRetry / anything else: the answer is not authoritative either way.
                HandleTransientFailure(last, now, $"renew of hold {holdId} returned {type}");
                return;
            }

            Volatile.Write(ref lastConfirmedTicks, Environment.TickCount64);
        }
        finally
        {
            verifyGate.Release();
        }
    }

    /// <summary>
    /// A verify attempt failed without a definitive answer. While the previous confirmation still
    /// proves the lease live the read may proceed on that proof; past the hard limit nothing can
    /// prove the chain intact, so the read is refused with a retryable error — deliberately not the
    /// permanent lost state, because the hold may in fact still be alive (the leader sweep renews it
    /// independently) and the next reachable renew settles the question authoritatively.
    /// </summary>
    private void HandleTransientFailure(long lastConfirmed, long now, string detail)
    {
        if (lastConfirmed != 0 && now - lastConfirmed < hardLimitMs)
        {
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug(
                    "Transient failure verifying snapshot protection of branch '{Branch}' ({Detail}); proceeding on the previous confirmation",
                    branchName, detail);
            return;
        }

        throw new CamusDBException(
            CamusDBErrorCodes.TransactionMustRetry,
            $"Could not verify that branch '{branchName}' still holds its ancestors' history pinned " +
            $"({detail}), and the last confirmation is too old to rely on. The read was refused rather " +
            "than risk an incomplete result; retry the operation");
    }

    private async Task MarkLostAsync(string holdId)
    {
        string reason =
            $"Snapshot hold {holdId} protecting the frozen ancestor view of branch '{branchName}' no " +
            "longer exists (it was released, or the reaper purged it after its lease lapsed), so ancestor " +
            "history at the fork point may already be reclaimed";

        // Latch before persisting so a persist failure still fails the branch closed locally.
        lostReason ??= reason;

        logger.LogError(
            "Branch '{Branch}' lost snapshot protection: hold {HoldId} refused renewal (DoesNotExist). " +
            "All reads and writes that consult the branch's ancestry will now fail closed",
            branchName, holdId);

        if (persistLostAsync is null)
            return;

        try
        {
            await persistLostAsync(reason).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Failed to persist the lost-protection marker for branch '{Branch}'; reads still fail " +
                "closed (a removed hold refuses renewal on every node), but a later open will rediscover " +
                "the loss instead of failing fast",
                branchName);
        }
    }

    private void ThrowIfLost()
    {
        string? reason = lostReason;
        if (reason is null)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.BranchSnapshotProtectionLost,
            $"{reason}. Reads and writes on this branch are refused because inherited rows can no " +
            "longer be returned completely; recreate the branch from its parent to get a fresh, " +
            "protected fork point");
    }
}
