/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
using Microsoft.Extensions.Logging;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Statistics;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Why an eviction attempt did not proceed. Returned rather than logged-and-forgotten so a caller
/// (and a test) can tell "nothing to do" from "something is still using it".
/// </summary>
internal enum DatabaseEvictionOutcome
{
    /// <summary>The descriptor was released and its per-database state freed.</summary>
    Evicted,

    /// <summary>No descriptor is open under that id.</summary>
    NotOpen,

    /// <summary>It has not been idle long enough.</summary>
    NotIdle,

    /// <summary>A caller holds a use-reference, a transaction is active, or DDL is in flight.</summary>
    InUse,

    /// <summary>An open branch database reads through this one as an ancestor.</summary>
    BranchAncestor,

    /// <summary>A drop owns the descriptor's lifetime; eviction must not interfere.</summary>
    Dropping,
}

/// <summary>
/// Releases database descriptors that nobody is using, so the set this node holds open tracks its
/// working set rather than every database it has ever touched.
///
/// <para><b>Why this is not just <see cref="DatabaseCloser"/>.</b> Closing is something a user asked
/// for: it removes the descriptor and disposes it, and any caller mid-operation is that user's
/// problem. Eviction is a background optimization nobody asked for, so it must be invisible — it may
/// never take a descriptor out from under a running statement, and when in doubt it does nothing and
/// tries again later. That difference is the whole class: everything below exists to make the answer
/// "refuse" in every case where "dispose" is not provably safe.</para>
///
/// <para><b>Why an idle window is a safety property and not just a policy.</b> The one interleaving
/// that could hurt is a caller that has resolved a descriptor but has not yet taken a use-reference —
/// for that instant the reference count is still zero and disposal would look safe. Resolution stamps
/// the descriptor before returning it (<see cref="DatabaseOpener.Open"/> calls
/// <see cref="DatabaseDescriptor.Touch"/>), and the stamp-to-reference gap contains no awaits, so a
/// descriptor with a caller inside that gap reports an idle time of approximately zero. Requiring
/// minutes of idleness therefore excludes the interleaving rather than merely making it unlikely.</para>
///
/// <para><b>Failure direction.</b> Every check is re-run after the descriptor is removed from the open
/// set, and a descriptor that has become busy in between is <em>not</em> disposed — it is put back.
/// A missed eviction costs memory until the next sweep; a wrong one costs a live statement an
/// <c>ObjectDisposedException</c>. The code is written to fail the first way.</para>
/// </summary>
internal sealed class DatabaseEvictor : IAsyncDisposable
{
    private readonly DatabaseDescriptors databaseDescriptors;

    private readonly StatisticsManager statistics;

    private readonly ILogger<ICamusDB> logger;

    private readonly int idleWindowMs;

    private readonly CancellationTokenSource cts = new();

    private Task? loop;

    public DatabaseEvictor(
        DatabaseDescriptors databaseDescriptors,
        StatisticsManager statistics,
        ILogger<ICamusDB> logger,
        int idleWindowMs)
    {
        this.databaseDescriptors = databaseDescriptors;
        this.statistics = statistics;
        this.logger = logger;
        this.idleWindowMs = idleWindowMs;
    }

    /// <summary>
    /// Number of descriptors this evictor has released, for diagnostics and tests.
    /// </summary>
    internal int EvictedCount;

    /// <summary>
    /// How often the sweep runs: often enough that a database is released reasonably soon after it
    /// goes idle, never more often than once a minute regardless of how long the window is.
    ///
    /// <para>Derived from the window rather than fixed so that a short window — which is what a test
    /// configures — is actually exercised by the loop instead of waiting out a production-sized tick.
    /// The consequence in production is that a database is released somewhere between one and two
    /// windows after its last use, which is the intended looseness: the window is a floor on idleness,
    /// not a deadline for reclamation.</para>
    /// </summary>
    private int TickMs => (int)Math.Clamp(idleWindowMs / 2L, 50, 60_000);

    /// <summary>
    /// Starts the periodic sweep. A no-op when idle eviction is disabled, so a deployment that wants
    /// databases to stay open once opened simply never has the loop.
    /// </summary>
    public void Start()
    {
        if (idleWindowMs <= 0)
            return;

        // Serialize concurrent Start calls: an unsynchronized `loop ??=` is check-then-act, and two
        // racing callers would each launch an independent loop for the process lifetime.
        lock (this)
        {
            loop ??= EvictionLoopAsync(cts.Token);
        }
    }

    private async Task EvictionLoopAsync(CancellationToken ct)
    {
        // try/catch INSIDE the loop: one failed sweep is logged and retried on the next tick, and
        // only cancellation ends the loop. Eviction is an optimization — a sweep that throws must
        // never take the loop down with it and leave the node growing unbounded thereafter.
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TickMs, ct).ConfigureAwait(false);
                EvictIdle(idleWindowMs);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break; // normal shutdown
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Idle database eviction sweep failed; retrying next tick");
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await cts.CancelAsync().ConfigureAwait(false);

        if (loop is not null)
        {
            try { await loop.ConfigureAwait(false); }
            catch { /* the loop swallows its own errors */ }
        }

        cts.Dispose();
    }

    /// <summary>
    /// Releases every descriptor that has been idle for at least <paramref name="idleWindowMs"/> and
    /// is provably unused, leaving the rest alone. Returns how many were released.
    /// </summary>
    public int EvictIdle(long idleWindowMs)
    {
        int evicted = 0;

        // Snapshot the ids first: evicting mutates the dictionary, and a database that becomes busy
        // mid-sweep is simply refused when its turn comes.
        foreach (string id in databaseDescriptors.Descriptors.Keys.ToList())
        {
            if (TryEvict(id, idleWindowMs) == DatabaseEvictionOutcome.Evicted)
                evicted++;
        }

        return evicted;
    }

    /// <summary>
    /// Releases one descriptor if — and only if — it is idle and provably unused. Never blocks on a
    /// busy database and never forces anything: a refusal is a normal outcome that the next sweep
    /// retries.
    /// </summary>
    public DatabaseEvictionOutcome TryEvict(string id, long idleWindowMs)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);

        if (!databaseDescriptors.Descriptors.TryGetValue(id, out AsyncLazy<DatabaseDescriptor>? lazy))
            return DatabaseEvictionOutcome.NotOpen;

        // A descriptor still being loaded has no state worth reclaiming and no safe answer to the
        // questions below; leave it for the next sweep rather than awaiting the load here, which
        // would make a sweep block on someone else's open.
        if (!lazy.IsStarted || !lazy.Task.IsCompletedSuccessfully)
            return DatabaseEvictionOutcome.InUse;

        DatabaseDescriptor descriptor = lazy.Task.Result;

        DatabaseEvictionOutcome blocked = CheckEvictable(descriptor, idleWindowMs);
        if (blocked != DatabaseEvictionOutcome.Evicted)
            return blocked;

        // Commit point. After this no new resolve can reach this descriptor; anything already holding
        // it either shows up in the re-check below or arrived before the idle window and cannot exist.
        if (!databaseDescriptors.Descriptors.TryRemove(
                new KeyValuePair<string, AsyncLazy<DatabaseDescriptor>>(id, lazy)))
            return DatabaseEvictionOutcome.NotOpen; // a drop or a close won the race and owns it now

        DatabaseEvictionOutcome recheck = CheckEvictable(descriptor, idleWindowMs);
        if (recheck != DatabaseEvictionOutcome.Evicted)
        {
            // Someone reached it between the two checks. Put it back rather than disposing it, and
            // put back the same instance so nobody ends up holding a descriptor the open set has
            // replaced — two live descriptors for one database would each carry their own in-memory
            // schema.
            AsyncLazy<DatabaseDescriptor> current = databaseDescriptors.Descriptors.GetOrAdd(id, lazy);

            if (!ReferenceEquals(current, lazy))
            {
                // A replacement was constructed inside that window. Nothing is disposed — the caller
                // holding this instance keeps working and it is collected when they are done — but it
                // is worth saying out loud, because it means two descriptors briefly existed.
                logger.LogWarning(
                    "Database '{Db}' was reopened while an idle eviction was being abandoned; the superseded descriptor was left intact",
                    descriptor.Name);
            }

            return recheck;
        }

        Release(descriptor);

        Interlocked.Increment(ref EvictedCount);

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation(
                "Released idle database '{Db}' after {Idle} ms without use", descriptor.Name, descriptor.IdleMilliseconds);

        return DatabaseEvictionOutcome.Evicted;
    }

    /// <summary>
    /// Answers whether a descriptor may be released right now, returning
    /// <see cref="DatabaseEvictionOutcome.Evicted"/> when every condition holds. Pure and cheap on
    /// purpose: it is run twice per attempt, once before the descriptor leaves the open set and once
    /// after, and the second run is what catches a caller that arrived in between.
    /// </summary>
    private DatabaseEvictionOutcome CheckEvictable(DatabaseDescriptor descriptor, long idleWindowMs)
    {
        // A drop is already driving this descriptor's lifetime through its own quiesce-and-drain
        // protocol. Two owners disposing one object is how a drop ends up completing against a
        // descriptor that has already been torn down.
        if (descriptor.IsDropped)
            return DatabaseEvictionOutcome.Dropping;

        if (descriptor.HasLiveUses)
            return DatabaseEvictionOutcome.InUse;

        if (descriptor.IdleMilliseconds < idleWindowMs)
            return DatabaseEvictionOutcome.NotIdle;

        // An open transaction outlives the statement that started it, so the reference count above is
        // not enough on its own: between two statements of one transaction nobody holds a reference,
        // yet disposing the manager would strand the transaction's locks and heartbeats. The same
        // applies to a transaction whose commit or rollback came back unresolved — it holds no
        // reference while the client is away, and it is exactly what the client will return to finish.
        if (descriptor.Transactions.HasUnfinishedTransactions)
            return DatabaseEvictionOutcome.InUse;

        // DDL in flight. Its proposer released the schema lock while waiting for the replicated apply,
        // so this is the signal that says "a schema change is mid-sequence"; disposing here would tear
        // down the very subscription that is supposed to receive it.
        if (descriptor.SchemaDdlSemaphore.CurrentCount == 0)
            return DatabaseEvictionOutcome.InUse;

        if (IsAncestorOfAnOpenBranch(descriptor.Id))
            return DatabaseEvictionOutcome.BranchAncestor;

        return DatabaseEvictionOutcome.Evicted;
    }

    /// <summary>
    /// True when some other open database was branched from this one. A branch reads its ancestors'
    /// keyspaces through the fork timestamps captured in its own ancestry, so an ancestor that is open
    /// is part of a live read path even though nothing holds a reference to its descriptor. Treating it
    /// as idle would be reasoning only about who called it, not about what depends on it.
    /// </summary>
    private bool IsAncestorOfAnOpenBranch(string id)
    {
        foreach (AsyncLazy<DatabaseDescriptor> lazy in databaseDescriptors.Descriptors.Values)
        {
            if (!lazy.IsStarted || !lazy.Task.IsCompletedSuccessfully)
                continue;

            DatabaseDescriptor other = lazy.Task.Result;
            if (string.Equals(other.Id, id, StringComparison.Ordinal))
                continue;

            foreach (DatabaseBranchAncestor ancestor in other.Ancestors)
            {
                if (string.Equals(ancestor.DatabaseId, id, StringComparison.Ordinal))
                    return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Frees everything the descriptor kept alive, including the per-database state that lives outside
    /// it. The descriptor's own <see cref="DatabaseDescriptor.Dispose"/> releases its schema-apply
    /// subscription, transactions manager, schema and semaphores; the statistics cache and the
    /// schema-ack tracker are process-wide maps keyed by database, so they have to be told separately
    /// or the memory this exists to reclaim stays held by them instead.
    /// </summary>
    private void Release(DatabaseDescriptor descriptor)
    {
        try
        {
            descriptor.Kahuna.ForgetSchemaAcks(descriptor.Id);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Could not release schema-ack state for database '{Db}'", descriptor.Name);
        }

        try
        {
            statistics.EvictDatabaseStats(descriptor.Id);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Could not release cached statistics for database '{Db}'", descriptor.Name);
        }

        descriptor.Dispose();
    }
}
