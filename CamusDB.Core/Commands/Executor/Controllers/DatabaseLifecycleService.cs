
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Maintenance;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using Kahuna;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// The whole-database operations: create, branch, open, close, drop, relink and rename. These are
/// the statements whose unit of work is a database rather than anything inside one, so they act on
/// the cross-database <see cref="DatabaseRegistry"/> and must be correct against each other rather
/// than against DML.
///
/// <para><b>The hard part here is the races between these operations, not any one of them.</b>
/// Drop versus branch-create is fenced from both sides — a per-id drop-intent marker plus an
/// authoritative source-liveness re-read the branch-create runs after publishing (the marker alone
/// only covers a drop still in flight; the liveness read catches a drop that already finished and
/// released it), plus a live-descendant scan the drop re-runs under the
/// source's <c>SchemaDdlSemaphore</c> — because Raft linearizability then guarantees exactly one of
/// the two wins with no orphaned child and no purged ancestor. Relink versus the orphan GC takes
/// that same per-id fence. Every ordering in this class ("metadata first, registry last",
/// "orphan record before unregister", "unregister before purge") is chosen so that a crash at any
/// point leaves recoverable state rather than stranded data, and each is called out where it
/// happens.</para>
/// </summary>
internal sealed class DatabaseLifecycleService
{
    private readonly ExecutorContext context;

    /// <summary>Configuration for this engine; injected, never ambient. See <see cref="ApplyOptions"/>.</summary>
    private CamusDBOptions options;

    private readonly CatalogsManager catalogs;

    private readonly DatabaseCreator databaseCreator;

    private readonly DatabaseCloser databaseCloser;

    private readonly DatabaseDropper databaseDropper;

    private readonly DatabaseDescriptors databaseDescriptors;

    private readonly StartupRecoveryService startupRecovery;

    internal DatabaseLifecycleService(
        ExecutorContext context,
        CamusDBOptions options,
        CatalogsManager catalogs,
        DatabaseCreator databaseCreator,
        DatabaseCloser databaseCloser,
        DatabaseDropper databaseDropper,
        DatabaseDescriptors databaseDescriptors,
        StartupRecoveryService startupRecovery
    )
    {
        this.context = context;
        this.options = options;
        this.catalogs = catalogs;
        this.databaseCreator = databaseCreator;
        this.databaseCloser = databaseCloser;
        this.databaseDropper = databaseDropper;
        this.databaseDescriptors = databaseDescriptors;
        this.startupRecovery = startupRecovery;
    }

    /// <summary>
    /// Swaps in a newly published configuration snapshot. Reference assignment is atomic and the
    /// record itself stays immutable; each operation pins the field once, so an in-flight statement
    /// keeps the snapshot it started with.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next) => options = next;

    public async Task<DatabaseDescriptor> CreateDatabase(CreateDatabaseTicket ticket)
    {
        context.Validator.Validate(ticket);

        DatabaseRegistry registry = await context.Registry.ConfigureAwait(false);
        string name = ticket.DatabaseName;

        // Resolve the target through the persistent registry (cache-first, live-KV fallback), not the
        // local cache alone. In a cluster the name may be registered on another node but absent from
        // this node's cache; a cache-only check would let CREATE IF NOT EXISTS run the whole create/
        // branch flow (allocating an id, acquiring a snapshot hold, copying metadata) only to fail late
        // at RegisterAsync, instead of returning the existing database here.
        DatabaseRegistryEntry? existing = await registry.TryResolveEntryAsync(name).ConfigureAwait(false);
        if (existing is not null)
        {
            if (ticket.IfNotExists)
                return await context.DatabaseOpener.Open(name).ConfigureAwait(false);

            throw new CamusDBException(
                CamusDBErrorCodes.DatabaseAlreadyExists,
                $"Database '{name}' already exists");
        }

        if (ticket.BranchFrom is not null)
            return await CreateBranchDatabaseAsync(ticket, registry, name).ConfigureAwait(false);

        string id = await registry.AllocateIdAsync().ConfigureAwait(false);

        await registry.RegisterAsync(name, id).ConfigureAwait(false);

        try
        {
            await databaseCreator.Create(name, id).ConfigureAwait(false);
            return await context.DatabaseOpener.Open(name).ConfigureAwait(false);
        }
        catch (Exception openEx)
        {
            // Roll back the registry entry so the name can be retried. Identity-checked: only this
            // create's entry may be removed — a replacement registered concurrently under the same
            // name must never be deleted by this compensation.
            try
            {
                await registry.RetractRegistrationAsync(name, id).ConfigureAwait(false);
            }
            catch (Exception unregEx)
            {
                context.Logger.LogError(unregEx,
                    "Failed to unregister database '{Name}' (id={Id}) after failed create — " +
                    "the name is now wedged: it appears registered but cannot be opened",
                    name, id);
            }

            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(openEx).Throw();
            throw; // unreachable
        }
    }

    /// <summary>
    /// Creates a branch database from <paramref name="ticket"/>.<see cref="CreateDatabaseTicket.BranchFrom"/>.
    /// The source must be schema-stable — <c>HeadSchemaVersion == SchemaVersion</c>, all columns and
    /// indexes in the Public state, and no persisted coordinator jobs — before the branch is taken.
    /// A materialized-view refresh in flight on the source does <em>not</em> block the fork: its job
    /// record and staging relation are the source's own work, and the copy leaves them out so the
    /// branch opens with the view's published contents and no rebuild to inherit (see
    /// <c>BranchMetaCopier</c>). Schema metadata is copied O(schema-size) to the branch namespace
    /// under the source's <c>SchemaDdlSemaphore</c> to keep the copy point consistent.  The registry entry is published
    /// last ("metadata first, registry last") so an orphaned namespace from a crash between the two
    /// steps is cleaned up by the startup orphan-namespace scrubber rather than being served to clients.
    /// </summary>
    private async Task<DatabaseDescriptor> CreateBranchDatabaseAsync(
        CreateDatabaseTicket ticket, DatabaseRegistry registry, string branchName)
    {
        CamusDBOptions currentOptions = options;

        DatabaseDescriptor sourceDescriptor = await context.DatabaseOpener.Open(ticket.BranchFrom!).ConfigureAwait(false);

        await sourceDescriptor.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            // Re-validate the source is still registered under the semaphore. DropDatabase acquires
            // the same semaphore before unregistering, so if Drop won the race it will have already
            // removed the source from the registry by the time we get here.
            if (registry.Get(ticket.BranchFrom!) is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.DatabaseDoesntExist,
                    $"Source database '{ticket.BranchFrom}' was dropped concurrently; branch creation aborted");

            // Schema stability: no committed schema entry may be un-applied.
            if (sourceDescriptor.HeadSchemaVersion != sourceDescriptor.Schema.SchemaVersion)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Database '{ticket.BranchFrom}' has in-flight schema changes; wait for them to complete before branching");

            // Schema stability: every column and index must be in the Public (terminal) state.
            // Non-Public elements indicate an online schema change is mid-sequence and the
            // schema metadata is not yet a consistent snapshot.
            foreach (TableSchema table in sourceDescriptor.Schema.Tables.Values)
            {
                if (table.Columns is not null)
                    foreach (TableColumnSchema col in table.Columns)
                        if (col.State != SchemaElementState.Public)
                            throw new CamusDBException(
                                CamusDBErrorCodes.InvalidInput,
                                $"Column '{col.Name}' in table '{table.Name}' of database '{ticket.BranchFrom}' is in state '{col.State}'; wait for the schema change to reach Public before branching");

                if (table.Indexes is not null)
                    foreach (TableIndexSchema idx in table.Indexes)
                        if (idx.State != SchemaElementState.Public)
                            throw new CamusDBException(
                                CamusDBErrorCodes.InvalidInput,
                                $"Index '{idx.Name}' in table '{table.Name}' of database '{ticket.BranchFrom}' is in state '{idx.State}'; wait for the schema change to reach Public before branching");
            }

            // Schema stability: no coordinator jobs may be in-flight.  A coordinator job
            // can resume after a leader change while element states momentarily read Public;
            // checking persisted jobs under SchemaDdlSemaphore fences that window.
            List<PersistedCoordinatorJob> coordinatorJobs =
                await catalogs.LoadCoordinatorJobsAsync(sourceDescriptor).ConfigureAwait(false);
            if (coordinatorJobs.Count > 0)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Database '{ticket.BranchFrom}' has {coordinatorJobs.Count} pending coordinator job(s); wait for them to complete before branching");

            // Mint a fork timestamp by going through Kahuna's transaction pipeline rather than
            // reading the Raft node's local HLC directly. The LocateAndStartTransaction call
            // routes through the partition actor's message queue, which serializes with any
            // in-progress partition-actor HLC advances (e.g. from an ongoing TrySet's ReceiveEvent).
            // This is the causal fence that ensures forkT is strictly after every write that was
            // already committed before branch creation, even in a multi-partition deployment where
            // the Raft node's local HLC might lag a partition actor's HLC. The transaction is
            // rolled back immediately — we only needed the causal timestamp.
            KvTransaction forkTx = await sourceDescriptor.Transactions.BeginAsync().ConfigureAwait(false);
            HLCTimestamp forkT = forkTx.TransactionId;
            await sourceDescriptor.Transactions.RollbackIfNotCompletedAsync(forkTx).ConfigureAwait(false);

            string branchId = await registry.AllocateIdAsync().ConfigureAwait(false);

            // Pin the immediate parent's MVCC history at forkT with a Kahuna snapshot-floor hold so
            // the branch's as-of-forkT reads stay correct even under aggressive revision reclamation.
            // The holder id is the branch's own stable id (acquire is idempotent by (holder, forkT)).
            // A hold that is not confirmed granted means durability cannot be guaranteed — fail the
            // create rather than register a branch whose frozen view GC may later reclaim. The hold
            // is renewed while the branch lives (leader-owned renewer) and released on leaf drop.
            IKahuna sourceKahuna = sourceDescriptor.Kahuna.Kahuna;
            (KeyValueResponseType holdType, string holdId, _) = await sourceKahuna
                .LocateAndAcquireSnapshotHold(branchId, forkT, currentOptions.BranchSnapshotHoldLeaseMs, CancellationToken.None)
                .ConfigureAwait(false);

            if (holdType != KeyValueResponseType.Set || string.IsNullOrEmpty(holdId))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Could not acquire a snapshot-floor hold on '{ticket.BranchFrom}' at the fork point (status {holdType}); branch not created because its frozen view could not be guaranteed durable");

            // The metadata copy below has no duration bound, and the registry-driven renewer only
            // sweeps holds of REGISTERED branches — during creation nothing else renews this hold.
            // A copy slower than the lease would publish a branch whose protection had already
            // lapsed. This keep-alive renews the hold privately for the whole create; ThrowIfLost
            // before RegisterAsync and again before success are the validation gates that make
            // "published" imply "protected". It never releases the hold — release on abort stays
            // with the catch block below, and on success ownership passes to the sweep renewer.
            SnapshotHoldLease creationKeepAlive = SnapshotHoldLease.AdoptForKeepAlive(
                sourceKahuna, context.Logger, holdId, forkT, currentOptions.BranchSnapshotHoldLeaseMs);

            bool metaCopied = false;
            bool registerAttempted = false;
            bool childRegistered = false;
            bool leaveMarkerForScrubber = false;
            try
            {
                // Write the pending-create marker FIRST, inside this try block so the catch below
                // releases the snapshot hold if the write fails. The marker must be confirmed
                // durable before CopyMetaForBranchAsync runs: if the process crashes after the
                // metadata copy but before RegisterAsync commits, the startup scrubber finds this
                // id unregistered and purges the orphaned namespace. If the marker write itself
                // fails, we propagate the error and CopyMetaForBranchAsync is never called —
                // so no orphan can exist without a recovery handle.
                await registry.TrackPendingBranchAsync(branchId).ConfigureAwait(false);

                // Ancestry chain: immediate parent first, then its ancestors (nearest-parent ordering).
                DatabaseRegistryEntry? sourceEntry = await registry.TryResolveEntryAsync(ticket.BranchFrom!).ConfigureAwait(false);
                List<DatabaseBranchAncestor> ancestors =
                [
                    new DatabaseBranchAncestor { DatabaseId = sourceDescriptor.Id, ForkTimestamp = forkT },
                    .. (sourceEntry?.Ancestors ?? [])
                ];

                // Copy schema metadata into the branch namespace before publishing the registry entry.
                // Pass forkT so the scan reads source metadata as-of the fork point: any schema
                // change committed after forkT on another cluster node is not included, keeping the
                // branch schema consistent with the row/index MVCC snapshot it inherits.
                // The pending marker above ensures that if the process crashes here, the orphaned
                // namespace is found by the startup scrubber and purged on next restart.
                await catalogs.CopyMetaForBranchAsync(sourceDescriptor, branchId, forkT).ConfigureAwait(false);
                metaCopied = true;

                // Publication gate: the hold must still be alive after the copy. Publishing past a
                // lapsed hold would register a branch whose inherited rows may already be reclaimed.
                creationKeepAlive.ThrowIfLost($"the creation of branch '{branchName}'");

                registerAttempted = true;
                await registry.RegisterAsync(branchName, branchId, ancestors, holdId).ConfigureAwait(false);
                childRegistered = true;

                // Cross-node drop-vs-branch-create fence: check whether DropDatabase set a drop-intent
                // marker on the source after we passed the source-still-registered check above but
                // before RegisterAsync committed.
                //
                // Raft linearizability guarantees: if drop's intent write committed before our
                // RegisterAsync, we observe it here and abort; if our RegisterAsync committed first,
                // drop's subsequent HasLiveDescendantsAsync scan sees our child and drop aborts
                // instead. Neither of those two orderings leaves an orphaned child. A third
                // ordering escapes this check alone; the liveness re-read below covers it.
                if (await registry.HasDropIntentAsync(sourceDescriptor.Id).ConfigureAwait(false))
                    throw new CamusDBException(
                        CamusDBErrorCodes.DatabaseDoesntExist,
                        $"Source database '{ticket.BranchFrom}' is being dropped concurrently; branch creation aborted");

                // The intent marker only fences a drop that is still in flight: DropDatabase releases
                // it after its purge completes, so a create that stalled between the metadata copy and
                // RegisterAsync can pass the intent check above even though the source is already fully
                // dropped. Close that half of the race by re-reading the source's liveness from
                // persistent KV by its immutable id — never the local cache, which cannot have observed
                // an unregister performed on another node. The ordering of the two reads makes the pair
                // complete: a drop whose descendant scan missed this child holds the intent from before
                // our RegisterAsync until after its unregister, so either the intent read above still
                // sees the marker, or this liveness read runs after the unregister and finds the id
                // gone. Database ids are never reused, so a recreate of the source's name under a new
                // id does not satisfy this check, while a concurrent rename (which preserves the id)
                // correctly does.
                if (await registry.TryResolveNameByIdAsync(sourceDescriptor.Id).ConfigureAwait(false) is null)
                    throw new CamusDBException(
                        CamusDBErrorCodes.DatabaseDoesntExist,
                        $"Source database '{ticket.BranchFrom}' was dropped concurrently; branch creation aborted");

                // Final validation before declaring success: the branch is published, so a lost
                // hold here throws into the catch below, which retracts the registration and
                // releases what is left rather than handing the caller an unprotected branch.
                creationKeepAlive.ThrowIfLost($"the creation of branch '{branchName}'");
            }
            catch (Exception abortEx)
            {
                // Stop the private renew loop before the cleanup below releases the hold, so the
                // loop does not race the release into a misleading "refused renewal" warning.
                await creationKeepAlive.DisposeAsync().ConfigureAwait(false);

                // The cleanup below (hold release + metadata purge + marker clear) is destructive,
                // and it is only safe once this create's registration is PROVED not to be live.
                // "childRegistered is false" is not that proof: RegisterAsync can fail after its
                // commit landed, or with an unknown commit outcome, and destroying the hold and
                // metadata of a durably registered branch leaves the registry pointing at a purged
                // namespace with no snapshot floor and no recovery handle. Establish the
                // authoritative publication state first, retracting the entry (identity-checked, so
                // a replacement registered by another create is never removed) when it is ours.
                bool unpublicationProved;
                if (!registerAttempted)
                {
                    // Registration was never attempted, so no registry entry with this id can exist.
                    unpublicationProved = true;
                }
                else
                {
                    // An unresolved registration commit can still land AFTER an absence check reads
                    // nothing, so absence is proof only when the registration failure had a resolved
                    // outcome (see the RegisterAsync failure contract).
                    bool commitOutcomeUnresolved = !childRegistered
                        && abortEx is CamusDBException { Code: CamusDBErrorCodes.TransactionFinalizeUnresolved };
                    try
                    {
                        RegistryRetraction retraction = await registry
                            .RetractRegistrationAsync(branchName, branchId).ConfigureAwait(false);

                        unpublicationProved = retraction switch
                        {
                            RegistryRetraction.Retracted => true,
                            RegistryRetraction.OwnedByOther => true,
                            _ => !commitOutcomeUnresolved,
                        };
                    }
                    catch (Exception retractEx)
                    {
                        unpublicationProved = false;
                        context.Logger.LogError(retractEx,
                            "Could not establish or retract the registration of branch '{Branch}' (id={BranchId}) after an aborted create",
                            branchName, branchId);
                    }
                }

                if (!unpublicationProved)
                {
                    // Indeterminate create: the branch may be — or may still become — durably
                    // registered. Retain the snapshot hold, the copied metadata, AND the
                    // pending-create marker so a startup scrubber / reconciliation can later finish
                    // either unpublication + cleanup or completion of the branch. Do NOT release the
                    // hold or purge the metadata here. Surface a retryable indeterminate error.
                    leaveMarkerForScrubber = true;
                    context.Logger.LogError(abortEx,
                        "Branch '{Branch}' (id={BranchId}) may remain registered after an aborted create; retaining snapshot hold, metadata, and pending-create marker for recovery",
                        branchName, branchId);
                    throw new CamusDBException(
                        CamusDBErrorCodes.TransactionMustRetry,
                        $"Branch '{branchName}' creation is in an indeterminate state (its registration could not be confirmed retracted after aborting); retry after reconciliation");
                }

                // Confirmed unpublished (never published, or now retracted): nothing will ever renew or
                // release this hold, so release it best-effort.
                try
                {
                    await sourceKahuna.LocateAndReleaseSnapshotHold(holdId, CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception releaseEx)
                {
                    context.Logger.LogWarning(releaseEx, "Failed to release snapshot hold {HoldId} after aborted branch creation of '{Branch}'", holdId, branchName);
                }

                // If metadata was already written before the abort, purge it inline so the orphaned
                // namespace does not linger. The marker is cleared (in the finally below) only when
                // the purge verifiably drained the namespace: an incomplete purge — a delete whose
                // outcome is not a terminal success, a failed scan, or an exception — keeps the
                // pending marker, because it is the startup scrubber's only handle on these keys.
                // The purge runs through the registry's client for the same reason the scrubber
                // does (see DatabaseRegistry.Kahuna); in production it is the shared node.
                if (metaCopied)
                {
                    bool purged;
                    try
                    {
                        purged = await startupRecovery.PurgeBranchMetaNamespaceAsync(branchId, registry.Kahuna).ConfigureAwait(false);
                    }
                    catch (Exception purgeEx)
                    {
                        context.Logger.LogWarning(purgeEx,
                            "Failed to inline-purge orphaned branch metadata for id {BranchId}; namespace will be reclaimed by startup scrubber",
                            branchId);
                        leaveMarkerForScrubber = true;
                        throw;
                    }

                    if (!purged)
                    {
                        leaveMarkerForScrubber = true;
                        context.Logger.LogWarning(
                            "Inline purge of orphaned branch metadata for id {BranchId} is incomplete; keeping the pending marker so the startup scrubber can resume it",
                            branchId);
                    }
                }
                throw;
            }
            finally
            {
                // Stop the private renew loop on every path (idempotent; on success ownership of
                // the still-live hold passes to the registry-driven sweep renewer).
                await creationKeepAlive.DisposeAsync().ConfigureAwait(false);

                // Remove the pending marker whether creation succeeded or was cleanly aborted.
                // EXCEPTION: if the inline purge did not verifiably complete, the namespace may
                // still be present and the marker is the scrubber's only handle — keep it so
                // startup can reclaim it.
                if (!leaveMarkerForScrubber)
                    await registry.ClearPendingBranchAsync(branchId).ConfigureAwait(false);
            }
        }
        finally
        {
            sourceDescriptor.SchemaDdlSemaphore.Release();
        }

        return await context.DatabaseOpener.Open(branchName).ConfigureAwait(false);
    }

    public async Task<DatabaseDescriptor> OpenDatabase(string database, bool recoveryMode = false)
    {
        return await context.DatabaseOpener.Open(database, recoveryMode).ConfigureAwait(false);
    }

    public async Task CloseDatabase(CloseDatabaseTicket ticket)
    {
        context.Validator.Validate(ticket);

        // Flush tail stats before the descriptor is torn down so debounced deltas survive shutdown.
        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();
        await context.Statistics.FlushAllAsync(database).ConfigureAwait(false);

        await databaseCloser.Close(database.Id).ConfigureAwait(false);
    }

    public async Task DropDatabase(DropDatabaseTicket ticket)
    {
        context.Validator.Validate(ticket);

        DatabaseRegistry registry = await context.Registry.ConfigureAwait(false);
        DatabaseRegistryEntry? entry = registry.Get(ticket.DatabaseName);
        if (entry is null)
        {
            if (ticket.IfExists)
                return;
            throw new CamusDBException(
                CamusDBErrorCodes.DatabaseDoesntExist,
                $"Database '{ticket.DatabaseName}' does not exist");
        }

        // Acquire the target's SchemaDdlSemaphore before the descendant check so that a concurrent
        // CreateBranchDatabaseAsync — which holds the source's SchemaDdlSemaphore through
        // RegisterAsync — cannot slip between the check and the Unregister on this node.
        // The semaphore is released before the heavier Drop/drain step so long-running DML on the
        // database can drain while the schema lock is no longer held.
        //
        // If the descriptor is not yet cached (database was never opened after server start) we open
        // it to obtain the semaphore. If the open itself fails the descriptor is unusable and no
        // concurrent branch-create can be in flight against it, so we fall through without the lock.
        //
        // This semaphore is node-local, and the registry KV scan and a remote Open are not atomic,
        // so it fences a branch-create on this node only. The cross-node case is fenced instead by
        // the replicated drop-intent marker acquired below, together with the source-liveness
        // re-read that branch-create runs after it publishes the child.
        DatabaseDescriptor? targetDescriptor = null;
        try
        {
            targetDescriptor = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        }
        catch
        {
            // Descriptor unavailable (faulted or being torn down). No concurrent branch-create can
            // be in flight against a descriptor that cannot be opened; proceed without the semaphore.
        }

        // Acquire a persistent drop-intent marker for this database id before the descendant scan.
        // CreateBranchDatabaseAsync checks this marker after registering a new child: if the marker
        // is set, branch-create unregisters the child and aborts. Together with HasLiveDescendantsAsync
        // this closes the cross-node race where a branch-create slips between the scan and UnregisterAsync
        // on a different cluster node. Raft linearizability ensures either:
        //   (a) intent committed before branch-create's RegisterAsync: branch-create sees it and aborts, or
        //   (b) branch-create's RegisterAsync committed first: HasLiveDescendantsAsync below sees the child
        //       and this drop aborts with DatabaseHasLiveDescendants.
        // A branch-create that stalls before RegisterAsync can also resume after this drop completes
        // and releases the marker. That ordering is fenced on the create side, which re-reads the
        // source's liveness by its immutable id after it publishes the child.
        // The intent is held through purge and released on every exit path (success, descendant check
        // failure, or Drop error) via the outer finally below.
        bool dropIntentAcquired = false;
        try
        {
            dropIntentAcquired = await registry.AcquireDropIntentAsync(entry.Id).ConfigureAwait(false);
            if (!dropIntentAcquired)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"A concurrent drop of '{ticket.DatabaseName}' is already in progress; retry later");
        }
        catch (CamusDBException) { throw; }
        catch (Exception intentEx)
        {
            // In cluster mode the drop-intent marker is the ONLY cross-node fence against a branch-create
            // racing on another node. If it cannot be acquired or confirmed, fail the drop CLOSED before
            // any destructive step (no orphan/drop marker, no unregister, no purge): the local
            // SchemaDdlSemaphore does not fence other nodes, so proceeding could unregister and purge the
            // parent while a remote branch-create publishes a child against it. Surface a retryable error.
            if (context.IsClusterMode)
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionMustRetry,
                    $"Could not acquire the cross-node drop fence for '{ticket.DatabaseName}' ({intentEx.Message}); retry the drop");

            // Standalone: no other node can race, so the single-node semaphore guard is sufficient and the
            // drop may proceed without the distributed fence.
            context.Logger.LogWarning(intentEx,
                "Could not acquire drop-intent marker for '{Database}' (standalone mode); proceeding under the single-node semaphore guard",
                ticket.DatabaseName);
        }

        // A root database dropped without FORCE is retained as a recoverable orphan: its keyspace is
        // kept on disk and the id becomes relinkable until the garbage collector reclaims it after the
        // retention window. Branch databases (and any FORCE drop) take the immediate-purge path — a
        // branch's recovery would require holding its parent's snapshot floor for the whole window, so
        // deferred drop is out of scope for branches.
        bool deferred = !ticket.Force && entry.Ancestors.Count == 0;

        // Single outer try/finally ensures the intent marker is released on every exit path:
        // successful drop, failed descendant check, or error during Drop/hold-release.
        try
        {
            if (targetDescriptor is not null)
                await targetDescriptor.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                // Re-check descendants under the semaphore (single-node guard) and after the intent
                // marker is set (cross-node guard). A branch-create on another node that registered its
                // child before we set the intent will be visible here; one that registered after we set
                // the intent will see the intent flag and abort itself.
                if (await registry.HasLiveDescendantsAsync(entry.Id).ConfigureAwait(false))
                    throw new CamusDBException(
                        CamusDBErrorCodes.DatabaseHasLiveDescendants,
                        $"Database '{ticket.DatabaseName}' cannot be dropped because it has live branch descendants. Drop all descendant branches first.");

                if (deferred)
                {
                    // Write the orphan record BEFORE unregistering so a crash between the two leaves the
                    // database still live (stale record, harmless) rather than data stranded with no
                    // recovery path. DroppedAt is minted from the HLC so the GC's eligibility decision is
                    // consistent across nodes.
                    HLCTimestamp droppedAt = context.SharedNode!.Raft.HybridLogicalClock
                        .SendOrLocalEvent(context.SharedNode.Raft.GetLocalNodeId());
                    await registry.WriteDatabaseOrphanAsync(new OrphanDatabaseRecord
                    {
                        Id = entry.Id,
                        FormerName = entry.Name,
                        DroppedAt = droppedAt,
                    }).ConfigureAwait(false);
                }
                else
                {
                    // Mark the drop in progress before unregistering so a crash during the (non-atomic,
                    // per-key) keyspace purge below can be resumed at startup. The marker is owner-scoped
                    // and cleared only after the purge fully completes.
                    await registry.MarkDroppingAsync(entry.Id).ConfigureAwait(false);
                }

                // Unregister first: once the registry KV entry is deleted and the in-memory cache
                // cleared, any concurrent Open(name) gets DatabaseDoesntExist immediately — the name
                // is unreachable before the descriptor is removed from the descriptor cache.
                // If Drop throws after this point, the name is already freed for recreate rather
                // than wedged (better failure mode than registering after Drop).
                await registry.UnregisterAsync(ticket.DatabaseName).ConfigureAwait(false);
            }
            finally
            {
                if (targetDescriptor is not null)
                    targetDescriptor.SchemaDdlSemaphore.Release();
            }

            // Deferred drop closes the database but leaves its keyspace intact for recovery.
            // Pass the shared node so a missing/faulted descriptor still purges the keyspace by id
            // (headless) instead of reporting a phantom success and leaking the keyspace.
            bool purged = await databaseDropper
                .Drop(entry.Id, purge: !deferred, headlessKahuna: context.SharedNode?.Kahuna)
                .ConfigureAwait(false);

            if (!deferred)
            {
                // Release the snapshot-floor hold this branch owned on its immediate parent so the
                // parent's pinned MVCC history can be reclaimed. Best-effort.
                if (!string.IsNullOrEmpty(entry.ImmediateParentHoldId) && context.SharedNode is not null)
                {
                    try
                    {
                        await context.SharedNode.Kahuna
                            .LocateAndReleaseSnapshotHold(entry.ImmediateParentHoldId, CancellationToken.None)
                            .ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        context.Logger.LogWarning(ex, "Failed to release snapshot hold {HoldId} for dropped branch '{Database}'", entry.ImmediateParentHoldId, ticket.DatabaseName);
                    }
                }

                // Clear the drop-in-progress marker ONLY if the purge verifiably completed. If it did
                // not (a delete/scan failure), leave the marker so the startup resume finishes the purge
                // — clearing it now would abandon leaked row/index/meta keys with no reclaim.
                if (purged)
                    await registry.ClearDroppingAsync(entry.Id).ConfigureAwait(false);
                else
                    context.Logger.LogWarning(
                        "DROP DATABASE FORCE for '{Database}' (id={Id}) did not fully purge; leaving drop-in-progress marker for startup resume",
                        ticket.DatabaseName, entry.Id);

                // A FORCE drop destroys the keyspace for good, so any stale orphan record for this id
                // (left by a crashed relink) must go too — otherwise the id stays "relinkable" to an
                // empty/partial keyspace.
                await registry.DeleteDatabaseOrphanAsync(entry.Id).ConfigureAwait(false);

                // Dropping the branch is the one action that resolves a lost-snapshot-protection
                // state, so retire its marker with it. Ids are never reused; best-effort.
                await registry.ClearSnapshotProtectionLostAsync(entry.Id).ConfigureAwait(false);
            }

            // Evict every cache entry for this database. The descriptor may no longer be available
            // after the drop, so use the entry id directly rather than targetDescriptor?.Cache.
            targetDescriptor?.Cache?.InvalidateDatabase(entry.Id);
        }
        finally
        {
            // Release the intent marker now that the keyspace is purged (or on any failure path).
            // Branch-creates that were blocked by this intent now see the id gone; any that already
            // checked and are in-flight will either unregister (if they saw the intent) or will be
            // caught by branch-create's authoritative post-publication liveness re-read of the
            // now-unregistered source id.
            if (dropIntentAcquired)
                await registry.ReleaseDropIntentAsync(entry.Id).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Recovers an orphaned (deferred-dropped) root database: re-attaches <see cref="RelinkDatabaseTicket.NewName"/>
    /// to the orphan's preserved id and opens it against the retained keyspace. The orphan's id, rows,
    /// indexes, and schema are all still on disk, so the reopened database is immediately populated.
    ///
    /// <para>Serialized against a concurrent GC purge (and another relink) of the same id via the same
    /// per-id drop-intent fence the GC takes. Ordering is register-then-delete-orphan so a crash between
    /// them leaves the database live with a stale orphan record (the GC skips ids that are registered)
    /// rather than data stranded with no name.</para>
    /// </summary>
    public async Task<DatabaseDescriptor> RelinkDatabase(RelinkDatabaseTicket ticket)
    {
        context.Validator.Validate(ticket);

        DatabaseRegistry registry = await context.Registry.ConfigureAwait(false);

        // Fence the id so a concurrent GC purge or a second relink cannot race this recovery. All the
        // authoritative state decisions below happen under this fence.
        bool fenced = await registry.AcquireDropIntentAsync(ticket.OrphanId).ConfigureAwait(false);
        if (!fenced)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"A concurrent operation on orphan id '{ticket.OrphanId}' is in progress; retry later");

        try
        {
            // Idempotency by id (not just by name): if the id is already live — a relink that committed
            // its registration but crashed before deleting the orphan record — do not mint a second
            // alias. Re-check authoritatively (cross-node) under the fence.
            string? existingName = await registry.TryResolveNameByIdAsync(ticket.OrphanId).ConfigureAwait(false);
            if (existingName is not null)
            {
                // Already relinked. If under the requested name, finish idempotently (clean any stale
                // orphan record and return the live database); otherwise it is a conflicting second name.
                // Database names are case-insensitive, so compare the stored name to the requested one
                // case-insensitively rather than assuming either was folded.
                if (!string.Equals(existingName, ticket.NewName, StringComparison.OrdinalIgnoreCase))
                    throw new CamusDBException(
                        CamusDBErrorCodes.DatabaseAlreadyExists,
                        $"Database id '{ticket.OrphanId}' is already live under name '{existingName}'");

                await registry.DeleteDatabaseOrphanAsync(ticket.OrphanId).ConfigureAwait(false);
                return await context.DatabaseOpener.Open(existingName).ConfigureAwait(false);
            }

            // The target name must be free (cache + live-KV check for cross-node visibility).
            if (await registry.TryResolveEntryAsync(ticket.NewName).ConfigureAwait(false) is not null)
                throw new CamusDBException(
                    CamusDBErrorCodes.DatabaseAlreadyExists,
                    $"Database '{ticket.NewName}' already exists");

            OrphanDatabaseRecord? orphan = await registry.TryGetDatabaseOrphanAsync(ticket.OrphanId).ConfigureAwait(false);
            if (orphan is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.OrphanNotFound,
                    $"No orphaned database with id '{ticket.OrphanId}' is available to relink (never dropped, or already reclaimed)");

            // Re-register the name to the preserved id; the keyspace is already on disk.
            await registry.RegisterAsync(ticket.NewName, orphan.Id).ConfigureAwait(false);

            // The database is live again — remove the orphan record so the GC leaves it alone.
            await registry.DeleteDatabaseOrphanAsync(orphan.Id).ConfigureAwait(false);

            return await context.DatabaseOpener.Open(ticket.NewName).ConfigureAwait(false);
        }
        finally
        {
            await registry.ReleaseDropIntentAsync(ticket.OrphanId).ConfigureAwait(false);
        }
    }

    public async Task RenameDatabase(RenameDatabaseTicket ticket)
    {
        DatabaseRegistry registry = await context.Registry.ConfigureAwait(false);

        // Delegates all validation (old-exists, new-free, reserved-name check) to the registry.
        await registry.RenameAsync(ticket.OldName, ticket.NewName).ConfigureAwait(false);

        // The descriptor cache is keyed by id (which is unchanged after rename), so the existing
        // cached descriptor continues to serve Open(newName) via the same live Kahuna node.
        // Do NOT evict the descriptor: evicting without flushing/disposing the node would orphan
        // the running SQLite+Raft process (standalone) or leak the schema-replication subscription
        // (cluster), and a second Load would open a second node on the same storage (double-writer).
        //
        // Instead refresh the descriptor's display name in place. It was previously left stale on the
        // theory that Name is display-only — but any path that fed it back into a by-name resolution
        // then resolved a name the registry no longer knows, which is exactly how INSERT broke after a
        // rename. Request-scoped code must still prefer ticket.DatabaseName; this keeps the fallback
        // honest rather than relying on every caller getting it right.
        await RefreshCachedDescriptorNameAsync(registry, ticket.NewName).ConfigureAwait(false);
    }

    /// <summary>
    /// Points the cached descriptor's display name at <paramref name="newName"/> after a committed
    /// rename.
    ///
    /// <para>Resolved by id rather than by matching the old name: the descriptor cache is keyed by id,
    /// the id is what a rename preserves, and the registry has already been updated so the new name
    /// resolves to it.</para>
    ///
    /// <para>Best-effort by design — the registry swap is already durable when this runs, so a failure
    /// here must not fail the statement; the worst case is a stale name in log output until the
    /// descriptor is recycled. Only a descriptor that is already materialized is touched: forcing an
    /// unstarted cache entry would load a database nobody asked for, and a later load already picks up
    /// the new name.</para>
    /// </summary>
    private async Task RefreshCachedDescriptorNameAsync(DatabaseRegistry registry, string newName)
    {
        try
        {
            string? id = await registry.TryResolveIdAsync(newName).ConfigureAwait(false);
            if (id is null)
                return;

            if (!databaseDescriptors.Descriptors.TryGetValue(id, out Nito.AsyncEx.AsyncLazy<DatabaseDescriptor>? lazy))
                return;

            if (!lazy.IsStarted || !lazy.Task.IsCompletedSuccessfully)
                return;

            lazy.Task.Result.SetName(newName);
        }
        catch (Exception ex)
        {
            context.Logger.LogWarning(ex, "Failed to refresh the cached descriptor name after renaming to {NewName}", newName);
        }
    }
}
