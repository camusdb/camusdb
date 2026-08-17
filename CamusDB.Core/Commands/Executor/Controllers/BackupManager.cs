/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using Kahuna.Shared.Communication.Rest;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Executes node-wide backup and point-in-time-recovery operations by delegating to the shared embedded
/// Kahuna node. Backups cover every database at once (they share one node), so this is a server-level
/// controller, not a database-scoped one. Its two jobs beyond delegation are (1) translating Kahuna's
/// public <see cref="KahunaBackupException"/> (via its <see cref="KahunaBackupOutcome"/>) and
/// cancellation into <see cref="CamusDBException"/> with a stable <see cref="CamusDBErrorCodes"/> value,
/// and (2) mapping Kahuna transport DTOs to CamusDB result models so the command surface never leaks
/// <c>Kahuna.Shared</c> types. Authorization (superuser) and the backup-configured gate are applied by
/// the caller in <c>CommandExecutor</c> before these run.
/// </summary>
internal sealed class BackupManager
{
    private readonly EmbeddedKahuna node;
    private readonly ILogger<ICamusDB> logger;

    public BackupManager(EmbeddedKahuna node, ILogger<ICamusDB> logger)
    {
        this.node = node;
        this.logger = logger;
    }

    /// <summary>
    /// Common gate for every backup/PITR admin operation: superuser authorization (when authentication
    /// is enabled) followed by the backup-configured check. Both are server-level and privileged, so a
    /// non-superuser is refused and an unconfigured node reports the capability as unavailable rather
    /// than leaking a raw engine exception.
    ///
    /// <para><paramref name="authenticationEnabled"/> is passed rather than read from a held
    /// configuration snapshot so the decision uses the caller's already-pinned snapshot — the same one
    /// the rest of that request was authorized against.</para>
    /// </summary>
    internal void EnsureAllowed(Principal? principal, bool authenticationEnabled)
    {
        if (authenticationEnabled)
        {
            if (principal is null)
                throw new CamusDBException(CamusDBErrorCodes.AuthenticationFailed, "Authentication required");
            if (!principal.IsSuperuser)
                throw new CamusDBException(CamusDBErrorCodes.InsufficientPrivilege, "Backup administration requires a superuser");
        }

        if (!node.IsBackupConfigured)
            throw new CamusDBException(
                CamusDBErrorCodes.BackupNotConfigured,
                "Backups are not configured on this node; set 'kahuna.backup_dir' and restart to enable them");
    }

    public async Task<BackupInfo> TakeBackup(TakeBackupTicket ticket, CancellationToken cancellationToken = default)
    {
        try
        {
            KahunaBackupInfo info = ticket.Kind switch
            {
                BackupKind.Full => await node.TakeFullBackupAsync(cancellationToken).ConfigureAwait(false),
                BackupKind.Incremental => await node.TakeIncrementalBackupAsync(ticket.ParentBackupId!.Value, cancellationToken).ConfigureAwait(false),
                BackupKind.Coordinated => await node.TakeCoordinatedBackupAsync(cancellationToken).ConfigureAwait(false),
                _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unknown backup kind '{ticket.Kind}'"),
            };

            Log.LogBackupTaken(logger, ticket.Kind, info.BackupId);
            return Map(info);
        }
        catch (Exception ex) when (Translate(ex) is { } mapped)
        {
            throw mapped;
        }
    }

    public async Task<IReadOnlyList<BackupInfo>> ListBackups(CancellationToken cancellationToken = default)
    {
        try
        {
            IReadOnlyList<KahunaBackupInfo> all = await node.ListBackupsAsync(cancellationToken).ConfigureAwait(false);
            return all.Select(Map).ToList();
        }
        catch (Exception ex) when (Translate(ex) is { } mapped)
        {
            throw mapped;
        }
    }

    public async Task<IReadOnlyList<BackupInfo>> GetBackupChain(GetBackupChainTicket ticket, CancellationToken cancellationToken = default)
    {
        try
        {
            IReadOnlyList<KahunaBackupInfo> chain = await node.GetBackupChainAsync(ticket.LeafBackupId, cancellationToken).ConfigureAwait(false);
            return chain.Select(Map).ToList();
        }
        catch (Exception ex) when (Translate(ex) is { } mapped)
        {
            throw mapped;
        }
    }

    public async Task<BackupGcResult> RunGarbageCollection(bool dryRun, CancellationToken cancellationToken = default)
    {
        try
        {
            KahunaBackupGcResult gc = await node.RunBackupGarbageCollectionAsync(dryRun, cancellationToken).ConfigureAwait(false);
            return new BackupGcResult(
                Applied: gc.Applied,
                BytesReclaimed: gc.BytesReclaimed,
                RetentionDeletions: gc.RetentionDeletions
                    .Select(d => new BackupGcDeletion(d.BackupId, d.Type, d.CreatedAtUtc, d.Bytes, d.Reason)).ToList(),
                OrphanReclamations: gc.OrphanReclamations
                    .Select(o => new BackupGcOrphan(o.Name, o.IsDirectory, o.Reason)).ToList());
        }
        catch (Exception ex) when (Translate(ex) is { } mapped)
        {
            throw mapped;
        }
    }

    public async Task<RestoreResult> Restore(RestoreBackupTicket ticket, CancellationToken cancellationToken = default)
    {
        // Restore is administrative and denied unless a server-owned restore root is configured (or the
        // unconfined opt-in is set). Refuse early with a clear message rather than letting Kahuna reject a
        // destination outside an unset root.
        if (!node.IsRemoteRestoreAllowed)
            throw new CamusDBException(
                CamusDBErrorCodes.RemoteRestoreDisabled,
                "Restore is disabled on this node; set 'kahuna.restore_root' to a directory restore targets must live under");

        // The ticket's target is a CamusDB *data root*. Kahuna's RestoreToAsync targetDir corresponds to
        // the node's storage path (which CamusDB lays out as {data_dir}/kv), so we restore into
        // {dataRoot}/kv — Kahuna then writes the RocksDB checkpoint into {dataRoot}/kv/{revision}, exactly
        // where a node booted with data_dir={dataRoot} looks for storage. We also create an empty
        // {dataRoot}/wal so the fresh node has its WAL directory. The result is directly bootable via
        // data_dir={dataRoot} with no manual file moves.
        string dataRoot = ticket.TargetDir;
        string storageTarget = Path.Combine(dataRoot, "kv");
        string walDir = Path.Combine(dataRoot, "wal");

        KahunaRestoreResponse response;
        try
        {
            response = await node
                .RestoreToAsync(ticket.LeafBackupId, storageTarget, ticket.TargetTimeMs, cancellationToken)
                .ConfigureAwait(false);

            Directory.CreateDirectory(walDir);
        }
        catch (Exception ex) when (Translate(ex) is { } mapped)
        {
            throw mapped;
        }
        catch (Exception ex)
        {
            // Any other failure while copying the base image or replaying WAL leaves a partial target
            // directory the operator must discard. Surface it as a restore failure rather than a raw 500.
            throw new CamusDBException(CamusDBErrorCodes.RestoreFailed, $"Restore failed: {ex.Message}");
        }

        Log.LogBackupRestored(logger, ticket.LeafBackupId, dataRoot);

        return new RestoreResult(
            DataRoot: dataRoot,
            PartitionsRestored: response.PartitionsRestored,
            EntriesApplied: response.EntriesApplied,
            LastAppliedPhysicalMs: response.LastAppliedPhysicalMs,
            MinRecoverablePhysicalMs: response.MinRecoverablePhysicalMs,
            MaxRecoverablePhysicalMs: response.MaxRecoverablePhysicalMs,
            Chain: response.Chain.Select(Map).ToList());
    }

    private static BackupInfo Map(KahunaBackupInfo info) => new(
        BackupId: info.BackupId,
        FormatVersion: info.FormatVersion,
        Type: info.Type,
        CreatedAtUtc: info.CreatedAtUtc,
        ParentBackupId: info.ParentBackupId,
        PartitionCount: info.PartitionCount,
        ClusterSnapshotNode: info.ClusterSnapshotNode,
        ClusterSnapshotPhysical: info.ClusterSnapshotPhysical,
        ClusterSnapshotCounter: info.ClusterSnapshotCounter,
        RequestedKind: info.RequestedKind,
        ActualKind: info.ActualKind,
        SubstitutionReason: info.SubstitutionReason,
        IsInvalid: info.IsInvalid,
        InvalidReason: info.InvalidReason,
        MinRecoverablePhysicalMs: info.MinRecoverablePhysicalMs,
        MaxRecoverablePhysicalMs: info.MaxRecoverablePhysicalMs,
        ClusterId: info.ClusterId,
        CoordinatorNode: info.CoordinatorNode);

    /// <summary>
    /// Maps a Kahuna backup/PITR failure to a <see cref="CamusDBException"/> with a stable error code, or
    /// returns null for an exception this controller does not classify (letting the caller wrap or
    /// rethrow). Kahuna exposes a public <see cref="KahunaBackupException"/> carrying a typed
    /// <see cref="KahunaBackupOutcome"/>, so classification is by that enum — never by exception-type name
    /// or message. Cancellation surfaces as <see cref="OperationCanceledException"/> (Kahuna does not wrap
    /// it), so it is handled explicitly.
    /// </summary>
    private static CamusDBException? Translate(Exception ex)
    {
        if (ex is OperationCanceledException)
            return new CamusDBException(CamusDBErrorCodes.BackupCancelled, "The backup operation was cancelled");

        if (ex is KahunaBackupException backupEx)
            return new CamusDBException(MapOutcome(backupEx.Outcome), backupEx.Message);

        return null;
    }

    /// <summary>Maps Kahuna's public backup outcome to the CamusDB error code with the right HTTP status.</summary>
    private static string MapOutcome(KahunaBackupOutcome outcome) => outcome switch
    {
        KahunaBackupOutcome.NotConfigured => CamusDBErrorCodes.BackupNotConfigured,
        KahunaBackupOutcome.ParentMissing => CamusDBErrorCodes.BackupParentMissing,
        KahunaBackupOutcome.NeedsFull => CamusDBErrorCodes.BackupNeedsFullBackup,
        KahunaBackupOutcome.CorruptChain => CamusDBErrorCodes.BackupChainInvalid,
        KahunaBackupOutcome.CorruptArtifact => CamusDBErrorCodes.BackupCorruptArtifact,
        KahunaBackupOutcome.TargetConflict => CamusDBErrorCodes.RestoreTargetConflict,
        KahunaBackupOutcome.TargetOutsideCoverage => CamusDBErrorCodes.RestorePointOutOfWindow,
        KahunaBackupOutcome.Cancelled => CamusDBErrorCodes.BackupCancelled,
        KahunaBackupOutcome.IoError => CamusDBErrorCodes.RestoreFailed,
        KahunaBackupOutcome.RetryableLeadershipLoss => CamusDBErrorCodes.BackupRetryableLeadershipLoss,
        KahunaBackupOutcome.ExactCheckpointUnavailable => CamusDBErrorCodes.BackupExactCheckpointUnavailable,
        KahunaBackupOutcome.UnsupportedFormat => CamusDBErrorCodes.BackupUnsupportedFormat,
        KahunaBackupOutcome.TopologyChanged => CamusDBErrorCodes.BackupTopologyChanged,
        KahunaBackupOutcome.NotBackupCoordinator => CamusDBErrorCodes.BackupNotCoordinator,
        KahunaBackupOutcome.InsecureRoot => CamusDBErrorCodes.BackupInsecureRoot,
        _ => CamusDBErrorCodes.RestoreFailed,
    };
}
