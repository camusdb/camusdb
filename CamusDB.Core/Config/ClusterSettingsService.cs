/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using CamusDB.Core.Config.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Config;

/// <summary>
/// Forwards a cluster-setting change from a non-leader node to the settings-partition leader over
/// the authenticated internal route, mirroring how a schema DDL forwards. Without this, "applied
/// consistently no matter which node received it" holds only for the node that happens to lead.
/// Returns true when the leader applied the change, null when the target answered not-leader (the
/// caller re-resolves and retries), and throws when the leader rejected the change outright.
/// </summary>
public interface IClusterSettingsForwarder
{
    /// <param name="value">The new scalar text, or null for a reset (drop the overlay entry).</param>
    Task<bool?> ForwardChangeAsync(string leader, string key, string? value, CancellationToken cancellationToken);
}

/// <summary>
/// One replicated cluster-setting change: set (<see cref="Value"/> non-null) or reset (null).
/// Serialized as the settings-log entry payload; conflict resolution is last-writer-wins in Raft
/// commit order, so no compare-and-swap or wall-clock ordering appears anywhere in this path.
/// </summary>
public sealed record ClusterSettingChange(string Key, string? Value);

/// <summary>
/// Owns the runtime cluster-settings overlay: the durable <c>_system/settings/</c> keyspace, the
/// replicated settings log that carries changes to every node, and the re-resolution that turns
/// the overlay into a published <see cref="CamusDBOptions"/> snapshot.
///
/// <para>A change is applied by rebuilding the node's configuration through the normal chain —
/// file, then CLI/environment, then the cluster overlay last, then
/// <see cref="ConfigDefinition.Validate"/> against the <em>result</em>, then
/// <see cref="ConfigResolver.Resolve"/> — so precedence, coercion, and cross-field invariants all
/// come from ordering rather than re-implementation. The resolved record is handed to the
/// <see cref="CamusDBOptionsHolder"/>, whose subscribers fan it out to the engine.</para>
///
/// <para>Durability follows the schema-replication precedent: the committed log is the source of
/// truth; one durable key per setting under <c>_system/settings/k:</c> is the load-time
/// checkpoint, written by the <em>proposer after</em> the commit (the apply callback runs inside
/// the commit pipeline and must never persist — persisting there deadlocks the partition); a node
/// that was down catches up by WAL replay through the settings-log restore buffer, and a
/// generation stamp — a plain key's <em>revision</em>, never a sequence value — closes the small
/// subscribe-window race at load.</para>
///
/// <para>In standalone mode the same validate → apply → persist → publish path runs with no
/// replication, so single-node behavior and tests exercise the real pipeline rather than a
/// bypass.</para>
/// </summary>
public sealed class ClusterSettingsService : IAsyncDisposable
{
    private const string SettingKeyPrefix = "_system/settings/k:";

    private const string SettingsBucket = "_system/settings";

    /// <summary>
    /// The generation stamp. The store-assigned <em>revision</em> of this key is the generation —
    /// its bytes are never read. A plain key rather than a sequence because a sequence's durable
    /// value is a reserved-block high-water mark that does not move per allocation, a mistake
    /// already paid for once in the database registry.
    /// </summary>
    private const string GenerationKey = "_system/settings/generation";

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly EmbeddedKahuna sharedNode;

    private readonly CamusDBOptionsHolder holder;

    private readonly Func<ConfigDefinition> baseDefinitionFactory;

    private readonly Func<CamusDBOptions, CamusDBOptions>? postResolve;

    private readonly bool isClusterMode;

    private readonly IClusterSettingsForwarder? forwarder;

    private readonly ILogger logger;

    /// <summary>
    /// Serializes every overlay mutation and publish on this node — the single-writer requirement:
    /// two concurrent applies that each read-modify-write the overlay could otherwise lose one
    /// another. Raft order decides which of two cluster-wide changes is second; this semaphore only
    /// makes the local application of that order atomic.
    /// </summary>
    private readonly SemaphoreSlim overlaySync = new(1, 1);

    /// <summary>Current overlay, keyed by yaml setting name. Mutated only under <see cref="overlaySync"/>.</summary>
    private readonly Dictionary<string, string> overlay = new(StringComparer.OrdinalIgnoreCase);

    private IDisposable? busSubscription;

    private bool started;

    public ClusterSettingsService(
        EmbeddedKahuna sharedNode,
        CamusDBOptionsHolder holder,
        Func<ConfigDefinition> baseDefinitionFactory,
        Func<CamusDBOptions, CamusDBOptions>? postResolve,
        bool isClusterMode,
        IClusterSettingsForwarder? forwarder,
        ILogger logger)
    {
        this.sharedNode = sharedNode;
        this.holder = holder;
        this.baseDefinitionFactory = baseDefinitionFactory;
        this.postResolve = postResolve;
        this.isClusterMode = isClusterMode;
        this.forwarder = forwarder;
        this.logger = logger;
    }

    private IKahuna Kahuna => sharedNode.Kahuna;

    /// <summary>
    /// Loads the persisted overlay, subscribes to the settings log, and publishes the merged
    /// configuration. Call after the embedded node has started and before the engine components
    /// that should see the merged view are resolved — the narrow boot window between
    /// <c>sharedNode.StartAsync()</c> and the executor's construction. The generation re-check
    /// closes the race where a change commits cluster-wide between the checkpoint scan and the
    /// bus subscription.
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        await sharedNode.WaitUntilStartedAsync(cancellationToken).ConfigureAwait(false);

        long generationBefore = await ReadGenerationAsync(cancellationToken).ConfigureAwait(false);

        await overlaySync.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await ScanOverlayLockedAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            overlaySync.Release();
        }

        busSubscription = sharedNode.RegisterClusterSettingsApply(
            (partition, entry) => OnLogEntryAsync(entry),
            (partition, entry) => OnLogEntryAsync(entry),
            onRestoreFinished: () => RepublishAsync());

        long generationAfter = await ReadGenerationAsync(cancellationToken).ConfigureAwait(false);
        if (generationAfter != generationBefore)
        {
            await overlaySync.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await ScanOverlayLockedAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                overlaySync.Release();
            }
        }

        await RepublishAsync().ConfigureAwait(false);
        started = true;
    }

    /// <summary>Current overlay entries — what the cluster carries beyond local configuration.</summary>
    public IReadOnlyList<(string Key, string Value)> List()
    {
        overlaySync.Wait();
        try
        {
            return overlay.OrderBy(static kv => kv.Key, StringComparer.Ordinal)
                          .Select(static kv => (kv.Key, kv.Value))
                          .ToList();
        }
        finally
        {
            overlaySync.Release();
        }
    }

    /// <summary>Sets one runtime cluster setting fleet-wide. See <see cref="ChangeAsync"/>.</summary>
    public Task SetAsync(string key, string valueText, CancellationToken cancellationToken = default)
        => ChangeAsync(new ClusterSettingChange(key, valueText), cancellationToken);

    /// <summary>
    /// Drops one overlay entry so the local chain resolves the key again — a node whose file names
    /// the key returns to its file value. Deliberately not "write the built-in default": that
    /// would leave the cluster permanently overriding every file with a value nobody chose.
    /// </summary>
    public Task ResetAsync(string key, CancellationToken cancellationToken = default)
        => ChangeAsync(new ClusterSettingChange(key, null), cancellationToken);

    private async Task ChangeAsync(ClusterSettingChange change, CancellationToken cancellationToken)
    {
        // TryApplyAsLeaderAsync rejects invalid keys and values before anything replicates: a value
        // that fails must never reach the log, because a committed-but-invalid change would apply
        // on every node and then fail identically on each of them. Standalone mode applies and
        // persists there directly, so the same statement works with no replication in the picture.
        for (int attempt = 0; attempt < 3; attempt++)
        {
            if (await TryApplyAsLeaderAsync(change, cancellationToken).ConfigureAwait(false))
                return;

            string leader = await sharedNode.WaitForClusterSettingsLeaderAsync(cancellationToken).ConfigureAwait(false);

            if (forwarder is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"This node does not lead the settings partition (leader: {leader}) and no forwarder is configured");

            bool? forwarded = await forwarder.ForwardChangeAsync(leader, change.Key, change.Value, cancellationToken)
                .ConfigureAwait(false);

            if (forwarded == true)
            {
                // Read-your-writes on the submitting node: wait until the replicated entry has
                // applied here, so the caller's next SHOW reflects the change it just made.
                await WaitForLocalApplyAsync(change, cancellationToken).ConfigureAwait(false);
                return;
            }

            // null means the target answered not-leader; loop re-resolves and retries.
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Could not reach a stable settings-partition leader to apply '{change.Key}'");
    }

    /// <summary>
    /// Validates and applies a change if this node currently leads the settings partition:
    /// replicate through Raft, then write the durable checkpoint from the proposer (the apply
    /// callback fired inline during the commit and never persists). Returns false when this node
    /// is not the leader — the caller (a local statement, or the internal forwarding route)
    /// decides whether to forward or answer not-leader, so a forwarded request can never chain
    /// from node to node.
    /// </summary>
    public async Task<bool> TryApplyAsLeaderAsync(ClusterSettingChange change, CancellationToken cancellationToken = default)
    {
        ValidateKey(change.Key);
        await ValidateResultingConfigurationAsync(change, cancellationToken).ConfigureAwait(false);

        if (!isClusterMode)
        {
            // Standalone: no partition, no leadership — this node is trivially the authority.
            await ApplyPersistAndPublishAsync(change, persist: true, cancellationToken).ConfigureAwait(false);
            return true;
        }

        if (!await sharedNode.AmIClusterSettingsLeaderAsync(cancellationToken).ConfigureAwait(false))
            return false;

        byte[] entry = JsonSerializer.SerializeToUtf8Bytes(change, JsonOptions);

        SchemaReplicationResult result =
            await sharedNode.ReplicateClusterSettingsChangeAsync(entry, cancellationToken).ConfigureAwait(false);

        if (result.Outcome == SchemaReplicationOutcome.Committed)
        {
            // The commit already fanned out to the local apply callback; the proposer's remaining
            // duty is the durable checkpoint, never written from apply.
            await PersistCheckpointAsync(change, cancellationToken).ConfigureAwait(false);
            return true;
        }

        if (result.Outcome == SchemaReplicationOutcome.NotLeader)
            return false; // leadership moved between the check and the proposal

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Cluster setting change for '{change.Key}' was not committed: {result.Outcome} {result.Status}");
    }

    /// <summary>
    /// The rejection taxonomy: unknown key, and known-but-restart-class key, each with an error
    /// that says which it was — "unknown setting" for a real setting that cannot be changed live
    /// sends the operator hunting for a typo that isn't there.
    /// </summary>
    private static void ValidateKey(string key)
    {
        switch (ClusterSettingsOverlay.Classify(key))
        {
            case ClusterSettingsOverlay.KeyClass.Unknown:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unknown cluster setting '{key}'");

            case ClusterSettingsOverlay.KeyClass.RestartOnly:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Setting '{key}' is restart-class: it is read once when the node is built and cannot be " +
                    "changed at runtime. Change it in the configuration file and restart the node.");
        }
    }

    /// <summary>
    /// Builds the configuration that would result from this change — base layers, current overlay
    /// with the change applied on top — and validates it, so range checks and cross-field
    /// invariants reject against the <em>result</em>, not the submitted key alone.
    /// </summary>
    private async Task ValidateResultingConfigurationAsync(ClusterSettingChange change, CancellationToken cancellationToken)
    {
        await overlaySync.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            Dictionary<string, string> candidate = new(overlay, StringComparer.OrdinalIgnoreCase);

            if (change.Value is null)
                candidate.Remove(change.Key);
            else
                candidate[change.Key] = change.Value;

            ResolveLocked(candidate);
        }
        finally
        {
            overlaySync.Release();
        }
    }

    /// <summary>Applies one committed or WAL-restored settings-log entry to this node. Never persists.</summary>
    private async Task<bool> OnLogEntryAsync(byte[] entryBytes)
    {
        ClusterSettingChange? change;
        try
        {
            change = JsonSerializer.Deserialize<ClusterSettingChange>(entryBytes, JsonOptions);
        }
        catch (JsonException e)
        {
            logger.LogError("Undecodable cluster-settings log entry skipped: {Message}", e.Message);
            return true;
        }

        if (change is null || string.IsNullOrEmpty(change.Key))
            return true;

        try
        {
            await ApplyPersistAndPublishAsync(change, persist: false, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            // A committed entry this node cannot resolve (e.g. its base configuration disagrees so
            // validation fails) must not wedge the commit pipeline. Log loudly; the overlay entry
            // is still recorded so a later re-resolve can succeed.
            logger.LogError("Failed to apply committed cluster setting '{Key}': {Message}", change.Key, e.Message);
        }

        return true;
    }

    /// <summary>
    /// The one mutation path: updates the overlay under the single-writer lock, optionally writes
    /// the durable checkpoint (proposer/standalone only — never from the apply callback), then
    /// resolves and publishes the merged snapshot.
    /// </summary>
    private async Task ApplyPersistAndPublishAsync(ClusterSettingChange change, bool persist, CancellationToken cancellationToken)
    {
        await overlaySync.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (change.Value is null)
                overlay.Remove(change.Key);
            else
                overlay[change.Key] = change.Value;

            CamusDBOptions resolved = ResolveLocked(overlay);
            holder.Publish(resolved);
        }
        finally
        {
            overlaySync.Release();
        }

        if (persist)
            await PersistCheckpointAsync(change, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Rebuilds the effective options from base layers plus <paramref name="entries"/>. Cluster
    /// precedence falls out of applying the overlay after every local layer, and validation runs
    /// against the resulting configuration. Throws (surfacing the validator's own message, which
    /// names both sides of a cross-field check) when the result is invalid.
    /// </summary>
    private CamusDBOptions ResolveLocked(Dictionary<string, string> entries)
    {
        ConfigDefinition definition = baseDefinitionFactory();

        foreach (KeyValuePair<string, string> entry in entries)
            ClusterSettingsOverlay.Apply(definition, entry.Key, entry.Value);

        definition.Validate();

        CamusDBOptions resolved = ConfigResolver.Resolve(definition);
        return postResolve is null ? resolved : postResolve(resolved);
    }

    private async Task RepublishAsync()
    {
        await overlaySync.WaitAsync().ConfigureAwait(false);
        try
        {
            holder.Publish(ResolveLocked(overlay));
        }
        catch (Exception e)
        {
            logger.LogError("Failed to republish cluster settings: {Message}", e.Message);
        }
        finally
        {
            overlaySync.Release();
        }
    }

    /// <summary>
    /// Proposer-side durability: one key per setting so a restarting node loads the overlay
    /// without replaying the whole log, plus the generation bump whose revision is the change
    /// stamp. A failure here is logged rather than surfaced — the committed log is the source of
    /// truth and WAL replay reconstructs the overlay on restart.
    /// </summary>
    private async Task PersistCheckpointAsync(ClusterSettingChange change, CancellationToken cancellationToken)
    {
        try
        {
            string key = SettingKeyPrefix + change.Key.ToLowerInvariant();

            if (change.Value is null)
                await Kahuna.LocateAndTryDeleteKeyValue(
                    HLCTimestamp.Zero, key, KeyValueDurability.Persistent, cancellationToken).ConfigureAwait(false);
            else
                await Kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, System.Text.Encoding.UTF8.GetBytes(change.Value), null, -1,
                    KeyValueFlags.Set, 0, KeyValueDurability.Persistent, cancellationToken).ConfigureAwait(false);

            await Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, GenerationKey, System.Text.Encoding.UTF8.GetBytes(change.Key), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            logger.LogWarning(
                "Cluster-settings checkpoint write for '{Key}' failed (the committed log remains the source " +
                "of truth; WAL replay recovers it on restart): {Message}", change.Key, e.Message);
        }
    }

    private async Task<long> ReadGenerationAsync(CancellationToken cancellationToken)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, GenerationKey, -1,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, cancellationToken).ConfigureAwait(false);

        // revision + 1 so "never written" (0) stays distinguishable from the first write (revision 0).
        return type == KeyValueResponseType.Get && entry is not null ? entry.Revision + 1 : 0;
    }

    /// <summary>Replaces the overlay with the persisted checkpoint. Caller holds <see cref="overlaySync"/>.</summary>
    private async Task ScanOverlayLockedAsync(CancellationToken cancellationToken)
    {
        overlay.Clear();

        KvTransaction tx = KvTransaction.CreateReadOnly();

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in Kahuna.LocateAndScanRange(
            tx.TransactionId, SettingsBucket, null, true, null, true, 1000,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, cancellationToken).ConfigureAwait(false))
        {
            if (!key.StartsWith(SettingKeyPrefix, StringComparison.Ordinal) || entry.Value is null)
                continue;

            overlay[key[SettingKeyPrefix.Length..]] = System.Text.Encoding.UTF8.GetString(entry.Value);
        }
    }

    /// <summary>
    /// Bounded wait until a forwarded change is visible in this node's own overlay, giving the
    /// submitting node read-your-writes without inventing an ack protocol: the change arrives via
    /// ordinary replication, this only waits for it.
    /// </summary>
    private async Task WaitForLocalApplyAsync(ClusterSettingChange change, CancellationToken cancellationToken)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(5);

        while (DateTime.UtcNow < deadline)
        {
            await overlaySync.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                bool applied = change.Value is null
                    ? !overlay.ContainsKey(change.Key)
                    : overlay.TryGetValue(change.Key, out string? current) &&
                      string.Equals(current, change.Value, StringComparison.Ordinal);

                if (applied)
                    return;
            }
            finally
            {
                overlaySync.Release();
            }

            await Task.Delay(25, cancellationToken).ConfigureAwait(false);
        }

        logger.LogWarning(
            "Forwarded cluster setting '{Key}' was accepted by the leader but has not applied locally yet; " +
            "it will arrive via replication", change.Key);
    }

    public ValueTask DisposeAsync()
    {
        busSubscription?.Dispose();
        overlaySync.Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>True once <see cref="StartAsync"/> completed; used by hosts that gate the SQL surface.</summary>
    public bool Started => started;
}
